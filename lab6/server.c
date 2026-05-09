#include "common.h"
#include <pthread.h>

#define THREAD_POOL_SIZE    10
#define MAX_CLIENTS         100
#define QUEUE_SIZE          128
#define MAX_OFFLINE         500
#define MAX_HIST            500
#define DEFAULT_HISTORY     20
#define HISTORY_FILE        "chat_history.json"
#define DEDUP_SIZE          32

/* ── Noise simulation ────────────────────────────────────────────────────── */
typedef struct {
    int   delay_ms;
    float drop_rate;
    float corrupt_rate;
} SimConfig;

static SimConfig sim = {0, 0.0f, 0.0f};

/* ── Types ───────────────────────────────────────────────────────────────── */
typedef struct {
    int  fd;
    char nickname[MAX_NAME];
    char ip[INET_ADDRSTRLEN];
    int  port;
    int  authenticated;
} Client;

typedef struct {
    char     sender[MAX_NAME];
    char     receiver[MAX_NAME];
    char     text[MAX_PAYLOAD];
    time_t   timestamp;
    uint32_t msg_id;
} OfflineMsg;

typedef struct {
    uint32_t msg_id;
    time_t   timestamp;
    char     sender[MAX_NAME];
    char     receiver[MAX_NAME];
    uint8_t  type;
    char     text[MAX_PAYLOAD];
    int      delivered;
    int      is_offline;
} HistEntry;

/* ── Globals ─────────────────────────────────────────────────────────────── */
static Client          clients[MAX_CLIENTS];
static int             client_count = 0;
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

static int             conn_queue[QUEUE_SIZE];
static int             q_head = 0, q_tail = 0, q_count = 0;
static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  queue_cond  = PTHREAD_COND_INITIALIZER;

static OfflineMsg      offline_queue[MAX_OFFLINE];
static int             offline_count = 0;
static pthread_mutex_t offline_mutex = PTHREAD_MUTEX_INITIALIZER;

static HistEntry       hist[MAX_HIST];
static int             hist_count = 0;
static pthread_mutex_t hist_mutex = PTHREAD_MUTEX_INITIALIZER;

static uint32_t        next_id = 1;
static pthread_mutex_t id_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ── Helpers ─────────────────────────────────────────────────────────────── */
static uint32_t alloc_id(void) {
    pthread_mutex_lock(&id_mutex);
    uint32_t id = next_id++;
    pthread_mutex_unlock(&id_mutex);
    return id;
}

static void build_msg(MessageEx *m, uint8_t type, uint32_t id, time_t ts,
                      const char *sender, const char *receiver, const char *payload) {
    memset(m, 0, sizeof(*m));
    m->type      = type;
    m->msg_id    = id;
    m->timestamp = ts ? ts : time(NULL);
    if (sender)   strncpy(m->sender,   sender,   MAX_NAME - 1);
    if (receiver) strncpy(m->receiver, receiver, MAX_NAME - 1);
    if (payload)  strncpy(m->payload,  payload,  MAX_PAYLOAD - 1);
    m->length = (uint32_t)(sizeof(MessageEx) - sizeof(uint32_t));
}

/* ── Noise emulation ─────────────────────────────────────────────────────── */
/* Returns 1 if message should be dropped, 0 otherwise.
   Order: delay → drop → corrupt (as per spec section 6.6). */
static int apply_noise(MessageEx *msg) {
    if (sim.delay_ms > 0) {
        printf("[Transport][SIM] DELAY applied: %d ms\n", sim.delay_ms);
        usleep((useconds_t)(sim.delay_ms * 1000));
    }

    if (sim.drop_rate > 0.0f) {
        float r = (float)rand() / (float)RAND_MAX;
        if (r < sim.drop_rate) {
            printf("[Transport][SIM] DROP (id=%u, rate=%.2f)\n",
                   msg->msg_id, sim.drop_rate);
            return 1;
        }
    }

    if (sim.corrupt_rate > 0.0f) {
        float r = (float)rand() / (float)RAND_MAX;
        if (r < sim.corrupt_rate) {
            int len = (int)strlen(msg->payload);
            if (len > 0) {
                int pos = rand() % len;
                msg->payload[pos] ^= 0x55;
                printf("[Transport][SIM] CORRUPT payload (id=%u)\n", msg->msg_id);
            }
        }
    }

    return 0;
}

/* ── TCP/IP logging ──────────────────────────────────────────────────────── */
static void log_recv(int bytes, const char *src_ip, int src_port, uint8_t type) {
    printf("[Network Access] frame received via network interface\n");
    printf("[Internet] src=%s dst=127.0.0.1 proto=TCP\n", src_ip);
    printf("[Transport] recv() %d bytes via TCP src_port=%d dst_port=%d\n",
           bytes, src_port, DEFAULT_PORT);
    printf("[Application] deserialize MessageEx -> %s\n", msg_type_str(type));
}

static void log_send(const char *dst_ip, uint8_t type) {
    printf("[Application] prepare %s\n", msg_type_str(type));
    printf("[Transport] send() via TCP\n");
    printf("[Internet] destination ip = %s\n", dst_ip);
    printf("[Network Access] frame sent to network interface\n");
}

/* ── History ─────────────────────────────────────────────────────────────── */
static void _save_hist_nolock(void) {
    FILE *f = fopen(HISTORY_FILE, "w");
    if (!f) return;
    fprintf(f, "[\n");
    for (int i = 0; i < hist_count; i++) {
        HistEntry *e = &hist[i];
        fprintf(f, "  {\n");
        fprintf(f, "    \"msg_id\": %u,\n",       e->msg_id);
        fprintf(f, "    \"timestamp\": %ld,\n",   (long)e->timestamp);
        fprintf(f, "    \"sender\": \"%s\",\n",   e->sender);
        fprintf(f, "    \"receiver\": \"%s\",\n", e->receiver);
        fprintf(f, "    \"type\": \"%s\",\n",     msg_type_str(e->type));
        fprintf(f, "    \"text\": \"%s\",\n",     e->text);
        fprintf(f, "    \"delivered\": %s,\n",    e->delivered  ? "true" : "false");
        fprintf(f, "    \"is_offline\": %s\n",    e->is_offline ? "true" : "false");
        fprintf(f, "  }%s\n", (i < hist_count - 1) ? "," : "");
    }
    fprintf(f, "]\n");
    fclose(f);
}

static void hist_add(uint32_t id, time_t ts, const char *sender, const char *receiver,
                     uint8_t type, const char *text, int delivered, int is_offline) {
    pthread_mutex_lock(&hist_mutex);
    if (hist_count < MAX_HIST) {
        HistEntry *e  = &hist[hist_count++];
        e->msg_id     = id;
        e->timestamp  = ts;
        e->type       = type;
        e->delivered  = delivered;
        e->is_offline = is_offline;
        strncpy(e->sender,   sender,                MAX_NAME - 1);
        strncpy(e->receiver, receiver ? receiver : "", MAX_NAME - 1);
        strncpy(e->text,     text,                  MAX_PAYLOAD - 1);
        _save_hist_nolock();
    }
    pthread_mutex_unlock(&hist_mutex);
}

static void hist_mark_delivered(uint32_t id) {
    pthread_mutex_lock(&hist_mutex);
    for (int i = 0; i < hist_count; i++) {
        if (hist[i].msg_id == id) {
            hist[i].delivered = 1;
            break;
        }
    }
    _save_hist_nolock();
    pthread_mutex_unlock(&hist_mutex);
}

/* ── Connection queue ────────────────────────────────────────────────────── */
static void enqueue(int fd) {
    pthread_mutex_lock(&queue_mutex);
    conn_queue[q_tail] = fd;
    q_tail = (q_tail + 1) % QUEUE_SIZE;
    q_count++;
    pthread_cond_signal(&queue_cond);
    pthread_mutex_unlock(&queue_mutex);
}

static int dequeue(void) {
    pthread_mutex_lock(&queue_mutex);
    while (q_count == 0)
        pthread_cond_wait(&queue_cond, &queue_mutex);
    int fd = conn_queue[q_head];
    q_head = (q_head + 1) % QUEUE_SIZE;
    q_count--;
    pthread_mutex_unlock(&queue_mutex);
    return fd;
}

/* ── Client list ─────────────────────────────────────────────────────────── */
static int add_client(int fd, const char *ip, int port, const char *nick) {
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++) {
        if (clients[i].authenticated &&
            strncmp(clients[i].nickname, nick, MAX_NAME - 1) == 0) {
            pthread_mutex_unlock(&clients_mutex);
            return -1;
        }
    }
    if (client_count >= MAX_CLIENTS) {
        pthread_mutex_unlock(&clients_mutex);
        return -1;
    }
    Client *c = &clients[client_count++];
    c->fd = fd; c->port = port; c->authenticated = 1;
    strncpy(c->ip,       ip,   INET_ADDRSTRLEN - 1);
    strncpy(c->nickname, nick, MAX_NAME - 1);
    pthread_mutex_unlock(&clients_mutex);
    return 0;
}

static void remove_client(int fd) {
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++) {
        if (clients[i].fd == fd) {
            clients[i] = clients[client_count - 1];
            client_count--;
            break;
        }
    }
    pthread_mutex_unlock(&clients_mutex);
}

/* ── Broadcast & unicast ─────────────────────────────────────────────────── */
static void broadcast_msg(MessageEx *msg, int exclude_fd) {
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++) {
        if (clients[i].authenticated && clients[i].fd != exclude_fd) {
            log_send(clients[i].ip, msg->type);
            send_msgex(clients[i].fd, msg);
        }
    }
    pthread_mutex_unlock(&clients_mutex);
}

static int send_to_nick(const char *nick, MessageEx *msg) {
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++) {
        if (clients[i].authenticated &&
            strncmp(clients[i].nickname, nick, MAX_NAME - 1) == 0) {
            log_send(clients[i].ip, msg->type);
            send_msgex(clients[i].fd, msg);
            pthread_mutex_unlock(&clients_mutex);
            return 0;
        }
    }
    pthread_mutex_unlock(&clients_mutex);
    return -1;
}

/* ── Offline queue ───────────────────────────────────────────────────────── */
static void offline_store(const char *sender, const char *receiver,
                          const char *text, time_t ts, uint32_t id) {
    pthread_mutex_lock(&offline_mutex);
    if (offline_count < MAX_OFFLINE) {
        OfflineMsg *m = &offline_queue[offline_count++];
        m->timestamp = ts;
        m->msg_id    = id;
        strncpy(m->sender,   sender,   MAX_NAME - 1);
        strncpy(m->receiver, receiver, MAX_NAME - 1);
        strncpy(m->text,     text,     MAX_PAYLOAD - 1);
    }
    pthread_mutex_unlock(&offline_mutex);
}

static int offline_deliver(int fd, const char *nick, const char *cip) {
    OfflineMsg pending[MAX_OFFLINE];
    int n_pending = 0;
    OfflineMsg remaining[MAX_OFFLINE];
    int n_remaining = 0;

    pthread_mutex_lock(&offline_mutex);
    for (int i = 0; i < offline_count; i++) {
        if (strncmp(offline_queue[i].receiver, nick, MAX_NAME - 1) == 0)
            pending[n_pending++] = offline_queue[i];
        else
            remaining[n_remaining++] = offline_queue[i];
    }
    memcpy(offline_queue, remaining, (size_t)n_remaining * sizeof(OfflineMsg));
    offline_count = n_remaining;
    pthread_mutex_unlock(&offline_mutex);

    for (int i = 0; i < n_pending; i++) {
        OfflineMsg *m = &pending[i];
        char tbuf[MAX_TIME_STR];
        format_time(m->timestamp, tbuf, sizeof(tbuf));

        char display[MAX_PAYLOAD + 128];
        snprintf(display, sizeof(display),
                 "[%s][id=%u][OFFLINE][%s -> %s]: %s",
                 tbuf, m->msg_id, m->sender, m->receiver, m->text);

        MessageEx msg;
        build_msg(&msg, MSG_PRIVATE, m->msg_id, m->timestamp,
                  m->sender, m->receiver, display);

        log_send(cip, MSG_PRIVATE);
        send_msgex(fd, &msg);
        hist_mark_delivered(m->msg_id);
    }

    return n_pending;
}

/* ── Dedup helpers (per-connection, called within worker) ────────────────── */
static int is_duplicate(uint32_t *seen, int count, uint32_t id) {
    for (int i = 0; i < count; i++) {
        if (seen[i] == id) return 1;
    }
    return 0;
}

static void record_id(uint32_t *seen, int *count, int *pos, uint32_t id) {
    seen[*pos] = id;
    *pos = (*pos + 1) % DEDUP_SIZE;
    if (*count < DEDUP_SIZE) (*count)++;
}

/* ── Worker thread ───────────────────────────────────────────────────────── */
static void *worker(void *arg) {
    (void)arg;

    while (1) {
        int fd = dequeue();

        struct sockaddr_in addr;
        socklen_t alen = sizeof(addr);
        getpeername(fd, (struct sockaddr *)&addr, &alen);
        char cip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &addr.sin_addr, cip, INET_ADDRSTRLEN);
        int cport = ntohs(addr.sin_port);

        printf("Client connected from %s:%d\n\n", cip, cport);

        /* ── Auth ──────────────────────────────────────────────────────── */
        MessageEx msg;
        if (recv_msgex(fd, &msg) < 0) { close(fd); continue; }
        log_recv((int)sizeof(MessageEx), cip, cport, msg.type);

        if (msg.type != MSG_AUTH) {
            MessageEx err;
            build_msg(&err, MSG_ERROR, 0, 0, "SERVER", "", "Send MSG_AUTH first");
            log_send(cip, MSG_ERROR);
            send_msgex(fd, &err);
            close(fd);
            continue;
        }

        char nick[MAX_NAME];
        strncpy(nick, msg.payload, MAX_NAME - 1);
        nick[MAX_NAME - 1] = '\0';
        for (int i = (int)strlen(nick) - 1; i >= 0 && nick[i] == ' '; i--)
            nick[i] = '\0';

        if (strlen(nick) == 0) {
            MessageEx err;
            build_msg(&err, MSG_ERROR, 0, 0, "SERVER", "", "Empty nickname");
            send_msgex(fd, &err);
            close(fd);
            continue;
        }

        if (add_client(fd, cip, cport, nick) < 0) {
            char buf[MAX_NAME + 32];
            snprintf(buf, sizeof(buf), "Nickname [%s] already in use", nick);
            MessageEx err;
            build_msg(&err, MSG_ERROR, 0, 0, "SERVER", "", buf);
            log_send(cip, MSG_ERROR);
            send_msgex(fd, &err);
            close(fd);
            continue;
        }

        printf("[Application] authentication success: %s\n", nick);
        printf("[Application] SYN -> ACK -> READY\n");
        printf("[Application] coffee powered TCP/IP stack initialized\n");
        printf("[Application] packets never sleep\n\n");

        /* Welcome */
        char welcome[MAX_PAYLOAD];
        snprintf(welcome, sizeof(welcome), "Welcome, %s!", nick);
        MessageEx wlc;
        build_msg(&wlc, MSG_WELCOME, 0, 0, "SERVER", nick, welcome);
        log_send(cip, MSG_WELCOME);
        send_msgex(fd, &wlc);

        /* Notify others */
        char join_buf[64];
        snprintf(join_buf, sizeof(join_buf), "User [%s] connected", nick);
        MessageEx jmsg;
        build_msg(&jmsg, MSG_SERVER_INFO, 0, 0, "SERVER", "", join_buf);
        broadcast_msg(&jmsg, fd);

        /* Deliver offline messages */
        int n_offline = offline_deliver(fd, nick, cip);
        if (n_offline == 0) {
            printf("[Application] no offline messages for %s\n", nick);
        } else {
            printf("[SERVER]: message delivered (maybe)\n");
            MessageEx info;
            build_msg(&info, MSG_SERVER_INFO, 0, 0, "SERVER", nick,
                      "message delivered (maybe)");
            log_send(cip, MSG_SERVER_INFO);
            send_msgex(fd, &info);
        }

        printf("User [%s] connected\n\n", nick);

        /* Per-connection dedup state */
        uint32_t seen_ids[DEDUP_SIZE] = {0};
        int      seen_count = 0, seen_pos = 0;

        /* ── Main loop ─────────────────────────────────────────────────── */
        int alive = 1;
        while (alive) {
            if (recv_msgex(fd, &msg) < 0) {
                printf("User [%s] disconnected\n\n", nick);
                alive = 0;
                break;
            }
            log_recv((int)sizeof(MessageEx), cip, cport, msg.type);

            /* Apply noise before app-level processing (section 6.2) */
            if (msg.type == MSG_TEXT || msg.type == MSG_PRIVATE ||
                msg.type == MSG_PING) {
                if (apply_noise(&msg))
                    continue; /* dropped — no further processing, no log */
            }

            switch (msg.type) {

                case MSG_TEXT: {
                    uint32_t client_id = msg.msg_id;

                    if (client_id != 0 &&
                        is_duplicate(seen_ids, seen_count, client_id)) {
                        printf("[Application][DEDUP] duplicate ignored (id=%u)\n\n",
                               client_id);
                        /* ACK so client stops retrying */
                        MessageEx ack;
                        build_msg(&ack, MSG_ACK, client_id, 0, "SERVER", nick, "");
                        printf("[Transport][ACK] send MSG_ACK (id=%u) [dedup]\n\n",
                               client_id);
                        log_send(cip, MSG_ACK);
                        send_msgex(fd, &ack);
                        break;
                    }
                    if (client_id != 0)
                        record_id(seen_ids, &seen_count, &seen_pos, client_id);

                    printf("[Application][ACK] process MSG_TEXT (id=%u)\n", client_id);

                    time_t   ts           = time(NULL);
                    uint32_t broadcast_id = alloc_id();
                    char tbuf[MAX_TIME_STR];
                    format_time(ts, tbuf, sizeof(tbuf));

                    char display[MAX_PAYLOAD + 128];
                    snprintf(display, sizeof(display),
                             "[%s][id=%u][%s]: %s", tbuf, broadcast_id, nick,
                             msg.payload);

                    MessageEx out;
                    build_msg(&out, MSG_TEXT, broadcast_id, ts, nick, "", display);
                    printf("%s\n\n", display);
                    broadcast_msg(&out, -1);
                    hist_add(broadcast_id, ts, nick, "", MSG_TEXT, msg.payload, 1, 0);

                    /* ACK with client's original id so client can match it */
                    MessageEx ack;
                    build_msg(&ack, MSG_ACK, client_id, 0, "SERVER", nick, "");
                    printf("[Transport][ACK] send MSG_ACK (id=%u)\n\n", client_id);
                    log_send(cip, MSG_ACK);
                    send_msgex(fd, &ack);
                    break;
                }

                case MSG_PRIVATE: {
                    uint32_t client_id = msg.msg_id;

                    if (client_id != 0 &&
                        is_duplicate(seen_ids, seen_count, client_id)) {
                        printf("[Application][DEDUP] duplicate ignored (id=%u)\n\n",
                               client_id);
                        MessageEx ack;
                        build_msg(&ack, MSG_ACK, client_id, 0, "SERVER", nick, "");
                        printf("[Transport][ACK] send MSG_ACK (id=%u) [dedup]\n\n",
                               client_id);
                        log_send(cip, MSG_ACK);
                        send_msgex(fd, &ack);
                        break;
                    }
                    if (client_id != 0)
                        record_id(seen_ids, &seen_count, &seen_pos, client_id);

                    printf("[Application] handle MSG_PRIVATE\n");
                    printf("[Application][ACK] process MSG_PRIVATE (id=%u)\n",
                           client_id);

                    time_t   ts  = time(NULL);
                    uint32_t id  = alloc_id();
                    char tbuf[MAX_TIME_STR];
                    format_time(ts, tbuf, sizeof(tbuf));

                    char target[MAX_NAME];
                    strncpy(target, msg.receiver, MAX_NAME - 1);
                    target[MAX_NAME - 1] = '\0';

                    /* fallback: parse "nick:text" from payload */
                    if (strlen(target) == 0) {
                        char *colon = strchr(msg.payload, ':');
                        if (colon) {
                            size_t tlen = (size_t)(colon - msg.payload);
                            if (tlen < MAX_NAME - 1) {
                                strncpy(target, msg.payload, tlen);
                                target[tlen] = '\0';
                                memmove(msg.payload, colon + 1,
                                        strlen(colon + 1) + 1);
                            }
                        }
                    }

                    char display[MAX_PAYLOAD + 128];
                    snprintf(display, sizeof(display),
                             "[%s][id=%u][PRIVATE][%s -> %s]: %s",
                             tbuf, id, nick, target, msg.payload);

                    MessageEx out;
                    build_msg(&out, MSG_PRIVATE, id, ts, nick, target, display);

                    if (send_to_nick(target, &out) < 0) {
                        printf("[Application] receiver %s is offline\n", target);
                        printf("[Application] store message in offline queue\n");
                        printf("[Application] if it works - don't touch it\n");
                        offline_store(nick, target, msg.payload, ts, id);
                        hist_add(id, ts, nick, target, MSG_PRIVATE,
                                 msg.payload, 0, 1);
                        printf("[Application] append record to history file "
                               "delivered=false\n\n");

                        MessageEx info;
                        build_msg(&info, MSG_SERVER_INFO, 0, 0, "SERVER", nick,
                                  "Message stored (recipient offline)");
                        log_send(cip, MSG_SERVER_INFO);
                        send_msgex(fd, &info);
                    } else {
                        hist_add(id, ts, nick, target, MSG_PRIVATE,
                                 msg.payload, 1, 0);
                    }

                    MessageEx ack;
                    build_msg(&ack, MSG_ACK, client_id, 0, "SERVER", nick, "");
                    printf("[Transport][ACK] send MSG_ACK (id=%u)\n\n", client_id);
                    log_send(cip, MSG_ACK);
                    send_msgex(fd, &ack);
                    break;
                }

                case MSG_PING: {
                    uint32_t ping_id = msg.msg_id;
                    printf("[Transport][PING] recv MSG_PING (id=%u)\n", ping_id);

                    MessageEx pong;
                    build_msg(&pong, MSG_PONG, ping_id, 0, "SERVER", nick, "");
                    printf("[Transport][PING] send MSG_PONG (id=%u)\n", ping_id);
                    log_send(cip, MSG_PONG);
                    send_msgex(fd, &pong);

                    printf("[Application] client=%s\n\n", nick);
                    break;
                }

                case MSG_LIST: {
                    printf("[Application] handle MSG_LIST\n");
                    char buf[MAX_PAYLOAD];
                    int pos = snprintf(buf, sizeof(buf), "Online users:\n");
                    pthread_mutex_lock(&clients_mutex);
                    for (int i = 0;
                         i < client_count && pos < (int)sizeof(buf) - 2; i++) {
                        if (clients[i].authenticated)
                            pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos,
                                            "%s\n", clients[i].nickname);
                    }
                    pthread_mutex_unlock(&clients_mutex);

                    MessageEx resp;
                    build_msg(&resp, MSG_SERVER_INFO, 0, 0, "SERVER", nick, buf);
                    log_send(cip, MSG_SERVER_INFO);
                    send_msgex(fd, &resp);
                    printf("\n");
                    break;
                }

                case MSG_HISTORY: {
                    printf("[Application] handle MSG_HISTORY\n");
                    int n = DEFAULT_HISTORY;
                    if (strlen(msg.payload) > 0) {
                        int parsed = atoi(msg.payload);
                        if (parsed > 0) n = parsed;
                        if (n > 100) n = 100;
                    }

                    HistEntry snap[100];
                    int snap_count = 0;

                    pthread_mutex_lock(&hist_mutex);
                    int start = hist_count - n;
                    if (start < 0) start = 0;
                    snap_count = hist_count - start;
                    if (snap_count > 100) snap_count = 100;
                    memcpy(snap, hist + start,
                           (size_t)snap_count * sizeof(HistEntry));
                    pthread_mutex_unlock(&hist_mutex);

                    for (int i = 0; i < snap_count; i++) {
                        HistEntry *e = &snap[i];
                        char tbuf[MAX_TIME_STR];
                        format_time(e->timestamp, tbuf, sizeof(tbuf));

                        char line[MAX_PAYLOAD + 128];
                        if (e->is_offline)
                            snprintf(line, sizeof(line),
                                     "[%s][id=%u][%s -> %s][OFFLINE]: %s",
                                     tbuf, e->msg_id, e->sender, e->receiver,
                                     e->text);
                        else if (e->type == MSG_PRIVATE)
                            snprintf(line, sizeof(line),
                                     "[%s][id=%u][%s -> %s][PRIVATE]: %s",
                                     tbuf, e->msg_id, e->sender, e->receiver,
                                     e->text);
                        else
                            snprintf(line, sizeof(line),
                                     "[%s][id=%u][%s]: %s",
                                     tbuf, e->msg_id, e->sender, e->text);

                        MessageEx hresp;
                        build_msg(&hresp, MSG_HISTORY_DATA, e->msg_id,
                                  e->timestamp, e->sender, e->receiver, line);
                        log_send(cip, MSG_HISTORY_DATA);
                        send_msgex(fd, &hresp);
                    }

                    MessageEx log_msg;
                    build_msg(&log_msg, MSG_HISTORY_DATA, 0, 0, "SERVER", nick,
                              "[LOG]: i love TCP/IP (don't tell UDP)");
                    send_msgex(fd, &log_msg);

                    printf("[Application] sent history (%d entries)\n\n",
                           snap_count);
                    break;
                }

                case MSG_BYE:
                    printf("[Application] handle MSG_BYE\n");
                    printf("User [%s] disconnected\n\n", nick);
                    alive = 0;
                    break;

                default:
                    printf("[Application] unknown message type: %d\n\n",
                           msg.type);
                    break;
            }
        }

        remove_client(fd);
        close(fd);

        char leave[64];
        snprintf(leave, sizeof(leave), "User [%s] disconnected", nick);
        MessageEx lmsg;
        build_msg(&lmsg, MSG_SERVER_INFO, 0, 0, "SERVER", "", leave);
        broadcast_msg(&lmsg, -1);
        printf("\n");
    }

    return NULL;
}

/* ── CLI argument parsing ────────────────────────────────────────────────── */
static void parse_args(int argc, char *argv[]) {
    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--delay=", 8) == 0) {
            sim.delay_ms = atoi(argv[i] + 8);
        } else if (strncmp(argv[i], "--drop=", 7) == 0) {
            sim.drop_rate = (float)atof(argv[i] + 7);
        } else if (strncmp(argv[i], "--corrupt=", 10) == 0) {
            sim.corrupt_rate = (float)atof(argv[i] + 10);
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            fprintf(stderr, "Usage: server [--delay=MS] [--drop=RATE] "
                            "[--corrupt=RATE]\n");
        }
    }
}

/* ── main ────────────────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    srand((unsigned int)time(NULL));
    parse_args(argc, argv);

    printf("Server started on port %d (thread pool: %d)\n",
           DEFAULT_PORT, THREAD_POOL_SIZE);

    if (sim.delay_ms > 0 || sim.drop_rate > 0.0f || sim.corrupt_rate > 0.0f) {
        printf("[Transport][SIM] Noise enabled: delay=%d ms  drop=%.2f  "
               "corrupt=%.2f\n",
               sim.delay_ms, sim.drop_rate, sim.corrupt_rate);
    }
    printf("\n");

    int sfd;
    struct sockaddr_in saddr;

    sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    int opt = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&saddr, 0, sizeof(saddr));
    saddr.sin_family      = AF_INET;
    saddr.sin_addr.s_addr = INADDR_ANY;
    saddr.sin_port        = htons(DEFAULT_PORT);

    if (bind(sfd, (struct sockaddr *)&saddr, sizeof(saddr)) < 0) {
        perror("bind"); close(sfd); exit(EXIT_FAILURE);
    }
    if (listen(sfd, THREAD_POOL_SIZE * 2) < 0) {
        perror("listen"); close(sfd); exit(EXIT_FAILURE);
    }

    pthread_t threads[THREAD_POOL_SIZE];
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        pthread_create(&threads[i], NULL, worker, NULL);
        pthread_detach(threads[i]);
    }

    struct sockaddr_in caddr;
    socklen_t clen = sizeof(caddr);
    while (1) {
        int cfd = accept(sfd, (struct sockaddr *)&caddr, &clen);
        if (cfd < 0) { perror("accept"); continue; }
        enqueue(cfd);
    }

    close(sfd);
    return 0;
}
