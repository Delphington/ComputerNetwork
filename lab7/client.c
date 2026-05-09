#include "common.h"
#include <pthread.h>
#include <signal.h>
#include <math.h>
#include <errno.h>

#define MAX_PENDING      64
#define RETRY_TIMEOUT_S  2
#define MAX_RETRIES      3
#define MAX_PING_RESULTS 256
#define PING_TIMEOUT_S   2

static volatile int    running  = 1;
static int             sockfd   = -1;
static SSL            *ssl_conn = NULL;
static char            nickname[MAX_NAME];
static pthread_mutex_t ssl_mtx  = PTHREAD_MUTEX_INITIALIZER;

/* ── Client-side msg_id generator ───────────────────────────────────────── */
static uint32_t        client_next_id = 1;
static pthread_mutex_t id_mtx         = PTHREAD_MUTEX_INITIALIZER;

static uint32_t next_client_id(void) {
    pthread_mutex_lock(&id_mtx);
    uint32_t id = client_next_id++;
    pthread_mutex_unlock(&id_mtx);
    return id;
}

/* ── Pending ACK queue ───────────────────────────────────────────────────── */
typedef struct {
    MessageEx msg;
    time_t    send_time;
    int       retries;
} PendingMsg;

static PendingMsg      pending[MAX_PENDING];
static int             pending_count = 0;
static pthread_mutex_t pending_mtx   = PTHREAD_MUTEX_INITIALIZER;

static void pending_add(const MessageEx *msg) {
    pthread_mutex_lock(&pending_mtx);
    if (pending_count < MAX_PENDING) {
        pending[pending_count].msg       = *msg;
        pending[pending_count].send_time = time(NULL);
        pending[pending_count].retries   = 0;
        pending_count++;
        printf("[Transport][RETRY] wait ACK (id=%u)\n",
               msg->msg_id);
        fflush(stdout);
    }
    pthread_mutex_unlock(&pending_mtx);
}

static void pending_remove(uint32_t id) {
    pthread_mutex_lock(&pending_mtx);
    for (int i = 0; i < pending_count; i++) {
        if (pending[i].msg.msg_id == id) {
            printf("\n[Transport][ACK] ACK received (id=%u)\n> ", id);
            fflush(stdout);
            pending[i] = pending[--pending_count];
            break;
        }
    }
    pthread_mutex_unlock(&pending_mtx);
}

/* ── Ping / PONG signaling ───────────────────────────────────────────────── */
static uint32_t        expect_pong_id = 0;
static int             pong_arrived   = 0;
static pthread_mutex_t pong_mtx       = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  pong_cv        = PTHREAD_COND_INITIALIZER;

static volatile int in_ping_mode = 0;

/* ── Ping statistics ─────────────────────────────────────────────────────── */
typedef struct {
    double rtt_ms;
    int    timed_out;
} PingResult;

static PingResult      ping_results[MAX_PING_RESULTS];
static int             ping_result_count = 0;
static int             ping_total        = 0;
static pthread_mutex_t stats_mtx         = PTHREAD_MUTEX_INITIALIZER;

/* ── Receive thread ──────────────────────────────────────────────────────── */
static void *recv_thread(void *arg) {
    (void)arg;
    MessageEx msg;

    while (running) {
        if (ssl_recv_msgex(ssl_conn, &msg) < 0) {
            if (running)
                printf("\nConnection closed by server\n");
            running = 0;
            break;
        }

        switch (msg.type) {
            case MSG_WELCOME:
                printf("\n%s\n> ", msg.payload);
                fflush(stdout);
                break;

            case MSG_TEXT:
                printf("\n%s\n[CLIENT]: hmm... TLS feels stable today\n> ",
                       msg.payload);
                fflush(stdout);
                break;

            case MSG_PRIVATE:
                printf("\n%s\n> ", msg.payload);
                fflush(stdout);
                break;

            case MSG_SERVER_INFO:
                printf("\n[SERVER]: %s\n> ", msg.payload);
                fflush(stdout);
                break;

            case MSG_ERROR:
                printf("\n[ERROR]: %s\n> ", msg.payload);
                fflush(stdout);
                break;

            case MSG_PONG: {
                pthread_mutex_lock(&pong_mtx);
                if (msg.msg_id == expect_pong_id) {
                    pong_arrived = 1;
                    pthread_cond_signal(&pong_cv);
                }
                pthread_mutex_unlock(&pong_mtx);

                if (!in_ping_mode) {
                    printf("\n[SERVER]: PONG\n"
                           "[LOG]: i love cast (no segmentation faults pls)\n> ");
                    fflush(stdout);
                }
                break;
            }

            case MSG_ACK:
                printf("[Security][ENC] SSL_read MSG_ACK (id=%u)\n", msg.msg_id);
                fflush(stdout);
                pending_remove(msg.msg_id);
                break;

            case MSG_HISTORY_DATA:
                if (strncmp(msg.payload, "[LOG]:", 6) == 0)
                    printf("\n%s\n> ", msg.payload);
                else
                    printf("\n%s", msg.payload);
                fflush(stdout);
                break;

            case MSG_BYE:
                printf("\nServer closed connection\n");
                running = 0;
                break;

            default:
                break;
        }
    }

    return NULL;
}

/* ── Retry thread ────────────────────────────────────────────────────────── */
static void *retry_thread(void *arg) {
    (void)arg;

    while (running) {
        sleep(1);
        if (!running) break;

        time_t now = time(NULL);

        MessageEx to_resend[MAX_PENDING];
        int        n_resend = 0;

        pthread_mutex_lock(&pending_mtx);
        for (int i = 0; i < pending_count; ) {
            PendingMsg *p = &pending[i];
            if (now - p->send_time < RETRY_TIMEOUT_S) { i++; continue; }

            p->retries++;

            if (p->retries > MAX_RETRIES) {
                printf("\n[Transport][RETRY] wait ACK timeout — "
                       "message undelivered (id=%u)\n> ",
                       p->msg.msg_id);
                fflush(stdout);
                pending[i] = pending[--pending_count];
            } else {
                printf("\n[Transport][RETRY] resend %d/%d (id=%u)\n> ",
                       p->retries, MAX_RETRIES, p->msg.msg_id);
                fflush(stdout);
                p->msg.timestamp = now;
                p->send_time     = now;
                to_resend[n_resend++] = p->msg;
                i++;
            }
        }
        pthread_mutex_unlock(&pending_mtx);

        if (n_resend > 0) {
            pthread_mutex_lock(&ssl_mtx);
            for (int i = 0; i < n_resend; i++) {
                if (ssl_conn)
                    ssl_send_msgex(ssl_conn, &to_resend[i]);
            }
            pthread_mutex_unlock(&ssl_mtx);
        }
    }

    return NULL;
}

/* ── /ping N ─────────────────────────────────────────────────────────────── */
static void do_ping(int n) {
    if (n <= 0) n = 10;
    if (n > MAX_PING_RESULTS) n = MAX_PING_RESULTS;

    pthread_mutex_lock(&stats_mtx);
    ping_result_count = 0;
    ping_total        = n;
    pthread_mutex_unlock(&stats_mtx);

    in_ping_mode = 1;
    double prev_rtt = -1.0;

    for (int i = 0; i < n; i++) {
        uint32_t id = next_client_id();

        MessageEx msg;
        memset(&msg, 0, sizeof(msg));
        msg.type      = MSG_PING;
        msg.msg_id    = id;
        msg.timestamp = time(NULL);
        msg.length    = (uint32_t)(sizeof(MessageEx) - sizeof(uint32_t));
        strncpy(msg.sender, nickname, MAX_NAME - 1);

        pthread_mutex_lock(&pong_mtx);
        expect_pong_id = id;
        pong_arrived   = 0;
        pthread_mutex_unlock(&pong_mtx);

        struct timespec t_send;
        clock_gettime(CLOCK_MONOTONIC, &t_send);

        pthread_mutex_lock(&ssl_mtx);
        if (ssl_conn) ssl_send_msgex(ssl_conn, &msg);
        pthread_mutex_unlock(&ssl_mtx);

        printf("[Transport][PING] send MSG_PING (id=%u)\n", id);
        fflush(stdout);

        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += PING_TIMEOUT_S;

        pthread_mutex_lock(&pong_mtx);
        int timed_out = 0;
        while (!pong_arrived) {
            int rc = pthread_cond_timedwait(&pong_cv, &pong_mtx, &deadline);
            if (rc == ETIMEDOUT) { timed_out = 1; break; }
        }
        pthread_mutex_unlock(&pong_mtx);

        if (timed_out) {
            printf("PING %d -> timeout\n", i + 1);
            fflush(stdout);
            prev_rtt = -1.0;

            pthread_mutex_lock(&stats_mtx);
            ping_results[ping_result_count].rtt_ms    = 0.0;
            ping_results[ping_result_count].timed_out = 1;
            ping_result_count++;
            pthread_mutex_unlock(&stats_mtx);
        } else {
            struct timespec t_recv;
            clock_gettime(CLOCK_MONOTONIC, &t_recv);
            double rtt = (t_recv.tv_sec - t_send.tv_sec) * 1000.0
                       + (t_recv.tv_nsec - t_send.tv_nsec) / 1e6;

            if (prev_rtt >= 0.0) {
                double jitter = fabs(rtt - prev_rtt);
                printf("PING %d -> RTT=%.1fms | Jitter=%.1fms\n",
                       i + 1, rtt, jitter);
            } else {
                printf("PING %d -> RTT=%.1fms\n", i + 1, rtt);
            }
            fflush(stdout);
            prev_rtt = rtt;

            pthread_mutex_lock(&stats_mtx);
            ping_results[ping_result_count].rtt_ms    = rtt;
            ping_results[ping_result_count].timed_out = 0;
            ping_result_count++;
            pthread_mutex_unlock(&stats_mtx);
        }
    }

    pthread_mutex_lock(&pong_mtx);
    expect_pong_id = 0;
    pthread_mutex_unlock(&pong_mtx);

    in_ping_mode = 0;
}

/* ── /netdiag ────────────────────────────────────────────────────────────── */
static void do_netdiag(void) {
    pthread_mutex_lock(&stats_mtx);
    int total     = ping_result_count;
    int total_all = ping_total;
    pthread_mutex_unlock(&stats_mtx);

    if (total == 0) {
        printf("No ping data. Run /ping first.\n");
        return;
    }

    double sum_rtt    = 0.0;
    double prev_rtt   = -1.0;
    double sum_jitter = 0.0;
    int    jitter_cnt = 0;
    int    lost       = 0;
    int    received   = 0;

    pthread_mutex_lock(&stats_mtx);
    for (int i = 0; i < total; i++) {
        if (ping_results[i].timed_out) {
            lost++;
            prev_rtt = -1.0;
        } else {
            double rtt = ping_results[i].rtt_ms;
            sum_rtt += rtt;
            received++;
            if (prev_rtt >= 0.0) {
                sum_jitter += fabs(rtt - prev_rtt);
                jitter_cnt++;
            }
            prev_rtt = rtt;
        }
    }
    pthread_mutex_unlock(&stats_mtx);

    double avg_rtt    = (received   > 0) ? sum_rtt    / received   : 0.0;
    double avg_jitter = (jitter_cnt > 0) ? sum_jitter / jitter_cnt : 0.0;
    double loss_pct   = (total_all  > 0) ? (double)lost / total_all * 100.0
                                          : 0.0;

    printf("\nRTT avg : %.1f ms\n", avg_rtt);
    printf("Jitter  : %.1f ms\n",   avg_jitter);
    printf("Loss    : %.1f%%\n",    loss_pct);

    char fname[64];
    snprintf(fname, sizeof(fname), "net_diag_%s.json", nickname);
    FILE *f = fopen(fname, "w");
    if (f) {
        fprintf(f, "{\n");
        fprintf(f, "  \"nickname\": \"%s\",\n",       nickname);
        fprintf(f, "  \"ping_sent\": %d,\n",          total_all);
        fprintf(f, "  \"ping_received\": %d,\n",      received);
        fprintf(f, "  \"ping_lost\": %d,\n",          lost);
        fprintf(f, "  \"rtt_avg_ms\": %.2f,\n",       avg_rtt);
        fprintf(f, "  \"jitter_avg_ms\": %.2f,\n",    avg_jitter);
        fprintf(f, "  \"loss_pct\": %.2f\n",          loss_pct);
        fprintf(f, "}\n");
        fclose(f);
        printf("Saved to %s\n", fname);
    }
}

/* ── SSL-protected send with ACK/retry tracking ──────────────────────────── */
static void send_reliable(MessageEx *msg) {
    printf("[Security][ENC] SSL_write %s (id=%u)\n",
           msg_type_str(msg->type), msg->msg_id);
    fflush(stdout);

    pthread_mutex_lock(&ssl_mtx);
    if (ssl_conn) ssl_send_msgex(ssl_conn, msg);
    pthread_mutex_unlock(&ssl_mtx);

    pending_add(msg);
}

/* ── main ────────────────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);

    const char *server_ip   = (argc > 1) ? argv[1] : "127.0.0.1";
    int         server_port = (argc > 2) ? atoi(argv[2]) : DEFAULT_PORT;

    /* OpenSSL init */
    SSL_library_init();
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();

    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
    if (!ctx) {
        ERR_print_errors_fp(stderr);
        exit(EXIT_FAILURE);
    }
    printf("[Security][TLS] SSL context created\n");

    /* Accept self-signed certificates */
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);

    /* TCP connect */
    struct sockaddr_in saddr;
    memset(&saddr, 0, sizeof(saddr));
    saddr.sin_family = AF_INET;
    saddr.sin_port   = htons((uint16_t)server_port);
    if (inet_pton(AF_INET, server_ip, &saddr.sin_addr) <= 0) {
        perror("inet_pton"); exit(EXIT_FAILURE);
    }

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    if (connect(sockfd, (struct sockaddr *)&saddr, sizeof(saddr)) < 0) {
        perror("connect"); close(sockfd); exit(EXIT_FAILURE);
    }
    printf("[Transport] connected to %s:%d\n", server_ip, server_port);

    /* Create SSL object and attach to socket */
    ssl_conn = SSL_new(ctx);
    if (!ssl_conn) {
        ERR_print_errors_fp(stderr);
        close(sockfd); exit(EXIT_FAILURE);
    }
    SSL_set_fd(ssl_conn, sockfd);

    /* TLS handshake */
    printf("[Security][TLS] handshake started\n");
    if (SSL_connect(ssl_conn) <= 0) {
        fprintf(stderr, "[Security][TLS] handshake failed\n");
        ERR_print_errors_fp(stderr);
        SSL_free(ssl_conn);
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    printf("[Security][TLS] handshake success\n");
    printf("[Security][TLS] connected securely\n");

    /* Check server certificate */
    X509 *cert = SSL_get_peer_certificate(ssl_conn);
    if (cert) {
        printf("[Security][CERT] server certificate received\n");
        char subject[256];
        X509_NAME_oneline(X509_get_subject_name(cert), subject, sizeof(subject));
        printf("[Security][CERT] subject: %s\n", subject);
        X509_free(cert);
    } else {
        printf("[Security][CERT] no certificate\n");
    }

    printf("[Security][ENC] encrypted channel established\n\n");

    /* Nickname */
    printf("Enter nickname: ");
    fflush(stdout);
    fgets(nickname, sizeof(nickname), stdin);
    nickname[strcspn(nickname, "\n")] = '\0';
    if (strlen(nickname) == 0) {
        fprintf(stderr, "Nickname cannot be empty\n");
        SSL_shutdown(ssl_conn); SSL_free(ssl_conn);
        close(sockfd); exit(EXIT_FAILURE);
    }

    /* Send MSG_AUTH */
    MessageEx auth;
    memset(&auth, 0, sizeof(auth));
    auth.type      = MSG_AUTH;
    auth.msg_id    = next_client_id();
    auth.timestamp = time(NULL);
    auth.length    = (uint32_t)(sizeof(MessageEx) - sizeof(uint32_t));
    strncpy(auth.sender,  nickname, MAX_NAME - 1);
    strncpy(auth.payload, nickname, MAX_PAYLOAD - 1);
    ssl_send_msgex(ssl_conn, &auth);

    /* Wait for WELCOME or ERROR */
    MessageEx resp;
    if (ssl_recv_msgex(ssl_conn, &resp) < 0) {
        fprintf(stderr, "Connection lost\n");
        SSL_shutdown(ssl_conn); SSL_free(ssl_conn);
        close(sockfd); exit(EXIT_FAILURE);
    }
    if (resp.type == MSG_ERROR) {
        printf("[ERROR]: %s\n", resp.payload);
        SSL_shutdown(ssl_conn); SSL_free(ssl_conn);
        close(sockfd); exit(EXIT_FAILURE);
    }
    if (resp.type == MSG_WELCOME)
        printf("%s\n", resp.payload);

    /* Start threads */
    pthread_t rtid, retry_tid;
    pthread_create(&rtid,      NULL, recv_thread,  NULL);
    pthread_create(&retry_tid, NULL, retry_thread, NULL);
    pthread_detach(retry_tid);

    printf("\n> ");
    fflush(stdout);

    char input[MAX_PAYLOAD];
    while (running && fgets(input, sizeof(input), stdin) != NULL) {
        input[strcspn(input, "\n")] = '\0';
        if (!running) break;

        if (strcmp(input, "/quit") == 0) {
            MessageEx msg;
            memset(&msg, 0, sizeof(msg));
            msg.type      = MSG_BYE;
            msg.msg_id    = next_client_id();
            msg.timestamp = time(NULL);
            msg.length    = (uint32_t)(sizeof(MessageEx) - sizeof(uint32_t));
            strncpy(msg.sender, nickname, MAX_NAME - 1);
            pthread_mutex_lock(&ssl_mtx);
            if (ssl_conn) ssl_send_msgex(ssl_conn, &msg);
            pthread_mutex_unlock(&ssl_mtx);
            running = 0;

        } else if (strncmp(input, "/ping", 5) == 0) {
            int n = 10;
            const char *rest = input + 5;
            while (*rest == ' ') rest++;
            if (*rest != '\0') {
                int parsed = atoi(rest);
                if (parsed > 0) n = parsed;
            }
            do_ping(n);
            printf("> ");
            fflush(stdout);

        } else if (strcmp(input, "/netdiag") == 0) {
            do_netdiag();
            printf("> ");
            fflush(stdout);

        } else if (strcmp(input, "/help") == 0) {
            printf("Available commands:\n");
            printf("  /help               - show this help\n");
            printf("  /list               - list online users\n");
            printf("  /history            - show last 20 messages\n");
            printf("  /history N          - show last N messages\n");
            printf("  /w <nick> <message> - private message\n");
            printf("  /ping               - send 10 pings, measure RTT\n");
            printf("  /ping N             - send N pings\n");
            printf("  /netdiag            - show network statistics\n");
            printf("  /quit               - disconnect\n");
            printf("Tip: packets never sleep (but now they're encrypted)\n> ");
            fflush(stdout);

        } else if (strcmp(input, "/list") == 0) {
            MessageEx msg;
            memset(&msg, 0, sizeof(msg));
            msg.type      = MSG_LIST;
            msg.msg_id    = next_client_id();
            msg.timestamp = time(NULL);
            msg.length    = (uint32_t)(sizeof(MessageEx) - sizeof(uint32_t));
            strncpy(msg.sender, nickname, MAX_NAME - 1);
            pthread_mutex_lock(&ssl_mtx);
            if (ssl_conn) ssl_send_msgex(ssl_conn, &msg);
            pthread_mutex_unlock(&ssl_mtx);
            printf("> ");
            fflush(stdout);

        } else if (strncmp(input, "/history", 8) == 0) {
            MessageEx msg;
            memset(&msg, 0, sizeof(msg));
            msg.type      = MSG_HISTORY;
            msg.msg_id    = next_client_id();
            msg.timestamp = time(NULL);
            msg.length    = (uint32_t)(sizeof(MessageEx) - sizeof(uint32_t));
            strncpy(msg.sender, nickname, MAX_NAME - 1);

            const char *rest = input + 8;
            while (*rest == ' ') rest++;
            if (strlen(rest) > 0) {
                if (atoi(rest) <= 0) {
                    printf("[ERROR]: /history N requires a positive integer\n> ");
                    fflush(stdout);
                    continue;
                }
                strncpy(msg.payload, rest, MAX_PAYLOAD - 1);
            }
            pthread_mutex_lock(&ssl_mtx);
            if (ssl_conn) ssl_send_msgex(ssl_conn, &msg);
            pthread_mutex_unlock(&ssl_mtx);
            printf("> ");
            fflush(stdout);

        } else if (strncmp(input, "/w ", 3) == 0) {
            char *rest  = input + 3;
            char *space = strchr(rest, ' ');
            if (!space) {
                printf("Usage: /w <nick> <message>\n> ");
                fflush(stdout);
            } else {
                *space = '\0';
                char *target = rest;
                char *text   = space + 1;

                MessageEx msg;
                memset(&msg, 0, sizeof(msg));
                msg.type      = MSG_PRIVATE;
                msg.msg_id    = next_client_id();
                msg.timestamp = time(NULL);
                msg.length    = (uint32_t)(sizeof(MessageEx) - sizeof(uint32_t));
                strncpy(msg.sender,   nickname, MAX_NAME - 1);
                strncpy(msg.receiver, target,   MAX_NAME - 1);
                strncpy(msg.payload,  text,     MAX_PAYLOAD - 1);
                send_reliable(&msg);
                printf("> ");
                fflush(stdout);
            }

        } else if (strlen(input) > 0) {
            MessageEx msg;
            memset(&msg, 0, sizeof(msg));
            msg.type      = MSG_TEXT;
            msg.msg_id    = next_client_id();
            msg.timestamp = time(NULL);
            msg.length    = (uint32_t)(sizeof(MessageEx) - sizeof(uint32_t));
            strncpy(msg.sender,  nickname, MAX_NAME - 1);
            strncpy(msg.payload, input,    MAX_PAYLOAD - 1);
            send_reliable(&msg);
            printf("> ");
            fflush(stdout);

        } else {
            printf("> ");
            fflush(stdout);
        }
    }

    running = 0;

    /* Shutdown SSL, then close socket to unblock recv_thread */
    pthread_mutex_lock(&ssl_mtx);
    if (ssl_conn) {
        SSL_shutdown(ssl_conn);
        SSL_free(ssl_conn);
        ssl_conn = NULL;
    }
    pthread_mutex_unlock(&ssl_mtx);

    if (sockfd >= 0) { close(sockfd); sockfd = -1; }

    pthread_join(rtid, NULL);

    SSL_CTX_free(ctx);
    printf("Disconnected\n");
    return 0;
}
