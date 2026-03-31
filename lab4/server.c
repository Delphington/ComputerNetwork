#include "common.h"
#include <pthread.h>

#define THREAD_POOL_SIZE 10
#define MAX_CLIENTS      100
#define QUEUE_SIZE       128

/* ── Структура клиента ─────────────────────────────────────────────────── */
typedef struct {
    int  fd;
    char nickname[32];
    char ip[INET_ADDRSTRLEN];
    int  port;
    int  authenticated;
} Client;

/* ── Глобальный список клиентов ────────────────────────────────────────── */
static Client          clients[MAX_CLIENTS];
static int             client_count = 0;
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ── Очередь входящих соединений ───────────────────────────────────────── */
static int             conn_queue[QUEUE_SIZE];
static int             q_head = 0, q_tail = 0, q_count = 0;
static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  queue_cond  = PTHREAD_COND_INITIALIZER;

/* ── Визуализация модели OSI ───────────────────────────────────────────── */
static const char *layer_name(int layer) {
    switch (layer) {
        case 1: return "Physical";
        case 2: return "Data Link";
        case 3: return "Network";
        case 4: return "Transport";
        case 5: return "Session";
        case 6: return "Presentation";
        case 7: return "Application";
        default: return "Unknown";
    }
}

static void log_osi(int layer, const char *msg) {
    printf("[Layer %d - %s] %s\n", layer, layer_name(layer), msg);
}

/* ── Очередь: операции ─────────────────────────────────────────────────── */
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

/* ── Управление списком клиентов ───────────────────────────────────────── */

/* Добавляет клиента; возвращает 0 при успехе, -1 если ник занят/список полон */
static int add_client(int fd, const char *ip, int port, const char *nick) {
    pthread_mutex_lock(&clients_mutex);
    /* проверяем уникальность никнейма */
    for (int i = 0; i < client_count; i++) {
        if (clients[i].authenticated &&
            strncmp(clients[i].nickname, nick, 31) == 0) {
            pthread_mutex_unlock(&clients_mutex);
            return -1; /* ник занят */
        }
    }
    if (client_count >= MAX_CLIENTS) {
        pthread_mutex_unlock(&clients_mutex);
        return -1; /* список полон */
    }
    clients[client_count].fd            = fd;
    clients[client_count].port          = port;
    clients[client_count].authenticated = 1;
    strncpy(clients[client_count].ip,       ip,   INET_ADDRSTRLEN - 1);
    strncpy(clients[client_count].nickname, nick, 31);
    client_count++;
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

/* ── Отправка одному клиенту по никнейму ───────────────────────────────── */
static int send_to_nick(const char *nick, uint8_t type, const char *data) {
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++) {
        if (clients[i].authenticated &&
            strncmp(clients[i].nickname, nick, 31) == 0) {
            log_osi(7, "prepare private message");
            log_osi(6, "serialize Message");
            log_osi(4, "send()");
            send_message(clients[i].fd, type, data);
            pthread_mutex_unlock(&clients_mutex);
            return 0;
        }
    }
    pthread_mutex_unlock(&clients_mutex);
    return -1; /* получатель не найден */
}

/* ── Широковещательная рассылка ────────────────────────────────────────── */
static void broadcast(uint8_t type, const char *data, int exclude_fd) {
    log_osi(7, "prepare broadcast");
    log_osi(6, "serialize Message");
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++) {
        if (clients[i].authenticated && clients[i].fd != exclude_fd) {
            log_osi(4, "send()");
            send_message(clients[i].fd, type, data);
        }
    }
    pthread_mutex_unlock(&clients_mutex);
}

/* ── Рабочий поток ─────────────────────────────────────────────────────── */
static void *worker_thread(void *arg) {
    (void)arg;

    while (1) {
        int client_fd = dequeue();

        /* Получаем адрес клиента */
        struct sockaddr_in addr;
        socklen_t addrlen = sizeof(addr);
        getpeername(client_fd, (struct sockaddr *)&addr, &addrlen);
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        int client_port = ntohs(addr.sin_port);

        printf("Client connected from %s:%d\n\n", client_ip, client_port);

        /* ── Аутентификация (Layer 5) ─────────────────────────────────── */
        Message msg;

        log_osi(4, "recv()");
        if (receive_message(client_fd, &msg) < 0) {
            close(client_fd);
            continue;
        }
        log_osi(6, "deserialize Message");

        if (msg.type != MSG_AUTH) {
            log_osi(5, "authentication failed: expected MSG_AUTH");
            log_osi(7, "prepare response");
            log_osi(6, "serialize Message");
            log_osi(4, "send()");
            send_message(client_fd, MSG_ERROR, "Send MSG_AUTH first");
            close(client_fd);
            continue;
        }

        log_osi(6, "parsed MSG_AUTH");

        /* Проверяем никнейм */
        char nickname[32];
        strncpy(nickname, msg.payload, 31);
        nickname[31] = '\0';
        /* обрезаем пробелы с конца */
        for (int i = (int)strlen(nickname) - 1; i >= 0 && nickname[i] == ' '; i--)
            nickname[i] = '\0';

        if (strlen(nickname) == 0) {
            log_osi(5, "authentication failed: empty nickname");
            send_message(client_fd, MSG_ERROR, "Nickname cannot be empty");
            close(client_fd);
            continue;
        }

        if (add_client(client_fd, client_ip, client_port, nickname) < 0) {
            log_osi(5, "authentication failed: nickname already in use");
            char err[64];
            snprintf(err, sizeof(err), "Nickname [%s] already in use", nickname);
            send_message(client_fd, MSG_ERROR, err);
            close(client_fd);
            continue;
        }

        log_osi(5, "authentication success");
        printf("User [%s] connected\n\n", nickname);

        /* Отправляем WELCOME */
        char welcome[128];
        snprintf(welcome, sizeof(welcome), "Welcome, %s!", nickname);
        log_osi(7, "prepare response");
        log_osi(6, "serialize Message");
        log_osi(4, "send()");
        send_message(client_fd, MSG_WELCOME, welcome);

        /* Уведомляем остальных */
        char join_buf[64];
        snprintf(join_buf, sizeof(join_buf), "User [%s] connected", nickname);
        broadcast(MSG_SERVER_INFO, join_buf, client_fd);
        printf("\n");

        /* ── Основной цикл обработки сообщений ────────────────────────── */
        int disconnected = 0;
        while (!disconnected) {
            log_osi(4, "recv()");
            if (receive_message(client_fd, &msg) < 0) {
                printf("User [%s] disconnected\n\n", nickname);
                disconnected = 1;
                break;
            }
            log_osi(6, "deserialize Message");
            log_osi(5, "client authenticated");

            switch (msg.type) {

                case MSG_TEXT: {
                    log_osi(7, "handle MSG_TEXT");
                    char buf[MAX_PAYLOAD + 64];
                    snprintf(buf, sizeof(buf), "[%s]: %s", nickname, msg.payload);
                    printf("[Layer 7 - Application] broadcast message\n");
                    printf("%s\n\n", buf);
                    broadcast(MSG_TEXT, buf, -1);
                    break;
                }

                case MSG_PRIVATE: {
                    log_osi(7, "handle MSG_PRIVATE");
                    /* формат payload: "target_nick:message" */
                    char *colon = strchr(msg.payload, ':');
                    if (!colon) {
                        send_message(client_fd, MSG_ERROR,
                                     "Bad format: /w <nick> <msg> sends nick:msg");
                        break;
                    }
                    *colon = '\0';
                    char *target = msg.payload;
                    char *text   = colon + 1;

                    char priv_buf[MAX_PAYLOAD + 64];
                    snprintf(priv_buf, sizeof(priv_buf),
                             "[PRIVATE][%s]: %s", nickname, text);

                    if (send_to_nick(target, MSG_PRIVATE, priv_buf) < 0) {
                        char err[MAX_PAYLOAD + 32];
                        snprintf(err, sizeof(err), "User [%s] not found", target);
                        send_message(client_fd, MSG_ERROR, err);
                    }
                    printf("\n");
                    break;
                }

                case MSG_PING:
                    log_osi(7, "handle MSG_PING");
                    log_osi(7, "prepare response (PONG)");
                    log_osi(6, "serialize Message");
                    log_osi(4, "send()");
                    send_message(client_fd, MSG_PONG, NULL);
                    printf("\n");
                    break;

                case MSG_BYE:
                    log_osi(7, "handle MSG_BYE");
                    printf("User [%s] disconnected\n\n", nickname);
                    disconnected = 1;
                    break;

                default:
                    log_osi(7, "handle unknown message type");
                    break;
            }
        }

        /* Удаляем из списка, уведомляем остальных */
        remove_client(client_fd);
        close(client_fd);

        char leave_buf[64];
        snprintf(leave_buf, sizeof(leave_buf), "User [%s] disconnected", nickname);
        broadcast(MSG_SERVER_INFO, leave_buf, -1);
        printf("\n");
    }

    return NULL;
}

/* ── main ──────────────────────────────────────────────────────────────── */
int main(void) {
    int server_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family      = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port        = htons(DEFAULT_PORT);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind"); close(server_fd); exit(EXIT_FAILURE);
    }
    if (listen(server_fd, THREAD_POOL_SIZE * 2) < 0) {
        perror("listen"); close(server_fd); exit(EXIT_FAILURE);
    }

    printf("Server started on port %d (thread pool: %d)\n\n",
           DEFAULT_PORT, THREAD_POOL_SIZE);

    /* Создаём пул потоков */
    pthread_t threads[THREAD_POOL_SIZE];
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        pthread_create(&threads[i], NULL, worker_thread, NULL);
        pthread_detach(threads[i]);
    }

    /* Цикл приёма подключений */
    while (1) {
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) { perror("accept"); continue; }
        enqueue(client_fd);
    }

    close(server_fd);
    return 0;
}
