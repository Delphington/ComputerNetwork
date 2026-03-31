#include "common.h"
#include <pthread.h>

#define THREAD_POOL_SIZE 10
#define MAX_CLIENTS      100
#define QUEUE_SIZE       128

/* ── Структура клиента ─────────────────────────────────────────────────── */
typedef struct {
    int  fd;
    char ip[INET_ADDRSTRLEN];
    int  port;
    char nickname[256];
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

/* ── Операции с очередью ───────────────────────────────────────────────── */
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
static void add_client(int fd, const char *ip, int port, const char *nick) {
    pthread_mutex_lock(&clients_mutex);
    if (client_count < MAX_CLIENTS) {
        clients[client_count].fd   = fd;
        clients[client_count].port = port;
        strncpy(clients[client_count].ip,       ip,   INET_ADDRSTRLEN - 1);
        strncpy(clients[client_count].nickname, nick, 255);
        client_count++;
    }
    pthread_mutex_unlock(&clients_mutex);
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

/* ── Широковещательная рассылка ────────────────────────────────────────── */
static void broadcast(uint8_t type, const char *data, int exclude_fd) {
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++) {
        if (clients[i].fd != exclude_fd)
            send_message(clients[i].fd, type, data);
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

        /* Ждём MSG_HELLO */
        Message msg;
        if (receive_message(client_fd, &msg) < 0 || msg.type != MSG_HELLO) {
            printf("Protocol error from %s:%d\n", client_ip, client_port);
            close(client_fd);
            continue;
        }

        char nickname[256];
        strncpy(nickname, msg.payload, 255);
        nickname[255] = '\0';
        printf("Client connected: %s [%s:%d]\n", nickname, client_ip, client_port);

        /* Отправляем MSG_WELCOME */
        char welcome[600];
        snprintf(welcome, sizeof(welcome), "Welcome, %s! Connected as %s [%s:%d]",
                 nickname, nickname, client_ip, client_port);
        send_message(client_fd, MSG_WELCOME, welcome);

        /* Добавляем в список и уведомляем остальных */
        add_client(client_fd, client_ip, client_port, nickname);
        char join_buf[512];
        snprintf(join_buf, sizeof(join_buf), "%s [%s:%d] joined the chat",
                 nickname, client_ip, client_port);
        broadcast(MSG_TEXT, join_buf, client_fd);

        /* Цикл обработки сообщений */
        int disconnected = 0;
        while (!disconnected) {
            if (receive_message(client_fd, &msg) < 0) {
                printf("Client disconnected: %s [%s:%d]\n", nickname, client_ip, client_port);
                disconnected = 1;
                break;
            }

            switch (msg.type) {
                case MSG_TEXT: {
                    char buf[MAX_PAYLOAD + 300];
                    snprintf(buf, sizeof(buf), "%s [%s:%d]: %s",
                             nickname, client_ip, client_port, msg.payload);
                    printf("%s\n", buf);
                    /* Рассылаем всем, включая отправителя */
                    broadcast(MSG_TEXT, buf, -1);
                    break;
                }
                case MSG_PING:
                    send_message(client_fd, MSG_PONG, NULL);
                    break;

                case MSG_BYE:
                    printf("Client disconnected: %s [%s:%d]\n", nickname, client_ip, client_port);
                    disconnected = 1;
                    break;

                default:
                    printf("[%s:%d] unknown type: %d\n", client_ip, client_port, msg.type);
                    break;
            }
        }

        /* Удаляем из списка, закрываем сокет, уведомляем остальных */
        remove_client(client_fd);
        close(client_fd);

        char leave_buf[512];
        snprintf(leave_buf, sizeof(leave_buf), "%s [%s:%d] left the chat",
                 nickname, client_ip, client_port);
        broadcast(MSG_TEXT, leave_buf, -1);
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

    printf("Server started on port %d (thread pool: %d)\n", DEFAULT_PORT, THREAD_POOL_SIZE);

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

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        printf("New connection from %s:%d\n", client_ip, ntohs(client_addr.sin_port));

        enqueue(client_fd);
    }

    close(server_fd);
    return 0;
}
