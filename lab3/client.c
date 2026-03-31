#include "common.h"
#include <pthread.h>
#include <signal.h>

#define RECONNECT_DELAY 2

static volatile int      running   = 1;
static volatile int      connected = 0;
static int               sockfd    = -1;
static char              nickname[256];
static struct sockaddr_in server_addr;
static pthread_mutex_t   sock_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ── Установить соединение ─────────────────────────────────────────────── */
static int do_connect(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    if (connect(fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* ── Рукопожатие HELLO / WELCOME ───────────────────────────────────────── */
static int do_handshake(int fd) {
    send_message(fd, MSG_HELLO, nickname);
    Message msg;
    if (receive_message(fd, &msg) < 0 || msg.type != MSG_WELCOME)
        return -1;
    printf("\n%s\n> ", msg.payload);
    fflush(stdout);
    return 0;
}

/* ── Поток приёма сообщений ────────────────────────────────────────────── */
static void *receive_thread(void *arg) {
    (void)arg;
    Message msg;

    while (running) {
        pthread_mutex_lock(&sock_mutex);
        int fd = sockfd;
        int is_connected = connected;
        pthread_mutex_unlock(&sock_mutex);

        if (!is_connected || fd < 0) {
            usleep(100000);
            continue;
        }

        if (receive_message(fd, &msg) < 0) {
            if (!running) break;

            printf("\nDisconnected. Reconnecting in %d s...\n", RECONNECT_DELAY);

            /* Помечаем как отключённого, закрываем старый сокет */
            pthread_mutex_lock(&sock_mutex);
            connected = 0;
            sockfd    = -1;
            pthread_mutex_unlock(&sock_mutex);
            close(fd);

            /* Цикл переподключения */
            while (running) {
                sleep(RECONNECT_DELAY);
                int new_fd = do_connect();
                if (new_fd < 0) {
                    printf("Reconnect failed, retrying...\n");
                    continue;
                }
                if (do_handshake(new_fd) < 0) {
                    close(new_fd);
                    printf("Handshake failed, retrying...\n");
                    continue;
                }
                pthread_mutex_lock(&sock_mutex);
                sockfd    = new_fd;
                connected = 1;
                pthread_mutex_unlock(&sock_mutex);
                printf("Reconnected!\n> ");
                fflush(stdout);
                break;
            }
            continue;
        }

        switch (msg.type) {
            case MSG_TEXT:
                printf("\n%s\n> ", msg.payload);
                fflush(stdout);
                break;

            case MSG_PONG:
                printf("\nPONG\n> ");
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

/* ── main ──────────────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);

    const char *server_ip = (argc > 1) ? argv[1] : "127.0.0.1";

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port   = htons(DEFAULT_PORT);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        perror("inet_pton");
        exit(EXIT_FAILURE);
    }

    printf("Enter your nickname: ");
    fgets(nickname, sizeof(nickname), stdin);
    nickname[strcspn(nickname, "\n")] = '\0';
    if (strlen(nickname) == 0) strcpy(nickname, "Anonymous");

    /* Первичное подключение */
    sockfd = do_connect();
    if (sockfd < 0) { perror("connect"); exit(EXIT_FAILURE); }
    printf("Connected to %s:%d\n", server_ip, DEFAULT_PORT);

    if (do_handshake(sockfd) < 0) {
        fprintf(stderr, "Handshake failed\n");
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    connected = 1;

    /* Запускаем поток приёма */
    pthread_t recv_tid;
    pthread_create(&recv_tid, NULL, receive_thread, NULL);

    printf("\nCommands:\n");
    printf("  /ping  - send PING\n");
    printf("  /quit  - disconnect\n");
    printf("  <text> - send message to everyone\n");
    printf("> ");
    fflush(stdout);

    char input[MAX_PAYLOAD];
    while (running && fgets(input, sizeof(input), stdin) != NULL) {
        input[strcspn(input, "\n")] = '\0';

        pthread_mutex_lock(&sock_mutex);
        int fd = sockfd;
        int is_connected = connected;
        pthread_mutex_unlock(&sock_mutex);

        if (!is_connected) {
            printf("Not connected, please wait...\n> ");
            fflush(stdout);
            continue;
        }

        if (strcmp(input, "/quit") == 0) {
            send_message(fd, MSG_BYE, NULL);
            running = 0;
        } else if (strcmp(input, "/ping") == 0) {
            send_message(fd, MSG_PING, NULL);
            printf("> ");
            fflush(stdout);
        } else if (strlen(input) > 0) {
            send_message(fd, MSG_TEXT, input);
            printf("> ");
            fflush(stdout);
        } else {
            printf("> ");
            fflush(stdout);
        }
    }

    running = 0;
    pthread_join(recv_tid, NULL);

    pthread_mutex_lock(&sock_mutex);
    if (sockfd >= 0) { close(sockfd); sockfd = -1; }
    pthread_mutex_unlock(&sock_mutex);

    printf("Disconnected\n");
    return 0;
}
