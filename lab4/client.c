#include "common.h"
#include <pthread.h>
#include <signal.h>

static volatile int      running   = 1;
static int               sockfd    = -1;
static char              nickname[32];
static struct sockaddr_in server_addr;
static pthread_mutex_t   sock_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ── Поток приёма сообщений ────────────────────────────────────────────── */
static void *receive_thread(void *arg) {
    (void)arg;
    Message msg;

    while (running) {
        if (receive_message(sockfd, &msg) < 0) {
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
                printf("\n%s\n> ", msg.payload);
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

    /* Подключение */
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    printf("Connected to %s:%d\n", server_ip, DEFAULT_PORT);

    /* Ввод никнейма и аутентификация */
    printf("Enter your nickname: ");
    fgets(nickname, sizeof(nickname), stdin);
    nickname[strcspn(nickname, "\n")] = '\0';
    if (strlen(nickname) == 0) {
        fprintf(stderr, "Nickname cannot be empty\n");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    /* Отправляем MSG_AUTH */
    send_message(sockfd, MSG_AUTH, nickname);

    /* Ждём ответа сервера (WELCOME или ERROR) */
    Message msg;
    if (receive_message(sockfd, &msg) < 0) {
        fprintf(stderr, "Server connection lost\n");
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    if (msg.type == MSG_ERROR) {
        printf("[ERROR]: %s\n", msg.payload);
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    if (msg.type == MSG_WELCOME) {
        printf("%s\n", msg.payload);
    }

    /* Запускаем поток приёма */
    pthread_t recv_tid;
    pthread_create(&recv_tid, NULL, receive_thread, NULL);

    printf("\nCommands:\n");
    printf("  /w <nick> <msg>  - private message\n");
    printf("  /ping            - send PING\n");
    printf("  /quit            - disconnect\n");
    printf("  <text>           - broadcast message\n");
    printf("> ");
    fflush(stdout);

    char input[MAX_PAYLOAD];
    while (running && fgets(input, sizeof(input), stdin) != NULL) {
        input[strcspn(input, "\n")] = '\0';

        pthread_mutex_lock(&sock_mutex);
        int fd = sockfd;
        pthread_mutex_unlock(&sock_mutex);

        if (strcmp(input, "/quit") == 0) {
            send_message(fd, MSG_BYE, NULL);
            running = 0;

        } else if (strcmp(input, "/ping") == 0) {
            send_message(fd, MSG_PING, NULL);
            printf("> ");
            fflush(stdout);

        } else if (strncmp(input, "/w ", 3) == 0) {
            /* /w <nick> <message> */
            char *rest  = input + 3;
            char *space = strchr(rest, ' ');
            if (!space) {
                printf("Usage: /w <nick> <message>\n> ");
            } else {
                *space = '\0';
                char *target = rest;
                char *text   = space + 1;
                char payload[MAX_PAYLOAD];
                snprintf(payload, sizeof(payload), "%s:%s", target, text);
                send_message(fd, MSG_PRIVATE, payload);
                printf("> ");
            }
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
