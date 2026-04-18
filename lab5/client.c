#include "common.h"
#include <pthread.h>
#include <signal.h>

static volatile int    running  = 1;
static int             sockfd   = -1;
static char            nickname[MAX_NAME];
static pthread_mutex_t sock_mtx = PTHREAD_MUTEX_INITIALIZER;

/* ── Receive thread ──────────────────────────────────────────────────────── */
static void *recv_thread(void *arg) {
    (void)arg;
    MessageEx msg;

    while (running) {
        if (recv_msgex(sockfd, &msg) < 0) {
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
                printf("\n%s\n[CLIENT]: hmm... TCP feels stable today\n> ", msg.payload);
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
                printf("\n[SERVER]: PONG\n[LOG]: i love cast (no segmentation faults pls)\n> ");
                fflush(stdout);
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

/* ── main ────────────────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);

    const char *server_ip = (argc > 1) ? argv[1] : "127.0.0.1";

    struct sockaddr_in saddr;
    memset(&saddr, 0, sizeof(saddr));
    saddr.sin_family = AF_INET;
    saddr.sin_port   = htons(DEFAULT_PORT);
    if (inet_pton(AF_INET, server_ip, &saddr.sin_addr) <= 0) {
        perror("inet_pton"); exit(EXIT_FAILURE);
    }

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    if (connect(sockfd, (struct sockaddr *)&saddr, sizeof(saddr)) < 0) {
        perror("connect"); close(sockfd); exit(EXIT_FAILURE);
    }
    printf("Connected\n");

    printf("Enter nickname: ");
    fflush(stdout);
    fgets(nickname, sizeof(nickname), stdin);
    nickname[strcspn(nickname, "\n")] = '\0';
    if (strlen(nickname) == 0) {
        fprintf(stderr, "Nickname cannot be empty\n");
        close(sockfd); exit(EXIT_FAILURE);
    }

    /* Send MSG_AUTH */
    MessageEx auth;
    memset(&auth, 0, sizeof(auth));
    auth.type      = MSG_AUTH;
    auth.timestamp = time(NULL);
    strncpy(auth.sender,  nickname, MAX_NAME - 1);
    strncpy(auth.payload, nickname, MAX_PAYLOAD - 1);
    auth.length = (uint32_t)(sizeof(MessageEx) - sizeof(uint32_t));
    send_msgex(sockfd, &auth);

    /* Wait for WELCOME or ERROR */
    MessageEx resp;
    if (recv_msgex(sockfd, &resp) < 0) {
        fprintf(stderr, "Connection lost\n");
        close(sockfd); exit(EXIT_FAILURE);
    }
    if (resp.type == MSG_ERROR) {
        printf("[ERROR]: %s\n", resp.payload);
        close(sockfd); exit(EXIT_FAILURE);
    }
    if (resp.type == MSG_WELCOME)
        printf("%s\n", resp.payload);

    /* Start receive thread */
    pthread_t rtid;
    pthread_create(&rtid, NULL, recv_thread, NULL);

    printf("\n> ");
    fflush(stdout);

    char input[MAX_PAYLOAD];
    while (running && fgets(input, sizeof(input), stdin) != NULL) {
        input[strcspn(input, "\n")] = '\0';
        if (!running) break;

        MessageEx msg;
        memset(&msg, 0, sizeof(msg));
        msg.timestamp = time(NULL);
        msg.length    = (uint32_t)(sizeof(MessageEx) - sizeof(uint32_t));
        strncpy(msg.sender, nickname, MAX_NAME - 1);

        if (strcmp(input, "/quit") == 0) {
            msg.type = MSG_BYE;
            send_msgex(sockfd, &msg);
            running = 0;

        } else if (strcmp(input, "/ping") == 0) {
            msg.type = MSG_PING;
            send_msgex(sockfd, &msg);
            printf("> ");
            fflush(stdout);

        } else if (strcmp(input, "/help") == 0) {
            printf("Available commands:\n");
            printf("/help\n");
            printf("/history\n");
            printf("/history N\n");
            printf("/list\n");
            printf("/quit\n");
            printf("/w <nick> <message>\n");
            printf("/ping\n");
            printf("Tip: meow, message delivered\n> ");
            fflush(stdout);

        } else if (strcmp(input, "/list") == 0) {
            msg.type = MSG_LIST;
            send_msgex(sockfd, &msg);
            printf("> ");
            fflush(stdout);

        } else if (strncmp(input, "/history", 8) == 0) {
            msg.type = MSG_HISTORY;
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
            send_msgex(sockfd, &msg);
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
                msg.type = MSG_PRIVATE;
                strncpy(msg.receiver, target, MAX_NAME - 1);
                strncpy(msg.payload,  text,   MAX_PAYLOAD - 1);
                send_msgex(sockfd, &msg);
                printf("> ");
                fflush(stdout);
            }

        } else if (strlen(input) > 0) {
            msg.type = MSG_TEXT;
            strncpy(msg.payload, input, MAX_PAYLOAD - 1);
            send_msgex(sockfd, &msg);
            printf("> ");
            fflush(stdout);

        } else {
            printf("> ");
            fflush(stdout);
        }
    }

    running = 0;
    pthread_join(rtid, NULL);

    pthread_mutex_lock(&sock_mtx);
    if (sockfd >= 0) { close(sockfd); sockfd = -1; }
    pthread_mutex_unlock(&sock_mtx);

    printf("Disconnected\n");
    return 0;
}
