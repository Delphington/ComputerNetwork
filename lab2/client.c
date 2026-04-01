#include "common.h"
#include <pthread.h>

int running = 1;
int sockfd;

// Функция для получения сообщений от сервера
void* receive_thread(void* arg) {
    (void)arg;
    Message msg;
    
    while (running) {
        if (receive_message(sockfd, &msg) < 0) {
            printf("\nConnection closed by server\n");
            running = 0;
            break;
        }
        
        switch (msg.type) {
            case MSG_TEXT:
                printf("\n[Server]: %s\n", msg.payload);
                printf("> ");
                fflush(stdout);
                break;
                
            case MSG_PONG:
                printf("\nPONG\n");
                printf("> ");
                fflush(stdout);
                break;
                
            case MSG_WELCOME:
                printf("%s\n", msg.payload);
                break;
                
            case MSG_BYE:
                printf("\nServer closed connection\n");
                running = 0;
                break;
                
            default:
                printf("\nUnknown message type: %d\n", msg.type);
                printf("> ");
                fflush(stdout);
                break;
        }
    }
    
    return NULL;
}

int main(void) {
    struct sockaddr_in server_addr;
    Message msg;
    pthread_t thread;
    char input[MAX_PAYLOAD];
    char nickname[256];
    
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }
    
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(DEFAULT_PORT);
    
    if (inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    
    // Подключение к серверу
    if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    
    printf("Connected to server\n");
    
    printf("Enter your nickname: ");
    fgets(nickname, sizeof(nickname), stdin);
    nickname[strcspn(nickname, "\n")] = '\0';
    
    send_message(sockfd, MSG_HELLO, nickname);
    
    if (receive_message(sockfd, &msg) < 0) {
        perror("recv welcome");
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    
    if (msg.type == MSG_WELCOME) {
        printf("%s\n", msg.payload);
    } else {
        printf("Protocol error\n");
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    
    // Создание потока для приема сообщений
    pthread_create(&thread, NULL, receive_thread, NULL);
    
    // Основной цикл ввода сообщений
    printf("\nCommands:\n");
    printf("  /ping - send PING\n");
    printf("  /quit - disconnect\n");
    printf("  anything else - send as text\n");
    printf("> ");
    
    while (running) {
        fgets(input, sizeof(input), stdin);
        input[strcspn(input, "\n")] = '\0';
        
        if (!running) break;
        
        if (strcmp(input, "/quit") == 0) {
            send_message(sockfd, MSG_BYE, NULL);
            running = 0;
            break;
        } else if (strcmp(input, "/ping") == 0) {
            send_message(sockfd, MSG_PING, NULL);
        } else if (strlen(input) > 0) {
            send_message(sockfd, MSG_TEXT, input);
        }
    }
    
    // Ожидание завершения потока приема
    pthread_join(thread, NULL);
    
    close(sockfd);
    printf("Disconnected\n");
    
    return 0;
}