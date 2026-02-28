#include "common.h"

int main(int argc, char* argv[]) {
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);
    Message msg;
    char client_ip[INET_ADDRSTRLEN];
    
    // Создание сокета
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }
    
    // Настройка адреса сервера
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(DEFAULT_PORT);
    
    // Привязка сокета
    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    
    // Ожидание подключения (максимум 1 клиент)
    if (listen(server_fd, 1) < 0) {
        perror("listen");
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    
    printf("Server started on port %d\n", DEFAULT_PORT);
    printf("Waiting for connection...\n");
    
    // Принятие клиента
    client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
    if (client_fd < 0) {
        perror("accept");
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
    printf("Client connected from %s:%d\n", client_ip, ntohs(client_addr.sin_port));
    
    // Получение MSG_HELLO
    if (receive_message(client_fd, &msg) < 0) {
        perror("recv hello");
        close(client_fd);
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    
    if (msg.type == MSG_HELLO) {
        printf("[%s:%d]: Hello (%s)\n", client_ip, ntohs(client_addr.sin_port), msg.payload);
        
        // Отправка MSG_WELCOME
        char welcome_msg[256];
        snprintf(welcome_msg, sizeof(welcome_msg), "Welcome %s:%d", client_ip, ntohs(client_addr.sin_port));
        send_message(client_fd, MSG_WELCOME, welcome_msg);
    } else {
        printf("Protocol error: expected HELLO\n");
        close(client_fd);
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    
    // Основной цикл обработки сообщений
    int running = 1;
    while (running) {
        if (receive_message(client_fd, &msg) < 0) {
            printf("Client disconnected\n");
            break;
        }
        
        switch (msg.type) {
            case MSG_TEXT:
                printf("[%s:%d]: %s\n", client_ip, ntohs(client_addr.sin_port), msg.payload);
                break;
                
            case MSG_PING:
                printf("[%s:%d]: PING received, sending PONG\n", client_ip, ntohs(client_addr.sin_port));
                send_message(client_fd, MSG_PONG, NULL);
                break;
                
            case MSG_BYE:
                printf("[%s:%d]: Client disconnected\n", client_ip, ntohs(client_addr.sin_port));
                running = 0;
                break;
                
            default:
                printf("[%s:%d]: Unknown message type: %d\n", client_ip, ntohs(client_addr.sin_port), msg.type);
                break;
        }
    }
    
    // Закрытие сокетов
    close(client_fd);
    close(server_fd);
    printf("Server stopped\n");
    
    return 0;
}