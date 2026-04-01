#include <iostream>
#include <cstring>
#include <arpa/inet.h>
#include <unistd.h>

int main() {
    int sockfd;
    char buffer[1024];
    struct sockaddr_in servaddr, cliaddr;

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    // Настройка адреса сервера
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;       // IPv4
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(8080);

    bind(sockfd, (const struct sockaddr *)&servaddr, sizeof(servaddr));

    std::cout << "Сервер запущен и ожидает сообщений..." << std::endl;

    while (true) {
        socklen_t len = sizeof(cliaddr);

        int n = recvfrom(sockfd, buffer, 1024, 0, (struct sockaddr *)&cliaddr, &len);
        buffer[n] = '\0';

        std::cout << "Клиент [" << inet_ntoa(cliaddr.sin_addr) << "]: " << buffer << std::endl;

        sendto(sockfd, buffer, n, 0, (struct sockaddr *)&cliaddr, len);
    }

    close(sockfd);
    return 0;
}