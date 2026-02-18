#include <iostream>
#include <cstring>
#include <arpa/inet.h>
#include <unistd.h>

int main() {
    int sockfd;
    char buffer[1024];
    struct sockaddr_in servaddr;

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(8080);
    servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");

    std::cout << "Введите сообщение: ";
    std::string msg;
    std::getline(std::cin, msg);

    sendto(sockfd, msg.c_str(), msg.length(), 0, (const struct sockaddr *)&servaddr, sizeof(servaddr));

    socklen_t len = sizeof(servaddr);
    int n = recvfrom(sockfd, buffer, 1024, 0, (struct sockaddr *)&servaddr, &len);
    buffer[n] = '\0';

    std::cout << "Ответ от сервера: " << buffer << std::endl;

    close(sockfd);
    return 0;
}