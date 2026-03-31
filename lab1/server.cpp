#include <iostream>
#include <cstring>
#include <arpa/inet.h>
#include <unistd.h>

int main() {
    int sockfd;                         // Дескриптор сокета
    char buffer[1024];                   // Буфер для приема данных
    struct sockaddr_in servaddr, cliaddr; // Структуры адресов сервера и клиента

    // Создание UDP сокета
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    // Настройка адреса сервера
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;       // IPv4
    servaddr.sin_addr.s_addr = INADDR_ANY; // Принимать соединения на всех интерфейсах
    servaddr.sin_port = htons(8080);      // Порт 8080

    // Привязка сокета к адресу
    bind(sockfd, (const struct sockaddr *)&servaddr, sizeof(servaddr));

    std::cout << "Сервер запущен и ожидает сообщений..." << std::endl;

    while (true) {
        socklen_t len = sizeof(cliaddr);   // Размер структуры клиента
        // Получение данных от клиента
        int n = recvfrom(sockfd, buffer, 1024, 0, (struct sockaddr *)&cliaddr, &len);
        buffer[n] = '\0';                   // Завершающий нуль для строки

        // Вывод сообщения с адресом клиента
        std::cout << "Клиент [" << inet_ntoa(cliaddr.sin_addr) << "]: " << buffer << std::endl;

        // Эхо-ответ
        sendto(sockfd, buffer, n, 0, (struct sockaddr *)&cliaddr, len);
    }

    close(sockfd);                         // Закрытие сокета
    return 0;
}