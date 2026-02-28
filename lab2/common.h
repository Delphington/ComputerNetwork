#ifndef COMMON_H
#define COMMON_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>

#define MAX_PAYLOAD 1024
#define DEFAULT_PORT 8888

// Типы сообщений
enum {
    MSG_HELLO = 1,   // клиент -> сервер (ник)
    MSG_WELCOME = 2, // сервер -> клиент
    MSG_TEXT = 3,    // текст
    MSG_PING = 4,
    MSG_PONG = 5,
    MSG_BYE = 6
};

// Структура сообщения
typedef struct {
    uint32_t length;          // длина поля type + payload
    uint8_t type;              // тип сообщения
    char payload[MAX_PAYLOAD]; // данные
} Message;

// Функция для отправки сообщения
int send_message(int sockfd, uint8_t type, const char* data) {
    Message msg;
    msg.type = type;
    
    if (data) {
        strncpy(msg.payload, data, MAX_PAYLOAD - 1);
        msg.payload[MAX_PAYLOAD - 1] = '\0';
    } else {
        msg.payload[0] = '\0';
    }
    
    msg.length = sizeof(uint8_t) + strlen(msg.payload) + 1; // +1 для нуль-терминатора
    
    // Отправляем структуру целиком
    if (send(sockfd, &msg, sizeof(uint32_t) + sizeof(uint8_t) + msg.length - sizeof(uint8_t), 0) <= 0) {
        return -1;
    }
    return 0;
}

// Функция для приема сообщения
int receive_message(int sockfd, Message* msg) {
    // Принимаем заголовок (length + type)
    if (recv(sockfd, &msg->length, sizeof(uint32_t), 0) <= 0) {
        return -1;
    }
    
    if (recv(sockfd, &msg->type, sizeof(uint8_t), 0) <= 0) {
        return -1;
    }
    
    int payload_len = msg->length - sizeof(uint8_t);
    if (payload_len > 0 && payload_len < MAX_PAYLOAD) {
        if (recv(sockfd, msg->payload, payload_len, 0) <= 0) {
            return -1;
        }
        msg->payload[payload_len] = '\0';
    } else {
        msg->payload[0] = '\0';
    }
    
    return 0;
}

#endif