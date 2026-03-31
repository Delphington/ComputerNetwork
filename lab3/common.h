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

enum {
    MSG_HELLO   = 1,
    MSG_WELCOME = 2,
    MSG_TEXT    = 3,
    MSG_PING    = 4,
    MSG_PONG    = 5,
    MSG_BYE     = 6
};

typedef struct {
    uint32_t length;
    uint8_t  type;
    char     payload[MAX_PAYLOAD];
} Message;

static inline int send_message(int sockfd, uint8_t type, const char *data) {
    Message msg;
    msg.type = type;
    if (data) {
        strncpy(msg.payload, data, MAX_PAYLOAD - 1);
        msg.payload[MAX_PAYLOAD - 1] = '\0';
    } else {
        msg.payload[0] = '\0';
    }
    msg.length = sizeof(uint8_t) + strlen(msg.payload) + 1;
    ssize_t total = sizeof(uint32_t) + msg.length;
    if (send(sockfd, &msg, total, MSG_NOSIGNAL) != total)
        return -1;
    return 0;
}

static inline int receive_message(int sockfd, Message *msg) {
    if (recv(sockfd, &msg->length, sizeof(uint32_t), MSG_WAITALL) <= 0)
        return -1;
    if (recv(sockfd, &msg->type, sizeof(uint8_t), MSG_WAITALL) <= 0)
        return -1;
    int payload_len = (int)msg->length - (int)sizeof(uint8_t);
    if (payload_len > 0 && payload_len < MAX_PAYLOAD) {
        if (recv(sockfd, msg->payload, payload_len, MSG_WAITALL) <= 0)
            return -1;
        msg->payload[payload_len] = '\0';
    } else {
        msg->payload[0] = '\0';
    }
    return 0;
}

#endif
