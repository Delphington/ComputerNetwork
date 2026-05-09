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
#include <time.h>

#define MAX_NAME     32
#define MAX_PAYLOAD  512
#define MAX_TIME_STR 32
#define DEFAULT_PORT 8888

enum {
    MSG_HELLO        = 1,
    MSG_WELCOME      = 2,
    MSG_TEXT         = 3,
    MSG_PING         = 4,
    MSG_PONG         = 5,
    MSG_BYE          = 6,
    MSG_AUTH         = 7,
    MSG_PRIVATE      = 8,
    MSG_ERROR        = 9,
    MSG_SERVER_INFO  = 10,
    MSG_LIST         = 11,
    MSG_HISTORY      = 12,
    MSG_HISTORY_DATA = 13,
    MSG_HELP         = 14,
    MSG_ACK          = 15
};

typedef struct {
    uint32_t length;
    uint8_t  type;
    uint32_t msg_id;
    char     sender[MAX_NAME];
    char     receiver[MAX_NAME];
    time_t   timestamp;
    char     payload[MAX_PAYLOAD];
} MessageEx;

static inline const char *msg_type_str(uint8_t type) {
    switch (type) {
        case MSG_HELLO:        return "MSG_HELLO";
        case MSG_WELCOME:      return "MSG_WELCOME";
        case MSG_TEXT:         return "MSG_TEXT";
        case MSG_PING:         return "MSG_PING";
        case MSG_PONG:         return "MSG_PONG";
        case MSG_BYE:          return "MSG_BYE";
        case MSG_AUTH:         return "MSG_AUTH";
        case MSG_PRIVATE:      return "MSG_PRIVATE";
        case MSG_ERROR:        return "MSG_ERROR";
        case MSG_SERVER_INFO:  return "MSG_SERVER_INFO";
        case MSG_LIST:         return "MSG_LIST";
        case MSG_HISTORY:      return "MSG_HISTORY";
        case MSG_HISTORY_DATA: return "MSG_HISTORY_DATA";
        case MSG_HELP:         return "MSG_HELP";
        case MSG_ACK:          return "MSG_ACK";
        default:               return "MSG_UNKNOWN";
    }
}

static inline void format_time(time_t t, char *buf, size_t len) {
    struct tm *tm_info = localtime(&t);
    strftime(buf, len, "%Y-%m-%d %H:%M:%S", tm_info);
}

static inline int send_msgex(int sockfd, MessageEx *msg) {
    msg->length = (uint32_t)(sizeof(MessageEx) - sizeof(uint32_t));
    ssize_t total = (ssize_t)sizeof(MessageEx);
    if (send(sockfd, msg, (size_t)total, MSG_NOSIGNAL) != total)
        return -1;
    return 0;
}

static inline int recv_msgex(int sockfd, MessageEx *msg) {
    ssize_t n = recv(sockfd, msg, sizeof(MessageEx), MSG_WAITALL);
    if (n <= 0) return -1;
    return 0;
}

#endif
