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
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509.h>

#define MAX_NAME     32
#define MAX_PAYLOAD  512
#define MAX_TIME_STR 32
#define DEFAULT_PORT 8080

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
    MSG_ACK          = 15,
    MSG_TLS_INFO     = 16,
    MSG_SECURE_ERROR = 17
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
        case MSG_TLS_INFO:     return "MSG_TLS_INFO";
        case MSG_SECURE_ERROR: return "MSG_SECURE_ERROR";
        default:               return "MSG_UNKNOWN";
    }
}

static inline void format_time(time_t t, char *buf, size_t len) {
    struct tm *tm_info = localtime(&t);
    strftime(buf, len, "%Y-%m-%d %H:%M:%S", tm_info);
}

static inline int ssl_send_msgex(SSL *ssl, MessageEx *msg) {
    msg->length = (uint32_t)(sizeof(MessageEx) - sizeof(uint32_t));
    int total  = 0;
    int target = (int)sizeof(MessageEx);
    const char *buf = (const char *)msg;
    while (total < target) {
        int n = SSL_write(ssl, buf + total, target - total);
        if (n <= 0) return -1;
        total += n;
    }
    return 0;
}

static inline int ssl_recv_msgex(SSL *ssl, MessageEx *msg) {
    int   total  = 0;
    int   target = (int)sizeof(MessageEx);
    char *buf    = (char *)msg;
    while (total < target) {
        int n = SSL_read(ssl, buf + total, target - total);
        if (n <= 0) return -1;
        total += n;
    }
    return 0;
}

#endif
