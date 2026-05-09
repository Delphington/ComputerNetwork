---
input links:
  - "[[Образование]]"
---
# Лабораторная работа №7

## Защищённое сетевое взаимодействие: интеграция SSL/TLS, сертификаты и шифрование трафика

---

## 1. Цель работы

Расширить функциональность Лабораторной работы №6, добавив защиту передаваемых данных на транспортном уровне с использованием SSL/TLS.

В предыдущей работе были реализованы механизмы надёжности: измерение RTT, jitter, loss, ACK, повторная отправка и защита от дубликатов . В данной лабораторной работе необходимо обеспечить **конфиденциальность и защищённость сетевого обмена**.

В рамках работы требуется:

- интегрировать библиотеку **OpenSSL**;
- сгенерировать и использовать сертификат сервера;
- заменить `send()` / `recv()` на `SSL_write()` / `SSL_read()`;
- реализовать процесс TLS-рукопожатия;
- сохранить логику ACK, retry и сетевой диагностики из ЛР6;
- добавить логирование защищённого соединения.

---

## 2. Теоретические сведения

### 2.1. Проблема открытой передачи данных

В ЛР2–ЛР6 обмен сообщениями выполнялся по TCP. TCP обеспечивает доставку байтов, но **не шифрует данные**.

Это означает, что при перехвате трафика злоумышленник может увидеть:

- текст сообщений;
- никнеймы пользователей;
- служебные команды;
- идентификаторы сообщений;
- данные диагностики сети.

---

### 2.2. SSL/TLS

**TLS** — протокол защиты сетевого соединения, который обеспечивает:

- шифрование данных;
- проверку подлинности сервера;
- защиту от перехвата;
- защиту от изменения данных при передаче.

Общая схема:

```c++
Client → TCP connect → TLS handshake → encrypted data exchange
```

После успешного TLS-рукопожатия прикладные сообщения передаются уже в зашифрованном виде.

---

### 2.3. TLS-рукопожатие

TLS-рукопожатие — процесс установления защищённого соединения.

Упрощённый порядок:

1. Клиент подключается к серверу по TCP.
2. Клиент начинает TLS-сессию.
3. Сервер отправляет сертификат.
4. Клиент проверяет сертификат.
5. Стороны согласуют параметры шифрования.
6. После успешного рукопожатия начинается защищённый обмен.

---

### 2.4. OpenSSL

Для реализации SSL/TLS в языке C используется библиотека **OpenSSL**.

Основные объекты:

```c++
SSL_CTX *ctx;   // контекст SSL
SSL     *ssl;   // SSL-соединение
```

Основные функции:

```c++
SSL_library_init();
SSL_CTX_new();
SSL_new();
SSL_set_fd();
SSL_accept();
SSL_connect();
SSL_read();
SSL_write();
SSL_shutdown();
```

---

## 3. Постановка задачи

На основе Лабораторной работы №6 необходимо модернизировать клиент и сервер, добавив защищённое соединение SSL/TLS.

В результате выполнения работы клиент и сервер должны обмениваться сообщениями только после успешного TLS-рукопожатия.

---

## 4. Генерация сертификатов

Перед запуском сервера необходимо создать закрытый ключ и самоподписанный сертификат.

Пример команды:

```cpp
openssl req -x509 -newkey rsa:2048 \  
	-keyout server.key \  
	-out server.crt \  
	-days 365 \  
	-nodes
```

После выполнения должны появиться файлы:

```cpp
server.key
server.crt
```

Назначение файлов:

- `server.key` — закрытый ключ сервера;
- `server.crt` — сертификат сервера.

---

## 5. Расширение протокола

В ЛР6 уже использовался расширенный протокол с `MSG_ACK` для подтверждения доставки .

В ЛР7 необходимо добавить новые типы сообщений:

```cpp
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
```

---

## 6. Требования к серверу

### 6.1. Инициализация OpenSSL

Сервер должен выполнить инициализацию библиотеки OpenSSL:

```cpp
SSL_library_init();  
SSL_load_error_strings();  
OpenSSL_add_all_algorithms();
```

---

### 6.2. Создание SSL-контекста

Сервер должен создать SSL-контекст:

```cpp
SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
```

Если контекст не создан, сервер должен вывести ошибку и завершить работу.

---

### 6.3. Загрузка сертификата и ключа

Сервер должен загрузить сертификат и приватный ключ:

```cpp
SSL_CTX_use_certificate_file(ctx, "server.crt", SSL_FILETYPE_PEM);
SSL_CTX_use_PrivateKey_file(ctx, "server.key", SSL_FILETYPE_PEM);
```

Также необходимо проверить соответствие ключа сертификату:

```cpp
SSL_CTX_check_private_key(ctx);
```

---

### 6.4. Создание TCP-сокета

Базовая логика TCP-сервера сохраняется из предыдущих лабораторных работ:

1. `socket()`
2. `bind()`
3. `listen()`
4. `accept()`

В ЛР3 сервер уже был расширен до многопоточной модели с постоянными соединениями и broadcast-рассылкой . В ЛР7 эта архитектура должна быть сохранена.

---

### 6.5. TLS-рукопожатие

После `accept()` сервер должен создать SSL-объект:

```cpp
SSL *ssl = SSL_new(ctx);
SSL_set_fd(ssl, client_sock);
```

Затем выполнить TLS-рукопожатие:

```cpp
if (SSL_accept(ssl) <= 0) {  
ERR_print_errors_fp(stderr);  
close(client_sock);  
}
```

Если рукопожатие успешно:

```cpp
[Security][TLS] handshake success
```

Если ошибка:

```cpp
[Security][TLS] handshake failed
```

---

### 6.6. Замена send/recv

В предыдущих лабораторных работах передача сообщений выполнялась через `send()` и `recv()` .

В ЛР7 необходимо заменить:

```cpp
send()
recv()
```

на:

```cpp
SSL_write()
SSL_read()
```

Пример отправки:

```cpp
int ssl_send_message(SSL *ssl, MessageEx *msg)  
{  
	return SSL_write(ssl, msg, sizeof(MessageEx));  
}
```

Пример приёма:

```cpp
int ssl_recv_message(SSL *ssl, MessageEx *msg)
{
    return SSL_read(ssl, msg, sizeof(MessageEx));
}
```

---

### 6.7. Обработка сообщений

После успешного TLS-рукопожатия сервер должен сохранить всю логику ЛР6:

- `MSG_AUTH` — аутентификация;
- `MSG_TEXT` — широковещательная рассылка;
- `MSG_PRIVATE` — личное сообщение;
- `MSG_PING` — ответ `MSG_PONG`;
- `MSG_ACK` — подтверждение доставки;
- повторная отправка при отсутствии ACK;
- защита от дубликатов;
- запись истории;
- диагностика сети.

---

## 7. Требования к клиенту

### 7.1. Инициализация OpenSSL

Клиент также должен выполнить:

```cpp
SSL_library_init();
SSL_load_error_strings();
OpenSSL_add_all_algorithms();
```

---

### 7.2. Создание SSL-контекста

```cpp
SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
```

---

### 7.3. TCP-подключение

Клиент сначала выполняет обычное TCP-подключение:

```cpp
socket();
connect();
```

После успешного `connect()` создаётся SSL-объект:

```cpp
SSL *ssl = SSL_new(ctx);
SSL_set_fd(ssl, sock);
```

---

### 7.4. TLS-рукопожатие клиента

Клиент должен вызвать:

```cpp
if (SSL_connect(ssl) <= 0) {
    ERR_print_errors_fp(stderr);
}
```

При успехе:

```cpp
[Security][TLS] connected securely
```

---

### 7.5. Проверка сертификата сервера

Минимальный вариант:

- клиент принимает самоподписанный сертификат;
- выводит информацию о сертификате.

Дополнительно можно реализовать проверку:

```cpp
X509 *cert = SSL_get_peer_certificate(ssl);
```

Если сертификат получен:

```cpp
[Security][CERT] server certificate received
```

Если сертификат отсутствует:

```cpp
[Security][CERT] no certificate
```

---

## 8. Логирование

В ЛР6 использовалось логирование по уровням TCP/IP и дополнительным механизмам: `[Transport][PING]`, `[Transport][SIM]`, `[Transport][RETRY]`, `[Transport][ACK]` .

В ЛР7 необходимо добавить новый уровень логирования:

```cpp
[Security]
[Security][TLS]
[Security][CERT]
[Security][ENC]
```

---

### Пример логов сервера

```cpp
[Transport] TCP connection accepted
[Security][TLS] SSL object created
[Security][TLS] handshake started
[Security][CERT] certificate server.crt loaded
[Security][TLS] handshake success
[Security][ENC] encrypted channel established

[Security][ENC] SSL_read MessageEx
[Application] deserialize MessageEx -> MSG_AUTH
[Application] authentication success: Alice

[Application][ACK] process MSG_TEXT (id=41)
[Security][ENC] SSL_write MSG_ACK (id=41)
```

---

### Пример логов клиента

```cpp
[Transport] connected to server
[Security][TLS] SSL context created
[Security][TLS] handshake started
[Security][CERT] server certificate received
[Security][TLS] handshake success
[Security][ENC] encrypted channel established

[Security][ENC] SSL_write MSG_TEXT (id=41)
[Transport][RETRY] wait ACK
[Security][ENC] SSL_read MSG_ACK (id=41)
[Transport][ACK] ACK received (id=41)
```

---

## 9. Алгоритм работы сервера

1. Инициализировать OpenSSL.
2. Создать SSL-контекст.
3. Загрузить сертификат `server.crt`.
4. Загрузить ключ `server.key`.
5. Создать TCP-сокет.
6. Выполнить `bind()`.
7. Выполнить `listen()`.
8. Создать пул потоков.
9. Принимать клиентов через `accept()`.
10. Для каждого клиента:
    - создать `SSL *`;
    - привязать SSL к сокету;
    - выполнить `SSL_accept()`;
    - при успехе перейти к обработке сообщений;
    - при ошибке закрыть соединение.
11. В цикле:
    - читать сообщения через `SSL_read()`;
    - обрабатывать команды;
    - отправлять ответы через `SSL_write()`.
12. При отключении:
    - выполнить `SSL_shutdown()`;
    - освободить `SSL_free()`;
    - закрыть сокет.

---

## 10. Алгоритм работы клиента

1. Инициализировать OpenSSL.
2. Создать SSL-контекст.
3. Создать TCP-сокет.
4. Подключиться к серверу через `connect()`.
5. Создать объект `SSL`.
6. Выполнить `SSL_connect()`.
7. Проверить наличие сертификата сервера.
8. Отправить `MSG_AUTH`.
9. Запустить поток приёма сообщений.
10. В основном потоке читать команды пользователя:
    - обычный текст → `MSG_TEXT`;
    - `/w <nick> <message>` → `MSG_PRIVATE`;
    - `/ping N` → `MSG_PING`;
    - `/netdiag` → вывод статистики;
    - `/history` → `MSG_HISTORY`;
    - `/list` → `MSG_LIST`;
    - `/quit` → `MSG_BYE`.
11. Все сообщения отправлять через `SSL_write()`.
12. Все сообщения получать через `SSL_read()`.

---

## 11. Сборка проекта

Пример компиляции сервера:

```cpp
gcc server.c -o server -lssl -lcrypto -lpthread
```

Пример компиляции клиента:

```cpp
gcc client.c -o client -lssl -lcrypto -lpthread
```

---

## 12. Параметры запуска

### Сервер

```cpp
./server 8080 --cert=server.crt --key=server.key
```

Дополнительно сохраняются параметры из ЛР6:

```cpp
./server 8080 --cert=server.crt --key=server.key --delay=100 --drop=0.2 --corrupt=0.1
```

---

### Клиент

```cpp
./client 127.0.0.1 8080
```

---

## 13. Ожидаемый результат

### Сервер

```cpp
[Security][TLS] OpenSSL initialized
[Security][CERT] certificate loaded: server.crt
[Security][CERT] private key loaded: server.key

[Transport] listening on port 8080
[Transport] TCP connection accepted

[Security][TLS] handshake started
[Security][TLS] handshake success
[Security][ENC] encrypted channel established

[Security][ENC] SSL_read MessageEx
[Application] deserialize MessageEx -> MSG_AUTH
[Application] authentication success: Alice

[Security][ENC] SSL_read MSG_TEXT (id=41)
[Application][ACK] process MSG_TEXT (id=41)
[Transport][ACK] send MSG_ACK (id=41)
[Security][ENC] SSL_write MSG_ACK (id=41)
```

---

### Клиент

```cpp
[Transport] connected to 127.0.0.1:8080
[Security][TLS] SSL context created
[Security][TLS] handshake started
[Security][CERT] server certificate received
[Security][TLS] handshake success
[Security][ENC] encrypted channel established

Enter nickname: Alice

> Hello
[Security][ENC] SSL_write MSG_TEXT (id=41)
[Transport][RETRY] wait ACK
[Security][ENC] SSL_read MSG_ACK (id=41)
[Transport][ACK] ACK received (id=41)
```