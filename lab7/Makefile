CC      = gcc
CFLAGS  = -Wall -Wextra -pthread
LDFLAGS = -lpthread -lm

.PHONY: all build deps cert clean

all: deps
	@$(MAKE) --no-print-directory build

deps:
	@dpkg -l libssl-dev 2>/dev/null | grep -q '^ii' \
		|| (echo "[make] Installing libssl-dev..." && sudo apt-get install -y libssl-dev)

build: server client

server: server.c common.h
	$(CC) $(CFLAGS) $(shell pkg-config --cflags openssl 2>/dev/null) \
		-o server server.c \
		$(shell pkg-config --libs openssl 2>/dev/null || echo "-lssl -lcrypto") $(LDFLAGS)

client: client.c common.h
	$(CC) $(CFLAGS) $(shell pkg-config --cflags openssl 2>/dev/null) \
		-o client client.c \
		$(shell pkg-config --libs openssl 2>/dev/null || echo "-lssl -lcrypto") $(LDFLAGS)

cert:
	openssl req -x509 -newkey rsa:2048 \
		-keyout server.key \
		-out server.crt \
		-days 365 \
		-nodes \
		-subj "/CN=localhost"

clean:
	rm -f server client chat_history.json net_diag_*.json
