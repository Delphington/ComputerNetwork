all: server client

server: server.c common.h
	gcc -Wall -Wextra -pthread -o server server.c -lm

client: client.c common.h
	gcc -Wall -Wextra -pthread -o client client.c -lm

clean:
	rm -f server client chat_history.json net_diag_*.json
