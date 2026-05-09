all: server client

server: server.c common.h
	gcc -Wall -Wextra -pthread -o server server.c

client: client.c common.h
	gcc -Wall -Wextra -pthread -o client client.c

clean:
	rm -f server client chat_history.json
