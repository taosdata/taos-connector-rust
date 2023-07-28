WS_FLAGS=-I. -I../../target/debug/ -L. -L../../target/debug/ -ltaosws -lm

all:
	# cargo build
	gcc -g show-databases.c ${WS_FLAGS} -o show-databases
	gcc -g ws-run-sql.c ${WS_FLAGS} -o ws-run-sql

.phony: all

basic:
	gcc -g ws-basic.c ${WS_FLAGS} -o ./target/ws-basic && ./target/ws-basic

bind:
	gcc -g ws-bind.c ${WS_FLAGS} -o ./target/ws-bind && ./target/ws-bind

muti:
	gcc -g ws-muti-bind.c ${WS_FLAGS} -o ./target/ws-muti-bind && ./target/ws-muti-bind
