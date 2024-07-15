WS_FLAGS=-I. -I../../target/debug/ -L. -L../../target/debug/ -ltaosws -lm

all:
	# cargo build
	gcc -g show-databases.c ${WS_FLAGS} -o show-databases
	gcc -g ws-run-sql.c ${WS_FLAGS} -o ws-run-sql

.phony: all

basic:
	gcc -g ws-basic.c ${WS_FLAGS} -o ./target/ws-basic && ./target/ws-basic

interface:
	gcc -g ws-interface.c ${WS_FLAGS} -o ./target/ws-interface && ./target/ws-interface

bind:
	gcc -g ws-bind.c ${WS_FLAGS} -o ./target/ws-bind && ./target/ws-bind

muti:
	gcc -g ws-muti-bind.c ${WS_FLAGS} -o ./target/ws-muti-bind && ./target/ws-muti-bind

table:
	gcc -g ws-table-muti-bind.c ${WS_FLAGS} -o ./target/ws-table-muti-bind && ./target/ws-table-muti-bind

get-err:
	gcc -g ws-get-err.c ${WS_FLAGS} -o ./target/ws-get-err && ./target/ws-get-err
fetch-row:
	gcc -g ws-fetch-row.c ${WS_FLAGS} -o ./target/ws-fetch-row && ./target/ws-fetch-row

tmq-basic:
	gcc -g ws-tmq-basic.c ${WS_FLAGS} -o ./target/ws-tmq-basic && ./target/ws-tmq-basic
