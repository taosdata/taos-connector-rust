# Copyright (c) 2022 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

WS_FLAGS=-I. -I../../target/debug/ -L. -L../../target/debug/ -ltaosws -lm

all:
	# cargo build
	gcc -g show-databases.c ${WS_FLAGS} -o show-databases
	gcc -g ws-run-sql.c ${WS_FLAGS} -o ws-run-sql

.PHONY: all

basic:
	gcc -g ws-basic.c ${WS_FLAGS} -o ./target/ws-basic && ./target/ws-basic
cloud:
	gcc -g ws-cloud.c ${WS_FLAGS} -o ./target/ws-cloud && ./target/ws-cloud

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

tmq-all:
	gcc -g ws-tmq-all.c ${WS_FLAGS} -o ./target/ws-tmq-all && ./target/ws-tmq-all	

sml:
	gcc -g ws-schemaless.c ${WS_FLAGS} -o ./target/ws-schemaless && ./target/ws-schemaless

errno:
	gcc -g ws-err-code.c ${WS_FLAGS} -o ./target/ws-err-code && ./target/ws-err-code


run-all: basic interface bind muti table fetch-row tmq-basic schemaless errno
