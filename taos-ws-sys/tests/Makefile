DEBUG_PATH=../../target/debug
FLAGS=-I${DEBUG_PATH} -L${DEBUG_PATH} -Wl,-rpath=${DEBUG_PATH} -ltaosws

test_native:
	gcc -g test_native.c ${FLAGS} -o ${DEBUG_PATH}/test_native
	./${DEBUG_PATH}/test_native
