DEBUG_PATH := ../../target/debug
LINK_FLAGS := -I$(DEBUG_PATH) -L$(DEBUG_PATH) -ltaosws

OS := $(shell uname)

ifeq ($(OS), Linux)
    export LD_LIBRARY_PATH=$(DEBUG_PATH)
else ifeq ($(OS), Darwin)
    export DYLD_LIBRARY_PATH=$(DEBUG_PATH)
else
    $(error Unsupported OS: $(OS))
endif

test_native:
	gcc -g test_native.c $(LINK_FLAGS) -o $(DEBUG_PATH)/test_native
	./$(DEBUG_PATH)/test_native
