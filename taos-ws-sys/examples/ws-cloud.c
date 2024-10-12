#include "taosws.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

int main()
{
    char *dsn = getenv("TDENGINE_CLOUD_DSN");
    assert(dsn != NULL);

    ws_enable_log("debug");

    printf("%s\n", dsn);
    WS_TAOS *taos = ws_connect(dsn);

    const char *version = ws_get_server_info(taos);
    dprintf(2, "Server version: %s\n", version);

    ws_close(taos);
}
