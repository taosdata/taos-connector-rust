#include "taosws.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

int main()
{
    char *dsn = getenv("TAOS_DSN");
    if (dsn == NULL)
    {
        dsn = "ws://localhost:6041";
    }
    ws_enable_log();

    ws_tmq_conf_t *r = ws_tmq_conf_new();

    enum ws_tmq_conf_res_t r1 = ws_tmq_conf_set(r, "td.connect.ip", "127.0.0.1");
    enum ws_tmq_conf_res_t r2 = ws_tmq_conf_set(r, "td.connect.port", "6041");

    ws_tmq_conf_destroy(r);

    printf("ok\n");
}
