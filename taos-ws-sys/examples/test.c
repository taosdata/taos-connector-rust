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
    WS_TAOS *taos = ws_connect(dsn);

    for (int i = 0; i < 50; i++)
    {
        // printf("query start\n");
        WS_RES *res = ws_query_timeout(taos, "insert into t0 values(now, 1)", 1);
        // printf("query end\n");
        if (res == NULL)
        {
            int errno = ws_errno(res);
            char *errstr = ws_errstr(taos);
            printf("Query failed[%d]: %s", errno, errstr);
            exit(-1);
        }

        int code = ws_free_result(res);
        if (code != 0)
        {
            printf("Free result failed[%d]", code);
            exit(-1);
        }
    }

    printf("close start\n");
    int code1 = ws_close(taos);
    printf("close end\n");
    if (code1 != 0)
    {
        printf("Close connection failed[%d]", code1);
        exit(-1);
    }

    return 0;
}
