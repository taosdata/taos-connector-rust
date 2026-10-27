/*
 * Copyright (c) 2024 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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
        dsn = "ws://localhost:6041/";
    }
    ws_enable_log("trace");
    WS_TAOS *taos = ws_connect(dsn);
    assert(taos != NULL);

    ws_query(taos, "use schemaless_test");
    char *pdata = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639";
    int total_rows = 0;
    WS_RES *rs = ws_schemaless_insert_raw_ttl_with_reqid(
        taos,
        pdata,
        strlen(pdata),
        &total_rows,
        1,
        4,
        0,
        12345);

    printf("%p\n", rs);
    int code = ws_errno(rs);
    printf("%x\n", code);

    if (code != 0)
    {
        const char *errstr = ws_errstr(rs);
        printf("------------------------%ld------1\n", errstr);
        dprintf(2, "Error [%6x]: %s", code, errstr);
        return code;
    }
    printf("------------------------------2\n");
    assert(rs != NULL);
    ws_free_result(rs);

    pdata = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=220i32,phase=0.31f64 1626006833640";
    rs = ws_schemaless_insert_raw(
        taos,
        pdata,
        strlen(pdata),
        &total_rows,
        1,
        4);
    code = ws_errno(rs);
    if (code != 0)
    {
        const char *errstr = ws_errstr(taos);
        dprintf(2, "Error [%6x]: %s", code, errstr);
        return code;
    }

    ws_free_result(rs);
}
