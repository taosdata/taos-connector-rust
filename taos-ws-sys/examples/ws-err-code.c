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
        dsn = "ws://localhost:6041";
    }

    int errno = 0;
    ws_enable_log("debug");
    WS_TAOS *taos = ws_connect(dsn);

    WS_RES *res1 = ws_query(taos, "select * from db_not_exsits.tb1");
    errno = ws_errno(res1);
    assert(errno != 0);
    ws_free_result(res1);

    ws_close(taos);

    WS_TAOS *taos2 = ws_connect(dsn);

    WS_RES *res2 = ws_query(taos2, "select to_iso8601(0) as ts");
    assert(res2 != NULL);
    errno = ws_errno(res2);
    assert(errno == 0);

    ws_free_result(res2);

    ws_close(taos2);
}
