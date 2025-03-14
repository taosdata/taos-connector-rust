#include <assert.h>
#include <stdio.h>
#include "taos.h"

int main()
{
    int code = taos_options(TSDB_OPTION_LOCALE, NULL);
    assert(code == 0);

    char *info = taos_get_client_info();
    printf("client info: %s\n", info);

    return 0;
}
