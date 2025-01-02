#include <assert.h>
#include "taos.h"

int main()
{
    int code = taos_options(TSDB_OPTION_LOCALE, NULL, "group.id", "10s", "enable.auto.commit", "false", NULL);
    assert(code == 0);

    return 0;
}
