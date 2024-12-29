#include "taos.h"

int main()
{
    init_log();

    int code = taos_options(TSDB_OPTION_LOCALE, NULL, "group.id", "10s", "enable.auto.commit", "false", NULL);
    if (code != 0)
    {
        return 1;
    }

    return 0;
}
