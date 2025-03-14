#include <assert.h>
#include <stdio.h>
#include "taos.h"

int main()
{
    // Test tmq_conf
    tmq_conf_t *tmq_conf = tmq_conf_new();
    assert(tmq_conf != NULL);

    tmq_conf_res_t conf_res = tmq_conf_set(tmq_conf, "group.id", "1");
    assert(conf_res == TMQ_CONF_OK);

    tmq_conf_destroy(tmq_conf);

    // Test tmq_list
    tmq_list_t *tmq_list = tmq_list_new();
    assert(tmq_list != NULL);

    int32_t list_res = tmq_list_append(tmq_list, "value");
    assert(list_res == 0);

    int32_t size = tmq_list_get_size(tmq_list);
    assert(size == 1);

    char **array = tmq_list_to_c_array(tmq_list);
    assert(array != NULL);

    printf("tmq_list_to_c_array: ");
    for (int i = 0; i < size; i++)
    {
        printf("%s ", array[i]);
    }
    printf("\n");

    tmq_list_destroy(tmq_list);

    return 0;
}
