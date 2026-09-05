/* Memory pressure workload: memory_hog [chunk_mib] [sleep_ms]. */
#include <stdint.h>
#include <unistd.h>
#include "workload_common.h"

struct allocation {
    struct allocation *next;
    unsigned char data[];
};

int main(int argc, char *argv[])
{
    unsigned long chunk_mib = 8, sleep_ms = 1000;
    if (argc > 3 || (argc > 1 && workload_parse(argv[1], 1,
            (SIZE_MAX - sizeof(struct allocation)) / (1024 * 1024), &chunk_mib)) ||
        (argc > 2 && workload_parse(argv[2], 0, UINT_MAX, &sleep_ms))) {
        fprintf(stderr, "Usage: %s [positive chunk_mib] [nonnegative sleep_ms]\n", argv[0]);
        return 1;
    }
    workload_init();
    const size_t bytes = (size_t)chunk_mib * 1024 * 1024;
    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) return 1;
    size_t count = 0;
    int result = 0;
    struct allocation *head = NULL;
    while (!workload_stopped) {
        struct allocation *block = malloc(sizeof(*block) + bytes);
        if (!block) { perror("memory_hog: malloc"); result = 1; break; }
        block->next = head;
        head = block;
        /* Volatile page writes cannot be optimized away at -O2. Retain all blocks. */
        volatile unsigned char *data = block->data;
        for (size_t i = 0; i < bytes; i += (size_t)page_size) data[i] = 'A';
        data[bytes - 1] = 'A';
        count++;
        printf("allocation=%zu chunk=%luMiB\n", count, chunk_mib);
        fflush(stdout);
        workload_sleep(sleep_ms);
    }
    while (head) {
        struct allocation *next = head->next;
        free(head);
        head = next;
    }
    return result;
}
