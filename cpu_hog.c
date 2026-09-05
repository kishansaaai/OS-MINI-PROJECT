/* CPU-bound workload: cpu_hog [seconds]. */
#include "workload_common.h"

int main(int argc, char *argv[])
{
    unsigned long duration = 10;
    if (argc > 2 || (argc == 2 && workload_parse(argv[1], 1, UINT_MAX, &duration))) {
        fprintf(stderr, "Usage: %s [seconds: 1..%u]\n", argv[0], UINT_MAX);
        return 1;
    }
    workload_init();
    struct timespec start, now;
    if (clock_gettime(CLOCK_MONOTONIC, &start) != 0) return 1;
    unsigned long last_report = 0;
    volatile unsigned long long accumulator = 0;
    while (!workload_stopped) {
        if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return 1;
        time_t elapsed = now.tv_sec - start.tv_sec - (now.tv_nsec < start.tv_nsec);
        if ((unsigned long)elapsed >= duration) break;
        accumulator = accumulator * 1664525ULL + 1013904223ULL;
        if ((unsigned long)elapsed != last_report) {
            last_report = (unsigned long)elapsed;
            printf("cpu_hog alive elapsed=%lu accumulator=%llu\n", last_report, accumulator);
            fflush(stdout);
        }
    }
    printf("cpu_hog done duration=%lu accumulator=%llu\n", duration, accumulator);
    return 0;
}
