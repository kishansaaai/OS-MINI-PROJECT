#ifndef WORKLOAD_COMMON_H
#define WORKLOAD_COMMON_H

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static volatile sig_atomic_t workload_stopped;

static void workload_signal(int sig)
{
    (void)sig;
    workload_stopped = 1;
}

static inline void workload_init(void)
{
    struct sigaction sa = {.sa_handler = workload_signal};
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
}

static inline int workload_parse(const char *arg, unsigned long min,
                                unsigned long max, unsigned long *out)
{
    if (!arg || !isdigit((unsigned char)arg[0])) return -1;
    char *end;
    errno = 0;
    unsigned long value = strtoul(arg, &end, 10);
    if (errno || *end || value < min || value > max) return -1;
    *out = value;
    return 0;
}

static inline void workload_sleep(unsigned long milliseconds)
{
    struct timespec remaining = {(time_t)(milliseconds / 1000),
                                 (long)(milliseconds % 1000) * 1000000L};
    while (!workload_stopped && nanosleep(&remaining, &remaining) != 0)
        if (errno != EINTR) break;
}

#endif
