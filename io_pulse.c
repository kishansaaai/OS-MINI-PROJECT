/* I/O workload: io_pulse [iterations] [sleep_ms] [output_path]. */
#include <fcntl.h>
#include <unistd.h>
#include "workload_common.h"

int main(int argc, char *argv[])
{
    unsigned long iterations = 20, sleep_ms = 200;
    if (argc > 4 || (argc > 1 && workload_parse(argv[1], 1, UINT_MAX, &iterations)) ||
        (argc > 2 && workload_parse(argv[2], 0, UINT_MAX, &sleep_ms))) {
        fprintf(stderr, "Usage: %s [positive iterations] [nonnegative sleep_ms] [output_path]\n", argv[0]);
        return 1;
    }
    workload_init();
    const char *path = argc > 3 ? argv[3] : "/tmp/io_pulse.out";
    int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) { perror("io_pulse: open"); return 1; }
    for (unsigned long i = 0; i < iterations && !workload_stopped; i++) {
        char line[128];
        int len = snprintf(line, sizeof(line), "io_pulse iteration=%lu\n", i + 1);
        size_t off = 0;
        while (off < (size_t)len) {
            ssize_t n = write(fd, line + off, (size_t)len - off);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) { perror("io_pulse: write"); close(fd); return 1; }
            off += (size_t)n;
        }
        if (fsync(fd) != 0) { perror("io_pulse: fsync"); close(fd); return 1; }
        printf("io_pulse wrote iteration=%lu\n", i + 1);
        fflush(stdout);
        workload_sleep(sleep_ms);
    }
    if (close(fd) != 0) { perror("io_pulse: close"); return 1; }
    return 0;
}
