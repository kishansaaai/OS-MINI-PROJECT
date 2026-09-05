#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

int main(int argc, char **argv)
{
    if (argc < 2) return 1;
    if (!strcmp(argv[1], "exit")) return argc > 2 ? atoi(argv[2]) : 37;
    if (!strcmp(argv[1], "args")) {
        for (int i = 2; i < argc; i++) printf("[%s]\n", argv[i]);
        return 0;
    }
    if (!strcmp(argv[1], "hold")) {
        signal(SIGTERM, SIG_IGN);
        puts("ready"); fflush(stdout);
        for (;;) pause();
    }
    if (!strcmp(argv[1], "emit")) {
        unsigned char buf[4096];
        for (size_t i = 0; i < sizeof(buf); i++) buf[i] = (unsigned char)i;
        for (int i = 0; i < 256; i++) {
            size_t off = 0;
            while (off < sizeof(buf)) {
                ssize_t n = write(1, buf + off, sizeof(buf) - off);
                if (n < 0 && errno == EINTR) continue;
                if (n <= 0) return 1;
                off += (size_t)n;
            }
        }
        return 0;
    }
    if (!strcmp(argv[1], "inspect")) {
        char hostname[64], byte;
        if (getpid() != 1 || gethostname(hostname, sizeof(hostname)) != 0 ||
            read(0, &byte, 1) != 0 || access("/proc/self/status", R_OK) != 0) return 2;
        for (int fd = 3; fd < 256; fd++) if (fcntl(fd, F_GETFD) != -1) return 3;
        printf("pid=1 hostname=%s stdin=eof descriptors=clean\n", hostname);
        return 0;
    }
    return 1;
}
