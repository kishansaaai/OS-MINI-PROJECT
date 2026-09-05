#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include "../monitor_ioctl.h"

_Static_assert(sizeof(struct monitor_request) == 88, "ioctl ABI must be stable");

static int expect(int fd, unsigned int cmd, struct monitor_request *req, int error)
{
    errno = 0;
    int result = ioctl(fd, cmd, req);
    if ((error && (result != -1 || errno != error)) || (!error && result != 0)) {
        fprintf(stderr, "ioctl: result=%d errno=%d expected=%d\n", result, errno, error);
        return 1;
    }
    return 0;
}

int main(void)
{
    int fd = open("/dev/container_monitor", O_RDWR);
    if (fd < 0) { perror("monitor device"); return 1; }
    struct monitor_request req = {.pid = getpid(), .soft_limit_bytes = 64ULL << 20,
                                  .hard_limit_bytes = 128ULL << 20, .container_id = "ioctl-probe"};
    int failed = expect(fd, 0, &req, ENOTTY);
    failed |= expect(fd, MONITOR_REGISTER, &req, 0);
    failed |= expect(fd, MONITOR_REGISTER, &req, EEXIST);
    failed |= expect(fd, MONITOR_UNREGISTER, &req, 0);
    failed |= expect(fd, MONITOR_UNREGISTER, &req, ENOENT);
    req.reserved = 1;
    failed |= expect(fd, MONITOR_REGISTER, &req, EINVAL);
    req.reserved = 0;
    req.soft_limit_bytes = req.hard_limit_bytes;
    failed |= expect(fd, MONITOR_REGISTER, &req, EINVAL);
    req.soft_limit_bytes = 0;
    failed |= expect(fd, MONITOR_REGISTER, &req, EINVAL);
    req.soft_limit_bytes = 64ULL << 20;
    memset(req.container_id, 'x', sizeof(req.container_id));
    failed |= expect(fd, MONITOR_REGISTER, &req, EINVAL);
    req.container_id[0] = 0;
    req.pid = -1;
    failed |= expect(fd, MONITOR_REGISTER, &req, EINVAL);
    pid_t child = fork();
    if (child == 0) {
        if (setuid(65534) != 0) _exit(1);
        _exit(expect(fd, MONITOR_REGISTER, &req, EPERM));
    }
    if (child < 0) failed = 1;
    else {
        int status;
        if (waitpid(child, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status)) failed = 1;
    }
    close(fd);
    if (!failed) puts("MONITOR_IOCTL_PASS");
    return failed;
}
