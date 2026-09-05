#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN MONITOR_NAME_LEN
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 64
#define DEFAULT_SOFT_MIB 40UL
#define DEFAULT_HARD_MIB 64UL
#define MAX_CONTAINERS 64
#define MAX_ARGS 8
#define RESPONSE_MAX 8192
#define IPC_TIMEOUT_MS 2000

typedef enum { CMD_SUPERVISOR, CMD_START, CMD_RUN, CMD_PS, CMD_LOGS, CMD_STOP } command_kind_t;
typedef enum { CONTAINER_RUNNING, CONTAINER_STOPPED, CONTAINER_KILLED, CONTAINER_EXITED } container_state_t;

/* Local, same-build protocol. Never accept strings/counts without validation. */
typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[PATH_MAX];
    char args[MAX_ARGS][PATH_MAX];
    int arg_count;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    unsigned int length;
    int more;
    char message[RESPONSE_MAX];
} control_response_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    dev_t root_dev;
    ino_t root_ino;
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes, hard_limit_bytes;
    int exit_code, exit_signal, stop_requested, run_client_fd;
    int log_fd, logs_done, log_error;
    int64_t stop_deadline;
    pthread_t producer;
    int producer_started;
    struct container_record *next;
} container_record_t;

typedef struct {
    container_record_t *container;
    size_t length; /* Zero is an ordered end-of-log marker. */
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head, tail, count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty, not_full, drained;
} bounded_buffer_t;

typedef struct {
    container_record_t *container;
    int pipe_fd;
    bounded_buffer_t *buffer;
} producer_args_t;

typedef struct {
    control_request_t request;
    int log_fd;
    int max_fd;
} child_config_t;

typedef struct {
    int server_fd, monitor_fd, log_dir_fd, lock_fd;
    char socket_path[sizeof(((struct sockaddr_un *)0)->sun_path)];
    pthread_t consumer;
    bounded_buffer_t buffer;
    container_record_t *containers;
    unsigned int count;
} supervisor_ctx_t;

static volatile sig_atomic_t should_stop;
static int signal_pipe[2] = {-1, -1};

static int64_t monotonic_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static const char *state_to_str(container_state_t state)
{
    switch (state) {
    case CONTAINER_RUNNING: return "running";
    case CONTAINER_STOPPED: return "stopped";
    case CONTAINER_KILLED: return "killed"; /* SIGKILL does not identify its sender. */
    case CONTAINER_EXITED: return "exited";
    }
    return "unknown";
}

static void usage(const char *prog)
{
    fprintf(stderr, "Usage:\n"
        "  %s supervisor [base-rootfs]\n"
        "  %s {start|run} <id> <rootfs> <command> [args...] [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s ps\n  %s {logs|stop} <id>\n"
        "Use -- to pass all remaining arguments literally to the command.\n",
        prog, prog, prog, prog);
}

static int copy_string(char *dst, size_t size, const char *src)
{
    if (strlen(src) >= size) return -1;
    memcpy(dst, src, strlen(src) + 1);
    return 0;
}

static int valid_id(const char *id)
{
    size_t len = strnlen(id, CONTAINER_ID_LEN);
    if (!len || len == CONTAINER_ID_LEN || !isalnum((unsigned char)id[0])) return 0;
    for (size_t i = 1; i < len; i++)
        if (!isalnum((unsigned char)id[i]) && id[i] != '_' && id[i] != '-') return 0;
    return 1;
}

static int validate_request(control_request_t *req)
{
    if (req->kind < CMD_START || req->kind > CMD_STOP) return -1;
    if (req->kind == CMD_PS) return 0;
    if (!valid_id(req->container_id)) return -1;
    if (req->kind != CMD_START && req->kind != CMD_RUN) return 0;
    if (!memchr(req->rootfs, 0, sizeof(req->rootfs)) ||
        !memchr(req->command, 0, sizeof(req->command)) || req->command[0] != '/' ||
        req->arg_count < 0 || req->arg_count > MAX_ARGS ||
        req->nice_value < -20 || req->nice_value > 19 ||
        !req->soft_limit_bytes || req->soft_limit_bytes >= req->hard_limit_bytes) return -1;
    for (int i = 0; i < req->arg_count; i++)
        if (!memchr(req->args[i], 0, sizeof(req->args[i]))) return -1;
    char resolved[PATH_MAX];
    struct stat st;
    if (!realpath(req->rootfs, resolved) || strcmp(resolved, "/") == 0 ||
        stat(resolved, &st) != 0 || !S_ISDIR(st.st_mode)) return -1;
    return copy_string(req->rootfs, sizeof(req->rootfs), resolved);
}

/* Fixed total deadline prevents a truncated/trickled request blocking the event loop. */
static int socket_transfer(int fd, void *data, size_t size, int sending, int timeout)
{
    size_t off = 0;
    int64_t deadline = monotonic_ms() + timeout;
    while (off < size) {
        int64_t left = deadline - monotonic_ms();
        if (left <= 0) { errno = ETIMEDOUT; return -1; }
        struct pollfd pfd = {fd, sending ? POLLOUT : POLLIN, 0};
        int r = poll(&pfd, 1, (int)left);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) { if (!r) errno = ETIMEDOUT; return -1; }
        ssize_t n = sending
            ? send(fd, (char *)data + off, size - off, MSG_DONTWAIT | MSG_NOSIGNAL)
            : recv(fd, (char *)data + off, size - off, MSG_DONTWAIT);
        if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
        if (n <= 0) { if (!n) errno = ECONNRESET; return -1; }
        off += (size_t)n;
    }
    return 0;
}

static int send_response(int fd, int status, const void *data, size_t length, int more)
{
    control_response_t response = {.status = status, .length = (unsigned int)length, .more = more};
    if (length > sizeof(response.message)) return -1;
    memcpy(response.message, data, length);
    return socket_transfer(fd, &response, sizeof(response), 1, IPC_TIMEOUT_MS);
}

static int reply(int fd, int status, const char *message)
{
    return send_response(fd, status, message, strlen(message), 0);
}

static int bb_init(bounded_buffer_t *b)
{
    memset(b, 0, sizeof(*b));
    int r = pthread_mutex_init(&b->mutex, NULL);
    if (r) return r;
    if ((r = pthread_cond_init(&b->not_empty, NULL))) goto mutex;
    if ((r = pthread_cond_init(&b->not_full, NULL))) goto empty;
    if ((r = pthread_cond_init(&b->drained, NULL))) goto full;
    return 0;
full: pthread_cond_destroy(&b->not_full);
empty: pthread_cond_destroy(&b->not_empty);
mutex: pthread_mutex_destroy(&b->mutex);
    return r;
}

static void bb_destroy(bounded_buffer_t *b)
{
    pthread_cond_destroy(&b->drained);
    pthread_cond_destroy(&b->not_full);
    pthread_cond_destroy(&b->not_empty);
    pthread_mutex_destroy(&b->mutex);
}

static void bb_push(bounded_buffer_t *b, const log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);
    while (b->count == LOG_BUFFER_CAPACITY)
        pthread_cond_wait(&b->not_full, &b->mutex);
    b->items[b->tail] = *item;
    b->tail = (b->tail + 1) % LOG_BUFFER_CAPACITY;
    b->count++;
    pthread_cond_signal(&b->not_empty);
    pthread_mutex_unlock(&b->mutex);
}

static void *consumer_thread_fn(void *arg)
{
    bounded_buffer_t *b = arg;
    for (;;) {
        pthread_mutex_lock(&b->mutex);
        while (!b->count && !b->shutting_down)
            pthread_cond_wait(&b->not_empty, &b->mutex);
        if (!b->count) { pthread_mutex_unlock(&b->mutex); break; }
        log_item_t item = b->items[b->head];
        b->head = (b->head + 1) % LOG_BUFFER_CAPACITY;
        b->count--;
        pthread_cond_signal(&b->not_full);
        pthread_mutex_unlock(&b->mutex);

        container_record_t *c = item.container;
        if (!item.length) {
            if (close(c->log_fd) != 0) c->log_error = 1;
            c->log_fd = -1;
            pthread_mutex_lock(&b->mutex);
            c->logs_done = 1;
            pthread_cond_broadcast(&b->drained);
            pthread_mutex_unlock(&b->mutex);
            continue;
        }
        size_t off = 0;
        while (off < item.length) {
            ssize_t n = write(c->log_fd, item.data + off, item.length - off);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) { c->log_error = 1; break; }
            off += (size_t)n;
        }
    }
    return NULL;
}

static void *producer_thread_fn(void *arg)
{
    producer_args_t *pa = arg;
    log_item_t item = {.container = pa->container};
    for (;;) {
        ssize_t n = read(pa->pipe_fd, item.data, sizeof(item.data));
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break;
        item.length = (size_t)n;
        bb_push(pa->buffer, &item);
    }
    close(pa->pipe_fd);
    item.length = 0;
    bb_push(pa->buffer, &item);
    free(pa);
    return NULL;
}

static void finish_logs(supervisor_ctx_t *ctx, container_record_t *c)
{
    if (!c->producer_started) return;
    pthread_join(c->producer, NULL);
    c->producer_started = 0;
    pthread_mutex_lock(&ctx->buffer.mutex);
    while (!c->logs_done)
        pthread_cond_wait(&ctx->buffer.drained, &ctx->buffer.mutex);
    pthread_mutex_unlock(&ctx->buffer.mutex);
    if (c->log_error) fprintf(stderr, "[supervisor] log write failed for %s\n", c->id);
}

static int child_error(const char *message, size_t length, int status)
{
    /* clone() runs alongside logging threads: avoid stdio/malloc locks here. */
    ssize_t ignored = write(STDERR_FILENO, message, length);
    (void)ignored;
    return status;
}

#define CHILD_ERROR(message, status) child_error(message "\n", sizeof(message "\n") - 1, status)

static int child_fn(void *arg)
{
    child_config_t *cfg = arg;
    control_request_t *req = &cfg->request;
    struct sigaction sa = {.sa_handler = SIG_DFL};
    sigemptyset(&sa.sa_mask);
    sigaction(SIGCHLD, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGPIPE, &sa, NULL);
    sigprocmask(SIG_SETMASK, &sa.sa_mask, NULL);
    if (dup2(cfg->log_fd, STDOUT_FILENO) < 0 || dup2(cfg->log_fd, STDERR_FILENO) < 0) return 1;
    int input = open("/dev/null", O_RDONLY);
    if (input < 0 || dup2(input, STDIN_FILENO) < 0) return 1;
    /* No socket, host directory, monitor, or other container's pipe may survive. */
#ifdef SYS_close_range
    if (syscall(SYS_close_range, 3U, ~0U, 0) != 0)
#endif
        for (int fd = 3; fd < cfg->max_fd; fd++) close(fd);

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0 ||
        sethostname(req->container_id, strlen(req->container_id)) != 0 ||
        chroot(req->rootfs) != 0 || chdir("/") != 0) {
        return CHILD_ERROR("child: namespace/rootfs setup failed", 1);
    }
    if ((mkdir("/proc", 0555) != 0 && errno != EEXIST) ||
        mount("proc", "/proc", "proc", MS_NOSUID | MS_NODEV | MS_NOEXEC, NULL) != 0) {
        return CHILD_ERROR("child: mount /proc failed", 1);
    }
    if (setpriority(PRIO_PROCESS, 0, req->nice_value) != 0) {
        return CHILD_ERROR("child: setpriority failed", 1);
    }
    char *argv[MAX_ARGS + 2];
    argv[0] = req->command;
    for (int i = 0; i < req->arg_count; i++) argv[i + 1] = req->args[i];
    argv[req->arg_count + 1] = NULL;
    execv(req->command, argv);
    return CHILD_ERROR("child: execv failed (check command, permissions and dynamic loader)", 127);
}

static int monitor_register(int fd, const control_request_t *req, pid_t pid)
{
    if (fd < 0) return 0;
    struct monitor_request mr = {.pid = pid, .soft_limit_bytes = req->soft_limit_bytes,
                                 .hard_limit_bytes = req->hard_limit_bytes};
    memcpy(mr.container_id, req->container_id, sizeof(mr.container_id));
    return ioctl(fd, MONITOR_REGISTER, &mr);
}

static void monitor_unregister(int fd, pid_t pid)
{
    if (fd < 0) return;
    struct monitor_request mr = {.pid = pid};
    if (ioctl(fd, MONITOR_UNREGISTER, &mr) != 0 && errno != ENOENT)
        perror("monitor unregister");
}

static container_record_t *find_container(supervisor_ctx_t *ctx, const char *id)
{
    for (container_record_t *c = ctx->containers; c; c = c->next)
        if (!strcmp(c->id, id)) return c;
    return NULL;
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        for (container_record_t *c = ctx->containers; c; c = c->next) {
            if (c->host_pid != pid || c->state != CONTAINER_RUNNING) continue;
            c->exit_signal = WIFSIGNALED(status) ? WTERMSIG(status) : 0;
            c->exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + c->exit_signal;
            c->state = c->stop_requested ? CONTAINER_STOPPED :
                (c->exit_signal == SIGKILL ? CONTAINER_KILLED : CONTAINER_EXITED);
            monitor_unregister(ctx->monitor_fd, pid);
            finish_logs(ctx, c);
            if (c->run_client_fd >= 0) {
                char message[256];
                snprintf(message, sizeof(message), "Container %s exited: code=%d signal=%d state=%s\n",
                         c->id, c->exit_code, c->exit_signal, state_to_str(c->state));
                reply(c->run_client_fd, c->exit_code, message);
                close(c->run_client_fd);
                c->run_client_fd = -1;
            }
            fprintf(stderr, "[supervisor] container %s (pid %d) exited: %s\n",
                    c->id, pid, state_to_str(c->state));
            break;
        }
    }
}

static container_record_t *launch_container(supervisor_ctx_t *ctx, const control_request_t *req,
                                             char *error, size_t error_size)
{
    struct stat st;
    if (stat(req->rootfs, &st) != 0) { snprintf(error, error_size, "ERROR: rootfs unavailable\n"); return NULL; }
    if (find_container(ctx, req->container_id)) {
        snprintf(error, error_size, "ERROR: container id already exists\n"); return NULL;
    }
    if (ctx->count >= MAX_CONTAINERS) {
        snprintf(error, error_size, "ERROR: container history is full (%d); restart supervisor\n", MAX_CONTAINERS);
        return NULL;
    }
    for (container_record_t *c = ctx->containers; c; c = c->next) {
        if (c->state == CONTAINER_RUNNING && c->root_dev == st.st_dev && c->root_ino == st.st_ino) {
            snprintf(error, error_size, "ERROR: rootfs already in use by container '%s'\n", c->id); return NULL;
        }
    }
    snprintf(error, error_size, "ERROR: failed to start container '%s'\n", req->container_id);
    int pipefd[2];
    if (pipe2(pipefd, O_CLOEXEC) != 0) return NULL;
    container_record_t *c = calloc(1, sizeof(*c));
    child_config_t *cfg = calloc(1, sizeof(*cfg));
    producer_args_t *pa = calloc(1, sizeof(*pa));
    char *stack = malloc(STACK_SIZE);
    if (!c || !cfg || !pa || !stack) goto fail_alloc;
    memcpy(c->id, req->container_id, sizeof(c->id));
    c->root_dev = st.st_dev; c->root_ino = st.st_ino;
    c->run_client_fd = -1;
    c->soft_limit_bytes = req->soft_limit_bytes;
    c->hard_limit_bytes = req->hard_limit_bytes;
    char name[CONTAINER_ID_LEN + 5];
    snprintf(name, sizeof(name), "%s.log", c->id);
    c->log_fd = openat(ctx->log_dir_fd, name, O_CREAT | O_WRONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, 0600);
    if (c->log_fd < 0) goto fail_alloc;
    if (fstat(c->log_fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_nlink != 1 ||
        ftruncate(c->log_fd, 0) != 0) goto fail_log;
    cfg->request = *req;
    cfg->log_fd = pipefd[1];
    long max_fd = sysconf(_SC_OPEN_MAX);
    cfg->max_fd = max_fd > 0 && max_fd <= INT_MAX ? (int)max_fd : 65536;
    pid_t pid = clone(child_fn, stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | CLONE_NEWIPC | SIGCHLD, cfg);
    if (pid < 0) { perror("launch: clone"); goto fail_log; }
    close(pipefd[1]); pipefd[1] = -1;
    /* clone without CLONE_VM gives the child private copies of stack and config. */
    free(stack); stack = NULL;
    free(cfg); cfg = NULL;
    c->host_pid = pid;
    c->started_at = time(NULL);
    c->state = CONTAINER_RUNNING;
    if (monitor_register(ctx->monitor_fd, req, pid) != 0 && errno != ESRCH) {
        perror("monitor register"); goto fail_child;
    }
    pa->container = c; pa->pipe_fd = pipefd[0]; pa->buffer = &ctx->buffer;
    int r = pthread_create(&c->producer, NULL, producer_thread_fn, pa);
    if (r) { errno = r; perror("launch: producer"); goto fail_child; }
    c->producer_started = 1;
    c->next = ctx->containers; ctx->containers = c; ctx->count++;
    fprintf(stderr, "[supervisor] started container %s pid=%d\n", c->id, pid);
    return c;
fail_child:
    kill(pid, SIGKILL);
    while (waitpid(pid, NULL, 0) < 0 && errno == EINTR) {}
    monitor_unregister(ctx->monitor_fd, pid);
fail_log:
    close(c->log_fd);
fail_alloc:
    close(pipefd[0]);
    if (pipefd[1] >= 0) close(pipefd[1]);
    free(c); free(cfg); free(pa); free(stack);
    return NULL;
}

static void handle_ps(supervisor_ctx_t *ctx, int fd)
{
    if (!ctx->containers) { reply(fd, 0, "No containers\n"); return; }
    const char *header = "ID                   PID      STATE      STARTED             SOFT(MiB) HARD(MiB)\n";
    if (send_response(fd, 0, header, strlen(header), 1) != 0) return;
    for (container_record_t *c = ctx->containers; c; c = c->next) {
        char line[256], timestamp[32];
        struct tm tm;
        localtime_r(&c->started_at, &tm);
        strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &tm);
        int len = snprintf(line, sizeof(line), "%-20s %-8d %-10s %s %-9lu %-9lu\n",
                           c->id, c->host_pid, state_to_str(c->state), timestamp,
                           c->soft_limit_bytes >> 20, c->hard_limit_bytes >> 20);
        if (send_response(fd, 0, line, (size_t)len, c->next != NULL) != 0) return;
    }
}

static void handle_logs(supervisor_ctx_t *ctx, const char *id, int client)
{
    char name[CONTAINER_ID_LEN + 5];
    snprintf(name, sizeof(name), "%s.log", id);
    int fd = openat(ctx->log_dir_fd, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
        if (fd >= 0) close(fd);
        reply(client, 1, "ERROR: no readable log file\n"); return;
    }
    /* Snapshot size so a busy writer cannot make a logs request run forever. */
    off_t remaining = st.st_size;
    char buf[RESPONSE_MAX];
    while (remaining > 0) {
        size_t size = remaining > (off_t)sizeof(buf) ? sizeof(buf) : (size_t)remaining;
        ssize_t n = read(fd, buf, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { reply(client, 1, "ERROR: log read failed\n"); close(fd); return; }
        if (send_response(client, 0, buf, (size_t)n, 1) != 0) { close(fd); return; }
        remaining -= n;
    }
    close(fd);
    reply(client, 0, "");
}

static void signal_handler(int sig)
{
    int saved = errno;
    if (sig != SIGCHLD) should_stop = 1;
    char byte = 1;
    ssize_t ignored = write(signal_pipe[1], &byte, 1);
    (void)ignored;
    errno = saved;
}

static void request_stop(container_record_t *c)
{
    if (c->stop_requested) return;
    c->stop_requested = 1;
    c->stop_deadline = monotonic_ms() + 2000;
    if (kill(c->host_pid, SIGTERM) != 0 && errno != ESRCH) perror("stop: SIGTERM");
}

static void enforce_stops(supervisor_ctx_t *ctx)
{
    int64_t now = monotonic_ms();
    for (container_record_t *c = ctx->containers; c; c = c->next)
        if (c->state == CONTAINER_RUNNING && c->stop_requested && c->stop_deadline <= now)
            if (kill(c->host_pid, SIGKILL) != 0 && errno != ESRCH) perror("stop: SIGKILL");
}

static int runtime_paths(char *directory, size_t size, char *socket_path, size_t socket_size)
{
    const char *dir = getenv("MINI_RUNTIME_DIR");
    if (!dir) dir = "/run/mini-runtime";
    if (dir[0] != '/' || copy_string(directory, size, dir) != 0) return -1;
    int n = snprintf(socket_path, socket_size, "%s/control.sock", dir);
    return n < 0 || (size_t)n >= socket_size ? -1 : 0;
}

static int prepare_runtime(supervisor_ctx_t *ctx)
{
    char directory[PATH_MAX];
    if (runtime_paths(directory, sizeof(directory), ctx->socket_path, sizeof(ctx->socket_path)) != 0) {
        fprintf(stderr, "MINI_RUNTIME_DIR must be an absolute path short enough for a Unix socket\n"); return -1;
    }
    if (mkdir(directory, 0700) != 0 && errno != EEXIST) return -1;
    int dir = open(directory, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (dir < 0) return -1;
    struct stat st;
    if (fstat(dir, &st) != 0 || st.st_uid != geteuid() || (st.st_mode & 077) != 0) {
        fprintf(stderr, "Runtime directory must be owned by the supervisor user and have mode 0700\n");
        close(dir); return -1;
    }
    ctx->lock_fd = openat(dir, "supervisor.lock", O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, 0600);
    if (ctx->lock_fd < 0 || fstat(ctx->lock_fd, &st) != 0 || !S_ISREG(st.st_mode) ||
        flock(ctx->lock_fd, LOCK_EX | LOCK_NB) != 0) {
        fprintf(stderr, "Cannot lock runtime directory (another supervisor may be running)\n");
        close(dir); return -1;
    }
    if (mkdirat(dir, "logs", 0700) != 0 && errno != EEXIST) { close(dir); return -1; }
    ctx->log_dir_fd = openat(dir, "logs", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (ctx->log_dir_fd < 0 || fstat(ctx->log_dir_fd, &st) != 0 || st.st_uid != geteuid() || (st.st_mode & 077) != 0) {
        close(dir); return -1;
    }
    /* Only the lock holder may remove a stale socket. Never unlink arbitrary files. */
    if (fstatat(dir, "control.sock", &st, AT_SYMLINK_NOFOLLOW) == 0) {
        if (!S_ISSOCK(st.st_mode) || unlinkat(dir, "control.sock", 0) != 0) { close(dir); return -1; }
    } else if (errno != ENOENT) { close(dir); return -1; }
    close(dir);
    ctx->server_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (ctx->server_fd < 0) return -1;
    struct sockaddr_un address = {.sun_family = AF_UNIX};
    memcpy(address.sun_path, ctx->socket_path, strlen(ctx->socket_path) + 1);
    if (bind(ctx->server_fd, (struct sockaddr *)&address, sizeof(address)) != 0) return -1;
    if (listen(ctx->server_fd, 16) != 0) { unlink(ctx->socket_path); return -1; }
    return 0;
}

static void handle_client(supervisor_ctx_t *ctx, int fd)
{
    struct ucred peer;
    socklen_t peer_size = sizeof(peer);
    if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &peer, &peer_size) != 0 || peer.uid != geteuid()) {
        reply(fd, 1, "ERROR: unauthorized client\n"); close(fd); return;
    }
    control_request_t req;
    if (socket_transfer(fd, &req, sizeof(req), 0, IPC_TIMEOUT_MS) != 0) { close(fd); return; }
    if (validate_request(&req) != 0) { reply(fd, 1, "ERROR: invalid request\n"); close(fd); return; }
    reap_children(ctx);
    if (req.kind == CMD_START || req.kind == CMD_RUN) {
        char message[256];
        container_record_t *c = launch_container(ctx, &req, message, sizeof(message));
        if (!c) reply(fd, 1, message);
        else {
            snprintf(message, sizeof(message), "%s container '%s' (pid %d)\n",
                     req.kind == CMD_RUN ? "Running" : "Started", c->id, c->host_pid);
            if (send_response(fd, 0, message, strlen(message), req.kind == CMD_RUN) == 0 && req.kind == CMD_RUN) {
                c->run_client_fd = fd;
                return; /* Closed after exit, including during supervisor shutdown. */
            }
        }
    } else if (req.kind == CMD_PS) handle_ps(ctx, fd);
    else if (req.kind == CMD_LOGS) handle_logs(ctx, req.container_id, fd);
    else if (req.kind == CMD_STOP) {
        container_record_t *c = find_container(ctx, req.container_id);
        if (!c) reply(fd, 1, "ERROR: no such container\n");
        else if (c->state != CONTAINER_RUNNING) reply(fd, 1, "ERROR: container is not running\n");
        else { request_stop(c); reply(fd, 0, "Stop requested (SIGKILL after 2 seconds if needed)\n"); }
    }
    close(fd);
}

static int run_supervisor(void)
{
    if (geteuid() != 0) { fprintf(stderr, "The supervisor requires root\n"); return 1; }
    umask(0077);
    supervisor_ctx_t ctx = {.server_fd = -1, .monitor_fd = -1, .log_dir_fd = -1, .lock_fd = -1};
    if (prepare_runtime(&ctx) != 0) {
        perror("prepare runtime");
        if (ctx.server_fd >= 0) close(ctx.server_fd);
        if (ctx.log_dir_fd >= 0) close(ctx.log_dir_fd);
        if (ctx.lock_fd >= 0) close(ctx.lock_fd);
        return 1;
    }
    int result = 1;
    if (pipe2(signal_pipe, O_NONBLOCK | O_CLOEXEC) != 0) goto cleanup;
    struct sigaction sa = {.sa_handler = signal_handler, .sa_flags = SA_NOCLDSTOP};
    sigemptyset(&sa.sa_mask);
    sigaction(SIGCHLD, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    int r = bb_init(&ctx.buffer);
    if (r) { errno = r; perror("log buffer"); goto cleanup; }
    r = pthread_create(&ctx.consumer, NULL, consumer_thread_fn, &ctx.buffer);
    if (r) { errno = r; perror("log consumer"); bb_destroy(&ctx.buffer); goto cleanup; }
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR | O_CLOEXEC);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] kernel monitor unavailable; memory limits are NOT enforced\n");
    fprintf(stderr, "[supervisor] listening on %s\n", ctx.socket_path);
    result = 0;
    while (!should_stop) {
        reap_children(&ctx);
        enforce_stops(&ctx);
        struct pollfd fds[] = {{ctx.server_fd, POLLIN, 0}, {signal_pipe[0], POLLIN, 0}};
        r = poll(fds, 2, 100);
        if (r < 0) { if (errno == EINTR) continue; perror("poll"); result = 1; break; }
        if (fds[1].revents & POLLIN) { char buf[128]; while (read(signal_pipe[0], buf, sizeof(buf)) > 0) {} }
        if (!should_stop && (fds[0].revents & POLLIN)) {
            int client = accept4(ctx.server_fd, NULL, NULL, SOCK_CLOEXEC);
            if (client >= 0) handle_client(&ctx, client);
            else if (errno != EINTR) perror("accept");
        }
    }
    for (container_record_t *c = ctx.containers; c; c = c->next)
        if (c->state == CONTAINER_RUNNING) request_stop(c);
    for (;;) {
        reap_children(&ctx);
        int running = 0;
        for (container_record_t *c = ctx.containers; c; c = c->next)
            if (c->state == CONTAINER_RUNNING) running++;
        if (!running) break;
        enforce_stops(&ctx);
        struct timespec delay = {0, 10000000};
        nanosleep(&delay, NULL);
    }
    /* All producers have joined and their EOF markers have been consumed. */
    pthread_mutex_lock(&ctx.buffer.mutex);
    ctx.buffer.shutting_down = 1;
    pthread_cond_signal(&ctx.buffer.not_empty);
    pthread_mutex_unlock(&ctx.buffer.mutex);
    pthread_join(ctx.consumer, NULL);
    bb_destroy(&ctx.buffer);
    while (ctx.containers) {
        container_record_t *next = ctx.containers->next;
        free(ctx.containers);
        ctx.containers = next;
    }
cleanup:
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    close(ctx.server_fd);
    unlink(ctx.socket_path);
    close(ctx.log_dir_fd);
    if (signal_pipe[0] >= 0) { close(signal_pipe[0]); close(signal_pipe[1]); }
    close(ctx.lock_fd);
    return result;
}

static int send_request(control_request_t *req)
{
    if (validate_request(req) != 0) {
        fprintf(stderr, "Invalid request: check ID, rootfs directory, absolute command, arguments and limits (0 < soft < hard)\n");
        return 1;
    }
    struct sockaddr_un address = {.sun_family = AF_UNIX};
    char directory[PATH_MAX];
    if (runtime_paths(directory, sizeof(directory), address.sun_path, sizeof(address.sun_path)) != 0) return 1;
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) { perror("socket"); return 1; }
    if (connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
        perror("connect (is the supervisor running?)"); close(fd); return 1;
    }
    if (socket_transfer(fd, req, sizeof(*req), 1, IPC_TIMEOUT_MS) != 0) {
        perror("send request"); close(fd); return 1;
    }
    int status = 1;
    for (;;) {
        /* Waiting for a run to finish is unbounded, but each frame must arrive whole. */
        struct pollfd pfd = {fd, POLLIN, 0};
        int r;
        do { r = poll(&pfd, 1, -1); } while (r < 0 && errno == EINTR);
        control_response_t response;
        if (r < 0 || socket_transfer(fd, &response, sizeof(response), 0, IPC_TIMEOUT_MS) != 0) {
            perror("read response"); status = 1; break;
        }
        if (response.length > sizeof(response.message) || (response.more != 0 && response.more != 1) ||
            response.status < 0 || response.status > 255) {
            fprintf(stderr, "Invalid supervisor response\n"); status = 1; break;
        }
        if (fwrite(response.message, 1, response.length, stdout) != response.length || fflush(stdout) != 0) {
            status = 1; break;
        }
        status = response.status;
        if (!response.more) break;
    }
    close(fd);
    return status;
}

static int parse_mib_flag(const char *value, unsigned long *out)
{
    if (!value[0] || !isdigit((unsigned char)value[0])) return -1;
    char *end;
    errno = 0;
    unsigned long mib = strtoul(value, &end, 10);
    if (errno || *end || !mib || mib > (ULONG_MAX >> 20)) return -1;
    *out = mib << 20;
    return 0;
}

static int parse_args_and_flags(control_request_t *req, int argc, char *argv[], int start)
{
    int literal = 0;
    for (int i = start; i < argc; i++) {
        const char *arg = argv[i];
        if (!literal && !strcmp(arg, "--")) { literal = 1; continue; }
        if (!literal && (!strcmp(arg, "--soft-mib") || !strcmp(arg, "--hard-mib") || !strcmp(arg, "--nice"))) {
            if (++i >= argc) return -1;
            if (!strcmp(arg, "--nice")) {
                char *end;
                errno = 0;
                long n = strtol(argv[i], &end, 10);
                if (errno || end == argv[i] || *end || n < -20 || n > 19) return -1;
                req->nice_value = (int)n;
            } else if (parse_mib_flag(argv[i], !strcmp(arg, "--soft-mib") ?
                                      &req->soft_limit_bytes : &req->hard_limit_bytes) != 0) return -1;
        } else {
            if (req->arg_count == MAX_ARGS || copy_string(req->args[req->arg_count], PATH_MAX, arg) != 0) return -1;
            req->arg_count++;
        }
    }
    return 0;
}

int main(int argc, char *argv[])
{
    if (argc == 2 && (!strcmp(argv[1], "--help") || !strcmp(argv[1], "-h"))) { usage(argv[0]); return 0; }
    if (argc < 2) { usage(argv[0]); return 1; }
    if (!strcmp(argv[1], "supervisor")) {
        if (argc > 3) { usage(argv[0]); return 1; }
        /* The old base-rootfs argument is accepted for command-line compatibility. */
        return run_supervisor();
    }
    control_request_t req = {.soft_limit_bytes = DEFAULT_SOFT_MIB << 20,
                             .hard_limit_bytes = DEFAULT_HARD_MIB << 20};
    if (!strcmp(argv[1], "start") || !strcmp(argv[1], "run")) {
        req.kind = !strcmp(argv[1], "start") ? CMD_START : CMD_RUN;
        if (argc < 5 || copy_string(req.container_id, sizeof(req.container_id), argv[2]) != 0 ||
            copy_string(req.rootfs, sizeof(req.rootfs), argv[3]) != 0 ||
            copy_string(req.command, sizeof(req.command), argv[4]) != 0 ||
            parse_args_and_flags(&req, argc, argv, 5) != 0) {
            fprintf(stderr, "Invalid or excessive arguments/options\n"); usage(argv[0]); return 1;
        }
    } else if (!strcmp(argv[1], "ps") && argc == 2) req.kind = CMD_PS;
    else if ((!strcmp(argv[1], "logs") || !strcmp(argv[1], "stop")) && argc == 3) {
        req.kind = !strcmp(argv[1], "logs") ? CMD_LOGS : CMD_STOP;
        if (copy_string(req.container_id, sizeof(req.container_id), argv[2]) != 0) return 1;
    } else { usage(argv[0]); return 1; }
    return send_request(&req);
}
