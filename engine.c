/*
 * engine.c — Supervised Multi-Container Runtime (User Space)
 *
 * Architecture:
 *   - Single binary used in two modes:
 *       supervisor: long-running daemon, owns all containers + logging pipeline
 *       CLI client: short-lived, sends one command over UNIX socket, prints response, exits
 *
 *   - IPC Path A (logging): container stdout/stderr → pipe → producer thread → bounded buffer → consumer thread → log file
 *   - IPC Path B (control): CLI → UNIX domain socket → supervisor → response
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* ─────────────────────────── constants ─────────────────────────── */

#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    64
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define LOG_DIR             "logs"
#define LOG_CHUNK_SIZE      4096
#define LOG_BUFFER_CAPACITY 64
#define DEFAULT_SOFT_MIB    40UL
#define DEFAULT_HARD_MIB    64UL
#define MAX_CONTAINERS      64
#define RESPONSE_MAX        8192

/* ─────────────────────────── enumerations ───────────────────────── */

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,       /* killed by stop command (stop_requested set)   */
    CONTAINER_KILLED,        /* killed by hard memory limit (stop_requested=0) */
    CONTAINER_EXITED         /* exited normally on its own                     */
} container_state_t;

/* ─────────────────────────── data structures ────────────────────── */

/*
 * Per-container metadata.
 * Protected by supervisor_ctx_t.metadata_lock for all concurrent access.
 */
typedef struct container_record {
    char                   id[CONTAINER_ID_LEN];
    char                   rootfs[PATH_MAX];
    pid_t                  host_pid;
    time_t                 started_at;
    container_state_t      state;
    unsigned long          soft_limit_bytes;
    unsigned long          hard_limit_bytes;
    int                    nice_value;
    int                    exit_code;
    int                    exit_signal;
    int                    stop_requested;   /* set before kill() in stop cmd  */
    char                   log_path[PATH_MAX];
    int                    run_client_fd;    /* -1 unless a 'run' client waits */
    struct container_record *next;
} container_record_t;

/* One item in the bounded logging buffer */
typedef struct {
    char   container_id[CONTAINER_ID_LEN];
    size_t length;
    char   data[LOG_CHUNK_SIZE];
} log_item_t;

/* Bounded buffer shared between producer threads and consumer thread */
typedef struct {
    log_item_t      items[LOG_BUFFER_CAPACITY];
    size_t          head;
    size_t          tail;
    size_t          count;
    int             shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} bounded_buffer_t;

/* Arguments passed to each per-container producer thread */
typedef struct {
    char              container_id[CONTAINER_ID_LEN];
    int               pipe_read_fd;    /* read-end of container's stdout/stderr pipe */
    bounded_buffer_t *buffer;
} producer_args_t;

/* What the supervisor passes into the clone()-d child */
typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[PATH_MAX];   /* full path inside the container */
    char args[8][PATH_MAX];   /* argv[1..] for execv, NULL-terminated by count */
    int  arg_count;
    int  nice_value;
    int  log_write_fd;        /* write-end of the logging pipe */
} child_config_t;

/* Parsed CLI request sent from client to supervisor over the UNIX socket */
typedef struct {
    command_kind_t kind;
    char           container_id[CONTAINER_ID_LEN];
    char           rootfs[PATH_MAX];
    char           command[PATH_MAX];
    char           args[8][PATH_MAX];
    int            arg_count;
    unsigned long  soft_limit_bytes;
    unsigned long  hard_limit_bytes;
    int            nice_value;
} control_request_t;

/* Response sent back from supervisor to CLI client */
typedef struct {
    int  status;                  /* 0 = ok, non-zero = error */
    char message[RESPONSE_MAX];
} control_response_t;

/* Global supervisor state — one instance, lives for the daemon's lifetime */
typedef struct {
    int                server_fd;
    int                monitor_fd;
    volatile int       should_stop;

    /* logging pipeline */
    pthread_t          consumer_thread;
    bounded_buffer_t   log_buffer;

    /* container registry */
    pthread_mutex_t    metadata_lock;
    container_record_t *containers;      /* linked list, guarded by metadata_lock */
} supervisor_ctx_t;

/* Global pointer so signal handlers can reach the supervisor */
static supervisor_ctx_t *g_ctx = NULL;

/* ─────────────────────────── helpers ────────────────────────────── */

static const char *state_to_str(container_state_t s)
{
    switch (s) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "hard_limit_killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage:\n"
        "  %s supervisor <base-rootfs>\n"
        "  %s start <id> <rootfs> <command> [args...] [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s run   <id> <rootfs> <command> [args...] [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s ps\n"
        "  %s logs <id>\n"
        "  %s stop <id>\n",
        prog, prog, prog, prog, prog, prog);
}

/* ─────────────────── bounded buffer ────────────────────────────── */

static int bb_init(bounded_buffer_t *b)
{
    memset(b, 0, sizeof(*b));
    int r;
    if ((r = pthread_mutex_init(&b->mutex, NULL))   != 0) return r;
    if ((r = pthread_cond_init(&b->not_empty, NULL)) != 0) {
        pthread_mutex_destroy(&b->mutex); return r;
    }
    if ((r = pthread_cond_init(&b->not_full, NULL))  != 0) {
        pthread_cond_destroy(&b->not_empty);
        pthread_mutex_destroy(&b->mutex); return r;
    }
    return 0;
}

static void bb_destroy(bounded_buffer_t *b)
{
    pthread_cond_destroy(&b->not_full);
    pthread_cond_destroy(&b->not_empty);
    pthread_mutex_destroy(&b->mutex);
}

static void bb_shutdown(bounded_buffer_t *b)
{
    pthread_mutex_lock(&b->mutex);
    b->shutting_down = 1;
    pthread_cond_broadcast(&b->not_empty);
    pthread_cond_broadcast(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
}

/*
 * Push one log item into the buffer.
 * Blocks if the buffer is full (producer-consumer backpressure).
 * Returns 0 on success, -1 if shutdown was requested.
 */
static int bb_push(bounded_buffer_t *b, const log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);

    while (b->count == LOG_BUFFER_CAPACITY && !b->shutting_down)
        pthread_cond_wait(&b->not_full, &b->mutex);

    if (b->shutting_down) {
        pthread_mutex_unlock(&b->mutex);
        return -1;
    }

    b->items[b->tail] = *item;
    b->tail = (b->tail + 1) % LOG_BUFFER_CAPACITY;
    b->count++;

    pthread_cond_signal(&b->not_empty);
    pthread_mutex_unlock(&b->mutex);
    return 0;
}

/*
 * Pop one log item from the buffer.
 * Blocks while empty; drains remaining items even after shutdown begins.
 * Returns 0 on success, -1 when shutdown and buffer is empty.
 */
static int bb_pop(bounded_buffer_t *b, log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);

    /* Wait while empty AND not yet shutting down */
    while (b->count == 0 && !b->shutting_down)
        pthread_cond_wait(&b->not_empty, &b->mutex);

    /* Even after shutdown starts, drain whatever is left */
    if (b->count == 0) {
        pthread_mutex_unlock(&b->mutex);
        return -1;   /* truly empty + shutdown */
    }

    *item = b->items[b->head];
    b->head = (b->head + 1) % LOG_BUFFER_CAPACITY;
    b->count--;

    pthread_cond_signal(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
    return 0;
}

/* ─────────────────── consumer thread ───────────────────────────── */

/*
 * Single consumer thread: pops items from the shared buffer and appends
 * them to the correct per-container log file. Exits only when the buffer
 * is shut down AND fully drained.
 */
static void *consumer_thread_fn(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (bb_pop(&ctx->log_buffer, &item) == 0) {
        /* Build the log file path */
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd >= 0) {
            /* Write the full chunk; retry on partial write */
            size_t written = 0;
            while (written < item.length) {
                ssize_t w = write(fd, item.data + written, item.length - written);
                if (w < 0) {
                    if (errno == EINTR) continue;
                    perror("consumer: log write");
                    break;
                }
                written += (size_t)w;
            }
            close(fd);
        }
    }

    return NULL;
}

/* ─────────────────── producer thread ───────────────────────────── */

/*
 * One producer thread per container.
 * Reads from the container's stdout/stderr pipe and pushes chunks into
 * the bounded buffer. Exits when the pipe's write-end is closed (EOF).
 */
static void *producer_thread_fn(void *arg)
{
    producer_args_t *pa = (producer_args_t *)arg;
    char buf[LOG_CHUNK_SIZE];
    ssize_t n;

    while ((n = read(pa->pipe_read_fd, buf, sizeof(buf))) > 0) {
        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, pa->container_id, CONTAINER_ID_LEN - 1);
        memcpy(item.data, buf, (size_t)n);
        item.length = (size_t)n;

        /* If the buffer is shutting down, give up */
        if (bb_push(pa->buffer, &item) != 0)
            break;
    }

    close(pa->pipe_read_fd);
    free(pa);
    return NULL;
}

/* ─────────────────── container child entry point ────────────────── */

/*
 * Executed inside the clone()-d child process.
 * Runs in isolated PID / UTS / mount namespaces.
 */
static int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Set the container's hostname */
    if (sethostname(cfg->id, strlen(cfg->id)) != 0)
        perror("sethostname");

    /* Pivot into the container's root filesystem */
    if (chroot(cfg->rootfs) != 0) {
        perror("child: chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("child: chdir");
        return 1;
    }

    /* Mount /proc so tools like ps work inside the container */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0)
        perror("child: mount /proc"); /* non-fatal; continue */

    /* Apply nice value if requested */
    if (cfg->nice_value != 0) {
        errno = 0;
        if (nice(cfg->nice_value) == -1 && errno != 0)
            perror("nice");
    }

    /* Redirect stdout and stderr to the logging pipe */
    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("child: dup2");
        return 1;
    }
    close(cfg->log_write_fd);

    /* Build argv for execv */
    char *argv[10];
    argv[0] = cfg->command;
    int i;
    for (i = 0; i < cfg->arg_count && i < 8; i++)
        argv[i + 1] = cfg->args[i];
    argv[i + 1] = NULL;

    execv(cfg->command, argv);
    perror("child: execv");
    return 1;
}

/* ─────────────────── kernel monitor helpers ─────────────────────── */

static void monitor_register(int fd, const char *id, pid_t pid,
                              unsigned long soft, unsigned long hard)
{
    if (fd < 0) return;
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = pid;
    req.soft_limit_bytes = soft;
    req.hard_limit_bytes = hard;
    strncpy(req.container_id, id, sizeof(req.container_id) - 1);
    if (ioctl(fd, MONITOR_REGISTER, &req) < 0)
        perror("monitor_register: ioctl");
}

static void monitor_unregister(int fd, const char *id, pid_t pid)
{
    if (fd < 0) return;
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = pid;
    strncpy(req.container_id, id, sizeof(req.container_id) - 1);
    if (ioctl(fd, MONITOR_UNREGISTER, &req) < 0)
        perror("monitor_unregister: ioctl");
}

/* ─────────────────── container metadata helpers ─────────────────── */

/* Caller must hold metadata_lock */
static container_record_t *find_container(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *c = ctx->containers;
    while (c) {
        if (strncmp(c->id, id, CONTAINER_ID_LEN) == 0)
            return c;
        c = c->next;
    }
    return NULL;
}

/* Caller must hold metadata_lock */
static container_record_t *find_container_by_pid(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *c = ctx->containers;
    while (c) {
        if (c->host_pid == pid)
            return c;
        c = c->next;
    }
    return NULL;
}

/* Allocate and prepend a new record; caller must hold metadata_lock */
static container_record_t *add_container(supervisor_ctx_t *ctx,
                                          const control_request_t *req,
                                          pid_t pid)
{
    container_record_t *c = calloc(1, sizeof(container_record_t));
    if (!c) return NULL;

    strncpy(c->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(c->rootfs, req->rootfs, PATH_MAX - 1);
    c->host_pid         = pid;
    c->started_at       = time(NULL);
    c->state            = CONTAINER_RUNNING;
    c->soft_limit_bytes = req->soft_limit_bytes;
    c->hard_limit_bytes = req->hard_limit_bytes;
    c->nice_value       = req->nice_value;
    c->exit_code        = -1;
    c->exit_signal      = 0;
    c->stop_requested   = 0;
    c->run_client_fd    = -1;
    snprintf(c->log_path, sizeof(c->log_path), "%s/%s.log", LOG_DIR, c->id);

    c->next       = ctx->containers;
    ctx->containers = c;
    return c;
}

/* ─────────────────── SIGCHLD handler ────────────────────────────── */

/*
 * We use a self-pipe trick: the signal handler writes one byte to a pipe,
 * and the main loop's select() detects it, then processes reaps safely
 * outside the signal handler context.
 */
static int sigchld_pipe[2] = {-1, -1};

static void sigchld_handler(int sig)
{
    (void)sig;
    char b = 1;
    write(sigchld_pipe[1], &b, 1);  /* async-signal-safe */
}

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx) g_ctx->should_stop = 1;
    /* Wake up select() by closing the server fd would be rude;
       instead we rely on EINTR from select() */
}

/*
 * Called from the main loop (not from signal handler) to safely reap
 * all exited children and update their metadata.
 */
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = find_container_by_pid(ctx, pid);
        if (c) {
            if (WIFEXITED(status)) {
                c->exit_code = WEXITSTATUS(status);
                c->exit_signal = 0;
                if (c->stop_requested)
                    c->state = CONTAINER_STOPPED;
                else
                    c->state = CONTAINER_EXITED;
            } else if (WIFSIGNALED(status)) {
                c->exit_signal = WTERMSIG(status);
                c->exit_code   = 128 + c->exit_signal;
                /*
                 * Grading rule (spec Task 4):
                 * hard_limit_killed only when SIGKILL AND stop_requested not set.
                 */
                if (c->exit_signal == SIGKILL && !c->stop_requested)
                    c->state = CONTAINER_KILLED;
                else
                    c->state = CONTAINER_STOPPED;
            }

            /* If a 'run' client is waiting, send the final exit code */
            if (c->run_client_fd >= 0) {
                control_response_t resp;
                memset(&resp, 0, sizeof(resp));
                resp.status = c->exit_code;
                snprintf(resp.message, sizeof(resp.message),
                         "Container %s exited: code=%d signal=%d state=%s\n",
                         c->id, c->exit_code, c->exit_signal,
                         state_to_str(c->state));
                /* Best-effort send; ignore errors */
                write(c->run_client_fd, &resp, sizeof(resp));
                close(c->run_client_fd);
                c->run_client_fd = -1;
            }

            monitor_unregister(ctx->monitor_fd, c->id, pid);
            fprintf(stderr, "[supervisor] container %s (pid %d) exited → %s\n",
                    c->id, pid, state_to_str(c->state));
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* ─────────────────── launch a container ────────────────────────── */

/*
 * Creates a new container (clone + namespaces), starts a producer thread,
 * registers with the kernel monitor, and records metadata.
 *
 * Returns the new container_record_t on success, NULL on error.
 * Caller must NOT hold metadata_lock.
 */
static container_record_t *launch_container(supervisor_ctx_t *ctx,
                                             const control_request_t *req)
{
    /* Check for ID collision */
    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        fprintf(stderr, "launch: container id '%s' already exists\n",
                req->container_id);
        return NULL;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
    pthread_mutex_lock(&ctx->metadata_lock);

    container_record_t *tmp = ctx->containers;
    while (tmp) {
        if ((strcmp(tmp->rootfs, req->rootfs) == 0) &&
            (tmp->state == CONTAINER_RUNNING || tmp->state == CONTAINER_STARTING)) {

            pthread_mutex_unlock(&ctx->metadata_lock);
            fprintf(stderr, "ERROR: rootfs already in use by container '%s'\n", tmp->id);
            return NULL;
        }
        tmp = tmp->next;
    }

    pthread_mutex_unlock(&ctx->metadata_lock);
    
    /* Create the logging pipe */
    int pipefd[2];
    if (pipe(pipefd) != 0) {
        perror("launch: pipe");
        return NULL;
    }

    /* Build the child config (heap-allocated; child reads it before exec) */
    child_config_t *cfg = calloc(1, sizeof(child_config_t));
    if (!cfg) { close(pipefd[0]); close(pipefd[1]); return NULL; }

    strncpy(cfg->id,      req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs,  req->rootfs,        PATH_MAX - 1);
    strncpy(cfg->command, req->command,        PATH_MAX - 1);
    cfg->nice_value   = req->nice_value;
    cfg->log_write_fd = pipefd[1];
    cfg->arg_count    = req->arg_count;
    int i;
    for (i = 0; i < req->arg_count && i < 8; i++)
        strncpy(cfg->args[i], req->args[i], PATH_MAX - 1);

    /* Allocate clone stack */
    char *stack = malloc(STACK_SIZE);
    if (!stack) { free(cfg); close(pipefd[0]); close(pipefd[1]); return NULL; }

    pid_t pid = clone(child_fn,
                      stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      cfg);
    if (pid < 0) {
        perror("launch: clone");
        free(stack); free(cfg);
        close(pipefd[0]); close(pipefd[1]);
        return NULL;
    }

    /* Parent does not need the write-end of the pipe */
    close(pipefd[1]);

    /* Record metadata */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = add_container(ctx, req, pid);
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (!rec) {
        /* We already cloned — kill the child and give up */
        kill(pid, SIGKILL);
        close(pipefd[0]);
        free(stack);
        free(cfg);
        return NULL;
    }

    /* Register with the kernel memory monitor */
    monitor_register(ctx->monitor_fd, req->container_id, pid,
                     req->soft_limit_bytes, req->hard_limit_bytes);

    /* Start the producer thread for this container's log pipe */
    producer_args_t *pa = calloc(1, sizeof(producer_args_t));
    if (pa) {
        strncpy(pa->container_id, req->container_id, CONTAINER_ID_LEN - 1);
        pa->pipe_read_fd = pipefd[0];
        pa->buffer       = &ctx->log_buffer;

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&tid, &attr, producer_thread_fn, pa) != 0) {
            perror("launch: pthread_create producer");
            close(pipefd[0]);
            free(pa);
        }
        pthread_attr_destroy(&attr);
    } else {
        close(pipefd[0]);
    }

    fprintf(stderr, "[supervisor] started container %s pid=%d\n",
            rec->id, pid);
    return rec;
}

/* ─────────────────── command handlers (supervisor side) ─────────── */

static void handle_start(supervisor_ctx_t *ctx,
                          const control_request_t *req,
                          int client_fd)
{
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    container_record_t *rec = launch_container(ctx, req);
    if (!rec) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message),
                 "ERROR: failed to start container '%s'\n", req->container_id);
    } else {
        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message),
                 "Started container '%s' (pid %d)\n", rec->id, rec->host_pid);
    }

    write(client_fd, &resp, sizeof(resp));
}

static void handle_run(supervisor_ctx_t *ctx,
                        const control_request_t *req,
                        int client_fd)
{
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    container_record_t *rec = launch_container(ctx, req);
    if (!rec) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message),
                 "ERROR: failed to start container '%s'\n", req->container_id);
        write(client_fd, &resp, sizeof(resp));
        return;
    }

    /*
     * Store the client fd so reap_children() can send the final response
     * when the container exits.  The client blocks reading from this fd.
     */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->run_client_fd = client_fd;  /* do NOT close client_fd here */
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Immediately acknowledge that the container started */
    resp.status = 0;
    snprintf(resp.message, sizeof(resp.message),
             "Running container '%s' (pid %d) — waiting for exit...\n",
             rec->id, rec->host_pid);
    write(client_fd, &resp, sizeof(resp));
    /*
     * Note: the client fd is NOT closed here.
     * reap_children() will send the final exit-status response and close it.
     */
}

static void handle_ps(supervisor_ctx_t *ctx, int client_fd)
{
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = ctx->containers;

    if (!c) {
        snprintf(resp.message, sizeof(resp.message), "No containers\n");
    } else {
        size_t off = 0;
        off += snprintf(resp.message + off, sizeof(resp.message) - off,
                        "%-20s %-8s %-10s %-18s %-10s %-10s\n",
                        "ID", "PID", "STATE", "STARTED",
                        "SOFT(MiB)", "HARD(MiB)");
        while (c && off < sizeof(resp.message) - 1) {
            char tsbuf[32];
            struct tm *tm = localtime(&c->started_at);
            strftime(tsbuf, sizeof(tsbuf), "%Y-%m-%d %H:%M:%S", tm);
            off += snprintf(resp.message + off, sizeof(resp.message) - off,
                            "%-20s %-8d %-10s %-18s %-10lu %-10lu\n",
                            c->id, c->host_pid, state_to_str(c->state),
                            tsbuf,
                            c->soft_limit_bytes >> 20,
                            c->hard_limit_bytes >> 20);
            c = c->next;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    resp.status = 0;
    write(client_fd, &resp, sizeof(resp));
}

static void handle_logs(supervisor_ctx_t *ctx,
                         const control_request_t *req,
                         int client_fd)
{
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    /* Find the log path */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = find_container(ctx, req->container_id);
    char log_path[PATH_MAX] = {0};
    if (c)
        strncpy(log_path, c->log_path, PATH_MAX - 1);
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (!c) {
        /* Also try the filesystem directly — container may have exited */
        snprintf(log_path, sizeof(log_path), "%s/%s.log",
                 LOG_DIR, req->container_id);
    }

    int fd = open(log_path, O_RDONLY);
    if (fd < 0) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message),
                 "No log file found for '%s'\n", req->container_id);
    } else {
        resp.status = 0;
        size_t off = 0;
        ssize_t n;
        while (off < sizeof(resp.message) - 1 &&
               (n = read(fd, resp.message + off,
                         sizeof(resp.message) - off - 1)) > 0)
            off += (size_t)n;
        resp.message[off] = '\0';
        close(fd);
        if (off == 0)
            snprintf(resp.message, sizeof(resp.message), "(empty log)\n");
    }

    write(client_fd, &resp, sizeof(resp));
}

static void handle_stop(supervisor_ctx_t *ctx,
                         const control_request_t *req,
                         int client_fd)
{
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = find_container(ctx, req->container_id);

    if (!c) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message),
                 "No such container: '%s'\n", req->container_id);
        pthread_mutex_unlock(&ctx->metadata_lock);
        write(client_fd, &resp, sizeof(resp));
        return;
    }

    if (c->state != CONTAINER_RUNNING && c->state != CONTAINER_STARTING) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message),
                 "Container '%s' is not running (state=%s)\n",
                 c->id, state_to_str(c->state));
        pthread_mutex_unlock(&ctx->metadata_lock);
        write(client_fd, &resp, sizeof(resp));
        return;
    }

    /*
     * Set stop_requested BEFORE sending the signal.
     * This lets reap_children() classify the termination correctly.
     */
    c->stop_requested = 1;

    pid_t pid = c->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Graceful: SIGTERM first, then escalate to SIGKILL */
    if (kill(pid, SIGTERM) != 0) {
        /* Process may have already exited between our check and the kill */
        if (errno != ESRCH) perror("stop: kill SIGTERM");
    }

    /* Give it 2 seconds to die cleanly, then SIGKILL */
    struct timespec ts = {0, 100000000L}; /* 100 ms */
    int tries = 20; /* 20 × 100ms = 2 s */
    while (tries-- > 0) {
        nanosleep(&ts, NULL);
        /* Check if it's gone */
        if (kill(pid, 0) != 0 && errno == ESRCH) break;
    }
    if (tries <= 0)
        kill(pid, SIGKILL);

    resp.status = 0;
    snprintf(resp.message, sizeof(resp.message),
             "Stopped container '%s'\n", req->container_id);
    write(client_fd, &resp, sizeof(resp));
}

/* ─────────────────── supervisor main loop ───────────────────────── */

/*
 * Parses raw bytes received from the CLI client into a control_request_t.
 * Wire format: the client sends sizeof(control_request_t) bytes verbatim.
 */
static int recv_request(int fd, control_request_t *req)
{
    size_t total = 0;
    char *buf = (char *)req;
    while (total < sizeof(*req)) {
        ssize_t n = read(fd, buf + total, sizeof(*req) - total);
        if (n <= 0) return -1;
        total += (size_t)n;
    }
    return 0;
}

static int run_supervisor(const char *rootfs_base)
{
    (void)rootfs_base;  /* kept for future use / namespace setup */

    fprintf(stderr, "[supervisor] starting up\n");

    /* Self-pipe for SIGCHLD */
    if (pipe(sigchld_pipe) != 0) { perror("pipe sigchld"); return 1; }
    fcntl(sigchld_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(sigchld_pipe[1], F_SETFL, O_NONBLOCK);

    /* Signal setup */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = sigterm_handler;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    /* UNIX domain socket */
    int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    unlink(CONTROL_PATH);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(server_fd); return 1;
    }
    if (listen(server_fd, 16) < 0) {
        perror("listen"); close(server_fd); return 1;
    }

    /* Initialize global supervisor context */
    supervisor_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = server_fd;
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] kernel monitor not available (running without it)\n");

    pthread_mutex_init(&ctx.metadata_lock, NULL);
    bb_init(&ctx.log_buffer);
    mkdir(LOG_DIR, 0755);

    /* Start the single consumer thread */
    if (pthread_create(&ctx.consumer_thread, NULL, consumer_thread_fn, &ctx) != 0) {
        perror("consumer thread");
        return 1;
    }

    g_ctx = &ctx;
    fprintf(stderr, "[supervisor] listening on %s\n", CONTROL_PATH);

    /* ── main event loop ── */
    while (!ctx.should_stop) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(server_fd, &rfds);
        FD_SET(sigchld_pipe[0], &rfds);
        int nfds = (server_fd > sigchld_pipe[0] ? server_fd : sigchld_pipe[0]) + 1;

        struct timeval tv = {1, 0}; /* 1-second timeout so we check should_stop */
        int r = select(nfds, &rfds, NULL, NULL, &tv);
        if (r < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }

        /* Drain the self-pipe and reap children */
        if (FD_ISSET(sigchld_pipe[0], &rfds)) {
            char drain[64];
            read(sigchld_pipe[0], drain, sizeof(drain));
            reap_children(&ctx);
        }

        /* Handle a new CLI connection */
        if (FD_ISSET(server_fd, &rfds)) {
            int client_fd = accept(server_fd, NULL, NULL);
            if (client_fd < 0) {
                if (errno != EINTR) perror("accept");
                continue;
            }

            control_request_t req;
            if (recv_request(client_fd, &req) != 0) {
                close(client_fd);
                continue;
            }

            switch (req.kind) {
            case CMD_START:
                handle_start(&ctx, &req, client_fd);
                close(client_fd);
                break;
            case CMD_RUN:
                handle_run(&ctx, &req, client_fd);
                /* client_fd intentionally NOT closed here — reap_children does it */
                break;
            case CMD_PS:
                handle_ps(&ctx, client_fd);
                close(client_fd);
                break;
            case CMD_LOGS:
                handle_logs(&ctx, &req, client_fd);
                close(client_fd);
                break;
            case CMD_STOP:
                handle_stop(&ctx, &req, client_fd);
                close(client_fd);
                break;
            default:
                close(client_fd);
                break;
            }
        }
    }

    /* ── orderly shutdown ── */
    fprintf(stderr, "[supervisor] shutting down...\n");

    /* Stop all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING || c->state == CONTAINER_STARTING) {
            c->stop_requested = 1;
            kill(c->host_pid, SIGTERM);
        }
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Give children a moment to exit, then force-reap */
    sleep(2);
    reap_children(&ctx);

    /* Shut down the logging pipeline and join consumer */
    bb_shutdown(&ctx.log_buffer);
    pthread_join(ctx.consumer_thread, NULL);
    bb_destroy(&ctx.log_buffer);

    /* Free container linked list */
    pthread_mutex_lock(&ctx.metadata_lock);
    c = ctx.containers;
    while (c) {
        container_record_t *next = c->next;
        if (c->run_client_fd >= 0) close(c->run_client_fd);
        free(c);
        c = next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    close(server_fd);
    unlink(CONTROL_PATH);
    close(sigchld_pipe[0]);
    close(sigchld_pipe[1]);

    fprintf(stderr, "[supervisor] exited cleanly\n");
    return 0;
}

/* ─────────────────── CLI client helpers ─────────────────────────── */

/*
 * Sends a request to the supervisor, reads one control_response_t, prints it.
 * For CMD_RUN we keep reading responses until the connection is closed
 * (the supervisor sends one on start, one more when the container exits).
 */
static int send_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect (is the supervisor running?)");
        close(fd); return 1;
    }

    /* Send the full request struct */
    size_t total = 0;
    const char *buf = (const char *)req;
    while (total < sizeof(*req)) {
        ssize_t n = write(fd, buf + total, sizeof(*req) - total);
        if (n <= 0) { perror("write request"); close(fd); return 1; }
        total += (size_t)n;
    }

    /* Read response(s) */
    int exit_status = 0;
    control_response_t resp;
    for (;;) {
        total = 0;
        buf   = (const char *)&resp;
        int got_eof = 0;
        while (total < sizeof(resp)) {
            ssize_t n = read(fd, (char *)buf + total, sizeof(resp) - total);
            if (n < 0) { if (errno == EINTR) continue; perror("read resp"); goto done; }
            if (n == 0) { got_eof = 1; break; }
            total += (size_t)n;
        }
        if (total == sizeof(resp)) {
            printf("%s", resp.message);
            exit_status = resp.status;
        }
        if (got_eof || req->kind != CMD_RUN) break;
    }

done:
    close(fd);
    return exit_status;
}

/* ─────────────────── flag parsing ──────────────────────────────── */

static int parse_mib_flag(const char *flag, const char *val, unsigned long *out)
{
    char *end;
    errno = 0;
    unsigned long mib = strtoul(val, &end, 10);
    if (errno || end == val || *end) {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, val); return -1;
    }
    if (mib > ULONG_MAX >> 20) {
        fprintf(stderr, "Value for %s too large\n", flag); return -1;
    }
    *out = mib << 20;
    return 0;
}

/*
 * Fills req->args[], req->arg_count from argv[start_index] up to the first
 * '--' flag (or argc), then processes remaining '--' flags.
 */
static int parse_args_and_flags(control_request_t *req,
                                 int argc, char *argv[], int start_index)
{
    int i = start_index;
    req->arg_count = 0;

    /* Collect positional args (before any -- flag) */
    while (i < argc && argv[i][0] != '-') {
        if (req->arg_count < 8)
            strncpy(req->args[req->arg_count++], argv[i], PATH_MAX - 1);
        i++;
    }

    /* Process -- flags in pairs */
    while (i < argc) {
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for %s\n", argv[i]); return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i+1], &req->soft_limit_bytes)) return -1;
        } else if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i+1], &req->hard_limit_bytes)) return -1;
        } else if (strcmp(argv[i], "--nice") == 0) {
            char *end;
            long v = strtol(argv[i+1], &end, 10);
            if (*end || v < -20 || v > 19) {
                fprintf(stderr, "Invalid --nice value: %s\n", argv[i+1]); return -1;
            }
            req->nice_value = (int)v;
        } else {
            fprintf(stderr, "Unknown flag: %s\n", argv[i]); return -1;
        }
        i += 2;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "soft limit cannot exceed hard limit\n"); return -1;
    }
    return 0;
}

/* ─────────────────── CLI command entry points ───────────────────── */

static int cmd_start_or_run(int argc, char *argv[], command_kind_t kind)
{
    if (argc < 5) {
        fprintf(stderr, "Usage: %s %s <id> <rootfs> <command> [args...] [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0], argv[1]);
        return 1;
    }

    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind              = kind;
    req.soft_limit_bytes  = DEFAULT_SOFT_MIB << 20;
    req.hard_limit_bytes  = DEFAULT_HARD_MIB << 20;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    strncpy(req.rootfs,       argv[3], PATH_MAX - 1);
    strncpy(req.command,      argv[4], PATH_MAX - 1);

    if (parse_args_and_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_request(&req);
}

static int cmd_logs(const char *id)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, id, CONTAINER_ID_LEN - 1);
    return send_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]); return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    return send_request(&req);
}

/* ─────────────────── main ───────────────────────────────────────── */

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start_or_run(argc, argv, CMD_START);

    if (strcmp(argv[1], "run") == 0)
        return cmd_start_or_run(argc, argv, CMD_RUN);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: %s logs <id>\n", argv[0]); return 1; }
        return cmd_logs(argv[2]);
    }

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}