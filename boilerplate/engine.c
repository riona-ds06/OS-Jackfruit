/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 32
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

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
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    pid_t host_pid;
    time_t started_at;
    time_t finished_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int monitor_registered;
    int producer_started;
    int producer_joined;
    int log_read_fd;
    pthread_t producer_thread;
    char log_path[PATH_MAX];
    char termination_reason[64];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    int exit_code;
    size_t payload_length;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_arg_t;

typedef struct supervisor_ctx {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    pthread_t listener_thread;
    pthread_t reaper_thread;
    pthread_t signal_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    pthread_cond_t state_changed;
    container_record_t *containers;
    char base_rootfs[PATH_MAX];
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int client_fd;
} client_handler_arg_t;

static volatile sig_atomic_t g_client_forward_stop = 0;
static char g_client_run_id[CONTAINER_ID_LEN];

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static ssize_t write_full(int fd, const void *buf, size_t count)
{
    const char *ptr = buf;
    size_t total = 0;

    while (total < count) {
        ssize_t written = write(fd, ptr + total, count - total);
        if (written < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (written == 0)
            break;
        total += (size_t)written;
    }

    return (ssize_t)total;
}

static ssize_t read_full(int fd, void *buf, size_t count)
{
    char *ptr = buf;
    size_t total = 0;

    while (total < count) {
        ssize_t nread = read(fd, ptr + total, count - total);
        if (nread < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (nread == 0)
            break;
        total += (size_t)nread;
    }

    return (ssize_t)total;
}

static int send_response(int fd,
                         int status,
                         int exit_code,
                         const char *message,
                         const char *payload,
                         size_t payload_length)
{
    control_response_t resp;

    memset(&resp, 0, sizeof(resp));
    resp.status = status;
    resp.exit_code = exit_code;
    resp.payload_length = payload_length;
    if (message)
        snprintf(resp.message, sizeof(resp.message), "%s", message);

    if (write_full(fd, &resp, sizeof(resp)) != (ssize_t)sizeof(resp))
        return -1;

    if (payload_length > 0 && payload) {
        if (write_full(fd, payload, payload_length) != (ssize_t)payload_length)
            return -1;
    }

    return 0;
}

static void format_timestamp(time_t when, char *buf, size_t buf_len)
{
    struct tm tm_info;

    if (when == 0) {
        snprintf(buf, buf_len, "-");
        return;
    }

    localtime_r(&when, &tm_info);
    strftime(buf, buf_len, "%Y-%m-%d %H:%M:%S", &tm_info);
}

static unsigned long bytes_to_mib(unsigned long bytes)
{
    return bytes >> 20;
}

static int state_is_active(container_state_t state)
{
    return state == CONTAINER_STARTING || state == CONTAINER_RUNNING;
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return 0;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 1;
}

static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *current = ctx->containers;

    while (current) {
        if (strncmp(current->id, id, sizeof(current->id)) == 0)
            return current;
        current = current->next;
    }

    return NULL;
}

static container_record_t *find_container_by_pid_locked(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *current = ctx->containers;

    while (current) {
        if (current->host_pid == pid)
            return current;
        current = current->next;
    }

    return NULL;
}

static int any_active_containers_locked(supervisor_ctx_t *ctx)
{
    container_record_t *current = ctx->containers;

    while (current) {
        if (state_is_active(current->state))
            return 1;
        current = current->next;
    }

    return 0;
}

static int ensure_log_dir(void)
{
    if (mkdir(LOG_DIR, 0755) < 0 && errno != EEXIST) {
        perror("mkdir logs");
        return -1;
    }
    return 0;
}

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;

    if (ensure_log_dir() != 0)
        return NULL;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) > 0) {
        char log_path[PATH_MAX];
        int fd;

        pthread_mutex_lock(&ctx->metadata_lock);
        {
            container_record_t *record = find_container_locked(ctx, item.container_id);
            if (record)
                snprintf(log_path, sizeof(log_path), "%s", record->log_path);
            else
                snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, item.container_id);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        fd = open(log_path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd < 0)
            continue;

        (void)write_full(fd, item.data, item.length);
        close(fd);
    }

    return NULL;
}

static void *producer_thread_main(void *arg)
{
    producer_arg_t *producer = arg;
    char buffer[LOG_CHUNK_SIZE];
    ssize_t nread;

    while ((nread = read(producer->fd, buffer, sizeof(buffer))) > 0) {
        log_item_t item;

        memset(&item, 0, sizeof(item));
        snprintf(item.container_id, sizeof(item.container_id), "%s", producer->container_id);
        item.length = (size_t)nread;
        memcpy(item.data, buffer, (size_t)nread);

        if (bounded_buffer_push(producer->buffer, &item) != 0)
            break;
    }

    close(producer->fd);
    free(producer);
    return NULL;
}

static int child_fn(void *arg)
{
    child_config_t *cfg = arg;

    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("sethostname");
        return 1;
    }

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0) {
        perror("mount private");
        return 1;
    }

    if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0) {
        perror("setpriority");
        return 1;
    }

    if (chdir(cfg->rootfs) < 0) {
        perror("chdir rootfs");
        return 1;
    }

    if (chroot(".") < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir /");
        return 1;
    }

    if (mkdir("/proc", 0555) < 0 && errno != EEXIST) {
        perror("mkdir /proc");
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount /proc");
        return 1;
    }

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }

    close(cfg->log_write_fd);

    execl("/bin/sh", "sh", "-c", cfg->command, (char *)NULL);
    perror("execl");
    return 127;
}

static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    snprintf(req.container_id, sizeof(req.container_id), "%s", container_id);

    if (monitor_fd < 0)
        return -1;

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

static int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    snprintf(req.container_id, sizeof(req.container_id), "%s", container_id);

    if (monitor_fd < 0)
        return -1;

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static int resolve_rootfs_path(const char *input, char *output, size_t output_len)
{
    char resolved[PATH_MAX];

    if (!realpath(input, resolved))
        return -1;

    if (strlen(resolved) >= output_len) {
        errno = ENAMETOOLONG;
        return -1;
    }

    snprintf(output, output_len, "%s", resolved);
    return 0;
}

static int validate_start_request_locked(supervisor_ctx_t *ctx,
                                         const control_request_t *req,
                                         const char *resolved_rootfs,
                                         char *error,
                                         size_t error_len)
{
    container_record_t *current = ctx->containers;

    if (find_container_locked(ctx, req->container_id)) {
        snprintf(error, error_len, "container id '%s' already exists", req->container_id);
        return -1;
    }

    while (current) {
        if (state_is_active(current->state) &&
            strncmp(current->rootfs, resolved_rootfs, sizeof(current->rootfs)) == 0) {
            snprintf(error, error_len, "rootfs already in use by container '%s'", current->id);
            return -1;
        }
        current = current->next;
    }

    return 0;
}

static int start_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           char *message,
                           size_t message_len)
{
    int pipefd[2] = {-1, -1};
    void *stack = NULL;
    child_config_t cfg;
    container_record_t *record = NULL;
    producer_arg_t *producer = NULL;
    char resolved_rootfs[PATH_MAX];
    pid_t child_pid;

    if (ensure_log_dir() != 0) {
        snprintf(message, message_len, "failed to create log directory");
        return -1;
    }

    if (resolve_rootfs_path(req->rootfs, resolved_rootfs, sizeof(resolved_rootfs)) != 0) {
        snprintf(message, message_len, "invalid rootfs: %s", strerror(errno));
        return -1;
    }

    if (pipe(pipefd) < 0) {
        snprintf(message, message_len, "pipe failed: %s", strerror(errno));
        return -1;
    }

    record = calloc(1, sizeof(*record));
    producer = calloc(1, sizeof(*producer));
    stack = malloc(STACK_SIZE);
    if (!record || !producer || !stack) {
        snprintf(message, message_len, "out of memory while starting container");
        goto error;
    }

    memset(&cfg, 0, sizeof(cfg));
    snprintf(cfg.id, sizeof(cfg.id), "%s", req->container_id);
    snprintf(cfg.rootfs, sizeof(cfg.rootfs), "%s", resolved_rootfs);
    snprintf(cfg.command, sizeof(cfg.command), "%s", req->command);
    cfg.nice_value = req->nice_value;
    cfg.log_write_fd = pipefd[1];

    memset(record, 0, sizeof(*record));
    snprintf(record->id, sizeof(record->id), "%s", req->container_id);
    snprintf(record->rootfs, sizeof(record->rootfs), "%s", resolved_rootfs);
    snprintf(record->command, sizeof(record->command), "%s", req->command);
    snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, req->container_id);
    snprintf(record->termination_reason,
             sizeof(record->termination_reason),
             "%s",
             "running");
    record->state = CONTAINER_STARTING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->nice_value = req->nice_value;
    record->log_read_fd = pipefd[0];
    record->started_at = time(NULL);

    pthread_mutex_lock(&ctx->metadata_lock);
    if (validate_start_request_locked(ctx, req, resolved_rootfs, message, message_len) != 0) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        goto error;
    }
    record->next = ctx->containers;
    ctx->containers = record;
    pthread_cond_broadcast(&ctx->state_changed);
    pthread_mutex_unlock(&ctx->metadata_lock);

    child_pid = clone(child_fn,
                      (char *)stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      &cfg);
    if (child_pid < 0) {
        snprintf(message, message_len, "clone failed: %s", strerror(errno));
        pthread_mutex_lock(&ctx->metadata_lock);
        if (ctx->containers == record) {
            ctx->containers = record->next;
        } else {
            container_record_t *cur = ctx->containers;
            while (cur && cur->next != record)
                cur = cur->next;
            if (cur)
                cur->next = record->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        goto error;
    }

    close(pipefd[1]);
    pipefd[1] = -1;

    record->host_pid = child_pid;
    record->state = CONTAINER_RUNNING;

    producer->fd = pipefd[0];
    pipefd[0] = -1;
    snprintf(producer->container_id, sizeof(producer->container_id), "%s", req->container_id);
    producer->buffer = &ctx->log_buffer;

    if (pthread_create(&record->producer_thread, NULL, producer_thread_main, producer) != 0) {
        snprintf(message, message_len, "failed to start log producer thread");
        kill(child_pid, SIGKILL);
        goto error;
    }
    record->producer_started = 1;
    producer = NULL;

    if (register_with_monitor(ctx->monitor_fd,
                              record->id,
                              record->host_pid,
                              record->soft_limit_bytes,
                              record->hard_limit_bytes) == 0) {
        record->monitor_registered = 1;
    } else if (ctx->monitor_fd >= 0) {
        fprintf(stderr, "warning: failed to register %s with kernel monitor\n", record->id);
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    pthread_cond_broadcast(&ctx->state_changed);
    pthread_mutex_unlock(&ctx->metadata_lock);

    snprintf(message,
             message_len,
             "started container '%s' pid=%d",
             record->id,
             record->host_pid);

    free(stack);
    return 0;

error:
    if (producer)
        free(producer);
    if (record)
        free(record);
    if (pipefd[0] >= 0)
        close(pipefd[0]);
    if (pipefd[1] >= 0)
        close(pipefd[1]);
    if (stack)
        free(stack);
    return -1;
}

static int collect_ps_payload(supervisor_ctx_t *ctx, char **payload, size_t *payload_len)
{
    size_t capacity = 32768;
    size_t used = 0;
    char *buf = malloc(capacity);
    container_record_t *current;

    if (!buf)
        return -1;

    used += (size_t)snprintf(buf + used,
                             capacity - used,
                             "ID\tPID\tSTATE\tREASON\tSOFT(MiB)\tHARD(MiB)\tNICE\tSTARTED\tFINISHED\tEXIT\n");

    pthread_mutex_lock(&ctx->metadata_lock);
    current = ctx->containers;
    while (current) {
        char started[32];
        char finished[32];
        int written;

        format_timestamp(current->started_at, started, sizeof(started));
        format_timestamp(current->finished_at, finished, sizeof(finished));

        written = snprintf(buf + used,
                           capacity - used,
                           "%s\t%d\t%s\t%s\t%lu\t%lu\t%d\t%s\t%s\tcode=%d signal=%d\n",
                           current->id,
                           current->host_pid,
                           state_to_string(current->state),
                           current->termination_reason,
                           bytes_to_mib(current->soft_limit_bytes),
                           bytes_to_mib(current->hard_limit_bytes),
                           current->nice_value,
                           started,
                           finished,
                           current->exit_code,
                           current->exit_signal);

        if (written < 0 || (size_t)written >= capacity - used) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            free(buf);
            errno = ENOSPC;
            return -1;
        }

        used += (size_t)written;
        current = current->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    *payload = buf;
    *payload_len = used;
    return 0;
}

static int collect_logs_payload(supervisor_ctx_t *ctx,
                                const char *container_id,
                                char **payload,
                                size_t *payload_len)
{
    char log_path[PATH_MAX];
    struct stat st;
    int fd;
    char *buf;

    pthread_mutex_lock(&ctx->metadata_lock);
    {
        container_record_t *record = find_container_locked(ctx, container_id);
        if (!record) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            errno = ENOENT;
            return -1;
        }
        snprintf(log_path, sizeof(log_path), "%s", record->log_path);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (stat(log_path, &st) < 0)
        return -1;

    buf = malloc((size_t)st.st_size + 1);
    if (!buf)
        return -1;

    fd = open(log_path, O_RDONLY);
    if (fd < 0) {
        free(buf);
        return -1;
    }

    if (read_full(fd, buf, (size_t)st.st_size) != st.st_size) {
        close(fd);
        free(buf);
        return -1;
    }
    close(fd);

    buf[st.st_size] = '\0';
    *payload = buf;
    *payload_len = (size_t)st.st_size;
    return 0;
}

static int stop_container(supervisor_ctx_t *ctx,
                          const char *container_id,
                          char *message,
                          size_t message_len)
{
    container_record_t *record;
    pid_t pid;
    int still_active = 0;
    int attempts;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_locked(ctx, container_id);
    if (!record) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(message, message_len, "unknown container '%s'", container_id);
        return -1;
    }

    if (!state_is_active(record->state)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(message,
                 message_len,
                 "container '%s' is already %s",
                 container_id,
                 state_to_string(record->state));
        return 0;
    }

    record->stop_requested = 1;
    snprintf(record->termination_reason,
             sizeof(record->termination_reason),
             "%s",
             "stop_requested");
    pid = record->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (kill(pid, SIGTERM) < 0) {
        snprintf(message, message_len, "failed to stop '%s': %s", container_id, strerror(errno));
        return -1;
    }

    for (attempts = 0; attempts < 10; attempts++) {
        usleep(100000);
        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_locked(ctx, container_id);
        still_active = record && state_is_active(record->state);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!still_active)
            break;
    }

    if (still_active) {
        if (kill(pid, SIGKILL) < 0 && errno != ESRCH) {
            snprintf(message,
                     message_len,
                     "failed to force-stop '%s': %s",
                     container_id,
                     strerror(errno));
            return -1;
        }
        snprintf(message,
                 message_len,
                 "stop escalated to SIGKILL for '%s' (pid=%d)",
                 container_id,
                 pid);
        return 0;
    }

    snprintf(message, message_len, "stop signal sent to '%s' (pid=%d)", container_id, pid);
    return 0;
}

static void finalize_container_exit(supervisor_ctx_t *ctx, pid_t pid, int status)
{
    container_record_t *record;
    pthread_t producer_thread;
    int join_producer = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_by_pid_locked(ctx, pid);
    if (!record) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    record->finished_at = time(NULL);
    record->host_pid = pid;

    if (WIFEXITED(status)) {
        record->exit_code = WEXITSTATUS(status);
        record->exit_signal = 0;
        if (record->stop_requested) {
            record->state = CONTAINER_STOPPED;
            snprintf(record->termination_reason,
                     sizeof(record->termination_reason),
                     "%s",
                     "stopped");
        } else {
            record->state = CONTAINER_EXITED;
            snprintf(record->termination_reason,
                     sizeof(record->termination_reason),
                     "%s",
                     "exited");
        }
    } else if (WIFSIGNALED(status)) {
        record->exit_code = 128 + WTERMSIG(status);
        record->exit_signal = WTERMSIG(status);
        if (record->stop_requested) {
            record->state = CONTAINER_STOPPED;
            snprintf(record->termination_reason,
                     sizeof(record->termination_reason),
                     "%s",
                     "stopped");
        } else if (WTERMSIG(status) == SIGKILL) {
            record->state = CONTAINER_KILLED;
            snprintf(record->termination_reason,
                     sizeof(record->termination_reason),
                     "%s",
                     "hard_limit_killed");
        } else {
            record->state = CONTAINER_KILLED;
            snprintf(record->termination_reason,
                     sizeof(record->termination_reason),
                     "%s",
                     "signaled");
        }
    }

    if (record->monitor_registered) {
        (void)unregister_from_monitor(ctx->monitor_fd, record->id, pid);
        record->monitor_registered = 0;
    }

    if (record->producer_started && !record->producer_joined) {
        producer_thread = record->producer_thread;
        record->producer_joined = 1;
        join_producer = 1;
    }

    pthread_cond_broadcast(&ctx->state_changed);
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (join_producer)
        pthread_join(producer_thread, NULL);
}

static void *reaper_thread_main(void *arg)
{
    supervisor_ctx_t *ctx = arg;

    for (;;) {
        int status;
        pid_t pid = waitpid(-1, &status, 0);

        if (pid < 0) {
            if (errno == EINTR)
                continue;
            if (errno == ECHILD) {
                pthread_mutex_lock(&ctx->metadata_lock);
                if (ctx->should_stop && !any_active_containers_locked(ctx)) {
                    pthread_mutex_unlock(&ctx->metadata_lock);
                    break;
                }
                pthread_mutex_unlock(&ctx->metadata_lock);
                usleep(100000);
                continue;
            }
            perror("waitpid");
            usleep(100000);
            continue;
        }

        finalize_container_exit(ctx, pid, status);
    }

    return NULL;
}

static int create_control_socket(void)
{
    int server_fd;
    struct sockaddr_un addr;

    unlink(CONTROL_PATH);

    server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", CONTROL_PATH);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(server_fd);
        return -1;
    }

    if (listen(server_fd, 32) < 0) {
        close(server_fd);
        unlink(CONTROL_PATH);
        return -1;
    }

    return server_fd;
}

static int await_container_exit(supervisor_ctx_t *ctx, const char *container_id)
{
    container_record_t *record;
    int exit_code;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (;;) {
        record = find_container_locked(ctx, container_id);
        if (!record) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            errno = ENOENT;
            return 1;
        }
        if (!state_is_active(record->state))
            break;
        pthread_cond_wait(&ctx->state_changed, &ctx->metadata_lock);
    }
    exit_code = record->exit_code;
    pthread_mutex_unlock(&ctx->metadata_lock);
    return exit_code;
}

static void *client_handler_main(void *arg)
{
    client_handler_arg_t *handler = arg;
    supervisor_ctx_t *ctx = handler->ctx;
    control_request_t req;
    int client_fd = handler->client_fd;
    char message[CONTROL_MESSAGE_LEN];

    free(handler);

    if (read_full(client_fd, &req, sizeof(req)) != (ssize_t)sizeof(req)) {
        close(client_fd);
        return NULL;
    }

    memset(message, 0, sizeof(message));

    switch (req.kind) {
    case CMD_START:
        if (start_container(ctx, &req, message, sizeof(message)) == 0)
            (void)send_response(client_fd, 0, 0, message, NULL, 0);
        else
            (void)send_response(client_fd, 1, 1, message, NULL, 0);
        break;

    case CMD_RUN:
        if (start_container(ctx, &req, message, sizeof(message)) == 0) {
            int exit_code = await_container_exit(ctx, req.container_id);
            char done_msg[CONTROL_MESSAGE_LEN];

            snprintf(done_msg,
                     sizeof(done_msg),
                     "container '%s' finished with status %d",
                     req.container_id,
                     exit_code);
            (void)send_response(client_fd, 0, exit_code, done_msg, NULL, 0);
        } else {
            (void)send_response(client_fd, 1, 1, message, NULL, 0);
        }
        break;

    case CMD_PS:
    {
        char *payload = NULL;
        size_t payload_len = 0;

        if (collect_ps_payload(ctx, &payload, &payload_len) == 0) {
            (void)send_response(client_fd, 0, 0, "container metadata", payload, payload_len);
            free(payload);
        } else {
            (void)send_response(client_fd, 1, 1, "failed to build ps output", NULL, 0);
        }
        break;
    }

    case CMD_LOGS:
    {
        char *payload = NULL;
        size_t payload_len = 0;

        if (collect_logs_payload(ctx, req.container_id, &payload, &payload_len) == 0) {
            (void)send_response(client_fd, 0, 0, "container logs", payload, payload_len);
            free(payload);
        } else {
            snprintf(message, sizeof(message), "no logs found for '%s'", req.container_id);
            (void)send_response(client_fd, 1, 1, message, NULL, 0);
        }
        break;
    }

    case CMD_STOP:
        if (stop_container(ctx, req.container_id, message, sizeof(message)) == 0)
            (void)send_response(client_fd, 0, 0, message, NULL, 0);
        else
            (void)send_response(client_fd, 1, 1, message, NULL, 0);
        break;

    default:
        (void)send_response(client_fd, 1, 1, "unsupported command", NULL, 0);
        break;
    }

    close(client_fd);
    return NULL;
}

static void *listener_thread_main(void *arg)
{
    supervisor_ctx_t *ctx = arg;

    for (;;) {
        fd_set rfds;
        struct timeval timeout;
        int ready;
        int client_fd;
        client_handler_arg_t *handler;
        pthread_t thread;

        pthread_mutex_lock(&ctx->metadata_lock);
        if (ctx->should_stop) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            break;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        FD_ZERO(&rfds);
        FD_SET(ctx->server_fd, &rfds);
        timeout.tv_sec = 0;
        timeout.tv_usec = 250000;

        ready = select(ctx->server_fd + 1, &rfds, NULL, NULL, &timeout);
        if (ready < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            usleep(100000);
            continue;
        }

        if (ready == 0)
            continue;

        client_fd = accept(ctx->server_fd, NULL, NULL);
        if (client_fd < 0) {
            pthread_mutex_lock(&ctx->metadata_lock);
            if (ctx->should_stop) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                break;
            }
            pthread_mutex_unlock(&ctx->metadata_lock);

            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            perror("accept");
            usleep(100000);
            continue;
        }

        handler = calloc(1, sizeof(*handler));
        if (!handler) {
            close(client_fd);
            continue;
        }

        handler->ctx = ctx;
        handler->client_fd = client_fd;

        if (pthread_create(&thread, NULL, client_handler_main, handler) != 0) {
            close(client_fd);
            free(handler);
            continue;
        }

        pthread_detach(thread);
    }

    return NULL;
}

static void request_supervisor_stop(supervisor_ctx_t *ctx)
{
    container_record_t *current;

    pthread_mutex_lock(&ctx->metadata_lock);
    if (ctx->should_stop) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }
    ctx->should_stop = 1;

    current = ctx->containers;
    while (current) {
        if (state_is_active(current->state)) {
            current->stop_requested = 1;
            snprintf(current->termination_reason,
                     sizeof(current->termination_reason),
                     "%s",
                     "supervisor_shutdown");
            kill(current->host_pid, SIGTERM);
        }
        current = current->next;
    }
    pthread_cond_broadcast(&ctx->state_changed);
    pthread_mutex_unlock(&ctx->metadata_lock);

    unlink(CONTROL_PATH);
}

static void *signal_thread_main(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    sigset_t set;
    int sig;

    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);

    while (sigwait(&set, &sig) == 0) {
        if (sig == SIGINT || sig == SIGTERM) {
            request_supervisor_stop(ctx);
            break;
        }
    }

    return NULL;
}

static void cleanup_records(supervisor_ctx_t *ctx)
{
    container_record_t *current = ctx->containers;

    while (current) {
        container_record_t *next = current->next;

        if (current->producer_started && !current->producer_joined)
            pthread_join(current->producer_thread, NULL);

        free(current);
        current = next;
    }

    ctx->containers = NULL;
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    sigset_t set;
    int rc;
    int grace_ticks = 50;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    if (resolve_rootfs_path(rootfs, ctx.base_rootfs, sizeof(ctx.base_rootfs)) != 0) {
        fprintf(stderr, "Invalid base rootfs '%s': %s\n", rootfs, strerror(errno));
        return 1;
    }

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = pthread_cond_init(&ctx.state_changed, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_cond_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_cond_destroy(&ctx.state_changed);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &set, NULL);

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "warning: /dev/container_monitor not available, continuing without kernel monitor\n");

    ctx.server_fd = create_control_socket();
    if (ctx.server_fd < 0) {
        perror("create_control_socket");
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_cond_destroy(&ctx.state_changed);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0 ||
        pthread_create(&ctx.reaper_thread, NULL, reaper_thread_main, &ctx) != 0 ||
        pthread_create(&ctx.listener_thread, NULL, listener_thread_main, &ctx) != 0 ||
        pthread_create(&ctx.signal_thread, NULL, signal_thread_main, &ctx) != 0) {
        perror("pthread_create");
        request_supervisor_stop(&ctx);
        bounded_buffer_begin_shutdown(&ctx.log_buffer);
        pthread_join(ctx.logger_thread, NULL);
        pthread_cond_destroy(&ctx.state_changed);
        pthread_mutex_destroy(&ctx.metadata_lock);
        bounded_buffer_destroy(&ctx.log_buffer);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        return 1;
    }

    printf("Supervisor ready. base_rootfs=%s control=%s\n", ctx.base_rootfs, CONTROL_PATH);
    fflush(stdout);

    pthread_join(ctx.signal_thread, NULL);
    pthread_join(ctx.listener_thread, NULL);

    if (ctx.server_fd >= 0) {
        close(ctx.server_fd);
        ctx.server_fd = -1;
    }

    while (grace_ticks-- > 0) {
        pthread_mutex_lock(&ctx.metadata_lock);
        if (!any_active_containers_locked(&ctx)) {
            pthread_mutex_unlock(&ctx.metadata_lock);
            break;
        }
        pthread_mutex_unlock(&ctx.metadata_lock);
        usleep(100000);
    }

    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *current = ctx.containers;
        while (current) {
            if (state_is_active(current->state))
                kill(current->host_pid, SIGKILL);
            current = current->next;
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    pthread_join(ctx.reaper_thread, NULL);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    cleanup_records(&ctx);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    unlink(CONTROL_PATH);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_cond_destroy(&ctx.state_changed);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

static void client_forward_signal(int signo)
{
    (void)signo;
    g_client_forward_stop = 1;
}

static int send_stop_request_sync(const char *container_id)
{
    int fd;
    struct sockaddr_un addr;
    control_request_t req;
    control_response_t resp;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    snprintf(req.container_id, sizeof(req.container_id), "%s", container_id);

    if (write_full(fd, &req, sizeof(req)) != (ssize_t)sizeof(req)) {
        close(fd);
        return -1;
    }

    (void)read_full(fd, &resp, sizeof(resp));
    close(fd);
    return 0;
}

static int read_response_with_optional_forwarding(int fd,
                                                  control_response_t *resp,
                                                  command_kind_t kind)
{
    size_t total = 0;
    int stop_forwarded = 0;

    while (total < sizeof(*resp)) {
        fd_set rfds;
        struct timeval timeout;
        int ready;

        FD_ZERO(&rfds);
        FD_SET(fd, &rfds);
        timeout.tv_sec = 0;
        timeout.tv_usec = 250000;

        ready = select(fd + 1, &rfds, NULL, NULL, &timeout);
        if (ready < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        if (ready == 0) {
            if (kind == CMD_RUN && g_client_forward_stop && !stop_forwarded) {
                send_stop_request_sync(g_client_run_id);
                stop_forwarded = 1;
            }
            continue;
        }

        if (FD_ISSET(fd, &rfds)) {
            ssize_t nread = read(fd, (char *)resp + total, sizeof(*resp) - total);
            if (nread < 0) {
                if (errno == EINTR)
                    continue;
                return -1;
            }
            if (nread == 0)
                return -1;
            total += (size_t)nread;
        }
    }

    return 0;
}

static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;
    struct sigaction old_int;
    struct sigaction old_term;
    struct sigaction action;
    char *payload = NULL;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    if (write_full(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write request");
        close(fd);
        return 1;
    }

    memset(&old_int, 0, sizeof(old_int));
    memset(&old_term, 0, sizeof(old_term));
    memset(&action, 0, sizeof(action));

    if (req->kind == CMD_RUN) {
        g_client_forward_stop = 0;
        snprintf(g_client_run_id, sizeof(g_client_run_id), "%s", req->container_id);
        action.sa_handler = client_forward_signal;
        sigemptyset(&action.sa_mask);
        sigaction(SIGINT, &action, &old_int);
        sigaction(SIGTERM, &action, &old_term);
    }

    if (read_response_with_optional_forwarding(fd, &resp, req->kind) != 0) {
        perror("read response");
        close(fd);
        if (req->kind == CMD_RUN) {
            sigaction(SIGINT, &old_int, NULL);
            sigaction(SIGTERM, &old_term, NULL);
        }
        return 1;
    }

    if (req->kind == CMD_RUN) {
        sigaction(SIGINT, &old_int, NULL);
        sigaction(SIGTERM, &old_term, NULL);
    }

    if (resp.payload_length > 0) {
        payload = malloc(resp.payload_length + 1);
        if (!payload) {
            close(fd);
            return 1;
        }

        if (read_full(fd, payload, resp.payload_length) != (ssize_t)resp.payload_length) {
            perror("read payload");
            free(payload);
            close(fd);
            return 1;
        }
        payload[resp.payload_length] = '\0';
    }

    if (resp.message[0] != '\0')
        fprintf(stdout, "%s\n", resp.message);
    if (payload)
        fwrite(payload, 1, resp.payload_length, stdout);

    free(payload);
    close(fd);

    if (req->kind == CMD_RUN)
        return resp.exit_code;

    return resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
