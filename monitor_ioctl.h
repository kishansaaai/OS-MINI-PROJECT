#ifndef MONITOR_IOCTL_H
#define MONITOR_IOCTL_H

#ifdef __KERNEL__
#include <linux/ioctl.h>
#else
#include <sys/ioctl.h>
#endif
#include <linux/types.h>

#define MONITOR_NAME_LEN 64

struct monitor_request {
    __s32 pid;
    __u32 reserved;
    __aligned_u64 soft_limit_bytes;
    __aligned_u64 hard_limit_bytes;
    char container_id[MONITOR_NAME_LEN];
};

#define MONITOR_MAGIC 'M'
#define MONITOR_REGISTER _IOW(MONITOR_MAGIC, 1, struct monitor_request)
#define MONITOR_UNREGISTER _IOW(MONITOR_MAGIC, 2, struct monitor_request)

#endif
