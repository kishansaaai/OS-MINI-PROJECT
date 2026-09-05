#include <linux/capability.h>
#include <linux/cdev.h>
#include <linux/compat.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/mm.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/uaccess.h>
#include <linux/user_namespace.h>
#include <linux/version.h>
#include <linux/workqueue.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"
#define CHECK_INTERVAL_MS 100
#define MAX_MONITORED 64

struct monitored_entry {
    struct pid *pid; /* Retain identity; never signal a reused numeric PID. */
    char container_id[MONITOR_NAME_LEN];
    u64 soft_limit, hard_limit;
    bool soft_triggered;
    struct list_head list;
};

static LIST_HEAD(monitored_list);
static DEFINE_MUTEX(monitored_lock);
static unsigned int entry_count;
static struct delayed_work monitor_work;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *monitor_class;

static void remove_entry(struct monitored_entry *entry)
{
    list_del(&entry->list);
    put_pid(entry->pid);
    kfree(entry);
    entry_count--;
}

/* mmput() and the mutex require process context, not a timer/softirq callback. */
static void check_memory(struct work_struct *work)
{
    struct monitored_entry *entry, *tmp;

    (void)work;
    mutex_lock(&monitored_lock);
    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        struct task_struct *task = get_pid_task(entry->pid, PIDTYPE_PID);
        struct mm_struct *mm;
        u64 rss;

        if (!task) {
            remove_entry(entry);
            continue;
        }
        mm = get_task_mm(task);
        if (!mm) {
            put_task_struct(task);
            remove_entry(entry);
            continue;
        }
        rss = (u64)get_mm_rss(mm) << PAGE_SHIFT;
        mmput(mm);
        if (rss > entry->soft_limit && !entry->soft_triggered) {
            pr_warn("[container_monitor] SOFT LIMIT container=%s pid=%d rss=%llu limit=%llu\n",
                    entry->container_id, pid_nr(entry->pid), rss, entry->soft_limit);
            entry->soft_triggered = true;
        }
        if (rss > entry->hard_limit) {
            int ret = send_sig(SIGKILL, task, 0);
            pr_warn("[container_monitor] HARD LIMIT container=%s pid=%d rss=%llu limit=%llu signal_result=%d\n",
                    entry->container_id, pid_nr(entry->pid), rss, entry->hard_limit, ret);
            if (!ret || ret == -ESRCH)
                remove_entry(entry);
        }
        put_task_struct(task);
    }
    mutex_unlock(&monitored_lock);
    schedule_delayed_work(&monitor_work, msecs_to_jiffies(CHECK_INTERVAL_MS));
}

static long monitor_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;
    struct monitored_entry *entry, *tmp;
    struct pid *pid;
    struct task_struct *task;
    long ret = 0;

    (void)file;
    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -ENOTTY;
    if (!ns_capable(&init_user_ns, CAP_SYS_ADMIN))
        return -EPERM;
    if (copy_from_user(&req, (void __user *)arg, sizeof(req)))
        return -EFAULT;
    if (req.pid <= 0 || req.reserved || !memchr(req.container_id, 0, sizeof(req.container_id)))
        return -EINVAL;
    pid = find_get_pid(req.pid);
    if (!pid)
        return cmd == MONITOR_UNREGISTER ? -ENOENT : -ESRCH;

    mutex_lock(&monitored_lock);
    if (cmd == MONITOR_UNREGISTER) {
        ret = -ENOENT;
        list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
            if (entry->pid == pid) {
                remove_entry(entry);
                ret = 0;
                break;
            }
        }
        goto out;
    }
    if (!req.container_id[0] || !req.soft_limit_bytes || req.soft_limit_bytes >= req.hard_limit_bytes) {
        ret = -EINVAL;
        goto out;
    }
    list_for_each_entry(entry, &monitored_list, list) {
        if (entry->pid == pid) {
            ret = -EEXIST;
            goto out;
        }
    }
    if (entry_count >= MAX_MONITORED) {
        ret = -ENOSPC;
        goto out;
    }
    task = get_pid_task(pid, PIDTYPE_PID);
    if (!task) {
        ret = -ESRCH;
        goto out;
    }
    put_task_struct(task);
    entry = kzalloc(sizeof(*entry), GFP_KERNEL);
    if (!entry) {
        ret = -ENOMEM;
        goto out;
    }
    entry->pid = get_pid(pid);
    entry->soft_limit = req.soft_limit_bytes;
    entry->hard_limit = req.hard_limit_bytes;
    strscpy(entry->container_id, req.container_id, sizeof(entry->container_id));
    list_add_tail(&entry->list, &monitored_list);
    entry_count++;
    /* Keep the lock until all accesses to the newly published entry finish. */
    pr_info("[container_monitor] Registered container=%s pid=%d soft=%llu hard=%llu\n",
            entry->container_id, pid_nr(entry->pid), entry->soft_limit, entry->hard_limit);
out:
    mutex_unlock(&monitored_lock);
    put_pid(pid);
    return ret;
}

static const struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
#ifdef CONFIG_COMPAT
    .compat_ioctl = compat_ptr_ioctl,
#endif
};

static int __init monitor_init(void)
{
    int ret;
    struct device *device;

    ret = alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME);
    if (ret)
        return ret;
    cdev_init(&c_dev, &fops);
    ret = cdev_add(&c_dev, dev_num, 1);
    if (ret)
        goto unregister;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    monitor_class = class_create(DEVICE_NAME);
#else
    monitor_class = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(monitor_class)) {
        ret = PTR_ERR(monitor_class);
        goto del_cdev;
    }
    device = device_create(monitor_class, NULL, dev_num, NULL, DEVICE_NAME);
    if (IS_ERR(device)) {
        ret = PTR_ERR(device);
        goto destroy_class;
    }
    INIT_DELAYED_WORK(&monitor_work, check_memory);
    schedule_delayed_work(&monitor_work, msecs_to_jiffies(CHECK_INTERVAL_MS));
    pr_info("[container_monitor] Module loaded: /dev/%s\n", DEVICE_NAME);
    return 0;
destroy_class:
    class_destroy(monitor_class);
del_cdev:
    cdev_del(&c_dev);
unregister:
    unregister_chrdev_region(dev_num, 1);
    return ret;
}

static void __exit monitor_exit(void)
{
    struct monitored_entry *entry, *tmp;

    cancel_delayed_work_sync(&monitor_work);
    mutex_lock(&monitored_lock);
    list_for_each_entry_safe(entry, tmp, &monitored_list, list)
        remove_entry(entry);
    mutex_unlock(&monitored_lock);
    device_destroy(monitor_class, dev_num);
    class_destroy(monitor_class);
    cdev_del(&c_dev);
    unregister_chrdev_region(dev_num, 1);
    pr_info("[container_monitor] Module unloaded\n");
}

module_init(monitor_init);
module_exit(monitor_exit);
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
