# Multi-Container Runtime

A small Linux runtime for learning about namespaces, process supervision, threaded logging, scheduling, and kernel memory monitoring. One supervisor manages multiple containers through a local CLI.

[![Runtime checks](https://github.com/kishansaaai/Multi-Container-Runtime/actions/workflows/ci.yml/badge.svg)](https://github.com/kishansaaai/Multi-Container-Runtime/actions/workflows/ci.yml)

## Scope and requirements

Use **trusted workloads in a disposable Linux VM**. This is an educational runtime, not a security boundary for hostile code. Containers run as root, share the host network, and retain privileges. There is no user namespace, capability dropping, seccomp policy, cgroup accounting, or image management. `chroot` alone does not make privileged processes safe to run on a valuable host.

- Ubuntu 22.04 and 24.04 are covered by CI.
- User-space builds/tests need a Linux C toolchain and Python 3. WSL2 can run these; privileged integration tests also require namespace support.
- Loading `monitor.ko` needs a VM with matching kernel headers. Sign the module or disable Secure Boot in the test VM.
- The supervisor needs root and permission to create PID, mount, UTS and IPC namespaces.

```sh
sudo apt update
sudo apt install -y build-essential python3 linux-headers-$(uname -r)
make                 # engine, static workloads, and monitor.ko
make ci              # user-space targets only; no kernel headers or root
bash environment-check.sh --build-only
```

Workloads are statically linked by default for use in Alpine. Override `WORKLOAD_LDFLAGS=` for dynamic builds only when the rootfs has matching libraries and a loader. `CC`, `CPPFLAGS`, `CFLAGS`, `LDFLAGS`, and `KDIR` are configurable. `make clean` removes build products and preserves logs and running supervisors.

## Prepare a root filesystem

Download an Alpine minirootfs for your architecture from the [official downloads page](https://alpinelinux.org/downloads/), verify its published checksum, and extract it into a new directory. For example, after placing the archive at `alpine-minirootfs.tar.gz`:

```sh
mkdir rootfs-base
sudo tar -xzf alpine-minirootfs.tar.gz -C rootfs-base
sudo cp -a rootfs-base rootfs-alpha
sudo cp -a rootfs-base rootfs-beta
sudo cp memory_hog cpu_hog io_pulse rootfs-alpha/
sudo cp memory_hog cpu_hog io_pulse rootfs-beta/
```

Each live container needs a separate writable rootfs. The supervisor rejects aliases and symlinks pointing to a directory already in use. Relative paths are resolved using the CLI's working directory. Do not modify or rename rootfs directories while in use.

## Start the supervisor

```sh
sudo insmod monitor.ko       # optional; required for memory monitoring
sudo ./engine supervisor
```

The legacy `supervisor <base-rootfs>` spelling remains accepted; that argument is unused. Each `start` or `run` supplies its own rootfs.

The default directory is `/run/mini-runtime`, containing:

- `control.sock`: private Unix socket, restricted to the supervisor user.
- `supervisor.lock`: prevents another supervisor from replacing the live socket.
- `logs/<id>.log`: captured stdout/stderr, with mode 0600.

The runtime directory must be owned by root with mode 0700. Set `MINI_RUNTIME_DIR` to the **same absolute path** for the supervisor and every client to override it:

```sh
sudo env MINI_RUNTIME_DIR=/run/my-runtime ./engine supervisor
sudo env MINI_RUNTIME_DIR=/run/my-runtime ./engine ps
```

`/run` usually clears on reboot. Choose a private directory on persistent storage if logs must survive reboots. Excessively long Unix socket paths are rejected.

Without the module, the supervisor warns that **memory limits are not enforced**. Other runtime features remain available.

## Commands

```sh
# Launch in the background
sudo ./engine start alpha ./rootfs-alpha /memory_hog --soft-mib 48 --hard-mib 80

# Wait for completion and return the command's exit status
sudo ./engine run beta ./rootfs-beta /bin/echo hello
sudo ./engine logs beta

# Command flags work; -- makes all remaining arguments literal
sudo ./engine run shell ./rootfs-beta /bin/sh -c 'echo hello; exit 7'
sudo ./engine run literal ./rootfs-beta /bin/echo -- --soft-mib literal

sudo ./engine ps
sudo ./engine stop alpha
```

- IDs contain 1–63 letters, digits, underscores, or hyphens and begin with a letter or digit. IDs remain reserved during a supervisor session.
- Commands must be absolute paths inside the rootfs. Up to eight command arguments are supported; excessive or overlong arguments are rejected rather than truncated.
- Defaults are **40 MiB soft / 64 MiB hard**. Limits must be positive integers with `soft < hard`; overflow is rejected. `--nice` accepts -20 through 19 and sets an absolute priority independent of the supervisor's priority.
- `start` reports process creation. If setup or execution subsequently fails, inspect `ps` and `logs`. `run` returns the exit status: 127 for execution failure, or `128 + signal` for signal termination.
- `stop` acknowledges immediately, sends SIGTERM, and escalates to SIGKILL after two seconds if necessary. Use `ps` to observe completion.
- Stdin is `/dev/null`; interactive shells are not supported. `logs` streams a binary-safe snapshot without the old 8 KiB truncation. A completed `run` guarantees its queued logs have been written.
- Up to 64 records, including exited containers, are retained. Restart the supervisor to clear history. Existing logs remain readable after restart; starting the same ID in a new session replaces its old log.

Press Ctrl+C in the supervisor terminal or send it SIGTERM for orderly shutdown. It stops containers, escalates if needed, reaps children, joins logging threads, drains the queue, and removes the socket. Afterwards, unload the optional module with `sudo rmmod monitor`.

## Memory monitoring

The module samples the **registered process's RSS** every 100 ms. It does not sum descendant memory or provide a cgroup-style ceiling. Brief spikes can be missed. Run `memory_hog` directly as the container command for these experiments.

```sh
# Warning-only experiment
sudo ./engine start softtest ./rootfs-alpha /memory_hog 1 500 --soft-mib 2 --hard-mib 100
sudo dmesg | grep 'SOFT LIMIT'
sudo ./engine stop softtest
# Wait for ps to show stopped before reusing this rootfs.

# Hard-limit experiment; expected exit status 137
sudo ./engine run hardtest ./rootfs-beta /memory_hog 1 100 --soft-mib 2 --hard-mib 8
sudo dmesg | grep 'HARD LIMIT'
```

A SIGKILL exit is labeled `killed`. The supervisor cannot distinguish the monitor from the OOM killer or another signal sender; confirm hard-limit events in the kernel log.

The monitor uses delayed work in process context, holds PID references to avoid signaling reused PIDs, validates ioctl input, limits registrations, and requires `CAP_SYS_ADMIN` in the initial user namespace. The ioctl ABI uses fixed-width, aligned fields for 32/64-bit compatibility.

**Upgrade:** rebuild both `engine` and `monitor.ko`, stop the old supervisor, and unload/reload the module. The ioctl layout and private client protocol changed. The previous `/tmp/mini_runtime.sock` and working-directory `logs/` paths are no longer used. Existing log files are preserved but not automatically migrated.

## Workloads and scheduling

```text
cpu_hog [seconds]                          default: 10
memory_hog [chunk_mib] [sleep_ms]           defaults: 8, 1000
io_pulse [iterations] [sleep_ms] [path]     defaults: 20, 200, /tmp/io_pulse.out
```

Arguments are validated for invalid values and overflow. `memory_hog` retains allocations and uses volatile page writes so optimization cannot eliminate RSS growth. It exits on allocation failure instead of spinning. All workloads handle SIGTERM and SIGINT, including as PID 1.

For scheduling comparisons, run two `cpu_hog` containers with different `--nice` values and pin their host PIDs to the **same available CPU** using `taskset -pc`. Observe `ps -o pid,ni,comm,%cpu -p <pid1>,<pid2>`. A useful comparison requires CPU contention and a sufficiently long observation window. `memory_hog` sleeps between allocations and is not a good CPU-share benchmark. Nice values affect scheduling weight; they do not impose CPU quotas.

## Validation

```sh
make test                 # CLI/protocol/workload tests; no root
sudo make integration     # also exercises real namespaces and supervision
make sanitize             # AddressSanitizer + UndefinedBehaviorSanitizer
sudo make integration     # exercise the sanitizer build as well
make clean && make        # restore normal static workload builds
shellcheck environment-check.sh tests/module-smoke.sh
sudo bash environment-check.sh  # build/load/device/unload preflight in a VM
make tests/monitor_probe
sudo sh tests/module-smoke.sh   # memory-limit/module lifecycle checks in a VM
```

CI builds on Ubuntu 22.04 and 24.04, compiles the module against distribution headers, runs unprivileged and privileged regressions, and checks sanitizer builds. Module loading and real enforcement are separate VM checks: hosted runner kernels may not match installed headers.

Tests cover argument forwarding, invalid/truncated IPC, binary logs larger than the queue, rootfs aliases, descriptor leaks, disconnected clients, duplicate supervisors, nonblocking stop, shutdown escalation, and actual RSS growth. They use private temporary runtime directories and a static fixture; no downloaded rootfs is needed.

## Implementation map

| File | Purpose |
| --- | --- |
| `engine.c` | CLI, supervisor, namespaces, lifecycle and bounded logging queue |
| `monitor.c` | Optional character device and delayed memory worker |
| `monitor_ioctl.h` | Shared ioctl ABI |
| `workload_common.h` | Workload validation, signals and interruptible sleep |
| `tests/` | Regression tests and VM module smoke check |
| `.github/workflows/ci.yml` | Linux builds, tests and shell checks |

Logging uses one joinable producer per container and a single consumer. An ordered end-of-log marker ensures the last byte is consumed before returning the exit status. Shutdown never destroys synchronization objects while producers can still use them.

Original demonstration images remain in [`screenshots/`](screenshots/). They show an earlier version's output and paths; the instructions above describe the current implementation.
