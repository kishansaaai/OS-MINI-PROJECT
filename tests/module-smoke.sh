#!/bin/sh
# Run only in a disposable Linux VM, with a matching, already-built monitor.ko.
set -eu
cd "$(dirname "$0")/.."
[ "$(id -u)" -eq 0 ] || { echo 'Run this test as root in a disposable VM.' >&2; exit 1; }
[ ! -d /sys/module/monitor ] || { echo 'Unload the existing monitor before this test.' >&2; exit 1; }
for file in engine memory_hog monitor.ko tests/monitor_probe; do
    [ -f "$file" ] || { echo "Missing $file; run make all tests/monitor_probe first." >&2; exit 1; }
done

test_dir=$(mktemp -d /tmp/mcr-module.XXXXXX)
export MINI_RUNTIME_DIR="$test_dir/runtime"
supervisor_pid=
inserted=0
cleanup() {
    if [ -n "$supervisor_pid" ]; then
        kill -TERM "$supervisor_pid" 2>/dev/null || true
        wait "$supervisor_pid" || true
    fi
    if [ "$inserted" -eq 1 ]; then rmmod monitor || true; fi
    # Only remove the private directory created by mktemp above.
    rm -rf -- "$test_dir"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

insmod ./monitor.ko
inserted=1
tries=0
while [ ! -c /dev/container_monitor ]; do
    tries=$((tries + 1))
    [ "$tries" -lt 50 ] || { echo 'Monitor device did not appear.' >&2; exit 1; }
    sleep 0.1
done
./tests/monitor_probe
mkdir -p "$test_dir/root/proc"
cp ./memory_hog "$test_dir/root/"
./engine supervisor >"$test_dir/supervisor.log" 2>&1 &
supervisor_pid=$!
tries=0
while [ ! -S "$MINI_RUNTIME_DIR/control.sock" ]; do
    tries=$((tries + 1))
    [ "$tries" -lt 50 ] || { cat "$test_dir/supervisor.log"; exit 1; }
    sleep 0.1
done

soft_id="soft-$$"
hard_id="hard-$$"
./engine start "$soft_id" "$test_dir/root" /memory_hog 1 200 --soft-mib 1 --hard-mib 128
tries=0
until dmesg | grep -q "SOFT LIMIT container=$soft_id "; do
    tries=$((tries + 1))
    [ "$tries" -lt 100 ] || { echo 'Soft-limit warning missing.' >&2; exit 1; }
    sleep 0.1
done
./engine ps | grep "$soft_id" | grep -q running
./engine stop "$soft_id"
tries=0
until ./engine ps | grep "$soft_id" | grep -q stopped; do
    tries=$((tries + 1))
    [ "$tries" -lt 100 ] || { echo 'Soft test failed to stop.' >&2; exit 1; }
    sleep 0.1
done

if timeout 15 ./engine run "$hard_id" "$test_dir/root" /memory_hog 1 50 --soft-mib 1 --hard-mib 4; then
    result=0
else
    result=$?
fi
[ "$result" -eq 137 ] || { echo "Expected hard-limit exit 137, got $result" >&2; exit 1; }
dmesg | grep "HARD LIMIT container=$hard_id "
./engine ps | grep "$hard_id" | grep -q killed
kill -TERM "$supervisor_pid"
wait "$supervisor_pid"
supervisor_pid=
[ ! -S "$MINI_RUNTIME_DIR/control.sock" ]
rmmod monitor
inserted=0
# Exercise worker cancellation and repeated module lifecycle.
insmod ./monitor.ko
inserted=1
sleep 0.2
rmmod monitor
inserted=0
echo MODULE_SMOKE_PASS
