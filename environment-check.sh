#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

ok() { echo "[OK] $1"; }
warn() { echo "[WARN] $1"; }
fail() { echo "[FAIL] $1" >&2; exit 1; }

case "${1:-}" in
    --build-only)
        [[ $# -eq 1 ]] || fail "Usage: $0 [--build-only]"
        [[ "$(uname -s)" == Linux ]] || fail "User-space builds require Linux."
        command -v make >/dev/null || fail "Install make."
        command -v "${CC:-cc}" >/dev/null || fail "Install a C compiler."
        make ci
        ok "User-space build complete; kernel headers, root and a VM are not required."
        exit 0
        ;;
    "") ;;
    *) fail "Usage: $0 [--build-only]" ;;
esac

[[ "$(uname -s)" == Linux ]] || fail "Kernel-module checks require Linux."
[[ "$(id -u)" -eq 0 ]] || fail "Run sudo bash environment-check.sh for module checks."
if grep -qi microsoft /proc/sys/kernel/osrelease /proc/version 2>/dev/null; then
    fail "Use a Linux VM with matching kernel headers for module loading. WSL supports --build-only."
fi
if command -v systemd-detect-virt >/dev/null 2>&1; then
    VIRT="$(systemd-detect-virt || true)"
    [[ "$VIRT" != none ]] || warn "Use a disposable VM for testing this educational kernel module."
fi
if command -v mokutil >/dev/null 2>&1; then
    SB_STATE="$(mokutil --sb-state 2>/dev/null || true)"
    if grep -qi "SecureBoot enabled" <<< "$SB_STATE"; then
        fail "Secure Boot is enabled. Sign the module or disable Secure Boot in the test VM."
    fi
fi
KBUILD_DIR="/lib/modules/$(uname -r)/build"
[[ -d "$KBUILD_DIR" ]] || fail "Install matching kernel headers for $(uname -r)."
make all
ok "User-space and kernel-module build succeeded."

INSERTED_BY_SCRIPT=0
cleanup() {
    if [[ "$INSERTED_BY_SCRIPT" -eq 1 ]]; then
        rmmod monitor || true
    fi
}
trap cleanup EXIT
if [[ -d /sys/module/monitor ]]; then
    warn "monitor is already loaded; unload/reload it manually to test the newly built ABI."
else
    insmod ./monitor.ko
    INSERTED_BY_SCRIPT=1
    ok "insmod monitor.ko succeeded."
fi
for _ in {1..10}; do
    [[ -c /dev/container_monitor ]] && break
    sleep 0.2
 done
[[ -c /dev/container_monitor ]] || fail "/dev/container_monitor is not a character device."
ok "Control device exists."
if [[ "$INSERTED_BY_SCRIPT" -eq 1 ]]; then
    rmmod monitor
    INSERTED_BY_SCRIPT=0
    ok "rmmod monitor succeeded."
fi
ok "Preflight passed."
