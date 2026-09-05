obj-m += monitor.o

KDIR ?= /lib/modules/$(shell uname -r)/build
PROJECT_DIR := $(CURDIR)
CC ?= cc
CFLAGS ?= -O2 -g
WARNINGS := -Wall -Wextra -Wpedantic
# If you want host-built workload binaries to run directly inside an Alpine
# rootfs, you can override this with WORKLOAD_LDFLAGS=-static when your
# toolchain supports it.
WORKLOAD_LDFLAGS ?= -static

USER_TARGETS := engine memory_hog cpu_hog io_pulse

all: $(USER_TARGETS) module

ci: $(USER_TARGETS)

module:
	$(MAKE) -C "$(KDIR)" M="$(PROJECT_DIR)" modules

engine: engine.c monitor_ioctl.h
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) -pthread -o $@ engine.c $(LDFLAGS)

memory_hog: memory_hog.c workload_common.h
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) -o $@ memory_hog.c $(LDFLAGS) $(WORKLOAD_LDFLAGS)

cpu_hog: cpu_hog.c workload_common.h
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) -o $@ cpu_hog.c $(LDFLAGS) $(WORKLOAD_LDFLAGS)

io_pulse: io_pulse.c workload_common.h
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) -o $@ io_pulse.c $(LDFLAGS) $(WORKLOAD_LDFLAGS)

monitor.ko: module

tests/fixture: tests/fixture.c
	$(CC) $(CPPFLAGS) -O2 -g $(WARNINGS) -static -o $@ $<

tests/monitor_probe: tests/monitor_probe.c monitor_ioctl.h
	$(CC) $(CPPFLAGS) -O2 -g $(WARNINGS) -static -o $@ $<

test: ci tests/fixture
	python3 -m unittest discover -s tests -v

integration: ci tests/fixture
	MINI_RUNTIME_INTEGRATION=1 python3 -m unittest discover -s tests -v

sanitize:
	$(MAKE) clean
	$(MAKE) test CFLAGS="-O1 -g -Werror -fsanitize=address,undefined -fno-omit-frame-pointer -fno-pie" LDFLAGS="-no-pie" WORKLOAD_LDFLAGS=

clean:
	if [ -d "$(KDIR)" ]; then $(MAKE) -C "$(KDIR)" M="$(PROJECT_DIR)" clean; fi
	rm -f $(USER_TARGETS) tests/fixture tests/monitor_probe *.o *.ko *.mod *.mod.c *.symvers *.order .*.cmd

.PHONY: all ci module clean test integration sanitize
