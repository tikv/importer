SHELL := /bin/bash
ENABLE_FEATURES ?=

# Pick an allocator
ifeq ($(TCMALLOC),1)
ENABLE_FEATURES += tcmalloc
else ifeq ($(SYSTEM_ALLOC),1)
# no feature needed for system allocator
else
ENABLE_FEATURES += jemalloc
endif

# Disable portable on MacOS to sidestep the compiler bug in clang 4.9
ifeq ($(shell uname -s),Darwin)
ROCKSDB_SYS_PORTABLE=0
endif

# Build portable binary by default unless disable explicitly
ifneq ($(ROCKSDB_SYS_PORTABLE),0)
ENABLE_FEATURES += portable
endif

# Enable sse4.2 by default unless disable explicitly
ifneq ($(ROCKSDB_SYS_SSE),0)
ENABLE_FEATURES += sse
endif

# ifneq ($(FAIL_POINT),1)
# ENABLE_FEATURES += no-fail
# endif

BUILD_INFO_GIT_FALLBACK := "Unknown (no git or not git repo)"
BUILD_INFO_RUSTC_FALLBACK := "Unknown"
export TIKV_BUILD_TIME := $(shell date -u '+%Y-%m-%d %I:%M:%S')
export TIKV_BUILD_GIT_HASH := $(shell git rev-parse HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})
export TIKV_BUILD_GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})
export TIKV_BUILD_RUSTC_VERSION := $(shell rustc --version 2> /dev/null || echo ${BUILD_INFO_RUSTC_FALLBACK})

.PHONY: all pre-clippy clippy pre-format format pre-audit audit clean build

all: format build test

pre-clippy: unset-override
	@rustup component add clippy

clippy: pre-clippy
	@cargo clippy --all --all-targets

pre-format: unset-override
	@rustup component add rustfmt

format: pre-format
	@cargo fmt --all -- --check >/dev/null || \
	cargo fmt --all

pre-audit:
	$(eval LATEST_AUDIT_VERSION := $(strip $(shell cargo search cargo-audit | head -n 1 | awk '{ gsub(/"/, "", $$3); print $$3 }')))
	$(eval CURRENT_AUDIT_VERSION = $(strip $(shell (cargo audit --version 2> /dev/null || echo "noop 0") | awk '{ print $$2 }')))
	@if [ "$(LATEST_AUDIT_VERSION)" != "$(CURRENT_AUDIT_VERSION)" ]; then \
		cargo install cargo-audit --force; \
	fi

audit: pre-audit
	cargo audit

clean:
	cargo clean

build:
	cargo build --no-default-features --features "${ENABLE_FEATURES}"

test:
	cargo test --no-default-features --features "${ENABLE_FEATURES}" --all ${EXTRA_CARGO_ARGS} -- --nocapture

release:
	cargo build --no-default-features --release --features "${ENABLE_FEATURES}"

unset-override:
	@# unset first in case of any previous overrides
	@if rustup override list | grep `pwd` > /dev/null; then rustup override unset; fi
