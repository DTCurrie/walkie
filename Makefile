MODULE_BINARY := bin/walkie

# Fall back to the host platform so a bare `make` works locally, not just under
# Viam cloud build (which sets VIAM_BUILD_OS / VIAM_BUILD_ARCH).
TARGET_OS   ?= $(shell go env GOOS)
TARGET_ARCH ?= $(shell go env GOARCH)
ifneq ($(VIAM_BUILD_OS),)
	TARGET_OS := $(VIAM_BUILD_OS)
endif
ifneq ($(VIAM_BUILD_ARCH),)
	TARGET_ARCH := $(VIAM_BUILD_ARCH)
endif

# This module is pure Go: the device I/O lives in viam:system-audio, so there is
# nothing here to link against and CGO is not required on any target.
GO_BUILD_ENV := CGO_ENABLED=0
GO_LDFLAGS   := -s -w

ifeq ($(TARGET_OS), windows)
	MODULE_BINARY := bin/walkie.exe
endif

# Every Go file matters, not just the ones next to the Makefile: without this,
# edits under internal/ would not trigger a rebuild.
GO_SOURCES := $(shell find . -name '*.go' -not -path './bin/*')

# The binary path is fixed (meta.json's entrypoint is bin/walkie), so make
# cannot tell one platform's build from another by filename. Without this stamp a
# `make build` on a dev machine followed by `viam module reload` to a Linux part
# -- which sets VIAM_BUILD_OS and re-runs make -- would find bin/walkie newer
# than its sources, skip the compile, and ship a darwin binary to Linux. The file
# is only rewritten when the platform actually changes, so same-platform builds
# stay incremental.
bin/.platform: FORCE
	@mkdir -p bin
	@echo '$(TARGET_OS)/$(TARGET_ARCH)' | cmp -s - $@ || echo '$(TARGET_OS)/$(TARGET_ARCH)' > $@
FORCE:
.PHONY: FORCE

$(MODULE_BINARY): Makefile go.mod go.sum bin/.platform $(GO_SOURCES)
	GOOS=$(TARGET_OS) GOARCH=$(TARGET_ARCH) $(GO_BUILD_ENV) \
		go build -ldflags '$(GO_LDFLAGS)' -o $(MODULE_BINARY) ./cmd/module

.PHONY: build lint test update setup module all clean system-audio dev-config
build: $(MODULE_BINARY)

# This module provides no audio hardware of its own; pair it with system-audio.
#
# viam-server only downloads "registry" modules for cloud-managed machines, so
# running from a plain local config file needs the module fetched by hand and
# referenced by absolute path. That is what "make dev-config" wires up.
SYSTEM_AUDIO_DIR ?= $(HOME)/.viam/local-modules/system-audio
SYSTEM_AUDIO_PLATFORM ?= $(shell go env GOOS)/$(shell go env GOARCH)

system-audio:
	rm -rf $(SYSTEM_AUDIO_DIR) $(SYSTEM_AUDIO_DIR).tmp
	mkdir -p $(SYSTEM_AUDIO_DIR).tmp
	viam module download --id viam:system-audio --version latest \
		--platform $(SYSTEM_AUDIO_PLATFORM) --destination $(SYSTEM_AUDIO_DIR).tmp
	mkdir -p $(SYSTEM_AUDIO_DIR)
	tar xzf $(SYSTEM_AUDIO_DIR).tmp/*/*.tar.gz -C $(SYSTEM_AUDIO_DIR)
	rm -rf $(SYSTEM_AUDIO_DIR).tmp
	@echo "installed to $(SYSTEM_AUDIO_DIR)"

# Materialise the developer config with this checkout's absolute paths, so no
# machine-specific path has to live in version control.
DEV_CONFIG := etc/dev-single-machine.local.json

dev-config: etc/dev-single-machine.json
	sed -e 's|REPLACE-WITH-HOME|$(HOME)|g' \
	    -e 's|REPLACE-WITH-REPO-ROOT|$(CURDIR)|g' \
	    etc/dev-single-machine.json > $(DEV_CONFIG)
	@echo "wrote $(DEV_CONFIG)"
	@echo "run: viam-server -config $(DEV_CONFIG) -debug"

lint:
	gofmt -s -w .
	go vet ./...
	golangci-lint run ./...

test:
	go test ./... -race

update:
	go get go.viam.com/rdk@latest
	go mod tidy

setup:
	go mod tidy

FIRST_RUN := $(shell jq -r '.first_run // empty' meta.json 2>/dev/null)
TAR_FILES := meta.json $(MODULE_BINARY)
ifneq ($(FIRST_RUN),)
	TAR_FILES += $(FIRST_RUN)
endif

# -ldflags '-s -w' already strips the binary at link time, so there is no
# separate strip step to get wrong.
module.tar.gz: meta.json $(MODULE_BINARY)
	tar czf $@ $(TAR_FILES)

module: test module.tar.gz
all: test module.tar.gz

clean:
	rm -rf bin module.tar.gz
