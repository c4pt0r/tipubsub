default: build

build: export GO111MODULE=on
build:
ifeq ($(TAGS),)
	$(CGO_FLAGS) go build -o bin/cli ./cmd/cli/main.go
else
	$(CGO_FLAGS) go build -tags "$(TAGS)" -o bin/cli ./cmd/cli/main.go
endif
