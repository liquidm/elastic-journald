PACKAGE := github.com/liquidm/elastic_journald

# http://stackoverflow.com/questions/322936/common-gnu-makefile-directory-path#comment11704496_324782
TOP := $(dir $(CURDIR)/$(word $(words $(MAKEFILE_LIST)),$(MAKEFILE_LIST)))

GOOP=goop
GO=$(GOOP) exec go
GOFMT=gofmt -w

GOFILES=$(shell git ls-files | grep '\.go$$')
MAINGO=$(wildcard main/*.go)
MAIN=$(patsubst main/%.go,%,$(MAINGO))

.PHONY: build run watch clean test fmt dep

all: build

build: fmt
	$(GO) build $(MAINGO)

run: build
	./$(MAIN)

watch:
	go get github.com/cespare/reflex
	reflex -t10s -r '\.go$$' -s -- sh -c 'make build test && ./$(MAIN)'

clean:
	$(GO) clean
	rm -f $(MAIN)

test: build
	go get github.com/smartystreets/goconvey
	$(GO) test

fmt:
	$(GOFMT) $(GOFILES)

dep:
	go get github.com/nitrous-io/goop
	goop install
	mkdir -p $(dir $(TOP)/.vendor/src/$(PACKAGE))
	ln -nfs $(TOP) $(TOP)/.vendor/src/$(PACKAGE)
