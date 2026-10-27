.PHONY: test lint build image

IMAGE_NAME ?= $(notdir $(CURDIR))

test:
	go test ./...

lint:
	test -z "$$(gofmt -l .)"
	go vet ./...

build:
	go build ./...

image:
	docker build -t $(IMAGE_NAME):local .
