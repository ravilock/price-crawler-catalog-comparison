.PHONY: test lint build image

IMAGE_NAME ?= $(notdir $(CURDIR))

test:
	go test ./...

lint:
	golangci-lint run

build:
	go build ./...

image:
	docker build -t $(IMAGE_NAME):local .
