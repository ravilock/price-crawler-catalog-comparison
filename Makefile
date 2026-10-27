.PHONY: test build image

IMAGE_NAME ?= $(notdir $(CURDIR))

test:
	go test ./...

build:
	go build ./...

image:
	docker build -t $(IMAGE_NAME):local .
