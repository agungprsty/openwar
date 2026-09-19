ifneq (,$(wildcard ./.env))
    include .env
    export
endif

.PHONY: build gateway worker backend-demo seed up down logs test docker-up docker-down

build:
	go build ./...

gateway:
	go run ./cmd/gateway serve $(ARGS)

worker:
	go run ./cmd/worker

backend-demo:
	go run ./cmd/backend-demo

EVENT ?= flash-sale-001
QTY ?= 1000
SHARDS ?= 32

seed:
	go run ./cmd/gateway seed-events --event $(EVENT) --qty $(QTY) --shards $(SHARDS)

up:
	docker compose -f deploy/docker-compose.yml up --build

down:
	docker compose -f deploy/docker-compose.yml down

logs:
	docker compose -f deploy/docker-compose.yml logs -f

docker-up:
	docker compose -f deploy/docker-compose.yml --profile tools up --build

test:
	result=$$(go vet ./... && go build ./...); echo "vet+build ok"

fmt:
	gofmt -l -w cmd internal

load-test:
	docker compose -f deploy/docker-compose.yml --profile tools run --rm -e EVENT=$(EVENT) k6

validate:
	./test/validate.sh $(EVENT) $(QTY)