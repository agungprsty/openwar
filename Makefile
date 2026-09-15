.PHONY: build gateway worker backend-demo seed up down logs test docker-up docker-down

build:
	go build ./...

gateway:
	go run ./cmd/gateway serve $(ARGS)

worker:
	go run ./cmd/worker

backend-demo:
	go run ./cmd/backend-demo

seed:
	go run ./cmd/gateway seed-events --event flash-sale-001 --qty 1000 --shards 32

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