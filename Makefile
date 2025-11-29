.PHONY: run-ingestor run-indexer up down

run-ingestor:
	go run services/ingestor/main.go

run-indexer:
	go run services/indexer/main.go

run-query:
	go run cmd/query/main.go $(ARGS)

run-coordinator:
	go run cmd/coordinator/main.go

up:
	docker compose up -d

down:
	docker compose down
