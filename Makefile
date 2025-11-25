.PHONY: run-ingestor run-indexer up down

run-ingestor:
	go run services/ingestor/main.go

run-indexer:
	go run services/indexer/main.go

up:
	docker compose up -d

down:
	docker compose down
