package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"chronos/pkg/storage"
	"chronos/pkg/types"

	"github.com/segmentio/kafka-go"
)

const (
	topic         = "chronos-logs"
	brokerAddress = "localhost:19092"
	groupID       = "indexer-group-1"
)

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer r.Close()

	log.Println("Indexer service started, consuming...")

	// Initialize MemTable (Block)
	memTable := storage.NewBlock()
	dataDir := "data"

	// Handle graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigchan
		log.Println("Shutting down indexer...")
		r.Close()
		os.Exit(0)
	}()

	for {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			break
		}

		var event types.Event
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			// Even if unmarshal fails, we should commit to avoid stuck loop?
			// Or maybe DLQ? For now, let's commit to move on.
			if err := r.CommitMessages(context.Background(), m); err != nil {
				log.Printf("Failed to commit message: %v", err)
			}
			continue
		}

		log.Printf("Indexed Event: ID=%s TS=%d Source=%s Msg=%s\n", event.ID, event.Timestamp, event.Source, event.Message)

		// Add to MemTable
		memTable.Add(event)

		// Check flush threshold
		if memTable.Size() >= 100 {
			log.Println("Flushing block to disk...")
			file, err := memTable.Flush(dataDir)
			if err != nil {
				log.Printf("Failed to flush block: %v", err)
			} else {
				log.Printf("Flushed %d events to %s", memTable.Size(), file)
				// Reset MemTable
				memTable = storage.NewBlock()
			}
		}

		// Commit offset after processing
		if err := r.CommitMessages(context.Background(), m); err != nil {
			log.Printf("Failed to commit message: %v", err)
		}
	}
}
