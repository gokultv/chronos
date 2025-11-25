package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
	"chronos/pkg/types"
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
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			break
		}

		var event types.Event
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			continue
		}

		log.Printf("Indexed Event: TS=%d Source=%s Msg=%s\n", event.Timestamp, event.Source, event.Message)
	}
}
