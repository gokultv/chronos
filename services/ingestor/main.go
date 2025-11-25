package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/google/uuid"
	"chronos/pkg/types"
)

const (
	topic         = "chronos-logs"
	brokerAddress = "localhost:19092"
)

func main() {
	// Initialize Kafka writer
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	http.HandleFunc("/ingest", func(wHttp http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(wHttp, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var event types.Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(wHttp, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// Basic validation
		if event.Timestamp == 0 {
			event.Timestamp = time.Now().UnixMilli()
		}
		if event.ID == "" {
			event.ID = uuid.New().String()
		}

		payload, err := json.Marshal(event)
		if err != nil {
			http.Error(wHttp, "Failed to marshal event", http.StatusInternalServerError)
			return
		}

		err = w.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(event.Source),
				Value: payload,
			},
		)
		if err != nil {
			log.Printf("Failed to write message: %v", err)
			http.Error(wHttp, "Failed to ingest event", http.StatusInternalServerError)
			return
		}

		wHttp.WriteHeader(http.StatusOK)
	})

	log.Println("Ingestor service starting on :8081")
	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatal(err)
	}
}
