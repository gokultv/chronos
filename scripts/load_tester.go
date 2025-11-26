package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Event struct {
	Message string `json:"msg"`
	Source  string `json:"source"`
}

func main() {
	url := "http://localhost:8081/ingest"
	count := 120 // Enough to trigger flush (threshold is 100)

	var wg sync.WaitGroup
	start := time.Now()

	fmt.Printf("Sending %d events to %s...\n", count, url)

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			event := Event{
				Message: fmt.Sprintf("Load test event %d", id),
				Source:  "load-tester",
			}
			payload, _ := json.Marshal(event)

			resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
			if err != nil {
				fmt.Printf("Error sending event %d: %v\n", id, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				fmt.Printf("Error response for event %d: %s\n", id, resp.Status)
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("Done! Sent %d events in %v\n", count, time.Since(start))
}
