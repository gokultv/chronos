package main

import (
	"compress/gzip"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"chronos/pkg/storage"
)

var s3Client *storage.S3Client

type SearchResult struct {
	Timestamp string `json:"timestamp"`
	Source    string `json:"source"`
	Message   string `json:"message"`
}

type SearchResponse struct {
	Matches []SearchResult `json:"matches"`
	Stats   SearchStats    `json:"stats"`
}

type SearchStats struct {
	ScannedSegments int    `json:"scanned_segments"`
	ScannedEvents   int    `json:"scanned_events"`
	Duration        string `json:"duration"`
	MatchCount      int    `json:"match_count"`
}

func main() {
	var err error
	s3Client, err = storage.NewS3Client("localhost:9000", "minioadmin", "minioadmin", "chronos-logs")
	if err != nil {
		log.Fatalf("Failed to initialize S3 client: %v", err)
	}

	if err := s3Client.EnsureBucket(); err != nil {
		log.Fatalf("Failed to ensure bucket exists: %v", err)
	}

	// Serve static files (UI)
	fs := http.FileServer(http.Dir("./ui"))
	http.Handle("/", fs)

	// API Endpoint
	http.HandleFunc("/search", handleSearch)

	port := ":8082"
	log.Printf("Coordinator service starting on %s", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal(err)
	}
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	sourceFilter := r.URL.Query().Get("source")
	containsFilter := r.URL.Query().Get("contains")
	// dataDir := "data" // Unused now that we use S3

	if sourceFilter == "" && containsFilter == "" {
		http.Error(w, "Please provide 'source' or 'contains' query parameter", http.StatusBadRequest)
		return
	}

	// List files from S3
	files, err := s3Client.ListFiles("")
	if err != nil {
		log.Printf("Failed to list S3 files: %v", err)
		http.Error(w, "Failed to list data files", http.StatusInternalServerError)
		return
	}

	start := time.Now()
	matches := make([]SearchResult, 0)
	scannedEvents := 0

	for _, file := range files {
		block, err := loadBlock(file)
		if err != nil {
			log.Printf("Error loading block %s: %v", file, err)
			continue
		}

		scannedEvents += block.Size()

		// Columnar Scan Logic
		matchedIndices := make([]int, 0)

		if sourceFilter != "" {
			for i, src := range block.Sources {
				if src == sourceFilter {
					if containsFilter != "" {
						if strings.Contains(block.Messages[i], containsFilter) {
							matchedIndices = append(matchedIndices, i)
						}
					} else {
						matchedIndices = append(matchedIndices, i)
					}
				}
			}
		} else if containsFilter != "" {
			for i, msg := range block.Messages {
				if strings.Contains(msg, containsFilter) {
					matchedIndices = append(matchedIndices, i)
				}
			}
		}

		// Reconstruct results
		for _, idx := range matchedIndices {
			matches = append(matches, SearchResult{
				Timestamp: time.UnixMilli(block.Timestamps[idx]).Format(time.RFC3339),
				Source:    block.Sources[idx],
				Message:   block.Messages[idx],
			})
		}
	}

	resp := SearchResponse{
		Matches: matches,
		Stats: SearchStats{
			ScannedSegments: len(files),
			ScannedEvents:   scannedEvents,
			Duration:        time.Since(start).String(),
			MatchCount:      len(matches),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func loadBlock(objectName string) (*storage.Block, error) {
	// Stream directly from S3
	obj, err := s3Client.GetObject(objectName)
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	gr, err := gzip.NewReader(obj)
	if err != nil {
		return nil, err
	}
	defer gr.Close()

	var block storage.Block
	if err := json.NewDecoder(gr).Decode(&block); err != nil {
		return nil, err
	}
	return &block, nil
}
