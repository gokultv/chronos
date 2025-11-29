package main

import (
	"compress/gzip"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"chronos/pkg/storage"
)

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
	dataDir := "data"

	if sourceFilter == "" && containsFilter == "" {
		http.Error(w, "Please provide 'source' or 'contains' query parameter", http.StatusBadRequest)
		return
	}

	files, err := filepath.Glob(filepath.Join(dataDir, "*.json.gz"))
	if err != nil {
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

func loadBlock(path string) (*storage.Block, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
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
