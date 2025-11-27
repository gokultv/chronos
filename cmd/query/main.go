package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"chronos/pkg/storage"
)

func main() {
	sourceFilter := flag.String("source", "", "Filter by source (exact match)")
	containsFilter := flag.String("contains", "", "Filter by message content (substring)")
	dataDir := flag.String("data", "data", "Directory containing data segments")
	flag.Parse()

	if *sourceFilter == "" && *containsFilter == "" {
		fmt.Println("Please provide at least one filter: -source or -contains")
		os.Exit(1)
	}

	files, err := filepath.Glob(filepath.Join(*dataDir, "*.json.gz"))
	if err != nil {
		fmt.Printf("Error listing files: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Scanning %d segments...\n", len(files))
	start := time.Now()
	matches := 0
	scanned := 0

	for _, file := range files {
		block, err := loadBlock(file)
		if err != nil {
			fmt.Printf("Error loading block %s: %v\n", file, err)
			continue
		}

		scanned += block.Size()

		// Columnar Scan
		// We find indices that match the criteria
		matchedIndices := make([]int, 0)

		// Optimization: If filtering by source, only scan the Sources column
		if *sourceFilter != "" {
			for i, src := range block.Sources {
				if src == *sourceFilter {
					// If also filtering by content, check that too
					if *containsFilter != "" {
						if strings.Contains(block.Messages[i], *containsFilter) {
							matchedIndices = append(matchedIndices, i)
						}
					} else {
						matchedIndices = append(matchedIndices, i)
					}
				}
			}
		} else if *containsFilter != "" {
			// Only filtering by content
			for i, msg := range block.Messages {
				if strings.Contains(msg, *containsFilter) {
					matchedIndices = append(matchedIndices, i)
				}
			}
		}

		// Print matches
		for _, idx := range matchedIndices {
			matches++
			fmt.Printf("[%s] %s: %s\n",
				time.UnixMilli(block.Timestamps[idx]).Format(time.RFC3339),
				block.Sources[idx],
				block.Messages[idx],
			)
		}
	}

	fmt.Printf("\nScanned %d events in %v. Found %d matches.\n", scanned, time.Since(start), matches)
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
