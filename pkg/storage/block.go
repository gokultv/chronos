package storage

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"chronos/pkg/types"
)

// Block represents a columnar chunk of data in memory.
type Block struct {
	IDs        []string `json:"ids"`
	Timestamps []int64  `json:"timestamps"`
	Sources    []string `json:"sources"`
	Messages   []string `json:"messages"`
}

// NewBlock creates a new empty Block.
func NewBlock() *Block {
	return &Block{
		IDs:        make([]string, 0),
		Timestamps: make([]int64, 0),
		Sources:    make([]string, 0),
		Messages:   make([]string, 0),
	}
}

// Add appends an event to the block (pivoting it to columns).
func (b *Block) Add(event types.Event) {
	b.IDs = append(b.IDs, event.ID)
	b.Timestamps = append(b.Timestamps, event.Timestamp)
	b.Sources = append(b.Sources, event.Source)
	b.Messages = append(b.Messages, event.Message)
}

// Size returns the number of records in the block.
func (b *Block) Size() int {
	return len(b.IDs)
}

// Flush writes the block to a gzipped JSON file in the specified directory.
// It returns the filename created.
func (b *Block) Flush(dir string) (string, error) {
	if b.Size() == 0 {
		return "", nil
	}

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}

	// Generate filename: segment_<timestamp>.json.gz
	filename := fmt.Sprintf("segment_%d.json.gz", time.Now().UnixNano())
	path := filepath.Join(dir, filename)

	file, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create Gzip writer
	gw := gzip.NewWriter(file)
	defer gw.Close()

	// Encode Block to JSON and write to Gzip writer
	encoder := json.NewEncoder(gw)
	if err := encoder.Encode(b); err != nil {
		return "", fmt.Errorf("failed to encode block: %w", err)
	}

	return path, nil
}
