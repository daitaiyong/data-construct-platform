package id

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	epochMillis  = int64(1704067200000)
	nodeBits     = 10
	sequenceBits = 12
	maxNodeID    = (1 << nodeBits) - 1
	maxSequence  = (1 << sequenceBits) - 1
)

type Generator struct {
	mu         sync.Mutex
	nodeID     int64
	lastMillis int64
	sequence   int64
}

func NewGenerator(nodeID int64) (*Generator, error) {
	if nodeID < 0 || nodeID > maxNodeID {
		return nil, fmt.Errorf("snowflake node ID must be between 0 and %d", maxNodeID)
	}
	return &Generator{nodeID: nodeID}, nil
}

func (g *Generator) Next() (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now().UnixMilli()
	if now < g.lastMillis {
		return "", fmt.Errorf("system clock moved backwards by %dms", g.lastMillis-now)
	}
	if now == g.lastMillis {
		g.sequence = (g.sequence + 1) & maxSequence
		if g.sequence == 0 {
			for now <= g.lastMillis {
				time.Sleep(time.Millisecond)
				now = time.Now().UnixMilli()
			}
		}
	} else {
		g.sequence = 0
	}
	if now < epochMillis {
		return "", fmt.Errorf("system clock precedes snowflake epoch")
	}
	g.lastMillis = now
	value := ((now - epochMillis) << (nodeBits + sequenceBits)) |
		(g.nodeID << sequenceBits) | g.sequence
	return strconv.FormatInt(value, 10), nil
}

var (
	defaultOnce sync.Once
	defaultGen  *Generator
	defaultErr  error
)

func New() (string, error) {
	defaultOnce.Do(func() {
		nodeID := int64(1)
		if value := os.Getenv("SNOWFLAKE_NODE_ID"); value != "" {
			nodeID, defaultErr = strconv.ParseInt(value, 10, 64)
			if defaultErr != nil {
				defaultErr = fmt.Errorf("parse SNOWFLAKE_NODE_ID: %w", defaultErr)
				return
			}
		}
		defaultGen, defaultErr = NewGenerator(nodeID)
	})
	if defaultErr != nil {
		return "", defaultErr
	}
	return defaultGen.Next()
}

func Valid(value string) bool {
	parsed, err := strconv.ParseInt(value, 10, 64)
	return err == nil && parsed > 0 && strconv.FormatInt(parsed, 10) == value
}
