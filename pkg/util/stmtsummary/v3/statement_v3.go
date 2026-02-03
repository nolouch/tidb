// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package statementv3 implements a push-based SQL statement statistics
// collection system for TiDB with gRPC communication to Vector Extensions.
//
// Key features:
// - Push-based architecture: proactively pushes data instead of polling
// - HdrHistogram for accurate percentile calculation (P50/P95/P99)
// - Memory protection with configurable limits and eviction strategies
// - Circuit breaker for resilient push operations
// - Extensible metrics via dynamic extended_metrics map
package stmtsummaryv3

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"go.uber.org/zap"
)

var (
	// GlobalStatementV3 is the global StatementV3 instance.
	GlobalStatementV3 *StatementV3
)

// StatementV3 is the main component that orchestrates statement collection,
// aggregation, and pushing to Vector.
type StatementV3 struct {
	cfg *Config

	// Core components
	aggregator *Aggregator
	pusher     *Pusher

	// Instance identification
	clusterID  string
	instanceID string

	// State
	enabled atomic.Bool
	closed  atomic.Bool
	mu      sync.RWMutex
}

// Setup initializes the global StatementV3 instance.
func Setup(cfg *Config, clusterID, instanceID string) error {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	s := &StatementV3{
		cfg:        cfg,
		clusterID:  clusterID,
		instanceID: instanceID,
	}

	// Create aggregator
	s.aggregator = NewAggregator(cfg)

	// Set up push if enabled
	if cfg.Push.Enabled && cfg.Push.Endpoint != "" {
		pusher, err := NewPusher(cfg, clusterID, instanceID)
		if err != nil {
			logutil.BgLogger().Warn("failed to create statement pusher, push disabled",
				zap.Error(err))
		} else {
			s.pusher = pusher
			// Connect aggregator to pusher
			s.aggregator.SetOnFlush(func(window *AggregationWindow) {
				s.pusher.Push(window)
			})
		}
	}

	s.enabled.Store(cfg.Enabled)
	GlobalStatementV3 = s

	logutil.BgLogger().Info("statement v3 initialized",
		zap.Bool("enabled", cfg.Enabled),
		zap.Bool("pushEnabled", cfg.Push.Enabled),
		zap.String("endpoint", cfg.Push.Endpoint),
		zap.Duration("aggregationWindow", cfg.AggregationWindow))

	return nil
}

// Close shuts down the global StatementV3 instance.
func Close() {
	if GlobalStatementV3 != nil {
		GlobalStatementV3.close()
	}
}

// Add records a statement execution to the global instance.
func Add(info *stmtsummary.StmtExecInfo) {
	if GlobalStatementV3 == nil || !GlobalStatementV3.enabled.Load() {
		return
	}
	GlobalStatementV3.aggregator.Add(info)
}

// Enabled returns whether StatementV3 is enabled.
func Enabled() bool {
	if GlobalStatementV3 == nil {
		return false
	}
	return GlobalStatementV3.enabled.Load()
}

// SetEnabled enables or disables StatementV3.
func SetEnabled(v bool) {
	if GlobalStatementV3 == nil {
		return
	}
	GlobalStatementV3.enabled.Store(v)
}

// Stats returns current statistics.
func Stats() *StatsInfo {
	if GlobalStatementV3 == nil {
		return nil
	}
	return GlobalStatementV3.stats()
}

// close shuts down StatementV3.
func (s *StatementV3) close() {
	if s.closed.Swap(true) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush aggregator
	if s.aggregator != nil {
		s.aggregator.Close()
	}

	// Close pusher
	if s.pusher != nil {
		if err := s.pusher.Close(); err != nil {
			logutil.BgLogger().Warn("error closing statement pusher", zap.Error(err))
		}
	}

	logutil.BgLogger().Info("statement v3 closed")
}

// stats returns current statistics.
func (s *StatementV3) stats() *StatsInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info := &StatsInfo{
		Enabled: s.enabled.Load(),
	}

	if s.aggregator != nil {
		aggStats := s.aggregator.Stats()
		info.DigestCount = aggStats.DigestCount
		info.MemoryBytes = aggStats.MemoryBytes
		info.WindowBegin = aggStats.WindowBegin
		info.WindowEnd = aggStats.WindowEnd
		info.HasOtherBucket = aggStats.HasOtherBucket
	}

	if s.pusher != nil {
		info.PushEnabled = true
		info.CircuitState = s.pusher.CircuitState().String()
	}

	return info
}

// StatsInfo holds statistics about the StatementV3 system.
type StatsInfo struct {
	Enabled        bool   `json:"enabled"`
	PushEnabled    bool   `json:"push_enabled"`
	DigestCount    int    `json:"digest_count"`
	MemoryBytes    int64  `json:"memory_bytes"`
	WindowBegin    interface{} `json:"window_begin"`
	WindowEnd      interface{} `json:"window_end"`
	HasOtherBucket bool   `json:"has_other_bucket"`
	CircuitState   string `json:"circuit_state"`
}

// Flush forces a flush of the current aggregation window.
func Flush() {
	if GlobalStatementV3 == nil || GlobalStatementV3.aggregator == nil {
		return
	}
	GlobalStatementV3.aggregator.Flush()
}

// Ping checks connectivity to the Vector service.
func Ping(ctx context.Context) error {
	if GlobalStatementV3 == nil || GlobalStatementV3.pusher == nil {
		return nil
	}
	return GlobalStatementV3.pusher.Ping()
}
