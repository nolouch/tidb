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
	"github.com/pingcap/tidb/pkg/util/vectorsvc"
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

	// Register the StmtSummaryProvider with the global vectorsvc registry
	// so the Pull Service can serve STATEMENTS_SUMMARY data.
	vectorsvc.Register(NewStmtSummaryProvider(s.aggregator, clusterID, instanceID))

	s.enabled.Store(cfg.Enabled)
	GlobalStatementV3 = s

	logutil.BgLogger().Info("statement v3 initialized",
		zap.Bool("enabled", cfg.Enabled),
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
	if GlobalStatementV3 == nil {
		return
	}
	if !GlobalStatementV3.enabled.Load() {
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

	// Unregister from vectorsvc
	vectorsvc.Unregister(statementsTableName)

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

// ApplyConfig applies updated configuration at runtime (hot-reload).
// This is called when Vector pushes new collection config via Ping.
func (s *StatementV3) ApplyConfig(cfg *Config) {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldCfg := s.cfg
	s.cfg = cfg

	// Update aggregator config
	if s.aggregator != nil {
		s.aggregator.UpdateConfig(cfg)
	}

	logutil.BgLogger().Info("statement v3 config hot-reloaded",
		zap.Duration("old_aggregation_window", oldCfg.AggregationWindow),
		zap.Duration("new_aggregation_window", cfg.AggregationWindow),
		zap.Int("old_max_digests", oldCfg.Memory.MaxDigestsPerWindow),
		zap.Int("new_max_digests", cfg.Memory.MaxDigestsPerWindow),
		zap.Int64("old_max_memory", oldCfg.Memory.MaxMemoryBytes),
		zap.Int64("new_max_memory", cfg.Memory.MaxMemoryBytes))
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

// getConfigCopy returns a copy of the current configuration.
// This is used by PushTargetControlServer to create a new pusher.
func (s *StatementV3) getConfigCopy() *Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg.Copy()
}

// setPusher sets a new pusher. Used by PushTargetControlServer.
func (s *StatementV3) setPusher(pusher *Pusher) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pusher = pusher
}

// closePusher closes the existing pusher if any. Used by PushTargetControlServer.
func (s *StatementV3) closePusher() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pusher != nil {
		if err := s.pusher.Close(); err != nil {
			logutil.BgLogger().Warn("error closing pusher", zap.Error(err))
		}
		s.pusher = nil
	}
}

// setAggregatorOnFlush sets the onFlush callback for the aggregator.
// Used by PushTargetControlServer to connect a new pusher.
func (s *StatementV3) setAggregatorOnFlush(onFlush func(*AggregationWindow)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.aggregator != nil {
		s.aggregator.SetOnFlush(onFlush)
	}
}

// GetPusherControlServer returns the PushTargetControlServer for this StatementV3.
func (s *StatementV3) GetPusherControlServer() *PushTargetControlServer {
	return NewPushTargetControlServer(s.clusterID, s.instanceID, s)
}
