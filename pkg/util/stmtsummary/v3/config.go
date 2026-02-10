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

package stmtsummaryv3

import (
	"time"

	stmtsummaryv3proto "github.com/pingcap/tidb/pkg/util/stmtsummary/v3/proto/v1"
)

const (
	// Default configuration values
	defaultEnabled              = false
	defaultPushEnabled          = false
	defaultAggregationWindow    = 60 * time.Second // 1 minute
	defaultPushInterval         = 60 * time.Second // 1 minute
	defaultPushBatchSize        = 1000
	defaultMaxDigestsPerWindow  = 10000
	defaultMaxMemoryBytes       = 64 * 1024 * 1024 // 64 MB
	defaultEarlyFlushThreshold  = 0.8
	defaultRetryMaxAttempts     = 3
	defaultRetryInitialDelay    = 1 * time.Second
	defaultRetryMaxDelay        = 30 * time.Second
	defaultPushTimeout          = 30 * time.Second
	defaultEnableInternalQuery  = false
	defaultEvictionStrategy     = EvictionAggregateToOther
)

// EvictionStrategy defines how to handle overflow when limits are reached.
type EvictionStrategy string

const (
	// EvictionDropNew drops new statements when limits are reached.
	EvictionDropNew EvictionStrategy = "drop_new"
	// EvictionLRU evicts least recently used statements.
	EvictionLRU EvictionStrategy = "evict_lru"
	// EvictionLowFrequency evicts low-frequency statements.
	EvictionLowFrequency EvictionStrategy = "evict_low_frequency"
	// EvictionAggregateToOther aggregates overflow to an "OTHER" bucket.
	EvictionAggregateToOther EvictionStrategy = "aggregate_to_other"
)

// Config holds the configuration for Statement V3.
type Config struct {
	// Enabled controls whether Statement V3 is enabled.
	Enabled bool `toml:"enabled" json:"enabled"`

	// EnableInternalQuery controls whether to collect internal queries.
	EnableInternalQuery bool `toml:"enable-internal-query" json:"enable_internal_query"`

	// AggregationWindow is the duration of each aggregation window.
	AggregationWindow time.Duration `toml:"aggregation-window" json:"aggregation_window"`

	// Push configuration
	Push PushConfig `toml:"push" json:"push"`

	// Memory protection configuration
	Memory MemoryConfig `toml:"memory" json:"memory"`

	// ExtendedMetrics configuration for dynamic metrics
	ExtendedMetrics []ExtendedMetricConfig `toml:"extended-metrics" json:"extended_metrics"`
}

// PushConfig holds configuration for pushing statements to Vector.
type PushConfig struct {
	// Enabled controls whether pushing is enabled.
	Enabled bool `toml:"enabled" json:"enabled"`

	// Endpoint is the gRPC endpoint of the Vector service.
	Endpoint string `toml:"endpoint" json:"endpoint"`

	// BatchSize is the maximum number of statements per batch.
	BatchSize int `toml:"batch-size" json:"batch_size"`

	// Interval is how often to push batches.
	Interval time.Duration `toml:"interval" json:"interval"`

	// Timeout is the timeout for push operations.
	Timeout time.Duration `toml:"timeout" json:"timeout"`

	// Retry configuration
	Retry RetryConfig `toml:"retry" json:"retry"`

	// TLS configuration
	TLS TLSConfig `toml:"tls" json:"tls"`

	// ContractURL is the URL to fetch the Vector requirements contract.
	ContractURL string `toml:"contract-url" json:"contract_url"`
}

// RetryConfig holds retry configuration for push operations.
type RetryConfig struct {
	// MaxAttempts is the maximum number of retry attempts.
	MaxAttempts int `toml:"max-attempts" json:"max_attempts"`

	// InitialDelay is the initial delay before the first retry.
	InitialDelay time.Duration `toml:"initial-delay" json:"initial_delay"`

	// MaxDelay is the maximum delay between retries.
	MaxDelay time.Duration `toml:"max-delay" json:"max_delay"`
}

// TLSConfig holds TLS configuration for gRPC connections.
type TLSConfig struct {
	// Enabled controls whether TLS is enabled.
	Enabled bool `toml:"enabled" json:"enabled"`

	// CertFile is the path to the client certificate file.
	CertFile string `toml:"cert-file" json:"cert_file"`

	// KeyFile is the path to the client key file.
	KeyFile string `toml:"key-file" json:"key_file"`

	// CAFile is the path to the CA certificate file.
	CAFile string `toml:"ca-file" json:"ca_file"`

	// ServerName is the server name for TLS verification.
	ServerName string `toml:"server-name" json:"server_name"`
}

// MemoryConfig holds memory protection configuration.
type MemoryConfig struct {
	// MaxDigestsPerWindow is the maximum number of unique digests per window.
	MaxDigestsPerWindow int `toml:"max-digests-per-window" json:"max_digests_per_window"`

	// MaxMemoryBytes is the maximum memory for the aggregation buffer.
	MaxMemoryBytes int64 `toml:"max-memory-bytes" json:"max_memory_bytes"`

	// EvictionStrategy defines how to handle overflow.
	EvictionStrategy EvictionStrategy `toml:"eviction-strategy" json:"eviction_strategy"`

	// EarlyFlushThreshold triggers early flush when memory usage exceeds this ratio.
	EarlyFlushThreshold float64 `toml:"early-flush-threshold" json:"early_flush_threshold"`
}

// ExtendedMetricConfig defines a dynamic extended metric.
type ExtendedMetricConfig struct {
	// Name is the metric name in the extended_metrics map.
	Name string `toml:"name" json:"name"`

	// Type is the metric value type (int64, double, string, bool).
	Type string `toml:"type" json:"type"`

	// Source is the path to extract the metric from StmtExecInfo.
	Source string `toml:"source" json:"source"`

	// Enabled controls whether this metric is collected.
	Enabled bool `toml:"enabled" json:"enabled"`

	// Aggregation defines how to aggregate the metric (sum, max, min, avg).
	Aggregation string `toml:"aggregation" json:"aggregation"`
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() *Config {
	return &Config{
		Enabled:             defaultEnabled,
		EnableInternalQuery: defaultEnableInternalQuery,
		AggregationWindow:   defaultAggregationWindow,
		Push: PushConfig{
			Enabled:   defaultPushEnabled,
			Endpoint:  "",
			BatchSize: defaultPushBatchSize,
			Interval:  defaultPushInterval,
			Timeout:   defaultPushTimeout,
			Retry: RetryConfig{
				MaxAttempts:  defaultRetryMaxAttempts,
				InitialDelay: defaultRetryInitialDelay,
				MaxDelay:     defaultRetryMaxDelay,
			},
		},
		Memory: MemoryConfig{
			MaxDigestsPerWindow: defaultMaxDigestsPerWindow,
			MaxMemoryBytes:      defaultMaxMemoryBytes,
			EvictionStrategy:    defaultEvictionStrategy,
			EarlyFlushThreshold: defaultEarlyFlushThreshold,
		},
	}
}

// MergeFromRemote applies a CollectionConfig received from Vector via Ping.
// Remote values override local defaults. Zero-values in the remote config are
// treated as "not set" and will not override.
func (c *Config) MergeFromRemote(cc *stmtsummaryv3proto.CollectionConfig) {
	if cc == nil {
		return
	}
	if cc.AggregationWindowSecs > 0 {
		c.AggregationWindow = time.Duration(cc.AggregationWindowSecs) * time.Second
	}
	c.EnableInternalQuery = cc.EnableInternalQuery
	if cc.PushBatchSize > 0 {
		c.Push.BatchSize = int(cc.PushBatchSize)
	}
	if cc.PushIntervalSecs > 0 {
		c.Push.Interval = time.Duration(cc.PushIntervalSecs) * time.Second
	}
	if cc.PushTimeoutSecs > 0 {
		c.Push.Timeout = time.Duration(cc.PushTimeoutSecs) * time.Second
	}
	if cc.MaxDigestsPerWindow > 0 {
		c.Memory.MaxDigestsPerWindow = int(cc.MaxDigestsPerWindow)
	}
	if cc.MaxMemoryBytes > 0 {
		c.Memory.MaxMemoryBytes = cc.MaxMemoryBytes
	}
	if cc.EvictionStrategy != "" {
		c.Memory.EvictionStrategy = EvictionStrategy(cc.EvictionStrategy)
	}
	if cc.EarlyFlushThreshold > 0 {
		c.Memory.EarlyFlushThreshold = cc.EarlyFlushThreshold
	}
	if cc.RetryMaxAttempts > 0 {
		c.Push.Retry.MaxAttempts = int(cc.RetryMaxAttempts)
	}
	if cc.RetryInitialDelayMs > 0 {
		c.Push.Retry.InitialDelay = time.Duration(cc.RetryInitialDelayMs) * time.Millisecond
	}
	if cc.RetryMaxDelayMs > 0 {
		c.Push.Retry.MaxDelay = time.Duration(cc.RetryMaxDelayMs) * time.Millisecond
	}
	if len(cc.ExtendedMetrics) > 0 {
		c.ExtendedMetrics = make([]ExtendedMetricConfig, 0, len(cc.ExtendedMetrics))
		for _, em := range cc.ExtendedMetrics {
			c.ExtendedMetrics = append(c.ExtendedMetrics, ExtendedMetricConfig{
				Name:        em.Name,
				Type:        em.Type,
				Source:      em.Source,
				Enabled:     em.Enabled,
				Aggregation: em.Aggregation,
			})
		}
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.AggregationWindow < time.Second {
		c.AggregationWindow = defaultAggregationWindow
	}
	if c.Push.BatchSize <= 0 {
		c.Push.BatchSize = defaultPushBatchSize
	}
	if c.Push.Interval < time.Second {
		c.Push.Interval = defaultPushInterval
	}
	if c.Push.Timeout <= 0 {
		c.Push.Timeout = defaultPushTimeout
	}
	if c.Push.Retry.MaxAttempts <= 0 {
		c.Push.Retry.MaxAttempts = defaultRetryMaxAttempts
	}
	if c.Memory.MaxDigestsPerWindow <= 0 {
		c.Memory.MaxDigestsPerWindow = defaultMaxDigestsPerWindow
	}
	if c.Memory.MaxMemoryBytes <= 0 {
		c.Memory.MaxMemoryBytes = defaultMaxMemoryBytes
	}
	if c.Memory.EarlyFlushThreshold <= 0 || c.Memory.EarlyFlushThreshold > 1 {
		c.Memory.EarlyFlushThreshold = defaultEarlyFlushThreshold
	}
	return nil
}
