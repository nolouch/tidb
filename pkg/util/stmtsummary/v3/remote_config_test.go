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
	"testing"
	"time"

	stmtsummaryv3proto "github.com/pingcap/tidb/pkg/util/vectorsvc/proto/v1"
	"github.com/stretchr/testify/require"
)

// TestMergeFromRemote_AllFields tests that MergeFromRemote applies all non-zero fields.
func TestMergeFromRemote_AllFields(t *testing.T) {
	cfg := DefaultConfig()

	cc := &stmtsummaryv3proto.CollectionConfig{
		AggregationWindowSecs: 120,
		EnableInternalQuery:   true,
		PushBatchSize:         500,
		PushIntervalSecs:      30,
		PushTimeoutSecs:       15,
		MaxDigestsPerWindow:   5000,
		MaxMemoryBytes:        128 * 1024 * 1024,
		EvictionStrategy:      "evict_lru",
		EarlyFlushThreshold:   0.9,
		RetryMaxAttempts:      5,
		RetryInitialDelayMs:   2000,
		RetryMaxDelayMs:       60000,
		ExtendedMetrics: []*stmtsummaryv3proto.ExtendedMetricDef{
			{
				Name:        "custom_metric",
				Type:        "int64",
				Source:      "exec_detail.custom",
				Enabled:     true,
				Aggregation: "sum",
			},
		},
		ConfigVersion: 42,
	}

	cfg.MergeFromRemote(cc)

	require.Equal(t, 120*time.Second, cfg.AggregationWindow)
	require.True(t, cfg.EnableInternalQuery)
	require.Equal(t, 500, cfg.Push.BatchSize)
	require.Equal(t, 30*time.Second, cfg.Push.Interval)
	require.Equal(t, 15*time.Second, cfg.Push.Timeout)
	require.Equal(t, 5000, cfg.Memory.MaxDigestsPerWindow)
	require.Equal(t, int64(128*1024*1024), cfg.Memory.MaxMemoryBytes)
	require.Equal(t, EvictionLRU, cfg.Memory.EvictionStrategy)
	require.InDelta(t, 0.9, cfg.Memory.EarlyFlushThreshold, 0.001)
	require.Equal(t, 5, cfg.Push.Retry.MaxAttempts)
	require.Equal(t, 2000*time.Millisecond, cfg.Push.Retry.InitialDelay)
	require.Equal(t, 60000*time.Millisecond, cfg.Push.Retry.MaxDelay)
	require.Len(t, cfg.ExtendedMetrics, 1)
	require.Equal(t, "custom_metric", cfg.ExtendedMetrics[0].Name)
	require.Equal(t, "int64", cfg.ExtendedMetrics[0].Type)
	require.Equal(t, "exec_detail.custom", cfg.ExtendedMetrics[0].Source)
	require.True(t, cfg.ExtendedMetrics[0].Enabled)
	require.Equal(t, "sum", cfg.ExtendedMetrics[0].Aggregation)
}

// TestMergeFromRemote_NilConfig tests that MergeFromRemote is a no-op when cc is nil.
func TestMergeFromRemote_NilConfig(t *testing.T) {
	cfg := DefaultConfig()
	original := *cfg

	cfg.MergeFromRemote(nil)

	require.Equal(t, original.AggregationWindow, cfg.AggregationWindow)
	require.Equal(t, original.Push.BatchSize, cfg.Push.BatchSize)
	require.Equal(t, original.Memory.MaxDigestsPerWindow, cfg.Memory.MaxDigestsPerWindow)
}

// TestMergeFromRemote_ZeroValuesPreserveDefaults tests that zero-values in
// the remote config do not override existing local config (except for booleans).
func TestMergeFromRemote_ZeroValuesPreserveDefaults(t *testing.T) {
	cfg := DefaultConfig()

	cc := &stmtsummaryv3proto.CollectionConfig{
		// All int/float fields are zero -> should not override
		AggregationWindowSecs: 0,
		PushBatchSize:         0,
		PushIntervalSecs:      0,
		PushTimeoutSecs:       0,
		MaxDigestsPerWindow:   0,
		MaxMemoryBytes:        0,
		EvictionStrategy:      "", // empty string -> no override
		EarlyFlushThreshold:   0,
		RetryMaxAttempts:      0,
		RetryInitialDelayMs:   0,
		RetryMaxDelayMs:       0,
		// EnableInternalQuery is a bool; false is the zero value but we set it explicitly
		EnableInternalQuery: false,
	}

	cfg.MergeFromRemote(cc)

	// All defaults should be preserved for numeric fields
	require.Equal(t, defaultAggregationWindow, cfg.AggregationWindow)
	require.Equal(t, defaultPushBatchSize, cfg.Push.BatchSize)
	require.Equal(t, defaultPushInterval, cfg.Push.Interval)
	require.Equal(t, defaultPushTimeout, cfg.Push.Timeout)
	require.Equal(t, defaultMaxDigestsPerWindow, cfg.Memory.MaxDigestsPerWindow)
	require.Equal(t, int64(defaultMaxMemoryBytes), cfg.Memory.MaxMemoryBytes)
	require.Equal(t, defaultEvictionStrategy, cfg.Memory.EvictionStrategy)
	require.Equal(t, defaultEarlyFlushThreshold, cfg.Memory.EarlyFlushThreshold)
	require.Equal(t, defaultRetryMaxAttempts, cfg.Push.Retry.MaxAttempts)
	require.Equal(t, defaultRetryInitialDelay, cfg.Push.Retry.InitialDelay)
	require.Equal(t, defaultRetryMaxDelay, cfg.Push.Retry.MaxDelay)
	// Bool is always set (false is a valid value from remote)
	require.False(t, cfg.EnableInternalQuery)
}

// TestMergeFromRemote_PartialOverride tests that only specified fields are overridden.
func TestMergeFromRemote_PartialOverride(t *testing.T) {
	cfg := DefaultConfig()

	cc := &stmtsummaryv3proto.CollectionConfig{
		AggregationWindowSecs: 300,
		MaxMemoryBytes:        256 * 1024 * 1024,
		// All other fields are zero -> no override
	}

	cfg.MergeFromRemote(cc)

	// Overridden fields
	require.Equal(t, 300*time.Second, cfg.AggregationWindow)
	require.Equal(t, int64(256*1024*1024), cfg.Memory.MaxMemoryBytes)

	// Defaults preserved
	require.Equal(t, defaultPushBatchSize, cfg.Push.BatchSize)
	require.Equal(t, defaultPushInterval, cfg.Push.Interval)
	require.Equal(t, defaultMaxDigestsPerWindow, cfg.Memory.MaxDigestsPerWindow)
	require.Equal(t, defaultEvictionStrategy, cfg.Memory.EvictionStrategy)
}

// TestMergeFromRemote_EvictionStrategies tests all eviction strategy values.
func TestMergeFromRemote_EvictionStrategies(t *testing.T) {
	strategies := []struct {
		remote   string
		expected EvictionStrategy
	}{
		{"drop_new", EvictionDropNew},
		{"evict_lru", EvictionLRU},
		{"evict_low_frequency", EvictionLowFrequency},
		{"aggregate_to_other", EvictionAggregateToOther},
	}

	for _, tt := range strategies {
		t.Run(tt.remote, func(t *testing.T) {
			cfg := DefaultConfig()
			cc := &stmtsummaryv3proto.CollectionConfig{
				EvictionStrategy: tt.remote,
			}
			cfg.MergeFromRemote(cc)
			require.Equal(t, tt.expected, cfg.Memory.EvictionStrategy)
		})
	}
}

// TestMergeFromRemote_MultipleExtendedMetrics tests that multiple extended metrics are applied.
func TestMergeFromRemote_MultipleExtendedMetrics(t *testing.T) {
	cfg := DefaultConfig()
	// Pre-populate with an existing metric
	cfg.ExtendedMetrics = []ExtendedMetricConfig{
		{Name: "old_metric", Type: "int64", Source: "old_source", Enabled: true, Aggregation: "sum"},
	}

	cc := &stmtsummaryv3proto.CollectionConfig{
		ExtendedMetrics: []*stmtsummaryv3proto.ExtendedMetricDef{
			{Name: "metric_a", Type: "int64", Source: "source_a", Enabled: true, Aggregation: "sum"},
			{Name: "metric_b", Type: "double", Source: "source_b", Enabled: false, Aggregation: "max"},
			{Name: "metric_c", Type: "string", Source: "source_c", Enabled: true, Aggregation: "min"},
		},
	}

	cfg.MergeFromRemote(cc)

	// Old metrics replaced by remote
	require.Len(t, cfg.ExtendedMetrics, 3)
	require.Equal(t, "metric_a", cfg.ExtendedMetrics[0].Name)
	require.Equal(t, "metric_b", cfg.ExtendedMetrics[1].Name)
	require.False(t, cfg.ExtendedMetrics[1].Enabled)
	require.Equal(t, "metric_c", cfg.ExtendedMetrics[2].Name)
}

// TestMergeFromRemote_EmptyExtendedMetricsPreservesExisting tests that an empty
// extended_metrics list in remote config preserves existing local metrics.
func TestMergeFromRemote_EmptyExtendedMetricsPreservesExisting(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ExtendedMetrics = []ExtendedMetricConfig{
		{Name: "local_metric", Type: "int64", Source: "local_source", Enabled: true, Aggregation: "sum"},
	}

	cc := &stmtsummaryv3proto.CollectionConfig{
		AggregationWindowSecs: 90,
		// ExtendedMetrics is nil/empty -> should not override
	}

	cfg.MergeFromRemote(cc)

	require.Equal(t, 90*time.Second, cfg.AggregationWindow)
	require.Len(t, cfg.ExtendedMetrics, 1)
	require.Equal(t, "local_metric", cfg.ExtendedMetrics[0].Name)
}

// TestAggregatorUpdateConfig tests that UpdateConfig properly updates the aggregator.
func TestAggregatorUpdateConfig(t *testing.T) {
	cfg := &Config{
		AggregationWindow:   time.Minute,
		EnableInternalQuery: false,
		Memory: MemoryConfig{
			MaxMemoryBytes:      64 * 1024 * 1024,
			MaxDigestsPerWindow: 10000,
			EvictionStrategy:    EvictionAggregateToOther,
			EarlyFlushThreshold: 0.8,
		},
	}
	aggregator := NewAggregator(cfg)
	defer aggregator.Close()

	// Update config
	newCfg := &Config{
		AggregationWindow:   2 * time.Minute,
		EnableInternalQuery: true,
		Memory: MemoryConfig{
			MaxMemoryBytes:      128 * 1024 * 1024,
			MaxDigestsPerWindow: 20000,
			EvictionStrategy:    EvictionLRU,
			EarlyFlushThreshold: 0.9,
		},
	}

	aggregator.UpdateConfig(newCfg)

	// Verify config is updated by checking that internal queries are now collected
	require.Equal(t, newCfg, aggregator.cfg)
}

// TestAggregatorUpdateConfig_WindowRespected tests that a config update
// with a new aggregation window is applied to subsequent windows.
func TestAggregatorUpdateConfig_WindowRespected(t *testing.T) {
	cfg := &Config{
		AggregationWindow:   time.Minute,
		EnableInternalQuery: true,
		Memory: MemoryConfig{
			MaxMemoryBytes:      1024 * 1024 * 1024,
			MaxDigestsPerWindow: 10000,
			EvictionStrategy:    EvictionAggregateToOther,
			EarlyFlushThreshold: 0.8,
		},
	}
	aggregator := NewAggregator(cfg)
	defer aggregator.Close()

	// Capture the current window end
	stats1 := aggregator.Stats()
	windowDuration1 := stats1.WindowEnd.Sub(stats1.WindowBegin)
	require.InDelta(t, time.Minute.Seconds(), windowDuration1.Seconds(), 1)

	// Update to a 5 minute window
	newCfg := &Config{
		AggregationWindow:   5 * time.Minute,
		EnableInternalQuery: true,
		Memory: MemoryConfig{
			MaxMemoryBytes:      1024 * 1024 * 1024,
			MaxDigestsPerWindow: 10000,
			EvictionStrategy:    EvictionAggregateToOther,
			EarlyFlushThreshold: 0.8,
		},
	}
	aggregator.UpdateConfig(newCfg)

	// Force a flush to create a new window with updated config
	aggregator.Flush()

	stats2 := aggregator.Stats()
	windowDuration2 := stats2.WindowEnd.Sub(stats2.WindowBegin)
	require.InDelta(t, (5 * time.Minute).Seconds(), windowDuration2.Seconds(), 1)
}

// TestStatementV3ApplyConfig tests the StatementV3.ApplyConfig method.
func TestStatementV3ApplyConfig(t *testing.T) {
	cfg := &Config{
		Enabled:             true,
		EnableInternalQuery: false,
		AggregationWindow:   time.Minute,
		Memory: MemoryConfig{
			MaxMemoryBytes:      64 * 1024 * 1024,
			MaxDigestsPerWindow: 10000,
			EvictionStrategy:    EvictionAggregateToOther,
			EarlyFlushThreshold: 0.8,
		},
	}

	s := &StatementV3{
		cfg:        cfg,
		aggregator: NewAggregator(cfg),
	}
	defer s.aggregator.Close()
	s.enabled.Store(true)

	newCfg := &Config{
		Enabled:             true,
		EnableInternalQuery: true,
		AggregationWindow:   2 * time.Minute,
		Memory: MemoryConfig{
			MaxMemoryBytes:      128 * 1024 * 1024,
			MaxDigestsPerWindow: 20000,
			EvictionStrategy:    EvictionLRU,
			EarlyFlushThreshold: 0.9,
		},
	}

	s.ApplyConfig(newCfg)

	require.Equal(t, newCfg, s.cfg)
	require.Equal(t, newCfg, s.aggregator.cfg)
}

// TestCollectionConfigProtoRoundTrip tests that CollectionConfig proto structs
// work correctly with getters.
func TestCollectionConfigProtoRoundTrip(t *testing.T) {
	cc := &stmtsummaryv3proto.CollectionConfig{
		AggregationWindowSecs: 120,
		EnableInternalQuery:   true,
		PushBatchSize:         500,
		PushIntervalSecs:      30,
		PushTimeoutSecs:       15,
		MaxDigestsPerWindow:   5000,
		MaxMemoryBytes:        128 * 1024 * 1024,
		EvictionStrategy:      "evict_lru",
		EarlyFlushThreshold:   0.9,
		RetryMaxAttempts:      5,
		RetryInitialDelayMs:   2000,
		RetryMaxDelayMs:       60000,
		ExtendedMetrics: []*stmtsummaryv3proto.ExtendedMetricDef{
			{
				Name:        "test_metric",
				Type:        "int64",
				Source:      "some.path",
				Enabled:     true,
				Aggregation: "sum",
			},
		},
		ConfigVersion: 99,
	}

	require.Equal(t, int32(120), cc.GetAggregationWindowSecs())
	require.True(t, cc.GetEnableInternalQuery())
	require.Equal(t, int32(500), cc.GetPushBatchSize())
	require.Equal(t, int32(30), cc.GetPushIntervalSecs())
	require.Equal(t, int32(15), cc.GetPushTimeoutSecs())
	require.Equal(t, int32(5000), cc.GetMaxDigestsPerWindow())
	require.Equal(t, int64(128*1024*1024), cc.GetMaxMemoryBytes())
	require.Equal(t, "evict_lru", cc.GetEvictionStrategy())
	require.InDelta(t, 0.9, cc.GetEarlyFlushThreshold(), 0.001)
	require.Equal(t, int32(5), cc.GetRetryMaxAttempts())
	require.Equal(t, int32(2000), cc.GetRetryInitialDelayMs())
	require.Equal(t, int32(60000), cc.GetRetryMaxDelayMs())
	require.Equal(t, int64(99), cc.GetConfigVersion())
	require.Len(t, cc.GetExtendedMetrics(), 1)
	require.Equal(t, "test_metric", cc.GetExtendedMetrics()[0].GetName())
	require.Equal(t, "int64", cc.GetExtendedMetrics()[0].GetType())
	require.Equal(t, "some.path", cc.GetExtendedMetrics()[0].GetSource())
	require.True(t, cc.GetExtendedMetrics()[0].GetEnabled())
	require.Equal(t, "sum", cc.GetExtendedMetrics()[0].GetAggregation())
}

// TestCollectionConfigProto_NilGetters tests that getters on nil return zero values.
func TestCollectionConfigProto_NilGetters(t *testing.T) {
	var cc *stmtsummaryv3proto.CollectionConfig
	require.Equal(t, int32(0), cc.GetAggregationWindowSecs())
	require.False(t, cc.GetEnableInternalQuery())
	require.Equal(t, int32(0), cc.GetPushBatchSize())
	require.Equal(t, "", cc.GetEvictionStrategy())
	require.Equal(t, int64(0), cc.GetConfigVersion())
	require.Nil(t, cc.GetExtendedMetrics())

	var em *stmtsummaryv3proto.ExtendedMetricDef
	require.Equal(t, "", em.GetName())
	require.Equal(t, "", em.GetType())
	require.Equal(t, "", em.GetSource())
	require.False(t, em.GetEnabled())
	require.Equal(t, "", em.GetAggregation())
}

// TestPingResponseCollectionConfig tests that PingResponse carries CollectionConfig.
func TestPingResponseCollectionConfig(t *testing.T) {
	resp := &stmtsummaryv3proto.PingResponse{
		Ok:              true,
		Version:         "1.0.0",
		SupportedTables: []string{"STATEMENTS_SUMMARY"},
		ProtocolVersion: "1.0",
		CollectionConfig: &stmtsummaryv3proto.CollectionConfig{
			AggregationWindowSecs: 60,
			PushBatchSize:         1000,
			ConfigVersion:         1,
		},
	}

	require.True(t, resp.Ok)
	require.NotNil(t, resp.GetCollectionConfig())
	require.Equal(t, int32(60), resp.GetCollectionConfig().GetAggregationWindowSecs())
	require.Equal(t, int32(1000), resp.GetCollectionConfig().GetPushBatchSize())
	require.Equal(t, int64(1), resp.GetCollectionConfig().GetConfigVersion())
}

// TestPingResponseNoCollectionConfig tests that PingResponse without CollectionConfig
// returns nil (backward compatible with older Vector versions).
func TestPingResponseNoCollectionConfig(t *testing.T) {
	resp := &stmtsummaryv3proto.PingResponse{
		Ok:              true,
		Version:         "0.9.0",
		SupportedTables: []string{"STATEMENTS_SUMMARY"},
		ProtocolVersion: "1.0",
		// No CollectionConfig set
	}

	require.True(t, resp.Ok)
	require.Nil(t, resp.GetCollectionConfig())

	// MergeFromRemote should handle nil gracefully
	cfg := DefaultConfig()
	cfg.MergeFromRemote(resp.GetCollectionConfig())
	// Defaults unchanged
	require.Equal(t, defaultAggregationWindow, cfg.AggregationWindow)
	require.Equal(t, defaultPushBatchSize, cfg.Push.BatchSize)
}

// TestMergeFromRemote_ThenValidate tests that a config after MergeFromRemote
// still passes Validate.
func TestMergeFromRemote_ThenValidate(t *testing.T) {
	cfg := DefaultConfig()
	cc := &stmtsummaryv3proto.CollectionConfig{
		AggregationWindowSecs: 300,
		PushBatchSize:         2000,
		PushIntervalSecs:      120,
		PushTimeoutSecs:       60,
		MaxDigestsPerWindow:   50000,
		MaxMemoryBytes:        512 * 1024 * 1024,
		EvictionStrategy:      "drop_new",
		EarlyFlushThreshold:   0.7,
		RetryMaxAttempts:      10,
		RetryInitialDelayMs:   500,
		RetryMaxDelayMs:       120000,
	}

	cfg.MergeFromRemote(cc)
	err := cfg.Validate()
	require.NoError(t, err)

	require.Equal(t, 300*time.Second, cfg.AggregationWindow)
	require.Equal(t, 2000, cfg.Push.BatchSize)
	require.Equal(t, 120*time.Second, cfg.Push.Interval)
	require.Equal(t, 60*time.Second, cfg.Push.Timeout)
	require.Equal(t, 50000, cfg.Memory.MaxDigestsPerWindow)
	require.Equal(t, int64(512*1024*1024), cfg.Memory.MaxMemoryBytes)
	require.Equal(t, EvictionDropNew, cfg.Memory.EvictionStrategy)
	require.InDelta(t, 0.7, cfg.Memory.EarlyFlushThreshold, 0.001)
	require.Equal(t, 10, cfg.Push.Retry.MaxAttempts)
}
