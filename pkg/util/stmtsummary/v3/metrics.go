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
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "tidb"
	subsystem = "statement_v3"
)

var (
	// Push metrics
	PushSuccessTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "push_success_total",
		Help:      "Total number of successful statement batch pushes",
	})

	PushFailureTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "push_failure_total",
		Help:      "Total number of failed statement batch pushes",
	})

	PushLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "push_latency_seconds",
		Help:      "Latency of statement batch pushes in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
	})

	PushBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "push_batch_size",
		Help:      "Number of statements per push batch",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1 to ~16k
	})

	PushMemoryBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "push_memory_bytes",
		Help:      "Current TiDB process memory usage (Alloc) when pushing statement batches",
	})

	PushStatementTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "push_statement_total",
		Help:      "Total number of statements observed across push attempts",
	})

	// Aggregator metrics
	AggregatorDigestCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "aggregator_digest_count",
		Help:      "Current number of unique statement digests in the aggregation window",
	})

	AggregatorMemoryBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "aggregator_memory_bytes",
		Help:      "Current memory usage of the statement aggregator in bytes",
	})

	// Overflow metrics
	DigestOverflowTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "digest_overflow_total",
		Help:      "Total number of statements aggregated to OTHER bucket due to digest limit",
	})

	MemoryOverflowTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "memory_overflow_total",
		Help:      "Total number of statements aggregated to OTHER bucket due to memory limit",
	})

	EvictedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "evicted_total",
		Help:      "Total number of statements evicted from aggregator",
	})

	OtherBucketCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "other_bucket_count",
		Help:      "Number of executions aggregated in the OTHER bucket",
	})

	// Circuit breaker metrics
	CircuitBreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "circuit_breaker_state",
		Help:      "Circuit breaker state (0=closed, 1=open, 2=half-open)",
	}, []string{"endpoint"})

	CircuitBreakerFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "circuit_breaker_failures_total",
		Help:      "Total number of circuit breaker failures",
	}, []string{"endpoint"})

	// Retry buffer metrics
	RetryBufferSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "retry_buffer_size",
		Help:      "Current number of batches in the retry buffer",
	})

	RetryBufferDroppedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "retry_buffer_dropped_total",
		Help:      "Total number of batches dropped from retry buffer due to overflow",
	})

	RetryAttemptsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "retry_attempts_total",
		Help:      "Total number of retry attempts",
	})

	// Window metrics
	WindowFlushTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "window_flush_total",
		Help:      "Total number of aggregation window flushes",
	})

	WindowFlushLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "window_flush_latency_seconds",
		Help:      "Latency of aggregation window flush operations",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms to ~2s
	})
)

func init() {
	prometheus.MustRegister(
		PushSuccessTotal,
		PushFailureTotal,
		PushLatency,
		PushBatchSize,
		PushMemoryBytes,
		PushStatementTotal,
		AggregatorDigestCount,
		AggregatorMemoryBytes,
		DigestOverflowTotal,
		MemoryOverflowTotal,
		EvictedTotal,
		OtherBucketCount,
		CircuitBreakerState,
		CircuitBreakerFailuresTotal,
		RetryBufferSize,
		RetryBufferDroppedTotal,
		RetryAttemptsTotal,
		WindowFlushTotal,
		WindowFlushLatency,
	)
}
