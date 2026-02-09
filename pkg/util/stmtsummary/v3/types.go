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
)

// OtherDigest is the special digest used for aggregated overflow statements.
const OtherDigest = "__OTHER__"

// StmtKey uniquely identifies a statement for aggregation.
type StmtKey struct {
	SchemaName        string
	Digest            string
	PlanDigest        string
	ResourceGroupName string
}

// StmtStats holds aggregated statistics for a single statement.
type StmtStats struct {
	// Identity fields
	Key           StmtKey
	NormalizedSQL string
	TableNames    string
	StmtType      string

	// Sample data (from first/latest execution)
	SampleSQL        string
	SamplePlan       string
	SampleBinaryPlan string
	PlanHint         string
	PrevSQL          string
	IndexNames       []string
	Charset          string
	Collation        string

	// Binding
	BindingSQL    string
	BindingDigest string

	// Execution statistics
	ExecCount   int64
	SumErrors   int64
	SumWarnings int64

	// Latency metrics (nanoseconds, converted to microseconds in proto)
	SumLatency        time.Duration
	MaxLatency        time.Duration
	MinLatency        time.Duration
	SumParseLatency   time.Duration
	MaxParseLatency   time.Duration
	SumCompileLatency time.Duration
	MaxCompileLatency time.Duration

	// Latency histogram for percentile calculation (HdrHistogram encoded)
	LatencyHistogram []byte

	// Resource usage
	SumMemBytes  int64
	MaxMemBytes  int64
	SumDiskBytes int64
	MaxDiskBytes int64
	SumTiDBCPU   time.Duration
	SumTiKVCPU   time.Duration

	// TiKV coprocessor metrics
	SumNumCopTasks       int64
	SumCopProcessTime    time.Duration
	MaxCopProcessTime    time.Duration
	MaxCopProcessAddress string
	SumCopWaitTime       time.Duration
	MaxCopWaitTime       time.Duration
	MaxCopWaitAddress    string

	// TiKV time
	SumProcessTime time.Duration
	MaxProcessTime time.Duration
	SumWaitTime    time.Duration
	MaxWaitTime    time.Duration
	SumBackoffTime time.Duration
	MaxBackoffTime time.Duration

	// Key scan metrics
	SumTotalKeys     int64
	MaxTotalKeys     int64
	SumProcessedKeys int64
	MaxProcessedKeys int64

	// RocksDB metrics
	SumRocksdbDeleteSkippedCount uint64
	MaxRocksdbDeleteSkippedCount uint64
	SumRocksdbKeySkippedCount    uint64
	MaxRocksdbKeySkippedCount    uint64
	SumRocksdbBlockCacheHitCount uint64
	MaxRocksdbBlockCacheHitCount uint64
	SumRocksdbBlockReadCount     uint64
	MaxRocksdbBlockReadCount     uint64
	SumRocksdbBlockReadByte      uint64
	MaxRocksdbBlockReadByte      uint64

	// Transaction metrics
	CommitCount          int64
	SumGetCommitTsTime   time.Duration
	MaxGetCommitTsTime   time.Duration
	SumPrewriteTime      time.Duration
	MaxPrewriteTime      time.Duration
	SumCommitTime        time.Duration
	MaxCommitTime        time.Duration
	SumLocalLatchTime    time.Duration
	MaxLocalLatchTime    time.Duration
	SumCommitBackoffTime int64
	MaxCommitBackoffTime int64
	SumResolveLockTime   int64
	MaxResolveLockTime   int64
	SumWriteKeys         int64
	MaxWriteKeys         int64
	SumWriteSizeBytes    int64
	MaxWriteSizeBytes    int64
	SumPrewriteRegionNum int64
	MaxPrewriteRegionNum int32
	SumTxnRetry          int64
	MaxTxnRetry          int
	SumBackoffTimes      int64
	BackoffTypes         map[string]int

	// Auth/User
	AuthUsers map[string]struct{}

	// Row statistics
	SumAffectedRows int64
	SumResultRows   int64
	MaxResultRows   int64
	MinResultRows   int64

	// Plan cache
	PlanInCache                  bool
	PlanCacheHits                int64
	PlanInBinding                bool
	PlanCacheUnqualifiedCount    int64
	PlanCacheUnqualifiedLastReason string

	// Timestamps
	FirstSeen time.Time
	LastSeen  time.Time

	// Flags
	IsInternal bool
	Prepared   bool

	// Multi-tenancy
	KeyspaceName string
	KeyspaceID   uint32

	// Other
	SumKVTotal           time.Duration
	SumPDTotal           time.Duration
	SumBackoffTotal      time.Duration
	SumWriteSQLRespTotal time.Duration
	ExecRetryCount       uint
	ExecRetryTime        time.Duration
	SumMemArbitration    float64
	MaxMemArbitration    float64
	StorageKV            bool
	StorageMPP           bool

	// Request-units
	SumRRU            float64
	MaxRRU            float64
	SumWRU            float64
	MaxWRU            float64
	SumRUWaitDuration time.Duration
	MaxRUWaitDuration time.Duration

	// Network traffic
	UnpackedBytesSentTiKVTotal            int64
	UnpackedBytesReceivedTiKVTotal        int64
	UnpackedBytesSentTiKVCrossZone        int64
	UnpackedBytesReceivedTiKVCrossZone    int64
	UnpackedBytesSentTiFlashTotal         int64
	UnpackedBytesReceivedTiFlashTotal     int64
	UnpackedBytesSentTiFlashCrossZone     int64
	UnpackedBytesReceivedTiFlashCrossZone int64

	// Extended metrics for dynamic additions
	ExtendedMetrics map[string]MetricValue

	// Memory tracking
	estimatedSize int64
}

// MetricValue represents a value in the extended metrics map.
type MetricValue struct {
	Type  MetricType
	Int64 int64
	Float64 float64
	String string
	Bool   bool
}

// MetricType defines the type of a metric value.
type MetricType int

const (
	MetricTypeInt64 MetricType = iota
	MetricTypeFloat64
	MetricTypeString
	MetricTypeBool
)

// NewStmtStats creates a new StmtStats with initialized fields.
func NewStmtStats(key StmtKey) *StmtStats {
	return &StmtStats{
		Key:             key,
		MinLatency:      time.Duration(1<<63 - 1), // Max duration
		MinResultRows:   1<<63 - 1,                // Max int64
		FirstSeen:       time.Now(),
		LastSeen:        time.Now(),
		BackoffTypes:    make(map[string]int),
		AuthUsers:       make(map[string]struct{}),
		ExtendedMetrics: make(map[string]MetricValue),
	}
}

// EstimatedSize returns the estimated memory size of this StmtStats.
func (s *StmtStats) EstimatedSize() int64 {
	if s.estimatedSize > 0 {
		return s.estimatedSize
	}
	// Base struct size (increased for new fields)
	size := int64(1024) // Approximate base size
	// Add string sizes
	size += int64(len(s.Key.SchemaName) + len(s.Key.Digest) + len(s.Key.PlanDigest) + len(s.Key.ResourceGroupName))
	size += int64(len(s.NormalizedSQL) + len(s.TableNames) + len(s.StmtType))
	size += int64(len(s.SampleSQL) + len(s.SamplePlan) + len(s.SampleBinaryPlan) + len(s.PlanHint) + len(s.PrevSQL))
	size += int64(len(s.Charset) + len(s.Collation))
	size += int64(len(s.BindingSQL) + len(s.BindingDigest))
	size += int64(len(s.KeyspaceName))
	size += int64(len(s.MaxCopProcessAddress) + len(s.MaxCopWaitAddress))
	size += int64(len(s.PlanCacheUnqualifiedLastReason))
	// IndexNames
	for _, name := range s.IndexNames {
		size += int64(len(name) + 16) // string header overhead
	}
	// Add histogram size
	size += int64(len(s.LatencyHistogram))
	// BackoffTypes map
	for k := range s.BackoffTypes {
		size += int64(len(k) + 24) // Key + value overhead
	}
	// AuthUsers map
	size += int64(len(s.AuthUsers) * 40) // Approximate per-entry overhead
	// Add extended metrics size
	for k, v := range s.ExtendedMetrics {
		size += int64(len(k) + 32) // Key + value overhead
		if v.Type == MetricTypeString {
			size += int64(len(v.String))
		}
	}
	s.estimatedSize = size
	return size
}

// AggregationWindow represents a time window for statement aggregation.
type AggregationWindow struct {
	// Begin is the start time of this window.
	Begin time.Time
	// End is the end time of this window.
	End time.Time
	// Statements maps statement keys to their aggregated stats.
	Statements map[StmtKey]*StmtStats
	// OtherBucket aggregates overflow statements.
	OtherBucket *StmtStats
	// CurrentMemory tracks current memory usage.
	CurrentMemory int64
	// DigestCount tracks the number of unique digests.
	DigestCount int
}

// NewAggregationWindow creates a new aggregation window.
func NewAggregationWindow(begin time.Time, duration time.Duration) *AggregationWindow {
	return &AggregationWindow{
		Begin:      begin,
		End:        begin.Add(duration),
		Statements: make(map[StmtKey]*StmtStats),
	}
}

// BatchInfo holds metadata about a batch of statements.
type BatchInfo struct {
	// ClusterID identifies the TiDB cluster.
	ClusterID string
	// InstanceID identifies the TiDB instance.
	InstanceID string
	// WindowStart is the start time of the aggregation window.
	WindowStart time.Time
	// WindowEnd is the end time of the aggregation window.
	WindowEnd time.Time
	// BatchSequence is the sequence number within the window.
	BatchSequence int32
	// BatchTimestamp is when the batch was created.
	BatchTimestamp time.Time
	// SchemaVersion is the proto schema version.
	SchemaVersion string
	// FieldNames lists the extended metric names for discovery.
	FieldNames []string
}

// PushResult holds the result of a push operation.
type PushResult struct {
	Success        bool
	Message        string
	ReceivedCount  int32
	AcceptedCount  int32
	RejectedCount  int32
	Errors         []string
	ReceivedAt     time.Time
	Latency        time.Duration
}

// CircuitState represents the state of a circuit breaker.
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

// String returns the string representation of CircuitState.
func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}
