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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/vectorsvc"
	stmtsummaryv3proto "github.com/pingcap/tidb/pkg/util/vectorsvc/proto/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// SchemaVersion is the current schema version for statement data.
	SchemaVersion = "1.0.0"
)

// Pusher handles pushing aggregated statements to Vector via gRPC.
type Pusher struct {
	cfg        *Config
	clusterID  string
	instanceID string

	// gRPC connection
	conn   *grpc.ClientConn
	client stmtsummaryv3proto.SystemTablePushServiceClient

	// Circuit breaker for fault tolerance
	circuitBreaker *vectorsvc.CircuitBreaker

	// Retry executor
	retryExecutor *vectorsvc.RetryExecutor

	// Batch queue
	pendingBatches chan *AggregationWindow

	// Retry buffer for failed batches
	retryBuffer     []*AggregationWindow
	retryBufferLock sync.Mutex

	// Sequence tracking
	batchSequence atomic.Int32

	// Remote config version (for change detection)
	remoteConfigVersion atomic.Int64

	// Callback to notify StatementV3 of config changes
	onConfigChange func(*Config)

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed atomic.Bool
}

// NewPusher creates a new Pusher with the given configuration.
func NewPusher(cfg *Config, clusterID, instanceID string) (*Pusher, error) {
	if !cfg.Push.Enabled {
		return nil, errors.New("push is not enabled")
	}
	if cfg.Push.Endpoint == "" {
		return nil, errors.New("push endpoint is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &Pusher{
		cfg:            cfg,
		clusterID:      clusterID,
		instanceID:     instanceID,
		ctx:            ctx,
		cancel:         cancel,
		pendingBatches: make(chan *AggregationWindow, 100),
	}

	// Initialize retry executor
	retryPolicy := vectorsvc.RetryPolicy{
		MaxAttempts:  cfg.Push.Retry.MaxAttempts,
		InitialDelay: cfg.Push.Retry.InitialDelay,
		MaxDelay:     cfg.Push.Retry.MaxDelay,
		Multiplier:   2.0,
		Jitter:       0.1,
	}
	p.retryExecutor = vectorsvc.NewRetryExecutor(retryPolicy)
	p.retryExecutor.OnRetry = func() {
		RetryAttemptsTotal.Inc()
	}

	// Initialize circuit breaker
	cbConfig := vectorsvc.CircuitBreakerConfig{
		FailureThreshold: cfg.Push.Retry.MaxAttempts,
		SuccessThreshold: 3,
		Timeout:          cfg.Push.Retry.MaxDelay,
	}
	p.circuitBreaker = vectorsvc.NewCircuitBreaker(cbConfig)
	p.circuitBreaker.SetOnStateChange(func(from, to vectorsvc.CircuitState) {
		logutil.BgLogger().Info("pusher circuit breaker state changed",
			zap.String("from", from.String()),
			zap.String("to", to.String()))
	})

	// Establish gRPC connection
	if err := p.connect(); err != nil {
		cancel()
		return nil, err
	}

	// Fetch and validate requirements contract
	if err := p.validateContract(ctx); err != nil {
		logutil.BgLogger().Warn("failed to validate requirements contract",
			zap.Error(err))
		// Continue anyway - contract validation is not critical for operation
	}

	// Initial Ping to fetch remote configuration from Vector
	resp, err := p.ping()
	if err != nil {
		logutil.BgLogger().Warn("initial ping failed, using local config",
			zap.Error(err))
	} else {
		p.applyRemoteConfig(resp)
	}

	// Start push worker
	p.wg.Add(1)
	go p.pushWorker()

	return p, nil
}

// connect establishes the gRPC connection.
func (p *Pusher) connect() error {
	var opts []grpc.DialOption

	if p.cfg.Push.TLS.Enabled {
		tlsConfig, err := p.loadTLSConfig()
		if err != nil {
			return err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.DialContext(p.ctx, p.cfg.Push.Endpoint, opts...)
	if err != nil {
		return err
	}

	p.conn = conn
	p.client = stmtsummaryv3proto.NewSystemTablePushServiceClient(conn)
	return nil
}

// loadTLSConfig loads TLS configuration from files.
func (p *Pusher) loadTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(p.cfg.Push.TLS.CertFile, p.cfg.Push.TLS.KeyFile)
	if err != nil {
		return nil, err
	}

	caCert, err := os.ReadFile(p.cfg.Push.TLS.CAFile)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		ServerName:   p.cfg.Push.TLS.ServerName,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// Push queues an aggregation window for pushing to Vector.
func (p *Pusher) Push(window *AggregationWindow) {
	if p.closed.Load() {
		return
	}

	select {
	case p.pendingBatches <- window:
	default:
		logutil.BgLogger().Warn("statement push queue full, dropping batch",
			zap.Time("windowBegin", window.Begin),
			zap.Time("windowEnd", window.End),
			zap.Int("statements", len(window.Statements)))
	}
}

// pushWorker processes pending batches.
func (p *Pusher) pushWorker() {
	defer p.wg.Done()

	retryTicker := time.NewTicker(p.cfg.Push.Retry.InitialDelay)
	defer retryTicker.Stop()

	configTicker := time.NewTicker(5 * time.Minute)
	defer configTicker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			// Drain remaining batches
			p.drainPendingBatches()
			return

		case window := <-p.pendingBatches:
			p.pushWithRetry(window)

		case <-retryTicker.C:
			p.retryFailedBatches()

		case <-configTicker.C:
			p.syncRemoteConfig()
		}
	}
}

// pushWithRetry attempts to push a window with retry logic.
func (p *Pusher) pushWithRetry(window *AggregationWindow) {
	// Check circuit breaker
	if !p.circuitBreaker.Allow() {
		logutil.BgLogger().Warn("circuit breaker open, buffering batch for retry",
			zap.String("state", p.circuitBreaker.State().String()))
		p.addToRetryBuffer(window)
		return
	}

	batch := p.buildBatch(window)
	statementCount := int64(len(batch.Statements))

	// Execute push with retry
	err := p.retryExecutor.Execute(p.ctx, func() error {
		result := p.doPush(batch)
		if !result.Success {
			return errors.New(result.Message)
		}
		// Record success metrics
		PushSuccessTotal.Inc()
		PushLatency.Observe(result.Latency.Seconds())
		PushBatchSize.Observe(float64(statementCount))
		return nil
	})

	if err != nil {
		// All retries exhausted
		p.circuitBreaker.RecordFailure()
		PushFailureTotal.Inc()
		CircuitBreakerFailuresTotal.WithLabelValues(p.cfg.Push.Endpoint).Inc()
		// Update circuit breaker state metric
		CircuitBreakerState.WithLabelValues(p.cfg.Push.Endpoint).Set(float64(p.circuitBreaker.State()))
		logutil.BgLogger().Warn("statement batch push failed after retries",
			zap.Error(err),
			zap.String("circuitState", p.circuitBreaker.State().String()))
		p.addToRetryBuffer(window)
	} else {
		p.circuitBreaker.RecordSuccess()
		// Update circuit breaker state metric
		CircuitBreakerState.WithLabelValues(p.cfg.Push.Endpoint).Set(float64(p.circuitBreaker.State()))
		logutil.BgLogger().Debug("statement batch pushed successfully",
			zap.Int32("sequence", batch.Metadata.BatchSequence))
	}
}

// buildBatch converts an AggregationWindow to a StatementBatch proto.
func (p *Pusher) buildBatch(window *AggregationWindow) *stmtsummaryv3proto.StatementBatch {
	batch := &stmtsummaryv3proto.StatementBatch{
		Metadata: &stmtsummaryv3proto.BatchMetadata{
			ClusterId:        p.clusterID,
			InstanceId:       p.instanceID,
			WindowStartMs:    window.Begin.UnixMilli(),
			WindowEndMs:      window.End.UnixMilli(),
			BatchSequence:    p.batchSequence.Add(1),
			BatchTimestampMs: time.Now().UnixMilli(),
			SchemaVersion:    SchemaVersion,
			FieldNames:       p.collectFieldNames(window),
		},
		Statements: make([]*stmtsummaryv3proto.Statement, 0, len(window.Statements)),
	}

	// Convert each statement
	for _, stats := range window.Statements {
		stmt := p.convertStats(stats)
		batch.Statements = append(batch.Statements, stmt)
	}

	// Include OTHER bucket if present
	if window.OtherBucket != nil {
		stmt := p.convertStats(window.OtherBucket)
		batch.Statements = append(batch.Statements, stmt)
	}

	return batch
}

// convertStats converts StmtStats to stmtsummaryv3proto.Statement.
func (p *Pusher) convertStats(stats *StmtStats) *stmtsummaryv3proto.Statement {
	// Get percentiles from histogram
	p50, p95, p99 := GetPercentiles(stats.LatencyHistogram)

	stmt := &stmtsummaryv3proto.Statement{
		// Identity
		Digest:        stats.Key.Digest,
		PlanDigest:    stats.Key.PlanDigest,
		SchemaName:    stats.Key.SchemaName,
		NormalizedSql: stats.NormalizedSQL,
		TableNames:    stats.TableNames,
		StmtType:      stats.StmtType,

		// Sample data
		SampleSql:  stats.SampleSQL,
		SamplePlan: stats.SamplePlan,
		PrevSql:    stats.PrevSQL,

		// Execution stats
		ExecCount:   stats.ExecCount,
		SumErrors:   stats.SumErrors,
		SumWarnings: stats.SumWarnings,

		// Latency (convert to microseconds)
		SumLatencyUs: stats.SumLatency.Microseconds(),
		MaxLatencyUs: stats.MaxLatency.Microseconds(),
		MinLatencyUs: stats.MinLatency.Microseconds(),
		AvgLatencyUs: 0, // Will be calculated
		P50LatencyUs: p50,
		P95LatencyUs: p95,
		P99LatencyUs: p99,

		// Parse/Compile
		SumParseLatencyUs:   stats.SumParseLatency.Microseconds(),
		MaxParseLatencyUs:   stats.MaxParseLatency.Microseconds(),
		SumCompileLatencyUs: stats.SumCompileLatency.Microseconds(),
		MaxCompileLatencyUs: stats.MaxCompileLatency.Microseconds(),

		// Resources
		SumMemBytes:  stats.SumMemBytes,
		MaxMemBytes:  stats.MaxMemBytes,
		SumDiskBytes: stats.SumDiskBytes,
		MaxDiskBytes: stats.MaxDiskBytes,
		SumTidbCpuUs: stats.SumTiDBCPU.Microseconds(),
		SumTikvCpuUs: stats.SumTiKVCPU.Microseconds(),

		// Coprocessor
		SumNumCopTasks:   stats.SumNumCopTasks,
		SumProcessTimeUs: stats.SumProcessTime.Microseconds(),
		MaxProcessTimeUs: stats.MaxProcessTime.Microseconds(),
		SumWaitTimeUs:    stats.SumWaitTime.Microseconds(),
		MaxWaitTimeUs:    stats.MaxWaitTime.Microseconds(),

		// Keys
		SumTotalKeys:     stats.SumTotalKeys,
		MaxTotalKeys:     stats.MaxTotalKeys,
		SumProcessedKeys: stats.SumProcessedKeys,
		MaxProcessedKeys: stats.MaxProcessedKeys,

		// Transaction
		CommitCount:       stats.CommitCount,
		SumPrewriteTimeUs: stats.SumPrewriteTime.Microseconds(),
		MaxPrewriteTimeUs: stats.MaxPrewriteTime.Microseconds(),
		SumCommitTimeUs:   stats.SumCommitTime.Microseconds(),
		MaxCommitTimeUs:   stats.MaxCommitTime.Microseconds(),
		SumWriteKeys:      stats.SumWriteKeys,
		MaxWriteKeys:      stats.MaxWriteKeys,
		SumWriteSizeBytes: stats.SumWriteSizeBytes,
		MaxWriteSizeBytes: stats.MaxWriteSizeBytes,

		// Rows
		SumAffectedRows: stats.SumAffectedRows,
		SumResultRows:   stats.SumResultRows,
		MaxResultRows:   stats.MaxResultRows,
		MinResultRows:   stats.MinResultRows,

		// Plan cache
		PlanInCache:   stats.PlanInCache,
		PlanCacheHits: stats.PlanCacheHits,

		// Timestamps
		FirstSeenMs: stats.FirstSeen.UnixMilli(),
		LastSeenMs:  stats.LastSeen.UnixMilli(),

		// Flags
		IsInternal: stats.IsInternal,
		Prepared:   stats.Prepared,

		// Multi-tenancy
		KeyspaceName:      stats.KeyspaceName,
		KeyspaceId:        stats.KeyspaceID,
		ResourceGroupName: stats.Key.ResourceGroupName,

		// Extended metrics
		ExtendedMetrics: make(map[string]*stmtsummaryv3proto.MetricValue),
	}

	// Calculate average latency
	if stats.ExecCount > 0 {
		stmt.AvgLatencyUs = stats.SumLatency.Microseconds() / stats.ExecCount
	}

	// Map new fields that are not in proto as extended metrics.
	// These fields were added to StmtStats to match v1 parity.
	em := stmt.ExtendedMetrics

	// Sample/Identity extended
	setStringMetric(em, "sample_binary_plan", stats.SampleBinaryPlan)
	setStringMetric(em, "plan_hint", stats.PlanHint)
	setStringMetric(em, "index_names", formatIndexNames(stats.IndexNames))
	setStringMetric(em, "charset", stats.Charset)
	setStringMetric(em, "collation", stats.Collation)
	setStringMetric(em, "binding_sql", stats.BindingSQL)
	setStringMetric(em, "binding_digest", stats.BindingDigest)
	setStringMetric(em, "sample_user", formatAuthUsers(stats.AuthUsers))

	// Coprocessor extended
	setInt64Metric(em, "sum_cop_process_time_us", stats.SumCopProcessTime.Microseconds())
	setInt64Metric(em, "max_cop_process_time_us", stats.MaxCopProcessTime.Microseconds())
	setStringMetric(em, "max_cop_process_address", stats.MaxCopProcessAddress)
	setInt64Metric(em, "sum_cop_wait_time_us", stats.SumCopWaitTime.Microseconds())
	setInt64Metric(em, "max_cop_wait_time_us", stats.MaxCopWaitTime.Microseconds())
	setStringMetric(em, "max_cop_wait_address", stats.MaxCopWaitAddress)

	// TiKV backoff
	setInt64Metric(em, "sum_backoff_time_us", stats.SumBackoffTime.Microseconds())
	setInt64Metric(em, "max_backoff_time_us", stats.MaxBackoffTime.Microseconds())

	// RocksDB
	setUint64Metric(em, "sum_rocksdb_delete_skipped_count", stats.SumRocksdbDeleteSkippedCount)
	setUint64Metric(em, "max_rocksdb_delete_skipped_count", stats.MaxRocksdbDeleteSkippedCount)
	setUint64Metric(em, "sum_rocksdb_key_skipped_count", stats.SumRocksdbKeySkippedCount)
	setUint64Metric(em, "max_rocksdb_key_skipped_count", stats.MaxRocksdbKeySkippedCount)
	setUint64Metric(em, "sum_rocksdb_block_cache_hit_count", stats.SumRocksdbBlockCacheHitCount)
	setUint64Metric(em, "max_rocksdb_block_cache_hit_count", stats.MaxRocksdbBlockCacheHitCount)
	setUint64Metric(em, "sum_rocksdb_block_read_count", stats.SumRocksdbBlockReadCount)
	setUint64Metric(em, "max_rocksdb_block_read_count", stats.MaxRocksdbBlockReadCount)
	setUint64Metric(em, "sum_rocksdb_block_read_byte", stats.SumRocksdbBlockReadByte)
	setUint64Metric(em, "max_rocksdb_block_read_byte", stats.MaxRocksdbBlockReadByte)

	// Transaction extended
	setInt64Metric(em, "sum_get_commit_ts_time_us", stats.SumGetCommitTsTime.Microseconds())
	setInt64Metric(em, "max_get_commit_ts_time_us", stats.MaxGetCommitTsTime.Microseconds())
	setInt64Metric(em, "sum_local_latch_time_us", stats.SumLocalLatchTime.Microseconds())
	setInt64Metric(em, "max_local_latch_time_us", stats.MaxLocalLatchTime.Microseconds())
	setInt64Metric(em, "sum_commit_backoff_time", stats.SumCommitBackoffTime)
	setInt64Metric(em, "max_commit_backoff_time", stats.MaxCommitBackoffTime)
	setInt64Metric(em, "sum_resolve_lock_time", stats.SumResolveLockTime)
	setInt64Metric(em, "max_resolve_lock_time", stats.MaxResolveLockTime)
	setInt64Metric(em, "sum_prewrite_region_num", stats.SumPrewriteRegionNum)
	setInt64Metric(em, "max_prewrite_region_num", int64(stats.MaxPrewriteRegionNum))
	setInt64Metric(em, "sum_txn_retry", stats.SumTxnRetry)
	setInt64Metric(em, "max_txn_retry", int64(stats.MaxTxnRetry))
	setInt64Metric(em, "sum_backoff_times", stats.SumBackoffTimes)
	setStringMetric(em, "backoff_types", formatBackoffTypes(stats.BackoffTypes))

	// Plan cache extended
	setBoolMetric(em, "plan_in_binding", stats.PlanInBinding)
	setInt64Metric(em, "plan_cache_unqualified_count", stats.PlanCacheUnqualifiedCount)
	setStringMetric(em, "plan_cache_unqualified_last_reason", stats.PlanCacheUnqualifiedLastReason)

	// Other
	setInt64Metric(em, "sum_kv_total_us", stats.SumKVTotal.Microseconds())
	setInt64Metric(em, "sum_pd_total_us", stats.SumPDTotal.Microseconds())
	setInt64Metric(em, "sum_backoff_total_us", stats.SumBackoffTotal.Microseconds())
	setInt64Metric(em, "sum_write_sql_resp_total_us", stats.SumWriteSQLRespTotal.Microseconds())
	setInt64Metric(em, "exec_retry_count", int64(stats.ExecRetryCount))
	setInt64Metric(em, "exec_retry_time_us", stats.ExecRetryTime.Microseconds())
	setDoubleMetric(em, "sum_mem_arbitration", stats.SumMemArbitration)
	setDoubleMetric(em, "max_mem_arbitration", stats.MaxMemArbitration)
	setBoolMetric(em, "storage_kv", stats.StorageKV)
	setBoolMetric(em, "storage_mpp", stats.StorageMPP)

	// RU
	setDoubleMetric(em, "sum_rru", stats.SumRRU)
	setDoubleMetric(em, "max_rru", stats.MaxRRU)
	setDoubleMetric(em, "sum_wru", stats.SumWRU)
	setDoubleMetric(em, "max_wru", stats.MaxWRU)
	setInt64Metric(em, "sum_ru_wait_duration_us", stats.SumRUWaitDuration.Microseconds())
	setInt64Metric(em, "max_ru_wait_duration_us", stats.MaxRUWaitDuration.Microseconds())

	// Network traffic
	setInt64Metric(em, "sum_unpacked_bytes_sent_tikv_total", stats.UnpackedBytesSentTiKVTotal)
	setInt64Metric(em, "sum_unpacked_bytes_received_tikv_total", stats.UnpackedBytesReceivedTiKVTotal)
	setInt64Metric(em, "sum_unpacked_bytes_sent_tikv_cross_zone", stats.UnpackedBytesSentTiKVCrossZone)
	setInt64Metric(em, "sum_unpacked_bytes_received_tikv_cross_zone", stats.UnpackedBytesReceivedTiKVCrossZone)
	setInt64Metric(em, "sum_unpacked_bytes_sent_tiflash_total", stats.UnpackedBytesSentTiFlashTotal)
	setInt64Metric(em, "sum_unpacked_bytes_received_tiflash_total", stats.UnpackedBytesReceivedTiFlashTotal)
	setInt64Metric(em, "sum_unpacked_bytes_sent_tiflash_cross_zone", stats.UnpackedBytesSentTiFlashCrossZone)
	setInt64Metric(em, "sum_unpacked_bytes_received_tiflash_cross_zone", stats.UnpackedBytesReceivedTiFlashCrossZone)

	// Convert user-defined extended metrics
	for name, value := range stats.ExtendedMetrics {
		em[name] = p.convertMetricValue(value)
	}

	return stmt
}

// Extended metric setter helpers.
func setStringMetric(m map[string]*stmtsummaryv3proto.MetricValue, name, val string) {
	m[name] = &stmtsummaryv3proto.MetricValue{Value: &stmtsummaryv3proto.MetricValue_StringVal{StringVal: val}}
}

func setInt64Metric(m map[string]*stmtsummaryv3proto.MetricValue, name string, val int64) {
	m[name] = &stmtsummaryv3proto.MetricValue{Value: &stmtsummaryv3proto.MetricValue_Int64Val{Int64Val: val}}
}

func setUint64Metric(m map[string]*stmtsummaryv3proto.MetricValue, name string, val uint64) {
	m[name] = &stmtsummaryv3proto.MetricValue{Value: &stmtsummaryv3proto.MetricValue_Int64Val{Int64Val: int64(val)}}
}

func setDoubleMetric(m map[string]*stmtsummaryv3proto.MetricValue, name string, val float64) {
	m[name] = &stmtsummaryv3proto.MetricValue{Value: &stmtsummaryv3proto.MetricValue_DoubleVal{DoubleVal: val}}
}

func setBoolMetric(m map[string]*stmtsummaryv3proto.MetricValue, name string, val bool) {
	m[name] = &stmtsummaryv3proto.MetricValue{Value: &stmtsummaryv3proto.MetricValue_BoolVal{BoolVal: val}}
}

// convertMetricValue converts internal MetricValue to proto MetricValue.
func (p *Pusher) convertMetricValue(v MetricValue) *stmtsummaryv3proto.MetricValue {
	switch v.Type {
	case MetricTypeInt64:
		return &stmtsummaryv3proto.MetricValue{Value: &stmtsummaryv3proto.MetricValue_Int64Val{Int64Val: v.Int64}}
	case MetricTypeFloat64:
		return &stmtsummaryv3proto.MetricValue{Value: &stmtsummaryv3proto.MetricValue_DoubleVal{DoubleVal: v.Float64}}
	case MetricTypeString:
		return &stmtsummaryv3proto.MetricValue{Value: &stmtsummaryv3proto.MetricValue_StringVal{StringVal: v.String}}
	case MetricTypeBool:
		return &stmtsummaryv3proto.MetricValue{Value: &stmtsummaryv3proto.MetricValue_BoolVal{BoolVal: v.Bool}}
	default:
		return nil
	}
}

// collectFieldNames collects all extended metric names for schema discovery.
func (p *Pusher) collectFieldNames(window *AggregationWindow) []string {
	names := make(map[string]struct{})
	for _, stats := range window.Statements {
		for name := range stats.ExtendedMetrics {
			names[name] = struct{}{}
		}
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	return result
}

// doPush performs the actual gRPC push.
func (p *Pusher) doPush(batch *stmtsummaryv3proto.StatementBatch) vectorsvc.PushResult {
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.Push.Timeout)
	defer cancel()

	start := time.Now()
	resp, err := p.client.PushStatements(ctx, batch)
	latency := time.Since(start)

	if err != nil {
		return vectorsvc.PushResult{
			Success: false,
			Message: err.Error(),
			Latency: latency,
		}
	}

	return vectorsvc.PushResult{
		Success:       resp.Success,
		Message:       resp.Message,
		ReceivedCount: resp.AcceptedCount + resp.RejectedCount,
		AcceptedCount: resp.AcceptedCount,
		RejectedCount: resp.RejectedCount,
		Errors:        resp.Errors,
		Latency:       latency,
	}
}

// Retry buffer methods

func (p *Pusher) addToRetryBuffer(window *AggregationWindow) {
	p.retryBufferLock.Lock()
	defer p.retryBufferLock.Unlock()

	// Limit retry buffer size
	if len(p.retryBuffer) >= 10 {
		// Drop oldest
		p.retryBuffer = p.retryBuffer[1:]
		RetryBufferDroppedTotal.Inc()
	}
	p.retryBuffer = append(p.retryBuffer, window)
	RetryBufferSize.Set(float64(len(p.retryBuffer)))
}

func (p *Pusher) retryFailedBatches() {
	if !p.circuitBreaker.Allow() {
		return
	}

	p.retryBufferLock.Lock()
	batches := p.retryBuffer
	p.retryBuffer = nil
	p.retryBufferLock.Unlock()

	RetryBufferSize.Set(0)

	for _, window := range batches {
		p.pushWithRetry(window)
	}
}

func (p *Pusher) drainPendingBatches() {
	for {
		select {
		case window := <-p.pendingBatches:
			batch := p.buildBatch(window)
			_ = p.doPush(batch)
		default:
			return
		}
	}
}

// Close closes the pusher and releases resources.
func (p *Pusher) Close() error {
	if p.closed.Swap(true) {
		return nil
	}

	p.cancel()
	p.wg.Wait()

	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

// Ping checks connectivity to the Vector service.
func (p *Pusher) Ping() error {
	_, err := p.ping()
	return err
}

// ping performs a Ping RPC and returns the full response.
func (p *Pusher) ping() (*stmtsummaryv3proto.PingResponse, error) {
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()

	resp, err := p.client.Ping(ctx, &stmtsummaryv3proto.PingRequest{
		ClusterId:  p.clusterID,
		InstanceId: p.instanceID,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, errors.New("ping failed: server returned not ok")
	}
	return resp, nil
}

// applyRemoteConfig applies the CollectionConfig from a PingResponse to the local config.
func (p *Pusher) applyRemoteConfig(resp *stmtsummaryv3proto.PingResponse) {
	cc := resp.CollectionConfig
	if cc == nil {
		return
	}

	p.cfg.MergeFromRemote(cc)
	p.remoteConfigVersion.Store(cc.ConfigVersion)

	logutil.BgLogger().Info("applied remote collection config from Vector",
		zap.Int64("config_version", cc.ConfigVersion),
		zap.Int32("aggregation_window_secs", cc.AggregationWindowSecs),
		zap.Int32("push_batch_size", cc.PushBatchSize),
		zap.Int32("push_interval_secs", cc.PushIntervalSecs),
		zap.Int32("max_digests_per_window", cc.MaxDigestsPerWindow),
		zap.Int64("max_memory_bytes", cc.MaxMemoryBytes))

	if p.onConfigChange != nil {
		p.onConfigChange(p.cfg)
	}
}

// syncRemoteConfig performs a Ping and applies new config if the version changed.
func (p *Pusher) syncRemoteConfig() {
	resp, err := p.ping()
	if err != nil {
		logutil.BgLogger().Debug("config sync ping failed", zap.Error(err))
		return
	}
	cc := resp.CollectionConfig
	if cc == nil {
		return
	}
	if cc.ConfigVersion <= p.remoteConfigVersion.Load() {
		return
	}

	logutil.BgLogger().Info("remote config version changed, applying update",
		zap.Int64("old_version", p.remoteConfigVersion.Load()),
		zap.Int64("new_version", cc.ConfigVersion))
	p.applyRemoteConfig(resp)
}

// SetOnConfigChange registers a callback invoked when remote config is applied.
func (p *Pusher) SetOnConfigChange(fn func(*Config)) {
	p.onConfigChange = fn
}

// CircuitState returns the current circuit breaker state.
func (p *Pusher) CircuitState() vectorsvc.CircuitState {
	return p.circuitBreaker.State()
}

// validateContract fetches and validates the requirements contract from Vector.
func (p *Pusher) validateContract(ctx context.Context) error {
	contractURL := p.cfg.Push.ContractURL
	if contractURL == "" {
		logutil.BgLogger().Info("no contract URL configured, skipping contract validation")
		return nil
	}

	client := NewContractClient(contractURL)
	if err := client.FetchContract(ctx); err != nil {
		return fmt.Errorf("fetch contract: %w", err)
	}

	result, err := client.Validate()
	if err != nil {
		return fmt.Errorf("validate contract: %w", err)
	}

	// Log validation result
	logutil.BgLogger().Info("contract validation completed",
		zap.String("version", result.Version),
		zap.String("publisher", result.Publisher),
		zap.Bool("valid", result.Valid),
		zap.Int("total_fields", result.TotalFields),
		zap.Int("missing", len(result.Missing)),
		zap.Int("warnings", len(result.Warnings)))

	// Log warnings for optional fields
	for _, warning := range result.Warnings {
		logutil.BgLogger().Warn("contract validation warning", zap.String("warning", warning))
	}

	// Fail startup if required fields are missing
	if !result.Valid {
		for _, missing := range result.Missing {
			logutil.BgLogger().Error("missing required field",
				zap.String("field", missing))
		}
		return fmt.Errorf("contract validation failed: missing %d required fields", len(result.Missing))
	}

	return nil
}
