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
	"os"
	"sync"
	"sync/atomic"
	"time"

	stmtsummaryv3proto "github.com/pingcap/tidb/pkg/util/stmtsummary/v3/proto/v1"
	"github.com/pingcap/tidb/pkg/util/logutil"
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

	// Circuit breaker state
	circuitState  atomic.Int32
	failureCount  atomic.Int32
	lastFailure   atomic.Int64
	successCount  atomic.Int32

	// Batch queue
	pendingBatches chan *AggregationWindow

	// Retry buffer for failed batches
	retryBuffer     []*AggregationWindow
	retryBufferLock sync.Mutex

	// Sequence tracking
	batchSequence atomic.Int32

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

	// Initialize circuit breaker as closed (healthy)
	p.circuitState.Store(int32(CircuitClosed))

	// Establish gRPC connection
	if err := p.connect(); err != nil {
		cancel()
		return nil, err
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
		}
	}
}

// pushWithRetry attempts to push a window with retry logic.
func (p *Pusher) pushWithRetry(window *AggregationWindow) {
	// Check circuit breaker
	if !p.canPush() {
		p.addToRetryBuffer(window)
		return
	}

	batch := p.buildBatch(window)
	result := p.doPush(batch)

	if result.Success {
		p.recordSuccess()
		logutil.BgLogger().Debug("statement batch pushed successfully",
			zap.Int32("accepted", result.AcceptedCount),
			zap.Duration("latency", result.Latency))
	} else {
		p.recordFailure()
		logutil.BgLogger().Warn("statement batch push failed",
			zap.String("message", result.Message),
			zap.Strings("errors", result.Errors))
		p.addToRetryBuffer(window)
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

	// Convert extended metrics
	for name, value := range stats.ExtendedMetrics {
		stmt.ExtendedMetrics[name] = p.convertMetricValue(value)
	}

	return stmt
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
func (p *Pusher) doPush(batch *stmtsummaryv3proto.StatementBatch) PushResult {
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.Push.Timeout)
	defer cancel()

	start := time.Now()
	resp, err := p.client.PushStatements(ctx, batch)
	latency := time.Since(start)

	if err != nil {
		return PushResult{
			Success: false,
			Message: err.Error(),
			Latency: latency,
		}
	}

	return PushResult{
		Success:       resp.Success,
		Message:       resp.Message,
		ReceivedCount: resp.AcceptedCount + resp.RejectedCount,
		AcceptedCount: resp.AcceptedCount,
		RejectedCount: resp.RejectedCount,
		Errors:        resp.Errors,
		Latency:       latency,
	}
}

// Circuit breaker methods

func (p *Pusher) canPush() bool {
	state := CircuitState(p.circuitState.Load())
	switch state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		// Check if we should transition to half-open
		lastFailure := time.UnixMilli(p.lastFailure.Load())
		if time.Since(lastFailure) > p.cfg.Push.Retry.MaxDelay {
			p.circuitState.Store(int32(CircuitHalfOpen))
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	default:
		return true
	}
}

func (p *Pusher) recordSuccess() {
	p.successCount.Add(1)
	state := CircuitState(p.circuitState.Load())
	if state == CircuitHalfOpen {
		// Transition to closed
		p.circuitState.Store(int32(CircuitClosed))
		p.failureCount.Store(0)
	}
}

func (p *Pusher) recordFailure() {
	p.failureCount.Add(1)
	p.lastFailure.Store(time.Now().UnixMilli())

	if p.failureCount.Load() >= int32(p.cfg.Push.Retry.MaxAttempts) {
		p.circuitState.Store(int32(CircuitOpen))
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
	}
	p.retryBuffer = append(p.retryBuffer, window)
}

func (p *Pusher) retryFailedBatches() {
	if !p.canPush() {
		return
	}

	p.retryBufferLock.Lock()
	batches := p.retryBuffer
	p.retryBuffer = nil
	p.retryBufferLock.Unlock()

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
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()

	resp, err := p.client.Ping(ctx, &stmtsummaryv3proto.PingRequest{
		ClusterId:  p.clusterID,
		InstanceId: p.instanceID,
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New("ping failed: server returned not ok")
	}
	return nil
}

// CircuitState returns the current circuit breaker state.
func (p *Pusher) CircuitState() CircuitState {
	return CircuitState(p.circuitState.Load())
}
