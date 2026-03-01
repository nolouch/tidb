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
	"strings"
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

var statementSummaryColumns = []string{
	"INSTANCE", "SUMMARY_BEGIN_TIME", "SUMMARY_END_TIME", "STMT_TYPE", "SCHEMA_NAME", "DIGEST", "DIGEST_TEXT",
	"TABLE_NAMES", "INDEX_NAMES", "SAMPLE_USER", "EXEC_COUNT", "SUM_ERRORS", "SUM_WARNINGS", "SUM_LATENCY",
	"MAX_LATENCY", "MIN_LATENCY", "AVG_LATENCY", "AVG_PARSE_LATENCY", "MAX_PARSE_LATENCY", "AVG_COMPILE_LATENCY",
	"MAX_COMPILE_LATENCY", "SUM_COP_TASK_NUM", "MAX_COP_PROCESS_TIME", "MAX_COP_PROCESS_ADDRESS", "MAX_COP_WAIT_TIME",
	"MAX_COP_WAIT_ADDRESS", "AVG_PROCESS_TIME", "MAX_PROCESS_TIME", "AVG_WAIT_TIME", "MAX_WAIT_TIME", "AVG_BACKOFF_TIME",
	"MAX_BACKOFF_TIME", "AVG_TOTAL_KEYS", "MAX_TOTAL_KEYS", "AVG_PROCESSED_KEYS", "MAX_PROCESSED_KEYS",
	"AVG_ROCKSDB_DELETE_SKIPPED_COUNT", "MAX_ROCKSDB_DELETE_SKIPPED_COUNT", "AVG_ROCKSDB_KEY_SKIPPED_COUNT",
	"MAX_ROCKSDB_KEY_SKIPPED_COUNT", "AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT", "MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT",
	"AVG_ROCKSDB_BLOCK_READ_COUNT", "MAX_ROCKSDB_BLOCK_READ_COUNT", "AVG_ROCKSDB_BLOCK_READ_BYTE",
	"MAX_ROCKSDB_BLOCK_READ_BYTE", "AVG_PREWRITE_TIME", "MAX_PREWRITE_TIME", "AVG_COMMIT_TIME", "MAX_COMMIT_TIME",
	"AVG_GET_COMMIT_TS_TIME", "MAX_GET_COMMIT_TS_TIME", "AVG_COMMIT_BACKOFF_TIME", "MAX_COMMIT_BACKOFF_TIME",
	"AVG_RESOLVE_LOCK_TIME", "MAX_RESOLVE_LOCK_TIME", "AVG_LOCAL_LATCH_WAIT_TIME", "MAX_LOCAL_LATCH_WAIT_TIME",
	"AVG_WRITE_KEYS", "MAX_WRITE_KEYS", "AVG_WRITE_SIZE", "MAX_WRITE_SIZE", "AVG_PREWRITE_REGIONS",
	"MAX_PREWRITE_REGIONS", "AVG_TXN_RETRY", "MAX_TXN_RETRY", "SUM_EXEC_RETRY", "SUM_EXEC_RETRY_TIME",
	"SUM_BACKOFF_TIMES", "BACKOFF_TYPES", "AVG_MEM", "MAX_MEM", "AVG_MEM_ARBITRATION", "MAX_MEM_ARBITRATION",
	"AVG_DISK", "MAX_DISK", "AVG_KV_TIME", "AVG_PD_TIME", "AVG_BACKOFF_TOTAL_TIME", "AVG_WRITE_SQL_RESP_TIME",
	"AVG_TIDB_CPU_TIME", "AVG_TIKV_CPU_TIME", "MAX_RESULT_ROWS", "MIN_RESULT_ROWS", "AVG_RESULT_ROWS", "PREPARED",
	"AVG_AFFECTED_ROWS", "FIRST_SEEN", "LAST_SEEN", "PLAN_IN_CACHE", "PLAN_CACHE_HITS", "PLAN_IN_BINDING",
	"QUERY_SAMPLE_TEXT", "PREV_SAMPLE_TEXT", "PLAN_DIGEST", "PLAN", "BINARY_PLAN", "BINDING_DIGEST",
	"BINDING_DIGEST_TEXT", "CHARSET", "COLLATION", "PLAN_HINT", "MAX_REQUEST_UNIT_READ", "AVG_REQUEST_UNIT_READ",
	"MAX_REQUEST_UNIT_WRITE", "AVG_REQUEST_UNIT_WRITE", "MAX_QUEUED_RC_TIME", "AVG_QUEUED_RC_TIME", "RESOURCE_GROUP",
	"PLAN_CACHE_UNQUALIFIED", "PLAN_CACHE_UNQUALIFIED_LAST_REASON", "SUM_UNPACKED_BYTES_SENT_TIKV_TOTAL",
	"SUM_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL", "SUM_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE",
	"SUM_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE", "SUM_UNPACKED_BYTES_SENT_TIFLASH_TOTAL",
	"SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL", "SUM_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE",
	"SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE", "STORAGE_KV", "STORAGE_MPP",
}

func nullProtoValue() *stmtsummaryv3proto.Value {
	return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_NullVal{NullVal: stmtsummaryv3proto.NullValue_NULL}}
}

func stringProtoValue(v string) *stmtsummaryv3proto.Value {
	return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_StringVal{StringVal: v}}
}

func int64ProtoValue(v int64) *stmtsummaryv3proto.Value {
	return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Int64Val{Int64Val: v}}
}

func uint64ProtoValue(v uint64) *stmtsummaryv3proto.Value {
	return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Uint64Val{Uint64Val: v}}
}

func float64ProtoValue(v float64) *stmtsummaryv3proto.Value {
	return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Float64Val{Float64Val: v}}
}

func boolProtoValue(v bool) *stmtsummaryv3proto.Value {
	return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_BoolVal{BoolVal: v}}
}

func timestampProtoValue(ms int64) *stmtsummaryv3proto.Value {
	return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_TimestampMs{TimestampMs: ms}}
}

func inferProtoDataType(v *stmtsummaryv3proto.Value) stmtsummaryv3proto.DataType {
	if v == nil || v.Kind == nil {
		return stmtsummaryv3proto.DataType_UNKNOWN
	}
	switch v.Kind.(type) {
	case *stmtsummaryv3proto.Value_StringVal:
		return stmtsummaryv3proto.DataType_STRING
	case *stmtsummaryv3proto.Value_Int64Val:
		return stmtsummaryv3proto.DataType_INT64
	case *stmtsummaryv3proto.Value_Uint64Val:
		return stmtsummaryv3proto.DataType_UINT64
	case *stmtsummaryv3proto.Value_Float64Val:
		return stmtsummaryv3proto.DataType_FLOAT64
	case *stmtsummaryv3proto.Value_BoolVal:
		return stmtsummaryv3proto.DataType_BOOL
	case *stmtsummaryv3proto.Value_BytesVal:
		return stmtsummaryv3proto.DataType_BYTES
	case *stmtsummaryv3proto.Value_TimestampMs:
		return stmtsummaryv3proto.DataType_TIMESTAMP
	case *stmtsummaryv3proto.Value_DurationUs:
		return stmtsummaryv3proto.DataType_DURATION
	case *stmtsummaryv3proto.Value_JsonVal:
		return stmtsummaryv3proto.DataType_JSON
	default:
		return stmtsummaryv3proto.DataType_UNKNOWN
	}
}

func statementSummaryProtoDataType(column string) (stmtsummaryv3proto.DataType, bool) {
	switch column {
	case "SUMMARY_BEGIN_TIME", "SUMMARY_END_TIME", "FIRST_SEEN", "LAST_SEEN":
		return stmtsummaryv3proto.DataType_TIMESTAMP, true
	case "PREPARED", "PLAN_IN_CACHE", "PLAN_IN_BINDING", "PLAN_CACHE_UNQUALIFIED", "STORAGE_KV", "STORAGE_MPP":
		return stmtsummaryv3proto.DataType_BOOL, true
	case "MAX_ROCKSDB_DELETE_SKIPPED_COUNT", "MAX_ROCKSDB_KEY_SKIPPED_COUNT", "MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT", "MAX_ROCKSDB_BLOCK_READ_COUNT", "MAX_ROCKSDB_BLOCK_READ_BYTE":
		return stmtsummaryv3proto.DataType_UINT64, true
	case "AVG_PARSE_LATENCY", "AVG_COMPILE_LATENCY", "AVG_PROCESS_TIME", "AVG_WAIT_TIME", "AVG_BACKOFF_TIME", "AVG_TOTAL_KEYS",
		"AVG_PROCESSED_KEYS", "AVG_ROCKSDB_DELETE_SKIPPED_COUNT", "AVG_ROCKSDB_KEY_SKIPPED_COUNT", "AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT",
		"AVG_ROCKSDB_BLOCK_READ_COUNT", "AVG_ROCKSDB_BLOCK_READ_BYTE", "AVG_PREWRITE_TIME", "AVG_COMMIT_TIME", "AVG_GET_COMMIT_TS_TIME",
		"AVG_COMMIT_BACKOFF_TIME", "AVG_RESOLVE_LOCK_TIME", "AVG_LOCAL_LATCH_WAIT_TIME", "AVG_WRITE_KEYS", "AVG_WRITE_SIZE",
		"AVG_PREWRITE_REGIONS", "AVG_TXN_RETRY", "AVG_MEM", "AVG_MEM_ARBITRATION", "MAX_MEM_ARBITRATION", "AVG_DISK",
		"AVG_KV_TIME", "AVG_PD_TIME", "AVG_BACKOFF_TOTAL_TIME", "AVG_WRITE_SQL_RESP_TIME", "AVG_TIDB_CPU_TIME", "AVG_TIKV_CPU_TIME",
		"AVG_RESULT_ROWS", "AVG_AFFECTED_ROWS", "MAX_REQUEST_UNIT_READ", "AVG_REQUEST_UNIT_READ", "MAX_REQUEST_UNIT_WRITE",
		"AVG_REQUEST_UNIT_WRITE", "AVG_QUEUED_RC_TIME":
		return stmtsummaryv3proto.DataType_FLOAT64, true
	case "INSTANCE", "STMT_TYPE", "SCHEMA_NAME", "DIGEST", "DIGEST_TEXT", "TABLE_NAMES", "INDEX_NAMES", "SAMPLE_USER",
		"MAX_COP_PROCESS_ADDRESS", "MAX_COP_WAIT_ADDRESS", "BACKOFF_TYPES", "QUERY_SAMPLE_TEXT", "PREV_SAMPLE_TEXT",
		"PLAN_DIGEST", "PLAN", "BINARY_PLAN", "BINDING_DIGEST", "BINDING_DIGEST_TEXT", "CHARSET", "COLLATION", "PLAN_HINT",
		"RESOURCE_GROUP", "PLAN_CACHE_UNQUALIFIED_LAST_REASON":
		return stmtsummaryv3proto.DataType_STRING, true
	case "EXEC_COUNT", "SUM_ERRORS", "SUM_WARNINGS", "SUM_LATENCY", "MAX_LATENCY", "MIN_LATENCY", "AVG_LATENCY",
		"MAX_PARSE_LATENCY", "MAX_COMPILE_LATENCY", "SUM_COP_TASK_NUM", "MAX_COP_PROCESS_TIME", "MAX_COP_WAIT_TIME",
		"MAX_PROCESS_TIME", "MAX_WAIT_TIME", "MAX_BACKOFF_TIME", "MAX_TOTAL_KEYS", "MAX_PROCESSED_KEYS",
		"MAX_PREWRITE_TIME", "MAX_COMMIT_TIME", "MAX_GET_COMMIT_TS_TIME", "MAX_COMMIT_BACKOFF_TIME",
		"MAX_RESOLVE_LOCK_TIME", "MAX_LOCAL_LATCH_WAIT_TIME", "MAX_WRITE_KEYS", "MAX_WRITE_SIZE", "MAX_PREWRITE_REGIONS",
		"MAX_TXN_RETRY", "SUM_EXEC_RETRY", "SUM_EXEC_RETRY_TIME", "SUM_BACKOFF_TIMES", "MAX_MEM", "MAX_DISK",
		"MAX_RESULT_ROWS", "MIN_RESULT_ROWS", "PLAN_CACHE_HITS", "MAX_QUEUED_RC_TIME", "SUM_UNPACKED_BYTES_SENT_TIKV_TOTAL",
		"SUM_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL", "SUM_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE", "SUM_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE",
		"SUM_UNPACKED_BYTES_SENT_TIFLASH_TOTAL", "SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL", "SUM_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE",
		"SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE":
		return stmtsummaryv3proto.DataType_INT64, true
	default:
		return stmtsummaryv3proto.DataType_UNKNOWN, false
	}
}

func avgFloat(sum float64, execCount int64) float64 {
	if execCount <= 0 {
		return 0
	}
	return sum / float64(execCount)
}

func avgInt64(sum int64, execCount int64) float64 {
	if execCount <= 0 {
		return 0
	}
	return float64(sum) / float64(execCount)
}

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

	batch := p.buildTableRowBatch(window)
	statementCount := int64(len(batch.Rows))

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

// buildTableRowBatch converts an AggregationWindow to a TableRowBatch with
// CLUSTER_STATEMENTS_SUMMARY-compatible schema/column names.
func (p *Pusher) buildTableRowBatch(window *AggregationWindow) *stmtsummaryv3proto.TableRowBatch {
	metadata := &stmtsummaryv3proto.BatchMetadata{
		ClusterId:        p.clusterID,
		InstanceId:       p.instanceID,
		WindowStartMs:    window.Begin.UnixMilli(),
		WindowEndMs:      window.End.UnixMilli(),
		BatchSequence:    p.batchSequence.Add(1),
		BatchTimestampMs: time.Now().UnixMilli(),
		SchemaVersion:    SchemaVersion,
		FieldNames:       statementSummaryColumns,
	}

	rows := make([]*stmtsummaryv3proto.TableRow, 0, len(window.Statements)+1)
	for _, stats := range window.Statements {
		rows = append(rows, p.toStatementSummaryRow(stats, window))
	}
	if window.OtherBucket != nil {
		rows = append(rows, p.toStatementSummaryRow(window.OtherBucket, window))
	}

	columns := make([]*stmtsummaryv3proto.Column, 0, len(statementSummaryColumns))
	for i, name := range statementSummaryColumns {
		dataType, fromProto := statementSummaryProtoDataType(name)
		if !fromProto {
			logutil.BgLogger().Warn("statement summary column type not mapped in proto type map, fallback to value inference",
				zap.String("column", name))
			dataType = stmtsummaryv3proto.DataType_UNKNOWN
			for _, row := range rows {
				if row == nil || i >= len(row.Values) {
					continue
				}
				inferred := inferProtoDataType(row.Values[i])
				if inferred != stmtsummaryv3proto.DataType_UNKNOWN {
					dataType = inferred
					logutil.BgLogger().Warn("statement summary column type inferred from row value",
						zap.String("column", name),
						zap.String("inferredType", inferred.String()))
					break
				}
			}
			if dataType == stmtsummaryv3proto.DataType_UNKNOWN {
				dataType = stmtsummaryv3proto.DataType_STRING
				logutil.BgLogger().Warn("statement summary column type fallback to STRING due empty/unknown values",
					zap.String("column", name))
			}
		}
		columns = append(columns, &stmtsummaryv3proto.Column{
			Name:     name,
			Ordinal:  int32(i),
			Type:     dataType,
			Nullable: true,
		})
	}

	schema := &stmtsummaryv3proto.TableSchema{
		TableName: "CLUSTER_STATEMENTS_SUMMARY",
		Columns:   columns,
	}

	return &stmtsummaryv3proto.TableRowBatch{
		Metadata:  metadata,
		TableName: "CLUSTER_STATEMENTS_SUMMARY",
		Schema:    schema,
		Rows:      rows,
	}
}

func (p *Pusher) toStatementSummaryRow(stats *StmtStats, window *AggregationWindow) *stmtsummaryv3proto.TableRow {
	stmt := p.convertStats(stats)
	execCount := stats.ExecCount

	row := map[string]*stmtsummaryv3proto.Value{
		"INSTANCE":                                stringProtoValue(p.instanceID),
		"SUMMARY_BEGIN_TIME":                      timestampProtoValue(window.Begin.UnixMilli()),
		"SUMMARY_END_TIME":                        timestampProtoValue(window.End.UnixMilli()),
		"STMT_TYPE":                               stringProtoValue(stats.StmtType),
		"SCHEMA_NAME":                             stringProtoValue(stats.Key.SchemaName),
		"DIGEST":                                  stringProtoValue(stats.Key.Digest),
		"DIGEST_TEXT":                             stringProtoValue(stats.NormalizedSQL),
		"TABLE_NAMES":                             stringProtoValue(stats.TableNames),
		"INDEX_NAMES":                             stringProtoValue(strings.Join(stats.IndexNames, ",")),
		"SAMPLE_USER":                             stringProtoValue(formatAuthUsers(stats.AuthUsers)),
		"EXEC_COUNT":                              int64ProtoValue(stats.ExecCount),
		"SUM_ERRORS":                              int64ProtoValue(stats.SumErrors),
		"SUM_WARNINGS":                            int64ProtoValue(stats.SumWarnings),
		"SUM_LATENCY":                             int64ProtoValue(stats.SumLatency.Microseconds()),
		"MAX_LATENCY":                             int64ProtoValue(stats.MaxLatency.Microseconds()),
		"MIN_LATENCY":                             int64ProtoValue(stats.MinLatency.Microseconds()),
		"AVG_LATENCY":                             int64ProtoValue(stmt.AvgLatencyUs),
		"AVG_PARSE_LATENCY":                       float64ProtoValue(avgInt64(stats.SumParseLatency.Microseconds(), execCount)),
		"MAX_PARSE_LATENCY":                       int64ProtoValue(stats.MaxParseLatency.Microseconds()),
		"AVG_COMPILE_LATENCY":                     float64ProtoValue(avgInt64(stats.SumCompileLatency.Microseconds(), execCount)),
		"MAX_COMPILE_LATENCY":                     int64ProtoValue(stats.MaxCompileLatency.Microseconds()),
		"SUM_COP_TASK_NUM":                        int64ProtoValue(stats.SumNumCopTasks),
		"MAX_COP_PROCESS_TIME":                    int64ProtoValue(stats.MaxCopProcessTime.Microseconds()),
		"MAX_COP_PROCESS_ADDRESS":                 stringProtoValue(stats.MaxCopProcessAddress),
		"MAX_COP_WAIT_TIME":                       int64ProtoValue(stats.MaxCopWaitTime.Microseconds()),
		"MAX_COP_WAIT_ADDRESS":                    stringProtoValue(stats.MaxCopWaitAddress),
		"AVG_PROCESS_TIME":                        float64ProtoValue(avgInt64(stats.SumProcessTime.Microseconds(), execCount)),
		"MAX_PROCESS_TIME":                        int64ProtoValue(stats.MaxProcessTime.Microseconds()),
		"AVG_WAIT_TIME":                           float64ProtoValue(avgInt64(stats.SumWaitTime.Microseconds(), execCount)),
		"MAX_WAIT_TIME":                           int64ProtoValue(stats.MaxWaitTime.Microseconds()),
		"AVG_BACKOFF_TIME":                        float64ProtoValue(avgInt64(stats.SumBackoffTime.Microseconds(), execCount)),
		"MAX_BACKOFF_TIME":                        int64ProtoValue(stats.MaxBackoffTime.Microseconds()),
		"AVG_TOTAL_KEYS":                          float64ProtoValue(avgInt64(stats.SumTotalKeys, execCount)),
		"MAX_TOTAL_KEYS":                          int64ProtoValue(stats.MaxTotalKeys),
		"AVG_PROCESSED_KEYS":                      float64ProtoValue(avgInt64(stats.SumProcessedKeys, execCount)),
		"MAX_PROCESSED_KEYS":                      int64ProtoValue(stats.MaxProcessedKeys),
		"AVG_ROCKSDB_DELETE_SKIPPED_COUNT":        float64ProtoValue(avgFloat(float64(stats.SumRocksdbDeleteSkippedCount), execCount)),
		"MAX_ROCKSDB_DELETE_SKIPPED_COUNT":        uint64ProtoValue(stats.MaxRocksdbDeleteSkippedCount),
		"AVG_ROCKSDB_KEY_SKIPPED_COUNT":           float64ProtoValue(avgFloat(float64(stats.SumRocksdbKeySkippedCount), execCount)),
		"MAX_ROCKSDB_KEY_SKIPPED_COUNT":           uint64ProtoValue(stats.MaxRocksdbKeySkippedCount),
		"AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT":       float64ProtoValue(avgFloat(float64(stats.SumRocksdbBlockCacheHitCount), execCount)),
		"MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT":       uint64ProtoValue(stats.MaxRocksdbBlockCacheHitCount),
		"AVG_ROCKSDB_BLOCK_READ_COUNT":            float64ProtoValue(avgFloat(float64(stats.SumRocksdbBlockReadCount), execCount)),
		"MAX_ROCKSDB_BLOCK_READ_COUNT":            uint64ProtoValue(stats.MaxRocksdbBlockReadCount),
		"AVG_ROCKSDB_BLOCK_READ_BYTE":             float64ProtoValue(avgFloat(float64(stats.SumRocksdbBlockReadByte), execCount)),
		"MAX_ROCKSDB_BLOCK_READ_BYTE":             uint64ProtoValue(stats.MaxRocksdbBlockReadByte),
		"AVG_PREWRITE_TIME":                       float64ProtoValue(avgInt64(stats.SumPrewriteTime.Microseconds(), execCount)),
		"MAX_PREWRITE_TIME":                       int64ProtoValue(stats.MaxPrewriteTime.Microseconds()),
		"AVG_COMMIT_TIME":                         float64ProtoValue(avgInt64(stats.SumCommitTime.Microseconds(), execCount)),
		"MAX_COMMIT_TIME":                         int64ProtoValue(stats.MaxCommitTime.Microseconds()),
		"AVG_GET_COMMIT_TS_TIME":                  float64ProtoValue(avgInt64(stats.SumGetCommitTsTime.Microseconds(), execCount)),
		"MAX_GET_COMMIT_TS_TIME":                  int64ProtoValue(stats.MaxGetCommitTsTime.Microseconds()),
		"AVG_COMMIT_BACKOFF_TIME":                 float64ProtoValue(avgInt64(stats.SumCommitBackoffTime, execCount)),
		"MAX_COMMIT_BACKOFF_TIME":                 int64ProtoValue(stats.MaxCommitBackoffTime),
		"AVG_RESOLVE_LOCK_TIME":                   float64ProtoValue(avgInt64(stats.SumResolveLockTime, execCount)),
		"MAX_RESOLVE_LOCK_TIME":                   int64ProtoValue(stats.MaxResolveLockTime),
		"AVG_LOCAL_LATCH_WAIT_TIME":               float64ProtoValue(avgInt64(stats.SumLocalLatchTime.Microseconds(), execCount)),
		"MAX_LOCAL_LATCH_WAIT_TIME":               int64ProtoValue(stats.MaxLocalLatchTime.Microseconds()),
		"AVG_WRITE_KEYS":                          float64ProtoValue(avgInt64(stats.SumWriteKeys, execCount)),
		"MAX_WRITE_KEYS":                          int64ProtoValue(stats.MaxWriteKeys),
		"AVG_WRITE_SIZE":                          float64ProtoValue(avgInt64(stats.SumWriteSizeBytes, execCount)),
		"MAX_WRITE_SIZE":                          int64ProtoValue(stats.MaxWriteSizeBytes),
		"AVG_PREWRITE_REGIONS":                    float64ProtoValue(avgInt64(stats.SumPrewriteRegionNum, execCount)),
		"MAX_PREWRITE_REGIONS":                    int64ProtoValue(int64(stats.MaxPrewriteRegionNum)),
		"AVG_TXN_RETRY":                           float64ProtoValue(avgInt64(stats.SumTxnRetry, execCount)),
		"MAX_TXN_RETRY":                           int64ProtoValue(int64(stats.MaxTxnRetry)),
		"SUM_EXEC_RETRY":                          int64ProtoValue(int64(stats.ExecRetryCount)),
		"SUM_EXEC_RETRY_TIME":                     int64ProtoValue(stats.ExecRetryTime.Microseconds()),
		"SUM_BACKOFF_TIMES":                       int64ProtoValue(stats.SumBackoffTimes),
		"BACKOFF_TYPES":                           stringProtoValue(formatBackoffTypes(stats.BackoffTypes)),
		"AVG_MEM":                                 float64ProtoValue(avgInt64(stats.SumMemBytes, execCount)),
		"MAX_MEM":                                 int64ProtoValue(stats.MaxMemBytes),
		"AVG_MEM_ARBITRATION":                     float64ProtoValue(avgFloat(stats.SumMemArbitration, execCount)),
		"MAX_MEM_ARBITRATION":                     float64ProtoValue(stats.MaxMemArbitration),
		"AVG_DISK":                                float64ProtoValue(avgInt64(stats.SumDiskBytes, execCount)),
		"MAX_DISK":                                int64ProtoValue(stats.MaxDiskBytes),
		"AVG_KV_TIME":                             float64ProtoValue(avgInt64(stats.SumKVTotal.Microseconds(), execCount)),
		"AVG_PD_TIME":                             float64ProtoValue(avgInt64(stats.SumPDTotal.Microseconds(), execCount)),
		"AVG_BACKOFF_TOTAL_TIME":                  float64ProtoValue(avgInt64(stats.SumBackoffTotal.Microseconds(), execCount)),
		"AVG_WRITE_SQL_RESP_TIME":                 float64ProtoValue(avgInt64(stats.SumWriteSQLRespTotal.Microseconds(), execCount)),
		"AVG_TIDB_CPU_TIME":                       float64ProtoValue(avgInt64(stats.SumTiDBCPU.Microseconds(), execCount)),
		"AVG_TIKV_CPU_TIME":                       float64ProtoValue(avgInt64(stats.SumTiKVCPU.Microseconds(), execCount)),
		"MAX_RESULT_ROWS":                         int64ProtoValue(stats.MaxResultRows),
		"MIN_RESULT_ROWS":                         int64ProtoValue(stats.MinResultRows),
		"AVG_RESULT_ROWS":                         float64ProtoValue(avgInt64(stats.SumResultRows, execCount)),
		"PREPARED":                                boolProtoValue(stats.Prepared),
		"AVG_AFFECTED_ROWS":                       float64ProtoValue(avgInt64(stats.SumAffectedRows, execCount)),
		"FIRST_SEEN":                              timestampProtoValue(stats.FirstSeen.UnixMilli()),
		"LAST_SEEN":                               timestampProtoValue(stats.LastSeen.UnixMilli()),
		"PLAN_IN_CACHE":                           boolProtoValue(stats.PlanInCache),
		"PLAN_CACHE_HITS":                         int64ProtoValue(stats.PlanCacheHits),
		"PLAN_IN_BINDING":                         boolProtoValue(stats.PlanInBinding),
		"QUERY_SAMPLE_TEXT":                       stringProtoValue(stats.SampleSQL),
		"PREV_SAMPLE_TEXT":                        stringProtoValue(stats.PrevSQL),
		"PLAN_DIGEST":                             stringProtoValue(stats.Key.PlanDigest),
		"PLAN":                                    stringProtoValue(stats.SamplePlan),
		"BINARY_PLAN":                             stringProtoValue(stats.SampleBinaryPlan),
		"BINDING_DIGEST":                          stringProtoValue(stats.BindingDigest),
		"BINDING_DIGEST_TEXT":                     stringProtoValue(stats.BindingSQL),
		"CHARSET":                                 stringProtoValue(stats.Charset),
		"COLLATION":                               stringProtoValue(stats.Collation),
		"PLAN_HINT":                               stringProtoValue(stats.PlanHint),
		"MAX_REQUEST_UNIT_READ":                   float64ProtoValue(stats.MaxRRU),
		"AVG_REQUEST_UNIT_READ":                   float64ProtoValue(avgFloat(stats.SumRRU, execCount)),
		"MAX_REQUEST_UNIT_WRITE":                  float64ProtoValue(stats.MaxWRU),
		"AVG_REQUEST_UNIT_WRITE":                  float64ProtoValue(avgFloat(stats.SumWRU, execCount)),
		"MAX_QUEUED_RC_TIME":                      int64ProtoValue(stats.MaxRUWaitDuration.Microseconds()),
		"AVG_QUEUED_RC_TIME":                      float64ProtoValue(avgInt64(stats.SumRUWaitDuration.Microseconds(), execCount)),
		"RESOURCE_GROUP":                          stringProtoValue(stats.Key.ResourceGroupName),
		"PLAN_CACHE_UNQUALIFIED":                  boolProtoValue(stats.PlanCacheUnqualifiedCount > 0),
		"PLAN_CACHE_UNQUALIFIED_LAST_REASON":      stringProtoValue(stats.PlanCacheUnqualifiedLastReason),
		"SUM_UNPACKED_BYTES_SENT_TIKV_TOTAL":      int64ProtoValue(stats.UnpackedBytesSentTiKVTotal),
		"SUM_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL":  int64ProtoValue(stats.UnpackedBytesReceivedTiKVTotal),
		"SUM_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE": int64ProtoValue(stats.UnpackedBytesSentTiKVCrossZone),
		"SUM_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE":    int64ProtoValue(stats.UnpackedBytesReceivedTiKVCrossZone),
		"SUM_UNPACKED_BYTES_SENT_TIFLASH_TOTAL":          int64ProtoValue(stats.UnpackedBytesSentTiFlashTotal),
		"SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL":      int64ProtoValue(stats.UnpackedBytesReceivedTiFlashTotal),
		"SUM_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE":     int64ProtoValue(stats.UnpackedBytesSentTiFlashCrossZone),
		"SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE": int64ProtoValue(stats.UnpackedBytesReceivedTiFlashCrossZone),
		"STORAGE_KV":  boolProtoValue(stats.StorageKV),
		"STORAGE_MPP": boolProtoValue(stats.StorageMPP),
	}

	values := make([]*stmtsummaryv3proto.Value, 0, len(statementSummaryColumns))
	for _, col := range statementSummaryColumns {
		if v, ok := row[col]; ok {
			values = append(values, v)
		} else {
			values = append(values, nullProtoValue())
		}
	}

	return &stmtsummaryv3proto.TableRow{Values: values}
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

	// Average metrics (computed from sum/exec_count)
	// Note: avg_latency is already in proto as AvgLatencyUs, so we don't add it here
	execCount := float64(stats.ExecCount)
	if execCount > 0 {
		// Latency averages (excluding avg_latency - already in proto)
		setDoubleMetric(em, "avg_parse_latency", float64(stats.SumParseLatency.Microseconds())/execCount)
		setDoubleMetric(em, "avg_compile_latency", float64(stats.SumCompileLatency.Microseconds())/execCount)
		setDoubleMetric(em, "avg_process_time", float64(stats.SumProcessTime.Microseconds())/execCount)
		setDoubleMetric(em, "avg_wait_time", float64(stats.SumWaitTime.Microseconds())/execCount)
		setDoubleMetric(em, "avg_backoff_time", float64(stats.SumBackoffTime.Microseconds())/execCount)

		// Resource averages
		setDoubleMetric(em, "avg_mem", float64(stats.SumMemBytes)/execCount)
		setDoubleMetric(em, "avg_disk", float64(stats.SumDiskBytes)/execCount)
		setDoubleMetric(em, "avg_tidb_cpu", float64(stats.SumTiDBCPU.Microseconds())/execCount)
		setDoubleMetric(em, "avg_tikv_cpu", float64(stats.SumTiKVCPU.Microseconds())/execCount)

		// Key averages
		setDoubleMetric(em, "avg_total_keys", float64(stats.SumTotalKeys)/execCount)
		setDoubleMetric(em, "avg_processed_keys", float64(stats.SumProcessedKeys)/execCount)

		// RocksDB averages
		setDoubleMetric(em, "avg_rocksdb_delete_skipped_count", float64(stats.SumRocksdbDeleteSkippedCount)/execCount)
		setDoubleMetric(em, "avg_rocksdb_key_skipped_count", float64(stats.SumRocksdbKeySkippedCount)/execCount)
		setDoubleMetric(em, "avg_rocksdb_block_cache_hit_count", float64(stats.SumRocksdbBlockCacheHitCount)/execCount)
		setDoubleMetric(em, "avg_rocksdb_block_read_count", float64(stats.SumRocksdbBlockReadCount)/execCount)
		setDoubleMetric(em, "avg_rocksdb_block_read_byte", float64(stats.SumRocksdbBlockReadByte)/execCount)

		// Transaction averages
		setDoubleMetric(em, "avg_prewrite_time", float64(stats.SumPrewriteTime.Microseconds())/execCount)
		setDoubleMetric(em, "avg_commit_time", float64(stats.SumCommitTime.Microseconds())/execCount)
		setDoubleMetric(em, "avg_local_latch_wait_time", float64(stats.SumLocalLatchTime.Microseconds())/execCount)
		setDoubleMetric(em, "avg_commit_backoff_time", float64(stats.SumCommitBackoffTime)/execCount)
		setDoubleMetric(em, "avg_resolve_lock_time", float64(stats.SumResolveLockTime)/execCount)
		setDoubleMetric(em, "avg_write_keys", float64(stats.SumWriteKeys)/execCount)
		setDoubleMetric(em, "avg_write_size", float64(stats.SumWriteSizeBytes)/execCount)
		setDoubleMetric(em, "avg_prewrite_regions", float64(stats.SumPrewriteRegionNum)/execCount)
		setDoubleMetric(em, "avg_txn_retry", float64(stats.SumTxnRetry)/execCount)

		// Result averages
		setDoubleMetric(em, "avg_result_rows", float64(stats.SumResultRows)/execCount)
		setDoubleMetric(em, "avg_affected_rows", float64(stats.SumAffectedRows)/execCount)

		// Other averages
		setDoubleMetric(em, "avg_kv_time", float64(stats.SumKVTotal.Microseconds())/execCount)
		setDoubleMetric(em, "avg_pd_time", float64(stats.SumPDTotal.Microseconds())/execCount)
		setDoubleMetric(em, "avg_backoff_total_time", float64(stats.SumBackoffTotal.Microseconds())/execCount)
		setDoubleMetric(em, "avg_write_sql_resp_time", float64(stats.SumWriteSQLRespTotal.Microseconds())/execCount)
		setDoubleMetric(em, "avg_mem_arbitration", stats.SumMemArbitration/execCount)
		setDoubleMetric(em, "avg_rru", stats.SumRRU/execCount)
		setDoubleMetric(em, "avg_wru", stats.SumWRU/execCount)
		setDoubleMetric(em, "avg_ru_wait_duration", float64(stats.SumRUWaitDuration.Microseconds())/execCount)
		setDoubleMetric(em, "avg_request_unit_read", stats.SumRRU/execCount)
		setDoubleMetric(em, "avg_request_unit_write", stats.SumWRU/execCount)
		setDoubleMetric(em, "avg_queued_rc_time", float64(stats.SumRUWaitDuration.Microseconds())/execCount)
	}

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
func (p *Pusher) doPush(batch *stmtsummaryv3proto.TableRowBatch) vectorsvc.PushResult {
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.Push.Timeout)
	defer cancel()

	start := time.Now()
	resp, err := p.client.PushTableRows(ctx, batch)
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
			batch := p.buildTableRowBatch(window)
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
