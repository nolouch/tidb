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
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/util/logutil"
	stmtsummaryv3proto "github.com/pingcap/tidb/pkg/util/stmtsummary/v3/proto/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// StatementsTable is the name of the statements table.
	StatementsTable = "STATEMENTS_SUMMARY"
	// DefaultBatchSize is the default number of rows per streaming response.
	DefaultBatchSize = 100
)

// PullServer handles pull requests for statement data.
type PullServer struct {
	aggregator *Aggregator
	clusterID  string
	instanceID string

	// Query parser
	parser *QueryParser
}

// NewPullServer creates a new PullServer.
func NewPullServer(aggregator *Aggregator, clusterID, instanceID string) *PullServer {
	return &PullServer{
		aggregator: aggregator,
		clusterID:  clusterID,
		instanceID: instanceID,
		parser:     NewQueryParser(),
	}
}

// QueryTable implements the SystemTablePullServiceServer interface.
// It streams table rows in batches.
func (s *PullServer) QueryTable(req *stmtsummaryv3proto.TableQuery, stream grpc.ServerStreamingServer[stmtsummaryv3proto.TableQueryResponse]) error {
	logutil.BgLogger().Info("QueryTable called",
		zap.String("table", req.Table),
		zap.String("where", req.GetWhere()),
		zap.Int32("limit", req.GetLimit()),
		zap.Int32("offset", req.GetOffset()))

	// Only support STATEMENTS_SUMMARY table
	if req.Table != StatementsTable {
		return status.Errorf(codes.NotFound, "table '%s' not found", req.Table)
	}

	// Parse WHERE clause
	var whereExpr *WhereExpression
	if req.Where != nil {
		expr, err := s.parser.ParseWhere(*req.Where)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid WHERE clause: %v", err)
		}
		whereExpr = expr
	}

	// Get data from aggregator
	stats := s.aggregator.GetCurrentStats()

	// Filter and convert to rows
	rows, totalCount := s.statsToRows(stats, whereExpr)

	// Send schema in first response
	schema := s.buildSchema()
	if err := stream.Send(&stmtsummaryv3proto.TableQueryResponse{
		Schema:     schema,
		Rows:       nil,
		TotalCount: int64(totalCount),
		HasMore:    len(rows) > 0,
	}); err != nil {
		return err
	}

	// Send rows in batches
	for i := 0; i < len(rows); i += DefaultBatchSize {
		end := i + DefaultBatchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[i:end]
		nextCursor := ""
		hasMore := end < len(rows)
		if hasMore {
			// Generate cursor for next page
			nextCursor = s.encodeCursor(end)
		}

		if err := stream.Send(&stmtsummaryv3proto.TableQueryResponse{
			Rows:       batch,
			NextCursor: &nextCursor,
			HasMore:    hasMore,
			TotalCount: int64(totalCount),
		}); err != nil {
			return err
		}
	}

	logutil.BgLogger().Info("QueryTable completed",
		zap.String("table", req.Table),
		zap.Int("rows_sent", len(rows)),
		zap.Int64("total_count", int64(totalCount)))

	return nil
}

// ListTables lists available tables.
func (s *PullServer) ListTables(ctx context.Context, req *stmtsummaryv3proto.ListTablesRequest) (*stmtsummaryv3proto.ListTablesResponse, error) {
	logutil.BgLogger().Info("ListTables called",
		zap.String("pattern", safeDeref(req.Pattern)))

	tables := []*stmtsummaryv3proto.TableMetadata{
		{
			Name:              StatementsTable,
			Comment:           "Aggregated SQL statement execution statistics",
			SupportsTimeRange: true,
			SupportsPush:      true,
			EstimatedRows:     int64(len(s.aggregator.GetCurrentStats())),
		},
	}

	// Apply pattern filter if specified
	if req.Pattern != nil {
		tables = s.filterTablesByPattern(tables, *req.Pattern)
	}

	return &stmtsummaryv3proto.ListTablesResponse{
		Tables: tables,
	}, nil
}

// DescribeTable returns the schema of a table.
func (s *PullServer) DescribeTable(ctx context.Context, req *stmtsummaryv3proto.DescribeTableRequest) (*stmtsummaryv3proto.DescribeTableResponse, error) {
	logutil.BgLogger().Info("DescribeTable called",
		zap.String("table", req.Table))

	if req.Table != StatementsTable {
		return nil, status.Errorf(codes.NotFound, "table '%s' not found", req.Table)
	}

	return &stmtsummaryv3proto.DescribeTableResponse{
		Schema: s.buildSchema(),
	}, nil
}

// statsToRows converts aggregated stats to table rows.
func (s *PullServer) statsToRows(stats []*StmtStats, where *WhereExpression) ([]*stmtsummaryv3proto.TableRow, int) {
	rows := make([]*stmtsummaryv3proto.TableRow, 0, len(stats))

	for _, stat := range stats {
		// Convert stat to map
		rowMap := s.statToMap(stat)

		// Apply WHERE filter
		if where != nil {
			match, err := where.Evaluate(rowMap)
			if err != nil {
				logutil.BgLogger().Warn("error evaluating WHERE clause",
					zap.String("digest", stat.Key.Digest),
					zap.Error(err))
				continue
			}
			if !match {
				continue
			}
		}

		// Convert to TableRow with all columns in default order
		row := &stmtsummaryv3proto.TableRow{
			Values: make([]*stmtsummaryv3proto.Value, 0),
		}

		allCols := getAllStatementColumns()
		for _, col := range allCols {
			if val, ok := rowMap[col]; ok {
				row.Values = append(row.Values, s.mapValueToProto(val))
			} else {
				// Null value for missing column
				row.Values = append(row.Values, &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_NullVal{NullVal: stmtsummaryv3proto.NullValue_NULL}})
			}
		}

		rows = append(rows, row)
	}

	return rows, len(stats)
}

// statToMap converts a StmtStats to a map for filtering.
func (s *PullServer) statToMap(stat *StmtStats) map[string]interface{} {
	p50, p95, p99 := GetPercentiles(stat.LatencyHistogram)
	execCount := max64(stat.ExecCount, 1)

	m := map[string]interface{}{
		// Identity
		"digest":         stat.Key.Digest,
		"plan_digest":    stat.Key.PlanDigest,
		"schema_name":    stat.Key.SchemaName,
		"normalized_sql": stat.NormalizedSQL,
		"table_names":    stat.TableNames,
		"stmt_type":      stat.StmtType,

		// Sample data
		"sample_sql":         stat.SampleSQL,
		"sample_plan":        stat.SamplePlan,
		"sample_binary_plan": stat.SampleBinaryPlan,
		"plan_hint":          stat.PlanHint,
		"prev_sql":           stat.PrevSQL,
		"index_names":        formatIndexNames(stat.IndexNames),
		"charset":            stat.Charset,
		"collation":          stat.Collation,

		// Binding
		"binding_sql":    stat.BindingSQL,
		"binding_digest": stat.BindingDigest,

		// Execution stats
		"exec_count":   stat.ExecCount,
		"sum_errors":   stat.SumErrors,
		"sum_warnings": stat.SumWarnings,

		// Latency
		"sum_latency_us":       stat.SumLatency.Microseconds(),
		"max_latency_us":       stat.MaxLatency.Microseconds(),
		"min_latency_us":       stat.MinLatency.Microseconds(),
		"avg_latency_us":       stat.SumLatency.Microseconds() / execCount,
		"p50_latency_us":       p50,
		"p95_latency_us":       p95,
		"p99_latency_us":       p99,
		"sum_parse_latency_us": stat.SumParseLatency.Microseconds(),
		"max_parse_latency_us": stat.MaxParseLatency.Microseconds(),
		"avg_parse_latency_us": stat.SumParseLatency.Microseconds() / execCount,
		"sum_compile_latency_us": stat.SumCompileLatency.Microseconds(),
		"max_compile_latency_us": stat.MaxCompileLatency.Microseconds(),
		"avg_compile_latency_us": stat.SumCompileLatency.Microseconds() / execCount,

		// Resource usage
		"sum_mem_bytes":  stat.SumMemBytes,
		"max_mem_bytes":  stat.MaxMemBytes,
		"sum_disk_bytes": stat.SumDiskBytes,
		"max_disk_bytes": stat.MaxDiskBytes,
		"sum_tidb_cpu_us": stat.SumTiDBCPU.Microseconds(),
		"sum_tikv_cpu_us": stat.SumTiKVCPU.Microseconds(),

		// Coprocessor
		"sum_cop_task_num":        stat.SumNumCopTasks,
		"sum_cop_process_time_us": stat.SumCopProcessTime.Microseconds(),
		"max_cop_process_time_us": stat.MaxCopProcessTime.Microseconds(),
		"max_cop_process_address": stat.MaxCopProcessAddress,
		"sum_cop_wait_time_us":    stat.SumCopWaitTime.Microseconds(),
		"max_cop_wait_time_us":    stat.MaxCopWaitTime.Microseconds(),
		"max_cop_wait_address":    stat.MaxCopWaitAddress,

		// TiKV time
		"sum_process_time_us": stat.SumProcessTime.Microseconds(),
		"max_process_time_us": stat.MaxProcessTime.Microseconds(),
		"avg_process_time_us": stat.SumProcessTime.Microseconds() / execCount,
		"sum_wait_time_us":    stat.SumWaitTime.Microseconds(),
		"max_wait_time_us":    stat.MaxWaitTime.Microseconds(),
		"avg_wait_time_us":    stat.SumWaitTime.Microseconds() / execCount,
		"sum_backoff_time_us": stat.SumBackoffTime.Microseconds(),
		"max_backoff_time_us": stat.MaxBackoffTime.Microseconds(),
		"avg_backoff_time_us": stat.SumBackoffTime.Microseconds() / execCount,

		// Key scan
		"sum_total_keys":     stat.SumTotalKeys,
		"max_total_keys":     stat.MaxTotalKeys,
		"avg_total_keys":     stat.SumTotalKeys / execCount,
		"sum_processed_keys": stat.SumProcessedKeys,
		"max_processed_keys": stat.MaxProcessedKeys,
		"avg_processed_keys": stat.SumProcessedKeys / execCount,

		// RocksDB
		"sum_rocksdb_delete_skipped_count": stat.SumRocksdbDeleteSkippedCount,
		"max_rocksdb_delete_skipped_count": stat.MaxRocksdbDeleteSkippedCount,
		"sum_rocksdb_key_skipped_count":    stat.SumRocksdbKeySkippedCount,
		"max_rocksdb_key_skipped_count":    stat.MaxRocksdbKeySkippedCount,
		"sum_rocksdb_block_cache_hit_count": stat.SumRocksdbBlockCacheHitCount,
		"max_rocksdb_block_cache_hit_count": stat.MaxRocksdbBlockCacheHitCount,
		"sum_rocksdb_block_read_count":     stat.SumRocksdbBlockReadCount,
		"max_rocksdb_block_read_count":     stat.MaxRocksdbBlockReadCount,
		"sum_rocksdb_block_read_byte":      stat.SumRocksdbBlockReadByte,
		"max_rocksdb_block_read_byte":      stat.MaxRocksdbBlockReadByte,

		// Transaction
		"commit_count":              stat.CommitCount,
		"sum_get_commit_ts_time_us": stat.SumGetCommitTsTime.Microseconds(),
		"max_get_commit_ts_time_us": stat.MaxGetCommitTsTime.Microseconds(),
		"sum_prewrite_time_us":      stat.SumPrewriteTime.Microseconds(),
		"max_prewrite_time_us":      stat.MaxPrewriteTime.Microseconds(),
		"sum_commit_time_us":        stat.SumCommitTime.Microseconds(),
		"max_commit_time_us":        stat.MaxCommitTime.Microseconds(),
		"sum_local_latch_time_us":   stat.SumLocalLatchTime.Microseconds(),
		"max_local_latch_time_us":   stat.MaxLocalLatchTime.Microseconds(),
		"sum_commit_backoff_time":   stat.SumCommitBackoffTime,
		"max_commit_backoff_time":   stat.MaxCommitBackoffTime,
		"sum_resolve_lock_time":     stat.SumResolveLockTime,
		"max_resolve_lock_time":     stat.MaxResolveLockTime,
		"sum_write_keys":            stat.SumWriteKeys,
		"max_write_keys":            stat.MaxWriteKeys,
		"sum_write_size_bytes":      stat.SumWriteSizeBytes,
		"max_write_size_bytes":      stat.MaxWriteSizeBytes,
		"sum_prewrite_region_num":   stat.SumPrewriteRegionNum,
		"max_prewrite_region_num":   int64(stat.MaxPrewriteRegionNum),
		"sum_txn_retry":             stat.SumTxnRetry,
		"max_txn_retry":             int64(stat.MaxTxnRetry),
		"sum_backoff_times":         stat.SumBackoffTimes,
		"backoff_types":             formatBackoffTypes(stat.BackoffTypes),

		// Auth/User
		"sample_user": formatAuthUsers(stat.AuthUsers),

		// Row statistics
		"sum_affected_rows": stat.SumAffectedRows,
		"sum_result_rows":   stat.SumResultRows,
		"max_result_rows":   stat.MaxResultRows,
		"min_result_rows":   stat.MinResultRows,
		"avg_result_rows":   stat.SumResultRows / execCount,
		"avg_affected_rows": stat.SumAffectedRows / execCount,

		// Plan cache
		"plan_in_cache":                     stat.PlanInCache,
		"plan_cache_hits":                   stat.PlanCacheHits,
		"plan_in_binding":                   stat.PlanInBinding,
		"plan_cache_unqualified_count":      stat.PlanCacheUnqualifiedCount,
		"plan_cache_unqualified_last_reason": stat.PlanCacheUnqualifiedLastReason,

		// Timestamps
		"first_seen_ms": stat.FirstSeen.UnixMilli(),
		"last_seen_ms":  stat.LastSeen.UnixMilli(),

		// Flags
		"is_internal": stat.IsInternal,
		"prepared":    stat.Prepared,

		// Multi-tenancy
		"keyspace_name":  stat.KeyspaceName,
		"keyspace_id":    stat.KeyspaceID,
		"resource_group": stat.Key.ResourceGroupName,

		// Other
		"sum_kv_total_us":           stat.SumKVTotal.Microseconds(),
		"sum_pd_total_us":           stat.SumPDTotal.Microseconds(),
		"sum_backoff_total_us":      stat.SumBackoffTotal.Microseconds(),
		"sum_write_sql_resp_total_us": stat.SumWriteSQLRespTotal.Microseconds(),
		"exec_retry_count":          int64(stat.ExecRetryCount),
		"exec_retry_time_us":        stat.ExecRetryTime.Microseconds(),
		"sum_mem_arbitration":       stat.SumMemArbitration,
		"max_mem_arbitration":       stat.MaxMemArbitration,
		"storage_kv":                stat.StorageKV,
		"storage_mpp":               stat.StorageMPP,

		// RU
		"sum_rru":               stat.SumRRU,
		"max_rru":               stat.MaxRRU,
		"sum_wru":               stat.SumWRU,
		"max_wru":               stat.MaxWRU,
		"sum_ru_wait_duration_us": stat.SumRUWaitDuration.Microseconds(),
		"max_ru_wait_duration_us": stat.MaxRUWaitDuration.Microseconds(),

		// Network traffic
		"sum_unpacked_bytes_sent_tikv_total":            stat.UnpackedBytesSentTiKVTotal,
		"sum_unpacked_bytes_received_tikv_total":        stat.UnpackedBytesReceivedTiKVTotal,
		"sum_unpacked_bytes_sent_tikv_cross_zone":       stat.UnpackedBytesSentTiKVCrossZone,
		"sum_unpacked_bytes_received_tikv_cross_zone":   stat.UnpackedBytesReceivedTiKVCrossZone,
		"sum_unpacked_bytes_sent_tiflash_total":         stat.UnpackedBytesSentTiFlashTotal,
		"sum_unpacked_bytes_received_tiflash_total":     stat.UnpackedBytesReceivedTiFlashTotal,
		"sum_unpacked_bytes_sent_tiflash_cross_zone":    stat.UnpackedBytesSentTiFlashCrossZone,
		"sum_unpacked_bytes_received_tiflash_cross_zone": stat.UnpackedBytesReceivedTiFlashCrossZone,
	}

	return m
}

// mapValueToProto converts a map value to proto Value.
func (s *PullServer) mapValueToProto(value interface{}) *stmtsummaryv3proto.Value {
	switch v := value.(type) {
	case string:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_StringVal{StringVal: v}}
	case int:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Int64Val{Int64Val: int64(v)}}
	case int64:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Int64Val{Int64Val: v}}
	case uint:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Uint64Val{Uint64Val: uint64(v)}}
	case uint32:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Uint64Val{Uint64Val: uint64(v)}}
	case uint64:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Uint64Val{Uint64Val: v}}
	case float64:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Float64Val{Float64Val: v}}
	case bool:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_BoolVal{BoolVal: v}}
	default:
		// Fallback to string
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_StringVal{StringVal: fmt.Sprintf("%v", v)}}
	}
}

// buildSchema builds the table schema.
func (s *PullServer) buildSchema() *stmtsummaryv3proto.TableSchema {
	columns := make([]*stmtsummaryv3proto.Column, 0)
	fieldTypes := getStatementColumnTypes()

	allCols := getAllStatementColumns()
	for i, col := range allCols {
		dataType := stmtsummaryv3proto.DataType_UNKNOWN
		if dt, ok := fieldTypes[col]; ok {
			dataType = dt
		}

		columns = append(columns, &stmtsummaryv3proto.Column{
			Name:     col,
			Ordinal:  int32(i),
			Type:     dataType,
			Nullable: col != "digest", // digest is required
		})
	}

	return &stmtsummaryv3proto.TableSchema{
		TableName: StatementsTable,
		Columns:   columns,
	}
}

// getStatementColumnTypes returns the data type mapping for all statement columns.
func getStatementColumnTypes() map[string]stmtsummaryv3proto.DataType {
	return map[string]stmtsummaryv3proto.DataType{
		// Identity
		"digest": stmtsummaryv3proto.DataType_STRING, "plan_digest": stmtsummaryv3proto.DataType_STRING,
		"schema_name": stmtsummaryv3proto.DataType_STRING, "normalized_sql": stmtsummaryv3proto.DataType_STRING,
		"table_names": stmtsummaryv3proto.DataType_STRING, "stmt_type": stmtsummaryv3proto.DataType_STRING,
		// Sample
		"sample_sql": stmtsummaryv3proto.DataType_STRING, "sample_plan": stmtsummaryv3proto.DataType_STRING,
		"sample_binary_plan": stmtsummaryv3proto.DataType_STRING, "plan_hint": stmtsummaryv3proto.DataType_STRING,
		"prev_sql": stmtsummaryv3proto.DataType_STRING, "index_names": stmtsummaryv3proto.DataType_STRING,
		"charset": stmtsummaryv3proto.DataType_STRING, "collation": stmtsummaryv3proto.DataType_STRING,
		// Binding
		"binding_sql": stmtsummaryv3proto.DataType_STRING, "binding_digest": stmtsummaryv3proto.DataType_STRING,
		// Execution stats
		"exec_count": stmtsummaryv3proto.DataType_INT64, "sum_errors": stmtsummaryv3proto.DataType_INT64,
		"sum_warnings": stmtsummaryv3proto.DataType_INT64,
		// Latency
		"sum_latency_us": stmtsummaryv3proto.DataType_INT64, "max_latency_us": stmtsummaryv3proto.DataType_INT64,
		"min_latency_us": stmtsummaryv3proto.DataType_INT64, "avg_latency_us": stmtsummaryv3proto.DataType_INT64,
		"p50_latency_us": stmtsummaryv3proto.DataType_INT64, "p95_latency_us": stmtsummaryv3proto.DataType_INT64,
		"p99_latency_us": stmtsummaryv3proto.DataType_INT64,
		"sum_parse_latency_us": stmtsummaryv3proto.DataType_INT64, "max_parse_latency_us": stmtsummaryv3proto.DataType_INT64,
		"avg_parse_latency_us": stmtsummaryv3proto.DataType_INT64,
		"sum_compile_latency_us": stmtsummaryv3proto.DataType_INT64, "max_compile_latency_us": stmtsummaryv3proto.DataType_INT64,
		"avg_compile_latency_us": stmtsummaryv3proto.DataType_INT64,
		// Resources
		"sum_mem_bytes": stmtsummaryv3proto.DataType_INT64, "max_mem_bytes": stmtsummaryv3proto.DataType_INT64,
		"sum_disk_bytes": stmtsummaryv3proto.DataType_INT64, "max_disk_bytes": stmtsummaryv3proto.DataType_INT64,
		"sum_tidb_cpu_us": stmtsummaryv3proto.DataType_INT64, "sum_tikv_cpu_us": stmtsummaryv3proto.DataType_INT64,
		// Coprocessor
		"sum_cop_task_num": stmtsummaryv3proto.DataType_INT64,
		"sum_cop_process_time_us": stmtsummaryv3proto.DataType_INT64, "max_cop_process_time_us": stmtsummaryv3proto.DataType_INT64,
		"max_cop_process_address": stmtsummaryv3proto.DataType_STRING,
		"sum_cop_wait_time_us": stmtsummaryv3proto.DataType_INT64, "max_cop_wait_time_us": stmtsummaryv3proto.DataType_INT64,
		"max_cop_wait_address": stmtsummaryv3proto.DataType_STRING,
		// TiKV time
		"sum_process_time_us": stmtsummaryv3proto.DataType_INT64, "max_process_time_us": stmtsummaryv3proto.DataType_INT64, "avg_process_time_us": stmtsummaryv3proto.DataType_INT64,
		"sum_wait_time_us": stmtsummaryv3proto.DataType_INT64, "max_wait_time_us": stmtsummaryv3proto.DataType_INT64, "avg_wait_time_us": stmtsummaryv3proto.DataType_INT64,
		"sum_backoff_time_us": stmtsummaryv3proto.DataType_INT64, "max_backoff_time_us": stmtsummaryv3proto.DataType_INT64, "avg_backoff_time_us": stmtsummaryv3proto.DataType_INT64,
		// Key scan
		"sum_total_keys": stmtsummaryv3proto.DataType_INT64, "max_total_keys": stmtsummaryv3proto.DataType_INT64, "avg_total_keys": stmtsummaryv3proto.DataType_INT64,
		"sum_processed_keys": stmtsummaryv3proto.DataType_INT64, "max_processed_keys": stmtsummaryv3proto.DataType_INT64, "avg_processed_keys": stmtsummaryv3proto.DataType_INT64,
		// RocksDB
		"sum_rocksdb_delete_skipped_count": stmtsummaryv3proto.DataType_UINT64, "max_rocksdb_delete_skipped_count": stmtsummaryv3proto.DataType_UINT64,
		"sum_rocksdb_key_skipped_count": stmtsummaryv3proto.DataType_UINT64, "max_rocksdb_key_skipped_count": stmtsummaryv3proto.DataType_UINT64,
		"sum_rocksdb_block_cache_hit_count": stmtsummaryv3proto.DataType_UINT64, "max_rocksdb_block_cache_hit_count": stmtsummaryv3proto.DataType_UINT64,
		"sum_rocksdb_block_read_count": stmtsummaryv3proto.DataType_UINT64, "max_rocksdb_block_read_count": stmtsummaryv3proto.DataType_UINT64,
		"sum_rocksdb_block_read_byte": stmtsummaryv3proto.DataType_UINT64, "max_rocksdb_block_read_byte": stmtsummaryv3proto.DataType_UINT64,
		// Transaction
		"commit_count": stmtsummaryv3proto.DataType_INT64,
		"sum_get_commit_ts_time_us": stmtsummaryv3proto.DataType_INT64, "max_get_commit_ts_time_us": stmtsummaryv3proto.DataType_INT64,
		"sum_prewrite_time_us": stmtsummaryv3proto.DataType_INT64, "max_prewrite_time_us": stmtsummaryv3proto.DataType_INT64,
		"sum_commit_time_us": stmtsummaryv3proto.DataType_INT64, "max_commit_time_us": stmtsummaryv3proto.DataType_INT64,
		"sum_local_latch_time_us": stmtsummaryv3proto.DataType_INT64, "max_local_latch_time_us": stmtsummaryv3proto.DataType_INT64,
		"sum_commit_backoff_time": stmtsummaryv3proto.DataType_INT64, "max_commit_backoff_time": stmtsummaryv3proto.DataType_INT64,
		"sum_resolve_lock_time": stmtsummaryv3proto.DataType_INT64, "max_resolve_lock_time": stmtsummaryv3proto.DataType_INT64,
		"sum_write_keys": stmtsummaryv3proto.DataType_INT64, "max_write_keys": stmtsummaryv3proto.DataType_INT64,
		"sum_write_size_bytes": stmtsummaryv3proto.DataType_INT64, "max_write_size_bytes": stmtsummaryv3proto.DataType_INT64,
		"sum_prewrite_region_num": stmtsummaryv3proto.DataType_INT64, "max_prewrite_region_num": stmtsummaryv3proto.DataType_INT64,
		"sum_txn_retry": stmtsummaryv3proto.DataType_INT64, "max_txn_retry": stmtsummaryv3proto.DataType_INT64,
		"sum_backoff_times": stmtsummaryv3proto.DataType_INT64, "backoff_types": stmtsummaryv3proto.DataType_STRING,
		// Auth
		"sample_user": stmtsummaryv3proto.DataType_STRING,
		// Row stats
		"sum_affected_rows": stmtsummaryv3proto.DataType_INT64, "sum_result_rows": stmtsummaryv3proto.DataType_INT64,
		"max_result_rows": stmtsummaryv3proto.DataType_INT64, "min_result_rows": stmtsummaryv3proto.DataType_INT64,
		"avg_result_rows": stmtsummaryv3proto.DataType_INT64, "avg_affected_rows": stmtsummaryv3proto.DataType_INT64,
		// Plan cache
		"plan_in_cache": stmtsummaryv3proto.DataType_BOOL, "plan_cache_hits": stmtsummaryv3proto.DataType_INT64,
		"plan_in_binding": stmtsummaryv3proto.DataType_BOOL,
		"plan_cache_unqualified_count": stmtsummaryv3proto.DataType_INT64, "plan_cache_unqualified_last_reason": stmtsummaryv3proto.DataType_STRING,
		// Timestamps
		"first_seen_ms": stmtsummaryv3proto.DataType_TIMESTAMP, "last_seen_ms": stmtsummaryv3proto.DataType_TIMESTAMP,
		// Flags
		"is_internal": stmtsummaryv3proto.DataType_BOOL, "prepared": stmtsummaryv3proto.DataType_BOOL,
		// Multi-tenancy
		"keyspace_name": stmtsummaryv3proto.DataType_STRING, "keyspace_id": stmtsummaryv3proto.DataType_UINT64,
		"resource_group": stmtsummaryv3proto.DataType_STRING,
		// Other
		"sum_kv_total_us": stmtsummaryv3proto.DataType_INT64, "sum_pd_total_us": stmtsummaryv3proto.DataType_INT64,
		"sum_backoff_total_us": stmtsummaryv3proto.DataType_INT64, "sum_write_sql_resp_total_us": stmtsummaryv3proto.DataType_INT64,
		"exec_retry_count": stmtsummaryv3proto.DataType_INT64, "exec_retry_time_us": stmtsummaryv3proto.DataType_INT64,
		"sum_mem_arbitration": stmtsummaryv3proto.DataType_FLOAT64, "max_mem_arbitration": stmtsummaryv3proto.DataType_FLOAT64,
		"storage_kv": stmtsummaryv3proto.DataType_BOOL, "storage_mpp": stmtsummaryv3proto.DataType_BOOL,
		// RU
		"sum_rru": stmtsummaryv3proto.DataType_FLOAT64, "max_rru": stmtsummaryv3proto.DataType_FLOAT64,
		"sum_wru": stmtsummaryv3proto.DataType_FLOAT64, "max_wru": stmtsummaryv3proto.DataType_FLOAT64,
		"sum_ru_wait_duration_us": stmtsummaryv3proto.DataType_INT64, "max_ru_wait_duration_us": stmtsummaryv3proto.DataType_INT64,
		// Network
		"sum_unpacked_bytes_sent_tikv_total": stmtsummaryv3proto.DataType_INT64, "sum_unpacked_bytes_received_tikv_total": stmtsummaryv3proto.DataType_INT64,
		"sum_unpacked_bytes_sent_tikv_cross_zone": stmtsummaryv3proto.DataType_INT64, "sum_unpacked_bytes_received_tikv_cross_zone": stmtsummaryv3proto.DataType_INT64,
		"sum_unpacked_bytes_sent_tiflash_total": stmtsummaryv3proto.DataType_INT64, "sum_unpacked_bytes_received_tiflash_total": stmtsummaryv3proto.DataType_INT64,
		"sum_unpacked_bytes_sent_tiflash_cross_zone": stmtsummaryv3proto.DataType_INT64, "sum_unpacked_bytes_received_tiflash_cross_zone": stmtsummaryv3proto.DataType_INT64,
	}
}

// filterTablesByPattern filters tables by a LIKE pattern.
func (s *PullServer) filterTablesByPattern(tables []*stmtsummaryv3proto.TableMetadata, pattern string) []*stmtsummaryv3proto.TableMetadata {
	result := make([]*stmtsummaryv3proto.TableMetadata, 0)

	for _, table := range tables {
		matched, _ := compareLike(table.Name, pattern)
		if matched {
			result = append(result, table)
		}
	}

	return result
}

// encodeCursor encodes pagination information into a cursor string.
func (s *PullServer) encodeCursor(offset int) string {
	data := fmt.Sprintf("%d", offset)
	return base64.StdEncoding.EncodeToString([]byte(data))
}

// decodeCursor decodes a cursor string.
func (s *PullServer) decodeCursor(cursor string) (offset int, err error) {
	data, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}

	n, err := fmt.Sscanf(string(data), "%d", &offset)
	if err != nil || n != 1 {
		return 0, fmt.Errorf("invalid cursor format")
	}

	return offset, nil
}

// getAllStatementColumns returns all column names for the statements table.
func getAllStatementColumns() []string {
	return []string{
		// Identity
		"digest", "plan_digest", "schema_name", "normalized_sql", "table_names", "stmt_type",
		// Sample
		"sample_sql", "sample_plan", "sample_binary_plan", "plan_hint", "prev_sql",
		"index_names", "charset", "collation",
		// Binding
		"binding_sql", "binding_digest",
		// Execution stats
		"exec_count", "sum_errors", "sum_warnings",
		// Latency
		"sum_latency_us", "max_latency_us", "min_latency_us", "avg_latency_us",
		"p50_latency_us", "p95_latency_us", "p99_latency_us",
		"sum_parse_latency_us", "max_parse_latency_us", "avg_parse_latency_us",
		"sum_compile_latency_us", "max_compile_latency_us", "avg_compile_latency_us",
		// Resources
		"sum_mem_bytes", "max_mem_bytes", "sum_disk_bytes", "max_disk_bytes",
		"sum_tidb_cpu_us", "sum_tikv_cpu_us",
		// Coprocessor
		"sum_cop_task_num",
		"sum_cop_process_time_us", "max_cop_process_time_us", "max_cop_process_address",
		"sum_cop_wait_time_us", "max_cop_wait_time_us", "max_cop_wait_address",
		// TiKV time
		"sum_process_time_us", "max_process_time_us", "avg_process_time_us",
		"sum_wait_time_us", "max_wait_time_us", "avg_wait_time_us",
		"sum_backoff_time_us", "max_backoff_time_us", "avg_backoff_time_us",
		// Key scan
		"sum_total_keys", "max_total_keys", "avg_total_keys",
		"sum_processed_keys", "max_processed_keys", "avg_processed_keys",
		// RocksDB
		"sum_rocksdb_delete_skipped_count", "max_rocksdb_delete_skipped_count",
		"sum_rocksdb_key_skipped_count", "max_rocksdb_key_skipped_count",
		"sum_rocksdb_block_cache_hit_count", "max_rocksdb_block_cache_hit_count",
		"sum_rocksdb_block_read_count", "max_rocksdb_block_read_count",
		"sum_rocksdb_block_read_byte", "max_rocksdb_block_read_byte",
		// Transaction
		"commit_count",
		"sum_get_commit_ts_time_us", "max_get_commit_ts_time_us",
		"sum_prewrite_time_us", "max_prewrite_time_us",
		"sum_commit_time_us", "max_commit_time_us",
		"sum_local_latch_time_us", "max_local_latch_time_us",
		"sum_commit_backoff_time", "max_commit_backoff_time",
		"sum_resolve_lock_time", "max_resolve_lock_time",
		"sum_write_keys", "max_write_keys",
		"sum_write_size_bytes", "max_write_size_bytes",
		"sum_prewrite_region_num", "max_prewrite_region_num",
		"sum_txn_retry", "max_txn_retry",
		"sum_backoff_times", "backoff_types",
		// Auth
		"sample_user",
		// Row stats
		"sum_affected_rows", "sum_result_rows", "max_result_rows", "min_result_rows",
		"avg_result_rows", "avg_affected_rows",
		// Plan cache
		"plan_in_cache", "plan_cache_hits", "plan_in_binding",
		"plan_cache_unqualified_count", "plan_cache_unqualified_last_reason",
		// Timestamps
		"first_seen_ms", "last_seen_ms",
		// Flags
		"is_internal", "prepared",
		// Multi-tenancy
		"keyspace_name", "keyspace_id", "resource_group",
		// Other
		"sum_kv_total_us", "sum_pd_total_us", "sum_backoff_total_us", "sum_write_sql_resp_total_us",
		"exec_retry_count", "exec_retry_time_us",
		"sum_mem_arbitration", "max_mem_arbitration",
		"storage_kv", "storage_mpp",
		// RU
		"sum_rru", "max_rru", "sum_wru", "max_wru",
		"sum_ru_wait_duration_us", "max_ru_wait_duration_us",
		// Network
		"sum_unpacked_bytes_sent_tikv_total", "sum_unpacked_bytes_received_tikv_total",
		"sum_unpacked_bytes_sent_tikv_cross_zone", "sum_unpacked_bytes_received_tikv_cross_zone",
		"sum_unpacked_bytes_sent_tiflash_total", "sum_unpacked_bytes_received_tiflash_total",
		"sum_unpacked_bytes_sent_tiflash_cross_zone", "sum_unpacked_bytes_received_tiflash_cross_zone",
	}
}

func formatIndexNames(names []string) string {
	return strings.Join(names, ",")
}

func formatBackoffTypes(types map[string]int) string {
	if len(types) == 0 {
		return ""
	}
	parts := make([]string, 0, len(types))
	for k, v := range types {
		parts = append(parts, fmt.Sprintf("%s:%d", k, v))
	}
	return strings.Join(parts, ",")
}

func formatAuthUsers(users map[string]struct{}) string {
	if len(users) == 0 {
		return ""
	}
	for user := range users {
		return user // Return first user as sample
	}
	return ""
}

func safeDeref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
