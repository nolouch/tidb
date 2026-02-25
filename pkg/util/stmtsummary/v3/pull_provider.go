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
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/vectorsvc"
	vectorsvcproto "github.com/pingcap/tidb/pkg/util/vectorsvc/proto/v1"
	"go.uber.org/zap"
)

const (
	statementsTableName = "STATEMENTS_SUMMARY"
)

// StmtSummaryProvider implements vectorsvc.TableDataProvider for STATEMENTS_SUMMARY.
type StmtSummaryProvider struct {
	aggregator *Aggregator
	clusterID  string
	instanceID string
	parser     *vectorsvc.QueryParser
}

// NewStmtSummaryProvider creates a new StmtSummaryProvider.
func NewStmtSummaryProvider(aggregator *Aggregator, clusterID, instanceID string) *StmtSummaryProvider {
	return &StmtSummaryProvider{
		aggregator: aggregator,
		clusterID:  clusterID,
		instanceID: instanceID,
		parser:     vectorsvc.NewQueryParser(),
	}
}

// TableName returns the table name this provider is responsible for.
func (p *StmtSummaryProvider) TableName() string {
	return statementsTableName
}

// QueryRows queries data and returns proto TableRow slices.
func (p *StmtSummaryProvider) QueryRows(_ context.Context, req *vectorsvcproto.TableQuery) ([]*vectorsvcproto.TableRow, error) {
	// Parse WHERE clause
	var whereExpr *vectorsvc.WhereExpression
	if req.Where != nil {
		expr, err := p.parser.ParseWhere(*req.Where)
		if err != nil {
			return nil, fmt.Errorf("invalid WHERE clause: %v", err)
		}
		whereExpr = expr
	}

	// Get data from aggregator
	stats := p.aggregator.GetCurrentStats()

	// Filter and convert to rows
	rows, _ := p.statsToRows(stats, whereExpr)
	return rows, nil
}

// Schema returns the table schema definition.
func (p *StmtSummaryProvider) Schema() *vectorsvcproto.TableSchema {
	return p.buildSchema()
}

// Metadata returns table metadata (used by ListTables).
func (p *StmtSummaryProvider) Metadata() *vectorsvcproto.TableMetadata {
	return &vectorsvcproto.TableMetadata{
		Name:              statementsTableName,
		Comment:           "Aggregated SQL statement execution statistics",
		SupportsTimeRange: true,
		SupportsPush:      true,
		EstimatedRows:     int64(len(p.aggregator.GetCurrentStats())),
	}
}

// statsToRows converts aggregated stats to table rows.
func (p *StmtSummaryProvider) statsToRows(stats []*StmtStats, where *vectorsvc.WhereExpression) ([]*vectorsvcproto.TableRow, int) {
	rows := make([]*vectorsvcproto.TableRow, 0, len(stats))

	for _, stat := range stats {
		rowMap := p.statToMap(stat)

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

		row := &vectorsvcproto.TableRow{
			Values: make([]*vectorsvcproto.Value, 0),
		}

		allCols := getAllStatementColumns()
		for _, col := range allCols {
			if val, ok := rowMap[col]; ok {
				row.Values = append(row.Values, vectorsvc.MapValueToProto(val))
			} else {
				row.Values = append(row.Values, &vectorsvcproto.Value{Kind: &vectorsvcproto.Value_NullVal{NullVal: vectorsvcproto.NullValue_NULL}})
			}
		}

		rows = append(rows, row)
	}

	return rows, len(stats)
}

// statToMap converts a StmtStats to a map for filtering.
func (p *StmtSummaryProvider) statToMap(stat *StmtStats) map[string]interface{} {
	p50, p95, p99 := GetPercentiles(stat.LatencyHistogram)
	execCount := max64(stat.ExecCount, 1)

	m := map[string]interface{}{
		"digest": stat.Key.Digest, "plan_digest": stat.Key.PlanDigest,
		"schema_name": stat.Key.SchemaName, "normalized_sql": stat.NormalizedSQL,
		"table_names": stat.TableNames, "stmt_type": stat.StmtType,
		"sample_sql": stat.SampleSQL, "sample_plan": stat.SamplePlan,
		"sample_binary_plan": stat.SampleBinaryPlan, "plan_hint": stat.PlanHint,
		"prev_sql": stat.PrevSQL, "index_names": formatIndexNames(stat.IndexNames),
		"charset": stat.Charset, "collation": stat.Collation,
		"binding_sql": stat.BindingSQL, "binding_digest": stat.BindingDigest,
		"exec_count": stat.ExecCount, "sum_errors": stat.SumErrors, "sum_warnings": stat.SumWarnings,
		"sum_latency_us": stat.SumLatency.Microseconds(), "max_latency_us": stat.MaxLatency.Microseconds(),
		"min_latency_us": stat.MinLatency.Microseconds(), "avg_latency_us": stat.SumLatency.Microseconds() / execCount,
		"p50_latency_us": p50, "p95_latency_us": p95, "p99_latency_us": p99,
		"sum_parse_latency_us": stat.SumParseLatency.Microseconds(), "max_parse_latency_us": stat.MaxParseLatency.Microseconds(),
		"avg_parse_latency_us": stat.SumParseLatency.Microseconds() / execCount,
		"sum_compile_latency_us": stat.SumCompileLatency.Microseconds(), "max_compile_latency_us": stat.MaxCompileLatency.Microseconds(),
		"avg_compile_latency_us": stat.SumCompileLatency.Microseconds() / execCount,
		"sum_mem_bytes": stat.SumMemBytes, "max_mem_bytes": stat.MaxMemBytes,
		"sum_disk_bytes": stat.SumDiskBytes, "max_disk_bytes": stat.MaxDiskBytes,
		"sum_tidb_cpu_us": stat.SumTiDBCPU.Microseconds(), "sum_tikv_cpu_us": stat.SumTiKVCPU.Microseconds(),
		"sum_cop_task_num": stat.SumNumCopTasks,
		"sum_cop_process_time_us": stat.SumCopProcessTime.Microseconds(), "max_cop_process_time_us": stat.MaxCopProcessTime.Microseconds(),
		"max_cop_process_address": stat.MaxCopProcessAddress,
		"sum_cop_wait_time_us": stat.SumCopWaitTime.Microseconds(), "max_cop_wait_time_us": stat.MaxCopWaitTime.Microseconds(),
		"max_cop_wait_address": stat.MaxCopWaitAddress,
		"sum_process_time_us": stat.SumProcessTime.Microseconds(), "max_process_time_us": stat.MaxProcessTime.Microseconds(),
		"avg_process_time_us": stat.SumProcessTime.Microseconds() / execCount,
		"sum_wait_time_us": stat.SumWaitTime.Microseconds(), "max_wait_time_us": stat.MaxWaitTime.Microseconds(),
		"avg_wait_time_us": stat.SumWaitTime.Microseconds() / execCount,
		"sum_backoff_time_us": stat.SumBackoffTime.Microseconds(), "max_backoff_time_us": stat.MaxBackoffTime.Microseconds(),
		"avg_backoff_time_us": stat.SumBackoffTime.Microseconds() / execCount,
		"sum_total_keys": stat.SumTotalKeys, "max_total_keys": stat.MaxTotalKeys,
		"avg_total_keys": stat.SumTotalKeys / execCount,
		"sum_processed_keys": stat.SumProcessedKeys, "max_processed_keys": stat.MaxProcessedKeys,
		"avg_processed_keys": stat.SumProcessedKeys / execCount,
		"sum_rocksdb_delete_skipped_count": stat.SumRocksdbDeleteSkippedCount, "max_rocksdb_delete_skipped_count": stat.MaxRocksdbDeleteSkippedCount,
		"sum_rocksdb_key_skipped_count": stat.SumRocksdbKeySkippedCount, "max_rocksdb_key_skipped_count": stat.MaxRocksdbKeySkippedCount,
		"sum_rocksdb_block_cache_hit_count": stat.SumRocksdbBlockCacheHitCount, "max_rocksdb_block_cache_hit_count": stat.MaxRocksdbBlockCacheHitCount,
		"sum_rocksdb_block_read_count": stat.SumRocksdbBlockReadCount, "max_rocksdb_block_read_count": stat.MaxRocksdbBlockReadCount,
		"sum_rocksdb_block_read_byte": stat.SumRocksdbBlockReadByte, "max_rocksdb_block_read_byte": stat.MaxRocksdbBlockReadByte,
		"commit_count": stat.CommitCount,
		"sum_get_commit_ts_time_us": stat.SumGetCommitTsTime.Microseconds(), "max_get_commit_ts_time_us": stat.MaxGetCommitTsTime.Microseconds(),
		"sum_prewrite_time_us": stat.SumPrewriteTime.Microseconds(), "max_prewrite_time_us": stat.MaxPrewriteTime.Microseconds(),
		"sum_commit_time_us": stat.SumCommitTime.Microseconds(), "max_commit_time_us": stat.MaxCommitTime.Microseconds(),
		"sum_local_latch_time_us": stat.SumLocalLatchTime.Microseconds(), "max_local_latch_time_us": stat.MaxLocalLatchTime.Microseconds(),
		"sum_commit_backoff_time": stat.SumCommitBackoffTime, "max_commit_backoff_time": stat.MaxCommitBackoffTime,
		"sum_resolve_lock_time": stat.SumResolveLockTime, "max_resolve_lock_time": stat.MaxResolveLockTime,
		"sum_write_keys": stat.SumWriteKeys, "max_write_keys": stat.MaxWriteKeys,
		"sum_write_size_bytes": stat.SumWriteSizeBytes, "max_write_size_bytes": stat.MaxWriteSizeBytes,
		"sum_prewrite_region_num": stat.SumPrewriteRegionNum, "max_prewrite_region_num": int64(stat.MaxPrewriteRegionNum),
		"sum_txn_retry": stat.SumTxnRetry, "max_txn_retry": int64(stat.MaxTxnRetry),
		"sum_backoff_times": stat.SumBackoffTimes, "backoff_types": formatBackoffTypes(stat.BackoffTypes),
		"sample_user":       formatAuthUsers(stat.AuthUsers),
		"sum_affected_rows": stat.SumAffectedRows, "sum_result_rows": stat.SumResultRows,
		"max_result_rows": stat.MaxResultRows, "min_result_rows": stat.MinResultRows,
		"avg_result_rows": stat.SumResultRows / execCount, "avg_affected_rows": stat.SumAffectedRows / execCount,
		"plan_in_cache": stat.PlanInCache, "plan_cache_hits": stat.PlanCacheHits,
		"plan_in_binding": stat.PlanInBinding,
		"plan_cache_unqualified_count": stat.PlanCacheUnqualifiedCount, "plan_cache_unqualified_last_reason": stat.PlanCacheUnqualifiedLastReason,
		"first_seen_ms": stat.FirstSeen.UnixMilli(), "last_seen_ms": stat.LastSeen.UnixMilli(),
		"is_internal": stat.IsInternal, "prepared": stat.Prepared,
		"keyspace_name": stat.KeyspaceName, "keyspace_id": stat.KeyspaceID,
		"resource_group": stat.Key.ResourceGroupName,
		"sum_kv_total_us": stat.SumKVTotal.Microseconds(), "sum_pd_total_us": stat.SumPDTotal.Microseconds(),
		"sum_backoff_total_us": stat.SumBackoffTotal.Microseconds(), "sum_write_sql_resp_total_us": stat.SumWriteSQLRespTotal.Microseconds(),
		"exec_retry_count": int64(stat.ExecRetryCount), "exec_retry_time_us": stat.ExecRetryTime.Microseconds(),
		"sum_mem_arbitration": stat.SumMemArbitration, "max_mem_arbitration": stat.MaxMemArbitration,
		"storage_kv": stat.StorageKV, "storage_mpp": stat.StorageMPP,
		"sum_rru": stat.SumRRU, "max_rru": stat.MaxRRU, "sum_wru": stat.SumWRU, "max_wru": stat.MaxWRU,
		"sum_ru_wait_duration_us": stat.SumRUWaitDuration.Microseconds(), "max_ru_wait_duration_us": stat.MaxRUWaitDuration.Microseconds(),
		"sum_unpacked_bytes_sent_tikv_total": stat.UnpackedBytesSentTiKVTotal, "sum_unpacked_bytes_received_tikv_total": stat.UnpackedBytesReceivedTiKVTotal,
		"sum_unpacked_bytes_sent_tikv_cross_zone": stat.UnpackedBytesSentTiKVCrossZone, "sum_unpacked_bytes_received_tikv_cross_zone": stat.UnpackedBytesReceivedTiKVCrossZone,
		"sum_unpacked_bytes_sent_tiflash_total": stat.UnpackedBytesSentTiFlashTotal, "sum_unpacked_bytes_received_tiflash_total": stat.UnpackedBytesReceivedTiFlashTotal,
		"sum_unpacked_bytes_sent_tiflash_cross_zone": stat.UnpackedBytesSentTiFlashCrossZone, "sum_unpacked_bytes_received_tiflash_cross_zone": stat.UnpackedBytesReceivedTiFlashCrossZone,
	}

	return m
}

// buildSchema builds the table schema.
func (p *StmtSummaryProvider) buildSchema() *vectorsvcproto.TableSchema {
	columns := make([]*vectorsvcproto.Column, 0)
	fieldTypes := getStatementColumnTypesV()

	allCols := getAllStatementColumns()
	for i, col := range allCols {
		dataType := vectorsvcproto.DataType_UNKNOWN
		if dt, ok := fieldTypes[col]; ok {
			dataType = dt
		}

		columns = append(columns, &vectorsvcproto.Column{
			Name:     col,
			Ordinal:  int32(i),
			Type:     dataType,
			Nullable: col != "digest",
		})
	}

	return &vectorsvcproto.TableSchema{
		TableName: statementsTableName,
		Columns:   columns,
	}
}

// getStatementColumnTypesV returns the data type mapping for all statement columns
// using vectorsvcproto types.
func getStatementColumnTypesV() map[string]vectorsvcproto.DataType {
	return map[string]vectorsvcproto.DataType{
		"digest": vectorsvcproto.DataType_STRING, "plan_digest": vectorsvcproto.DataType_STRING,
		"schema_name": vectorsvcproto.DataType_STRING, "normalized_sql": vectorsvcproto.DataType_STRING,
		"table_names": vectorsvcproto.DataType_STRING, "stmt_type": vectorsvcproto.DataType_STRING,
		"sample_sql": vectorsvcproto.DataType_STRING, "sample_plan": vectorsvcproto.DataType_STRING,
		"sample_binary_plan": vectorsvcproto.DataType_STRING, "plan_hint": vectorsvcproto.DataType_STRING,
		"prev_sql": vectorsvcproto.DataType_STRING, "index_names": vectorsvcproto.DataType_STRING,
		"charset": vectorsvcproto.DataType_STRING, "collation": vectorsvcproto.DataType_STRING,
		"binding_sql": vectorsvcproto.DataType_STRING, "binding_digest": vectorsvcproto.DataType_STRING,
		"exec_count": vectorsvcproto.DataType_INT64, "sum_errors": vectorsvcproto.DataType_INT64,
		"sum_warnings": vectorsvcproto.DataType_INT64,
		"sum_latency_us": vectorsvcproto.DataType_INT64, "max_latency_us": vectorsvcproto.DataType_INT64,
		"min_latency_us": vectorsvcproto.DataType_INT64, "avg_latency_us": vectorsvcproto.DataType_INT64,
		"p50_latency_us": vectorsvcproto.DataType_INT64, "p95_latency_us": vectorsvcproto.DataType_INT64,
		"p99_latency_us": vectorsvcproto.DataType_INT64,
		"sum_parse_latency_us": vectorsvcproto.DataType_INT64, "max_parse_latency_us": vectorsvcproto.DataType_INT64,
		"avg_parse_latency_us": vectorsvcproto.DataType_INT64,
		"sum_compile_latency_us": vectorsvcproto.DataType_INT64, "max_compile_latency_us": vectorsvcproto.DataType_INT64,
		"avg_compile_latency_us": vectorsvcproto.DataType_INT64,
		"sum_mem_bytes": vectorsvcproto.DataType_INT64, "max_mem_bytes": vectorsvcproto.DataType_INT64,
		"sum_disk_bytes": vectorsvcproto.DataType_INT64, "max_disk_bytes": vectorsvcproto.DataType_INT64,
		"sum_tidb_cpu_us": vectorsvcproto.DataType_INT64, "sum_tikv_cpu_us": vectorsvcproto.DataType_INT64,
		"sum_cop_task_num":        vectorsvcproto.DataType_INT64,
		"sum_cop_process_time_us": vectorsvcproto.DataType_INT64, "max_cop_process_time_us": vectorsvcproto.DataType_INT64,
		"max_cop_process_address": vectorsvcproto.DataType_STRING,
		"sum_cop_wait_time_us": vectorsvcproto.DataType_INT64, "max_cop_wait_time_us": vectorsvcproto.DataType_INT64,
		"max_cop_wait_address": vectorsvcproto.DataType_STRING,
		"sum_process_time_us": vectorsvcproto.DataType_INT64, "max_process_time_us": vectorsvcproto.DataType_INT64, "avg_process_time_us": vectorsvcproto.DataType_INT64,
		"sum_wait_time_us": vectorsvcproto.DataType_INT64, "max_wait_time_us": vectorsvcproto.DataType_INT64, "avg_wait_time_us": vectorsvcproto.DataType_INT64,
		"sum_backoff_time_us": vectorsvcproto.DataType_INT64, "max_backoff_time_us": vectorsvcproto.DataType_INT64, "avg_backoff_time_us": vectorsvcproto.DataType_INT64,
		"sum_total_keys": vectorsvcproto.DataType_INT64, "max_total_keys": vectorsvcproto.DataType_INT64, "avg_total_keys": vectorsvcproto.DataType_INT64,
		"sum_processed_keys": vectorsvcproto.DataType_INT64, "max_processed_keys": vectorsvcproto.DataType_INT64, "avg_processed_keys": vectorsvcproto.DataType_INT64,
		"sum_rocksdb_delete_skipped_count": vectorsvcproto.DataType_UINT64, "max_rocksdb_delete_skipped_count": vectorsvcproto.DataType_UINT64,
		"sum_rocksdb_key_skipped_count": vectorsvcproto.DataType_UINT64, "max_rocksdb_key_skipped_count": vectorsvcproto.DataType_UINT64,
		"sum_rocksdb_block_cache_hit_count": vectorsvcproto.DataType_UINT64, "max_rocksdb_block_cache_hit_count": vectorsvcproto.DataType_UINT64,
		"sum_rocksdb_block_read_count": vectorsvcproto.DataType_UINT64, "max_rocksdb_block_read_count": vectorsvcproto.DataType_UINT64,
		"sum_rocksdb_block_read_byte": vectorsvcproto.DataType_UINT64, "max_rocksdb_block_read_byte": vectorsvcproto.DataType_UINT64,
		"commit_count":              vectorsvcproto.DataType_INT64,
		"sum_get_commit_ts_time_us": vectorsvcproto.DataType_INT64, "max_get_commit_ts_time_us": vectorsvcproto.DataType_INT64,
		"sum_prewrite_time_us": vectorsvcproto.DataType_INT64, "max_prewrite_time_us": vectorsvcproto.DataType_INT64,
		"sum_commit_time_us": vectorsvcproto.DataType_INT64, "max_commit_time_us": vectorsvcproto.DataType_INT64,
		"sum_local_latch_time_us": vectorsvcproto.DataType_INT64, "max_local_latch_time_us": vectorsvcproto.DataType_INT64,
		"sum_commit_backoff_time": vectorsvcproto.DataType_INT64, "max_commit_backoff_time": vectorsvcproto.DataType_INT64,
		"sum_resolve_lock_time": vectorsvcproto.DataType_INT64, "max_resolve_lock_time": vectorsvcproto.DataType_INT64,
		"sum_write_keys": vectorsvcproto.DataType_INT64, "max_write_keys": vectorsvcproto.DataType_INT64,
		"sum_write_size_bytes": vectorsvcproto.DataType_INT64, "max_write_size_bytes": vectorsvcproto.DataType_INT64,
		"sum_prewrite_region_num": vectorsvcproto.DataType_INT64, "max_prewrite_region_num": vectorsvcproto.DataType_INT64,
		"sum_txn_retry": vectorsvcproto.DataType_INT64, "max_txn_retry": vectorsvcproto.DataType_INT64,
		"sum_backoff_times": vectorsvcproto.DataType_INT64, "backoff_types": vectorsvcproto.DataType_STRING,
		"sample_user":       vectorsvcproto.DataType_STRING,
		"sum_affected_rows": vectorsvcproto.DataType_INT64, "sum_result_rows": vectorsvcproto.DataType_INT64,
		"max_result_rows": vectorsvcproto.DataType_INT64, "min_result_rows": vectorsvcproto.DataType_INT64,
		"avg_result_rows": vectorsvcproto.DataType_INT64, "avg_affected_rows": vectorsvcproto.DataType_INT64,
		"plan_in_cache": vectorsvcproto.DataType_BOOL, "plan_cache_hits": vectorsvcproto.DataType_INT64,
		"plan_in_binding":                    vectorsvcproto.DataType_BOOL,
		"plan_cache_unqualified_count":       vectorsvcproto.DataType_INT64,
		"plan_cache_unqualified_last_reason": vectorsvcproto.DataType_STRING,
		"first_seen_ms": vectorsvcproto.DataType_TIMESTAMP, "last_seen_ms": vectorsvcproto.DataType_TIMESTAMP,
		"is_internal": vectorsvcproto.DataType_BOOL, "prepared": vectorsvcproto.DataType_BOOL,
		"keyspace_name": vectorsvcproto.DataType_STRING, "keyspace_id": vectorsvcproto.DataType_UINT64,
		"resource_group": vectorsvcproto.DataType_STRING,
		"sum_kv_total_us": vectorsvcproto.DataType_INT64, "sum_pd_total_us": vectorsvcproto.DataType_INT64,
		"sum_backoff_total_us": vectorsvcproto.DataType_INT64, "sum_write_sql_resp_total_us": vectorsvcproto.DataType_INT64,
		"exec_retry_count": vectorsvcproto.DataType_INT64, "exec_retry_time_us": vectorsvcproto.DataType_INT64,
		"sum_mem_arbitration": vectorsvcproto.DataType_FLOAT64, "max_mem_arbitration": vectorsvcproto.DataType_FLOAT64,
		"storage_kv": vectorsvcproto.DataType_BOOL, "storage_mpp": vectorsvcproto.DataType_BOOL,
		"sum_rru": vectorsvcproto.DataType_FLOAT64, "max_rru": vectorsvcproto.DataType_FLOAT64,
		"sum_wru": vectorsvcproto.DataType_FLOAT64, "max_wru": vectorsvcproto.DataType_FLOAT64,
		"sum_ru_wait_duration_us": vectorsvcproto.DataType_INT64, "max_ru_wait_duration_us": vectorsvcproto.DataType_INT64,
		"sum_unpacked_bytes_sent_tikv_total": vectorsvcproto.DataType_INT64, "sum_unpacked_bytes_received_tikv_total": vectorsvcproto.DataType_INT64,
		"sum_unpacked_bytes_sent_tikv_cross_zone": vectorsvcproto.DataType_INT64, "sum_unpacked_bytes_received_tikv_cross_zone": vectorsvcproto.DataType_INT64,
		"sum_unpacked_bytes_sent_tiflash_total": vectorsvcproto.DataType_INT64, "sum_unpacked_bytes_received_tiflash_total": vectorsvcproto.DataType_INT64,
		"sum_unpacked_bytes_sent_tiflash_cross_zone": vectorsvcproto.DataType_INT64, "sum_unpacked_bytes_received_tiflash_cross_zone": vectorsvcproto.DataType_INT64,
	}
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

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
