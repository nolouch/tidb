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
		"query_sample_text": stat.SampleSQL, "plan": stat.SamplePlan,
		"sample_binary_plan": stat.SampleBinaryPlan, "plan_hint": stat.PlanHint,
		"prev_sample_text": stat.PrevSQL, "index_names": formatIndexNames(stat.IndexNames),
		"charset": stat.Charset, "collation": stat.Collation,
		"binding_sql": stat.BindingSQL, "binding_digest": stat.BindingDigest,
		"exec_count": stat.ExecCount, "sum_errors": stat.SumErrors, "sum_warnings": stat.SumWarnings,
		"sum_latency": stat.SumLatency.Microseconds(), "max_latency": stat.MaxLatency.Microseconds(),
		"min_latency": stat.MinLatency.Microseconds(), "avg_latency": stat.SumLatency.Microseconds() / execCount,
		"p50_latency": p50, "p95_latency": p95, "p99_latency": p99,
		"sum_parse_latency": stat.SumParseLatency.Microseconds(), "max_parse_latency": stat.MaxParseLatency.Microseconds(),
		"avg_parse_latency":   stat.SumParseLatency.Microseconds() / execCount,
		"sum_compile_latency": stat.SumCompileLatency.Microseconds(), "max_compile_latency": stat.MaxCompileLatency.Microseconds(),
		"avg_compile_latency": stat.SumCompileLatency.Microseconds() / execCount,
		"sum_mem":             stat.SumMemBytes, "max_mem": stat.MaxMemBytes,
		"avg_mem":  float64(stat.SumMemBytes) / float64(execCount),
		"sum_disk": stat.SumDiskBytes, "max_disk": stat.MaxDiskBytes,
		"avg_disk":     float64(stat.SumDiskBytes) / float64(execCount),
		"sum_tidb_cpu": stat.SumTiDBCPU.Microseconds(), "sum_tikv_cpu": stat.SumTiKVCPU.Microseconds(),
		"avg_tidb_cpu": stat.SumTiDBCPU.Microseconds() / execCount, "avg_tikv_cpu": stat.SumTiKVCPU.Microseconds() / execCount,
		"sum_cop_task_num":     stat.SumNumCopTasks,
		"sum_cop_process_time": stat.SumCopProcessTime.Microseconds(), "max_cop_process_time": stat.MaxCopProcessTime.Microseconds(),
		"max_cop_process_address": stat.MaxCopProcessAddress,
		"sum_cop_wait_time":       stat.SumCopWaitTime.Microseconds(), "max_cop_wait_time": stat.MaxCopWaitTime.Microseconds(),
		"max_cop_wait_address": stat.MaxCopWaitAddress,
		"sum_process_time":     stat.SumProcessTime.Microseconds(), "max_process_time": stat.MaxProcessTime.Microseconds(),
		"avg_process_time": stat.SumProcessTime.Microseconds() / execCount,
		"sum_wait_time":    stat.SumWaitTime.Microseconds(), "max_wait_time": stat.MaxWaitTime.Microseconds(),
		"avg_wait_time":    stat.SumWaitTime.Microseconds() / execCount,
		"sum_backoff_time": stat.SumBackoffTime.Microseconds(), "max_backoff_time": stat.MaxBackoffTime.Microseconds(),
		"avg_backoff_time": stat.SumBackoffTime.Microseconds() / execCount,
		"sum_total_keys":   stat.SumTotalKeys, "max_total_keys": stat.MaxTotalKeys,
		"avg_total_keys":     stat.SumTotalKeys / execCount,
		"sum_processed_keys": stat.SumProcessedKeys, "max_processed_keys": stat.MaxProcessedKeys,
		"avg_processed_keys":               stat.SumProcessedKeys / execCount,
		"sum_rocksdb_delete_skipped_count": stat.SumRocksdbDeleteSkippedCount, "max_rocksdb_delete_skipped_count": stat.MaxRocksdbDeleteSkippedCount,
		"avg_rocksdb_delete_skipped_count": float64(stat.SumRocksdbDeleteSkippedCount) / float64(execCount),
		"sum_rocksdb_key_skipped_count":    stat.SumRocksdbKeySkippedCount, "max_rocksdb_key_skipped_count": stat.MaxRocksdbKeySkippedCount,
		"avg_rocksdb_key_skipped_count":     float64(stat.SumRocksdbKeySkippedCount) / float64(execCount),
		"sum_rocksdb_block_cache_hit_count": stat.SumRocksdbBlockCacheHitCount, "max_rocksdb_block_cache_hit_count": stat.MaxRocksdbBlockCacheHitCount,
		"avg_rocksdb_block_cache_hit_count": float64(stat.SumRocksdbBlockCacheHitCount) / float64(execCount),
		"sum_rocksdb_block_read_count":      stat.SumRocksdbBlockReadCount, "max_rocksdb_block_read_count": stat.MaxRocksdbBlockReadCount,
		"avg_rocksdb_block_read_count": float64(stat.SumRocksdbBlockReadCount) / float64(execCount),
		"sum_rocksdb_block_read_byte":  stat.SumRocksdbBlockReadByte, "max_rocksdb_block_read_byte": stat.MaxRocksdbBlockReadByte,
		"avg_rocksdb_block_read_byte": float64(stat.SumRocksdbBlockReadByte) / float64(execCount),
		"commit_count":                stat.CommitCount,
		"sum_get_commit_ts_time":      stat.SumGetCommitTsTime.Microseconds(), "max_get_commit_ts_time": stat.MaxGetCommitTsTime.Microseconds(),
		"sum_prewrite_time": stat.SumPrewriteTime.Microseconds(), "max_prewrite_time": stat.MaxPrewriteTime.Microseconds(),
		"avg_prewrite_time": stat.SumPrewriteTime.Microseconds() / execCount,
		"sum_commit_time":   stat.SumCommitTime.Microseconds(), "max_commit_time": stat.MaxCommitTime.Microseconds(),
		"avg_commit_time":      stat.SumCommitTime.Microseconds() / execCount,
		"sum_local_latch_time": stat.SumLocalLatchTime.Microseconds(), "max_local_latch_wait_time": stat.MaxLocalLatchTime.Microseconds(),
		"avg_local_latch_wait_time": stat.SumLocalLatchTime.Microseconds() / execCount,
		"sum_commit_backoff_time":   stat.SumCommitBackoffTime, "max_commit_backoff_time": stat.MaxCommitBackoffTime,
		"avg_commit_backoff_time": float64(stat.SumCommitBackoffTime) / float64(execCount),
		"sum_resolve_lock_time":   stat.SumResolveLockTime, "max_resolve_lock_time": stat.MaxResolveLockTime,
		"avg_resolve_lock_time": float64(stat.SumResolveLockTime) / float64(execCount),
		"sum_write_keys":        stat.SumWriteKeys, "max_write_keys": stat.MaxWriteKeys,
		"avg_write_keys": float64(stat.SumWriteKeys) / float64(execCount),
		"sum_write_size": stat.SumWriteSizeBytes, "max_write_size": stat.MaxWriteSizeBytes,
		"avg_write_size":          float64(stat.SumWriteSizeBytes) / float64(execCount),
		"sum_prewrite_region_num": stat.SumPrewriteRegionNum, "max_prewrite_regions": int64(stat.MaxPrewriteRegionNum),
		"avg_prewrite_regions": float64(stat.SumPrewriteRegionNum) / float64(execCount),
		"sum_txn_retry":        stat.SumTxnRetry, "max_txn_retry": int64(stat.MaxTxnRetry),
		"avg_txn_retry":     float64(stat.SumTxnRetry) / float64(execCount),
		"sum_backoff_times": stat.SumBackoffTimes, "backoff_types": formatBackoffTypes(stat.BackoffTypes),
		"sample_user":       formatAuthUsers(stat.AuthUsers),
		"sum_affected_rows": stat.SumAffectedRows, "sum_result_rows": stat.SumResultRows,
		"max_result_rows": stat.MaxResultRows, "min_result_rows": stat.MinResultRows,
		"avg_result_rows": stat.SumResultRows / execCount, "avg_affected_rows": stat.SumAffectedRows / execCount,
		"plan_in_cache": stat.PlanInCache, "plan_cache_hits": stat.PlanCacheHits,
		"plan_in_binding":        stat.PlanInBinding,
		"plan_cache_unqualified": stat.PlanCacheUnqualifiedCount, "plan_cache_unqualified_last_reason": stat.PlanCacheUnqualifiedLastReason,
		"first_seen": stat.FirstSeen.UnixMilli(), "last_seen": stat.LastSeen.UnixMilli(),
		"is_internal": stat.IsInternal, "prepared": stat.Prepared,
		"keyspace_name": stat.KeyspaceName, "keyspace_id": stat.KeyspaceID,
		"resource_group": stat.Key.ResourceGroupName,
		"sum_kv_total":   stat.SumKVTotal.Microseconds(), "sum_pd_total": stat.SumPDTotal.Microseconds(),
		"avg_kv_time": stat.SumKVTotal.Microseconds() / execCount, "avg_pd_time": stat.SumPDTotal.Microseconds() / execCount,
		"sum_backoff_total": stat.SumBackoffTotal.Microseconds(), "sum_write_sql_resp_total": stat.SumWriteSQLRespTotal.Microseconds(),
		"avg_backoff_total_time": stat.SumBackoffTotal.Microseconds() / execCount, "avg_write_sql_resp_time": stat.SumWriteSQLRespTotal.Microseconds() / execCount,
		"exec_retry_count": int64(stat.ExecRetryCount), "exec_retry_time": stat.ExecRetryTime.Microseconds(),
		"sum_mem_arbitration": stat.SumMemArbitration, "max_mem_arbitration": stat.MaxMemArbitration,
		"avg_mem_arbitration": stat.SumMemArbitration / float64(execCount),
		"storage_kv":          stat.StorageKV, "storage_mpp": stat.StorageMPP,
		"sum_rru": stat.SumRRU, "max_rru": stat.MaxRRU, "avg_rru": stat.SumRRU / float64(execCount),
		"sum_wru": stat.SumWRU, "max_wru": stat.MaxWRU, "avg_wru": stat.SumWRU / float64(execCount),
		"sum_ru_wait_duration": stat.SumRUWaitDuration.Microseconds(), "max_ru_wait_duration": stat.MaxRUWaitDuration.Microseconds(),
		"avg_ru_wait_duration":  stat.SumRUWaitDuration.Microseconds() / execCount,
		"avg_request_unit_read": stat.SumRRU / float64(execCount), "max_request_unit_read": stat.MaxRRU,
		"avg_request_unit_write": stat.SumWRU / float64(execCount), "max_request_unit_write": stat.MaxWRU,
		"avg_queued_rc_time": stat.SumRUWaitDuration.Microseconds() / execCount, "max_queued_rc_time": stat.MaxRUWaitDuration.Microseconds(),
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
		"query_sample_text": vectorsvcproto.DataType_STRING, "plan": vectorsvcproto.DataType_STRING,
		"sample_binary_plan": vectorsvcproto.DataType_STRING, "plan_hint": vectorsvcproto.DataType_STRING,
		"prev_sample_text": vectorsvcproto.DataType_STRING, "index_names": vectorsvcproto.DataType_STRING,
		"charset": vectorsvcproto.DataType_STRING, "collation": vectorsvcproto.DataType_STRING,
		"binding_sql": vectorsvcproto.DataType_STRING, "binding_digest": vectorsvcproto.DataType_STRING,
		"exec_count": vectorsvcproto.DataType_INT64, "sum_errors": vectorsvcproto.DataType_INT64,
		"sum_warnings": vectorsvcproto.DataType_INT64,
		"sum_latency":  vectorsvcproto.DataType_INT64, "max_latency": vectorsvcproto.DataType_INT64,
		"min_latency": vectorsvcproto.DataType_INT64, "avg_latency": vectorsvcproto.DataType_INT64,
		"p50_latency": vectorsvcproto.DataType_INT64, "p95_latency": vectorsvcproto.DataType_INT64,
		"p99_latency":       vectorsvcproto.DataType_INT64,
		"sum_parse_latency": vectorsvcproto.DataType_INT64, "max_parse_latency": vectorsvcproto.DataType_INT64,
		"avg_parse_latency":   vectorsvcproto.DataType_INT64,
		"sum_compile_latency": vectorsvcproto.DataType_INT64, "max_compile_latency": vectorsvcproto.DataType_INT64,
		"avg_compile_latency": vectorsvcproto.DataType_INT64,
		"sum_mem":             vectorsvcproto.DataType_INT64, "max_mem": vectorsvcproto.DataType_INT64, "avg_mem": vectorsvcproto.DataType_FLOAT64,
		"sum_disk": vectorsvcproto.DataType_INT64, "max_disk": vectorsvcproto.DataType_INT64, "avg_disk": vectorsvcproto.DataType_FLOAT64,
		"sum_tidb_cpu": vectorsvcproto.DataType_INT64, "max_tidb_cpu": vectorsvcproto.DataType_INT64, "avg_tidb_cpu": vectorsvcproto.DataType_FLOAT64,
		"sum_tikv_cpu": vectorsvcproto.DataType_INT64, "max_tikv_cpu": vectorsvcproto.DataType_INT64, "avg_tikv_cpu": vectorsvcproto.DataType_FLOAT64,
		"sum_cop_task_num":     vectorsvcproto.DataType_INT64,
		"sum_cop_process_time": vectorsvcproto.DataType_INT64, "max_cop_process_time": vectorsvcproto.DataType_INT64,
		"max_cop_process_address": vectorsvcproto.DataType_STRING,
		"sum_cop_wait_time":       vectorsvcproto.DataType_INT64, "max_cop_wait_time": vectorsvcproto.DataType_INT64,
		"max_cop_wait_address": vectorsvcproto.DataType_STRING,
		"sum_process_time":     vectorsvcproto.DataType_INT64, "max_process_time": vectorsvcproto.DataType_INT64, "avg_process_time": vectorsvcproto.DataType_INT64,
		"sum_wait_time": vectorsvcproto.DataType_INT64, "max_wait_time": vectorsvcproto.DataType_INT64, "avg_wait_time": vectorsvcproto.DataType_INT64,
		"sum_backoff_time": vectorsvcproto.DataType_INT64, "max_backoff_time": vectorsvcproto.DataType_INT64, "avg_backoff_time": vectorsvcproto.DataType_INT64,
		"sum_total_keys": vectorsvcproto.DataType_INT64, "max_total_keys": vectorsvcproto.DataType_INT64, "avg_total_keys": vectorsvcproto.DataType_INT64,
		"sum_processed_keys": vectorsvcproto.DataType_INT64, "max_processed_keys": vectorsvcproto.DataType_INT64, "avg_processed_keys": vectorsvcproto.DataType_INT64,
		"sum_rocksdb_delete_skipped_count": vectorsvcproto.DataType_UINT64, "max_rocksdb_delete_skipped_count": vectorsvcproto.DataType_UINT64, "avg_rocksdb_delete_skipped_count": vectorsvcproto.DataType_FLOAT64,
		"sum_rocksdb_key_skipped_count": vectorsvcproto.DataType_UINT64, "max_rocksdb_key_skipped_count": vectorsvcproto.DataType_UINT64, "avg_rocksdb_key_skipped_count": vectorsvcproto.DataType_FLOAT64,
		"sum_rocksdb_block_cache_hit_count": vectorsvcproto.DataType_UINT64, "max_rocksdb_block_cache_hit_count": vectorsvcproto.DataType_UINT64, "avg_rocksdb_block_cache_hit_count": vectorsvcproto.DataType_FLOAT64,
		"sum_rocksdb_block_read_count": vectorsvcproto.DataType_UINT64, "max_rocksdb_block_read_count": vectorsvcproto.DataType_UINT64, "avg_rocksdb_block_read_count": vectorsvcproto.DataType_FLOAT64,
		"sum_rocksdb_block_read_byte": vectorsvcproto.DataType_UINT64, "max_rocksdb_block_read_byte": vectorsvcproto.DataType_UINT64, "avg_rocksdb_block_read_byte": vectorsvcproto.DataType_FLOAT64,
		"commit_count":           vectorsvcproto.DataType_INT64,
		"sum_get_commit_ts_time": vectorsvcproto.DataType_INT64, "max_get_commit_ts_time": vectorsvcproto.DataType_INT64,
		"sum_prewrite_time": vectorsvcproto.DataType_INT64, "max_prewrite_time": vectorsvcproto.DataType_INT64, "avg_prewrite_time": vectorsvcproto.DataType_FLOAT64,
		"sum_commit_time": vectorsvcproto.DataType_INT64, "max_commit_time": vectorsvcproto.DataType_INT64, "avg_commit_time": vectorsvcproto.DataType_FLOAT64,
		"sum_local_latch_time": vectorsvcproto.DataType_INT64, "max_local_latch_wait_time": vectorsvcproto.DataType_INT64, "avg_local_latch_wait_time": vectorsvcproto.DataType_FLOAT64,
		"sum_commit_backoff_time": vectorsvcproto.DataType_INT64, "max_commit_backoff_time": vectorsvcproto.DataType_INT64, "avg_commit_backoff_time": vectorsvcproto.DataType_FLOAT64,
		"sum_resolve_lock_time": vectorsvcproto.DataType_INT64, "max_resolve_lock_time": vectorsvcproto.DataType_INT64, "avg_resolve_lock_time": vectorsvcproto.DataType_FLOAT64,
		"sum_write_keys": vectorsvcproto.DataType_INT64, "max_write_keys": vectorsvcproto.DataType_INT64, "avg_write_keys": vectorsvcproto.DataType_FLOAT64,
		"sum_write_size": vectorsvcproto.DataType_INT64, "max_write_size": vectorsvcproto.DataType_INT64, "avg_write_size": vectorsvcproto.DataType_FLOAT64,
		"sum_prewrite_region_num": vectorsvcproto.DataType_INT64, "max_prewrite_regions": vectorsvcproto.DataType_INT64, "avg_prewrite_regions": vectorsvcproto.DataType_FLOAT64,
		"sum_txn_retry": vectorsvcproto.DataType_INT64, "max_txn_retry": vectorsvcproto.DataType_INT64, "avg_txn_retry": vectorsvcproto.DataType_FLOAT64,
		"sum_backoff_times": vectorsvcproto.DataType_INT64, "backoff_types": vectorsvcproto.DataType_STRING,
		"sample_user":       vectorsvcproto.DataType_STRING,
		"sum_affected_rows": vectorsvcproto.DataType_INT64, "sum_result_rows": vectorsvcproto.DataType_INT64,
		"max_result_rows": vectorsvcproto.DataType_INT64, "min_result_rows": vectorsvcproto.DataType_INT64,
		"avg_result_rows": vectorsvcproto.DataType_INT64, "avg_affected_rows": vectorsvcproto.DataType_INT64,
		"plan_in_cache": vectorsvcproto.DataType_BOOL, "plan_cache_hits": vectorsvcproto.DataType_INT64,
		"plan_in_binding":                    vectorsvcproto.DataType_BOOL,
		"plan_cache_unqualified":             vectorsvcproto.DataType_INT64,
		"plan_cache_unqualified_last_reason": vectorsvcproto.DataType_STRING,
		"first_seen":                         vectorsvcproto.DataType_TIMESTAMP, "last_seen": vectorsvcproto.DataType_TIMESTAMP,
		"is_internal": vectorsvcproto.DataType_BOOL, "prepared": vectorsvcproto.DataType_BOOL,
		"keyspace_name": vectorsvcproto.DataType_STRING, "keyspace_id": vectorsvcproto.DataType_UINT64,
		"resource_group": vectorsvcproto.DataType_STRING,
		"sum_kv_total":   vectorsvcproto.DataType_INT64, "sum_pd_total": vectorsvcproto.DataType_INT64,
		"sum_backoff_total": vectorsvcproto.DataType_INT64, "sum_write_sql_resp_total": vectorsvcproto.DataType_INT64,
		"exec_retry_count": vectorsvcproto.DataType_INT64, "exec_retry_time": vectorsvcproto.DataType_INT64,
		"sum_mem_arbitration": vectorsvcproto.DataType_FLOAT64, "max_mem_arbitration": vectorsvcproto.DataType_FLOAT64, "avg_mem_arbitration": vectorsvcproto.DataType_FLOAT64,
		"storage_kv": vectorsvcproto.DataType_BOOL, "storage_mpp": vectorsvcproto.DataType_BOOL,
		"sum_rru": vectorsvcproto.DataType_FLOAT64, "max_rru": vectorsvcproto.DataType_FLOAT64, "avg_rru": vectorsvcproto.DataType_FLOAT64,
		"sum_wru": vectorsvcproto.DataType_FLOAT64, "max_wru": vectorsvcproto.DataType_FLOAT64, "avg_wru": vectorsvcproto.DataType_FLOAT64,
		"sum_ru_wait_duration": vectorsvcproto.DataType_INT64, "max_ru_wait_duration": vectorsvcproto.DataType_INT64, "avg_ru_wait_duration": vectorsvcproto.DataType_FLOAT64,
		"max_request_unit_read": vectorsvcproto.DataType_FLOAT64, "avg_request_unit_read": vectorsvcproto.DataType_FLOAT64,
		"max_request_unit_write": vectorsvcproto.DataType_FLOAT64, "avg_request_unit_write": vectorsvcproto.DataType_FLOAT64,
		"max_queued_rc_time": vectorsvcproto.DataType_INT64, "avg_queued_rc_time": vectorsvcproto.DataType_FLOAT64,
		"avg_kv_time": vectorsvcproto.DataType_FLOAT64, "avg_pd_time": vectorsvcproto.DataType_FLOAT64,
		"avg_backoff_total_time": vectorsvcproto.DataType_FLOAT64, "avg_write_sql_resp_time": vectorsvcproto.DataType_FLOAT64,
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
		"query_sample_text", "plan", "sample_binary_plan", "plan_hint", "prev_sample_text",
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
		"sum_mem_bytes", "max_mem_bytes", "avg_mem", "sum_disk_bytes", "max_disk_bytes", "avg_disk",
		"sum_tidb_cpu_us", "avg_tidb_cpu", "sum_tikv_cpu_us", "avg_tikv_cpu",
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
		"sum_rocksdb_delete_skipped_count", "max_rocksdb_delete_skipped_count", "avg_rocksdb_delete_skipped_count",
		"sum_rocksdb_key_skipped_count", "max_rocksdb_key_skipped_count", "avg_rocksdb_key_skipped_count",
		"sum_rocksdb_block_cache_hit_count", "max_rocksdb_block_cache_hit_count", "avg_rocksdb_block_cache_hit_count",
		"sum_rocksdb_block_read_count", "max_rocksdb_block_read_count", "avg_rocksdb_block_read_count",
		"sum_rocksdb_block_read_byte", "max_rocksdb_block_read_byte", "avg_rocksdb_block_read_byte",
		// Transaction
		"commit_count",
		"sum_get_commit_ts_time_us", "max_get_commit_ts_time_us",
		"sum_prewrite_time_us", "max_prewrite_time_us", "avg_prewrite_time",
		"sum_commit_time_us", "max_commit_time_us", "avg_commit_time",
		"sum_local_latch_time_us", "max_local_latch_wait_time", "avg_local_latch_wait_time",
		"sum_commit_backoff_time", "max_commit_backoff_time", "avg_commit_backoff_time",
		"sum_resolve_lock_time", "max_resolve_lock_time", "avg_resolve_lock_time",
		"sum_write_keys", "max_write_keys", "avg_write_keys",
		"sum_write_size", "max_write_size", "avg_write_size",
		"sum_prewrite_region_num", "max_prewrite_regions", "avg_prewrite_regions",
		"sum_txn_retry", "max_txn_retry", "avg_txn_retry",
		"sum_backoff_times", "backoff_types",
		// Auth
		"sample_user",
		// Row stats
		"sum_affected_rows", "sum_result_rows", "max_result_rows", "min_result_rows",
		"avg_result_rows", "avg_affected_rows",
		// Plan cache
		"plan_in_cache", "plan_cache_hits", "plan_in_binding",
		"plan_cache_unqualified", "plan_cache_unqualified_last_reason",
		// Timestamps
		"first_seen", "last_seen",
		// Flags
		"is_internal", "prepared",
		// Multi-tenancy
		"keyspace_name", "keyspace_id", "resource_group",
		// Other
		"sum_kv_total_us", "avg_kv_time", "sum_pd_total_us", "avg_pd_time", "sum_backoff_total_us", "avg_backoff_total_time", "sum_write_sql_resp_total_us", "avg_write_sql_resp_time",
		"exec_retry_count", "exec_retry_time_us",
		"sum_mem_arbitration", "max_mem_arbitration", "avg_mem_arbitration",
		"storage_kv", "storage_mpp",
		// RU
		"sum_rru", "max_rru", "avg_rru", "sum_wru", "max_wru", "avg_wru",
		"sum_ru_wait_duration_us", "max_ru_wait_duration_us", "avg_ru_wait_duration",
		"max_request_unit_read", "avg_request_unit_read", "max_request_unit_write", "avg_request_unit_write",
		"max_queued_rc_time", "avg_queued_rc_time",
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
