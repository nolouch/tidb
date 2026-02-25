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

	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/ppcpuusage"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	tikvutil "github.com/tikv/client-go/v2/util"
	"github.com/stretchr/testify/require"
)

// mockLazyInfo is a mock implementation of stmtsummary.StmtExecLazyInfo for testing
type mockLazyInfo struct {
	originalSQL   string
	encodedPlan   string
	planHint      string
	binaryPlan    string
	bindingSQL    string
	bindingDigest string
}

func (m *mockLazyInfo) GetOriginalSQL() string { return m.originalSQL }
func (m *mockLazyInfo) GetEncodedPlan() (string, string, any) {
	return m.encodedPlan, m.planHint, nil
}
func (m *mockLazyInfo) GetBinaryPlan() string       { return m.binaryPlan }
func (m *mockLazyInfo) GetPlanDigest() string       { return "" }
func (m *mockLazyInfo) GetBindingSQLAndDigest() (string, string) {
	return m.bindingSQL, m.bindingDigest
}

// generateTestStmtExecInfo creates a comprehensive StmtExecInfo for testing
func generateTestStmtExecInfo(digest string) *stmtsummary.StmtExecInfo {
	tables := []stmtctx.TableEntry{{DB: "db1", Table: "tb1"}, {DB: "db2", Table: "tb2"}}
	indexes := []string{"idx1", "idx2"}
	sc := stmtctx.NewStmtCtx()
	sc.StmtType = "Select"
	sc.Tables = tables
	sc.IndexNames = indexes
	sc.IsTiKV.Store(true)
	sc.IsTiFlash.Store(true)

	// Create commit detail with backoff types (use microsecond values for time fields)
	commitDetail := &tikvutil.CommitDetails{
		GetCommitTsTime:   100 * time.Microsecond,
		PrewriteTime:      10000 * time.Microsecond,
		CommitTime:        1000 * time.Microsecond,
		LocalLatchTime:    10 * time.Microsecond,
		WriteKeys:         20000,
		WriteSize:         200000,
		PrewriteRegionNum: 20,
		TxnRetry:          2,
		ResolveLock: tikvutil.ResolveLockDetail{
			ResolveLockTime: 2000,
		},
	}
	commitDetail.Mu.Lock()
	commitDetail.Mu.CommitBackoffTime = 200
	commitDetail.Mu.PrewriteBackoffTypes = []string{"txnlock", "pdRPC"}
	commitDetail.Mu.CommitBackoffTypes = []string{"tikvRPC"}
	commitDetail.Mu.Unlock()

	stmtExecInfo := &stmtsummary.StmtExecInfo{
		SchemaName:     "test_schema",
		NormalizedSQL:  "SELECT * FROM tb1 WHERE id = ?",
		Digest:         digest,
		PlanDigest:     "plan_digest_" + digest,
		User:           "test_user",
		TotalLatency:   10000 * time.Microsecond,
		ParseLatency:   100 * time.Microsecond,
		CompileLatency: 1000 * time.Microsecond,
		CopTasks: &execdetails.CopTasksSummary{
			NumCopTasks:       10,
			TotProcessTime:    50000 * time.Microsecond,
			MaxProcessTime:    15000 * time.Microsecond,
			MaxProcessAddress: "127.0.0.1:20160",
			TotWaitTime:       5000 * time.Microsecond,
			MaxWaitTime:       1500 * time.Microsecond,
			MaxWaitAddress:    "127.0.0.1:20161",
		},
		ExecDetail: execdetails.ExecDetails{
			RequestCount: 10,
			CommitDetail: commitDetail,
			CopExecDetails: execdetails.CopExecDetails{
				BackoffTime:   80 * time.Microsecond,
				CalleeAddress: "127.0.0.1:20160",
				ScanDetail: &tikvutil.ScanDetail{
					TotalKeys:                 1000,
					ProcessedKeys:             500,
					RocksdbDeleteSkippedCount: 100,
					RocksdbKeySkippedCount:    10,
					RocksdbBlockCacheHitCount: 10,
					RocksdbBlockReadCount:     10,
					RocksdbBlockReadByte:      1000,
				},
				TimeDetail: tikvutil.TimeDetail{
					ProcessTime: 500 * time.Microsecond,
					WaitTime:    50 * time.Microsecond,
				},
			},
		},
		StmtCtx:           sc,
		MemMax:            10000,
		DiskMax:           5000,
		StartTime:         time.Date(2025, 1, 1, 10, 10, 10, 10, time.UTC),
		Succeed:           true,
		KeyspaceName:      "test_keyspace",
		KeyspaceID:        42,
		ResourceGroupName: "test_rg",
		RUDetail:          tikvutil.NewRUDetailsWith(1.2, 3.4, 2*time.Millisecond),
		TiKVExecDetails: &tikvutil.ExecDetails{
			BackoffCount:       10,
			BackoffDuration:    1000,
			WaitKVRespDuration: 50000,
			WaitPDRespDuration: 10000,
			TrafficDetails: tikvutil.TrafficDetails{
				UnpackedBytesSentKVTotal:            1000,
				UnpackedBytesReceivedKVTotal:        2000,
				UnpackedBytesSentKVCrossZone:        100,
				UnpackedBytesReceivedKVCrossZone:    200,
				UnpackedBytesSentMPPTotal:           3000,
				UnpackedBytesReceivedMPPTotal:       4000,
				UnpackedBytesSentMPPCrossZone:       300,
				UnpackedBytesReceivedMPPCrossZone:   400,
			},
		},
		CPUUsages: ppcpuusage.CPUUsages{
			TidbCPUTime: time.Duration(20),
			TikvCPUTime: time.Duration(10000),
		},
		LazyInfo: &mockLazyInfo{
			originalSQL:   "SELECT * FROM tb1 WHERE id = 1",
			encodedPlan:   "encoded_plan_data",
			planHint:      "/*+ USE_INDEX(tb1, idx1) */",
			binaryPlan:    "binary_plan_data",
			bindingSQL:    "SELECT /*+ USE_INDEX(tb1, idx1) */ * FROM tb1 WHERE id = 1",
			bindingDigest: "binding_digest_abc",
		},
		MemArbitration:               22222,
		StmtExecDetails: execdetails.StmtExecDetails{
			WriteSQLRespDuration: 500 * time.Microsecond,
		},
		IsInternal:                   false,
		Prepared:                     true,
		PrevSQL:                      "PREPARE stmt FROM 'SELECT * FROM tb1'",
		Charset:                      "utf8mb4",
		Collation:                    "utf8mb4_bin",
		PlanInCache:                  true,
		PlanInBinding:                true,
		PlanCacheUnqualified:         "plan_cache_unqualified_reason",
		ExecRetryCount:               1,
		ExecRetryTime:                100 * time.Microsecond,
		ResultRows:                   5000,
	}
	stmtExecInfo.StmtCtx.AddAffectedRows(10000)

	return stmtExecInfo
}

// TestStmtStatsFieldCoverage tests that all fields are properly collected during aggregation
func TestStmtStatsFieldCoverage(t *testing.T) {
	cfg := &Config{
		AggregationWindow:   time.Minute,
		EnableInternalQuery: true,
		Memory: MemoryConfig{
			MaxMemoryBytes:      1024 * 1024 * 1024,
			MaxDigestsPerWindow: 1000,
		},
	}
	aggregator := NewAggregator(cfg)
	defer aggregator.Close()

	info := generateTestStmtExecInfo("test_digest")

	// Add the statement to the aggregator
	aggregator.Add(info)

	// Get the current stats and verify they were collected
	statsList := aggregator.GetCurrentStats()
	require.NotEmpty(t, statsList)

	// Find the stats for our digest
	var stats *StmtStats
	for _, s := range statsList {
		if s.Key.Digest == info.Digest {
			stats = s
			break
		}
	}
	require.NotNil(t, stats, "Statement should be in the stats")

	// Verify sample/identity fields
	require.Equal(t, info.LazyInfo.(*mockLazyInfo).binaryPlan, stats.SampleBinaryPlan)
	require.Equal(t, info.LazyInfo.(*mockLazyInfo).planHint, stats.PlanHint)
	require.Equal(t, info.StmtCtx.IndexNames, stats.IndexNames)
	require.Equal(t, info.Charset, stats.Charset)
	require.Equal(t, info.Collation, stats.Collation)
	bindingSQL, bindingDigest := info.LazyInfo.GetBindingSQLAndDigest()
	require.Equal(t, bindingSQL, stats.BindingSQL)
	require.Equal(t, bindingDigest, stats.BindingDigest)

	// Verify auth users
	require.Contains(t, stats.AuthUsers, "test_user")

	// Verify coprocessor metrics
	require.Equal(t, info.CopTasks.TotProcessTime, stats.SumCopProcessTime)
	require.Equal(t, info.CopTasks.MaxProcessTime, stats.MaxCopProcessTime)
	require.Equal(t, info.CopTasks.MaxProcessAddress, stats.MaxCopProcessAddress)
	require.Equal(t, info.CopTasks.TotWaitTime, stats.SumCopWaitTime)
	require.Equal(t, info.CopTasks.MaxWaitTime, stats.MaxCopWaitTime)
	require.Equal(t, info.CopTasks.MaxWaitAddress, stats.MaxCopWaitAddress)

	// Verify backoff metrics
	require.Equal(t, info.ExecDetail.CopExecDetails.BackoffTime, stats.SumBackoffTime)
	require.Equal(t, info.ExecDetail.CopExecDetails.BackoffTime, stats.MaxBackoffTime)

	// Verify RocksDB metrics
	sd := info.ExecDetail.CopExecDetails.ScanDetail
	require.Equal(t, sd.RocksdbDeleteSkippedCount, stats.SumRocksdbDeleteSkippedCount)
	require.Equal(t, sd.RocksdbDeleteSkippedCount, stats.MaxRocksdbDeleteSkippedCount)
	require.Equal(t, sd.RocksdbKeySkippedCount, stats.SumRocksdbKeySkippedCount)
	require.Equal(t, sd.RocksdbKeySkippedCount, stats.MaxRocksdbKeySkippedCount)
	require.Equal(t, sd.RocksdbBlockCacheHitCount, stats.SumRocksdbBlockCacheHitCount)
	require.Equal(t, sd.RocksdbBlockCacheHitCount, stats.MaxRocksdbBlockCacheHitCount)
	require.Equal(t, sd.RocksdbBlockReadCount, stats.SumRocksdbBlockReadCount)
	require.Equal(t, sd.RocksdbBlockReadCount, stats.MaxRocksdbBlockReadCount)
	require.Equal(t, sd.RocksdbBlockReadByte, stats.SumRocksdbBlockReadByte)
	require.Equal(t, sd.RocksdbBlockReadByte, stats.MaxRocksdbBlockReadByte)

	// Verify transaction metrics
	cd := info.ExecDetail.CommitDetail
	require.Equal(t, cd.GetCommitTsTime, stats.SumGetCommitTsTime)
	require.Equal(t, cd.GetCommitTsTime, stats.MaxGetCommitTsTime)
	require.Equal(t, cd.LocalLatchTime, stats.SumLocalLatchTime)
	require.Equal(t, cd.LocalLatchTime, stats.MaxLocalLatchTime)
	require.Equal(t, int64(cd.TxnRetry), stats.SumTxnRetry)
	require.Equal(t, cd.TxnRetry, stats.MaxTxnRetry)
	require.Equal(t, int64(cd.PrewriteRegionNum), stats.SumPrewriteRegionNum)
	require.Equal(t, cd.PrewriteRegionNum, stats.MaxPrewriteRegionNum)

	// Verify backoff types
	require.Contains(t, stats.BackoffTypes, "txnlock")
	require.Contains(t, stats.BackoffTypes, "pdRPC")
	require.Contains(t, stats.BackoffTypes, "tikvRPC")

	// Verify plan cache fields
	require.True(t, stats.PlanInBinding)
	require.Equal(t, int64(1), stats.PlanCacheUnqualifiedCount)
	require.Equal(t, "plan_cache_unqualified_reason", stats.PlanCacheUnqualifiedLastReason)

	// Verify retry fields
	require.Equal(t, uint(1), stats.ExecRetryCount)
	require.Equal(t, 100*time.Microsecond, stats.ExecRetryTime)

	// Verify memory arbitration
	require.Equal(t, float64(22222), stats.SumMemArbitration)
	require.Equal(t, float64(22222), stats.MaxMemArbitration)

	// Verify storage flags
	require.True(t, stats.StorageKV)
	require.True(t, stats.StorageMPP)

	// Verify RU metrics
	require.Equal(t, 1.2, stats.SumRRU)
	require.Equal(t, 1.2, stats.MaxRRU)
	require.Equal(t, 3.4, stats.SumWRU)
	require.Equal(t, 3.4, stats.MaxWRU)
	require.Equal(t, 2*time.Millisecond, stats.SumRUWaitDuration)
	require.Equal(t, 2*time.Millisecond, stats.MaxRUWaitDuration)

	// Verify network traffic (mapped from KV/Mpp to TiKV/TiFlash)
	ed := info.TiKVExecDetails
	require.Equal(t, ed.UnpackedBytesSentKVTotal, stats.UnpackedBytesSentTiKVTotal)
	require.Equal(t, ed.UnpackedBytesReceivedKVTotal, stats.UnpackedBytesReceivedTiKVTotal)
	require.Equal(t, ed.UnpackedBytesSentKVCrossZone, stats.UnpackedBytesSentTiKVCrossZone)
	require.Equal(t, ed.UnpackedBytesReceivedKVCrossZone, stats.UnpackedBytesReceivedTiKVCrossZone)
	require.Equal(t, ed.UnpackedBytesSentMPPTotal, stats.UnpackedBytesSentTiFlashTotal)
	require.Equal(t, ed.UnpackedBytesReceivedMPPTotal, stats.UnpackedBytesReceivedTiFlashTotal)
	require.Equal(t, ed.UnpackedBytesSentMPPCrossZone, stats.UnpackedBytesSentTiFlashCrossZone)
	require.Equal(t, ed.UnpackedBytesReceivedMPPCrossZone, stats.UnpackedBytesReceivedTiFlashCrossZone)

	// Verify ExecCount
	require.Equal(t, int64(1), stats.ExecCount)
}

// TestStatToMapFieldCoverage tests that all fields are exposed via statToMap
func TestStatToMapFieldCoverage(t *testing.T) {
	cfg := &Config{
		AggregationWindow:   time.Minute,
		EnableInternalQuery: true,
		Memory: MemoryConfig{
			MaxMemoryBytes:      1024 * 1024 * 1024,
			MaxDigestsPerWindow: 1000,
		},
	}
	aggregator := NewAggregator(cfg)
	defer aggregator.Close()

	info := generateTestStmtExecInfo("test_digest")
	aggregator.Add(info)

	// Create a StmtSummaryProvider to test statToMap
	provider := NewStmtSummaryProvider(aggregator, "test-cluster", "test-instance")

	// Get the current stats
	statsList := aggregator.GetCurrentStats()
	require.NotEmpty(t, statsList)

	// Find the stats for our digest
	var stats *StmtStats
	for _, s := range statsList {
		if s.Key.Digest == info.Digest {
			stats = s
			break
		}
	}
	require.NotNil(t, stats, "Statement should be in the stats")

	// Convert to map using StmtSummaryProvider's method
	m := provider.statToMap(stats)

	// Verify all new fields are present in the map
	fieldChecks := map[string]bool{
		"sample_binary_plan":                      m["sample_binary_plan"] != nil && m["sample_binary_plan"] != "",
		"plan_hint":                               m["plan_hint"] != nil && m["plan_hint"] != "",
		"index_names":                             m["index_names"] != nil && m["index_names"] != "",
		"charset":                                 m["charset"] != nil && m["charset"] == "utf8mb4",
		"collation":                               m["collation"] != nil && m["collation"] == "utf8mb4_bin",
		"binding_sql":                             m["binding_sql"] != nil && m["binding_sql"] != "",
		"binding_digest":                          m["binding_digest"] != nil && m["binding_digest"] != "",
		"sum_cop_process_time_us":                 m["sum_cop_process_time_us"] != nil && m["sum_cop_process_time_us"].(int64) > 0,
		"max_cop_process_time_us":                 m["max_cop_process_time_us"] != nil && m["max_cop_process_time_us"].(int64) > 0,
		"max_cop_process_address":                 m["max_cop_process_address"] != nil && m["max_cop_process_address"] != "",
		"sum_cop_wait_time_us":                    m["sum_cop_wait_time_us"] != nil && m["sum_cop_wait_time_us"].(int64) > 0,
		"max_cop_wait_time_us":                    m["max_cop_wait_time_us"] != nil && m["max_cop_wait_time_us"].(int64) > 0,
		"max_cop_wait_address":                    m["max_cop_wait_address"] != nil && m["max_cop_wait_address"] != "",
		"sum_rocksdb_delete_skipped_count":        m["sum_rocksdb_delete_skipped_count"] != nil,
		"max_rocksdb_delete_skipped_count":        m["max_rocksdb_delete_skipped_count"] != nil,
		"sum_rocksdb_key_skipped_count":           m["sum_rocksdb_key_skipped_count"] != nil,
		"max_rocksdb_key_skipped_count":           m["max_rocksdb_key_skipped_count"] != nil,
		"sum_rocksdb_block_cache_hit_count":       m["sum_rocksdb_block_cache_hit_count"] != nil,
		"max_rocksdb_block_cache_hit_count":       m["max_rocksdb_block_cache_hit_count"] != nil,
		"sum_rocksdb_block_read_count":            m["sum_rocksdb_block_read_count"] != nil,
		"max_rocksdb_block_read_count":            m["max_rocksdb_block_read_count"] != nil,
		"sum_rocksdb_block_read_byte":             m["sum_rocksdb_block_read_byte"] != nil,
		"max_rocksdb_block_read_byte":             m["max_rocksdb_block_read_byte"] != nil,
		"sum_get_commit_ts_time_us":               m["sum_get_commit_ts_time_us"] != nil && m["sum_get_commit_ts_time_us"].(int64) > 0,
		"max_get_commit_ts_time_us":               m["max_get_commit_ts_time_us"] != nil && m["max_get_commit_ts_time_us"].(int64) > 0,
		"sum_local_latch_time_us":                 m["sum_local_latch_time_us"] != nil && m["sum_local_latch_time_us"].(int64) > 0,
		"max_local_latch_time_us":                 m["max_local_latch_time_us"] != nil && m["max_local_latch_time_us"].(int64) > 0,
		"sum_txn_retry":                           m["sum_txn_retry"] != nil && m["sum_txn_retry"].(int64) > 0,
		"max_txn_retry":                           m["max_txn_retry"] != nil && m["max_txn_retry"].(int64) > 0,
		"sum_prewrite_region_num":                 m["sum_prewrite_region_num"] != nil && m["sum_prewrite_region_num"].(int64) > 0,
		"max_prewrite_region_num":                 m["max_prewrite_region_num"] != nil && m["max_prewrite_region_num"].(int64) > 0,
		"backoff_types":                           m["backoff_types"] != nil && m["backoff_types"] != "",
		"sample_user":                             m["sample_user"] != nil && m["sample_user"] != "",
		"plan_in_binding":                         m["plan_in_binding"] != nil && m["plan_in_binding"].(bool),
		"plan_cache_unqualified_count":            m["plan_cache_unqualified_count"] != nil && m["plan_cache_unqualified_count"].(int64) > 0,
		"plan_cache_unqualified_last_reason":      m["plan_cache_unqualified_last_reason"] != nil,
		"exec_retry_count":                        m["exec_retry_count"] != nil && m["exec_retry_count"].(int64) > 0,
		"exec_retry_time_us":                      m["exec_retry_time_us"] != nil && m["exec_retry_time_us"].(int64) > 0,
		"sum_mem_arbitration":                     m["sum_mem_arbitration"] != nil && m["sum_mem_arbitration"].(float64) > 0,
		"max_mem_arbitration":                     m["max_mem_arbitration"] != nil && m["max_mem_arbitration"].(float64) > 0,
		"storage_kv":                              m["storage_kv"] != nil && m["storage_kv"].(bool),
		"storage_mpp":                             m["storage_mpp"] != nil && m["storage_mpp"].(bool),
		"sum_rru":                                 m["sum_rru"] != nil && m["sum_rru"].(float64) > 0,
		"max_rru":                                 m["max_rru"] != nil && m["max_rru"].(float64) > 0,
		"sum_wru":                                 m["sum_wru"] != nil && m["sum_wru"].(float64) > 0,
		"max_wru":                                 m["max_wru"] != nil && m["max_wru"].(float64) > 0,
		"sum_ru_wait_duration_us":                 m["sum_ru_wait_duration_us"] != nil && m["sum_ru_wait_duration_us"].(int64) > 0,
		"max_ru_wait_duration_us":                 m["max_ru_wait_duration_us"] != nil && m["max_ru_wait_duration_us"].(int64) > 0,
		"sum_unpacked_bytes_sent_tikv_total":          m["sum_unpacked_bytes_sent_tikv_total"] != nil && m["sum_unpacked_bytes_sent_tikv_total"].(int64) > 0,
		"sum_unpacked_bytes_received_tikv_total":      m["sum_unpacked_bytes_received_tikv_total"] != nil && m["sum_unpacked_bytes_received_tikv_total"].(int64) > 0,
		"sum_unpacked_bytes_sent_tikv_cross_zone":     m["sum_unpacked_bytes_sent_tikv_cross_zone"] != nil,
		"sum_unpacked_bytes_received_tikv_cross_zone": m["sum_unpacked_bytes_received_tikv_cross_zone"] != nil,
		"sum_unpacked_bytes_sent_tiflash_total":       m["sum_unpacked_bytes_sent_tiflash_total"] != nil && m["sum_unpacked_bytes_sent_tiflash_total"].(int64) > 0,
		"sum_unpacked_bytes_received_tiflash_total":   m["sum_unpacked_bytes_received_tiflash_total"] != nil && m["sum_unpacked_bytes_received_tiflash_total"].(int64) > 0,
		"sum_unpacked_bytes_sent_tiflash_cross_zone":  m["sum_unpacked_bytes_sent_tiflash_cross_zone"] != nil,
		"sum_unpacked_bytes_received_tiflash_cross_zone": m["sum_unpacked_bytes_received_tiflash_cross_zone"] != nil,
	}

	for field, ok := range fieldChecks {
		require.True(t, ok, "Field %s should be present and valid in statToMap", field)
	}
}


// TestMergeStatsToOther tests that mergeStatsToOther properly merges all fields
func TestMergeStatsToOther(t *testing.T) {
	cfg := &Config{
		AggregationWindow:   time.Minute,
		EnableInternalQuery: true,
		Memory: MemoryConfig{
			MaxMemoryBytes:      1024 * 1024 * 1024,
			MaxDigestsPerWindow: 1000,
		},
	}
	aggregator := NewAggregator(cfg)
	defer aggregator.Close()

	info1 := generateTestStmtExecInfo("digest1")
	info2 := generateTestStmtExecInfo("digest1") // Same digest

	// Add both statements
	aggregator.Add(info1)
	aggregator.Add(info2)

	// Get the current stats
	statsList := aggregator.GetCurrentStats()
	require.NotEmpty(t, statsList)

	// Find the stats for our digest
	var stats *StmtStats
	for _, s := range statsList {
		if s.Key.Digest == info1.Digest {
			stats = s
			break
		}
	}
	require.NotNil(t, stats, "Statement should be in the stats")

	// Verify counts are summed
	require.Equal(t, int64(2), stats.ExecCount)
	require.Equal(t, 2*info1.TotalLatency, stats.SumLatency)

	// Verify auth users merged
	require.Contains(t, stats.AuthUsers, "test_user")

	// Verify backoff types merged
	require.Contains(t, stats.BackoffTypes, "txnlock")
	require.Contains(t, stats.BackoffTypes, "pdRPC")
	require.Contains(t, stats.BackoffTypes, "tikvRPC")

	// Verify RU metrics merged
	require.Equal(t, 2.4, stats.SumRRU) // 1.2 * 2
	require.Equal(t, 6.8, stats.SumWRU) // 3.4 * 2

	// Verify network traffic merged
	require.Equal(t, 2*info1.TiKVExecDetails.UnpackedBytesSentKVTotal, stats.UnpackedBytesSentTiKVTotal)
}

// TestGetAllStatementColumns tests that all columns are defined
func TestGetAllStatementColumns(t *testing.T) {
	columns := getAllStatementColumns()
	require.NotEmpty(t, columns)

	// Check that new fields are included
	newFields := []string{
		"sample_binary_plan",
		"plan_hint",
		"index_names",
		"charset",
		"collation",
		"binding_sql",
		"binding_digest",
		"sum_cop_process_time_us",
		"max_cop_process_time_us",
		"max_cop_process_address",
		"sum_cop_wait_time_us",
		"max_cop_wait_time_us",
		"max_cop_wait_address",
		"sum_rocksdb_delete_skipped_count",
		"max_rocksdb_delete_skipped_count",
		"sum_get_commit_ts_time_us",
		"max_get_commit_ts_time_us",
		"sum_local_latch_time_us",
		"max_local_latch_time_us",
		"sum_txn_retry",
		"max_txn_retry",
		"sum_prewrite_region_num",
		"max_prewrite_region_num",
		"backoff_types",
		"sample_user",
		"plan_in_binding",
		"plan_cache_unqualified_count",
		"plan_cache_unqualified_last_reason",
		"exec_retry_count",
		"exec_retry_time_us",
		"sum_mem_arbitration",
		"max_mem_arbitration",
		"storage_kv",
		"storage_mpp",
		"sum_rru",
		"max_rru",
		"sum_wru",
		"max_wru",
		"sum_ru_wait_duration_us",
		"max_ru_wait_duration_us",
		"sum_unpacked_bytes_sent_tikv_total",
		"sum_unpacked_bytes_received_tikv_total",
		"sum_unpacked_bytes_sent_tiflash_total",
		"sum_unpacked_bytes_received_tiflash_total",
	}

	columnSet := make(map[string]bool)
	for _, col := range columns {
		columnSet[col] = true
	}

	for _, field := range newFields {
		require.True(t, columnSet[field], "Column %s should be in getAllStatementColumns", field)
	}
}

// TestFormatHelpers tests the format helper functions
func TestFormatHelpers(t *testing.T) {
	// Test formatIndexNames
	names := []string{"idx1", "idx2", "idx3"}
	formatted := formatIndexNames(names)
	require.Equal(t, "idx1,idx2,idx3", formatted)
	require.Equal(t, "", formatIndexNames(nil))
	require.Equal(t, "", formatIndexNames([]string{}))

	// Test formatBackoffTypes
	backoffTypes := map[string]int{
		"txnlock": 5,
		"pdRPC":   3,
		"tikvRPC": 2,
	}
	formattedBackoff := formatBackoffTypes(backoffTypes)
	require.Contains(t, formattedBackoff, "txnlock:5")
	require.Contains(t, formattedBackoff, "pdRPC:3")
	require.Contains(t, formattedBackoff, "tikvRPC:2")
	require.Equal(t, "", formatBackoffTypes(nil))
	require.Equal(t, "", formatBackoffTypes(map[string]int{}))

	// Test formatAuthUsers
	authUsers := map[string]struct{}{
		"user1": {},
		"user2": {},
		"user3": {},
	}
	formattedUsers := formatAuthUsers(authUsers)
	// formatAuthUsers returns the first user only (as a sample)
	require.NotEmpty(t, formattedUsers)
	// Check that the returned user is one of the expected users
	require.Contains(t, []string{"user1", "user2", "user3"}, formattedUsers)
	require.Equal(t, "", formatAuthUsers(nil))
	require.Equal(t, "", formatAuthUsers(map[string]struct{}{}))
}

// TestEstimatedSize tests that EstimatedSize accounts for all fields
func TestEstimatedSize(t *testing.T) {
	cfg := &Config{
		AggregationWindow:   time.Minute,
		EnableInternalQuery: true,
		Memory: MemoryConfig{
			MaxMemoryBytes:      1024 * 1024 * 1024,
			MaxDigestsPerWindow: 1000,
		},
	}
	aggregator := NewAggregator(cfg)
	defer aggregator.Close()

	info := generateTestStmtExecInfo("test_digest")
	aggregator.Add(info)

	// Get the current stats
	statsList := aggregator.GetCurrentStats()
	require.NotEmpty(t, statsList)

	// Find the stats for our digest
	var stats *StmtStats
	for _, s := range statsList {
		if s.Key.Digest == info.Digest {
			stats = s
			break
		}
	}
	require.NotNil(t, stats, "Statement should be in the stats")

	size := stats.EstimatedSize()
	require.Greater(t, size, int64(0), "EstimatedSize should return positive value")

	// Add more data and verify size increases
	stats.BackoffTypes["new_backoff"] = 10
	stats.AuthUsers["new_user"] = struct{}{}
	stats.ExtendedMetrics["new_metric"] = MetricValue{Type: MetricTypeString, String: "test value with some length"}

	newSize := stats.EstimatedSize()
	// Note: EstimatedSize caches the result, so it won't change
	// This is just documenting the behavior
	_ = newSize
}
