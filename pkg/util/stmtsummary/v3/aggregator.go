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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	tikvutil "github.com/tikv/client-go/v2/util"
)

const (
	// histogramMinValue is the minimum value for latency histogram (1 microsecond).
	histogramMinValue = 1
	// histogramMaxValue is the maximum value for latency histogram (1 hour in microseconds).
	histogramMaxValue = 3600 * 1000 * 1000
	// histogramSignificantFigures is the number of significant figures for histogram.
	histogramSignificantFigures = 3
)

// Aggregator aggregates statement execution statistics within time windows.
// It uses HdrHistogram for accurate percentile calculation and supports
// memory protection through digest limits and memory budgets.
type Aggregator struct {
	cfg *Config

	// Current aggregation window
	window     *AggregationWindow
	windowLock sync.RWMutex

	// Histogram pool to reduce allocations
	histogramPool sync.Pool

	// Memory tracking
	currentMemory atomic.Int64

	// Flush callback when window rotates
	onFlush func(*AggregationWindow)

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed atomic.Bool
}

// NewAggregator creates a new Aggregator with the given configuration.
func NewAggregator(cfg *Config) *Aggregator {
	ctx, cancel := context.WithCancel(context.Background())
	a := &Aggregator{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
		histogramPool: sync.Pool{
			New: func() any {
				return hdrhistogram.New(histogramMinValue, histogramMaxValue, histogramSignificantFigures)
			},
		},
	}
	a.window = a.newWindow(time.Now())

	// Start rotation loop
	a.wg.Add(1)
	go a.rotationLoop()

	return a
}

// SetOnFlush sets the callback function called when a window is flushed.
func (a *Aggregator) SetOnFlush(fn func(*AggregationWindow)) {
	a.onFlush = fn
}

// Add records a statement execution into the current aggregation window.
func (a *Aggregator) Add(info *stmtsummary.StmtExecInfo) {
	if a.closed.Load() {
		return
	}

	// Skip internal queries if not enabled
	if info.IsInternal && !a.cfg.EnableInternalQuery {
		return
	}

	key := StmtKey{
		SchemaName:        info.SchemaName,
		Digest:            info.Digest,
		PlanDigest:        info.PlanDigest,
		ResourceGroupName: info.ResourceGroupName,
	}

	a.windowLock.Lock()
	defer a.windowLock.Unlock()

	// Check if we need to rotate window
	if time.Now().After(a.window.End) {
		a.rotateWindowLocked()
	}

	// Check if digest already exists
	stats, exists := a.window.Statements[key]
	if exists {
		a.mergeToStats(stats, info)
		return
	}

	// Check digest count limit
	if a.window.DigestCount >= a.cfg.Memory.MaxDigestsPerWindow {
		a.recordToOther(info)
		return
	}

	// Check memory limit
	estimatedSize := a.estimateInfoSize(info)
	if a.currentMemory.Load()+estimatedSize > a.cfg.Memory.MaxMemoryBytes {
		switch a.cfg.Memory.EvictionStrategy {
		case EvictionAggregateToOther:
			a.recordToOther(info)
			return
		case EvictionLowFrequency:
			a.evictLowFrequencyLocked(estimatedSize)
		case EvictionDropNew:
			return
		}
	}

	// Create new stats entry
	stats = a.newStatsFromInfo(key, info)
	a.window.Statements[key] = stats
	a.window.DigestCount++
	a.currentMemory.Add(stats.EstimatedSize())
}

// newWindow creates a new aggregation window.
func (a *Aggregator) newWindow(begin time.Time) *AggregationWindow {
	return &AggregationWindow{
		Begin:      begin,
		End:        begin.Add(a.cfg.AggregationWindow),
		Statements: make(map[StmtKey]*StmtStats),
	}
}

// newStatsFromInfo creates a new StmtStats from StmtExecInfo.
func (a *Aggregator) newStatsFromInfo(key StmtKey, info *stmtsummary.StmtExecInfo) *StmtStats {
	stats := NewStmtStats(key)

	// Identity fields
	stats.NormalizedSQL = info.NormalizedSQL
	stats.StmtType = info.StmtCtx.StmtType
	stats.TableNames = a.formatTableNames(info)

	// Sample data (captured once at first execution, following v1's newStmtSummaryStats)
	stats.SampleSQL = info.LazyInfo.GetOriginalSQL()
	samplePlan, planHint, _ := info.LazyInfo.GetEncodedPlan()
	if len(samplePlan) > stmtsummary.MaxEncodedPlanSizeInBytes {
		samplePlan = plancodec.PlanDiscardedEncoded
	}
	stats.SamplePlan = samplePlan
	stats.PlanHint = planHint
	binPlan := info.LazyInfo.GetBinaryPlan()
	if len(binPlan) > stmtsummary.MaxEncodedPlanSizeInBytes {
		binPlan = plancodec.BinaryPlanDiscardedEncoded
	}
	stats.SampleBinaryPlan = binPlan
	stats.PrevSQL = info.PrevSQL
	stats.IndexNames = info.StmtCtx.IndexNames
	stats.Charset = info.Charset
	stats.Collation = info.Collation

	// Binding
	bindingSQL, bindingDigest := info.LazyInfo.GetBindingSQLAndDigest()
	stats.BindingSQL = bindingSQL
	stats.BindingDigest = bindingDigest

	// Flags
	stats.IsInternal = info.IsInternal
	stats.Prepared = info.Prepared

	// Multi-tenancy
	stats.KeyspaceName = info.KeyspaceName
	stats.KeyspaceID = info.KeyspaceID

	// Initialize histogram
	h := a.getHistogram()
	_ = h.RecordValue(info.TotalLatency.Microseconds())
	encoded, _ := h.Encode(hdrhistogram.V2EncodingCookieBase)
	stats.LatencyHistogram = encoded
	a.putHistogram(h)

	// Merge first execution stats
	a.mergeToStats(stats, info)

	return stats
}

// mergeToStats merges execution info into existing stats.
// This follows v1's stmtSummaryStats.add() (statement_summary.go:711).
func (a *Aggregator) mergeToStats(stats *StmtStats, info *stmtsummary.StmtExecInfo) {
	// Auth users
	if len(info.User) > 0 {
		stats.AuthUsers[info.User] = struct{}{}
	}

	// Execution count
	stats.ExecCount++
	if !info.Succeed {
		stats.SumErrors++
	}
	stats.SumWarnings += int64(info.StmtCtx.WarningCount())

	// Latency
	latency := info.TotalLatency
	stats.SumLatency += latency
	if latency > stats.MaxLatency {
		stats.MaxLatency = latency
	}
	if latency < stats.MinLatency {
		stats.MinLatency = latency
	}

	// Parse/Compile latency
	stats.SumParseLatency += info.ParseLatency
	if info.ParseLatency > stats.MaxParseLatency {
		stats.MaxParseLatency = info.ParseLatency
	}
	stats.SumCompileLatency += info.CompileLatency
	if info.CompileLatency > stats.MaxCompileLatency {
		stats.MaxCompileLatency = info.CompileLatency
	}

	// Update histogram
	if len(stats.LatencyHistogram) > 0 {
		h := a.getHistogram()
		existingHist, err := hdrhistogram.Decode(stats.LatencyHistogram)
		if err == nil && existingHist != nil {
			h.Merge(existingHist)
		}
		_ = h.RecordValue(latency.Microseconds())
		encoded, _ := h.Encode(hdrhistogram.V2EncodingCookieBase)
		stats.LatencyHistogram = encoded
		a.putHistogram(h)
	}

	// Coprocessor
	if info.CopTasks != nil {
		stats.SumNumCopTasks += int64(info.CopTasks.NumCopTasks)
		stats.SumCopProcessTime += info.CopTasks.TotProcessTime
		if info.CopTasks.MaxProcessTime > stats.MaxCopProcessTime {
			stats.MaxCopProcessTime = info.CopTasks.MaxProcessTime
			stats.MaxCopProcessAddress = info.CopTasks.MaxProcessAddress
		}
		stats.SumCopWaitTime += info.CopTasks.TotWaitTime
		if info.CopTasks.MaxWaitTime > stats.MaxCopWaitTime {
			stats.MaxCopWaitTime = info.CopTasks.MaxWaitTime
			stats.MaxCopWaitAddress = info.CopTasks.MaxWaitAddress
		}
	}

	// TiKV time
	stats.SumProcessTime += info.ExecDetail.TimeDetail.ProcessTime
	if info.ExecDetail.TimeDetail.ProcessTime > stats.MaxProcessTime {
		stats.MaxProcessTime = info.ExecDetail.TimeDetail.ProcessTime
	}
	stats.SumWaitTime += info.ExecDetail.TimeDetail.WaitTime
	if info.ExecDetail.TimeDetail.WaitTime > stats.MaxWaitTime {
		stats.MaxWaitTime = info.ExecDetail.TimeDetail.WaitTime
	}
	stats.SumBackoffTime += info.ExecDetail.BackoffTime
	if info.ExecDetail.BackoffTime > stats.MaxBackoffTime {
		stats.MaxBackoffTime = info.ExecDetail.BackoffTime
	}

	// Key scan & RocksDB
	if info.ExecDetail.ScanDetail != nil {
		scan := info.ExecDetail.ScanDetail
		stats.SumTotalKeys += scan.TotalKeys
		if scan.TotalKeys > stats.MaxTotalKeys {
			stats.MaxTotalKeys = scan.TotalKeys
		}
		stats.SumProcessedKeys += scan.ProcessedKeys
		if scan.ProcessedKeys > stats.MaxProcessedKeys {
			stats.MaxProcessedKeys = scan.ProcessedKeys
		}
		stats.SumRocksdbDeleteSkippedCount += scan.RocksdbDeleteSkippedCount
		if scan.RocksdbDeleteSkippedCount > stats.MaxRocksdbDeleteSkippedCount {
			stats.MaxRocksdbDeleteSkippedCount = scan.RocksdbDeleteSkippedCount
		}
		stats.SumRocksdbKeySkippedCount += scan.RocksdbKeySkippedCount
		if scan.RocksdbKeySkippedCount > stats.MaxRocksdbKeySkippedCount {
			stats.MaxRocksdbKeySkippedCount = scan.RocksdbKeySkippedCount
		}
		stats.SumRocksdbBlockCacheHitCount += scan.RocksdbBlockCacheHitCount
		if scan.RocksdbBlockCacheHitCount > stats.MaxRocksdbBlockCacheHitCount {
			stats.MaxRocksdbBlockCacheHitCount = scan.RocksdbBlockCacheHitCount
		}
		stats.SumRocksdbBlockReadCount += scan.RocksdbBlockReadCount
		if scan.RocksdbBlockReadCount > stats.MaxRocksdbBlockReadCount {
			stats.MaxRocksdbBlockReadCount = scan.RocksdbBlockReadCount
		}
		stats.SumRocksdbBlockReadByte += scan.RocksdbBlockReadByte
		if scan.RocksdbBlockReadByte > stats.MaxRocksdbBlockReadByte {
			stats.MaxRocksdbBlockReadByte = scan.RocksdbBlockReadByte
		}
	}

	// Transaction
	if info.ExecDetail.CommitDetail != nil {
		commit := info.ExecDetail.CommitDetail
		stats.CommitCount++
		stats.SumPrewriteTime += commit.PrewriteTime
		if commit.PrewriteTime > stats.MaxPrewriteTime {
			stats.MaxPrewriteTime = commit.PrewriteTime
		}
		stats.SumCommitTime += commit.CommitTime
		if commit.CommitTime > stats.MaxCommitTime {
			stats.MaxCommitTime = commit.CommitTime
		}
		stats.SumGetCommitTsTime += commit.GetCommitTsTime
		if commit.GetCommitTsTime > stats.MaxGetCommitTsTime {
			stats.MaxGetCommitTsTime = commit.GetCommitTsTime
		}
		resolveLockTime := atomic.LoadInt64(&commit.ResolveLock.ResolveLockTime)
		stats.SumResolveLockTime += resolveLockTime
		if resolveLockTime > stats.MaxResolveLockTime {
			stats.MaxResolveLockTime = resolveLockTime
		}
		stats.SumLocalLatchTime += commit.LocalLatchTime
		if commit.LocalLatchTime > stats.MaxLocalLatchTime {
			stats.MaxLocalLatchTime = commit.LocalLatchTime
		}
		stats.SumWriteKeys += int64(commit.WriteKeys)
		if int64(commit.WriteKeys) > stats.MaxWriteKeys {
			stats.MaxWriteKeys = int64(commit.WriteKeys)
		}
		stats.SumWriteSizeBytes += int64(commit.WriteSize)
		if int64(commit.WriteSize) > stats.MaxWriteSizeBytes {
			stats.MaxWriteSizeBytes = int64(commit.WriteSize)
		}
		prewriteRegionNum := atomic.LoadInt32(&commit.PrewriteRegionNum)
		stats.SumPrewriteRegionNum += int64(prewriteRegionNum)
		if prewriteRegionNum > stats.MaxPrewriteRegionNum {
			stats.MaxPrewriteRegionNum = prewriteRegionNum
		}
		stats.SumTxnRetry += int64(commit.TxnRetry)
		if commit.TxnRetry > stats.MaxTxnRetry {
			stats.MaxTxnRetry = commit.TxnRetry
		}
		commit.Mu.Lock()
		commitBackoffTime := commit.Mu.CommitBackoffTime
		stats.SumCommitBackoffTime += commitBackoffTime
		if commitBackoffTime > stats.MaxCommitBackoffTime {
			stats.MaxCommitBackoffTime = commitBackoffTime
		}
		stats.SumBackoffTimes += int64(len(commit.Mu.PrewriteBackoffTypes))
		for _, backoffType := range commit.Mu.PrewriteBackoffTypes {
			stats.BackoffTypes[backoffType]++
		}
		stats.SumBackoffTimes += int64(len(commit.Mu.CommitBackoffTypes))
		for _, backoffType := range commit.Mu.CommitBackoffTypes {
			stats.BackoffTypes[backoffType]++
		}
		commit.Mu.Unlock()
	}

	// Plan cache
	if info.PlanInCache {
		stats.PlanInCache = true
		stats.PlanCacheHits++
	} else {
		stats.PlanInCache = false
	}
	if info.PlanCacheUnqualified != "" {
		stats.PlanCacheUnqualifiedCount++
		stats.PlanCacheUnqualifiedLastReason = info.PlanCacheUnqualified
	}

	// SPM
	if info.PlanInBinding {
		stats.PlanInBinding = true
	} else {
		stats.PlanInBinding = false
	}

	// Memory/Disk
	stats.SumAffectedRows += int64(info.StmtCtx.AffectedRows())
	stats.SumMemBytes += info.MemMax
	if info.MemMax > stats.MaxMemBytes {
		stats.MaxMemBytes = info.MemMax
	}
	stats.SumMemArbitration += info.MemArbitration
	if info.MemArbitration > stats.MaxMemArbitration {
		stats.MaxMemArbitration = info.MemArbitration
	}
	stats.SumDiskBytes += info.DiskMax
	if info.DiskMax > stats.MaxDiskBytes {
		stats.MaxDiskBytes = info.DiskMax
	}

	// Timestamps
	if info.StartTime.Before(stats.FirstSeen) {
		stats.FirstSeen = info.StartTime
	}
	if stats.LastSeen.Before(info.StartTime) {
		stats.LastSeen = info.StartTime
	}

	// Retry
	if info.ExecRetryCount > 0 {
		stats.ExecRetryCount += info.ExecRetryCount
		stats.ExecRetryTime += info.ExecRetryTime
	}

	// Result rows
	if info.ResultRows > 0 {
		stats.SumResultRows += info.ResultRows
		if info.ResultRows > stats.MaxResultRows {
			stats.MaxResultRows = info.ResultRows
		}
		if info.ResultRows < stats.MinResultRows {
			stats.MinResultRows = info.ResultRows
		}
	} else {
		stats.MinResultRows = 0
	}

	// KV/PD/Backoff/WriteSQLResp totals
	stats.SumKVTotal += time.Duration(atomic.LoadInt64(&info.TiKVExecDetails.WaitKVRespDuration))
	stats.SumPDTotal += time.Duration(atomic.LoadInt64(&info.TiKVExecDetails.WaitPDRespDuration))
	stats.SumBackoffTotal += time.Duration(atomic.LoadInt64(&info.TiKVExecDetails.BackoffDuration))
	stats.SumWriteSQLRespTotal += info.StmtExecDetails.WriteSQLRespDuration

	// CPU time
	stats.SumTiDBCPU += info.CPUUsages.TidbCPUTime
	stats.SumTiKVCPU += info.CPUUsages.TikvCPUTime

	// Network traffic
	addNetworkTraffic(stats, info.TiKVExecDetails)

	// Request-units
	addRUSummary(stats, info.RUDetail)

	// Storage flags
	stats.StorageKV = info.StmtCtx.IsTiKV.Load()
	stats.StorageMPP = info.StmtCtx.IsTiFlash.Load()

	// Extended metrics (from config)
	a.collectExtendedMetrics(stats, info)
}

// addRUSummary adds RU detail to stats, following v1's StmtRUSummary.Add().
func addRUSummary(stats *StmtStats, info *tikvutil.RUDetails) {
	if info != nil {
		rru := info.RRU()
		stats.SumRRU += rru
		if rru > stats.MaxRRU {
			stats.MaxRRU = rru
		}
		wru := info.WRU()
		stats.SumWRU += wru
		if wru > stats.MaxWRU {
			stats.MaxWRU = wru
		}
		ruWaitDur := info.RUWaitDuration()
		stats.SumRUWaitDuration += ruWaitDur
		if ruWaitDur > stats.MaxRUWaitDuration {
			stats.MaxRUWaitDuration = ruWaitDur
		}
	}
}

// addNetworkTraffic adds network traffic to stats, following v1's StmtNetworkTrafficSummary.Add().
func addNetworkTraffic(stats *StmtStats, info *tikvutil.ExecDetails) {
	if info != nil {
		stats.UnpackedBytesSentTiKVTotal += info.UnpackedBytesSentKVTotal
		stats.UnpackedBytesReceivedTiKVTotal += info.UnpackedBytesReceivedKVTotal
		stats.UnpackedBytesSentTiKVCrossZone += info.UnpackedBytesSentKVCrossZone
		stats.UnpackedBytesReceivedTiKVCrossZone += info.UnpackedBytesReceivedKVCrossZone
		stats.UnpackedBytesSentTiFlashTotal += info.UnpackedBytesSentMPPTotal
		stats.UnpackedBytesReceivedTiFlashTotal += info.UnpackedBytesReceivedMPPTotal
		stats.UnpackedBytesSentTiFlashCrossZone += info.UnpackedBytesSentMPPCrossZone
		stats.UnpackedBytesReceivedTiFlashCrossZone += info.UnpackedBytesReceivedMPPCrossZone
	}
}

// recordToOther aggregates overflow statements to the OTHER bucket.
func (a *Aggregator) recordToOther(info *stmtsummary.StmtExecInfo) {
	if a.window.OtherBucket == nil {
		key := StmtKey{
			Digest: OtherDigest,
		}
		a.window.OtherBucket = NewStmtStats(key)
		a.window.OtherBucket.NormalizedSQL = "Aggregated overflow statements"
	}
	a.mergeToStats(a.window.OtherBucket, info)
}

// evictLowFrequencyLocked evicts low-frequency statements to free memory.
func (a *Aggregator) evictLowFrequencyLocked(neededBytes int64) {
	type kv struct {
		key   StmtKey
		count int64
		size  int64
	}

	var sorted []kv
	for k, s := range a.window.Statements {
		sorted = append(sorted, kv{k, s.ExecCount, s.EstimatedSize()})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].count < sorted[j].count
	})

	freedBytes := int64(0)
	for _, item := range sorted {
		if freedBytes >= neededBytes {
			break
		}

		stats := a.window.Statements[item.key]
		// Move to OTHER bucket
		if a.window.OtherBucket == nil {
			key := StmtKey{Digest: OtherDigest}
			a.window.OtherBucket = NewStmtStats(key)
			a.window.OtherBucket.NormalizedSQL = "Aggregated overflow statements"
		}
		a.mergeStatsToOther(a.window.OtherBucket, stats)

		delete(a.window.Statements, item.key)
		freedBytes += item.size
		a.currentMemory.Add(-item.size)
		a.window.DigestCount--
	}
}

// mergeStatsToOther merges stats into the OTHER bucket.
func (a *Aggregator) mergeStatsToOther(other *StmtStats, stats *StmtStats) {
	// Auth users
	for user := range stats.AuthUsers {
		other.AuthUsers[user] = struct{}{}
	}

	other.ExecCount += stats.ExecCount
	other.SumErrors += stats.SumErrors
	other.SumWarnings += stats.SumWarnings

	// Latency
	other.SumLatency += stats.SumLatency
	if stats.MaxLatency > other.MaxLatency {
		other.MaxLatency = stats.MaxLatency
	}
	if stats.MinLatency < other.MinLatency {
		other.MinLatency = stats.MinLatency
	}
	other.SumParseLatency += stats.SumParseLatency
	if stats.MaxParseLatency > other.MaxParseLatency {
		other.MaxParseLatency = stats.MaxParseLatency
	}
	other.SumCompileLatency += stats.SumCompileLatency
	if stats.MaxCompileLatency > other.MaxCompileLatency {
		other.MaxCompileLatency = stats.MaxCompileLatency
	}

	// Coprocessor
	other.SumNumCopTasks += stats.SumNumCopTasks
	other.SumCopProcessTime += stats.SumCopProcessTime
	if stats.MaxCopProcessTime > other.MaxCopProcessTime {
		other.MaxCopProcessTime = stats.MaxCopProcessTime
		other.MaxCopProcessAddress = stats.MaxCopProcessAddress
	}
	other.SumCopWaitTime += stats.SumCopWaitTime
	if stats.MaxCopWaitTime > other.MaxCopWaitTime {
		other.MaxCopWaitTime = stats.MaxCopWaitTime
		other.MaxCopWaitAddress = stats.MaxCopWaitAddress
	}

	// TiKV time
	other.SumProcessTime += stats.SumProcessTime
	if stats.MaxProcessTime > other.MaxProcessTime {
		other.MaxProcessTime = stats.MaxProcessTime
	}
	other.SumWaitTime += stats.SumWaitTime
	if stats.MaxWaitTime > other.MaxWaitTime {
		other.MaxWaitTime = stats.MaxWaitTime
	}
	other.SumBackoffTime += stats.SumBackoffTime
	if stats.MaxBackoffTime > other.MaxBackoffTime {
		other.MaxBackoffTime = stats.MaxBackoffTime
	}

	// Key scan
	other.SumTotalKeys += stats.SumTotalKeys
	if stats.MaxTotalKeys > other.MaxTotalKeys {
		other.MaxTotalKeys = stats.MaxTotalKeys
	}
	other.SumProcessedKeys += stats.SumProcessedKeys
	if stats.MaxProcessedKeys > other.MaxProcessedKeys {
		other.MaxProcessedKeys = stats.MaxProcessedKeys
	}

	// RocksDB
	other.SumRocksdbDeleteSkippedCount += stats.SumRocksdbDeleteSkippedCount
	if stats.MaxRocksdbDeleteSkippedCount > other.MaxRocksdbDeleteSkippedCount {
		other.MaxRocksdbDeleteSkippedCount = stats.MaxRocksdbDeleteSkippedCount
	}
	other.SumRocksdbKeySkippedCount += stats.SumRocksdbKeySkippedCount
	if stats.MaxRocksdbKeySkippedCount > other.MaxRocksdbKeySkippedCount {
		other.MaxRocksdbKeySkippedCount = stats.MaxRocksdbKeySkippedCount
	}
	other.SumRocksdbBlockCacheHitCount += stats.SumRocksdbBlockCacheHitCount
	if stats.MaxRocksdbBlockCacheHitCount > other.MaxRocksdbBlockCacheHitCount {
		other.MaxRocksdbBlockCacheHitCount = stats.MaxRocksdbBlockCacheHitCount
	}
	other.SumRocksdbBlockReadCount += stats.SumRocksdbBlockReadCount
	if stats.MaxRocksdbBlockReadCount > other.MaxRocksdbBlockReadCount {
		other.MaxRocksdbBlockReadCount = stats.MaxRocksdbBlockReadCount
	}
	other.SumRocksdbBlockReadByte += stats.SumRocksdbBlockReadByte
	if stats.MaxRocksdbBlockReadByte > other.MaxRocksdbBlockReadByte {
		other.MaxRocksdbBlockReadByte = stats.MaxRocksdbBlockReadByte
	}

	// Transaction
	other.CommitCount += stats.CommitCount
	other.SumGetCommitTsTime += stats.SumGetCommitTsTime
	if stats.MaxGetCommitTsTime > other.MaxGetCommitTsTime {
		other.MaxGetCommitTsTime = stats.MaxGetCommitTsTime
	}
	other.SumPrewriteTime += stats.SumPrewriteTime
	if stats.MaxPrewriteTime > other.MaxPrewriteTime {
		other.MaxPrewriteTime = stats.MaxPrewriteTime
	}
	other.SumCommitTime += stats.SumCommitTime
	if stats.MaxCommitTime > other.MaxCommitTime {
		other.MaxCommitTime = stats.MaxCommitTime
	}
	other.SumLocalLatchTime += stats.SumLocalLatchTime
	if stats.MaxLocalLatchTime > other.MaxLocalLatchTime {
		other.MaxLocalLatchTime = stats.MaxLocalLatchTime
	}
	other.SumCommitBackoffTime += stats.SumCommitBackoffTime
	if stats.MaxCommitBackoffTime > other.MaxCommitBackoffTime {
		other.MaxCommitBackoffTime = stats.MaxCommitBackoffTime
	}
	other.SumResolveLockTime += stats.SumResolveLockTime
	if stats.MaxResolveLockTime > other.MaxResolveLockTime {
		other.MaxResolveLockTime = stats.MaxResolveLockTime
	}
	other.SumWriteKeys += stats.SumWriteKeys
	if stats.MaxWriteKeys > other.MaxWriteKeys {
		other.MaxWriteKeys = stats.MaxWriteKeys
	}
	other.SumWriteSizeBytes += stats.SumWriteSizeBytes
	if stats.MaxWriteSizeBytes > other.MaxWriteSizeBytes {
		other.MaxWriteSizeBytes = stats.MaxWriteSizeBytes
	}
	other.SumPrewriteRegionNum += stats.SumPrewriteRegionNum
	if stats.MaxPrewriteRegionNum > other.MaxPrewriteRegionNum {
		other.MaxPrewriteRegionNum = stats.MaxPrewriteRegionNum
	}
	other.SumTxnRetry += stats.SumTxnRetry
	if stats.MaxTxnRetry > other.MaxTxnRetry {
		other.MaxTxnRetry = stats.MaxTxnRetry
	}
	other.SumBackoffTimes += stats.SumBackoffTimes
	for k, v := range stats.BackoffTypes {
		other.BackoffTypes[k] += v
	}

	// Memory/Disk
	other.SumMemBytes += stats.SumMemBytes
	if stats.MaxMemBytes > other.MaxMemBytes {
		other.MaxMemBytes = stats.MaxMemBytes
	}
	other.SumDiskBytes += stats.SumDiskBytes
	if stats.MaxDiskBytes > other.MaxDiskBytes {
		other.MaxDiskBytes = stats.MaxDiskBytes
	}
	other.SumMemArbitration += stats.SumMemArbitration
	if stats.MaxMemArbitration > other.MaxMemArbitration {
		other.MaxMemArbitration = stats.MaxMemArbitration
	}

	// Row stats
	other.SumAffectedRows += stats.SumAffectedRows
	other.SumResultRows += stats.SumResultRows
	if stats.MaxResultRows > other.MaxResultRows {
		other.MaxResultRows = stats.MaxResultRows
	}
	if stats.MinResultRows < other.MinResultRows {
		other.MinResultRows = stats.MinResultRows
	}

	// Plan cache
	other.PlanCacheHits += stats.PlanCacheHits
	other.PlanCacheUnqualifiedCount += stats.PlanCacheUnqualifiedCount

	// Retry
	other.ExecRetryCount += stats.ExecRetryCount
	other.ExecRetryTime += stats.ExecRetryTime

	// Timestamps
	if stats.FirstSeen.Before(other.FirstSeen) {
		other.FirstSeen = stats.FirstSeen
	}
	if stats.LastSeen.After(other.LastSeen) {
		other.LastSeen = stats.LastSeen
	}

	// Other totals
	other.SumKVTotal += stats.SumKVTotal
	other.SumPDTotal += stats.SumPDTotal
	other.SumBackoffTotal += stats.SumBackoffTotal
	other.SumWriteSQLRespTotal += stats.SumWriteSQLRespTotal
	other.SumTiDBCPU += stats.SumTiDBCPU
	other.SumTiKVCPU += stats.SumTiKVCPU

	// RU
	other.SumRRU += stats.SumRRU
	if stats.MaxRRU > other.MaxRRU {
		other.MaxRRU = stats.MaxRRU
	}
	other.SumWRU += stats.SumWRU
	if stats.MaxWRU > other.MaxWRU {
		other.MaxWRU = stats.MaxWRU
	}
	other.SumRUWaitDuration += stats.SumRUWaitDuration
	if stats.MaxRUWaitDuration > other.MaxRUWaitDuration {
		other.MaxRUWaitDuration = stats.MaxRUWaitDuration
	}

	// Network traffic
	other.UnpackedBytesSentTiKVTotal += stats.UnpackedBytesSentTiKVTotal
	other.UnpackedBytesReceivedTiKVTotal += stats.UnpackedBytesReceivedTiKVTotal
	other.UnpackedBytesSentTiKVCrossZone += stats.UnpackedBytesSentTiKVCrossZone
	other.UnpackedBytesReceivedTiKVCrossZone += stats.UnpackedBytesReceivedTiKVCrossZone
	other.UnpackedBytesSentTiFlashTotal += stats.UnpackedBytesSentTiFlashTotal
	other.UnpackedBytesReceivedTiFlashTotal += stats.UnpackedBytesReceivedTiFlashTotal
	other.UnpackedBytesSentTiFlashCrossZone += stats.UnpackedBytesSentTiFlashCrossZone
	other.UnpackedBytesReceivedTiFlashCrossZone += stats.UnpackedBytesReceivedTiFlashCrossZone
}

// collectExtendedMetrics collects configured extended metrics.
// Note: RU consumption is now handled by dedicated fields (SumRRU, SumWRU, etc.).
func (a *Aggregator) collectExtendedMetrics(stats *StmtStats, info *stmtsummary.StmtExecInfo) {
	for _, metric := range a.cfg.ExtendedMetrics {
		if !metric.Enabled {
			continue
		}
		// TODO: Implement dynamic metric extraction based on Source path
		_ = stats
		_ = info
		_ = metric
	}
}

// rotateWindowLocked rotates the current window (must be called with lock held).
func (a *Aggregator) rotateWindowLocked() {
	oldWindow := a.window
	a.window = a.newWindow(time.Now())
	a.currentMemory.Store(0)

	if a.onFlush != nil && (len(oldWindow.Statements) > 0 || oldWindow.OtherBucket != nil) {
		go a.onFlush(oldWindow)
	}
}

// rotationLoop periodically checks and rotates windows.
func (a *Aggregator) rotationLoop() {
	defer a.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.windowLock.Lock()
			if time.Now().After(a.window.End) {
				a.rotateWindowLocked()
			}
			a.windowLock.Unlock()
		}
	}
}

// Flush forces a flush of the current window.
func (a *Aggregator) Flush() {
	a.windowLock.Lock()
	defer a.windowLock.Unlock()
	a.rotateWindowLocked()
}

// Close stops the aggregator and flushes remaining data.
func (a *Aggregator) Close() {
	if a.closed.Swap(true) {
		return
	}
	a.cancel()
	a.wg.Wait()
	a.Flush()
}

// GetPercentiles extracts P50, P95, P99 from histogram data.
func GetPercentiles(histogramData []byte) (p50, p95, p99 int64) {
	if len(histogramData) == 0 {
		return 0, 0, 0
	}
	hist, err := hdrhistogram.Decode(histogramData)
	if err != nil || hist == nil {
		return 0, 0, 0
	}
	return hist.ValueAtPercentile(50), hist.ValueAtPercentile(95), hist.ValueAtPercentile(99)
}

// Helper functions

func (a *Aggregator) formatTableNames(info *stmtsummary.StmtExecInfo) string {
	if info.StmtCtx == nil || len(info.StmtCtx.Tables) == 0 {
		return ""
	}
	var result string
	for i, t := range info.StmtCtx.Tables {
		if i > 0 {
			result += ","
		}
		if t.DB != "" {
			result += t.DB + "."
		}
		result += t.Table
	}
	return result
}

func (a *Aggregator) estimateInfoSize(info *stmtsummary.StmtExecInfo) int64 {
	size := int64(512) // Base size
	size += int64(len(info.SchemaName) + len(info.Digest) + len(info.PlanDigest))
	size += int64(len(info.NormalizedSQL))
	size += int64(len(info.PrevSQL))
	return size
}

func (a *Aggregator) getHistogram() *hdrhistogram.Histogram {
	h := a.histogramPool.Get().(*hdrhistogram.Histogram)
	h.Reset()
	return h
}

func (a *Aggregator) putHistogram(h *hdrhistogram.Histogram) {
	a.histogramPool.Put(h)
}

// Stats returns current aggregator statistics.
func (a *Aggregator) Stats() AggregatorStats {
	a.windowLock.RLock()
	defer a.windowLock.RUnlock()
	return AggregatorStats{
		DigestCount:   a.window.DigestCount,
		MemoryBytes:   a.currentMemory.Load(),
		WindowBegin:   a.window.Begin,
		WindowEnd:     a.window.End,
		HasOtherBucket: a.window.OtherBucket != nil,
	}
}

// GetCurrentStats returns a snapshot of current statement stats for querying.
func (a *Aggregator) GetCurrentStats() []*StmtStats {
	a.windowLock.RLock()
	defer a.windowLock.RUnlock()

	stats := make([]*StmtStats, 0, len(a.window.Statements))
	for _, s := range a.window.Statements {
		stats = append(stats, s)
	}

	if a.window.OtherBucket != nil {
		stats = append(stats, a.window.OtherBucket)
	}

	return stats
}

// AggregatorStats holds aggregator statistics for monitoring.
type AggregatorStats struct {
	DigestCount    int
	MemoryBytes    int64
	WindowBegin    time.Time
	WindowEnd      time.Time
	HasOtherBucket bool
}

