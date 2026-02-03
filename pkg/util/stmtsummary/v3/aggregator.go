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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"go.uber.org/zap"
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

	// Sample data
	stats.SampleSQL = info.LazyInfo.GetOriginalSQL()
	samplePlan, _, _ := info.LazyInfo.GetEncodedPlan()
	stats.SamplePlan = samplePlan
	stats.PrevSQL = info.PrevSQL

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
func (a *Aggregator) mergeToStats(stats *StmtStats, info *stmtsummary.StmtExecInfo) {
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

	// Memory/Disk
	stats.SumMemBytes += info.MemMax
	if info.MemMax > stats.MaxMemBytes {
		stats.MaxMemBytes = info.MemMax
	}
	stats.SumDiskBytes += info.DiskMax
	if info.DiskMax > stats.MaxDiskBytes {
		stats.MaxDiskBytes = info.DiskMax
	}

	// CPU time
	stats.SumTiDBCPU += info.CPUUsages.TidbCPUTime
	stats.SumTiKVCPU += info.CPUUsages.TikvCPUTime

	// Coprocessor
	if info.CopTasks != nil {
		stats.SumNumCopTasks += int64(info.CopTasks.NumCopTasks)
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

	// Key scan
	if info.ExecDetail.ScanDetail != nil {
		stats.SumTotalKeys += info.ExecDetail.ScanDetail.TotalKeys
		if info.ExecDetail.ScanDetail.TotalKeys > stats.MaxTotalKeys {
			stats.MaxTotalKeys = info.ExecDetail.ScanDetail.TotalKeys
		}
		stats.SumProcessedKeys += info.ExecDetail.ScanDetail.ProcessedKeys
		if info.ExecDetail.ScanDetail.ProcessedKeys > stats.MaxProcessedKeys {
			stats.MaxProcessedKeys = info.ExecDetail.ScanDetail.ProcessedKeys
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
		stats.SumWriteKeys += int64(commit.WriteKeys)
		if int64(commit.WriteKeys) > stats.MaxWriteKeys {
			stats.MaxWriteKeys = int64(commit.WriteKeys)
		}
		stats.SumWriteSizeBytes += int64(commit.WriteSize)
		if int64(commit.WriteSize) > stats.MaxWriteSizeBytes {
			stats.MaxWriteSizeBytes = int64(commit.WriteSize)
		}
	}

	// Row stats
	stats.SumAffectedRows += int64(info.StmtCtx.AffectedRows())
	if info.ResultRows > 0 {
		stats.SumResultRows += info.ResultRows
		if info.ResultRows > stats.MaxResultRows {
			stats.MaxResultRows = info.ResultRows
		}
		if info.ResultRows < stats.MinResultRows {
			stats.MinResultRows = info.ResultRows
		}
	}

	// Plan cache
	if info.PlanInCache {
		stats.PlanInCache = true
		stats.PlanCacheHits++
	}

	// Timestamps
	if info.StartTime.Before(stats.FirstSeen) {
		stats.FirstSeen = info.StartTime
	}
	if info.StartTime.After(stats.LastSeen) {
		stats.LastSeen = info.StartTime
	}

	// Extended metrics (from config)
	a.collectExtendedMetrics(stats, info)
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
	other.ExecCount += stats.ExecCount
	other.SumErrors += stats.SumErrors
	other.SumWarnings += stats.SumWarnings
	other.SumLatency += stats.SumLatency
	if stats.MaxLatency > other.MaxLatency {
		other.MaxLatency = stats.MaxLatency
	}
	if stats.MinLatency < other.MinLatency {
		other.MinLatency = stats.MinLatency
	}
	other.SumMemBytes += stats.SumMemBytes
	if stats.MaxMemBytes > other.MaxMemBytes {
		other.MaxMemBytes = stats.MaxMemBytes
	}
	other.SumDiskBytes += stats.SumDiskBytes
	if stats.MaxDiskBytes > other.MaxDiskBytes {
		other.MaxDiskBytes = stats.MaxDiskBytes
	}
	// ... merge other fields as needed
}

// collectExtendedMetrics collects configured extended metrics.
func (a *Aggregator) collectExtendedMetrics(stats *StmtStats, info *stmtsummary.StmtExecInfo) {
	for _, metric := range a.cfg.ExtendedMetrics {
		if !metric.Enabled {
			continue
		}
		// TODO: Implement dynamic metric extraction based on Source path
		// For now, handle known metrics
		switch metric.Name {
		case "sum_ru_consumption":
			if info.RUDetail != nil {
				existing, ok := stats.ExtendedMetrics[metric.Name]
				if ok {
					stats.ExtendedMetrics[metric.Name] = MetricValue{
						Type:    MetricTypeFloat64,
						Float64: existing.Float64 + info.RUDetail.RRU() + info.RUDetail.WRU(),
					}
				} else {
					stats.ExtendedMetrics[metric.Name] = MetricValue{
						Type:    MetricTypeFloat64,
						Float64: info.RUDetail.RRU() + info.RUDetail.WRU(),
					}
				}
			}
		}
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

// AggregatorStats holds aggregator statistics for monitoring.
type AggregatorStats struct {
	DigestCount    int
	MemoryBytes    int64
	WindowBegin    time.Time
	WindowEnd      time.Time
	HasOtherBucket bool
}

func init() {
	// Silence unused import warning for logutil
	_ = logutil.BgLogger
	_ = zap.String
}
