// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// metrics option
var (
	executorCounterMergeJoinExec            prometheus.Counter
	executorCountHashJoinExec               prometheus.Counter
	executorCounterHashAggExec              prometheus.Counter
	executorStreamAggExec                   prometheus.Counter
	executorCounterSortExec                 prometheus.Counter
	executorCounterTopNExec                 prometheus.Counter
	executorCounterNestedLoopApplyExec      prometheus.Counter
	executorCounterIndexLookUpJoin          prometheus.Counter
	executorCounterIndexLookUpExecutor      prometheus.Counter
	executorCounterIndexMergeReaderExecutor prometheus.Counter

	fastAnalyzeHistogramSample        prometheus.Observer
	fastAnalyzeHistogramAccessRegions prometheus.Observer
	fastAnalyzeHistogramScanKeys      prometheus.Observer

	sessionExecuteRunDurationInternal prometheus.Observer
	sessionExecuteRunDurationGeneral  prometheus.Observer
	totalTiFlashQuerySuccCounter      prometheus.Counter

	stmtNodeCounterUse       prometheus.Counter
	stmtNodeCounterShow      prometheus.Counter
	stmtNodeCounterBegin     prometheus.Counter
	stmtNodeCounterCommit    prometheus.Counter
	stmtNodeCounterRollback  prometheus.Counter
	stmtNodeCounterInsert    prometheus.Counter
	stmtNodeCounterReplace   prometheus.Counter
	stmtNodeCounterDelete    prometheus.Counter
	stmtNodeCounterUpdate    prometheus.Counter
	stmtNodeCounterSelect    prometheus.Counter
	stmtNodeCounterSavepoint prometheus.Counter

	totalQueryProcHistogramGeneral  prometheus.Observer
	totalCopProcHistogramGeneral    prometheus.Observer
	totalCopWaitHistogramGeneral    prometheus.Observer
	totalQueryProcHistogramInternal prometheus.Observer
	totalCopProcHistogramInternal   prometheus.Observer
	totalCopWaitHistogramInternal   prometheus.Observer

	transactionDurationPessimisticRollback prometheus.Observer
	transactionDurationOptimisticRollback  prometheus.Observer

	// pre-define observers for non-internal queries
	execBuildLocking       prometheus.Observer
	execOpenLocking        prometheus.Observer
	execNextLocking        prometheus.Observer
	execLockLocking        prometheus.Observer
	execBuildFinal         prometheus.Observer
	execOpenFinal          prometheus.Observer
	execNextFinal          prometheus.Observer
	execLockFinal          prometheus.Observer
	execCommitPrewrite     prometheus.Observer
	execCommitCommit       prometheus.Observer
	execCommitWaitCommitTS prometheus.Observer
	execCommitWaitLatestTS prometheus.Observer
	execCommitWaitLatch    prometheus.Observer
	execCommitWaitBinlog   prometheus.Observer
	execWriteResponse      prometheus.Observer
	execUnknown            prometheus.Observer

	// pre-define observers for internal queries
	execBuildLockingInternal       prometheus.Observer
	execOpenLockingInternal        prometheus.Observer
	execNextLockingInternal        prometheus.Observer
	execLockLockingInternal        prometheus.Observer
	execBuildFinalInternal         prometheus.Observer
	execOpenFinalInternal          prometheus.Observer
	execNextFinalInternal          prometheus.Observer
	execLockFinalInternal          prometheus.Observer
	execCommitPrewriteInternal     prometheus.Observer
	execCommitCommitInternal       prometheus.Observer
	execCommitWaitCommitTSInternal prometheus.Observer
	execCommitWaitLatestTSInternal prometheus.Observer
	execCommitWaitLatchInternal    prometheus.Observer
	execCommitWaitBinlogInternal   prometheus.Observer
	execWriteResponseInternal      prometheus.Observer
	execUnknownInternal            prometheus.Observer

	phaseDurationObserverMap         map[string]prometheus.Observer
	phaseDurationObserverMapInternal map[string]prometheus.Observer
)

func init() {
	InitMetricsVars()
	InitPhaseDurationObserverMap()
	task.RegisterExecutorMetrics(InitMetricsVars)
}

// InitMetricsVars init executor metrics counter
func InitMetricsVars() {
	log.Info("init executor metrics")
	executorCounterMergeJoinExec = metrics.ExecutorCounter.WithLabelValues("MergeJoinExec")
	executorCountHashJoinExec = metrics.ExecutorCounter.WithLabelValues("HashJoinExec")
	executorCounterHashAggExec = metrics.ExecutorCounter.WithLabelValues("HashAggExec")
	executorStreamAggExec = metrics.ExecutorCounter.WithLabelValues("StreamAggExec")
	executorCounterSortExec = metrics.ExecutorCounter.WithLabelValues("SortExec")
	executorCounterTopNExec = metrics.ExecutorCounter.WithLabelValues("TopNExec")
	executorCounterNestedLoopApplyExec = metrics.ExecutorCounter.WithLabelValues("NestedLoopApplyExec")
	executorCounterIndexLookUpJoin = metrics.ExecutorCounter.WithLabelValues("IndexLookUpJoin")
	executorCounterIndexLookUpExecutor = metrics.ExecutorCounter.WithLabelValues("IndexLookUpExecutor")
	executorCounterIndexMergeReaderExecutor = metrics.ExecutorCounter.WithLabelValues("IndexMergeReaderExecutor")

	fastAnalyzeHistogramSample = metrics.FastAnalyzeHistogram.WithLabelValues(metrics.LblGeneral, "sample")
	fastAnalyzeHistogramAccessRegions = metrics.FastAnalyzeHistogram.WithLabelValues(metrics.LblGeneral, "access_regions")
	fastAnalyzeHistogramScanKeys = metrics.FastAnalyzeHistogram.WithLabelValues(metrics.LblGeneral, "scan_keys")

	sessionExecuteRunDurationInternal = metrics.SessionExecuteRunDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteRunDurationGeneral = metrics.SessionExecuteRunDuration.WithLabelValues(metrics.LblGeneral)
	totalTiFlashQuerySuccCounter = metrics.TiFlashQueryTotalCounter.WithLabelValues("", metrics.LblOK)

	stmtNodeCounterUse = metrics.StmtNodeCounter.WithLabelValues("Use")
	stmtNodeCounterShow = metrics.StmtNodeCounter.WithLabelValues("Show")
	stmtNodeCounterBegin = metrics.StmtNodeCounter.WithLabelValues("Begin")
	stmtNodeCounterCommit = metrics.StmtNodeCounter.WithLabelValues("Commit")
	stmtNodeCounterRollback = metrics.StmtNodeCounter.WithLabelValues("Rollback")
	stmtNodeCounterInsert = metrics.StmtNodeCounter.WithLabelValues("Insert")
	stmtNodeCounterReplace = metrics.StmtNodeCounter.WithLabelValues("Replace")
	stmtNodeCounterDelete = metrics.StmtNodeCounter.WithLabelValues("Delete")
	stmtNodeCounterUpdate = metrics.StmtNodeCounter.WithLabelValues("Update")
	stmtNodeCounterSelect = metrics.StmtNodeCounter.WithLabelValues("Select")
	stmtNodeCounterSavepoint = metrics.StmtNodeCounter.WithLabelValues("Savepoint")

	totalQueryProcHistogramGeneral = metrics.TotalQueryProcHistogram.WithLabelValues(metrics.LblGeneral)
	totalCopProcHistogramGeneral = metrics.TotalCopProcHistogram.WithLabelValues(metrics.LblGeneral)
	totalCopWaitHistogramGeneral = metrics.TotalCopWaitHistogram.WithLabelValues(metrics.LblGeneral)
	totalQueryProcHistogramInternal = metrics.TotalQueryProcHistogram.WithLabelValues(metrics.LblInternal)
	totalCopProcHistogramInternal = metrics.TotalCopProcHistogram.WithLabelValues(metrics.LblInternal)
	totalCopWaitHistogramInternal = metrics.TotalCopWaitHistogram.WithLabelValues(metrics.LblInternal)

	transactionDurationPessimisticRollback = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblRollback)
	transactionDurationOptimisticRollback = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblRollback)

	// pre-define observers for non-internal queries
	execBuildLocking = metrics.ExecPhaseDuration.WithLabelValues(phaseBuildLocking, "0")
	execOpenLocking = metrics.ExecPhaseDuration.WithLabelValues(phaseOpenLocking, "0")
	execNextLocking = metrics.ExecPhaseDuration.WithLabelValues(phaseNextLocking, "0")
	execLockLocking = metrics.ExecPhaseDuration.WithLabelValues(phaseLockLocking, "0")
	execBuildFinal = metrics.ExecPhaseDuration.WithLabelValues(phaseBuildFinal, "0")
	execOpenFinal = metrics.ExecPhaseDuration.WithLabelValues(phaseOpenFinal, "0")
	execNextFinal = metrics.ExecPhaseDuration.WithLabelValues(phaseNextFinal, "0")
	execLockFinal = metrics.ExecPhaseDuration.WithLabelValues(phaseLockFinal, "0")
	execCommitPrewrite = metrics.ExecPhaseDuration.WithLabelValues(phaseCommitPrewrite, "0")
	execCommitCommit = metrics.ExecPhaseDuration.WithLabelValues(phaseCommitCommit, "0")
	execCommitWaitCommitTS = metrics.ExecPhaseDuration.WithLabelValues(phaseCommitWaitCommitTS, "0")
	execCommitWaitLatestTS = metrics.ExecPhaseDuration.WithLabelValues(phaseCommitWaitLatestTS, "0")
	execCommitWaitLatch = metrics.ExecPhaseDuration.WithLabelValues(phaseCommitWaitLatch, "0")
	execCommitWaitBinlog = metrics.ExecPhaseDuration.WithLabelValues(phaseCommitWaitBinlog, "0")
	execWriteResponse = metrics.ExecPhaseDuration.WithLabelValues(phaseWriteResponse, "0")
	execUnknown = metrics.ExecPhaseDuration.WithLabelValues("unknown", "0")

	// pre-define observers for internal queries
	execBuildLockingInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseBuildLocking, "1")
	execOpenLockingInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseOpenLocking, "1")
	execNextLockingInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseNextLocking, "1")
	execLockLockingInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseLockLocking, "1")
	execBuildFinalInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseBuildFinal, "1")
	execOpenFinalInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseOpenFinal, "1")
	execNextFinalInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseNextFinal, "1")
	execLockFinalInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseLockFinal, "1")
	execCommitPrewriteInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseCommitPrewrite, "1")
	execCommitCommitInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseCommitCommit, "1")
	execCommitWaitCommitTSInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseCommitWaitCommitTS, "1")
	execCommitWaitLatestTSInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseCommitWaitLatestTS, "1")
	execCommitWaitLatchInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseCommitWaitLatch, "1")
	execCommitWaitBinlogInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseCommitWaitBinlog, "1")
	execWriteResponseInternal = metrics.ExecPhaseDuration.WithLabelValues(phaseWriteResponse, "1")
	execUnknownInternal = metrics.ExecPhaseDuration.WithLabelValues("unknown", "1")
}

const (
	phaseBuildLocking       = "build:locking"
	phaseOpenLocking        = "open:locking"
	phaseNextLocking        = "next:locking"
	phaseLockLocking        = "lock:locking"
	phaseBuildFinal         = "build:final"
	phaseOpenFinal          = "open:final"
	phaseNextFinal          = "next:final"
	phaseLockFinal          = "lock:final"
	phaseCommitPrewrite     = "commit:prewrite"
	phaseCommitCommit       = "commit:commit"
	phaseCommitWaitCommitTS = "commit:wait:commit-ts"
	phaseCommitWaitLatestTS = "commit:wait:latest-ts"
	phaseCommitWaitLatch    = "commit:wait:local-latch"
	phaseCommitWaitBinlog   = "commit:wait:prewrite-binlog"
	phaseWriteResponse      = "write-response"
)

// InitPhaseDurationObserverMap init observer map
func InitPhaseDurationObserverMap() {
	phaseDurationObserverMap = map[string]prometheus.Observer{
		phaseBuildLocking:       execBuildLocking,
		phaseOpenLocking:        execOpenLocking,
		phaseNextLocking:        execNextLocking,
		phaseLockLocking:        execLockLocking,
		phaseBuildFinal:         execBuildFinal,
		phaseOpenFinal:          execOpenFinal,
		phaseNextFinal:          execNextFinal,
		phaseLockFinal:          execLockFinal,
		phaseCommitPrewrite:     execCommitPrewrite,
		phaseCommitCommit:       execCommitCommit,
		phaseCommitWaitCommitTS: execCommitWaitCommitTS,
		phaseCommitWaitLatestTS: execCommitWaitLatestTS,
		phaseCommitWaitLatch:    execCommitWaitLatch,
		phaseCommitWaitBinlog:   execCommitWaitBinlog,
		phaseWriteResponse:      execWriteResponse,
	}
	phaseDurationObserverMapInternal = map[string]prometheus.Observer{
		phaseBuildLocking:       execBuildLockingInternal,
		phaseOpenLocking:        execOpenLockingInternal,
		phaseNextLocking:        execNextLockingInternal,
		phaseLockLocking:        execLockLockingInternal,
		phaseBuildFinal:         execBuildFinalInternal,
		phaseOpenFinal:          execOpenFinalInternal,
		phaseNextFinal:          execNextFinalInternal,
		phaseLockFinal:          execLockFinalInternal,
		phaseCommitPrewrite:     execCommitPrewriteInternal,
		phaseCommitCommit:       execCommitCommitInternal,
		phaseCommitWaitCommitTS: execCommitWaitCommitTSInternal,
		phaseCommitWaitLatestTS: execCommitWaitLatestTSInternal,
		phaseCommitWaitLatch:    execCommitWaitLatchInternal,
		phaseCommitWaitBinlog:   execCommitWaitBinlogInternal,
		phaseWriteResponse:      execWriteResponseInternal,
	}
}
