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

package session

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	statementPerTransactionPessimisticOK    prometheus.Observer
	statementPerTransactionPessimisticError prometheus.Observer
	statementPerTransactionOptimisticOK     prometheus.Observer
	statementPerTransactionOptimisticError  prometheus.Observer
	transactionDurationPessimisticCommit    prometheus.Observer
	transactionDurationPessimisticAbort     prometheus.Observer
	transactionDurationOptimisticCommit     prometheus.Observer
	transactionDurationOptimisticAbort      prometheus.Observer

	sessionExecuteCompileDurationInternal prometheus.Observer
	sessionExecuteCompileDurationGeneral  prometheus.Observer
	sessionExecuteParseDurationInternal   prometheus.Observer
	sessionExecuteParseDurationGeneral    prometheus.Observer

	telemetryCTEUsageRecurCTE       prometheus.Counter
	telemetryCTEUsageNonRecurCTE    prometheus.Counter
	telemetryCTEUsageNotCTE         prometheus.Counter
	telemetryMultiSchemaChangeUsage prometheus.Counter
	telemetryFlashbackClusterUsage  prometheus.Counter

	telemetryTablePartitionUsage                prometheus.Counter
	telemetryTablePartitionListUsage            prometheus.Counter
	telemetryTablePartitionRangeUsage           prometheus.Counter
	telemetryTablePartitionHashUsage            prometheus.Counter
	telemetryTablePartitionRangeColumnsUsage    prometheus.Counter
	telemetryTablePartitionRangeColumnsGt1Usage prometheus.Counter
	telemetryTablePartitionRangeColumnsGt2Usage prometheus.Counter
	telemetryTablePartitionRangeColumnsGt3Usage prometheus.Counter
	telemetryTablePartitionListColumnsUsage     prometheus.Counter
	telemetryTablePartitionMaxPartitionsUsage   prometheus.Counter
	telemetryTablePartitionCreateIntervalUsage  prometheus.Counter
	telemetryTablePartitionAddIntervalUsage     prometheus.Counter
	telemetryTablePartitionDropIntervalUsage    prometheus.Counter
	telemetryExchangePartitionUsage             prometheus.Counter

	telemetryLockUserUsage          prometheus.Counter
	telemetryUnlockUserUsage        prometheus.Counter
	telemetryCreateOrAlterUserUsage prometheus.Counter
)

func init() {
	InitMetricsVars()
	task.RegisterSessionMetrics(InitMetricsVars)
}

// InitMetricsVars init session metrics counter
func InitMetricsVars() {
	log.Info("init session metrics")
	statementPerTransactionPessimisticOK = metrics.StatementPerTransaction.WithLabelValues(metrics.LblPessimistic, metrics.LblOK)
	statementPerTransactionPessimisticError = metrics.StatementPerTransaction.WithLabelValues(metrics.LblPessimistic, metrics.LblError)
	statementPerTransactionOptimisticOK = metrics.StatementPerTransaction.WithLabelValues(metrics.LblOptimistic, metrics.LblOK)
	statementPerTransactionOptimisticError = metrics.StatementPerTransaction.WithLabelValues(metrics.LblOptimistic, metrics.LblError)
	transactionDurationPessimisticCommit = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblCommit)
	transactionDurationPessimisticAbort = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblAbort)
	transactionDurationOptimisticCommit = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblCommit)
	transactionDurationOptimisticAbort = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblAbort)

	sessionExecuteCompileDurationInternal = metrics.SessionExecuteCompileDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteCompileDurationGeneral = metrics.SessionExecuteCompileDuration.WithLabelValues(metrics.LblGeneral)
	sessionExecuteParseDurationInternal = metrics.SessionExecuteParseDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteParseDurationGeneral = metrics.SessionExecuteParseDuration.WithLabelValues(metrics.LblGeneral)

	telemetryCTEUsageRecurCTE = metrics.TelemetrySQLCTECnt.WithLabelValues("recurCTE")
	telemetryCTEUsageNonRecurCTE = metrics.TelemetrySQLCTECnt.WithLabelValues("nonRecurCTE")
	telemetryCTEUsageNotCTE = metrics.TelemetrySQLCTECnt.WithLabelValues("notCTE")
	telemetryMultiSchemaChangeUsage = metrics.TelemetryMultiSchemaChangeCnt
	telemetryFlashbackClusterUsage = metrics.TelemetryFlashbackClusterCnt

	telemetryTablePartitionUsage = metrics.TelemetryTablePartitionCnt
	telemetryTablePartitionListUsage = metrics.TelemetryTablePartitionListCnt
	telemetryTablePartitionRangeUsage = metrics.TelemetryTablePartitionRangeCnt
	telemetryTablePartitionHashUsage = metrics.TelemetryTablePartitionHashCnt
	telemetryTablePartitionRangeColumnsUsage = metrics.TelemetryTablePartitionRangeColumnsCnt
	telemetryTablePartitionRangeColumnsGt1Usage = metrics.TelemetryTablePartitionRangeColumnsGt1Cnt
	telemetryTablePartitionRangeColumnsGt2Usage = metrics.TelemetryTablePartitionRangeColumnsGt2Cnt
	telemetryTablePartitionRangeColumnsGt3Usage = metrics.TelemetryTablePartitionRangeColumnsGt3Cnt
	telemetryTablePartitionListColumnsUsage = metrics.TelemetryTablePartitionListColumnsCnt
	telemetryTablePartitionMaxPartitionsUsage = metrics.TelemetryTablePartitionMaxPartitionsCnt
	telemetryTablePartitionCreateIntervalUsage = metrics.TelemetryTablePartitionCreateIntervalPartitionsCnt
	telemetryTablePartitionAddIntervalUsage = metrics.TelemetryTablePartitionAddIntervalPartitionsCnt
	telemetryTablePartitionDropIntervalUsage = metrics.TelemetryTablePartitionDropIntervalPartitionsCnt
	telemetryExchangePartitionUsage = metrics.TelemetryExchangePartitionCnt

	telemetryLockUserUsage = metrics.TelemetryAccountLockCnt.WithLabelValues("lockUser")
	telemetryUnlockUserUsage = metrics.TelemetryAccountLockCnt.WithLabelValues("unlockUser")
	telemetryCreateOrAlterUserUsage = metrics.TelemetryAccountLockCnt.WithLabelValues("createOrAlterUser")
}
