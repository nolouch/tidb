// Copyright 2023 PingCAP, Inc.
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

package sem

import (
	"sync/atomic"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// enableStrictMode changes some variable's default value and restrictions.
func enableStrictMode() {
	variable.SetSysVarMin(variable.ValidatePasswordLength, 8)
	variable.SetSysVarMin(variable.ValidatePasswordMixedCaseCount, 1)
	variable.SetSysVarMin(variable.ValidatePasswordNumberCount, 1)
	variable.SetSysVarPossibleValues(variable.ValidatePasswordPolicy, []string{"MEDIUM", "STRONG"})
}

// disableStrictMode changes variable's default value and restrictions back to normal.
func disableStrictMode() {
	variable.SetSysVarMin(variable.ValidatePasswordLength, 0)
	variable.SetSysVarMin(variable.ValidatePasswordMixedCaseCount, 0)
	variable.SetSysVarMin(variable.ValidatePasswordNumberCount, 0)
	variable.SetSysVarPossibleValues(variable.ValidatePasswordPolicy, []string{"LOW", "MEDIUM", "STRONG"})
}

// IsStrictMode checks if sem is in strict mode.
func IsStrictMode() bool {
	return atomic.LoadInt32(&semEnabled) == 2
}

// strictModeRestrictedPrivilege defines privileges that are restricted in enhanced mode.
func strictModeRestrictedPrivilege(privNameInUpper string) bool {
	if len(privNameInUpper) >= 12 && privNameInUpper[:11] == restrictedPriv {
		return true
	}
	switch privNameInUpper {
	case
		placementAdmin,
		backupAdmin,
		restoreAdmin,
		resourceGroupAdmin:
		return true
	}
	return false
}

// strictModeInvisibleTable defines tables that are invisible in strict mode.
func strictModeInvisibleTable(dbLowerName, tblLowerName string) bool {
	switch dbLowerName {
	case informationSchema:
		switch tblLowerName {
		case
			attributes,
			clusterConfig,
			clusterHardware,
			clusterInfo,
			clusterLoad,
			clusterLog,
			clusterSlowQuery,
			clusterStatementsSummary,
			clusterStatementsSummaryEvicted,
			clusterStatementsSummaryHistory,
			clusterSystemInfo,
			inspectionResult,
			inspectionRules,
			inspectionSummary,
			metricsSummary,
			metricsSummaryByLabel,
			metricsTables,
			placementPolicies,
			resourceGroups,
			slowQuery,
			statementsSummary,
			statementsSummaryEvicted,
			statementsSummaryHistory,
			tidbHotRegions,
			tidbHotRegionsHistory,
			tidbServersInfo,
			tiflashSegments,
			tiflashTables,
			tikvRegionPeers,
			tikvRegionStatus,
			tikvStoreStatus:
			return true
		}
	case performanceSchema:
		switch tblLowerName {
		case
			pdProfileAllocs,
			pdProfileBlock,
			pdProfileCPU,
			pdProfileGoroutines,
			pdProfileMemory,
			pdProfileMutex,
			tidbProfileAllocs,
			tidbProfileBlock,
			tidbProfileCPU,
			tidbProfileGoroutines,
			tidbProfileMemory,
			tidbProfileMutex,
			tikvProfileCPU:
			return true
		}
	case mysql.SystemDB:
		switch tblLowerName {
		case
			exprPushdownBlacklist,
			gcDeleteRange,
			gcDeleteRangeDone,
			optRuleBlacklist,
			tidb,
			tidbTTLJobHistory,
			tidbTTLTableStatus,
			tidbTTLTask:
			return true
		}
	case metricsSchema:
		return true
	}
	return false
}

// strictModeInvisibleSysVar defines variables that are invisible in strict mode.
func strictModeInvisibleSysVar(varNameInLower string) bool {
	switch varNameInLower {
	case
		variable.DataDir,
		variable.PluginDir,
		variable.PluginLoad,
		variable.TiDBCheckMb4ValueInUTF8,
		variable.TiDBConfig,
		variable.TiDBEnableCollectExecutionInfo,
		variable.TiDBEnableSlowLog,
		variable.TiDBEnableTelemetry,
		variable.TiDBExpensiveQueryTimeThreshold,
		variable.TiDBForcePriority,
		variable.TiDBGeneralLog,
		variable.TiDBMemoryUsageAlarmRatio,
		variable.TiDBMetricSchemaRangeDuration,
		variable.TiDBMetricSchemaStep,
		variable.TiDBOptWriteRowID,
		variable.TiDBPProfSQLCPU,
		variable.TiDBRecordPlanInSlowLog,
		variable.TiDBRedactLog,
		variable.TiDBRestrictedReadOnly,
		variable.TiDBRowFormatVersion,
		variable.TiDBSlowQueryFile,
		variable.TiDBSlowLogThreshold,
		//TODO: add variable.TiDBSlowTxnLogThreshold after upgrade to 7.1
		variable.TiDBTopSQLMaxMetaCount,
		variable.TiDBTopSQLMaxTimeSeriesCount:
		return true
	}
	return false
}

// strictModeReadOnlySysVar defines variables that are read-only in strict mode.
func strictModeReadOnlySysVar(varNameInLower string) bool {
	switch varNameInLower {
	case
		variable.InteractiveTimeout,
		variable.MaxAllowedPacket,
		variable.SkipNameResolve,
		variable.SQLLogBin,
		variable.TiDBCDCWriteSource,
		variable.TiDBDDLDiskQuota,
		variable.TiDBDDLEnableFastReorg,
		variable.TiDBDDLErrorCountLimit,
		variable.TiDBDDLFlashbackConcurrency,
		variable.TiDBDDLReorgBatchSize,
		variable.TiDBDDLReorgPriority,
		variable.TiDBDDLReorgWorkerCount,
		variable.TiDBEnable1PC,
		variable.TiDBEnableAsyncCommit,
		variable.TiDBEnableAutoAnalyze,
		variable.TiDBEnableDDL,
		variable.TiDBEnableGCAwareMemoryTrack,
		variable.TiDBEnableGOGCTuner,
		variable.TiDBEnableLocalTxn,
		variable.TiDBEnableResourceControl,
		variable.TiDBEnableStmtSummary,
		variable.TiDBEnableTopSQL,
		variable.TiDBEnableTSOFollowerProxy,
		variable.TiDBGCConcurrency,
		variable.TiDBGCEnable,
		variable.TiDBGCLifetime,
		variable.TiDBGCMaxWaitTime,
		variable.TiDBGCRunInterval,
		variable.TiDBGCScanLockMode,
		variable.TiDBGenerateBinaryPlan,
		variable.TiDBGOGCTunerThreshold,
		variable.TiDBGuaranteeLinearizability,
		variable.TiDBIsolationReadEngines,
		variable.TiDBLogFileMaxDays,
		variable.TiDBPlacementMode,
		variable.TiDBScatterRegion,
		variable.TiDBServerMemoryLimit,
		variable.TiDBServerMemoryLimitGCTrigger,
		variable.TiDBServerMemoryLimitSessMinSize,
		variable.TiDBSimplifiedMetrics,
		variable.TiDBStatsLoadSyncWait,
		variable.TiDBStmtSummaryEnablePersistent,
		variable.TiDBStmtSummaryFileMaxBackups,
		variable.TiDBStmtSummaryFileMaxDays,
		variable.TiDBStmtSummaryFileMaxSize,
		variable.TiDBStmtSummaryFilename,
		variable.TiDBStmtSummaryHistorySize,
		variable.TiDBStmtSummaryInternalQuery,
		variable.TiDBStmtSummaryMaxSQLLength,
		variable.TiDBStmtSummaryMaxStmtCount,
		variable.TiDBStmtSummaryRefreshInterval,
		variable.TiDBSysProcScanConcurrency,
		variable.TiDBTSOClientBatchMaxWaitTime,
		variable.TiDBTTLDeleteBatchSize,
		variable.TiDBTTLDeleteRateLimit,
		variable.TiDBTTLDeleteWorkerCount,
		variable.TiDBTTLJobEnable,
		variable.TiDBTTLJobScheduleWindowEndTime,
		variable.TiDBTTLJobScheduleWindowStartTime,
		// TODO: add variable.TiDBTTLRunningTasks after 7.1
		variable.TiDBTTLScanBatchSize,
		variable.TiDBTTLScanWorkerCount,
		variable.TiDBTxnMode,
		variable.TiDBWaitSplitRegionFinish,
		variable.TiDBWaitSplitRegionTimeout,
		variable.TiDBTxnScope,
		variable.ValidatePasswordEnable,
		variable.WaitTimeout:
		return true

	}
	return false
}
