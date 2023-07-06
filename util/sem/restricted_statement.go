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
	"fmt"
	"strings"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/dbterror"
)

// cloudAdminName is the username of cloud_admin.
const cloudAdminName = "cloud_admin"

// IsRestrictedStatement checks if the statement is allowed to execute.
func IsRestrictedStatement(stmt ast.Node) error {
	// Only check a statement when enhanced sem is enabled.
	if !IsStrictMode() {
		return nil
	}
	return strictModeRestrictedStatement(stmt)
}

// strictModeRestrictedStatement checks if the statement is a restricted under enhanced sem,
// and returns an error if it is.
func strictModeRestrictedStatement(stmt ast.Node) error {
	switch x := stmt.(type) {
	case *ast.DeallocateStmt,
		*ast.DeleteStmt,
		*ast.ExecuteStmt,
		*ast.ExplainStmt,
		*ast.ExplainForStmt,
		*ast.TraceStmt,
		*ast.InsertStmt,
		*ast.LockStatsStmt,
		*ast.UnlockStatsStmt,
		*ast.IndexAdviseStmt,
		*ast.PlanReplayerStmt,
		*ast.PrepareStmt,
		*ast.SelectStmt,
		*ast.SetOprStmt,
		*ast.UpdateStmt,
		*ast.DoStmt,
		*ast.SetStmt,
		*ast.AnalyzeTableStmt,
		*ast.CreateBindingStmt,
		*ast.DropBindingStmt,
		*ast.SetBindingStmt,
		*ast.CompactTableStmt:
		return nil
	case *ast.LoadDataStmt:
		return verifyLoadData(x)
	case *ast.AdminStmt:
		return verifyAdmin(x)
	case *ast.LoadStatsStmt:
		return verifyLoadStats(x)
	case *ast.ShowStmt:
		return verifyShow(x)
	case *ast.SetConfigStmt:
		return verifySetConfig(x)
	case *ast.BinlogStmt, *ast.FlushStmt, *ast.UseStmt, *ast.BRIEStmt,
		*ast.BeginStmt, *ast.CommitStmt, *ast.SavepointStmt, *ast.ReleaseSavepointStmt, *ast.RollbackStmt, *ast.CreateUserStmt, *ast.SetPwdStmt, *ast.AlterInstanceStmt,
		*ast.GrantStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.RevokeStmt, *ast.KillStmt, *ast.DropStatsStmt,
		*ast.GrantRoleStmt, *ast.RevokeRoleStmt, *ast.SetRoleStmt, *ast.SetDefaultRoleStmt, *ast.ShutdownStmt,
		*ast.RenameUserStmt, *ast.NonTransactionalDMLStmt, *ast.SetSessionStatesStmt:
		return verifySimple(x)
	case ast.DDLNode:
		return verifyDDL(x)
	case *ast.ChangeStmt:
		return verifyChange(x)
	case *ast.SplitRegionStmt:
		return verifySplitRegion(x)
	}

	return nil
}
func verifyDDL(stmt ast.DDLNode) error {
	switch s := stmt.(type) {
	case
		*ast.CreateDatabaseStmt,
		*ast.AlterDatabaseStmt,
		*ast.DropDatabaseStmt,
		*ast.DropTableStmt,
		*ast.DropSequenceStmt,
		*ast.RenameTableStmt,
		*ast.CreateViewStmt,
		*ast.CreateSequenceStmt,
		*ast.CreateIndexStmt,
		*ast.DropIndexStmt,
		*ast.LockTablesStmt,
		*ast.UnlockTablesStmt,
		*ast.CleanupTableLockStmt,
		*ast.RepairTableStmt,
		*ast.TruncateTableStmt,
		*ast.AlterSequenceStmt,
		*ast.RecoverTableStmt,
		*ast.FlashBackDatabaseStmt,
		*ast.FlashBackTableStmt:
		return nil
	case *ast.CreateTableStmt:
		for _, option := range s.Options {
			if option.Tp == ast.TableOptionTTL ||
				option.Tp == ast.TableOptionTTLEnable ||
				option.Tp == ast.TableOptionTTLJobInterval {
				return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("TTL")
			}
		}
		return nil
	case *ast.AlterTableStmt:
		for _, spec := range s.Specs {
			if spec.Tp == ast.AlterTableRemoveTTL {
				return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("TTL")
			}
		}
		return nil
	case *ast.AlterPlacementPolicyStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ALTER PLACEMENT POLICY")
	case *ast.CreatePlacementPolicyStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("CREATE PLACEMENT POLICY")
	case *ast.DropPlacementPolicyStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("DROP PLACEMENT POLICY")
	case *ast.DropResourceGroupStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("DROP RESOURCE GROUP")
	case *ast.CreateResourceGroupStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("CREATE RESOURCE GROUP")
	case *ast.FlashBackToTimestampStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("FLASHBACK CLUSTER")
	case *ast.AlterResourceGroupStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ALTER RESOURCE GROUP")

	}
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause(fmt.Sprintf("Unsupported DDL %T", stmt))
}

func verifySimple(stmt ast.Node) error {
	switch s := stmt.(type) {
	case
		*ast.GrantRoleStmt,
		*ast.FlushStmt,
		*ast.BeginStmt,
		*ast.CommitStmt,
		*ast.SavepointStmt,
		*ast.ReleaseSavepointStmt,
		*ast.RollbackStmt,
		*ast.CreateUserStmt,
		*ast.AlterUserStmt,
		*ast.DropUserStmt,
		*ast.SetPwdStmt,
		*ast.SetSessionStatesStmt,
		*ast.KillStmt,
		*ast.BinlogStmt,
		*ast.DropStatsStmt,
		*ast.SetRoleStmt,
		*ast.RevokeRoleStmt,
		*ast.SetDefaultRoleStmt,
		*ast.AdminStmt,
		*ast.GrantStmt,
		*ast.RevokeStmt,
		*ast.NonTransactionalDMLStmt,
		*ast.UseStmt:
		return nil
	// Renaming "cloud_admin@%" is not allowed.
	case *ast.RenameUserStmt:
		for _, userToUser := range s.UserToUsers {
			if userToUser.OldUser.Username == cloudAdminName && userToUser.OldUser.Hostname == "%" {
				return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause(fmt.Sprintf("RENAME USER %s", userToUser.OldUser))
			}
		}
		return nil
	case *ast.AlterInstanceStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ALTER INSTANCE")
	case *ast.ShutdownStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHUTDOWN")
	case *ast.BRIEStmt:
		return verifyBRIE(s)
	}
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause(fmt.Sprintf("Unsupported Executor %T", stmt))
}

func verifyBRIE(stmt *ast.BRIEStmt) error {
	switch stmt.Kind {
	case ast.BRIEKindBackup:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("BACKUP")
	case ast.BRIEKindRestore:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("RESTORE")
	}
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("BRIE")
}

func verifyShow(stmt *ast.ShowStmt) error {
	switch stmt.Tp {
	case
		ast.ShowNone,
		ast.ShowEngines,
		ast.ShowDatabases,
		ast.ShowTables,
		ast.ShowTableStatus,
		ast.ShowColumns,
		ast.ShowWarnings,
		ast.ShowCharset,
		ast.ShowVariables,
		ast.ShowStatus,
		ast.ShowCollation,
		ast.ShowCreateTable,
		ast.ShowCreateView,
		ast.ShowCreateUser,
		ast.ShowCreateSequence,
		ast.ShowGrants,
		ast.ShowTriggers,
		ast.ShowProcedureStatus,
		ast.ShowIndex,
		ast.ShowProcessList,
		ast.ShowCreateDatabase,
		ast.ShowEvents,
		ast.ShowStatsExtended,
		ast.ShowStatsMeta,
		ast.ShowStatsHistograms,
		ast.ShowStatsTopN,
		ast.ShowStatsBuckets,
		ast.ShowStatsHealthy,
		ast.ShowStatsLocked,
		ast.ShowHistogramsInFlight,
		ast.ShowColumnStatsUsage,
		ast.ShowProfile,
		ast.ShowProfiles,
		ast.ShowMasterStatus,
		ast.ShowPrivileges,
		ast.ShowErrors,
		ast.ShowBindings,
		ast.ShowBindingCacheStatus,
		ast.ShowOpenTables,
		ast.ShowAnalyzeStatus,
		ast.ShowBuiltins,
		ast.ShowTableNextRowId,
		ast.ShowImports,
		ast.ShowCreateImport,
		ast.ShowSessionStates,
		// "SHOW CONFIG" command is necessary for lightning to function,
		// therefore, it's access is restricted via privileges.
		ast.ShowConfig:
		return nil
	case ast.ShowCreateResourceGroup:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW CREATE RESOURCE GROUP")
	case ast.ShowCreatePlacementPolicy:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW CREATE PLACEMENT POLICY")
	case ast.ShowPlugins:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW PLUGINS")
	case ast.ShowDrainerStatus:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW DRAINER STATUS")
	case ast.ShowPumpStatus:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW PUMP STATUS")
	case ast.ShowBackups:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW BACKUPS")
	case ast.ShowRestores:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW RESTORES")
	case ast.ShowPlacement:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW PLACEMENT POLICY")
	case ast.ShowPlacementForDatabase:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW PLACEMENT FOR DATABASE")
	case ast.ShowPlacementForTable:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW PLACEMENT FOR TABLE")
	case ast.ShowPlacementForPartition:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW PLACEMENT FOR PARTITION")
	case ast.ShowPlacementLabels:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW PLACEMENT LABELS")
	case ast.ShowRegions:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW TABLE REGIONS")
	}
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("Unsupported SHOW type")
}

func verifyChange(stmt *ast.ChangeStmt) error {
	switch strings.ToUpper(stmt.NodeType) {
	case ast.PumpType:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("CHANGE PUMP")
	case ast.DrainerType:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("CHANGE DRAINER")
	}
	return nil
}

func verifyLoadStats(stmt *ast.LoadStatsStmt) error {
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("LOAD STATS")
}

func verifySplitRegion(stmt *ast.SplitRegionStmt) error {
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SPLIT REGION")
}

func verifyLoadData(stmt *ast.LoadDataStmt) error {
	if stmt.FileLocRef == ast.FileLocClient {
		return nil
	}
	// Only support load remote data from trusted sources.
	whiteList := config.GetGlobalConfig().Security.RemoteDataWhiteList
	for _, whiteListedPrefix := range whiteList {
		if strings.HasPrefix(stmt.Path, whiteListedPrefix) {
			return nil
		}
	}
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("LOAD DATA INFILE")
}

func verifyAdmin(stmt *ast.AdminStmt) error {
	switch stmt.Tp {
	case
		ast.AdminShowDDL,
		ast.AdminCheckTable,
		ast.AdminShowDDLJobs,
		ast.AdminCancelDDLJobs,
		ast.AdminCheckIndex,
		ast.AdminRecoverIndex,
		ast.AdminCleanupIndex,
		ast.AdminCheckIndexRange,
		ast.AdminShowDDLJobQueries,
		ast.AdminShowDDLJobQueriesWithRange,
		ast.AdminChecksumTable,
		ast.AdminShowNextRowID,
		ast.AdminReloadExprPushdownBlacklist,
		ast.AdminReloadOptRuleBlacklist,
		ast.AdminFlushBindings,
		ast.AdminCaptureBindings,
		ast.AdminEvolveBindings,
		ast.AdminReloadBindings,
		ast.AdminReloadStatistics,
		ast.AdminFlushPlanCache:
		return nil
	case ast.AdminPluginDisable:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ADMIN PLUGIN DISABLE")
	case ast.AdminPluginEnable:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ADMIN PLUGIN ENABLE")
	case ast.AdminShowSlow:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ADMIN SHOW SLOW")
	case ast.AdminShowTelemetry:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ADMIN SHOW TELEMETRY")
	case ast.AdminResetTelemetryID:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ADMIN RESET TELEMETRY ID")
	}
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause(fmt.Sprintf("Unsupported statement: %T", stmt))
}

func verifySetConfig(stmt *ast.SetConfigStmt) error {
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SET CONFIG")
}
