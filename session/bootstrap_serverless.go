// Copyright 2022 PingCAP, Inc.
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

package session

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/keyspace"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	serverlessVersionVar = "serverless_version"
	// serverlessVersion2 added support for Disaggregated TiFlash, as a result, we can no safely enable mpp
	// and two previously forbidden executor push-down.
	serverlessVersion2 = 2
	// serverlessVersion3 adds json contains to tikv push-down-blacklist and adds reference_priv to cloud_admin.
	serverlessVersion3 = 3
	// serverlessVersion4 adds 6.4-6.6 newly added push-down-blacklist items.
	serverlessVersion4 = 4
	// serverlessVersion5 changes variable `tidb_stmt_summary_refresh_interval` and `tidb_stmt_summary_max_stmt_count`.
	serverlessVersion5 = 5
	// serverlessVersion6 fixes few push down executor name.
	serverlessVersion6 = 6
	// serverlessVersion7 disable async commit.
	serverlessVersion7 = 7
	// serverlessVersion8 adjusts push-down blacklist for CSE and TiFlash 6.5
	serverlessVersion8 = 8
	// serverlessVersion9 change variable `max_execution_time` to `30m`.
	serverlessVersion9 = 9
	// serverlessVersion10 disable 1pc.
	serverlessVersion10 = 10
	// serverlessVersion11 grants `cloud_admin` with the privilege that `grant 'role_admin' to <user>`
	serverlessVersion11 = 11
	// serverlessVersion12 creates missing 'role_admin' user.
	serverlessVersion12 = 12
	// serverlessVersion13 is marked as a no-op.
	serverlessVersion13 = 13
	// serverlessVersion14 reverts the change of serverlessVersion11.
	serverlessVersion14 = 14
	// serverlessVersion10 rename user cloud_admin to prefix.cloud_admin`.
	serverlessVersion15 = 15
)

const (
	// defaultMaxExecutionTime is the max execution time for serverless.
	defaultMaxExecutionTime = int(30 * time.Minute / time.Millisecond)
)

// currentServerlessVersion is defined as a variable, so we can modify its value for testing.
// please make sure this is the largest version
var currentServerlessVersion int64 = serverlessVersion15

var bootstrapServerlessVersion = []func(Session, int64){
	upgradeToServerlessVer2,
	upgradeToServerlessVer3,
	upgradeToServerlessVer4,
	upgradeToServerlessVer5,
	upgradeToServerlessVer6,
	upgradeToServerlessVer7,
	upgradeToServerlessVer8,
	upgradeToServerlessVer9,
	upgradeToServerlessVer10,
	upgradeToServerlessVer11,
	upgradeToServerlessVer12,
	upgradeToServerlessVer13,
	upgradeToServerlessVer14,
	upgradeToServerlessVer15,
}

// updateServerlessVersion updates serverless version variable in mysql.TiDB table.
func updateServerlessVersion(s Session) {
	// Update serverless version.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "Serverless bootstrap version. Do not delete.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, serverlessVersionVar, currentServerlessVersion, currentServerlessVersion,
	)
}

// getServerlessVersion gets serverless version from mysql.tidb table.
func getServerlessVersion(s Session) (int64, error) {
	sVal, isNull, err := getTiDBVar(s, serverlessVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		return 0, nil
	}
	return strconv.ParseInt(sVal, 10, 64)
}

// runServerlessUpgrade check and runs upgradeServerless functions on given store if necessary.
func runServerlessUpgrade(store kv.Storage) {
	s, err := createSession(store)
	if err != nil {
		// Bootstrap fail will cause program exit.
		logutil.BgLogger().Fatal("createSession error", zap.Error(err))
	}
	s.sessionVars.EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	s.SetValue(sessionctx.Initing, true)
	upgradeServerless(s)
	s.ClearValue(sessionctx.Initing)
}

// upgradeServerless execute some upgrade work if system is bootstrapped by tidb with lower serverless version.
func upgradeServerless(s Session) {
	ver, err := getServerlessVersion(s)
	terror.MustNil(err)

	if ver >= currentServerlessVersion {
		return
	}

	// Do upgrade works then update bootstrap version.
	for _, upgradeFunc := range bootstrapServerlessVersion {
		upgradeFunc(s, ver)
	}
	updateServerlessVersion(s)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	_, err = s.ExecuteInternal(ctx, "COMMIT")

	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("upgrade serverless version failed",
			zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if serverless version is already upgraded.
		v, err1 := getServerlessVersion(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("upgrade serverless version failed", zap.Error(err1))
		}
		if v >= currentServerlessVersion {
			// It is already bootstrapped/upgraded by a TiDB server with higher serverless version.
			return
		}
		logutil.BgLogger().Fatal("[Upgrade] upgrade serverless version failed",
			zap.Int64("from", ver),
			zap.Int64("to", currentServerlessVersion),
			zap.Error(err))
	}
}

// Serverless upgrade functions.
// NOTE: Upgrade functions below will only be executed on cluster that's already bootstrapped by a TiDB with lower serverless version,
// When applying changes, add more upgrade function instead of modifying them.
// Addition of them requires changes to serverless bootstrap procedures further below.

func upgradeToServerlessVer2(s Session, ver int64) {
	if ver >= serverlessVersion2 {
		return
	}

	// Enable mpp.
	mustExecute(s, "INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?;",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBAllowMPPExecution, variable.On, variable.On)

	// Remove lead/lag from pushdown_blacklist.
	mustExecute(s, "DELETE FROM mysql.expr_pushdown_blacklist where name in "+
		"(\"Lead\", \"Lag\") and store_type = \"tiflash\"")
}

func upgradeToServerlessVer3(s Session, ver int64) {
	if ver >= serverlessVersion3 {
		return
	}

	mustExecute(s, "INSERT HIGH_PRIORITY INTO mysql.expr_pushdown_blacklist VALUES"+
		"('json_contains','tikv', 'Compatibility with tikv 6.1')")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET References_priv='Y' WHERE User='cloud_admin' AND Host='%'")
}

func upgradeToServerlessVer4(s Session, ver int64) {
	if ver >= serverlessVersion4 {
		return
	}

	mustExecute(s, "INSERT HIGH_PRIORITY INTO mysql.expr_pushdown_blacklist VALUES"+
		"('json_valid','tikv', 'Compatibility with tikv 6.1'),"+
		"('json_unquote','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('json_extract','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('regexp','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('regexp_like','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('regexp_substr','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('regexp_instr','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('regexp_replace','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('Cast.CastJsonAsString','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('Extract.ExtractDuration','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('least.LeastString','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('greatest.GreatestString','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('unhex','tiflash', 'Compatibility with tiflash 6.1')",
	)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.expr_pushdown_blacklist"+
		" SET name='Cast.CastTimeAsDuration' WHERE name='Cast.ScalarFuncSig_CastTimeAsDuration' and store_type = 'tiflash'")
}

func upgradeToServerlessVer5(s Session, ver int64) {
	if ver >= serverlessVersion5 {
		return
	}
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBStmtSummaryRefreshInterval, 60, 60,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBStmtSummaryMaxStmtCount, 1000, 1000,
	)
}

func upgradeToServerlessVer6(s Session, ver int64) {
	if ver >= serverlessVersion6 {
		return
	}
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.expr_pushdown_blacklist"+
		" SET name='regexp_like' WHERE name='RegexpLike' and store_type = 'tikv'")

	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.expr_pushdown_blacklist"+
		" SET name='regexp_substr' WHERE name='RegexpSubstr' and store_type = 'tikv'")

	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.expr_pushdown_blacklist"+
		" SET name='regexp_instr' WHERE name='RegexpInStr' and store_type = 'tikv'")

	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.expr_pushdown_blacklist"+
		" SET name='regexp_replace' WHERE name='RegexpReplace' and store_type = 'tikv'")

	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.expr_pushdown_blacklist"+
		" SET name='get_format' WHERE name='GetFormat' and store_type = 'tiflash'")
}

func upgradeToServerlessVer7(s Session, ver int64) {
	if ver >= serverlessVersion7 {
		return
	}
	mustExecute(s, "set @@global.tidb_enable_async_commit=OFF;")
}

func upgradeToServerlessVer8(s Session, ver int64) {
	if ver >= serverlessVersion8 {
		return
	}

	// Remove executors from pushdown_blacklist that tiflash 6.5 supports.
	mustExecute(s, "DELETE FROM mysql.expr_pushdown_blacklist WHERE LOWER(name) IN "+
		"('hex',"+
		"'get_format',"+
		"'space',"+
		"'cast.casttimeasduration',"+
		"'reverse',"+
		"'elt',"+
		"'repeat',"+
		"'rightshift',"+
		"'leftshift',"+
		"'json_unquote',"+
		"'json_extract',"+
		"'regexp',"+
		"'regexp_like',"+
		"'regexp_substr',"+
		"'regexp_instr',"+
		"'cast.castjsonasstring',"+
		"'extract.extractduration') "+
		"AND LOWER(store_type) = 'tiflash'")

	// Remove executors from pushdown_blacklist that cse 6.5 supports.
	mustExecute(s, "DELETE FROM mysql.expr_pushdown_blacklist WHERE LOWER(name) IN "+
		"('regexp',"+
		"'regexp_like',"+
		"'regexp_substr',"+
		"'regexp_instr',"+
		"'regexp_replace',"+
		"'json_contains',"+
		"'json_valid') "+
		"AND LOWER(store_type) = 'tikv'")
}

func upgradeToServerlessVer9(s Session, ver int64) {
	if ver >= serverlessVersion9 {
		return
	}
	mustExecute(s, "set @@global.max_execution_time=%?", defaultMaxExecutionTime)
}

func upgradeToServerlessVer10(s Session, ver int64) {
	if ver >= serverlessVersion10 {
		return
	}

	mustExecute(s, "set @@global.tidb_enable_1pc=OFF")
}

func upgradeToServerlessVer11(s Session, ver int64) {
	if ver >= serverlessVersion11 {
		return
	}
	insertGlobalGrants(s, "cloud_admin", "ROLE_ADMIN", "Y")
}

func upgradeToServerlessVer12(s Session, ver int64) {
	if ver >= serverlessVersion12 {
		return
	}

	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.user SET `+
		`Host = "%", `+
		`User = "role_admin", `+
		`authentication_string = "", `+
		`plugin = "mysql_native_password", `+
		`Select_priv = "Y", `+
		`Insert_priv = "Y", `+
		`Update_priv = "Y", `+
		`Delete_priv = "Y", `+
		`Create_priv = "Y", `+
		`Drop_priv = "Y", `+
		`Process_priv = "Y", `+
		`Grant_priv = "Y", `+
		`References_priv = "Y", `+
		`Alter_priv = "Y", `+
		`Show_db_priv = "Y", `+
		`Super_priv = "Y", `+
		`Create_tmp_table_priv = "Y", `+
		`Lock_tables_priv = "Y", `+
		`Execute_priv = "Y", `+
		`Create_view_priv = "Y", `+
		`Show_view_priv = "Y", `+
		`Create_routine_priv = "Y", `+
		`Alter_routine_priv = "Y", `+
		`Index_priv = "Y", `+
		`Create_user_priv = "Y", `+
		`Event_priv = "Y", `+
		`Repl_slave_priv = "Y", `+
		`Repl_client_priv = "Y", `+
		`Trigger_priv = "Y", `+
		`Create_role_priv = "Y", `+
		`Drop_role_priv = "Y", `+
		`Account_locked = "Y", `+
		`Shutdown_priv = "N", `+
		`Reload_priv = "Y", `+
		`FILE_priv = "Y", `+
		`Config_priv = "N", `+
		`Create_Tablespace_Priv = "Y", `+
		`User_attributes = NULL, `+
		`Token_issuer = "";`,
	)

	// GRANT ROLE_ADMIN ON *.* to 'role_admin';
	insertGlobalGrants(s, "role_admin", "ROLE_ADMIN", "N")

	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.global_priv SET `+
		`Host = "%", `+
		`User = "role_admin", `+
		`Priv = "{}";`,
	)
}

func upgradeToServerlessVer13(s Session, ver int64) {
	if ver >= serverlessVersion13 {
		return
	}
	// no-op
}

func upgradeToServerlessVer14(s Session, ver int64) {
	if ver >= serverlessVersion14 {
		return
	}
	mustExecute(s, `DELETE FROM mysql.global_grants where `+
		`User = %? `+
		`AND Host = "%" `+
		`AND Priv = %?`,
		"cloud_admin",
		"ROLE_ADMIN",
	)
}

func upgradeToServerlessVer15(s Session, ver int64) {
	if ver >= serverlessVersion15 {
		return
	}
	if prefix := keyspace.GetKeyspaceNameBySettings(); prefix != "" {
		cloudAdminName := prefix + ".cloud_admin"
		mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET User=%? WHERE User='cloud_admin' AND Host='%'", cloudAdminName)
		mustExecute(s, "UPDATE HIGH_PRIORITY mysql.global_priv SET User=%? WHERE User='cloud_admin' AND Host='%'", cloudAdminName)
		mustExecute(s, "UPDATE HIGH_PRIORITY mysql.global_grants SET User=%? WHERE User='cloud_admin' AND Host='%'", cloudAdminName)
	}
}

// Serverless bootstrap procedures.
// NOTE: The following methods will only be executed once at doDMLWorks during TiDB Bootstrap,
// therefore any modification of it requires addition to the serverless version upgrade function above
// in order to cover those already bootstrapped.

// bootstrapServerlessPushdownBlacklist writes serverless expr pushdown blacklist into mysql.expr_pushdown_blacklist.
// This is required when using CSE and TiFlash at a lower version than TiDB.
func bootstrapServerlessPushdownBlacklist(s Session) {
	mustExecute(s, "INSERT HIGH_PRIORITY INTO mysql.expr_pushdown_blacklist VALUES "+
		"('regexp_replace','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('least.LeastString','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('greatest.GreatestString','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('unhex','tiflash', 'Compatibility with tiflash 6.1')",
	)
}

// bootstrapServerlessVariables writes serverless global variables into mysql.GLOBAL_VARIABLES.
func bootstrapServerlessVariables(s Session) {
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBAnalyzeVersion, 1, 1,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBRedactLog, variable.On, variable.On,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBStmtSummaryRefreshInterval, 60, 60,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBStmtSummaryMaxStmtCount, 1000, 1000,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBEnableAsyncCommit, variable.Off, variable.Off,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBEnable1PC, variable.Off, variable.Off,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable,
		variable.MaxExecutionTime,
		defaultMaxExecutionTime,
		defaultMaxExecutionTime,
	)
}

// bootstrapServerlessRoot writes root user's privilege into mysql.user.
// Not using grant/revoke statement because this is executed prior to privilege module load.
// Root username are passed as argument since it's prefixed by keyspace token.
func bootstrapServerlessRoot(s Session, userName string) {
	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.user SET `+
		`Host = "%", `+
		`User = %?, `+
		`authentication_string = "", `+
		`plugin = "mysql_native_password", `+
		`Select_priv = "Y", `+
		`Insert_priv = "Y", `+
		`Update_priv = "Y", `+
		`Delete_priv = "Y", `+
		`Create_priv = "Y", `+
		`Drop_priv = "Y", `+
		`Process_priv = "Y", `+
		`Grant_priv = "Y", `+
		`References_priv = "Y", `+
		`Alter_priv = "Y", `+
		`Show_db_priv = "Y", `+
		`Super_priv = "Y", `+
		`Create_tmp_table_priv = "Y", `+
		`Lock_tables_priv = "Y", `+
		`Execute_priv = "Y", `+
		`Create_view_priv = "Y", `+
		`Show_view_priv = "Y", `+
		`Create_routine_priv = "Y", `+
		`Alter_routine_priv = "Y", `+
		`Index_priv = "Y", `+
		`Create_user_priv = "Y", `+
		`Event_priv = "Y", `+
		`Repl_slave_priv = "Y", `+
		`Repl_client_priv = "Y", `+
		`Trigger_priv = "Y", `+
		`Create_role_priv = "Y", `+
		`Drop_role_priv = "Y", `+
		`Account_locked = "N", `+
		`Shutdown_priv = "N", `+ // REVOKE SHUTDOWN ON *.* FROM root;
		`Reload_priv = "Y", `+
		`FILE_priv = "Y", `+
		`Config_priv = "N", `+ // REVOKE CONFIG ON *.* FROM root;
		`Create_Tablespace_Priv = "Y", `+
		`User_attributes = NULL, `+
		`Token_issuer = "" `,
		userName,
	)
}

// bootstrapCloudAdmin creates user cloud_admin and configure its privilege into mysql.user.
func bootstrapCloudAdmin(s Session, userName string) {
	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.user SET `+
		`Host = "%", `+
		`User = "%?", `+
		`authentication_string = "", `+
		`plugin = "mysql_native_password", `+
		`Select_priv = "Y", `+
		`Insert_priv = "Y", `+
		`Update_priv = "Y", `+
		`Delete_priv = "Y", `+
		`Create_priv = "Y", `+
		`Drop_priv = "Y", `+
		`Process_priv = "Y", `+
		`Grant_priv = "N", `+
		`References_priv = "Y", `+
		`Alter_priv = "Y", `+
		`Show_db_priv = "Y", `+
		`Super_priv = "N", `+
		`Create_tmp_table_priv = "N", `+
		`Lock_tables_priv = "N", `+
		`Execute_priv = "N", `+
		`Create_view_priv = "Y", `+
		`Show_view_priv = "N", `+
		`Create_routine_priv = "N", `+
		`Alter_routine_priv = "N", `+
		`Index_priv = "Y", `+
		`Create_user_priv = "Y", `+
		`Event_priv = "N", `+
		`Repl_slave_priv = "N", `+
		`Repl_client_priv = "N", `+
		`Trigger_priv = "N", `+
		`Create_role_priv = "Y", `+
		`Drop_role_priv = "N", `+
		`Account_locked = "N", `+
		`Shutdown_priv = "Y", `+
		`Reload_priv = "Y", `+
		`FILE_priv = "N", `+
		`Config_priv = "Y", `+
		`Create_Tablespace_Priv = "N", `+
		`User_attributes = NULL, `+
		`Token_issuer = "" `,
		userName,
	)

	insertGlobalGrants(s, userName, "DASHBOARD_CLIENT", "N")
	insertGlobalGrants(s, userName, "SYSTEM_VARIABLES_ADMIN", "N")
	insertGlobalGrants(s, userName, "CONNECTION_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTRICTED_VARIABLES_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTRICTED_STATUS_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTRICTED_CONNECTION_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTRICTED_USER_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTRICTED_TABLES_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTRICTED_REPLICA_WRITER_ADMIN", "N")
	insertGlobalGrants(s, userName, "BACKUP_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTORE_ADMIN", "N")
	insertGlobalGrants(s, userName, "SYSTEM_USER", "Y")

	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.global_priv SET `+
		`Host = "%", `+
		`User = "%?", `+
		`Priv = "{}"`,
		userName,
	)
}

// bootstrapRoleAdmin creates user role_admin and configure its privileges.
func bootstrapRoleAdmin(s Session) {
	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.user SET `+
		`Host = "%", `+
		`User = "role_admin", `+
		`authentication_string = "", `+
		`plugin = "mysql_native_password", `+
		`Select_priv = "Y", `+
		`Insert_priv = "Y", `+
		`Update_priv = "Y", `+
		`Delete_priv = "Y", `+
		`Create_priv = "Y", `+
		`Drop_priv = "Y", `+
		`Process_priv = "Y", `+
		`Grant_priv = "Y", `+
		`References_priv = "Y", `+
		`Alter_priv = "Y", `+
		`Show_db_priv = "Y", `+
		`Super_priv = "Y", `+
		`Create_tmp_table_priv = "Y", `+
		`Lock_tables_priv = "Y", `+
		`Execute_priv = "Y", `+
		`Create_view_priv = "Y", `+
		`Show_view_priv = "Y", `+
		`Create_routine_priv = "Y", `+
		`Alter_routine_priv = "Y", `+
		`Index_priv = "Y", `+
		`Create_user_priv = "Y", `+
		`Event_priv = "Y", `+
		`Repl_slave_priv = "Y", `+
		`Repl_client_priv = "Y", `+
		`Trigger_priv = "Y", `+
		`Create_role_priv = "Y", `+
		`Drop_role_priv = "Y", `+
		`Account_locked = "Y", `+
		`Shutdown_priv = "N", `+
		`Reload_priv = "Y", `+
		`FILE_priv = "Y", `+
		`Config_priv = "N", `+
		`Create_Tablespace_Priv = "Y", `+
		`User_attributes = NULL, `+
		`Token_issuer = "";`,
	)

	// GRANT ROLE_ADMIN ON *.* to 'role_admin';
	insertGlobalGrants(s, "role_admin", "ROLE_ADMIN", "N")

	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.global_priv SET `+
		`Host = "%", `+
		`User = "role_admin", `+
		`Priv = "{}";`,
	)
}

// insertGlobalGrants inserts user's privilege into mysql.global_grants.
func insertGlobalGrants(s Session, userName, priv, grant string) {
	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.global_grants SET `+
		`USER = %?, `+
		`HOST = "%", `+
		`PRIV = %?, `+
		`WITH_GRANT_OPTION = %?`,
		userName,
		priv,
		grant,
	)
}
