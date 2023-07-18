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

package config

import (
	"fmt"
	"strings"
)

// BootstrapControl contains ratelimit configuration options.
type BootstrapControl struct {
	SkipServerlessVariables bool `toml:"skip-serverless-variables" json:"skip-serverless-variables"`
	SkipRootPriv            bool `toml:"skip-root-priv" json:"skip-root-priv"`
	SkipCloudAdminPriv      bool `toml:"skip-cloud-admin-priv" json:"skip-cloud-admin-priv"`
	SkipRoleAdminPriv       bool `toml:"skip-role-admin-priv" json:"skip-role-admin-priv"`
	SkipPushdownBlacklist   bool `toml:"skip-pushdown-blacklist" json:"skip-pushdown-blacklist"`
}

// defaultBootstrapControl creates a new BootstrapControl.
func defaultBootstrapControl() BootstrapControl {
	return BootstrapControl{
		SkipServerlessVariables: false,
		SkipRootPriv:            false,
		SkipCloudAdminPriv:      false,
		SkipRoleAdminPriv:       false,
		SkipPushdownBlacklist:   false,
	}
}

// DefaultResourceGroup is the default resource group name for all txns and snapshots.
var DefaultResourceGroup string

// TiDBWorker is the config for TiDB worker.
type TiDBWorker struct {
	// Enable indicates whether to start the TiDB worker manager.
	Enable bool `toml:"enable" json:"enable"`
	// Role indicates the role of the TiDB worker.
	Role string `toml:"role" json:"role"`
	// Image specifies the desired image of the TiDB worker.
	TidbPool string `toml:"tidb-pool" json:"tidb-pool"`
	// RegistryAddr specifies the address of the TiDB worker service.
	RegistryAddr string `toml:"registry-addr" json:"registry-addr"`
}

const (
	// RoleMaster is the role for user tidb.
	RoleMaster = "master"
	// RoleGCWorker is the role for GC worker.
	RoleGCWorker = "gc"
	// RoleGCV2Worker is the role for GCV2 worker.
	RoleGCV2Worker = "gcv2"
)

// defaultTiDBWorker creates a new TiDBWorker.
func defaultTiDBWorker() TiDBWorker {
	return TiDBWorker{
		Enable:       false,
		Role:         RoleMaster,
		TidbPool:     "tidb-pool",
		RegistryAddr: "root:@tcp(serverless-cluster-tidb.tidb-serverless.svc:4000)/test",
	}
}

// Valid validates the TiDBWorker config.
func (w *TiDBWorker) Valid(c *Config) error {
	// Skip validation if TiDB worker is disabled.
	if !w.Enable {
		return nil
	}
	w.Role = strings.ToLower(w.Role)
	switch w.Role {
	case RoleMaster:
		if !c.EnableSafePointV2 {
			// When running as master without enabling SafePointV2, need to disable GC drop table.
			c.SkipGCDropTable = true
		}
	case RoleGCWorker:
		// Disable DDL when running as GC worker.
		c.Instance.TiDBEnableDDL.Store(false)
	case RoleGCV2Worker:
		// GCV2 worker must have SafePointV2 enabled.
		if !c.EnableSafePointV2 {
			return fmt.Errorf("must enable safe point V2 to run as GCV2 worker")
		}
		// Disable DDL when running as GCV2 worker.
		c.Instance.TiDBEnableDDL.Store(false)
	default:
		return fmt.Errorf("invalid tidb worker role %s", w.Role)
	}
	return nil
}
