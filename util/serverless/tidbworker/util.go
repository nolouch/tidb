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

package tidbworker

import (
	"github.com/pingcap/tidb/config"
)

// IsMaster returns whether the current TiDB is of role master.
func IsMaster() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleMaster
}

// IsGCWorker returns whether the current TiDB is a GC worker.
func IsGCWorker() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleGCWorker
}

// IsGCV2Worker returns whether the current TiDB is a GCV2 worker.
func IsGCV2Worker() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleGCV2Worker
}
