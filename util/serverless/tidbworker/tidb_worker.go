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
	"context"
	"sync"

	"github.com/pingcap/tidb/sessionctx"

	workercli "github.com/tidbcloud/aws-shared-provider/pkg/tidbworker/client"
)

var (
	// GlobalTiDBWorkerManager is the global TiDB worker manager
	GlobalTiDBWorkerManager Manager
	// once is here to make sure the initialization of GlobalTiDBWorkerManager is done only once.
	once sync.Once
)

// Manager is used to manage TiDB worker.
type Manager interface {
	workercli.Client
	// InitializeGC registers all existing GC tasks to TiDB worker service.
	InitializeGC(ctx context.Context, sctx sessionctx.Context) error
	// InitializeGCV2 registers the initial GCV2 task to TiDB worker service, this is used to make sure
	// at least one GCV2 task exists in TiDB worker service.
	InitializeGCV2(ctx context.Context) error
	// Role returns the role of the TiDB worker.
	Role() string
}
