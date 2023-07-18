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
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/sessionctx"
	workercli "github.com/tidbcloud/aws-shared-provider/pkg/tidbworker/client"
	"go.uber.org/zap"
)

var _ Manager = &manager{}

type manager struct {
	client workercli.Client
	role   string
}

// InitManager initialize the global TiDB worker manager with the given backend.
func InitManager(ctx context.Context, keyspaceName string, cfg config.TiDBWorker) (err error) {
	once.Do(func() {
		var c workercli.Client
		c, err = workercli.NewClientWithContext(ctx, keyspaceName, cfg.TidbPool, cfg.RegistryAddr)
		if err != nil {
			log.Error("[tidb worker] failed to connect to tidb worker service", zap.Error(err))
			return
		}
		GlobalTiDBWorkerManager = &manager{
			client: c,
			role:   cfg.Role,
		}
	})
	return err
}

func (m *manager) InitializeGC(ctx context.Context, sctx sessionctx.Context) error {
	log.Info("[tidb-worker] initialize GC tasks")
	tasks, err := ddlutil.LoadDeleteRanges(ctx, sctx, math.MaxUint64)
	if err != nil {
		return errors.Trace(err)
	}
	for _, task := range tasks {
		err := m.RegisterGC(ctx, task.Ts)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (m *manager) InitializeGCV2(ctx context.Context) error {
	log.Info("[tidb-worker] initialize GCV2 tasks")
	// Use 0 as the timestamp to make sure this task can be cleaned by the completion of any other GCV2 task.
	err := m.RegisterGCV2(ctx, time.Now().Unix(), 0)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *manager) RegisterGC(ctx context.Context, ts uint64) error {
	log.Info("[tidb-worker] register a GC task to worker service", zap.Uint64("ts", ts))
	return m.client.RegisterGC(ctx, ts)
}

func (m *manager) RecycleGC(ctx context.Context, safePoint uint64) error {
	log.Info("[tidb-worker] notify worker service to recycle a GC task", zap.Uint64("safe-point", safePoint))
	return m.client.RecycleGC(ctx, safePoint)
}

func (m *manager) RegisterGCV2(ctx context.Context, gcLastRunTime int64, ts uint64) error {
	log.Info("[tidb-worker] register a GCV2 task to worker service",
		zap.Int64("gc-last-run-time", gcLastRunTime),
		zap.Uint64("ts", ts),
	)
	return m.client.RegisterGCV2(ctx, gcLastRunTime, ts)
}

func (m *manager) RecycleGCV2(ctx context.Context, safePoint uint64) error {
	log.Info("[tidb-worker] register a GCV2 task to worker service", zap.Uint64("safe-point", safePoint))
	return m.client.RecycleGCV2(ctx, safePoint)
}

func (m *manager) Role() string {
	return m.role
}
