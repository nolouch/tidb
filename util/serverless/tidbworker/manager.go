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
	"os"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/serverless/tidbworker/backend"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var _ Manager = &manager{}

const (
	// workerCheckInterval is the interval between two checks of whether the worker is still needed.
	workerCheckInterval = 1 * time.Minute
)

type manager struct {
	ctx        context.Context
	cancel     context.CancelFunc
	backend    backend.Backend
	keyspaceID uint32
	isWorker   bool

	// timeWindow specify that if the last register time is within this time window, we don't need to register again.
	// Note that it's calculated separately for gc and ddl.
	timeWindow time.Duration
	// lastRegister holds last register time for each key type.
	// It's used to avoid unnecessary register operations within the time Window.
	keyInfos map[KeyType]*keyInfo
	// mu protects keyInfo.
	mu sync.Mutex
}

// keyInfo holds information about each key type.
type keyInfo struct {
	lastRegister time.Time
}

// InitManager initialize the global TiDB worker manager with the given backend.
func InitManager(keyspaceID tikv.KeyspaceID, cfg config.TiDBWorker, backend backend.Backend) error {
	once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		m := &manager{
			ctx:        ctx,
			cancel:     cancel,
			backend:    backend,
			keyspaceID: uint32(keyspaceID),
			isWorker:   cfg.IsWorker,
			timeWindow: time.Duration(cfg.TimeWindowSeconds) * time.Second,
			keyInfos:   make(map[KeyType]*keyInfo),
		}
		// Initialize all supported key types.
		m.keyInfos[GCKey] = &keyInfo{}
		logutil.BgLogger().Info("[tidb worker] initialize tidb worker manager",
			zap.Bool("isWorker", m.isWorker),
			zap.Duration("timeWindow", m.timeWindow),
		)
		GlobalTiDBWorkerManager = m
		if m.isWorker {
			go m.workerLoop()
		}
	})
	return nil
}

// IsWorker returns whether the current TiDB is a worker.
func (m *manager) IsWorker() bool {
	return m.isWorker
}

// Close closes the manager and the underlying storage backend.
func (m *manager) Close() {
	if GlobalTiDBWorkerManager == nil {
		return
	}
	if m.backend != nil {
		terror.Log(errors.Trace(m.backend.Close()))
	}
	if m.cancel != nil {
		m.cancel()
	}
}

// Register registers the given key type with the given timestamp.
// It also records the last register time for each key type to avoid unnecessary register operations
// within the time window.
func (m *manager) Register(keyType KeyType, timestamp time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	keyInfo, ok := m.keyInfos[keyType]
	if !ok {
		logutil.BgLogger().Error("[tidb worker] unsupported key type", zap.String("keyType", string(keyType)))
		return errors.Errorf("unsupported key type %v", keyType)
	}
	// Check whether the last register time is within the time window, if so, skip register.
	if keyInfo.lastRegister.Add(m.timeWindow).After(timestamp) {
		logutil.BgLogger().Info("[tidb worker] register key within time window, skip",
			zap.String("keyType", string(keyType)),
			zap.Time("lastRegisterTime", keyInfo.lastRegister),
			zap.Int64("timestamp", timestamp.Unix()),
			zap.Duration("timeWindow", m.timeWindow),
		)
		return nil
	}

	// Register key and update last register time.
	if err := m.backend.Set(m.ctx, string(keyType), timestamp); err != nil {
		logutil.BgLogger().Error("[tidb worker] register key failed",
			zap.String("keyType", string(keyType)),
			zap.Int64("timestamp", timestamp.Unix()),
			zap.Error(err),
		)
		return err
	}
	keyInfo.lastRegister = timestamp
	logutil.BgLogger().Info("[tidb worker] register key success",
		zap.String("keyType", string(keyType)),
		zap.Int64("timestamp", timestamp.Unix()),
	)
	return nil
}

// Clear all registered keys of the given key type with timestamp before the given.
func (m *manager) Clear(keyType KeyType, timestamp time.Time) error {
	// It's only safe the clear the keys a time window before.
	timestamp = timestamp.Add(-m.timeWindow)
	if err := m.backend.DelBefore(m.ctx, string(keyType), timestamp); err != nil {
		logutil.BgLogger().Error("[tidb worker] clear key failed",
			zap.String("keyType", string(keyType)),
			zap.Int64("timestamp", timestamp.Unix()),
			zap.Error(err),
		)
		return err
	}
	logutil.BgLogger().Info("[tidb worker] clear key success",
		zap.String("keyType", string(keyType)),
		zap.Int64("timestamp", timestamp.Unix()),
	)
	return nil
}

// Done checks whether the given key type has no registered key.
func (m *manager) Done(keyType KeyType) (bool, error) {
	empty, err := m.backend.Empty(m.ctx, string(keyType))
	if err != nil {
		logutil.BgLogger().Error("[tidb worker] check whether key is empty failed",
			zap.String("keyType", string(keyType)),
			zap.Error(err),
		)
		return false, err
	}
	if !empty {
		logutil.BgLogger().Info("[tidb worker] key is not empty",
			zap.String("keyType", string(keyType)),
		)
		return false, nil
	}
	logutil.BgLogger().Info("[tidb worker] key is empty",
		zap.String("keyType", string(keyType)),
	)
	return true, nil
}

// workerLoop is called when tidb is started as a worker,
// it periodically calls GCDone and DDLDone, if there are both finished, then calls os.Exit(0).
func (m *manager) workerLoop() {
	ticker := time.NewTicker(workerCheckInterval)
	defer ticker.Stop()
	logutil.BgLogger().Info("[tidb worker] start tidb worker loop")
	for {
		select {
		case <-m.ctx.Done():
			logutil.BgLogger().Info("[tidb worker] context done, exiting worker loop")
			return
		case <-ticker.C:
			gcDone, err := m.Done(GCKey)
			if err != nil {
				logutil.BgLogger().Error("[tidb worker] check GCDone failed", zap.Error(err))
			}
			if gcDone {
				logutil.BgLogger().Info("[tidb worker] no job left, exit")
				os.Exit(0)
			}

			logutil.BgLogger().Info("[tidb worker] gc job not finished yet, keep tidb worker alive")
		}
	}

}
