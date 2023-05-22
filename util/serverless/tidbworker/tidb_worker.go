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
	"sync"
	"time"
)

var (
	// GlobalTiDBWorkerManager is the global TiDB worker manage
	GlobalTiDBWorkerManager Manager
	// once is here to make sure the initialization of GlobalTiDBWorkerManager is done only once.
	once sync.Once
)

// KeyType represents the type of the key that's being registered.
// Different key types may have different logic to determine whether the TiDB worker is needed.
type KeyType string

const (
	// GCKey is the key type for GC.
	GCKey = "gc"
)

// Manager is used to manage TiDB worker.
type Manager interface {
	// IsWorker returns whether the current TiDB is a worker.
	IsWorker() bool
	// Close closes the manager.
	Close()

	// Register registers a key to the backend, which indicates that tidb worker is needed.
	Register(keyType KeyType, timestamp time.Time) error
	// Clear clears the appropriate key of the key type according to the given time.
	Clear(keyType KeyType, timestamp time.Time) error
	// Done returns whether no more tidb worker is needed for the given key type.
	Done(keyType KeyType) (bool, error)
}
