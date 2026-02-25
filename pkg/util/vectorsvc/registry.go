// Copyright 2025 PingCAP, Inc.
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

package vectorsvc

import (
	"sync"
)

var (
	mu        sync.RWMutex
	providers = make(map[string]TableDataProvider)
)

// Register registers a TableDataProvider for its table name.
// If a provider with the same table name already exists, it is replaced.
func Register(p TableDataProvider) {
	mu.Lock()
	defer mu.Unlock()
	providers[p.TableName()] = p
}

// Unregister removes a provider by table name.
func Unregister(tableName string) {
	mu.Lock()
	defer mu.Unlock()
	delete(providers, tableName)
}

// Get returns the provider for the given table name.
func Get(tableName string) (TableDataProvider, bool) {
	mu.RLock()
	defer mu.RUnlock()
	p, ok := providers[tableName]
	return p, ok
}

// List returns all registered providers.
func List() []TableDataProvider {
	mu.RLock()
	defer mu.RUnlock()
	result := make([]TableDataProvider, 0, len(providers))
	for _, p := range providers {
		result = append(result, p)
	}
	return result
}
