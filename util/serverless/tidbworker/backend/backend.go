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

package backend

import (
	"context"
	"time"
)

// Backend is the interface for the backend storage for tidb worker keys.
type Backend interface {
	// Set register a key to the backend with a timestamp.
	Set(ctx context.Context, KeyType string, timestamp time.Time) error
	// DelBefore deletes keys with timestamp less than the given time.
	DelBefore(ctx context.Context, KeyType string, timestamp time.Time) error
	// Empty returns whether there are no key left of the given key type.
	Empty(ctx context.Context, KeyType string) (bool, error)
	// Close closes the backend.
	Close() error
}
