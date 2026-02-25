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
	"context"

	vectorsvcproto "github.com/pingcap/tidb/pkg/util/vectorsvc/proto/v1"
)

// TableDataProvider is the abstraction for system table data providers.
// Each system table (e.g. STATEMENTS_SUMMARY, SLOW_QUERY) implements this
// interface and registers itself with the global registry.
type TableDataProvider interface {
	// TableName returns the table name this provider is responsible for.
	TableName() string

	// QueryRows queries data and returns proto TableRow slices.
	QueryRows(ctx context.Context, req *vectorsvcproto.TableQuery) ([]*vectorsvcproto.TableRow, error)

	// Schema returns the table schema definition.
	Schema() *vectorsvcproto.TableSchema

	// Metadata returns table metadata (used by ListTables).
	Metadata() *vectorsvcproto.TableMetadata
}
