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
	"encoding/base64"
	"fmt"

	"github.com/pingcap/tidb/pkg/util/logutil"
	vectorsvcproto "github.com/pingcap/tidb/pkg/util/vectorsvc/proto/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// DefaultBatchSize is the default number of rows per streaming response.
	DefaultBatchSize = 100
)

// PullService implements the SystemTablePullServiceServer interface.
// It dispatches requests to registered TableDataProviders.
type PullService struct {
	vectorsvcproto.UnimplementedSystemTablePullServiceServer

	clusterID  string
	instanceID string
}

// NewPullService creates a new generic PullService.
func NewPullService(clusterID, instanceID string) *PullService {
	return &PullService{
		clusterID:  clusterID,
		instanceID: instanceID,
	}
}

// QueryTable implements the SystemTablePullServiceServer interface.
// It streams table rows in batches by dispatching to the appropriate provider.
func (s *PullService) QueryTable(req *vectorsvcproto.TableQuery, stream grpc.ServerStreamingServer[vectorsvcproto.TableQueryResponse]) error {
	logutil.BgLogger().Info("QueryTable called",
		zap.String("table", req.Table),
		zap.String("where", req.GetWhere()),
		zap.Int32("limit", req.GetLimit()),
		zap.Int32("offset", req.GetOffset()))

	provider, ok := Get(req.Table)
	if !ok {
		return status.Errorf(codes.NotFound, "table '%s' not found", req.Table)
	}

	rows, err := provider.QueryRows(stream.Context(), req)
	if err != nil {
		return status.Errorf(codes.Internal, "query failed: %v", err)
	}

	totalCount := len(rows)

	// Send schema in first response
	schema := provider.Schema()
	if err := stream.Send(&vectorsvcproto.TableQueryResponse{
		Schema:     schema,
		Rows:       nil,
		TotalCount: int64(totalCount),
		HasMore:    len(rows) > 0,
	}); err != nil {
		return err
	}

	// Send rows in batches
	for i := 0; i < len(rows); i += DefaultBatchSize {
		end := i + DefaultBatchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[i:end]
		nextCursor := ""
		hasMore := end < len(rows)
		if hasMore {
			nextCursor = encodeCursor(end)
		}

		if err := stream.Send(&vectorsvcproto.TableQueryResponse{
			Rows:       batch,
			NextCursor: &nextCursor,
			HasMore:    hasMore,
			TotalCount: int64(totalCount),
		}); err != nil {
			return err
		}
	}

	logutil.BgLogger().Info("QueryTable completed",
		zap.String("table", req.Table),
		zap.Int("rows_sent", len(rows)),
		zap.Int64("total_count", int64(totalCount)))

	return nil
}

// ListTables lists available tables from all registered providers.
func (s *PullService) ListTables(_ context.Context, req *vectorsvcproto.ListTablesRequest) (*vectorsvcproto.ListTablesResponse, error) {
	logutil.BgLogger().Info("ListTables called",
		zap.String("pattern", SafeDeref(req.Pattern)))

	allProviders := List()
	tables := make([]*vectorsvcproto.TableMetadata, 0, len(allProviders))
	for _, p := range allProviders {
		tables = append(tables, p.Metadata())
	}

	// Apply pattern filter if specified
	if req.Pattern != nil {
		tables = filterTablesByPattern(tables, *req.Pattern)
	}

	return &vectorsvcproto.ListTablesResponse{
		Tables: tables,
	}, nil
}

// DescribeTable returns the schema of a table.
func (s *PullService) DescribeTable(_ context.Context, req *vectorsvcproto.DescribeTableRequest) (*vectorsvcproto.DescribeTableResponse, error) {
	logutil.BgLogger().Info("DescribeTable called",
		zap.String("table", req.Table))

	provider, ok := Get(req.Table)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table '%s' not found", req.Table)
	}

	return &vectorsvcproto.DescribeTableResponse{
		Schema: provider.Schema(),
	}, nil
}

// filterTablesByPattern filters tables by a LIKE pattern.
func filterTablesByPattern(tables []*vectorsvcproto.TableMetadata, pattern string) []*vectorsvcproto.TableMetadata {
	result := make([]*vectorsvcproto.TableMetadata, 0)
	for _, table := range tables {
		matched, _ := CompareLike(table.Name, pattern)
		if matched {
			result = append(result, table)
		}
	}
	return result
}

// encodeCursor encodes pagination information into a cursor string.
func encodeCursor(offset int) string {
	data := fmt.Sprintf("%d", offset)
	return base64.StdEncoding.EncodeToString([]byte(data))
}

// SafeDeref safely dereferences a string pointer, returning empty string for nil.
func SafeDeref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
