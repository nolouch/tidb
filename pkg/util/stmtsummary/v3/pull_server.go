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

package stmtsummaryv3

import (
	"context"
	"encoding/base64"
	"fmt"

	stmtsummaryv3proto "github.com/pingcap/tidb/pkg/util/stmtsummary/v3/proto/v1"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// StatementsTable is the name of the statements table.
	StatementsTable = "STATEMENTS_SUMMARY"
	// DefaultBatchSize is the default number of rows per streaming response.
	DefaultBatchSize = 100
)

// PullServer handles pull requests for statement data.
type PullServer struct {
	aggregator *Aggregator
	clusterID  string
	instanceID string

	// Query parser
	parser *QueryParser
}

// NewPullServer creates a new PullServer.
func NewPullServer(aggregator *Aggregator, clusterID, instanceID string) *PullServer {
	return &PullServer{
		aggregator: aggregator,
		clusterID:  clusterID,
		instanceID: instanceID,
		parser:     NewQueryParser(),
	}
}

// QueryTable implements the SystemTablePullServiceServer interface.
// It streams table rows in batches.
func (s *PullServer) QueryTable(req *stmtsummaryv3proto.TableQuery, stream grpc.ServerStreamingServer[stmtsummaryv3proto.TableQueryResponse]) error {
	logutil.BgLogger().Info("QueryTable called",
		zap.String("table", req.Table),
		zap.String("where", req.GetWhere()),
		zap.Int32("limit", req.GetLimit()),
		zap.Int32("offset", req.GetOffset()))

	// Only support STATEMENTS_SUMMARY table
	if req.Table != StatementsTable {
		return status.Errorf(codes.NotFound, "table '%s' not found", req.Table)
	}

	// Parse WHERE clause
	var whereExpr *WhereExpression
	if req.Where != nil {
		expr, err := s.parser.ParseWhere(*req.Where)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid WHERE clause: %v", err)
		}
		whereExpr = expr
	}

	// Get data from aggregator
	stats := s.aggregator.GetCurrentStats()

	// Filter and convert to rows
	rows, totalCount := s.statsToRows(stats, whereExpr)

	// Send schema in first response
	schema := s.buildSchema()
	if err := stream.Send(&stmtsummaryv3proto.TableQueryResponse{
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
			// Generate cursor for next page
			nextCursor = s.encodeCursor(end)
		}

		if err := stream.Send(&stmtsummaryv3proto.TableQueryResponse{
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

// ListTables lists available tables.
func (s *PullServer) ListTables(ctx context.Context, req *stmtsummaryv3proto.ListTablesRequest) (*stmtsummaryv3proto.ListTablesResponse, error) {
	logutil.BgLogger().Info("ListTables called",
		zap.String("pattern", safeDeref(req.Pattern)))

	tables := []*stmtsummaryv3proto.TableMetadata{
		{
			Name:              StatementsTable,
			Comment:           "Aggregated SQL statement execution statistics",
			SupportsTimeRange: true,
			SupportsPush:      true,
			EstimatedRows:     int64(len(s.aggregator.GetCurrentStats())),
		},
	}

	// Apply pattern filter if specified
	if req.Pattern != nil {
		tables = s.filterTablesByPattern(tables, *req.Pattern)
	}

	return &stmtsummaryv3proto.ListTablesResponse{
		Tables: tables,
	}, nil
}

// DescribeTable returns the schema of a table.
func (s *PullServer) DescribeTable(ctx context.Context, req *stmtsummaryv3proto.DescribeTableRequest) (*stmtsummaryv3proto.DescribeTableResponse, error) {
	logutil.BgLogger().Info("DescribeTable called",
		zap.String("table", req.Table))

	if req.Table != StatementsTable {
		return nil, status.Errorf(codes.NotFound, "table '%s' not found", req.Table)
	}

	return &stmtsummaryv3proto.DescribeTableResponse{
		Schema: s.buildSchema(),
	}, nil
}

// statsToRows converts aggregated stats to table rows.
func (s *PullServer) statsToRows(stats []*StmtStats, where *WhereExpression) ([]*stmtsummaryv3proto.TableRow, int) {
	rows := make([]*stmtsummaryv3proto.TableRow, 0, len(stats))

	for _, stat := range stats {
		// Convert stat to map
		rowMap := s.statToMap(stat)

		// Apply WHERE filter
		if where != nil {
			match, err := where.Evaluate(rowMap)
			if err != nil {
				logutil.BgLogger().Warn("error evaluating WHERE clause",
					zap.String("digest", stat.Key.Digest),
					zap.Error(err))
				continue
			}
			if !match {
				continue
			}
		}

		// Convert to TableRow with all columns in default order
		row := &stmtsummaryv3proto.TableRow{
			Values: make([]*stmtsummaryv3proto.Value, 0),
		}

		allCols := getAllStatementColumns()
		for _, col := range allCols {
			if val, ok := rowMap[col]; ok {
				row.Values = append(row.Values, s.mapValueToProto(val))
			} else {
				// Null value for missing column
				row.Values = append(row.Values, &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_NullVal{NullVal: stmtsummaryv3proto.NullValue_NULL}})
			}
		}

		rows = append(rows, row)
	}

	return rows, len(stats)
}

// statToMap converts a StmtStats to a map for filtering.
func (s *PullServer) statToMap(stat *StmtStats) map[string]interface{} {
	p50, p95, p99 := GetPercentiles(stat.LatencyHistogram)

	return map[string]interface{}{
		"digest":           stat.Key.Digest,
		"plan_digest":      stat.Key.PlanDigest,
		"schema_name":      stat.Key.SchemaName,
		"normalized_sql":   stat.NormalizedSQL,
		"table_names":      stat.TableNames,
		"stmt_type":        stat.StmtType,
		"sample_sql":       stat.SampleSQL,
		"sample_plan":      stat.SamplePlan,
		"prev_sql":         stat.PrevSQL,
		"exec_count":       stat.ExecCount,
		"sum_errors":       stat.SumErrors,
		"sum_warnings":     stat.SumWarnings,
		"sum_latency_us":   stat.SumLatency.Microseconds(),
		"max_latency_us":   stat.MaxLatency.Microseconds(),
		"min_latency_us":   stat.MinLatency.Microseconds(),
		"avg_latency_us":   stat.SumLatency.Microseconds() / max64(stat.ExecCount, 1),
		"p50_latency_us":   p50,
		"p95_latency_us":   p95,
		"p99_latency_us":   p99,
		"sum_mem_bytes":    stat.SumMemBytes,
		"max_mem_bytes":    stat.MaxMemBytes,
		"sum_disk_bytes":   stat.SumDiskBytes,
		"max_disk_bytes":   stat.MaxDiskBytes,
		"sum_tidb_cpu_us":  stat.SumTiDBCPU.Microseconds(),
		"sum_tikv_cpu_us":  stat.SumTiKVCPU.Microseconds(),
		"first_seen_ms":    stat.FirstSeen.UnixMilli(),
		"last_seen_ms":     stat.LastSeen.UnixMilli(),
		"is_internal":      stat.IsInternal,
		"prepared":         stat.Prepared,
		"keyspace_name":    stat.KeyspaceName,
		"resource_group":   stat.Key.ResourceGroupName,
	}
}

// mapValueToProto converts a map value to proto Value.
func (s *PullServer) mapValueToProto(value interface{}) *stmtsummaryv3proto.Value {
	switch v := value.(type) {
	case string:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_StringVal{StringVal: v}}
	case int:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Int64Val{Int64Val: int64(v)}}
	case int64:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Int64Val{Int64Val: v}}
	case uint:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Uint64Val{Uint64Val: uint64(v)}}
	case uint32:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Uint64Val{Uint64Val: uint64(v)}}
	case uint64:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_Uint64Val{Uint64Val: v}}
	case bool:
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_BoolVal{BoolVal: v}}
	default:
		// Fallback to string
		return &stmtsummaryv3proto.Value{Kind: &stmtsummaryv3proto.Value_StringVal{StringVal: fmt.Sprintf("%v", v)}}
	}
}

// buildSchema builds the table schema.
func (s *PullServer) buildSchema() *stmtsummaryv3proto.TableSchema {
	columns := make([]*stmtsummaryv3proto.Column, 0)
	fieldTypes := map[string]stmtsummaryv3proto.DataType{
		"digest":           stmtsummaryv3proto.DataType_STRING,
		"plan_digest":      stmtsummaryv3proto.DataType_STRING,
		"schema_name":      stmtsummaryv3proto.DataType_STRING,
		"normalized_sql":   stmtsummaryv3proto.DataType_STRING,
		"table_names":      stmtsummaryv3proto.DataType_STRING,
		"stmt_type":        stmtsummaryv3proto.DataType_STRING,
		"sample_sql":       stmtsummaryv3proto.DataType_STRING,
		"sample_plan":      stmtsummaryv3proto.DataType_STRING,
		"prev_sql":         stmtsummaryv3proto.DataType_STRING,
		"exec_count":       stmtsummaryv3proto.DataType_INT64,
		"sum_errors":       stmtsummaryv3proto.DataType_INT64,
		"sum_warnings":     stmtsummaryv3proto.DataType_INT64,
		"sum_latency_us":   stmtsummaryv3proto.DataType_INT64,
		"max_latency_us":   stmtsummaryv3proto.DataType_INT64,
		"min_latency_us":   stmtsummaryv3proto.DataType_INT64,
		"avg_latency_us":   stmtsummaryv3proto.DataType_INT64,
		"p50_latency_us":   stmtsummaryv3proto.DataType_INT64,
		"p95_latency_us":   stmtsummaryv3proto.DataType_INT64,
		"p99_latency_us":   stmtsummaryv3proto.DataType_INT64,
		"sum_mem_bytes":    stmtsummaryv3proto.DataType_INT64,
		"max_mem_bytes":    stmtsummaryv3proto.DataType_INT64,
		"sum_disk_bytes":   stmtsummaryv3proto.DataType_INT64,
		"max_disk_bytes":   stmtsummaryv3proto.DataType_INT64,
		"sum_tidb_cpu_us":  stmtsummaryv3proto.DataType_INT64,
		"sum_tikv_cpu_us":  stmtsummaryv3proto.DataType_INT64,
		"first_seen_ms":    stmtsummaryv3proto.DataType_TIMESTAMP,
		"last_seen_ms":     stmtsummaryv3proto.DataType_TIMESTAMP,
		"is_internal":      stmtsummaryv3proto.DataType_BOOL,
		"prepared":         stmtsummaryv3proto.DataType_BOOL,
		"keyspace_name":    stmtsummaryv3proto.DataType_STRING,
		"resource_group":   stmtsummaryv3proto.DataType_STRING,
	}

	allCols := getAllStatementColumns()
	for i, col := range allCols {
		dataType := stmtsummaryv3proto.DataType_UNKNOWN
		if dt, ok := fieldTypes[col]; ok {
			dataType = dt
		}

		columns = append(columns, &stmtsummaryv3proto.Column{
			Name:     col,
			Ordinal:  int32(i),
			Type:     dataType,
			Nullable: col != "digest", // digest is required
		})
	}

	return &stmtsummaryv3proto.TableSchema{
		TableName: StatementsTable,
		Columns:   columns,
	}
}

// filterTablesByPattern filters tables by a LIKE pattern.
func (s *PullServer) filterTablesByPattern(tables []*stmtsummaryv3proto.TableMetadata, pattern string) []*stmtsummaryv3proto.TableMetadata {
	result := make([]*stmtsummaryv3proto.TableMetadata, 0)

	for _, table := range tables {
		matched, _ := compareLike(table.Name, pattern)
		if matched {
			result = append(result, table)
		}
	}

	return result
}

// encodeCursor encodes pagination information into a cursor string.
func (s *PullServer) encodeCursor(offset int) string {
	data := fmt.Sprintf("%d", offset)
	return base64.StdEncoding.EncodeToString([]byte(data))
}

// decodeCursor decodes a cursor string.
func (s *PullServer) decodeCursor(cursor string) (offset int, err error) {
	data, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}

	n, err := fmt.Sscanf(string(data), "%d", &offset)
	if err != nil || n != 1 {
		return 0, fmt.Errorf("invalid cursor format")
	}

	return offset, nil
}

// getAllStatementColumns returns all column names for the statements table.
func getAllStatementColumns() []string {
	return []string{
		"digest",
		"plan_digest",
		"schema_name",
		"normalized_sql",
		"table_names",
		"stmt_type",
		"sample_sql",
		"sample_plan",
		"prev_sql",
		"exec_count",
		"sum_errors",
		"sum_warnings",
		"sum_latency_us",
		"max_latency_us",
		"min_latency_us",
		"avg_latency_us",
		"p50_latency_us",
		"p95_latency_us",
		"p99_latency_us",
		"sum_mem_bytes",
		"max_mem_bytes",
		"sum_disk_bytes",
		"max_disk_bytes",
		"sum_tidb_cpu_us",
		"sum_tikv_cpu_us",
		"first_seen_ms",
		"last_seen_ms",
		"is_internal",
		"prepared",
		"keyspace_name",
		"resource_group",
	}
}

func safeDeref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
