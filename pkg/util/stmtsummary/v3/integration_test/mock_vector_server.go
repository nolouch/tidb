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

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	stmtsummaryv3proto "github.com/pingcap/tidb/pkg/util/vectorsvc/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MockVectorPushService simulates Vector's push data receiver.
// Vector listens on this service for TiDB to push statement batches.
type MockVectorPushService struct {
	stmtsummaryv3proto.UnimplementedSystemTablePushServiceServer

	mu             sync.Mutex
	batches        []*stmtsummaryv3proto.StatementBatch
	rowBatches     []*stmtsummaryv3proto.TableRowBatch
	batchCount     atomic.Int64
	statementCount atomic.Int64

	// SQL verification: map of digest -> expected SQL pattern
	expectedSQLs map[string]string
	// Track which digests we've seen
	seenDigests map[string]int
}

// SetExpectedSQLs sets the expected SQL patterns for verification.
// Call this before running the test to pre-populate expected SQLs.
func (s *MockVectorPushService) SetExpectedSQLs(sqls map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expectedSQLs = sqls
	s.seenDigests = make(map[string]int)
	log.Printf("[MockVector] Set %d expected SQL patterns for verification", len(sqls))
}

// VerifyExpectedSQLs checks if all expected SQLs were received.
func (s *MockVectorPushService) VerifyExpectedSQLs() (missing []string, allSeen bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	allSeen = true
	for digest, pattern := range s.expectedSQLs {
		count := s.seenDigests[digest]
		if count == 0 {
			missing = append(missing, fmt.Sprintf("digest=%s pattern=%s", digest, pattern))
			allSeen = false
		}
	}
	return
}

// GetReceivedSQLs returns a summary of received SQL statements grouped by digest.
func (s *MockVectorPushService) GetReceivedSQLs() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[string]int)
	for k, v := range s.seenDigests {
		result[k] = v
	}
	return result
}

func (s *MockVectorPushService) PushStatements(ctx context.Context, batch *stmtsummaryv3proto.StatementBatch) (*stmtsummaryv3proto.PushResponse, error) {
	s.mu.Lock()
	s.batches = append(s.batches, batch)
	s.mu.Unlock()

	cnt := s.batchCount.Add(1)
	stmtCnt := int64(len(batch.Statements))
	totalStmts := s.statementCount.Add(stmtCnt)

	log.Printf("[MockVector] PushStatements received: batch #%d, statements=%d (total=%d), cluster=%s, instance=%s, window=[%d, %d]",
		cnt, stmtCnt, totalStmts,
		batch.GetMetadata().GetClusterId(),
		batch.GetMetadata().GetInstanceId(),
		batch.GetMetadata().GetWindowStartMs(),
		batch.GetMetadata().GetWindowEndMs())

	// Process and verify each statement
	verifiedCount := 0
	for i, stmt := range batch.Statements {
		digest := stmt.GetDigest()
		normalizedSQL := stmt.GetNormalizedSql()
		sampleSQL := stmt.GetSampleSql()

		// Track seen digests
		s.mu.Lock()
		s.seenDigests[digest]++
		s.mu.Unlock()

		// Check if this matches an expected SQL pattern
		var matchedPattern string
		s.mu.Lock()
		for expectedDigest, pattern := range s.expectedSQLs {
			// Match by digest or by SQL pattern
			if digest == expectedDigest || strings.Contains(normalizedSQL, pattern) || strings.Contains(sampleSQL, pattern) {
				matchedPattern = pattern
				break
			}
		}
		s.mu.Unlock()

		// Log statement details
		if matchedPattern != "" {
			log.Printf("  [%d] ✓ MATCHED: digest=%s exec_count=%d avg_latency=%dus schema=%s type=%s",
				i, digest, stmt.GetExecCount(), stmt.GetAvgLatencyUs(),
				stmt.GetSchemaName(), stmt.GetStmtType())
			log.Printf("      pattern=%s sample=%s", matchedPattern, truncateStr(sampleSQL, 80))
			verifiedCount++
		} else if i < 10 { // Print first 10 unmatched statements
			log.Printf("  [%d] digest=%s exec_count=%d avg_latency=%dus schema=%s type=%s",
				i, digest, stmt.GetExecCount(), stmt.GetAvgLatencyUs(),
				stmt.GetSchemaName(), stmt.GetStmtType())
			log.Printf("      normalized=%s", truncateStr(normalizedSQL, 80))
		}
	}

	if verifiedCount > 0 {
		log.Printf("  [VERIFICATION] %d statements matched expected patterns", verifiedCount)
	}

	if len(batch.Statements) > 10 {
		log.Printf("  ... and %d more statements (total=%d)", len(batch.Statements)-10, len(batch.Statements))
	}

	return &stmtsummaryv3proto.PushResponse{
		Success: true,
		Message: fmt.Sprintf("Received %d statements", stmtCnt),
	}, nil
}

func (s *MockVectorPushService) PushTableRows(ctx context.Context, batch *stmtsummaryv3proto.TableRowBatch) (*stmtsummaryv3proto.PushResponse, error) {
	s.mu.Lock()
	s.rowBatches = append(s.rowBatches, batch)
	s.mu.Unlock()

	log.Printf("[MockVector] PushTableRows received: table=%s, rows=%d",
		batch.GetTableName(), len(batch.GetRows()))

	return &stmtsummaryv3proto.PushResponse{
		Success: true,
		Message: fmt.Sprintf("Received %d rows", len(batch.GetRows())),
	}, nil
}

func (s *MockVectorPushService) Ping(ctx context.Context, req *stmtsummaryv3proto.PingRequest) (*stmtsummaryv3proto.PingResponse, error) {
	log.Printf("[MockVector] Ping from cluster=%s instance=%s",
		req.GetClusterId(), req.GetInstanceId())
	return &stmtsummaryv3proto.PingResponse{
		Ok:              true,
		Version:         "mock-vector-0.1.0",
		SupportedTables: []string{"STATEMENTS_SUMMARY"},
	}, nil
}

func (s *MockVectorPushService) GetStats() (int64, int64) {
	return s.batchCount.Load(), s.statementCount.Load()
}

func truncateStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// registerWithTiDB calls TiDB's StatementPushControl.RegisterPushTarget
// to register this mock Vector as the push target.
func registerWithTiDB(tidbAddr, vectorListenAddr string) error {
	conn, err := grpc.NewClient(tidbAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to TiDB gRPC at %s: %w", tidbAddr, err)
	}
	defer conn.Close()

	client := stmtsummaryv3proto.NewStatementPushControlClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.RegisterPushTarget(ctx, &stmtsummaryv3proto.RegisterPushTargetRequest{
		VectorEndpoint:   vectorListenAddr,
		VectorInstanceId: "mock-vector-001",
		VectorVersion:    "mock-0.1.0",
		CollectionConfig: &stmtsummaryv3proto.CollectionConfig{
			AggregationWindowSecs: 10,  // 10秒聚合窗口，方便测试
			PushBatchSize:         1000,
			PushIntervalSecs:      10,  // 每10秒 push 一次
			PushTimeoutSecs:       30,
			MaxDigestsPerWindow:   10000,
			ConfigVersion:         1,
		},
	})
	if err != nil {
		return fmt.Errorf("RegisterPushTarget failed: %w", err)
	}

	log.Printf("[MockVector] RegisterPushTarget response: success=%v, message=%s, registered_at=%d",
		resp.GetSuccess(), resp.GetMessage(), resp.GetRegisteredAtMs())
	return nil
}

// setupExpectedSQLs configures the expected SQL patterns for this test.
func setupExpectedSQLs(mockService *MockVectorPushService) {
	// These patterns should match the SQL executed in the test
	expected := map[string]string{
		// We'll match by SQL pattern instead of digest since digest is hash-based
		"SELECT":  "SELECT",
		"INSERT":  "INSERT",
		"UPDATE":  "UPDATE",
		"CREATE":  "CREATE",
		"DELETE":  "DELETE", // if present
	}
	mockService.SetExpectedSQLs(expected)
	log.Printf("[MockVector] Expected SQL patterns configured: SELECT, INSERT, UPDATE, CREATE")
}

func main() {
	vectorPort := "50051"
	tidbGRPCAddr := "127.0.0.1:10080" // TiDB's default status/gRPC port
	autoVerify := os.Getenv("AUTO_VERIFY") == "1" // Enable auto-verification mode

	if p := os.Getenv("VECTOR_PORT"); p != "" {
		vectorPort = p
	}
	if a := os.Getenv("TIDB_GRPC_ADDR"); a != "" {
		tidbGRPCAddr = a
	}

	// --- Create mock service ---
	mockService := &MockVectorPushService{
		expectedSQLs: make(map[string]string),
		seenDigests:  make(map[string]int),
	}

	// Setup expected SQL patterns for verification
	if autoVerify {
		setupExpectedSQLs(mockService)
	}

	// --- Start Mock Vector gRPC server ---
	lis, err := net.Listen("tcp", ":"+vectorPort)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", vectorPort, err)
	}

	srv := grpc.NewServer()
	stmtsummaryv3proto.RegisterSystemTablePushServiceServer(srv, mockService)

	go func() {
		log.Printf("[MockVector] Listening on :%s (waiting for TiDB push)", vectorPort)
		log.Printf("[MockVector] Auto-verify mode: %v", autoVerify)
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// --- Register with TiDB ---
	vectorListenAddr := fmt.Sprintf("127.0.0.1:%s", vectorPort)
	log.Printf("[MockVector] Registering with TiDB at %s ...", tidbGRPCAddr)

	// Retry registration a few times (TiDB might not be ready yet)
	for i := 0; i < 10; i++ {
		err = registerWithTiDB(tidbGRPCAddr, vectorListenAddr)
		if err == nil {
			log.Printf("[MockVector] Successfully registered with TiDB!")
			break
		}
		log.Printf("[MockVector] Registration attempt %d failed: %v, retrying in 3s...", i+1, err)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		log.Printf("[MockVector] WARNING: Could not register with TiDB: %v", err)
		log.Printf("[MockVector] The server is still running; you can register manually via grpcurl.")
	}

	// --- Print stats periodically ---
	doneCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-doneCh:
				return
			case <-ticker.C:
				batches, stmts := mockService.GetStats()
				log.Printf("[MockVector] Stats: batches=%d, total_statements=%d", batches, stmts)
			}
		}
	}()

	// --- Wait for shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("[MockVector] Shutting down...")
	close(doneCh)
	srv.GracefulStop()

	batches, stmts := mockService.GetStats()
	log.Printf("[MockVector] Final stats: batches=%d, total_statements=%d", batches, stmts)

	// --- Verify expected SQLs (if auto-verify mode) ---
	if autoVerify {
		log.Printf("[MockVector] Running verification...")
		missing, allSeen := mockService.VerifyExpectedSQLs()
		if allSeen {
			log.Printf("[MockVector] ✓ VERIFICATION PASSED: All expected SQL patterns were received!")
		} else {
			log.Printf("[MockVector] ✗ VERIFICATION FAILED: Missing patterns:")
			for _, m := range missing {
				log.Printf("    - %s", m)
			}
			log.Printf("[MockVector] Received SQL summary:")
			for digest, count := range mockService.GetReceivedSQLs() {
				log.Printf("    digest=%s: %d times", digest, count)
			}
		}
	}
}
