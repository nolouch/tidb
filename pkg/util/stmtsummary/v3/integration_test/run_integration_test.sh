#!/bin/bash
#
# Statement V3 Integration Test Script
#
# Prerequisites:
#   1. TiDB compiled with V3 support (make server)
#   2. grpcurl installed: go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
#   3. mysql client installed
#
# This script runs through the full end-to-end flow:
#   TiDB startup → V3 init → Mock Vector registration → SQL execution → Push verification
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TIDB_DIR="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"  # tidb root
TIDB_BIN="${TIDB_DIR}/bin/tidb-server"
MOCK_VECTOR_BIN="${SCRIPT_DIR}/mock_vector_server"

# Ports
TIDB_PORT=4000
TIDB_STATUS_PORT=10080
VECTOR_PORT=50051

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

cleanup() {
    info "Cleaning up..."
    [ -n "${TIDB_PID:-}" ]  && kill "$TIDB_PID" 2>/dev/null && wait "$TIDB_PID" 2>/dev/null || true
    [ -n "${VECTOR_PID:-}" ] && kill "$VECTOR_PID" 2>/dev/null && wait "$VECTOR_PID" 2>/dev/null || true
    [ -d "${TMPDIR:-}" ] && rm -rf "$TMPDIR"
    info "Done."
}
trap cleanup EXIT

########################################
# Phase 0: Build
########################################
info "============================================"
info "Phase 0: Build"
info "============================================"

info "Building TiDB server..."
cd "$TIDB_DIR"
if [ ! -f "$TIDB_BIN" ]; then
    make server 2>&1 | tail -5
fi
info "TiDB binary: $TIDB_BIN"

info "Building Mock Vector server..."
cd "$SCRIPT_DIR"
go build -o "$MOCK_VECTOR_BIN" mock_vector_server.go
info "Mock Vector binary: $MOCK_VECTOR_BIN"

########################################
# Phase 1: Start TiDB with V3 enabled
########################################
info ""
info "============================================"
info "Phase 1: Start TiDB (V3 enabled, mocktikv)"
info "============================================"

TMPDIR=$(mktemp -d)
TIDB_LOG="${TMPDIR}/tidb.log"
TIDB_CONFIG="${TMPDIR}/tidb.toml"

cat > "$TIDB_CONFIG" <<EOF
[instance]
tidb_stmt_summary_v3_enabled = true

[status]
status-port = ${TIDB_STATUS_PORT}

[log]
level = "info"
EOF

info "Config: $TIDB_CONFIG"
info "Log:    $TIDB_LOG"

"$TIDB_BIN" \
    --store=unistore \
    --path="$TMPDIR/tidb-data" \
    --config="$TIDB_CONFIG" \
    -P "$TIDB_PORT" \
    --log-file="$TIDB_LOG" \
    &
TIDB_PID=$!
info "TiDB started, PID=$TIDB_PID"

# Wait for TiDB to be ready
info "Waiting for TiDB to be ready..."
for i in $(seq 1 30); do
    if mysql -h 127.0.0.1 -P "$TIDB_PORT" -u root -e "SELECT 1" >/dev/null 2>&1; then
        info "TiDB is ready! (took ${i}s)"
        break
    fi
    if [ "$i" -eq 30 ]; then
        error "TiDB failed to start within 30s"
        cat "$TIDB_LOG" | tail -30
        exit 1
    fi
    sleep 1
done

########################################
# Phase 2: Verify V3 init via gRPC
########################################
info ""
info "============================================"
info "Phase 2: Verify V3 gRPC service registered"
info "============================================"

PROTO_FILE="${TIDB_DIR}/pkg/util/stmtsummary/v3/proto/v1/systemtable.proto"
PROTO_DIR="$(dirname "$PROTO_FILE")"

info "Listing gRPC services on TiDB status port..."
GRPC_SERVICES=$(grpcurl -plaintext "127.0.0.1:${TIDB_STATUS_PORT}" list 2>&1 || true)
echo "$GRPC_SERVICES"

if echo "$GRPC_SERVICES" | grep -q "StatementPushControl"; then
    info "StatementPushControl service is registered!"
else
    warn "StatementPushControl service NOT found in gRPC service list."
    warn "This might be expected if reflection is not enabled."
    warn "Proceeding with direct proto-based calls..."
fi

########################################
# Phase 3: Start Mock Vector & Register
########################################
info ""
info "============================================"
info "Phase 3: Start Mock Vector & Register"
info "============================================"

VECTOR_LOG="${TMPDIR}/vector.log"

VECTOR_PORT=$VECTOR_PORT \
TIDB_GRPC_ADDR="127.0.0.1:${TIDB_STATUS_PORT}" \
AUTO_VERIFY=1 \
"$MOCK_VECTOR_BIN" > "$VECTOR_LOG" 2>&1 &
VECTOR_PID=$!
info "Mock Vector started, PID=$VECTOR_PID, log=$VECTOR_LOG (AUTO_VERIFY=1)"

# Wait for registration
sleep 5
info "Mock Vector registration log:"
cat "$VECTOR_LOG"

if grep -q "Successfully registered" "$VECTOR_LOG"; then
    info "Mock Vector successfully registered with TiDB!"
else
    warn "Registration might have failed, check $VECTOR_LOG"
fi

########################################
# Phase 4: Execute SQL workload
########################################
info ""
info "============================================"
info "Phase 4: Execute SQL workload"
info "============================================"

info "Creating test database and table..."
mysql -h 127.0.0.1 -P "$TIDB_PORT" -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS test_v3;
USE test_v3;
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    email VARCHAR(200),
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (name, email, age) VALUES
    ('Alice', 'alice@example.com', 30),
    ('Bob', 'bob@example.com', 25),
    ('Charlie', 'charlie@example.com', 35);
SQL
info "Test data created."

info "Running mixed SQL workload with distinct patterns..."
info "  - SELECT queries (various WHERE clauses)"
info "  - INSERT queries"
info "  - UPDATE queries"
info "  - CREATE/DROP TABLE"

# Distinct SQL patterns for verification
for i in $(seq 1 30); do
    mysql -h 127.0.0.1 -P "$TIDB_PORT" -u root -D test_v3 -e "
        SELECT * FROM users WHERE id = $((i % 3 + 1));
        SELECT COUNT(*) FROM users WHERE age > $((i % 40));
        SELECT name, email FROM users ORDER BY created_at DESC LIMIT 10;
        INSERT INTO users (name, email, age) VALUES ('User_${i}', 'user${i}@test.com', $((20 + i % 50)));
        UPDATE users SET age = age + 1 WHERE id = $((i % 3 + 1));
    " 2>/dev/null
done

# Additional distinct patterns
info "Running additional distinct SQL patterns..."
mysql -h 127.0.0.1 -P "$TIDB_PORT" -u root -D test_v3 -e "
    CREATE TABLE temp_test (id INT, value VARCHAR(50));
    INSERT INTO temp_test VALUES (1, 'test'), (2, 'test2');
    SELECT * FROM temp_test;
    DROP TABLE temp_test;
    DELETE FROM users WHERE age > 100;
    SHOW TABLES;
" 2>/dev/null

info "Workload complete: ~160 SQL statements executed with distinct patterns."

########################################
# Phase 5: Wait for aggregation window flush
########################################
info ""
info "============================================"
info "Phase 5: Wait for aggregation & push"
info "============================================"

info "Waiting 55s for aggregation window to flush..."
info "(Mock Vector configures 10s aggregation window + 10s push interval)"
for i in $(seq 1 6); do
    sleep 10
    info "  ... ${i}0s elapsed"
    VECTOR_BATCHES=$(grep -c "PushStatements received" "$VECTOR_LOG" 2>/dev/null || echo "0")
    if [ "$VECTOR_BATCHES" -gt 0 ]; then
        info "  Push detected! ($VECTOR_BATCHES batches so far)"
    fi
    # Check if TiDB is flushing data
    TIDB_FLUSH=$(grep -c "\[stmt-v3\] Flushing window" "$TIDB_LOG" 2>/dev/null || echo "0")
    if [ "$TIDB_FLUSH" -gt 0 ]; then
        info "  TiDB flushes detected: $TIDB_FLUSH"
    fi
    # Check for Pusher logs
    PUSHER_LOGS=$(grep -c "\[stmt-v3\] Pusher\." "$TIDB_LOG" 2>/dev/null || echo "0")
    if [ "$PUSHER_LOGS" -gt 0 ]; then
        info "  Pusher activity detected: $PUSHER_LOGS log lines"
    fi
done

########################################
# Phase 6: Verify results
########################################
info ""
info "============================================"
info "Phase 6: Verify Results"
info "============================================"

info "--- Mock Vector received data ---"
VECTOR_BATCHES=$(grep -c "PushStatements received" "$VECTOR_LOG" 2>/dev/null || echo "0")
VECTOR_STMTS=$(grep "Stats:" "$VECTOR_LOG" | tail -1 || echo "none")

info "Batches received: $VECTOR_BATCHES"
info "Latest stats: $VECTOR_STMTS"

echo ""
info "--- Detailed push log ---"
grep -E "(PushStatements|digest=|Stats:)" "$VECTOR_LOG" || true

echo ""
info "--- TiDB V3 related logs ---"
grep -i "statement v3\|stmtsummaryv3\|RegisterPushTarget\|push target\|\[stmt-v3\]" "$TIDB_LOG" | tail -30 || true

echo ""
info "--- TiDB onFlush & Pusher debug logs ---"
grep "\[stmt-v3\]" "$TIDB_LOG" || echo "No stmt-v3 debug logs found"

echo ""
info "--- SQL Pattern Verification ---"
VERIFIED_COUNT=$(grep -c "✓ MATCHED" "$VECTOR_LOG" 2>/dev/null || echo "0")
SELECT_MATCHED=$(grep -c "MATCHED.*SELECT" "$VECTOR_LOG" 2>/dev/null || echo "0")
INSERT_MATCHED=$(grep -c "MATCHED.*INSERT" "$VECTOR_LOG" 2>/dev/null || echo "0")
UPDATE_MATCHED=$(grep -c "MATCHED.*UPDATE" "$VECTOR_LOG" 2>/dev/null || echo "0")
CREATE_MATCHED=$(grep -c "MATCHED.*CREATE" "$VECTOR_LOG" 2>/dev/null || echo "0")
info "Verified SQL patterns: SELECT=$SELECT_MATCHED, INSERT=$INSERT_MATCHED, UPDATE=$UPDATE_MATCHED, CREATE=$CREATE_MATCHED"

echo ""
info "--- Matched statements sample ---"
grep "✓ MATCHED" "$VECTOR_LOG" | head -10 || true

echo ""
if [ "$VECTOR_BATCHES" -gt 0 ] && [ "$VERIFIED_COUNT" -gt 0 ]; then
    info "============================================"
    info "  INTEGRATION TEST PASSED!"
    info "  TiDB → V3 Aggregator → Pusher → Mock Vector"
    info "  Batches: $VECTOR_BATCHES, Verified Patterns: $VERIFIED_COUNT"
    info "  SELECT=$SELECT_MATCHED INSERT=$INSERT_MATCHED UPDATE=$UPDATE_MATCHED CREATE=$CREATE_MATCHED"
    info "============================================"
    exit 0
elif [ "$VECTOR_BATCHES" -gt 0 ]; then
    warn "============================================"
    warn "  INTEGRATION TEST PARTIALLY PASSED"
    warn "  Data received but SQL pattern verification failed."
    warn "  Check $VECTOR_LOG for details."
    warn "============================================"
    exit 1
else
    error "============================================"
    error "  INTEGRATION TEST FAILED!"
    error "  No push data received by Mock Vector."
    error "  Check logs:"
    error "    TiDB:   $TIDB_LOG"
    error "    Vector: $VECTOR_LOG"
    error "============================================"
    exit 1
fi
