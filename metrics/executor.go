// Copyright 2018 PingCAP, Inc.
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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ExecutorCounter records the number of expensive executors.
	ExecutorCounter *prometheus.CounterVec

	// StmtNodeCounter records the number of statement with the same type.
	StmtNodeCounter *prometheus.CounterVec

	// DbStmtNodeCounter records the number of statement with the same type and db.
	DbStmtNodeCounter *prometheus.CounterVec

	// ExecPhaseDuration records the duration of each execution phase.
	ExecPhaseDuration *prometheus.SummaryVec

	// LastStmtTimestamp records record last count stmt node.
	LastStmtTimestamp int64 = 0
)

// DefineExecutorMetrics defines excutor metrics.
func DefineExecutorMetrics() {
	// ExecutorCounter records the number of expensive executors.
	ExecutorCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "expensive_total",
			Help:      "Counter of Expensive Executors.",
		}, []string{LblType},
	)

	// StmtNodeCounter records the number of statement with the same type.
	StmtNodeCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "statement_total",
			Help:      "Counter of StmtNode.",
		}, []string{LblType})

	// DbStmtNodeCounter records the number of statement with the same type and db.
	DbStmtNodeCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "statement_db_total",
			Help:      "Counter of StmtNode by Database.",
		}, []string{LblDb, LblType})

	// ExecPhaseDuration records the duration of each execution phase.
	ExecPhaseDuration = NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "phase_duration_seconds",
			Help:      "Summary of each execution phase duration.",
		}, []string{LblPhase, LblInternal})
}
