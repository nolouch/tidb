// Copyright 2022 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/log"
	br_task "github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Metrics of common plans
	planCacheCounter     prometheus.Counter
	planCacheMissCounter prometheus.Counter
	// Metrics of logical plan builder
	pseudoEstimationNotAvailable prometheus.Counter
	pseudoEstimationOutdate      prometheus.Counter
)

func init() {
	InitMetricsVars()
	br_task.RegisterPlannercoreMetrics(InitMetricsVars)
}

// InitMetricsVars init planner core metrics counter
func InitMetricsVars() {
	log.Info("init planner core metrics")
	planCacheCounter = metrics.PlanCacheCounter.WithLabelValues("prepare")
	planCacheMissCounter = metrics.PlanCacheMissCounter.WithLabelValues("cache_miss")
	pseudoEstimationNotAvailable = metrics.PseudoEstimation.WithLabelValues("nodata")
	pseudoEstimationOutdate = metrics.PseudoEstimation.WithLabelValues("outdate")
}
