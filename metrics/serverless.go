// Copyright 2022 PingCAP, Inc.
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
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// TagTenantID for tenant_id
	TagTenantID = "tenant_id"
	// TagProjectID for project_id
	TagProjectID = "project_id"
	// TagClusterID for cluster_id
	TagClusterID = "cluster_id"
	// TagKeyspaceID for keyspace_id
	TagKeyspaceID = "keyspace_id"
)

// Identify serverless cluster. Should be setup before register metrics.
var (
	ServerlessLabels    prometheus.Labels
	ServerlessTenantID  string
	ServerlessProjectID string
	ServerlessClusterID string
)

// SetServerlessLabels sets tenantID, projectID and clusterID.
func SetServerlessLabels(serverlessTenantID, serverlessProjectID, serverlessClusterID string, keyspaceID uint32) {
	ServerlessLabels = make(prometheus.Labels)
	ServerlessLabels[TagTenantID] = serverlessTenantID
	ServerlessLabels[TagProjectID] = serverlessProjectID
	ServerlessLabels[TagClusterID] = serverlessClusterID
	ServerlessLabels[TagKeyspaceID] = fmt.Sprintf("%d", keyspaceID)

	ServerlessTenantID = serverlessTenantID
	ServerlessProjectID = serverlessProjectID
	ServerlessClusterID = serverlessClusterID
}

// NewCounter wraps a prometheus.NewCounter.
func NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
	opts.ConstLabels = ServerlessLabels
	return prometheus.NewCounter(opts)
}

// NewCounterVec wraps a prometheus.NewCounterVec.
func NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	opts.ConstLabels = ServerlessLabels
	return prometheus.NewCounterVec(opts, labelNames)
}

// NewGauge wraps a prometheus.NewGauge.
func NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	opts.ConstLabels = ServerlessLabels
	return prometheus.NewGauge(opts)
}

// NewGaugeVec wraps a prometheus.NewGaugeVec.
func NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	opts.ConstLabels = ServerlessLabels
	return prometheus.NewGaugeVec(opts, labelNames)
}

// NewHistogram wraps a prometheus.NewHistogram.
func NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	opts.ConstLabels = ServerlessLabels
	return prometheus.NewHistogram(opts)
}

// NewHistogramVec wraps a prometheus.NewHistogramVec.
func NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	opts.ConstLabels = ServerlessLabels
	return prometheus.NewHistogramVec(opts, labelNames)
}

// NewSummaryVec wraps a prometheus.NewSummaryVec.
func NewSummaryVec(opts prometheus.SummaryOpts, labelNames []string) *prometheus.SummaryVec {
	opts.ConstLabels = ServerlessLabels
	return prometheus.NewSummaryVec(opts, labelNames)
}
