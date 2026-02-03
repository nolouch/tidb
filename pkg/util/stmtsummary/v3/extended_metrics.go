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
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// MetricAggregationType defines how to aggregate a metric.
type MetricAggregationType string

const (
	AggregationSum  MetricAggregationType = "sum"
	AggregationMax  MetricAggregationType = "max"
	AggregationMin  MetricAggregationType = "min"
	AggregationAvg  MetricAggregationType = "avg"
	AggregationLast MetricAggregationType = "last"
)

// ExtendedMetricProcessor handles config-driven extended metrics.
type ExtendedMetricProcessor struct {
	configs map[string]ExtendedMetricConfig
}

// NewExtendedMetricProcessor creates a new ExtendedMetricProcessor.
func NewExtendedMetricProcessor(configs []ExtendedMetricConfig) *ExtendedMetricProcessor {
	p := &ExtendedMetricProcessor{
		configs: make(map[string]ExtendedMetricConfig),
	}

	for _, cfg := range configs {
		if cfg.Enabled && cfg.Name != "" {
			p.configs[cfg.Name] = cfg
			logutil.BgLogger().Info("enabled extended metric",
				zap.String("name", cfg.Name),
				zap.String("source", cfg.Source),
				zap.String("type", cfg.Type),
				zap.String("aggregation", cfg.Aggregation))
		}
	}

	return p
}

// ExtractMetrics extracts extended metrics from the execution info.
// The source is typically a field path like "exec_info.network_bytes"
func (p *ExtendedMetricProcessor) ExtractMetrics(execInfo any) map[string]MetricValue {
	result := make(map[string]MetricValue)

	for name, cfg := range p.configs {
		value, err := p.extractValue(execInfo, cfg.Source)
		if err != nil {
			logutil.BgLogger().Debug("failed to extract extended metric",
				zap.String("name", name),
				zap.String("source", cfg.Source),
				zap.Error(err))
			continue
		}

		result[name] = p.convertValue(value, cfg.Type)
	}

	return result
}

// extractValue extracts a value from the execInfo using the source path.
func (p *ExtendedMetricProcessor) extractValue(execInfo any, source string) (any, error) {
	if execInfo == nil {
		return nil, fmt.Errorf("execInfo is nil")
	}

	parts := strings.Split(source, ".")
	current := reflect.ValueOf(execInfo)

	// Handle pointer types
	if current.Kind() == reflect.Ptr {
		if current.IsNil() {
			return nil, fmt.Errorf("nil pointer at %s", parts[0])
		}
		current = current.Elem()
	}

	for i, part := range parts {
		if current.Kind() == reflect.Struct {
			field := current.FieldByName(part)
			if !field.IsValid() {
				// Try to find by tag (e.g., `json:"field_name"`)
				field = p.findFieldByTag(current, part)
				if !field.IsValid() {
					return nil, fmt.Errorf("field not found: %s", part)
				}
			}

			// Handle pointer fields
			if field.Kind() == reflect.Ptr {
				if field.IsNil() {
					return nil, fmt.Errorf("nil pointer at %s", part)
				}
				field = field.Elem()
			}

			if i < len(parts)-1 {
				current = field
			} else {
				return field.Interface(), nil
			}
		} else if current.Kind() == reflect.Map {
			// Handle map types
			mapValue := current.MapIndex(reflect.ValueOf(part))
			if !mapValue.IsValid() {
				return nil, fmt.Errorf("key not found: %s", part)
			}

			if i < len(parts)-1 {
				current = mapValue
				if current.Kind() == reflect.Ptr && !current.IsNil() {
					current = current.Elem()
				}
			} else {
				return mapValue.Interface(), nil
			}
		} else {
			return nil, fmt.Errorf("unsupported type at %s: %v", part, current.Kind())
		}
	}

	return nil, fmt.Errorf("path not found: %s", source)
}

// findFieldByTag tries to find a struct field by its json tag.
func (p *ExtendedMetricProcessor) findFieldByTag(v reflect.Value, tag string) reflect.Value {
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag != "" {
			// Remove "omitempty" and other options
			parts := strings.Split(jsonTag, ",")
			if parts[0] == tag {
				return v.Field(i)
			}
		}
	}
	return reflect.Value{}
}

// convertValue converts a value to the specified type.
func (p *ExtendedMetricProcessor) convertValue(value any, targetType string) MetricValue {
	if value == nil {
		return MetricValue{Type: MetricTypeInt64, Int64: 0}
	}

	switch targetType {
	case "int64":
		switch v := value.(type) {
		case int:
			return MetricValue{Type: MetricTypeInt64, Int64: int64(v)}
		case int8:
			return MetricValue{Type: MetricTypeInt64, Int64: int64(v)}
		case int16:
			return MetricValue{Type: MetricTypeInt64, Int64: int64(v)}
		case int32:
			return MetricValue{Type: MetricTypeInt64, Int64: int64(v)}
		case int64:
			return MetricValue{Type: MetricTypeInt64, Int64: v}
		case uint:
			return MetricValue{Type: MetricTypeInt64, Int64: int64(v)}
		case uint8:
			return MetricValue{Type: MetricTypeInt64, Int64: int64(v)}
		case uint16:
			return MetricValue{Type: MetricTypeInt64, Int64: int64(v)}
		case uint32:
			return MetricValue{Type: MetricTypeInt64, Int64: int64(v)}
		case uint64:
			return MetricValue{Type: MetricTypeInt64, Int64: int64(v)}
		case float32:
			return MetricValue{Type: MetricTypeInt64, Int64: int64(v)}
		case float64:
			return MetricValue{Type: MetricTypeInt64, Int64: int64(v)}
		default:
			return MetricValue{Type: MetricTypeInt64, Int64: 0}
		}
	case "double", "float64":
		switch v := value.(type) {
		case int:
			return MetricValue{Type: MetricTypeFloat64, Float64: float64(v)}
		case int64:
			return MetricValue{Type: MetricTypeFloat64, Float64: float64(v)}
		case uint64:
			return MetricValue{Type: MetricTypeFloat64, Float64: float64(v)}
		case float32:
			return MetricValue{Type: MetricTypeFloat64, Float64: float64(v)}
		case float64:
			return MetricValue{Type: MetricTypeFloat64, Float64: v}
		default:
			return MetricValue{Type: MetricTypeFloat64, Float64: 0}
		}
	case "string":
		return MetricValue{Type: MetricTypeString, String: fmt.Sprintf("%v", value)}
	case "bool":
		switch v := value.(type) {
		case bool:
			return MetricValue{Type: MetricTypeBool, Bool: v}
		case int:
			return MetricValue{Type: MetricTypeBool, Bool: v != 0}
		case int64:
			return MetricValue{Type: MetricTypeBool, Bool: v != 0}
		case float64:
			return MetricValue{Type: MetricTypeBool, Bool: v != 0}
		case string:
			b, _ := strconv.ParseBool(v)
			return MetricValue{Type: MetricTypeBool, Bool: b}
		default:
			return MetricValue{Type: MetricTypeBool, Bool: false}
		}
	default:
		return MetricValue{Type: MetricTypeInt64, Int64: 0}
	}
}

// AggregateMetrics aggregates multiple metric values based on the aggregation type.
func (p *ExtendedMetricProcessor) AggregateMetrics(values []MetricValue, name string) MetricValue {
	cfg, ok := p.configs[name]
	if !ok {
		// Default to sum
		return p.aggregateSum(values)
	}

	switch MetricAggregationType(cfg.Aggregation) {
	case AggregationSum:
		return p.aggregateSum(values)
	case AggregationMax:
		return p.aggregateMax(values)
	case AggregationMin:
		return p.aggregateMin(values)
	case AggregationAvg:
		return p.aggregateAvg(values)
	case AggregationLast:
		if len(values) > 0 {
			return values[len(values)-1]
		}
		return MetricValue{Type: MetricTypeInt64, Int64: 0}
	default:
		return p.aggregateSum(values)
	}
}

func (p *ExtendedMetricProcessor) aggregateSum(values []MetricValue) MetricValue {
	if len(values) == 0 {
		return MetricValue{Type: MetricTypeInt64, Int64: 0}
	}

	result := MetricValue{Type: values[0].Type}

	switch values[0].Type {
	case MetricTypeInt64:
		var sum int64
		for _, v := range values {
			sum += v.Int64
		}
		result.Int64 = sum
	case MetricTypeFloat64:
		var sum float64
		for _, v := range values {
			sum += v.Float64
		}
		result.Float64 = sum
	default:
		// For non-numeric types, return the last value
		if len(values) > 0 {
			result = values[len(values)-1]
		}
	}

	return result
}

func (p *ExtendedMetricProcessor) aggregateMax(values []MetricValue) MetricValue {
	if len(values) == 0 {
		return MetricValue{Type: MetricTypeInt64, Int64: 0}
	}

	result := values[0]

	for i := 1; i < len(values); i++ {
		switch result.Type {
		case MetricTypeInt64:
			if values[i].Int64 > result.Int64 {
				result.Int64 = values[i].Int64
			}
		case MetricTypeFloat64:
			if values[i].Float64 > result.Float64 {
				result.Float64 = values[i].Float64
			}
		}
	}

	return result
}

func (p *ExtendedMetricProcessor) aggregateMin(values []MetricValue) MetricValue {
	if len(values) == 0 {
		return MetricValue{Type: MetricTypeInt64, Int64: 0}
	}

	result := values[0]

	for i := 1; i < len(values); i++ {
		switch result.Type {
		case MetricTypeInt64:
			if values[i].Int64 < result.Int64 {
				result.Int64 = values[i].Int64
			}
		case MetricTypeFloat64:
			if values[i].Float64 < result.Float64 {
				result.Float64 = values[i].Float64
			}
		}
	}

	return result
}

func (p *ExtendedMetricProcessor) aggregateAvg(values []MetricValue) MetricValue {
	if len(values) == 0 {
		return MetricValue{Type: MetricTypeFloat64, Float64: 0}
	}

	sum := p.aggregateSum(values)
	count := float64(len(values))

	switch sum.Type {
	case MetricTypeInt64:
		return MetricValue{
			Type:    MetricTypeFloat64,
			Float64: float64(sum.Int64) / count,
		}
	case MetricTypeFloat64:
		return MetricValue{
			Type:    MetricTypeFloat64,
			Float64: sum.Float64 / count,
		}
	default:
		return sum
	}
}

// GetConfig returns the configuration for a metric.
func (p *ExtendedMetricProcessor) GetConfig(name string) (ExtendedMetricConfig, bool) {
	cfg, ok := p.configs[name]
	return cfg, ok
}

// HasMetric returns true if a metric is configured.
func (p *ExtendedMetricProcessor) HasMetric(name string) bool {
	_, ok := p.configs[name]
	return ok
}

// MetricNames returns all configured metric names.
func (p *ExtendedMetricProcessor) MetricNames() []string {
	names := make([]string, 0, len(p.configs))
	for name := range p.configs {
		names = append(names, name)
	}
	return names
}

// DefaultExtendedMetricConfigs returns default extended metric configurations.
func DefaultExtendedMetricConfigs() []ExtendedMetricConfig {
	return []ExtendedMetricConfig{
		{
			Name:        "network_bytes",
			Type:        "int64",
			Source:      "networkBytes", // Field in StmtExecInfo
			Enabled:     false,
			Aggregation: "sum",
		},
		{
			Name:        "connection_id",
			Type:        "int64",
			Source:      "connectionID",
			Enabled:     false,
			Aggregation: "last",
		},
		{
			Name:        "user",
			Type:        "string",
			Source:      "user",
			Enabled:     false,
			Aggregation: "last",
		},
		{
			Name:        "database",
			Type:        "string",
			Source:      "database",
			Enabled:     false,
			Aggregation: "last",
		},
		{
			Name:        "plan_from_cache",
			Type:        "bool",
			Source:      "planFromCache",
			Enabled:     false,
			Aggregation: "last",
		},
		{
			Name:        "rewrite_time_us",
			Type:        "int64",
			Source:      "rewriteTime",
			Enabled:     false,
			Aggregation: "sum",
		},
		{
			Name:        "optimize_time_us",
			Type:        "int64",
			Source:      "optimizeTime",
			Enabled:     false,
			Aggregation: "sum",
		},
		{
			Name:        "exec_start_time",
			Type:        "int64",
			Source:      "execStartTime",
			Enabled:     false,
			Aggregation: "last",
		},
	}
}

// MergeExtendedMetrics merges two maps of extended metrics.
// When both maps have the same key, the aggregation type determines the result.
func MergeExtendedMetrics(a, b map[string]MetricValue, processor *ExtendedMetricProcessor) map[string]MetricValue {
	result := make(map[string]MetricValue)

	// Copy all metrics from a
	for k, v := range a {
		result[k] = v
	}

	// Merge metrics from b
	for k, v := range b {
		if existing, ok := result[k]; ok {
			// Aggregate values
			result[k] = processor.AggregateMetrics([]MetricValue{existing, v}, k)
		} else {
			result[k] = v
		}
	}

	return result
}

// DurationToMicroseconds converts a time.Duration to microseconds.
func DurationToMicroseconds(d time.Duration) int64 {
	return d.Microseconds()
}
