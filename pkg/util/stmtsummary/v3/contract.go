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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// FieldType represents the data type of a field in the contract.
type FieldType string

const (
	FieldTypeString   FieldType = "string"
	FieldTypeInt64    FieldType = "int64"
	FieldTypeUint64   FieldType = "uint64"
	FieldTypeFloat64  FieldType = "float64"
	FieldTypeBool     FieldType = "bool"
	FieldTypeBytes    FieldType = "bytes"
	FieldTypeTimestamp FieldType = "timestamp"
	FieldTypeDuration FieldType = "duration"
)

// FieldRequirement defines a field requirement in the contract.
type FieldRequirement struct {
	Name         string   `yaml:"name"`
	Type         FieldType `yaml:"type"`
	Description  string   `yaml:"description,omitempty"`
	DefaultValue string   `yaml:"default_value,omitempty"`
}

// RequirementsContract defines the requirements contract from Vector.
type RequirementsContract struct {
	Version        string             `yaml:"version"`
	Publisher      string             `yaml:"publisher"`
	RequiredFields []FieldRequirement `yaml:"required_fields"`
	OptionalFields []FieldRequirement `yaml:"optional_fields"`
}

// ValidationResult represents the result of contract validation.
type ValidationResult struct {
	Valid       bool
	Missing     []string
	Warnings    []string
	Version     string
	Publisher   string
	TotalFields int
}

// ContractClient handles fetching and validating the requirements contract.
type ContractClient struct {
	client    *http.Client
	contract  *RequirementsContract
	contractURL string
}

// NewContractClient creates a new ContractClient.
func NewContractClient(contractURL string) *ContractClient {
	return &ContractClient{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		contractURL: contractURL,
	}
}

// FetchContract fetches the requirements contract from Vector.
func (c *ContractClient) FetchContract(ctx context.Context) error {
	if c.contractURL == "" {
		logutil.BgLogger().Info("no contract URL configured, skipping contract validation")
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, "GET", c.contractURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch contract: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	var contract RequirementsContract
	if err := yaml.Unmarshal(data, &contract); err != nil {
		return fmt.Errorf("parse contract YAML: %w", err)
	}

	c.contract = &contract
	logutil.BgLogger().Info("fetched requirements contract",
		zap.String("version", contract.Version),
		zap.String("publisher", contract.Publisher),
		zap.Int("required_fields", len(contract.RequiredFields)),
		zap.Int("optional_fields", len(contract.OptionalFields)))

	return nil
}

// Validate validates TiDB's capabilities against the contract.
func (c *ContractClient) Validate() (*ValidationResult, error) {
	if c.contract == nil {
		return &ValidationResult{Valid: true}, nil
	}

	result := &ValidationResult{
		Version:     c.contract.Version,
		Publisher:   c.contract.Publisher,
		TotalFields: len(c.contract.RequiredFields) + len(c.contract.OptionalFields),
	}

	// Get TiDB's supported fields
	tidbFields := c.getTiDBSupportedFields()

	// Check required fields
	for _, req := range c.contract.RequiredFields {
		if !c.isFieldSupported(req.Name, req.Type, tidbFields) {
			result.Missing = append(result.Missing, req.Name)
		}
	}

	// Check optional fields
	for _, opt := range c.contract.OptionalFields {
		if !c.isFieldSupported(opt.Name, opt.Type, tidbFields) {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("optional field '%s' not provided by TiDB", opt.Name))
		}
	}

	result.Valid = len(result.Missing) == 0

	return result, nil
}

// isFieldSupported checks if a field is supported by TiDB.
func (c *ContractClient) isFieldSupported(name string, fieldType FieldType, tidbFields map[string]FieldType) bool {
	_, ok := tidbFields[name]
	if !ok {
		return false
	}
	// For simplicity, accept any compatible type
	// In production, you might want stricter type checking
	return true
}

// getTiDBSupportedFields returns the fields supported by TiDB.
func (c *ContractClient) getTiDBSupportedFields() map[string]FieldType {
	return map[string]FieldType{
		// Identity fields
		"digest":         FieldTypeString,
		"plan_digest":    FieldTypeString,
		"schema_name":    FieldTypeString,
		"normalized_sql": FieldTypeString,
		"table_names":    FieldTypeString,
		"stmt_type":      FieldTypeString,

		// Sample data
		"sample_sql":  FieldTypeString,
		"sample_plan": FieldTypeString,
		"prev_sql":    FieldTypeString,

		// Execution stats
		"exec_count":    FieldTypeInt64,
		"sum_errors":    FieldTypeInt64,
		"sum_warnings":  FieldTypeInt64,

		// Latency
		"sum_latency_us":  FieldTypeInt64,
		"max_latency_us":  FieldTypeInt64,
		"min_latency_us":  FieldTypeInt64,
		"avg_latency_us":  FieldTypeInt64,
		"p50_latency_us":  FieldTypeInt64,
		"p95_latency_us":  FieldTypeInt64,
		"p99_latency_us":  FieldTypeInt64,

		// Parse/Compile
		"sum_parse_latency_us":   FieldTypeInt64,
		"max_parse_latency_us":   FieldTypeInt64,
		"sum_compile_latency_us": FieldTypeInt64,
		"max_compile_latency_us": FieldTypeInt64,

		// Resources
		"sum_mem_bytes":  FieldTypeInt64,
		"max_mem_bytes":  FieldTypeInt64,
		"sum_disk_bytes": FieldTypeInt64,
		"max_disk_bytes": FieldTypeInt64,
		"sum_tidb_cpu_us": FieldTypeInt64,
		"sum_tikv_cpu_us": FieldTypeInt64,

		// Coprocessor
		"sum_num_cop_tasks":  FieldTypeInt64,
		"sum_process_time_us": FieldTypeInt64,
		"max_process_time_us": FieldTypeInt64,
		"sum_wait_time_us":    FieldTypeInt64,
		"max_wait_time_us":    FieldTypeInt64,

		// Keys
		"sum_total_keys":     FieldTypeInt64,
		"max_total_keys":     FieldTypeInt64,
		"sum_processed_keys": FieldTypeInt64,
		"max_processed_keys": FieldTypeInt64,

		// Transaction
		"commit_count":        FieldTypeInt64,
		"sum_prewrite_time_us": FieldTypeInt64,
		"max_prewrite_time_us": FieldTypeInt64,
		"sum_commit_time_us":   FieldTypeInt64,
		"max_commit_time_us":   FieldTypeInt64,
		"sum_write_keys":       FieldTypeInt64,
		"max_write_keys":       FieldTypeInt64,
		"sum_write_size_bytes": FieldTypeInt64,
		"max_write_size_bytes": FieldTypeInt64,

		// Rows
		"sum_affected_rows": FieldTypeInt64,
		"sum_result_rows":   FieldTypeInt64,
		"max_result_rows":   FieldTypeInt64,
		"min_result_rows":   FieldTypeInt64,

		// Plan cache
		"plan_in_cache":   FieldTypeBool,
		"plan_cache_hits": FieldTypeInt64,

		// Timestamps
		"first_seen_ms": FieldTypeTimestamp,
		"last_seen_ms":  FieldTypeTimestamp,

		// Flags
		"is_internal": FieldTypeBool,
		"prepared":    FieldTypeBool,

		// Multi-tenancy
		"keyspace_name":        FieldTypeString,
		"keyspace_id":          FieldTypeUint64,
		"resource_group_name":  FieldTypeString,
	}
}

// GetContract returns the fetched contract.
func (c *ContractClient) GetContract() *RequirementsContract {
	return c.contract
}

// LoadContractFromBytes loads a contract from bytes (useful for testing).
func (c *ContractClient) LoadContractFromBytes(data []byte) error {
	var contract RequirementsContract
	if err := yaml.Unmarshal(data, &contract); err != nil {
		return fmt.Errorf("parse contract YAML: %w", err)
	}
	c.contract = &contract
	return nil
}

// CreateSampleContract creates a sample contract for testing.
func CreateSampleContract() []byte {
	contract := RequirementsContract{
		Version:   "1.0.0",
		Publisher: "vector-extensions",
		RequiredFields: []FieldRequirement{
			{
				Name:        "digest",
				Type:        FieldTypeString,
				Description: "SQL statement digest (fingerprint)",
			},
			{
				Name:        "exec_count",
				Type:        FieldTypeInt64,
				Description: "Total execution count in the window",
			},
			{
				Name:        "sum_latency_us",
				Type:        FieldTypeInt64,
				Description: "Total latency in microseconds",
			},
		},
		OptionalFields: []FieldRequirement{
			{
				Name:         "sum_disk_bytes",
				Type:         FieldTypeInt64,
				Description:  "Total disk I/O bytes",
				DefaultValue: "0",
			},
			{
				Name:         "p99_latency_us",
				Type:         FieldTypeInt64,
				Description:  "P99 latency in microseconds",
				DefaultValue: "0",
			},
		},
	}

	data, err := yaml.Marshal(contract)
	if err != nil {
		return nil
	}
	return data
}

// ServeContractHTTP serves the contract via HTTP for other TiDB instances.
func ServeContractHTTP(port int, contract *RequirementsContract) error {
	data, err := yaml.Marshal(contract)
	if err != nil {
		return err
	}

	http.HandleFunc("/contract", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/yaml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})

	addr := fmt.Sprintf(":%d", port)
	logutil.BgLogger().Info("serving contract via HTTP",
		zap.String("addr", addr),
		zap.String("path", "/contract"))

	return http.ListenAndServe(addr, nil)
}

// FetchContractFromURL fetches a contract from a URL (utility function).
func FetchContractFromURL(ctx context.Context, url string) (*RequirementsContract, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var contract RequirementsContract
	if err := yaml.Unmarshal(data, &contract); err != nil {
		return nil, err
	}

	return &contract, nil
}

// ParseContract parses a contract from YAML bytes.
func ParseContract(data []byte) (*RequirementsContract, error) {
	var contract RequirementsContract
	if err := yaml.Unmarshal(data, &contract); err != nil {
		return nil, err
	}
	return &contract, nil
}

// ValidateContract validates a contract against TiDB capabilities.
func ValidateContract(contract *RequirementsContract) (*ValidationResult, error) {
	client := &ContractClient{contract: contract}
	return client.Validate()
}

// FormatValidationResult formats a validation result as a string.
func FormatValidationResult(result *ValidationResult) string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("Contract Validation Result:\n"))
	buf.WriteString(fmt.Sprintf("  Version: %s\n", result.Version))
	buf.WriteString(fmt.Sprintf("  Publisher: %s\n", result.Publisher))
	buf.WriteString(fmt.Sprintf("  Valid: %v\n", result.Valid))

	if len(result.Missing) > 0 {
		buf.WriteString(fmt.Sprintf("  Missing Required Fields (%d):\n", len(result.Missing)))
		for _, field := range result.Missing {
			buf.WriteString(fmt.Sprintf("    - %s\n", field))
		}
	}

	if len(result.Warnings) > 0 {
		buf.WriteString(fmt.Sprintf("  Warnings (%d):\n", len(result.Warnings)))
		for _, warning := range result.Warnings {
			buf.WriteString(fmt.Sprintf("    - %s\n", warning))
		}
	}

	return buf.String()
}
