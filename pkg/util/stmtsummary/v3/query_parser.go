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
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Operator represents a comparison operator.
type Operator string

const (
	OpEqual        Operator = "="
	OpNotEqual     Operator = "!="
	OpLessThan     Operator = "<"
	OpLessEqual    Operator = "<="
	OpGreaterThan  Operator = ">"
	OpGreaterEqual Operator = ">="
	OpLike         Operator = "LIKE"
	OpIn           Operator = "IN"
	OpNotIn        Operator = "NOT IN"
)

// LogicalOperator represents a logical operator.
type LogicalOperator string

const (
	LogAnd LogicalOperator = "AND"
	LogOr  LogicalOperator = "OR"
)

// Condition represents a single WHERE condition.
type Condition struct {
	Field    string
	Operator Operator
	Value    interface{}
}

// Expression represents a WHERE expression (can be a condition or logical combination).
type Expression interface {
	// Evaluate evaluates the expression against a row.
	Evaluate(row map[string]interface{}) (bool, error)
}

// WhereExpression represents a parsed WHERE clause.
type WhereExpression struct {
	Conditions []Condition
	LogicalOps  []LogicalOperator
}

// Evaluate evaluates the WHERE expression against a row.
func (w *WhereExpression) Evaluate(row map[string]interface{}) (bool, error) {
	if len(w.Conditions) == 0 {
		return true, nil
	}

	result, err := w.Conditions[0].Evaluate(row)
	if err != nil {
		return false, err
	}

	for i := 0; i < len(w.LogicalOps); i++ {
		nextResult, err := w.Conditions[i+1].Evaluate(row)
		if err != nil {
			return false, err
		}

		switch w.LogicalOps[i] {
		case LogAnd:
			result = result && nextResult
		case LogOr:
			result = result || nextResult
		}
	}

	return result, nil
}

// Evaluate evaluates a condition against a row.
func (c *Condition) Evaluate(row map[string]interface{}) (bool, error) {
	fieldValue, exists := row[c.Field]
	if !exists {
		return false, nil
	}

	switch c.Operator {
	case OpEqual:
		return compareEqual(fieldValue, c.Value)
	case OpNotEqual:
		result, err := compareEqual(fieldValue, c.Value)
		return !result, err
	case OpLessThan:
		return compareLess(fieldValue, c.Value)
	case OpLessEqual:
		less, err := compareLess(fieldValue, c.Value)
		if err != nil {
			return false, err
		}
		eq, err := compareEqual(fieldValue, c.Value)
		return less || eq, err
	case OpGreaterThan:
		less, err := compareLess(fieldValue, c.Value)
		return !less, err
	case OpGreaterEqual:
		less, err := compareLess(fieldValue, c.Value)
		if err != nil {
			return false, err
		}
		eq, err := compareEqual(fieldValue, c.Value)
		return !less || eq, err
	case OpLike:
		return compareLike(fieldValue, c.Value)
	case OpIn:
		return compareIn(fieldValue, c.Value)
	case OpNotIn:
		result, err := compareIn(fieldValue, c.Value)
		return !result, err
	default:
		return false, fmt.Errorf("unsupported operator: %s", c.Operator)
	}
}

// compareEqual compares two values for equality.
func compareEqual(a, b interface{}) (bool, error) {
	aStr, bStr := stringify(a), stringify(b)
	return aStr == bStr, nil
}

// compareLess compares two values for less than.
func compareLess(a, b interface{}) (bool, error) {
	aNum, okA := toFloat64(a)
	bNum, okB := toFloat64(b)

	if !okA || !okB {
		// Fall back to string comparison
		return stringify(a) < stringify(b), nil
	}

	return aNum < bNum, nil
}

// compareLike performs a LIKE comparison.
func compareLike(a, b interface{}) (bool, error) {
	pattern, ok := b.(string)
	if !ok {
		return false, fmt.Errorf("LIKE pattern must be string")
	}

	value := stringify(a)

	// Convert SQL LIKE pattern to regex
	// % -> .*, _ -> .
	regexPattern := strings.ReplaceAll(regexp.QuoteMeta(pattern), "%", ".*")
	regexPattern = strings.ReplaceAll(regexPattern, "_", ".")
	regexPattern = "^" + regexPattern + "$"

	matched, err := regexp.MatchString(regexPattern, value)
	if err != nil {
		return false, err
	}

	return matched, nil
}

// compareIn checks if a value is in a list.
func compareIn(a, b interface{}) (bool, error) {
	list, ok := b.([]interface{})
	if !ok {
		return false, fmt.Errorf("IN value must be a list")
	}

	aStr := stringify(a)
	for _, item := range list {
		if stringify(item) == aStr {
			return true, nil
		}
	}

	return false, nil
}

// stringify converts a value to its string representation.
func stringify(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case int, int32, int64:
		return fmt.Sprintf("%d", val)
	case uint, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%f", val)
	case bool:
		return strconv.FormatBool(val)
	case time.Time:
		return fmt.Sprintf("%d", val.UnixMilli())
	default:
		return fmt.Sprintf("%v", val)
	}
}

// toFloat64 attempts to convert a value to float64.
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// QueryParser parses SQL WHERE clauses.
type QueryParser struct{}

// NewQueryParser creates a new QueryParser.
func NewQueryParser() *QueryParser {
	return &QueryParser{}
}

// ParseWhere parses a WHERE clause string into a WhereExpression.
func (p *QueryParser) ParseWhere(whereClause string) (*WhereExpression, error) {
	if strings.TrimSpace(whereClause) == "" {
		return &WhereExpression{}, nil
	}

	// Simple parser: split by AND first, then by OR
	// This is a simplified implementation; a full SQL parser would be more complex

	// Split by AND (outside of parentheses)
	andParts := p.splitByLogicalOp(whereClause, "AND")

	expr := &WhereExpression{
		Conditions: make([]Condition, 0, len(andParts)),
		LogicalOps:  make([]LogicalOperator, 0, len(andParts)-1),
	}

	for i, part := range andParts {
		part = strings.TrimSpace(part)

		// Check if this part contains OR
		orParts := p.splitByLogicalOp(part, "OR")
		if len(orParts) > 1 {
			// Handle OR by combining conditions
			for j, orPart := range orParts {
				cond, err := p.parseCondition(orPart)
				if err != nil {
					return nil, err
				}
				expr.Conditions = append(expr.Conditions, cond)
				if j < len(orParts)-1 {
					expr.LogicalOps = append(expr.LogicalOps, LogOr)
				}
			}
		} else {
			cond, err := p.parseCondition(part)
			if err != nil {
				return nil, err
			}
			expr.Conditions = append(expr.Conditions, cond)
		}

		if i < len(andParts)-1 && len(orParts) == 1 {
			expr.LogicalOps = append(expr.LogicalOps, LogAnd)
		}
	}

	return expr, nil
}

// splitByLogicalOp splits a string by a logical operator, respecting parentheses.
func (p *QueryParser) splitByLogicalOp(s, op string) []string {
	var parts []string
	var current strings.Builder
	depth := 0

	for i := 0; i < len(s); i++ {
		c := s[i]

		if c == '(' {
			depth++
			current.WriteByte(c)
		} else if c == ')' {
			depth--
			current.WriteByte(c)
		} else if depth == 0 {
			// Check if we're at the logical operator
			if i+len(op) <= len(s) && s[i:i+len(op)] == op {
				// Check if it's a word boundary
				beforeOk := i == 0 || !isWordChar(s[i-1])
				afterOk := i+len(op) >= len(s) || !isWordChar(s[i+len(op)])

				if beforeOk && afterOk {
					parts = append(parts, strings.TrimSpace(current.String()))
					current.Reset()
					i += len(op) - 1
					continue
				}
			}
			current.WriteByte(c)
		} else {
			current.WriteByte(c)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}

	if len(parts) == 0 {
		return []string{s}
	}

	return parts
}

// isWordChar checks if a character is a word character.
func isWordChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// parseCondition parses a single condition.
func (p *QueryParser) parseCondition(s string) (Condition, error) {
	s = strings.TrimSpace(s)

	// Remove surrounding parentheses
	if len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' {
		s = s[1 : len(s)-1]
		s = strings.TrimSpace(s)
	}

	// Try to match each operator in order of precedence
	operators := []Operator{
		OpNotIn,
		OpIn,
		OpLike,
		OpGreaterEqual,
		OpLessEqual,
		OpNotEqual,
		OpGreaterThan,
		OpLessThan,
		OpEqual,
	}

	for _, op := range operators {
		opStr := string(op)
		idx := strings.Index(strings.ToUpper(s), opStr)
		if idx != -1 {
			field := strings.TrimSpace(s[:idx])
			valueStr := strings.TrimSpace(s[idx+len(opStr):])

			// Parse value
			value, err := p.parseValue(valueStr)
			if err != nil {
				return Condition{}, err
			}

			return Condition{
				Field:    field,
				Operator: op,
				Value:    value,
			}, nil
		}
	}

	return Condition{}, fmt.Errorf("could not parse condition: %s", s)
}

// parseValue parses a value string into an interface{}.
func (p *QueryParser) parseValue(s string) (interface{}, error) {
	s = strings.TrimSpace(s)

	// Check for list (IN clause)
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		s = s[1 : len(s)-1]
		parts := strings.Split(s, ",")
		values := make([]interface{}, 0, len(parts))
		for _, part := range parts {
			val, err := p.parseSingleValue(strings.TrimSpace(part))
			if err != nil {
				return nil, err
			}
			values = append(values, val)
		}
		return values, nil
	}

	return p.parseSingleValue(s)
}

// parseSingleValue parses a single value (not a list).
func (p *QueryParser) parseSingleValue(s string) (interface{}, error) {
	s = strings.TrimSpace(s)

	// Remove quotes if present
	if len(s) >= 2 && (s[0] == '\'' && s[len(s)-1] == '\'') {
		return s[1 : len(s)-1], nil
	}
	if len(s) >= 2 && (s[0] == '"' && s[len(s)-1] == '"') {
		return s[1 : len(s)-1], nil
	}

	// Try to parse as int
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, nil
	}

	// Try to parse as float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}

	// Try to parse as bool
	if strings.EqualFold(s, "true") {
		return true, nil
	}
	if strings.EqualFold(s, "false") {
		return false, nil
	}

	// Return as string
	return s, nil
}

// ParseColumns parses a comma-separated list of column names.
func (p *QueryParser) ParseColumns(columnsStr string) []string {
	if strings.TrimSpace(columnsStr) == "" || strings.TrimSpace(columnsStr) == "*" {
		return nil // nil means all columns
	}

	columns := strings.Split(columnsStr, ",")
	result := make([]string, 0, len(columns))
	for _, col := range columns {
		col = strings.TrimSpace(col)
		if col != "" {
			result = append(result, col)
		}
	}
	return result
}

// ParseLimit parses a LIMIT clause.
func (p *QueryParser) ParseLimit(limitStr string) (int, error) {
	if strings.TrimSpace(limitStr) == "" {
		return 0, nil // 0 means no limit
	}

	return strconv.Atoi(strings.TrimSpace(limitStr))
}

// ParseOrderBy parses an ORDER BY clause.
func (p *QueryParser) ParseOrderBy(orderByStr string) ([]OrderByField, error) {
	if strings.TrimSpace(orderByStr) == "" {
		return nil, nil
	}

	parts := strings.Split(orderByStr, ",")
	result := make([]OrderByField, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		components := strings.Fields(part)

		if len(components) == 0 {
			continue
		}

		field := components[0]
		ascending := true

		if len(components) > 1 {
			if strings.ToUpper(components[1]) == "DESC" {
				ascending = false
			}
		}

		result = append(result, OrderByField{
			Field:     field,
			Ascending: ascending,
		})
	}

	return result, nil
}

// OrderByField represents a field in an ORDER BY clause.
type OrderByField struct {
	Field     string
	Ascending bool
}

// TableQuery represents a parsed table query.
type TableQuery struct {
	TableName    string
	Columns      []string
	Where        *WhereExpression
	OrderBy      []OrderByField
	Limit        int
	Offset       int
}

// ParseTableQuery parses a full table query string.
func (p *QueryParser) ParseTableQuery(query string) (*TableQuery, error) {
	// Simple SQL-like parser
	// Format: SELECT col1, col2 FROM table WHERE cond ORDER BY field LIMIT n

	query = strings.TrimSpace(query)
	if query == "" {
		return nil, fmt.Errorf("empty query")
	}

	result := &TableQuery{}

	// Parse SELECT
	selectIdx := strings.Index(strings.ToUpper(query), "SELECT")
	if selectIdx == -1 {
		return nil, fmt.Errorf("missing SELECT clause")
	}

	fromIdx := strings.Index(strings.ToUpper(query), "FROM")
	if fromIdx == -1 {
		return nil, fmt.Errorf("missing FROM clause")
	}

	// Parse columns
	columnsStr := query[selectIdx+6:fromIdx]
	result.Columns = p.ParseColumns(columnsStr)

	// Parse table name
	remaining := query[fromIdx+4:]
	whereIdx := strings.Index(strings.ToUpper(remaining), "WHERE")

	var tableName string
	if whereIdx != -1 {
		tableName = remaining[:whereIdx]
		remaining = remaining[whereIdx+5:]
	} else {
		orderIdx := strings.Index(strings.ToUpper(remaining), "ORDER BY")
		limitIdx := strings.Index(strings.ToUpper(remaining), "LIMIT")

		var nextIdx int = -1
		if orderIdx != -1 && (nextIdx == -1 || orderIdx < nextIdx) {
			nextIdx = orderIdx
		}
		if limitIdx != -1 && (nextIdx == -1 || limitIdx < nextIdx) {
			nextIdx = limitIdx
		}

		if nextIdx != -1 {
			tableName = remaining[:nextIdx]
		} else {
			tableName = remaining
		}
		remaining = ""
	}
	result.TableName = strings.TrimSpace(tableName)

	// Parse WHERE
	if whereIdx != -1 {
		orderIdx := strings.Index(strings.ToUpper(remaining), "ORDER BY")
		limitIdx := strings.Index(strings.ToUpper(remaining), "LIMIT")

		var whereEnd int = -1
		if orderIdx != -1 && (whereEnd == -1 || orderIdx < whereEnd) {
			whereEnd = orderIdx
		}
		if limitIdx != -1 && (whereEnd == -1 || limitIdx < whereEnd) {
			whereEnd = limitIdx
		}

		var whereStr string
		if whereEnd != -1 {
			whereStr = remaining[:whereEnd]
			remaining = remaining[whereEnd:]
		} else {
			whereStr = remaining
			remaining = ""
		}

		where, err := p.ParseWhere(whereStr)
		if err != nil {
			return nil, fmt.Errorf("parse WHERE: %w", err)
		}
		result.Where = where
	}

	// Parse ORDER BY
	orderIdx := strings.Index(strings.ToUpper(remaining), "ORDER BY")
	if orderIdx != -1 {
		remaining = remaining[orderIdx+8:]
		limitIdx := strings.Index(strings.ToUpper(remaining), "LIMIT")

		var orderByStr string
		if limitIdx != -1 {
			orderByStr = remaining[:limitIdx]
			remaining = remaining[limitIdx:]
		} else {
			orderByStr = remaining
			remaining = ""
		}

		orderBy, err := p.ParseOrderBy(orderByStr)
		if err != nil {
			return nil, fmt.Errorf("parse ORDER BY: %w", err)
		}
		result.OrderBy = orderBy
	}

	// Parse LIMIT
	limitIdx := strings.Index(strings.ToUpper(remaining), "LIMIT")
	if limitIdx != -1 {
		limitStr := remaining[limitIdx+5:]
		limit, err := p.ParseLimit(limitStr)
		if err != nil {
			return nil, fmt.Errorf("parse LIMIT: %w", err)
		}
		result.Limit = limit
	}

	return result, nil
}
