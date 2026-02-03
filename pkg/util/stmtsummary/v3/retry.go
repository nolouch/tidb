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
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// RetryPolicy defines the retry policy for push operations.
type RetryPolicy struct {
	// MaxAttempts is the maximum number of retry attempts.
	MaxAttempts int

	// InitialDelay is the initial delay before the first retry.
	InitialDelay time.Duration

	// MaxDelay is the maximum delay between retries.
	MaxDelay time.Duration

	// Multiplier is the exponential backoff multiplier.
	Multiplier float64

	// Jitter is the randomization factor (0.0 to 1.0).
	Jitter float64
}

// DefaultRetryPolicy returns a default retry policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
		Jitter:       0.1,
	}
}

// RetryExecutor executes a function with retry logic.
type RetryExecutor struct {
	policy RetryPolicy
}

// NewRetryExecutor creates a new RetryExecutor.
func NewRetryExecutor(policy RetryPolicy) *RetryExecutor {
	return &RetryExecutor{policy: policy}
}

// Execute executes the given function with retry logic.
func (r *RetryExecutor) Execute(ctx context.Context, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt < r.policy.MaxAttempts; attempt++ {
		if err := fn(); err != nil {
			lastErr = err

			// Check if context is cancelled
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Don't retry on the last attempt
			if attempt == r.policy.MaxAttempts-1 {
				break
			}

			// Record retry attempt
			RetryAttemptsTotal.Inc()

			// Calculate delay with exponential backoff and jitter
			delay := r.calculateDelay(attempt)

			logutil.BgLogger().Warn("push attempt failed, will retry",
				zap.Int("attempt", attempt+1),
				zap.Int("maxAttempts", r.policy.MaxAttempts),
				zap.Duration("delay", delay),
				zap.Error(err))

			// Wait before retry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		} else {
			return nil
		}
	}

	return lastErr
}

// calculateDelay calculates the delay for a given attempt.
func (r *RetryExecutor) calculateDelay(attempt int) time.Duration {
	// Calculate exponential delay
	delay := float64(r.policy.InitialDelay) * math.Pow(r.policy.Multiplier, float64(attempt))

	// Apply max delay cap
	if delay > float64(r.policy.MaxDelay) {
		delay = float64(r.policy.MaxDelay)
	}

	// Apply jitter
	if r.policy.Jitter > 0 {
		jitter := delay * r.policy.Jitter * (2*rand.Float64() - 1)
		delay += jitter
	}

	return time.Duration(delay)
}

// CircuitBreakerConfig defines the configuration for the circuit breaker.
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of failures before opening the circuit.
	FailureThreshold int

	// SuccessThreshold is the number of successes in half-open state before closing.
	SuccessThreshold int

	// Timeout is the duration the circuit stays open before transitioning to half-open.
	Timeout time.Duration
}

// DefaultCircuitBreakerConfig returns a default circuit breaker configuration.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 3,
		Timeout:          30 * time.Second,
	}
}

// CircuitBreaker implements the circuit breaker pattern.
type CircuitBreaker struct {
	config CircuitBreakerConfig

	// State
	state        CircuitState
	failures     int
	successes    int
	lastFailure  time.Time
	stateChanged time.Time

	// Callbacks
	onStateChange func(from, to CircuitState)
}

// NewCircuitBreaker creates a new CircuitBreaker.
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		config:        config,
		state:         CircuitClosed,
		stateChanged:  time.Now(),
		onStateChange: nil,
	}
}

// SetOnStateChange sets the callback for state changes.
func (cb *CircuitBreaker) SetOnStateChange(fn func(from, to CircuitState)) {
	cb.onStateChange = fn
}

// Allow returns true if the operation is allowed to proceed.
func (cb *CircuitBreaker) Allow() bool {
	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		// Check if timeout has elapsed
		if time.Since(cb.stateChanged) > cb.config.Timeout {
			cb.transitionTo(CircuitHalfOpen)
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	default:
		return true
	}
}

// RecordSuccess records a successful operation.
func (cb *CircuitBreaker) RecordSuccess() {
	switch cb.state {
	case CircuitClosed:
		// Reset failures in closed state
		cb.failures = 0
	case CircuitHalfOpen:
		cb.successes++
		if cb.successes >= cb.config.SuccessThreshold {
			cb.transitionTo(CircuitClosed)
		}
	}
}

// RecordFailure records a failed operation.
func (cb *CircuitBreaker) RecordFailure() {
	cb.lastFailure = time.Now()

	switch cb.state {
	case CircuitClosed:
		cb.failures++
		if cb.failures >= cb.config.FailureThreshold {
			cb.transitionTo(CircuitOpen)
		}
	case CircuitHalfOpen:
		cb.transitionTo(CircuitOpen)
	}
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() CircuitState {
	return cb.state
}

// transitionTo transitions the circuit breaker to a new state.
func (cb *CircuitBreaker) transitionTo(newState CircuitState) {
	if cb.state == newState {
		return
	}

	oldState := cb.state
	cb.state = newState
	cb.stateChanged = time.Now()

	// Reset counters on state change
	switch newState {
	case CircuitClosed:
		cb.failures = 0
		cb.successes = 0
	case CircuitOpen:
		cb.successes = 0
	case CircuitHalfOpen:
		cb.failures = 0
		cb.successes = 0
	}

	logutil.BgLogger().Info("circuit breaker state changed",
		zap.String("from", oldState.String()),
		zap.String("to", newState.String()))

	if cb.onStateChange != nil {
		cb.onStateChange(oldState, newState)
	}
}

// Metrics returns the current circuit breaker metrics.
func (cb *CircuitBreaker) Metrics() CircuitBreakerMetrics {
	return CircuitBreakerMetrics{
		State:         cb.state.String(),
		Failures:      cb.failures,
		Successes:     cb.successes,
		LastFailure:   cb.lastFailure,
		StateChanged:  cb.stateChanged,
	}
}

// CircuitBreakerMetrics holds metrics about the circuit breaker.
type CircuitBreakerMetrics struct {
	State        string
	Failures     int
	Successes    int
	LastFailure  time.Time
	StateChanged time.Time
}
