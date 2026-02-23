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
	"fmt"
	"sync"
	"time"

	stmtsummaryv3proto "github.com/pingcap/tidb/pkg/util/stmtsummary/v3/proto/v1"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PushTargetControlServer implements the StatementPushControl service.
// This service allows Vector to proactively register with TiDB, eliminating
// the need for TiDB to configure Vector's endpoint manually.
type PushTargetControlServer struct {
	stmtsummaryv3proto.UnimplementedStatementPushControlServer

	// Cluster and instance identification
	clusterID  string
	instanceID string

	// Registered Vector information (only one at a time for now)
	mu              sync.RWMutex
	registeredVector *RegisteredVector

	// Reference to StatementV3 for creating/updating pusher
	stmtV3 *StatementV3
}

// RegisteredVector holds information about a registered Vector instance.
type RegisteredVector struct {
	VectorEndpoint      string
	VectorInstanceID    string
	VectorVersion       string
	RegisteredAt        time.Time
	LastUpdatedAt       time.Time
	ConfigVersion       int64
	CollectionConfig    *stmtsummaryv3proto.CollectionConfig
}

// NewPushTargetControlServer creates a new PushTargetControlServer.
func NewPushTargetControlServer(clusterID, instanceID string, stmtV3 *StatementV3) *PushTargetControlServer {
	return &PushTargetControlServer{
		clusterID:  clusterID,
		instanceID: instanceID,
		stmtV3:     stmtV3,
	}
}

// RegisterPushTarget handles Vector's registration request.
// Vector calls this to register itself as the push target for TiDB.
func (s *PushTargetControlServer) RegisterPushTarget(ctx context.Context, req *stmtsummaryv3proto.RegisterPushTargetRequest) (*stmtsummaryv3proto.RegisterPushTargetResponse, error) {
	logutil.BgLogger().Info("RegisterPushTarget called",
		zap.String("vector_endpoint", req.VectorEndpoint),
		zap.String("vector_instance_id", req.VectorInstanceId),
		zap.String("vector_version", req.VectorVersion))

	if req.VectorEndpoint == "" {
		return nil, status.Error(codes.InvalidArgument, "vector_endpoint is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Store the registered Vector information
	now := time.Now()
	if s.registeredVector != nil {
		// Update existing registration
		s.registeredVector.VectorEndpoint = req.VectorEndpoint
		s.registeredVector.LastUpdatedAt = now
		if req.CollectionConfig != nil {
			s.registeredVector.CollectionConfig = req.CollectionConfig
			s.registeredVector.ConfigVersion = req.CollectionConfig.ConfigVersion
		}
		logutil.BgLogger().Info("Updating existing Vector registration",
			zap.String("previous_endpoint", s.registeredVector.VectorEndpoint),
			zap.String("new_endpoint", req.VectorEndpoint))
	} else {
		// New registration
		s.registeredVector = &RegisteredVector{
			VectorEndpoint:   req.VectorEndpoint,
			VectorInstanceID: req.VectorInstanceId,
			VectorVersion:    req.VectorVersion,
			RegisteredAt:     now,
			LastUpdatedAt:    now,
			CollectionConfig: req.CollectionConfig,
		}
		if req.CollectionConfig != nil {
			s.registeredVector.ConfigVersion = req.CollectionConfig.ConfigVersion
		}
	}

	// Apply the configuration and create/update the pusher
	appliedConfig, err := s.applyPushTarget(req.VectorEndpoint, req.CollectionConfig)
	if err != nil {
		logutil.BgLogger().Error("Failed to apply push target configuration",
			zap.Error(err))
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to apply config: %v", err))
	}

	logutil.BgLogger().Info("Successfully registered Vector as push target",
		zap.String("endpoint", req.VectorEndpoint),
		zap.String("config_version", fmt.Sprint(s.registeredVector.ConfigVersion)))

	return &stmtsummaryv3proto.RegisterPushTargetResponse{
		Success:       true,
		Message:       "Successfully registered Vector as push target",
		AppliedConfig: appliedConfig,
		RegisteredAtMs: now.UnixMilli(),
	}, nil
}

// UnregisterPushTarget handles Vector's unregistration request.
func (s *PushTargetControlServer) UnregisterPushTarget(ctx context.Context, req *stmtsummaryv3proto.UnregisterPushTargetRequest) (*stmtsummaryv3proto.UnregisterPushTargetResponse, error) {
	logutil.BgLogger().Info("UnregisterPushTarget called",
		zap.String("vector_instance_id", req.VectorInstanceId),
		zap.Bool("stop_immediately", req.StopImmediately))

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.registeredVector == nil {
		return &stmtsummaryv3proto.UnregisterPushTargetResponse{
			Success: false,
			Message: "No Vector is currently registered",
		}, nil
	}

	// Verify instance ID matches
	if s.registeredVector.VectorInstanceID != req.VectorInstanceId {
		return &stmtsummaryv3proto.UnregisterPushTargetResponse{
			Success: false,
			Message: fmt.Sprintf("Instance ID mismatch: expected %s, got %s",
				s.registeredVector.VectorInstanceID, req.VectorInstanceId),
		}, nil
	}

	// Stop the pusher
	if s.stmtV3 != nil {
		s.stmtV3.closePusher()
	}

	s.registeredVector = nil

	now := time.Now()
	logutil.BgLogger().Info("Successfully unregistered Vector",
		zap.Int64("timestamp_ms", now.UnixMilli()))

	return &stmtsummaryv3proto.UnregisterPushTargetResponse{
		Success:         true,
		Message:         "Successfully unregistered Vector",
		UnregisteredAtMs: now.UnixMilli(),
	}, nil
}

// UpdatePushConfig handles Vector's config update request.
func (s *PushTargetControlServer) UpdatePushConfig(ctx context.Context, req *stmtsummaryv3proto.UpdatePushConfigRequest) (*stmtsummaryv3proto.UpdatePushConfigResponse, error) {
	logutil.BgLogger().Info("UpdatePushConfig called",
		zap.String("vector_instance_id", req.VectorInstanceId))

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.registeredVector == nil {
		return nil, status.Error(codes.FailedPrecondition, "No Vector is currently registered")
	}

	// Verify instance ID matches
	if s.registeredVector.VectorInstanceID != req.VectorInstanceId {
		return nil, status.Error(codes.PermissionDenied, fmt.Sprintf("Instance ID mismatch: expected %s, got %s",
			s.registeredVector.VectorInstanceID, req.VectorInstanceId))
	}

	if req.CollectionConfig == nil {
		return nil, status.Error(codes.InvalidArgument, "collection_config is required")
	}

	// Update the stored config
	s.registeredVector.CollectionConfig = req.CollectionConfig
	s.registeredVector.ConfigVersion = req.CollectionConfig.ConfigVersion
	s.registeredVector.LastUpdatedAt = time.Now()

	// Apply the new configuration
	appliedConfig, err := s.applyPushTarget(s.registeredVector.VectorEndpoint, req.CollectionConfig)
	if err != nil {
		logutil.BgLogger().Error("Failed to apply updated config",
			zap.Error(err))
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to apply config: %v", err))
	}

	now := time.Now()
	logutil.BgLogger().Info("Successfully updated push config",
		zap.Int64("config_version", req.CollectionConfig.ConfigVersion),
		zap.Int64("timestamp_ms", now.UnixMilli()))

	return &stmtsummaryv3proto.UpdatePushConfigResponse{
		Success:      true,
		Message:      "Successfully updated push config",
		AppliedConfig: appliedConfig,
		UpdatedAtMs:  now.UnixMilli(),
	}, nil
}

// applyPushTarget applies the push target configuration to StatementV3.
// It creates or updates the pusher with the provided endpoint and config.
func (s *PushTargetControlServer) applyPushTarget(endpoint string, configProto *stmtsummaryv3proto.CollectionConfig) (*stmtsummaryv3proto.CollectionConfig, error) {
	if s.stmtV3 == nil {
		return nil, fmt.Errorf("StatementV3 is not initialized")
	}

	// Get current config and make a copy
	cfg := s.stmtV3.getConfigCopy()

	// Apply remote config from Vector
	if configProto != nil {
		cfg.MergeFromRemote(configProto)
	}

	// Set the endpoint from Vector
	cfg.Push.Enabled = true
	cfg.Push.Endpoint = endpoint

	// Apply the updated config to StatementV3 (including Aggregator)
	// This must be done before closing the old pusher to avoid race conditions
	s.stmtV3.ApplyConfig(cfg)

	// Close existing pusher if any
	s.stmtV3.closePusher()

	// Create new pusher with the updated config
	pusher, err := NewPusher(cfg, s.clusterID, s.instanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to create pusher: %w", err)
	}

	// Set up the connection between aggregator and pusher
	s.stmtV3.setAggregatorOnFlush(func(window *AggregationWindow) {
		pusher.Push(window)
	})

	// Register config change callback
	pusher.SetOnConfigChange(func(newCfg *Config) {
		s.stmtV3.ApplyConfig(newCfg)
	})

	// Set the new pusher
	s.stmtV3.setPusher(pusher)

	// Build the applied config proto for response
	appliedConfig := buildCollectionConfigProto(cfg)

	return appliedConfig, nil
}

// GetRegisteredVector returns the currently registered Vector information (for testing).
func (s *PushTargetControlServer) GetRegisteredVector() *RegisteredVector {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.registeredVector == nil {
		return nil
	}
	// Return a copy
	cp := *s.registeredVector
	return &cp
}

// buildCollectionConfigProto converts a Config to CollectionConfig proto.
func buildCollectionConfigProto(cfg *Config) *stmtsummaryv3proto.CollectionConfig {
	cc := &stmtsummaryv3proto.CollectionConfig{
		AggregationWindowSecs: int32(cfg.AggregationWindow.Seconds()),
		EnableInternalQuery:   cfg.EnableInternalQuery,
		PushBatchSize:         int32(cfg.Push.BatchSize),
		PushIntervalSecs:      int32(cfg.Push.Interval.Seconds()),
		PushTimeoutSecs:       int32(cfg.Push.Timeout.Seconds()),
		MaxDigestsPerWindow:   int32(cfg.Memory.MaxDigestsPerWindow),
		MaxMemoryBytes:        cfg.Memory.MaxMemoryBytes,
		EvictionStrategy:      string(cfg.Memory.EvictionStrategy),
		EarlyFlushThreshold:   cfg.Memory.EarlyFlushThreshold,
		RetryMaxAttempts:      int32(cfg.Push.Retry.MaxAttempts),
		RetryInitialDelayMs:   int32(cfg.Push.Retry.InitialDelay.Milliseconds()),
		RetryMaxDelayMs:       int32(cfg.Push.Retry.MaxDelay.Milliseconds()),
		ConfigVersion:         0, // Will be set by caller
	}

	// Convert extended metrics
	if len(cfg.ExtendedMetrics) > 0 {
		cc.ExtendedMetrics = make([]*stmtsummaryv3proto.ExtendedMetricDef, len(cfg.ExtendedMetrics))
		for i, em := range cfg.ExtendedMetrics {
			cc.ExtendedMetrics[i] = &stmtsummaryv3proto.ExtendedMetricDef{
				Name:        em.Name,
				Type:        em.Type,
				Source:      em.Source,
				Enabled:     em.Enabled,
				Aggregation: em.Aggregation,
			}
		}
	}

	return cc
}
