// Copyright 2023 PingCAP, Inc.
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

package remote

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/pingcap/tidb/table"
	"github.com/tikv/client-go/v2/oracle"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	rm "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

/*
	Remote load data worker API:

	1. init task:
		POST /load_data?cluster_id=%d&start_ts=%d&commit_ts=%d

	2. put chunk:
		PUT /load_data?cluster_id=%d&start_ts=%d&chunk_id=%d

		key_len(2) + key(key_len) + val_len(4) + value(val_len)
		key_len(2) + key(key_len) + val_len(4) + value(val_len)
		...

	3. build task:
		POST /load_data?cluster_id=%d&start_ts=%d&build=true&compression=zstd&split_size=%d&split_keys=%d

	4. get task states:
		GET /load_data?cluster_id=%d&start_ts=%d

		{"canceled": false, "finished": false, "error": "", "created-files": 10, "ingested-regions": 3}

	5. clean up task:
		DELETE /load_data?cluster_id=%d&start_ts=%d
*/

// LoadDataStates is json data that returned by remote server GET API.
type LoadDataStates struct {
	Canceled        bool   `json:"canceled"`
	Finished        bool   `json:"finished"`
	Error           string `json:"error"`
	CreatedFiles    int    `json:"created-files"`
	IngestedRegions int    `json:"ingested-regions"`
}

func NewRemoteBackend(
	ctx context.Context,
	tls *common.TLS,
	cfg *config.Config,
	g glue.Glue,
	keyspaceName string,
) (backend.Backend, error) {
	pdCtl, err := pdutil.NewPdController(ctx, keyspaceName, cfg.TiDB.PdAddr, tls.TLSConfig(), tls.ToPDSecurityOption())
	if err != nil {
		return backend.MakeBackend(nil), common.NormalizeOrWrapErr(common.ErrCreatePDClient, err)
	}

	var pdCliForTiKV *tikvclient.CodecPDClient
	if keyspaceName == "" {
		pdCliForTiKV = tikvclient.NewCodecPDClient(tikvclient.ModeTxn, pdCtl.GetPDClient())
	} else {
		pdCliForTiKV, err = tikvclient.NewCodecPDClientWithKeyspace(tikvclient.ModeTxn, pdCtl.GetPDClient(), keyspaceName)
		if err != nil {
			return backend.MakeBackend(nil), common.ErrCreatePDClient.Wrap(err).GenWithStackByArgs()
		}
	}

	keyspace := pdCliForTiKV.GetCodec().GetKeyspace()
	worker := &remoteBackend{
		encBuilder:       local.NewEncodingBuilder(ctx),
		targetInfoGetter: local.NewTargetInfoGetter(tls, g, cfg.TiDB.PdAddr, keyspaceName),
		workerAddr:       cfg.TikvImporter.Addr,
		pdCtl:            pdCtl,
		tls:              tls,
		pdAddr:           cfg.TiDB.PdAddr,
		g:                g,
		keyspace:         keyspace,
		logger:           log.FromContext(ctx),
		reportWriteBytes: func(int64) {},
	}

	if m, ok := metric.FromContext(ctx); ok {
		worker.metrics = m
	}

	if cfg.RUConfig.ReportWRU && worker.metrics != nil {
		keyspaceID := uint32(pdCliForTiKV.GetCodec().GetKeyspaceID())
		err = worker.setupReportWRU(ctx, keyspaceID, keyspaceName, cfg)
		if err != nil {
			worker.logger.Warn("failed to setup report wru", zap.Error(err))
			return backend.MakeBackend(nil), err
		}
	}

	return backend.MakeBackend(worker), nil
}

type remoteBackend struct {
	encBuilder       backend.EncodingBuilder
	targetInfoGetter backend.TargetInfoGetter
	workerAddr       string
	pdCtl            *pdutil.PdController
	tls              *common.TLS
	pdAddr           string
	g                glue.Glue
	keyspace         []byte
	metrics          *metric.Metrics
	dupRows          kv.Rows
	engines          sync.Map
	logger           log.Logger
	reportWriteBytes func(int64)
}

// Close the connection to the backend.
func (b *remoteBackend) Close() {
}

// MakeEmptyRows creates an empty collection of encoded rows.
func (b *remoteBackend) MakeEmptyRows() kv.Rows {
	return b.encBuilder.MakeEmptyRows()
}

// RetryImportDelay returns the duration to sleep when retrying an import
func (b *remoteBackend) RetryImportDelay() time.Duration {
	return 0
}

// ShouldPostProcess returns whether KV-specific post-processing should be
// performed for this backend. Post-processing includes checksum and analyze.
func (b *remoteBackend) ShouldPostProcess() bool {
	return true
}

// NewEncoder creates an encoder of a TiDB table.
func (b *remoteBackend) NewEncoder(ctx context.Context, tbl table.Table, options *kv.SessionOptions) (kv.Encoder, error) {
	return b.encBuilder.NewEncoder(ctx, tbl, options)
}

func (b *remoteBackend) OpenEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	physical, logical, err := b.pdCtl.GetPDClient().GetTS(ctx)
	if err != nil {
		return err
	}
	ts := oracle.ComposeTS(physical, logical)
	e, _ := b.engines.LoadOrStore(engineUUID, &engine{
		tbl:              cfg.TableInfo,
		addr:             b.workerAddr,
		clusterID:        b.pdCtl.GetPDClient().GetClusterID(ctx),
		ts:               ts,
		reportWriteBytes: b.reportWriteBytes,
	})
	engine := e.(*engine)
	if engine.ts == ts {
		// newly created engine.
		err = b.loadDataInit(engine.clusterID, engine.ts)
		if err != nil {
			b.engines.Delete(engineUUID)
			return err
		}
	}
	return nil
}

func (b *remoteBackend) loadDataInit(clusterID, ts uint64) error {
	url := fmt.Sprintf(
		"%s/load_data?cluster_id=%d&start_ts=%d&commit_ts=%d",
		b.workerAddr,
		clusterID,
		ts,
		ts+1)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("failed to open engine %s", resp.Status)
	}
	return nil
}

func (b *remoteBackend) CloseEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	return nil
}

func (b *remoteBackend) ImportEngine(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		return err
	}
	err = b.loadDataBuild(engine, ctx, regionSplitSize, regionSplitKeys)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			states, err1 := b.loadDataGetStates(engine, ctx)
			if err1 != nil {
				return err1
			}
			if states.Canceled {
				b.logger.Error("loadData canceled", zap.Uint64("start_ts", engine.ts), zap.String("error", states.Error))
				return errors.Errorf("load data canceled, start_ts:%d, error:%s", engine.ts, states.Error)
			} else if states.Finished {
				b.logger.Info("loadData finished", zap.Uint64("start_ts", engine.ts))
				return nil
			} else {
				b.logger.Info(
					"loadData states",
					zap.Uint64("start_ts", engine.ts),
					zap.Int("created_files", states.CreatedFiles),
					zap.Int("ingested_regions", states.IngestedRegions))
			}
		}
	}
}

func sendRequest(ctx context.Context, method, url string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, errors.Errorf("failed to create request %s", url)
	}
	req.WithContext(ctx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Errorf("failed to send request %s", url)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("failed to send request %s, status %s", url, resp.Status)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (b *remoteBackend) loadDataBuild(engine *engine, ctx context.Context, splitSize, splitKeys int64) error {
	maxChunkID := engine.chunkID.Load()
	url := fmt.Sprintf("%s/load_data?cluster_id=%d&start_ts=%d&build=true&compression=zstd&split_size=%d&split_keys=%d",
		b.workerAddr, engine.clusterID, engine.ts, splitSize, splitKeys)
	chunkIDs := make([]uint64, 0, maxChunkID)
	for i := uint64(1); i <= maxChunkID; i++ {
		chunkIDs = append(chunkIDs, i)
	}
	jsonData, err := json.Marshal(chunkIDs)
	if err != nil {
		return err
	}
	_, err = sendRequest(ctx, "POST", url, bytes.NewReader(jsonData))
	return err
}

func (b *remoteBackend) loadDataGetStates(engine *engine, ctx context.Context) (*LoadDataStates, error) {
	url := fmt.Sprintf("%s/load_data?cluster_id=%d&start_ts=%d", b.workerAddr, engine.clusterID, engine.ts)
	data, err := sendRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	states := new(LoadDataStates)
	err = json.Unmarshal(data, states)
	if err != nil {
		return nil, err
	}
	return states, nil
}

func (b *remoteBackend) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		return err
	}
	return b.loadDataCleanUp(engine, ctx)
}

func (b *remoteBackend) loadDataCleanUp(engine *engine, ctx context.Context) error {
	url := fmt.Sprintf("%s/load_data?cluster_id=%d&start_ts=%d", b.workerAddr, engine.clusterID, engine.ts)
	_, err := sendRequest(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}
	return nil
}

// CheckRequirements performs the check whether the backend satisfies the
// version requirements
func (b *remoteBackend) CheckRequirements(context.Context, *backend.CheckCtx) error {
	return nil
}

// FetchRemoteTableModels obtains the models of all tables given the schema
// name. The returned table info does not need to be precise if the encoder,
// is not requiring them, but must at least fill in the following fields for
// TablesFromMeta to succeed:
//   - Name
//   - State (must be model.StatePublic)
//   - ID
//   - Columns
//   - Name
//   - State (must be model.StatePublic)
//   - Offset (must be 0, 1, 2, ...)
//   - PKIsHandle (true = do not generate _tidb_rowid)
func (b *remoteBackend) FetchRemoteTableModels(ctx context.Context, schemaName string) ([]*model.TableInfo, error) {
	return b.targetInfoGetter.FetchRemoteTableModels(ctx, schemaName)
}

// FlushEngine ensures all KV pairs written to an open engine has been
// synchronized, such that kill-9'ing Lightning afterwards and resuming from
// checkpoint can recover the exact same content.
//
// This method is only relevant for local backend, and is no-op for all
// other backends.
func (b *remoteBackend) FlushEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil
}

// FlushAllEngines performs FlushEngine on all opened engines. This is a
// very expensive operation and should only be used in some rare situation
// (e.g. preparing to resolve a disk quota violation).
func (b *remoteBackend) FlushAllEngines(ctx context.Context) error {
	return nil
}

// EngineFileSizes obtains the size occupied locally of all engines managed
// by this backend. This method is used to compute disk quota.
// It can return nil if the content are all stored remotely.
func (b *remoteBackend) EngineFileSizes() []backend.EngineFileSize {
	return nil
}

// ResetEngine clears all written KV pairs in this opened engine.
func (b *remoteBackend) ResetEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return errors.New("cannot reset an engine in Remote backend")
}

// LocalWriter obtains a thread-local EngineWriter for writing rows into the given engine.
func (b *remoteBackend) LocalWriter(_ context.Context, _ *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		return nil, err
	}
	client := &client{
		e:        engine,
		keyspace: b.keyspace,
	}
	return client, nil
}

func (b *remoteBackend) CollectLocalDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *kv.SessionOptions) (bool, error) {
	// Duplicate check is done on the remote side during writing.
	return false, nil
}

func (b *remoteBackend) CollectRemoteDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *kv.SessionOptions) (bool, error) {
	// Duplicate check is done on the remote side during writing.
	return false, nil
}

func (b *remoteBackend) ResolveDuplicateRows(ctx context.Context, tbl table.Table, tableName string, algorithm config.DuplicateResolutionAlgorithm) error {
	return nil
}

func (b *remoteBackend) TotalMemoryConsume() int64 {
	return 0
}

func (b *remoteBackend) getEngine(engineUUID uuid.UUID) (*engine, error) {
	v, ok := b.engines.Load(engineUUID)
	if !ok {
		return nil, errors.Errorf("could not found engine for %s", engineUUID.String())
	}
	return v.(*engine), nil
}

type engine struct {
	ts               uint64
	tbl              *checkpoints.TidbTableInfo
	addr             string
	clusterID        uint64
	chunkID          atomic.Uint64
	reportWriteBytes func(int64)
}

func (e *engine) allocChunkID() uint64 {
	return e.chunkID.Add(1)
}

// client define a local writer that send KV pairs to remote worker.
type client struct {
	e        *engine
	buf      []byte
	keyspace []byte
}

const BatchSize int = 8 * 1024 * 1024

func (w *client) AppendRows(
	ctx context.Context,
	tableName string,
	columnNames []string,
	rows kv.Rows,
) error {
	kvs := kv.KvPairsFromRows(rows)
	if len(kvs) == 0 {
		return nil
	}
	lenBuf := make([]byte, 4)
	for _, pair := range kvs {
		binary.LittleEndian.PutUint16(lenBuf[:2], uint16(len(pair.Key)+len(w.keyspace)))
		w.buf = append(w.buf, lenBuf[:2]...)
		w.buf = append(w.buf, w.keyspace...)
		w.buf = append(w.buf, pair.Key...)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(pair.Val)))
		w.buf = append(w.buf, lenBuf...)
		w.buf = append(w.buf, pair.Val...)
	}
	if len(w.buf) > BatchSize {
		return w.addChunk(ctx)
	}
	return nil
}

func (w *client) addChunk(ctx context.Context) error {
	// The chunkID must be unique in a task, it doesn't need to be autoincrement.
	chunkID := w.e.allocChunkID()
	url := fmt.Sprintf("%s/load_data?cluster_id=%d&start_ts=%d&chunk_id=%d", w.e.addr, w.e.clusterID, w.e.ts, chunkID)
	// TODO: retry
	_, err := sendRequest(ctx, "PUT", url, bytes.NewReader(w.buf))
	if err != nil {
		return err
	}
	w.e.reportWriteBytes(int64(len(w.buf)))
	w.buf = w.buf[:0]
	return nil
}

func (w *client) IsSynced() bool {
	return false
}

func (w *client) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	if len(w.buf) > 0 {
		err := w.addChunk(ctx)
		if err != nil {
			return status(false), err
		}
	}
	return status(true), nil
}

type status bool

func (s status) Flushed() bool {
	return bool(s)
}

func newRemoteRequestInfo(writeBytes, replicaNumber int64) remoteRequestInfo {
	return remoteRequestInfo{
		writeBytes:    writeBytes,
		replicaNumber: replicaNumber,
	}
}

type remoteRequestInfo struct {
	writeBytes    int64
	replicaNumber int64
}

func (r remoteRequestInfo) IsWrite() bool {
	return true
}

func (r remoteRequestInfo) WriteBytes() uint64 {
	return uint64(r.writeBytes)
}

func (r remoteRequestInfo) ReplicaNumber() int64 {
	return r.replicaNumber
}

func (r remoteRequestInfo) StoreID() uint64 {
	return 0
}

func (b *remoteBackend) setupReportWRU(ctx context.Context, keyspaceID uint32, keyspaceName string, cfg *config.Config) error {
	// remote backend used to import new table data,
	// so we can use default placement rule as repliace count.
	placementRules, err := pdutil.GetPlacementRules(ctx, b.pdAddr, b.tls.TLSConfig())
	if err != nil {
		b.logger.Warn("failed to get placement rules", zap.Error(err))
		return err
	}
	var defaultRule *pdtypes.Rule
	for _, rule := range placementRules {
		if rule.ID == "default" {
			defaultRule = &rule
			break
		}
	}
	if defaultRule == nil {
		b.logger.Warn("failed to get default placement rule")
		return errors.New("failed to get default placement rule")
	}

	kvCalculator := rm.KVCalculator{
		RUConfig: &rm.RUConfig{
			WriteBaseCost:         rm.RequestUnit(cfg.RUConfig.WriteBaseCost),
			WritePerBatchBaseCost: rm.RequestUnit(cfg.RUConfig.WritePerBatchBaseCost),
			WriteBytesCost:        rm.RequestUnit(cfg.RUConfig.WriteCostPerByte),
		},
	}

	wruMetrics := b.metrics.WRUCostCounter.WithLabelValues(fmt.Sprintf("%d", keyspaceID))
	b.reportWriteBytes = func(bytes int64) {
		reqInfo := newRemoteRequestInfo(bytes, int64(defaultRule.Count))
		consumption := &rmpb.Consumption{}
		kvCalculator.BeforeKVRequest(consumption, reqInfo)
		wruMetrics.Add(consumption.WRU)
	}

	b.logger.Info("setup report WRU", zap.Uint32("keyspaceId", keyspaceID),
		zap.String("keyspaceName", keyspaceName),
		zap.Int("replicaNumber", defaultRule.Count),
		zap.Float64("writeBaseCost", cfg.RUConfig.WriteBaseCost),
		zap.Float64("writePerBatchBaseCost", cfg.RUConfig.WritePerBatchBaseCost),
		zap.Float64("WriteCostPerByte", cfg.RUConfig.WriteCostPerByte),
	)
	return nil
}
