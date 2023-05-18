package serverless

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/gctuner"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

// MemoryScaleHandle is used to scale memory limit of a pod.
type MemoryScaleHandle struct {
	exitCh chan struct{}
	sm     atomic.Value

	namespace      string
	podName        string
	nodeIP         string
	minMemoryLimit uint64
	maxMemoryLimit uint64
}

// NewMemoryScaleHandle creates a new MemoryScaleHandle.
func NewMemoryScaleHandle(exitCh chan struct{}) *MemoryScaleHandle {
	namespace := os.Getenv("NAMESPACE")
	podName := os.Getenv("POD_NAME")
	nodeIP := os.Getenv("NODE_IP")
	minMemoryLimitStr := os.Getenv("MIN_MEMORY_LIMIT")
	maxMemoryLimitStr := os.Getenv("MAX_MEMORY_LIMIT")

	if namespace == "" || podName == "" || nodeIP == "" {
		logutil.BgLogger().Info("env NAMESPACE, POD_NAME, NODE_IP not set, skip memory scale")
	}

	if minMemoryLimitStr == "" || maxMemoryLimitStr == "" {
		logutil.BgLogger().Info("env MIN_MEMORY_LIMIT or MAX_MEMORY_LIMIT not set, skip memory scale")
	}

	minMemoryLimit, err := strconv.ParseUint(minMemoryLimitStr, 10, 64)
	if err != nil {
		logutil.BgLogger().Info("env MIN_MEMORY_LIMIT is bad format, skip memory scale")
	}

	maxMemoryLimit, err := strconv.ParseUint(maxMemoryLimitStr, 10, 64)
	if err != nil {
		logutil.BgLogger().Info("env MAX_MEMORY_LIMIT is bad format, skip memory scale")
	}
	return &MemoryScaleHandle{
		exitCh:         exitCh,
		namespace:      namespace,
		podName:        podName,
		nodeIP:         nodeIP,
		minMemoryLimit: minMemoryLimit,
		maxMemoryLimit: maxMemoryLimit,
	}
}

// SetSessionManager sets the session manager.
func (mw *MemoryScaleHandle) SetSessionManager(sm util.SessionManager) *MemoryScaleHandle {
	mw.sm.Store(sm)
	return mw
}

// Pod stores memory usage information of a pod.
type Pod struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Limit     int64  `json:"limit,omitempty"`
	MinLimit  int64  `json:"min_limit,omitempty"`
	MaxLimit  int64  `json:"max_limit,omitempty"`
	Used      int64  `json:"used,omitempty"`
}

func (mw *MemoryScaleHandle) report(stat *runtime.MemStats, limit uint64) {
	pod := &Pod{
		Name:      mw.podName,
		Namespace: mw.namespace,
		Limit:     int64(limit),
		MinLimit:  int64(mw.minMemoryLimit),
		MaxLimit:  int64(mw.maxMemoryLimit),
		Used:      int64(stat.HeapInuse),
	}
	data, _ := json.Marshal(pod)
	res, err := http.Post("http://"+mw.nodeIP+":4040/report", "application/json", bytes.NewReader(data))
	if err != nil {
		logutil.BgLogger().Error("report memory usage failed", zap.Error(err))
		return
	}
	if res.StatusCode != http.StatusOK {
		logutil.BgLogger().Error("report memory usage failed", zap.Int("status", res.StatusCode))
	}
}

// ScaleRequest is the request to scale a pod's memory limit.
type ScaleRequest struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	Limit     int64  `json:"limit,omitempty"`
}

func (mw *MemoryScaleHandle) scaleMemory(to uint64) bool {
	req := &ScaleRequest{
		Namespace: mw.namespace,
		Name:      mw.podName,
		Limit:     int64(to),
	}
	data, _ := json.Marshal(req)
	res, err := http.Post("http://"+mw.nodeIP+":4040/scale", "application/json", bytes.NewReader(data))
	if err != nil {
		logutil.BgLogger().Error("scale memory failed", zap.Error(err))
		return false
	}
	if res.StatusCode != http.StatusOK {
		logutil.BgLogger().Error("scale memory failed", zap.Int("status", res.StatusCode))
		return false
	}
	logutil.BgLogger().Info("scale memory success", zap.Uint64("limit", to))
	memory.ServerMemoryLimit.Store(to)
	gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
	return true
}

// Run runs the memory scale handle if it is configured.
func (mw *MemoryScaleHandle) Run() {
	if mw.namespace == "" || mw.podName == "" || mw.nodeIP == "" {
		return
	}
	if mw.minMemoryLimit == 0 || mw.maxMemoryLimit == 0 {
		return
	}

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	lastReport, lastIncrease := time.Now(), time.Now()
	for {
		select {
		case <-ticker.C:
			stats := memory.ForceReadMemStats()
			limit := memory.ServerMemoryLimit.Load()
			if time.Since(lastReport) > time.Second {
				mw.report(stats, limit)
				lastReport = time.Now()
			}
			if limit != mw.maxMemoryLimit && stats.HeapAlloc > limit*8/10 {
				limit *= 2
				if limit > mw.maxMemoryLimit {
					limit = mw.maxMemoryLimit
				}
				ok := mw.scaleMemory(limit)
				if ok {
					lastIncrease = time.Now()
				}
			} else if limit != mw.minMemoryLimit && time.Since(lastIncrease) > time.Minute && stats.HeapAlloc < limit/2 {
				limit /= 2
				if limit < mw.minMemoryLimit {
					limit = mw.minMemoryLimit
				}
				mw.scaleMemory(limit)
			}
		case <-mw.exitCh:
			return
		}
	}
}
