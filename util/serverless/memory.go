package serverless

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/pingcap/tidb/util/gctuner"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

var (
	namespace      string
	podName        string
	nodeIP         string
	minMemoryLimit uint64
	maxMemoryLimit uint64
)

// StartMemoryScaler starts a memory scaler.
func StartMemoryScaler(q chan struct{}) {
	namespace = os.Getenv("NAMESPACE")
	podName = os.Getenv("POD_NAME")
	nodeIP = os.Getenv("NODE_IP")
	minMemoryLimitStr := os.Getenv("MIN_MEMORY_LIMIT")
	maxMemoryLimitStr := os.Getenv("MAX_MEMORY_LIMIT")

	if namespace == "" || podName == "" || nodeIP == "" {
		logutil.BgLogger().Info("env NAMESPACE, POD_NAME, NODE_IP not set, skip init memory limits")
		return
	}

	if minMemoryLimitStr == "" || maxMemoryLimitStr == "" {
		logutil.BgLogger().Info("env MIN_MEMORY_LIMIT or MAX_MEMORY_LIMIT not set, skip init memory limits")
		return
	}

	var err error
	minMemoryLimit, err = strconv.ParseUint(minMemoryLimitStr, 10, 64)
	if err != nil {
		logutil.BgLogger().Info("env MIN_MEMORY_LIMIT is bad format, skip init memory limits")
		return
	}

	maxMemoryLimit, err = strconv.ParseUint(maxMemoryLimitStr, 10, 64)
	if err != nil {
		logutil.BgLogger().Info("env MAX_MEMORY_LIMIT is bad format, skip init memory limits")
		return
	}

	memory.ServerMemoryLimitOriginText.Store(minMemoryLimitStr)
	memory.ServerMemoryLimit.Store(minMemoryLimit)
	memory.MaxServerMemoryLimit.Store(maxMemoryLimit)
	memory.MaxServerMemoryLimitText.Store(maxMemoryLimitStr)
	gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()

	go NewMemoryScaler(q).Run()
	logutil.BgLogger().Info("start memory scaler success")
}

// MemoryScaler is used to scale memory limit of a pod.
type MemoryScaler struct {
	exitCh chan struct{}
}

// NewMemoryScaler creates a new MemoryScaleHandle.
func NewMemoryScaler(exitCh chan struct{}) *MemoryScaler {
	return &MemoryScaler{
		exitCh: exitCh,
	}
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

func (ms *MemoryScaler) report(stat *runtime.MemStats, limit uint64) {
	pod := &Pod{
		Name:      podName,
		Namespace: namespace,
		Limit:     int64(limit),
		MinLimit:  int64(minMemoryLimit),
		MaxLimit:  int64(maxMemoryLimit),
		Used:      int64(stat.HeapInuse),
	}
	data, _ := json.Marshal(pod)
	res, err := http.Post("http://"+nodeIP+":4040/report", "application/json", bytes.NewReader(data))
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

func (ms *MemoryScaler) scaleMemory(to uint64) bool {
	req := &ScaleRequest{
		Namespace: namespace,
		Name:      podName,
		Limit:     int64(to),
	}
	data, _ := json.Marshal(req)
	res, err := http.Post("http://"+nodeIP+":4040/scale", "application/json", bytes.NewReader(data))
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
func (ms *MemoryScaler) Run() {
	if minMemoryLimit == 0 || maxMemoryLimit == 0 {
		return
	}

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	lastReport, lastIncrease, lastIncreseFailed := time.Now(), time.Now(), time.Now()
	for {
		select {
		case <-ticker.C:
			stats := memory.ForceReadMemStats()
			limit := memory.ServerMemoryLimit.Load()
			if time.Since(lastReport) > time.Second {
				ms.report(stats, limit)
				lastReport = time.Now()
			}
			if limit != maxMemoryLimit && stats.HeapAlloc > limit*8/10 && time.Since(lastIncreseFailed) > time.Second {
				limit *= 2
				if limit > maxMemoryLimit {
					limit = maxMemoryLimit
				}
				ok := ms.scaleMemory(limit)
				if ok {
					lastIncrease = time.Now()
				} else {
					lastIncreseFailed = time.Now()
				}
			} else if limit != minMemoryLimit && time.Since(lastIncrease) > time.Minute && stats.HeapAlloc < limit/2 {
				limit /= 2
				if limit < minMemoryLimit {
					limit = minMemoryLimit
				}
				ms.scaleMemory(limit)
			}
		case <-ms.exitCh:
			return
		}
	}
}
