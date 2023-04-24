package keyspace

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type rateLimiter struct {
	sync.Mutex
	accumulateSpeed int
	maxToken        int

	token      int
	lastUpdate int64
	dataSize   int64
}

func (r *rateLimiter) Usage() int64 {
	return atomic.LoadInt64(&r.dataSize)
}

func (r *rateLimiter) MaxToken() int {
	r.Lock()
	defer r.Unlock()
	return r.maxToken
}

func (r *rateLimiter) Consume(n int) (bool, time.Duration) {
	r.Lock()
	defer r.Unlock()

	now := time.Now().Unix()
	if now > r.lastUpdate {
		r.token += int(now-r.lastUpdate) * r.accumulateSpeed
		if r.token > r.maxToken {
			r.token = r.maxToken
		}
		r.lastUpdate = now
	}

	if n <= r.token {
		r.token -= n
		return true, 0
	}
	if n > r.maxToken {
		return false, 0
	}
	return false, time.Duration((n-r.token)/r.accumulateSpeed+1) * time.Second
}

func (r *rateLimiter) updateSpeed(dataSize int64) {
	r.Lock()
	defer r.Unlock()

	cfg := config.GetGlobalConfig().Ratelimit

	speed, max := cfg.FullSpeed, cfg.FullSpeedCapacity
	if dataSize > cfg.LowSpeedWatermark {
		speed, max = cfg.LowSpeed, cfg.LowSpeedCapacity
	}
	if dataSize > cfg.BlockWriteWatermark {
		speed, max = 10, cfg.LowSpeedCapacity/2 // set a minimal value, or tidb-server may fail to start.
	}

	if speed != r.accumulateSpeed || max != r.maxToken {
		logutil.BgLogger().Info("update rate limit speed", zap.Int("speed", speed), zap.Int("max", max))
	}

	r.accumulateSpeed = speed
	r.maxToken = max
	if r.token > r.maxToken {
		r.token = r.maxToken
	}
	atomic.StoreInt64(&r.dataSize, dataSize)
}

func (r *rateLimiter) StartAdjustLimit(pdAddrs []string, keyspaceID uint32) {
	r.adjustLimit(pdAddrs, keyspaceID)
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			<-ticker.C
			r.adjustLimit(pdAddrs, keyspaceID)
		}
	}()
}

// PDRegionStats is the json response from PD.
type PDRegionStats struct {
	StorageSize     *int64 `json:"storage_size"`
	UserStorageSize *int64 `json:"user_storage_size"`
}

func (r *rateLimiter) adjustLimit(pdAddrs []string, keyspaceID uint32) {
	start, end := r.keyspaceRange(keyspaceID)
	for _, addr := range pdAddrs {
		path := fmt.Sprintf("/pd/api/v1/stats/region?start_key=%s&end_key=%s", url.QueryEscape(string(start)), url.QueryEscape(string(end)))
		res, err := util.InternalHTTPClient().Get(util.ComposeURL(addr, path))
		if err != nil {
			logutil.BgLogger().Warn("get region stats failed", zap.String("pd", addr), zap.Error(err))
			continue
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			logutil.BgLogger().Warn("get region stats failed", zap.String("pd", addr), zap.Int("status", res.StatusCode))
			continue
		}
		var stats PDRegionStats
		err = json.NewDecoder(res.Body).Decode(&stats)
		if err != nil {
			logutil.BgLogger().Warn("decode region stats failed", zap.String("pd", addr), zap.Error(err))
			continue
		}

		var userStorageSize int64
		if stats.UserStorageSize != nil {
			userStorageSize = *(stats.UserStorageSize)
		} else if stats.StorageSize != nil {
			userStorageSize = *(stats.StorageSize)
		}
		r.updateSpeed(userStorageSize * 1024 * 1024) // storage size unit is MiB.
		return
	}
	logutil.BgLogger().Error("get region stats failed from all PDs")
}

func (r *rateLimiter) keyspaceRange(id uint32) ([]byte, []byte) {
	var start, end [4]byte
	binary.BigEndian.PutUint32(start[:], id)
	binary.BigEndian.PutUint32(end[:], id+1)
	start[0], end[0] = 'x', 'x' // we only care about txn data.
	if id == 0xffffff {
		end[0] = 'x' + 1 // handle overflow for max keyspace id.
	}
	return codec.EncodeBytes(nil, start[:]), codec.EncodeBytes(nil, end[:])
}

// Limiter is an instance of rateLimiter
var Limiter = &rateLimiter{
	accumulateSpeed: 10 * 1024,
	maxToken:        1024 * 1024,
	token:           1024 * 1024,
	lastUpdate:      time.Now().Unix(),
}
