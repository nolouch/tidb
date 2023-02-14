package standby

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var lastActive int64

// UpdateLastActive makes sure `lastActive` is not less than current time.
func UpdateLastActive(t time.Time) {
	for {
		last := atomic.LoadInt64(&lastActive)
		if last >= t.Unix() {
			return
		}
		if atomic.CompareAndSwapInt64(&lastActive, last, t.Unix()) {
			return
		}
	}
}

// StartWatchLastActive watches `lastActive` and exits the process if it is not updated for a long time.
func StartWatchLastActive(maxIdleSecs int) {
	UpdateLastActive(time.Now())
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
		for {
			<-ticker.C
			last := atomic.LoadInt64(&lastActive)
			if time.Now().Unix()-last > int64(maxIdleSecs) {
				logutil.BgLogger().Info("connection idle for too long, exiting", zap.Int("max-idle-seconds", maxIdleSecs))
				os.Exit(0)
			}
		}
	}()
}
