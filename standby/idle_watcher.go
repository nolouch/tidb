package standby

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/signal"
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

type sessionManager interface {
	ConnectionCount() int
	GetUserProcessList() map[uint64]*util.ProcessInfo
}

// StartWatchLastActive watches `lastActive` and exits the process if it is not updated for a long time.
func StartWatchLastActive(sm sessionManager, maxIdleSecs int) {
	UpdateLastActive(time.Now())
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
		for {
			<-ticker.C
			last := atomic.LoadInt64(&lastActive)
			if time.Now().Unix()-last > int64(maxIdleSecs) {
				connCount := sm.ConnectionCount()
				var processCount int
				for _, p := range sm.GetUserProcessList() {
					if p.Command != mysql.ComSleep { // ignore sleep sessions (waiting for client query).
						processCount++
					}
				}
				logutil.BgLogger().Info("connection idle for too long",
					zap.Int("max-idle-seconds", maxIdleSecs),
					zap.Int("connection-count", connCount),
					zap.Int("process-count", processCount))

				if connCount == 0 || processCount == 0 {
					signal.TiDBExit()
				}
			}
		}
	}()
}
