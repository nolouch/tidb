// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metric

import (
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

const (
	zeroDuration = time.Duration(0)
	job          = "tidb-lightning"
)

type pushFn func()

// PushMetrics pushes metrics in background.
func PushMetrics(ctx context.Context, addrs []string, interval time.Duration, labels map[string]string) pushFn {
	if interval == zeroDuration || len(addrs) == 0 {
		log.L().Info("disable Prometheus pushgateway client")
		return func() {}
	}
	// Now we use `FmtText` to push metrics, because `FmtProtoDelim` is not supported by vmagent.
	// TODO: use `FmtProtoDelim` to push metrics after vmagent supports it.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3745 for more details.
	pushers := make([]*push.Pusher, 0, len(addrs))
	for _, addr := range addrs {
		pusher := push.New(addr, job).Format(expfmt.FmtText).Gatherer(prometheus.DefaultGatherer)
		for k, v := range labels {
			pusher = pusher.Grouping(k, v)
		}
		pushers = append(pushers, pusher)
	}
	push := func() {
		for i, pusher := range pushers {
			err := pusher.Push()
			if err != nil {
				log.L().Error("could not push metrics to prometheus pushgateway", zap.String("err", err.Error()), zap.String("server addr", addrs[i]))
			}
		}
	}
	log.L().Info("start to push metrics to prometheus pushgateway", zap.Strings("server addrs", addrs), zap.String("interval", interval.String()))
	go pushMetricsPeriodically(ctx, interval, push)

	return push
}

func pushMetricsPeriodically(ctx context.Context, interval time.Duration, push func()) {
	for {
		select {
		case <-ctx.Done():
			log.L().Info("stop pushing metrics to prometheus pushgateway")
			return
		case <-time.After(interval):
			push()
		}
	}
}
