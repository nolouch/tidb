package standby

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	standbyState   = "standby"
	activatedState = "activated"
)

// ActivateRequest is the request body for activating the tidb server.
type ActivateRequest struct {
	KeyspaceName string `json:"keyspace_name"`
}

var (
	mu              sync.RWMutex
	state           = standbyState
	activateRequest ActivateRequest

	// activationTimeout specifies the maximum allowed time for tidb to activate from standby mode.
	activationTimeout uint
)

var activateCh = make(chan struct{}, 1)

// Handler returns a handler to query tidb pool status or activate or exit the tidb server.
func Handler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/tidb-pool/status", statusHandler)
	mux.HandleFunc("/tidb-pool/activate", func(w http.ResponseWriter, r *http.Request) {
		var req ActivateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.KeyspaceName == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		mu.Lock()
		if state == standbyState {
			state = activatedState
			activateRequest = req
			activateCh <- struct{}{}
		} else if activateRequest.KeyspaceName != req.KeyspaceName {
			mu.Unlock()
			w.WriteHeader(http.StatusPreconditionFailed)
			w.Write([]byte("server is not in standby mode"))
			return
		}
		// if client tries to activate with same keyspace name, wait for ready signal and return 200.
		mu.Unlock()

		timeout := make(<-chan time.Time)
		if activationTimeout > 0 {
			timeout = time.After(time.Duration(activationTimeout) * time.Second)
		}

		select {
		case <-r.Context().Done(): // client closed connection.
			go func() {
				EndStandby(errors.New("client closed connection"))
				os.Exit(1)
			}()
		case <-timeout: // reach hardlimit timeout from config.
			logutil.BgLogger().Warn("timeout waiting for activation")
			w.WriteHeader(http.StatusRequestTimeout)
			w.Write([]byte("timeout waiting for activation"))
			go func() {
				EndStandby(errors.New("timeout waiting for activation"))
				os.Exit(1)
			}()
		case <-serverStartCh:
			if startServerErr != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(startServerErr.Error()))
				return
			}
			statusHandler(w, r)
		}
	})
	mux.HandleFunc("/tidb-pool/exit", func(w http.ResponseWriter, r *http.Request) {
		logutil.BgLogger().Info("receiving exit signal, exiting...")
		os.Exit(0)
	})
	return mux
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	defer mu.RUnlock()
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"state": "%s", "keyspace_name": "%s"}`, state, activateRequest.KeyspaceName)
}

var server *http.Server

// StartStandby starts a http server to listen and wait for activation signal.
func StartStandby(host string, port uint, timeout uint) ActivateRequest {
	mux := Handler()
	// handle liveness probe.
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	server = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", host, port),
		Handler: mux,
	}
	activationTimeout = timeout
	logutil.BgLogger().Info("tidb-server is now running as standby, waiting for activation...", zap.String("addr", server.Addr))
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logutil.BgLogger().Warn("failed to start tidb-server as standby", zap.Error(err))
			os.Exit(1)
		}
	}()

	<-activateCh

	mu.RLock()
	defer mu.RUnlock()
	return activateRequest
}

var (
	serverStartCh  = make(chan struct{})
	startServerErr error
	endOnce        sync.Once
)

// EndStandby is used to notify the temp http server that the tidb server is ready or failed to init.
func EndStandby(err error) {
	endOnce.Do(func() {
		startServerErr = err
		close(serverStartCh)
		if server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			server.Shutdown(ctx)
		}
	})
}
