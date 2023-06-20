package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	storeerr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
)

// HealthHandler is the http handler for health check.
type HealthHandler struct {
	dom *domain.Domain
	sm  util.SessionManager
}

// NewHealthHandler creates a new health handler.
func NewHealthHandler(dom *domain.Domain, sm util.SessionManager) *HealthHandler {
	return &HealthHandler{dom: dom, sm: sm}
}

type healthResponse struct {
	Status string `json:"status"`
	Token  string `json:"token"`
	Error  string `json:"error,omitempty"`
}

// ServeHTTP implements the http.Handler interface.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
	defer cancel()

	var err error
	response := &healthResponse{
		Status: "up",
		Token:  config.GetGlobalKeyspaceName(),
	}
	defer func() {
		if err != nil && !terror.ErrorEqual(err, storeerr.ErrClientResourceGroupThrottled) { // ignore throttled error
			response.Status = "down"
			response.Error = err.Error()
		}
		json.NewEncoder(w).Encode(response)
	}()

	se, err := session.CreateSessionWithDomain(h.dom.Store(), h.dom)
	if se != nil {
		defer se.Close()
	}
	if err != nil {
		return
	}
	se.SetSessionManager(h.sm)
	rs, err := se.ExecuteInternal(ctx, "SELECT variable_name FROM mysql.tidb LIMIT 1")
	if err != nil {
		return
	}
	defer rs.Close()
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return
	}
	if len(rows) != 1 {
		err = errors.New("timeout to read mysql.tidb")
	}
}
