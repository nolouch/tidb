package server

import (
	"context"
	"net/http"
	"time"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
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

// ServeHTTP implements the http.Handler interface.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
	defer cancel()
	se, err := session.CreateSessionWithDomain(h.dom.Store(), h.dom)
	if se != nil {
		defer se.Close()
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	se.SetSessionManager(h.sm)
	rs, err := se.ExecuteInternal(ctx, "SELECT variable_name FROM mysql.tidb LIMIT 1")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	defer rs.Close()
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	if len(rows) != 1 {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("timeout to read mysql.tidb"))
		return
	}
	w.Write([]byte("up"))
}
