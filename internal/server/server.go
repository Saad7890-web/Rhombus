package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Saad7890-web/rhombus/internal/observability"
	"github.com/Saad7890-web/rhombus/internal/replay"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type Server struct {
	cfg    Config
	pinger Pinger
	obs    *observability.Collector
	mux    *http.ServeMux
	http   *http.Server
}

type statusResponse struct {
	Status  string `json:"status"`
	Service string `json:"service,omitempty"`
	Version string `json:"version,omitempty"`
}

type readyResponse struct {
	Status string `json:"status"`
	DB     string `json:"db,omitempty"`
	Error  string `json:"error,omitempty"`
}

func New(cfg Config, pinger Pinger) (*Server, error) {
	if cfg.Address == "" {
		return nil, errors.New("server address is required")
	}
	if pinger == nil {
		return nil, errors.New("pinger is nil")
	}

	mux := http.NewServeMux()
	s := &Server{
		cfg:    cfg,
		pinger: pinger,
		mux:    mux,
	}

	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/version", s.handleVersion)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/", s.handleRoot)

	s.http = &http.Server{
		Addr:              cfg.Address,
		Handler:           s,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s, nil
}

func (s *Server) SetObserver(obs *observability.Collector) {
	s.obs = obs
}

func (s *Server) MountReplay(h *replay.Handler) {
	if s == nil || h == nil {
		return
	}
	h.Register(s.mux)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler := http.Handler(s.mux)
	if s.obs != nil {
		handler = s.obsRequestMiddleware(handler)
	}
	handler.ServeHTTP(w, r)
}

func (s *Server) obsRequestMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := r.Header.Get("X-Request-Id")
		if traceID == "" {
			traceID = r.Header.Get("traceparent")
		}
		ctx := observability.WithTraceID(r.Context(), traceID)
		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) ListenAndServe() error {
	if s == nil || s.http == nil {
		return errors.New("server is not initialized")
	}
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.http == nil {
		return nil
	}
	return s.http.Shutdown(ctx)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{
		Status:  "ok",
		Service: s.cfg.ServiceName,
		Version: s.cfg.ServiceVersion,
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ReadinessTimeout)
	defer cancel()

	if err := s.pinger.Ping(ctx); err != nil {
		if s.obs != nil {
			s.obs.IncDBError()
			s.obs.Log(r.Context(), "error", "readiness failed", map[string]any{
				"error": err.Error(),
			})
		}
		writeJSON(w, http.StatusServiceUnavailable, readyResponse{
			Status: "not-ready",
			DB:     "down",
			Error:  err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, readyResponse{
		Status: "ready",
		DB:     "up",
	})
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{
		Status:  "ok",
		Service: s.cfg.ServiceName,
		Version: s.cfg.ServiceVersion,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.obs == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "observability not configured",
		})
		return
	}
	s.obs.MetricsHandler().ServeHTTP(w, r)
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, statusResponse{
		Status:  "ok",
		Service: s.cfg.ServiceName,
		Version: s.cfg.ServiceVersion,
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) String() string {
	return fmt.Sprintf("%s(%s)", s.cfg.ServiceName, s.cfg.ServiceVersion)
}