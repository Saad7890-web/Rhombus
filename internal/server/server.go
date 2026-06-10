package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type Server struct {
	cfg    Config
	pinger Pinger
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
	mux.HandleFunc("/", s.handleRoot)

	s.http = &http.Server{
		Addr:              cfg.Address,
		Handler:           loggingMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s, nil
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

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}

func (s *Server) String() string {
	return fmt.Sprintf("%s(%s)", s.cfg.ServiceName, s.cfg.ServiceVersion)
}