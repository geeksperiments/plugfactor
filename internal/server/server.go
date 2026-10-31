package server

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/geeksperiments/plugfactor/internal/pipeline"
)

//go:embed ui.html
var uiHTML []byte

// Server serves the PlugFactor Web Studio UI and REST API.
type Server struct {
	engine    *pipeline.Engine
	mu        sync.RWMutex
	lastBuild *pipeline.BuildResult
}

// New creates a new PlugFactor HTTP Server.
func New(engine *pipeline.Engine) *Server {
	return &Server{engine: engine}
}

// Handler returns the HTTP router for the server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleUI)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/build", s.handleBuild)
	mux.HandleFunc("/api/last-build", s.handleLastBuild)
	return mux
}

func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(uiHTML)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	cfg := s.engine.Config()
	resp := map[string]any{
		"neo4j_configured":   cfg.Neo4jConfigured(),
		"neo4j_uri":          cfg.Neo4jURI,
		"daytona_configured": cfg.DaytonaConfigured(),
		"daytona_target":     cfg.DaytonaTarget,
		"nosana_configured":  cfg.NosanaConfigured(),
		"nosana_model":       cfg.NosanaModel,
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleBuild(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}

	var req pipeline.BuildRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("invalid JSON: %v", err)})
		return
	}
	if req.Prompt == "" {
		req.Prompt = "Build an 8-voice polyphonic acid subtractive synth with resonant filter and ADSR envelope"
	}

	res, err := s.engine.ExecuteVisibleLoop(r.Context(), req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	s.mu.Lock()
	s.lastBuild = res
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleLastBuild(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lastBuild == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no builds yet"})
		return
	}
	writeJSON(w, http.StatusOK, s.lastBuild)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
