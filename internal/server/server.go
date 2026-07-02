// Package server exposes the jukebox over HTTP: a small JSON API plus the
// static party UI. It implements the client-session layer described in the
// spec: writes are routed to the leader (forwarded automatically), reads are
// linearizable via the Raft read-index, and each request carries (clientId,
// seq) so retries are de-duplicated by the state machine.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/rakman09/jamraft/internal/jukebox"
	"github.com/rakman09/jamraft/internal/raft"
	"github.com/rakman09/jamraft/web"
)

// Server serves the API and UI for one node.
type Server struct {
	node     *raft.Node
	sm       *jukebox.Jukebox
	peerHTTP map[string]string // node id -> http base url (e.g. http://host:8080)
	shutdown func()
	client   *http.Client
}

// New creates a Server. peerHTTP maps every node id (including self) to its HTTP
// base URL, used to forward writes/reads to the current leader. shutdown is
// invoked by the demo "kill node" endpoint (may be nil).
func New(node *raft.Node, sm *jukebox.Jukebox, peerHTTP map[string]string, shutdown func()) *Server {
	return &Server{
		node:     node,
		sm:       sm,
		peerHTTP: peerHTTP,
		shutdown: shutdown,
		client:   &http.Client{Timeout: 3 * time.Second},
	}
}

// Handler returns the HTTP handler for the node.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/queue", s.handleQueue)
	mux.HandleFunc("/api/enqueue", s.opHandler(jukebox.OpEnqueue))
	mux.HandleFunc("/api/play-next", s.opHandler(jukebox.OpPlayNext))
	mux.HandleFunc("/api/vote-skip", s.opHandler(jukebox.OpVoteSkip))
	mux.HandleFunc("/api/reorder", s.opHandler(jukebox.OpReorder))
	mux.HandleFunc("/api/admin/kill", s.handleKill)

	static := http.FileServer(http.FS(web.FS))
	mux.Handle("/", static)
	return withCORS(mux)
}

func withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// StatusResponse is returned by /api/status.
type StatusResponse struct {
	raft.Status
	LeaderHTTP string `json:"leaderHttp"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := s.node.Status()
	writeJSON(w, http.StatusOK, StatusResponse{Status: st, LeaderHTTP: s.peerHTTP[st.LeaderID]})
}

// requestBody is the shape accepted by the write endpoints.
type requestBody struct {
	Title    string `json:"title"`
	AddedBy  string `json:"addedBy"`
	SongID   string `json:"songId"`
	ToIndex  int    `json:"toIndex"`
	Voter    string `json:"voter"`
	ClientID string `json:"clientId"`
	Seq      uint64 `json:"seq"`
}

func (s *Server) opHandler(op string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
			return
		}
		raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))

		// If we are not the leader, forward the raw request to the leader.
		if !s.node.IsLeader() {
			if s.forward(w, r, raw) {
				return
			}
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no leader available"})
			return
		}

		var body requestBody
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
				return
			}
		}
		cmd := jukebox.Command{
			Op:       op,
			ClientID: body.ClientID,
			Seq:      body.Seq,
			SongID:   body.SongID,
			ToIndex:  body.ToIndex,
			Voter:    body.Voter,
		}
		if op == jukebox.OpEnqueue {
			id := body.SongID
			if id == "" {
				id = body.ClientID + "-" + itoa(body.Seq)
			}
			cmd.Song = &jukebox.Song{ID: id, Title: body.Title, AddedBy: body.AddedBy}
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		res, err := s.node.Submit(ctx, cmd.Encode())
		if err != nil {
			// Leadership may have changed between the check and submit.
			if err == raft.ErrNotLeader && s.forward(w, r, raw) {
				return
			}
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		var out jukebox.Result
		json.Unmarshal(res, &out)
		writeJSON(w, http.StatusOK, out)
	}
}

func (s *Server) handleQueue(w http.ResponseWriter, r *http.Request) {
	// Linearizable read: only the confirmed leader may serve it.
	if !s.node.IsLeader() {
		if s.forwardGet(w, r) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no leader available"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if _, err := s.node.LinearizableRead(ctx); err != nil {
		if s.forwardGet(w, r) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, s.sm.View())
}

func (s *Server) handleKill(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "shutting down"})
	if s.shutdown != nil {
		go func() {
			time.Sleep(100 * time.Millisecond)
			s.shutdown()
		}()
	}
}

// forward re-issues a write to the current leader and copies back its response.
func (s *Server) forward(w http.ResponseWriter, r *http.Request, body []byte) bool {
	leaderURL := s.peerHTTP[s.node.LeaderID()]
	if leaderURL == "" {
		return false
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, leaderURL+r.URL.Path, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	copyResponse(w, resp)
	return true
}

func (s *Server) forwardGet(w http.ResponseWriter, r *http.Request) bool {
	leaderURL := s.peerHTTP[s.node.LeaderID()]
	if leaderURL == "" {
		return false
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, leaderURL+r.URL.Path, nil)
	if err != nil {
		return false
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	copyResponse(w, resp)
	return true
}

func copyResponse(w http.ResponseWriter, resp *http.Response) {
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
