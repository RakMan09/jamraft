// Package client is a leader-aware jukebox client. It talks to any node's HTTP
// API (nodes forward writes to the leader) and attaches a stable (clientId,
// seq) to every logical operation so that retries are de-duplicated exactly
// once by the replicated state machine.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/rakman09/jamraft/internal/jukebox"
)

// Client is safe for concurrent use.
type Client struct {
	addrs    []string
	clientID string
	seq      uint64
	http     *http.Client
}

// New creates a client that will try the given HTTP base URLs in order.
func New(addrs []string) *Client {
	return &Client{
		addrs:    addrs,
		clientID: fmt.Sprintf("client-%d", rand.Int63()),
		http:     &http.Client{Timeout: 5 * time.Second},
	}
}

// ID returns this client's stable id.
func (c *Client) ID() string { return c.clientID }

func (c *Client) nextSeq() uint64 { return atomic.AddUint64(&c.seq, 1) }

type opRequest struct {
	Title    string `json:"title,omitempty"`
	AddedBy  string `json:"addedBy,omitempty"`
	SongID   string `json:"songId,omitempty"`
	ToIndex  int    `json:"toIndex,omitempty"`
	Voter    string `json:"voter,omitempty"`
	ClientID string `json:"clientId"`
	Seq      uint64 `json:"seq"`
}

// Enqueue adds a song.
func (c *Client) Enqueue(ctx context.Context, title, addedBy string) (jukebox.Result, error) {
	return c.do(ctx, "/api/enqueue", opRequest{Title: title, AddedBy: addedBy})
}

// PlayNext advances to the next song.
func (c *Client) PlayNext(ctx context.Context) (jukebox.Result, error) {
	return c.do(ctx, "/api/play-next", opRequest{})
}

// VoteSkip votes to skip the current song.
func (c *Client) VoteSkip(ctx context.Context, voter string) (jukebox.Result, error) {
	return c.do(ctx, "/api/vote-skip", opRequest{Voter: voter})
}

// Reorder moves a song within the queue.
func (c *Client) Reorder(ctx context.Context, songID string, toIndex int) (jukebox.Result, error) {
	return c.do(ctx, "/api/reorder", opRequest{SongID: songID, ToIndex: toIndex})
}

// Queue returns the current linearizable queue view.
func (c *Client) Queue(ctx context.Context) (jukebox.Result, error) {
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		if err := ctx.Err(); err != nil {
			return jukebox.Result{}, err
		}
		addr := c.addrs[attempt%len(c.addrs)]
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, addr+"/api/queue", nil)
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(backoff(attempt))
			continue
		}
		res, ok := decode(resp)
		if ok {
			return res, nil
		}
		lastErr = fmt.Errorf("status %d", resp.StatusCode)
		time.Sleep(backoff(attempt))
	}
	return jukebox.Result{}, lastErr
}

// do sends a write with a stable seq, retrying across nodes until success. The
// seq is allocated once so retries are idempotent.
func (c *Client) do(ctx context.Context, path string, body opRequest) (jukebox.Result, error) {
	body.ClientID = c.clientID
	body.Seq = c.nextSeq()
	payload, _ := json.Marshal(body)

	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		if err := ctx.Err(); err != nil {
			return jukebox.Result{}, err
		}
		addr := c.addrs[attempt%len(c.addrs)]
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, addr+path, bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(backoff(attempt))
			continue
		}
		res, ok := decode(resp)
		if ok {
			return res, nil
		}
		lastErr = fmt.Errorf("status %d", resp.StatusCode)
		time.Sleep(backoff(attempt))
	}
	return jukebox.Result{}, lastErr
}

func decode(resp *http.Response) (jukebox.Result, bool) {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return jukebox.Result{}, false
	}
	var res jukebox.Result
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return jukebox.Result{}, false
	}
	return res, true
}

func backoff(attempt int) time.Duration {
	d := time.Duration(20*(attempt+1)) * time.Millisecond
	if d > 300*time.Millisecond {
		d = 300 * time.Millisecond
	}
	return d
}
