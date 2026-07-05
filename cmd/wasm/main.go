//go:build js && wasm

// Command wasm compiles the real JamRaft Raft implementation to WebAssembly and
// runs an entire in-process cluster inside the browser tab. The deterministic
// in-process transport (internal/transport) maps perfectly onto a single WASM
// runtime, so the live "kill node / isolate / drop messages" controls drive the
// actual consensus code — leader elections and log replication happen for real,
// in the browser, with no backend.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"syscall/js"
	"time"

	"github.com/rakman09/jamraft/internal/cluster"
	"github.com/rakman09/jamraft/internal/jukebox"
)

type demo struct {
	mu       sync.Mutex
	c        *cluster.SimCluster
	ids      []string
	down     map[string]bool
	isolated map[string]bool
	dropRate float64
	lastView clusterView // cached so the UI keeps showing the queue during leader gaps
	seq      uint64
}

type nodeView struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Term        uint64 `json:"term"`
	Commit      uint64 `json:"commit"`
	Applied     uint64 `json:"applied"`
	LastIndex   uint64 `json:"lastIndex"`
	LogSize     int    `json:"logSize"`
	SnapshotIdx uint64 `json:"snapshotIndex"`
	Leader      bool   `json:"leader"`
	Down        bool   `json:"down"`
	Isolated    bool   `json:"isolated"`
}

type clusterView struct {
	Nodes      []nodeView      `json:"nodes"`
	LeaderID   string          `json:"leaderId"`
	Queue      []*jukebox.Song `json:"queue"`
	NowPlaying *jukebox.Song   `json:"nowPlaying"`
	SkipVotes  int             `json:"skipVotes"`
	Delivered  uint64          `json:"delivered"`
	Dropped    uint64          `json:"dropped"`
	DropRate   float64         `json:"dropRate"`
	Size       int             `json:"size"`
}

var d = &demo{down: map[string]bool{}, isolated: map[string]bool{}}

func main() {
	register()
	// Boot a default 5-node cluster.
	d.reset(5)
	js.Global().Set("jamraftReady", js.ValueOf(true))
	select {} // keep the Go runtime (and cluster goroutines) alive
}

func register() {
	set := func(name string, fn func(js.Value, []js.Value) any) {
		js.Global().Set(name, js.FuncOf(fn))
	}
	set("jrReset", func(_ js.Value, args []js.Value) any {
		n := 5
		if len(args) > 0 && args[0].Type() == js.TypeNumber {
			n = args[0].Int()
		}
		d.reset(n)
		return nil
	})
	set("jrState", func(_ js.Value, _ []js.Value) any { return d.state() })
	set("jrEnqueue", func(_ js.Value, args []js.Value) any {
		title, addedBy := "", ""
		if len(args) > 0 {
			title = args[0].String()
		}
		if len(args) > 1 {
			addedBy = args[1].String()
		}
		d.submit(jukebox.Command{Op: jukebox.OpEnqueue, Song: &jukebox.Song{Title: title, AddedBy: addedBy}})
		return nil
	})
	set("jrPlayNext", func(_ js.Value, _ []js.Value) any {
		d.submit(jukebox.Command{Op: jukebox.OpPlayNext})
		return nil
	})
	set("jrVoteSkip", func(_ js.Value, args []js.Value) any {
		voter := "guest"
		if len(args) > 0 {
			voter = args[0].String()
		}
		d.submit(jukebox.Command{Op: jukebox.OpVoteSkip, Voter: voter})
		return nil
	})
	set("jrKill", func(_ js.Value, args []js.Value) any { d.kill(args[0].String()); return nil })
	set("jrRestart", func(_ js.Value, args []js.Value) any { d.restart(args[0].String()); return nil })
	set("jrToggleIsolate", func(_ js.Value, args []js.Value) any { d.toggleIsolate(args[0].String()); return nil })
	set("jrSetDrop", func(_ js.Value, args []js.Value) any {
		if len(args) > 0 {
			d.setDrop(args[0].Float())
		}
		return nil
	})
	set("jrHeal", func(_ js.Value, _ []js.Value) any { d.heal(); return nil })
}

func (d *demo) reset(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.c != nil {
		d.c.StopAll()
	}
	if n < 1 {
		n = 1
	}
	if n > 7 {
		n = 7
	}
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i)
	}
	opts := cluster.DefaultOptions()
	opts.Seed = time.Now().UnixNano()
	opts.SnapshotThreshold = 25
	d.c = cluster.New(ids, opts)
	d.c.Net.SetLatency(2*time.Millisecond, 12*time.Millisecond)
	d.ids = ids
	d.down = map[string]bool{}
	d.isolated = map[string]bool{}
	d.dropRate = 0
	d.lastView = clusterView{}
	d.seq = 0
}

func (d *demo) submit(cmd jukebox.Command) {
	d.mu.Lock()
	c := d.c
	d.seq++
	cmd.ClientID = "ui"
	cmd.Seq = d.seq
	d.mu.Unlock()
	if c == nil {
		return
	}
	// Run in a goroutine so the JS callback returns immediately; the goroutine
	// may block until a leader commits the entry.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		c.Submit(ctx, cmd.Encode())
	}()
}

func (d *demo) kill(id string) {
	d.mu.Lock()
	c, ok := d.c, d.down[id]
	if !ok {
		d.down[id] = true
	}
	d.mu.Unlock()
	if c != nil {
		c.Crash(id)
	}
}

func (d *demo) restart(id string) {
	d.mu.Lock()
	c := d.c
	delete(d.down, id)
	d.mu.Unlock()
	if c != nil {
		c.Restart(id)
	}
}

func (d *demo) toggleIsolate(id string) {
	d.mu.Lock()
	c := d.c
	if d.isolated[id] {
		delete(d.isolated, id)
		if c != nil {
			c.Net.Rejoin(id)
		}
	} else {
		d.isolated[id] = true
		if c != nil {
			c.Net.Isolate(id)
		}
	}
	d.mu.Unlock()
}

func (d *demo) setDrop(rate float64) {
	d.mu.Lock()
	c := d.c
	d.dropRate = rate
	d.mu.Unlock()
	if c != nil {
		c.Net.SetDropRate(rate)
	}
}

func (d *demo) heal() {
	d.mu.Lock()
	c := d.c
	d.isolated = map[string]bool{}
	d.dropRate = 0
	d.mu.Unlock()
	if c != nil {
		c.Net.Heal()
		c.Net.SetDropRate(0)
	}
}

func (d *demo) state() string {
	d.mu.Lock()
	c := d.c
	ids := d.ids
	down := d.down
	isolated := d.isolated
	dropRate := d.dropRate
	d.mu.Unlock()
	if c == nil {
		return "{}"
	}

	view := clusterView{Size: len(ids), DropRate: dropRate}
	delivered, dropped := c.Net.Stats()
	view.Delivered, view.Dropped = delivered, dropped

	var leader string
	for _, id := range ids {
		nv := nodeView{ID: id, Down: down[id], Isolated: isolated[id]}
		if n := c.Node(id); n != nil {
			st := n.Status()
			nv.Role = st.Role
			nv.Term = st.Term
			nv.Commit = st.CommitIndex
			nv.Applied = st.LastApplied
			nv.LastIndex = st.LastIndex
			nv.LogSize = st.LogSize
			nv.SnapshotIdx = st.SnapshotIdx
			nv.Leader = n.IsLeader()
			if nv.Leader && !nv.Isolated {
				leader = id
			}
		} else {
			nv.Role = "down"
		}
		view.Nodes = append(view.Nodes, nv)
	}
	view.LeaderID = leader

	// Show the queue from a connected leader; otherwise keep the last snapshot so
	// the UI doesn't flicker to empty during elections.
	if leader != "" {
		r := c.StateMachine(leader).View()
		view.Queue = r.Queue
		view.NowPlaying = r.NowPlaying
		view.SkipVotes = r.SkipVotes
		d.mu.Lock()
		d.lastView = view
		d.mu.Unlock()
	} else {
		d.mu.Lock()
		view.Queue = d.lastView.Queue
		view.NowPlaying = d.lastView.NowPlaying
		view.SkipVotes = d.lastView.SkipVotes
		d.mu.Unlock()
	}

	b, _ := json.Marshal(view)
	return string(b)
}
