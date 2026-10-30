package main

import (
	"encoding/json"
	"os"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Ledger is this node's private, bilateral record of work exchanged with each
// peer, BitTorrent style. There is no global ledger: every node decides on its
// own whom to serve, preferring peers that have served it.
type Ledger struct {
	mu    sync.Mutex
	path  string
	Peers map[string]*Account `json:"peers"`
}

type Account struct {
	Gave int64 `json:"gave"` // credits of work this peer did for us
	Took int64 `json:"took"` // credits of work we did for this peer
}

func OpenLedger(path string) *Ledger {
	l := &Ledger{path: path, Peers: map[string]*Account{}}
	if b, err := os.ReadFile(path); err == nil {
		json.Unmarshal(b, l)
	}
	return l
}

func (l *Ledger) acct(p peer.ID) *Account {
	a := l.Peers[p.String()]
	if a == nil {
		a = &Account{}
		l.Peers[p.String()] = a
	}
	return a
}

// Balance is how much p is ahead: positive means p has done more for us than we for p.
func (l *Ledger) Balance(p peer.ID) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	a := l.acct(p)
	return a.Gave - a.Took
}

func (l *Ledger) Served(p peer.ID, credits int64)   { l.add(p, 0, credits) }
func (l *Ledger) Received(p peer.ID, credits int64) { l.add(p, credits, 0) }

func (l *Ledger) add(p peer.ID, gave, took int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	a := l.acct(p)
	a.Gave += gave
	a.Took += took
	b, _ := json.MarshalIndent(l, "", "  ")
	tmp := l.path + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		os.Rename(tmp, l.path)
	}
}

func (l *Ledger) Snapshot() map[string]Account {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]Account, len(l.Peers))
	for k, v := range l.Peers {
		out[k] = *v
	}
	return out
}
