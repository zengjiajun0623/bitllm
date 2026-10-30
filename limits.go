package main

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Gossip limits. Peer ids cost nothing, so these do not stop a determined
// attacker alone; they keep gossip state from growing without limit.

const (
	maxPeers          = 4096 // announcing peers we remember
	maxClaims         = 20000
	maxAnnounceModels = 16
	maxModelName      = 128
	maxAnnounceSlots  = 256
)

// originRate allows one gossip message per origin per interval.
type originRate struct {
	mu    sync.Mutex
	every time.Duration
	last  map[peer.ID]time.Time
}

func (o *originRate) allow(p peer.ID, now time.Time) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.last == nil {
		o.last = map[peer.ID]time.Time{}
	}
	if t, ok := o.last[p]; ok && now.Sub(t) < o.every {
		return false
	}
	if len(o.last) >= 4*maxPeers {
		for id, t := range o.last {
			if now.Sub(t) > o.every {
				delete(o.last, id)
			}
		}
	}
	o.last[p] = now
	return true
}

func validAnnounce(a *Announce) bool {
	if len(a.Models) == 0 || len(a.Models) > maxAnnounceModels ||
		a.Free < 0 || a.Max < 1 || a.Free > a.Max || a.Max > maxAnnounceSlots || len(a.Version) > 64 {
		return false
	}
	for _, m := range a.Models {
		if m == "" || len(m) > maxModelName {
			return false
		}
	}
	return true
}
