package main

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

// Abuse limits. Peer ids cost nothing, so none of these stop a determined
// attacker alone; they bound what one identity, or a burst of new ones, can
// take from a node, and keep gossip state from growing without limit.

const (
	peerRequestsPerMinute = 30 // per remote peer, inference requests we serve
	peerRequestBurst      = 10
	maxPeers              = 4096 // announcing peers we remember
	maxClaims             = 20000
	maxAnnounceModels     = 16
	maxModelName          = 128
	maxAnnounceSlots      = 256
)

// peerLimiter is a token bucket per remote peer.
type peerLimiter struct {
	mu   sync.Mutex
	bkts map[peer.ID]*bucket
}

type bucket struct {
	lim  *rate.Limiter
	last time.Time
}

func (pl *peerLimiter) allow(p peer.ID, now time.Time) bool {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if pl.bkts == nil {
		pl.bkts = map[peer.ID]*bucket{}
	}
	b := pl.bkts[p]
	if b == nil {
		if len(pl.bkts) >= maxPeers {
			for id, o := range pl.bkts { // drop buckets idle long enough to be full again
				if now.Sub(o.last) > time.Minute {
					delete(pl.bkts, id)
				}
			}
		}
		b = &bucket{lim: rate.NewLimiter(rate.Every(time.Minute/peerRequestsPerMinute), peerRequestBurst)}
		pl.bkts[p] = b
	}
	b.last = now
	return b.lim.AllowN(now, 1)
}

// reservedSlots is how many serving slots only contributors (peers that have
// served us more than we served them) may use. A single-slot node reserves none,
// or strangers could never be served.
func reservedSlots(concurrency int) int {
	if concurrency < 2 {
		return 0
	}
	return max(1, concurrency/4)
}

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
