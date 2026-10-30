package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func randomPeer(t *testing.T) peer.ID {
	k, _, _ := crypto.GenerateEd25519Key(nil)
	p, err := peer.IDFromPrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAdmitReservesSlotsForContributors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := testNode(t, ctx, Config{Concurrency: 4}) // 1 slot reserved
	stranger, friend := randomPeer(t), randomPeer(t)
	n.ledger.Received(friend, 100) // friend has served us
	now := time.Now()
	for i := range 3 {
		if s, _ := n.admit(stranger, now); s != 0 {
			t.Fatalf("stranger refused slot %d", i+1)
		}
	}
	if s, _ := n.admit(stranger, now); s != 429 {
		t.Fatal("stranger took the reserved slot")
	}
	if s, _ := n.admit(friend, now); s != 0 {
		t.Fatal("contributor refused the reserved slot")
	}
	if s, _ := n.admit(friend, now); s != 429 {
		t.Fatal("admitted past concurrency")
	}
	if reservedSlots(1) != 0 {
		t.Fatal("a single-slot node must not reserve its only slot")
	}
}

func TestPeerRateLimit(t *testing.T) {
	var pl peerLimiter
	p, q := randomPeer(t), randomPeer(t)
	now := time.Now()
	for i := range peerRequestBurst {
		if !pl.allow(p, now) {
			t.Fatalf("refused request %d within the burst", i+1)
		}
	}
	if pl.allow(p, now) {
		t.Fatal("allowed past the burst")
	}
	if !pl.allow(q, now) {
		t.Fatal("one peer's burst limited another")
	}
	if !pl.allow(p, now.Add(time.Minute/peerRequestsPerMinute)) {
		t.Fatal("bucket did not refill")
	}
}

func TestValidAnnounce(t *testing.T) {
	ok := Announce{Models: []string{"m"}, Free: 1, Max: 2}
	if !validAnnounce(&ok) {
		t.Fatal("rejected a normal announce")
	}
	many := make([]string, maxAnnounceModels+1)
	for i := range many {
		many[i] = "m"
	}
	for name, a := range map[string]Announce{
		"no models":  {Max: 1},
		"too many":   {Models: many, Max: 1},
		"long name":  {Models: []string{strings.Repeat("x", maxModelName+1)}, Max: 1},
		"empty name": {Models: []string{""}, Max: 1},
		"free > max": {Models: []string{"m"}, Free: 3, Max: 2},
		"negative":   {Models: []string{"m"}, Free: -1, Max: 2},
		"huge max":   {Models: []string{"m"}, Max: maxAnnounceSlots + 1},
		"zero slots": {Models: []string{"m"}},
	} {
		if validAnnounce(&a) {
			t.Fatalf("accepted %s", name)
		}
	}
}

func TestOriginRate(t *testing.T) {
	o := originRate{every: time.Second}
	p := randomPeer(t)
	now := time.Now()
	if !o.allow(p, now) || o.allow(p, now.Add(500*time.Millisecond)) || !o.allow(p, now.Add(time.Second)) {
		t.Fatal("origin rate wrong")
	}
}
