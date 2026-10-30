package main

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func newKey(t *testing.T) (crypto.PrivKey, string) {
	k, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := peer.IDFromPrivateKey(k)
	return k, id.String()
}

// forge makes a receipt signed by both parties, as two colluding keys could.
func forge(t *testing.T, ck, sk crypto.PrivKey, r Receipt) SignedReceipt {
	cs, _ := ck.Sign(r.bytes())
	ss, _ := sk.Sign(r.bytes())
	return SignedReceipt{Receipt: r, ClientSig: cs, ServerSig: ss}
}

func TestReceiptCreditsFollowTokens(t *testing.T) {
	ck, cid := newKey(t)
	sk, sid := newKey(t)
	r := Receipt{Job: "j1", Model: "m", Client: cid, Server: sid, Prompt: 40, Completion: 10, Credits: 20, TS: time.Now().Unix()}
	if sr := forge(t, ck, sk, r); !sr.Valid() {
		t.Fatal("honest receipt rejected")
	}
	r.Credits = 1e18
	if sr := forge(t, ck, sk, r); sr.Valid() {
		t.Fatal("credits not derived from tokens, yet accepted")
	}
	r.Completion, r.Credits = 1e15, price(40, 1e15)
	if sr := forge(t, ck, sk, r); sr.Valid() {
		t.Fatal("absurd token count accepted")
	}
}

func TestReceiptGossipLimits(t *testing.T) {
	ck, cid := newKey(t)
	sk, sid := newKey(t)
	rs := &Receipts{seen: map[string]bool{}, pending: map[string]*pendingJob{}}
	server, _ := peer.Decode(sid)
	other, _ := peer.Decode(cid)
	mk := func(job string, ts int64) *SignedReceipt {
		sr := forge(t, ck, sk, Receipt{Job: job, Model: "m", Client: cid, Server: sid, Prompt: 4, Completion: 1, Credits: 2, TS: ts})
		return &sr
	}
	now := time.Now().Unix()
	if rs.acceptGossip(other, mk("a", now)) {
		t.Fatal("receipt relayed under another origin accepted")
	}
	if rs.acceptGossip(server, mk("b", now-2*3600)) {
		t.Fatal("stale receipt accepted")
	}
	ok := 0
	for i := range receiptPerServer + 50 {
		if rs.acceptGossip(server, mk(string(rune('A'+i%26))+time.Now().String(), now)) {
			ok++
		}
	}
	// The minute may roll over mid-loop; allow one extra window.
	if ok < receiptPerServer || ok > 2*receiptPerServer {
		t.Fatalf("per-server limit let %d through", ok)
	}
}

func TestGatewayRefusesRebinding(t *testing.T) {
	ctx := t.Context()
	n := testNode(t, ctx, Config{})
	for host, want := range map[string]int{
		"127.0.0.1:7778": 200, "localhost:7778": 200, "[::1]:7778": 200, "192.168.1.5:7778": 200,
		"evil.example:7778": 403, "localhost.evil.example": 403,
	} {
		r := httptest.NewRequest("GET", "/status", nil)
		r.Host = host
		w := httptest.NewRecorder()
		n.Gateway().ServeHTTP(w, r)
		if w.Code != want {
			t.Errorf("Host %s: %d, want %d", host, w.Code, want)
		}
	}
	// Same Origin and Host, as a rebinding page sends, is still refused.
	r := httptest.NewRequest("POST", "/account/link", nil)
	r.Host = "evil.example:7778"
	r.Header.Set("Origin", "http://evil.example:7778")
	w := httptest.NewRecorder()
	n.Gateway().ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("rebinding POST: %d", w.Code)
	}
}

func TestClamp(t *testing.T) {
	n := &Node{cfg: Config{MaxTokens: 1000}}
	b := map[string]any{"n": 8.0, "best_of": 8.0, "max_tokens": 1e9}
	n.clamp("/chat/completions", b)
	if _, ok := b["n"]; ok || b["max_tokens"] != 1000.0 {
		t.Fatalf("chat: %v", b)
	}
	b = map[string]any{}
	n.clamp("/messages", b)
	if b["max_tokens"] != 1000.0 {
		t.Fatalf("default cap: %v", b)
	}
	b = map[string]any{"max_tokens": 50.0}
	n.clamp("/messages", b)
	if b["max_tokens"] != 50.0 {
		t.Fatalf("lower request changed: %v", b)
	}
	b = map[string]any{}
	n.clamp("/completions", b)
	if _, ok := b["max_tokens"]; ok {
		t.Fatalf("legacy completions default changed: %v", b)
	}
}
