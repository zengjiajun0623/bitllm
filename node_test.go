package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// mockUpstream is an OpenAI-compatible server with one model; every reply
// costs 40 prompt + 10 completion tokens = 20 credits.
func mockUpstream(t *testing.T, model string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			fmt.Fprintf(w, `{"data":[{"id":%q}]}`, model)
		case "/v1/chat/completions":
			var req struct {
				Model         string `json:"model"`
				Stream        bool   `json:"stream"`
				StreamOptions struct {
					IncludeUsage bool `json:"include_usage"`
				} `json:"stream_options"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			if req.Model != model {
				http.Error(w, "wrong model "+req.Model, 404)
				return
			}
			usage := `{"prompt_tokens":40,"completion_tokens":10}`
			if !req.Stream {
				fmt.Fprintf(w, `{"choices":[{"message":{"content":"hello world"}}],"usage":%s}`, usage)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			for _, tok := range []string{"hel", "lo ", "world"} {
				fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", tok)
			}
			if req.StreamOptions.IncludeUsage {
				fmt.Fprintf(w, "data: {\"choices\":[],\"usage\":%s}\n\n", usage)
			}
			fmt.Fprint(w, "data: [DONE]\n\n")
		default:
			http.NotFound(w, r)
		}
	}))
}

func testNode(t *testing.T, ctx context.Context, cfg Config) *Node {
	cfg.Listen = []string{"/ip4/127.0.0.1/tcp/0"}
	cfg.DataDir = t.TempDir()
	cfg.DHTServer = true
	cfg.AnnounceEvery = 300 * time.Millisecond
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 2
	}
	n, err := NewNode(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.host.Close() })
	return n
}

func post(n *Node, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	n.Gateway().ServeHTTP(w, r)
	return w
}

func TestNetwork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	up := mockUpstream(t, "local-ds")
	defer up.Close()

	boot := testNode(t, ctx, Config{})
	bs := boot.Addrs()[:1]
	server := testNode(t, ctx, Config{Bootstrap: bs, Upstream: up.URL + "/v1",
		Models: map[string]string{"deepseek-v4.1-flash": "local-ds"}, FreeCredits: 30})
	client := testNode(t, ctx, Config{Bootstrap: bs})

	// The client only knows the bootstrap node; it must learn about the server
	// through the network (DHT rendezvous + gossip), not a direct dial.
	deadline := time.Now().Add(20 * time.Second)
	for len(client.candidates("deepseek-v4.1-flash")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("client never discovered the server's model")
		}
		time.Sleep(100 * time.Millisecond)
	}

	w := httptest.NewRecorder()
	client.Gateway().ServeHTTP(w, httptest.NewRequest("GET", "/v1/models", nil))
	if !strings.Contains(w.Body.String(), `"id":"deepseek-v4.1-flash"`) {
		t.Fatalf("models: %s", w.Body)
	}

	w = post(client, `{"model":"deepseek-v4.1-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"world"`) || !strings.Contains(w.Body.String(), "[DONE]") {
		t.Fatalf("stream: %d %s", w.Code, w.Body)
	}
	if w.Header().Get("X-Llmpool-Peer") != server.host.ID().String() {
		t.Fatalf("served by %q", w.Header().Get("X-Llmpool-Peer"))
	}

	w = post(client, `{"model":"deepseek-v4.1-flash","messages":[{"role":"user","content":"hi"}]}`)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "hello world") {
		t.Fatalf("non-stream: %d %s", w.Code, w.Body)
	}

	// Both sides recorded the same 2 x 20 credits, each in its own ledger.
	if b := client.ledger.Balance(server.host.ID()); b != 40 {
		t.Fatalf("client sees server balance %d, want 40", b)
	}
	if b := server.ledger.Balance(client.host.ID()); b != -40 {
		t.Fatalf("server sees client balance %d, want -40", b)
	}

	// Both jobs produced countersigned receipts, gossiped to every node.
	srvID := server.host.ID().String()
	waitCredits := func(n *Node, who string) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); ; {
			if c := n.receipts.Tally()[srvID]; c.Credits == 40 && c.Jobs == 2 && c.Clients == 1 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s tally: %+v", who, n.receipts.Tally())
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	waitCredits(client, "client")
	waitCredits(boot, "bootstrap")
	waitCredits(server, "server")

	// A node that joins later catches up from the bootstrap node.
	late := testNode(t, ctx, Config{Bootstrap: bs})
	waitCredits(late, "late joiner")

	// Tampering with a receipt breaks its signatures.
	sr := late.receipts.all[0]
	if !sr.Valid() {
		t.Fatal("stored receipt invalid")
	}
	sr.Receipt.Credits *= 10
	if sr.Valid() {
		t.Fatal("inflated receipt still valid")
	}

	// The client is now 40 in debt, past the server's 30 free credits.
	w = post(client, `{"model":"deepseek-v4.1-flash","messages":[]}`)
	if w.Code != 402 {
		t.Fatalf("debt limit: %d %s", w.Code, w.Body)
	}

	w = post(client, `{"model":"no-such-model","messages":[]}`)
	if w.Code != 503 {
		t.Fatalf("unknown model: %d %s", w.Code, w.Body)
	}

	// The server's own gateway answers from its own upstream, free.
	w = post(server, `{"model":"deepseek-v4.1-flash","messages":[]}`)
	if w.Code != 200 || w.Header().Get("X-Llmpool-Peer") != "" {
		t.Fatalf("local: %d %s", w.Code, w.Body)
	}
}

// TestRelayed: server and client accept no inbound connections at all (worse
// than NAT: hole punching cannot work either), so every byte must go through
// the bootstrap node's relay.
func TestRelayed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	up := mockUpstream(t, "local-ds")
	defer up.Close()

	boot := testNode(t, ctx, Config{})
	bs := boot.Addrs()[:1]
	natted := func(cfg Config) *Node {
		cfg.Bootstrap, cfg.Private, cfg.DataDir = bs, true, t.TempDir()
		cfg.AnnounceEvery, cfg.Concurrency = 300*time.Millisecond, 2
		n, err := NewNode(ctx, cfg) // no Listen: unreachable except via relay
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { n.host.Close() })
		return n
	}
	server := natted(Config{Upstream: up.URL + "/v1", Models: map[string]string{"ds": "local-ds"}, FreeCredits: 1000})
	client := natted(Config{})

	deadline := time.Now().Add(30 * time.Second)
	for {
		w := post(client, `{"model":"ds","stream":true,"messages":[]}`)
		if w.Code == 200 && strings.Contains(w.Body.String(), "[DONE]") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never served through the relay; last: %d %s", w.Code, w.Body)
		}
		time.Sleep(500 * time.Millisecond)
	}
	relayed := false
	for _, c := range client.host.Network().ConnsToPeer(server.host.ID()) {
		relayed = relayed || strings.Contains(c.RemoteMultiaddr().String(), "/p2p-circuit")
	}
	if !relayed {
		t.Fatal("expected a relayed connection to the server")
	}
}

// TestBusyQueues: with one slot on the only server, concurrent requests queue
// instead of failing.
func TestBusyQueues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			fmt.Fprint(w, `{"data":[{"id":"m"}]}`)
			return
		}
		time.Sleep(700 * time.Millisecond)
		fmt.Fprint(w, `{"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":1}}`)
	}))
	defer slow.Close()
	boot := testNode(t, ctx, Config{})
	bs := boot.Addrs()[:1]
	server := testNode(t, ctx, Config{Bootstrap: bs, Upstream: slow.URL + "/v1", Concurrency: 1, FreeCredits: 1000})
	client := testNode(t, ctx, Config{Bootstrap: bs})
	for deadline := time.Now().Add(20 * time.Second); len(client.candidates("m")) == 0; {
		if time.Now().After(deadline) {
			t.Fatal("no discovery")
		}
		time.Sleep(100 * time.Millisecond)
	}
	codes := make(chan int, 3)
	for range 3 {
		go func() { codes <- post(client, `{"model":"m","messages":[]}`).Code }()
	}
	for range 3 {
		if c := <-codes; c != 200 {
			t.Fatalf("got %d, want every request served in turn", c)
		}
	}
	for deadline := time.Now().Add(10 * time.Second); server.receipts.Tally()[server.host.ID().String()].Jobs != 3; {
		if time.Now().After(deadline) {
			t.Fatalf("receipts: %+v", server.receipts.Tally())
		}
		time.Sleep(100 * time.Millisecond)
	}
}
