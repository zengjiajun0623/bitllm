// bitllm: a peer-to-peer network of local LLMs. Every node is the same
// program: it can share an OpenAI-compatible server with the network and it
// runs a local gateway that answers from any peer. No coordinator.
//
//	bitllm --upstream http://localhost:8080/v1 --bootstrap /ip4/.../p2p/12D3...
//	curl localhost:7778/v1/models
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(s string) error { *l = append(*l, s); return nil }

func main() {
	home, _ := os.UserHomeDir()
	var (
		cfg          Config
		models, boot listFlag
		announce     listFlag
		wsPort       int
		gateway      string
		port         int
	)
	flag.StringVar(&cfg.Upstream, "upstream", "", "OpenAI-compatible base URL to share, e.g. http://localhost:8080/v1 (empty: use the network only)")
	flag.StringVar(&cfg.UpstreamKey, "upstream-key", "", "API key for the upstream")
	flag.Var(&models, "model", "local-name or local-name=public-name to share (repeatable; default: every upstream model)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 1, "requests served in parallel")
	flag.Var(&boot, "bootstrap", "multiaddr of a known peer, /ip4/.../tcp/4001/p2p/12D3... (repeatable)")
	flag.StringVar(&gateway, "gateway", "127.0.0.1:7778", "local OpenAI-compatible gateway address")
	flag.IntVar(&port, "port", 4001, "p2p port, TCP and UDP (QUIC)")
	flag.IntVar(&wsPort, "ws-port", 0, "also accept libp2p over WebSocket on 127.0.0.1:PORT, for an HTTPS tunnel (Cloudflare Tunnel etc.) in front of a bootstrap node")
	flag.Var(&announce, "announce", "public multiaddr to advertise, e.g. /dns4/boot.example.com/tcp/443/wss (repeatable)")
	flag.StringVar(&cfg.DataDir, "data", filepath.Join(home, ".bitllm"), "identity and ledger directory")
	flag.BoolVar(&cfg.DHTServer, "dht-server", false, "always act as a DHT server (for bootstrap nodes with a public address)")
	flag.BoolVar(&cfg.MDNS, "mdns", true, "find peers on the local network")
	flag.BoolVar(&cfg.UPnP, "upnp", true, "ask the router (UPnP/NAT-PMP) to forward the p2p port so peers can reach us directly")
	flag.Int64Var(&cfg.FreeCredits, "free-credits", 50_000, "credits a peer may take from us before it must give back")
	flag.Parse()

	cfg.Upstream = strings.TrimRight(cfg.Upstream, "/")
	cfg.Bootstrap = boot
	cfg.Private = !cfg.DHTServer
	cfg.AnnounceEvery = 10 * time.Second
	cfg.Listen = []string{
		fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port),
		fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", port),
		fmt.Sprintf("/ip6/::/tcp/%d", port),
		fmt.Sprintf("/ip6/::/udp/%d/quic-v1", port),
	}
	if wsPort > 0 {
		cfg.Listen = append(cfg.Listen, fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/ws", wsPort))
	}
	cfg.Announce = announce
	if len(models) > 0 {
		cfg.Models = map[string]string{}
		for _, m := range models {
			local, public, _ := strings.Cut(m, "=")
			if public == "" {
				public = local
			}
			cfg.Models[public] = local
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	n, err := NewNode(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("peer id %s", n.host.ID())
	for _, a := range n.Addrs() {
		log.Printf("listening %s", a)
	}
	if cfg.Upstream != "" {
		log.Printf("sharing %v from %s", cfg.Models, cfg.Upstream)
	}
	log.Printf("gateway http://%s/v1", gateway)
	srv := &http.Server{Addr: gateway, Handler: n.Gateway()}
	go func() { <-ctx.Done(); srv.Close() }()
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
