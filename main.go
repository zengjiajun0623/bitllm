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

// Version is set at release time with -ldflags "-X main.Version=v0.1.3".
var Version = "dev"

// DefaultBootstrap is used when no --bootstrap is given. Anyone can run a
// bootstrap node; this one sits behind a Cloudflare Tunnel.
var DefaultBootstrap = []string{
	"/dns4/boot.bitllm.org/tcp/443/wss/p2p/12D3KooWQEQQJo9RgXGDnioD3zK5JyP7QenbY7pGdLk6rhP24rib",
}

// DefaultRoots are the accounts at the top of the invite tree (zengjiajun.eth).
// With none, membership is off and every node is treated as a member.
var DefaultRoots = []string{"0xf3f527b70af6012f328d3ede7fcd3f5f3ca6cba2"}

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(s string) error { *l = append(*l, s); return nil }

func main() {
	home, _ := os.UserHomeDir()
	if len(os.Args) > 1 && os.Args[1] == "account" {
		accountCmd(os.Args[2:], home)
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "invite" {
		inviteCmd(os.Args[2:])
		return
	}
	var (
		cfg          Config
		models, boot listFlag
		announce     listFlag
		roots        listFlag
		wsPort       int
		gateway      string
		port         int
	)
	flag.StringVar(&cfg.Upstream, "upstream", "", "OpenAI-compatible base URL to share, e.g. http://localhost:8080/v1 (empty: use the network only)")
	flag.StringVar(&cfg.UpstreamKey, "upstream-key", "", "API key for the upstream")
	flag.Var(&models, "model", "local-name or local-name=public-name to share (repeatable; default: every upstream model)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 1, "requests served in parallel")
	flag.Var(&boot, "bootstrap", "multiaddr of a known peer, /ip4/.../tcp/4001/p2p/12D3... (repeatable; default: boot.bitllm.org)")
	flag.StringVar(&gateway, "gateway", "127.0.0.1:7778", "local OpenAI-compatible gateway address")
	flag.IntVar(&port, "port", 4001, "p2p port, TCP and UDP (QUIC)")
	flag.IntVar(&wsPort, "ws-port", 0, "also accept libp2p over WebSocket on 127.0.0.1:PORT, for an HTTPS tunnel (Cloudflare Tunnel etc.) in front of a bootstrap node")
	flag.Var(&roots, "roots", "root account of the invite tree (repeatable; default: the BitLLM root). For private networks and tests")
	flag.Var(&announce, "announce", "public multiaddr to advertise, e.g. /dns4/boot.example.com/tcp/443/wss (repeatable)")
	flag.StringVar(&cfg.DataDir, "data", filepath.Join(home, ".bitllm"), "identity and ledger directory")
	flag.BoolVar(&cfg.DHTServer, "dht-server", false, "always act as a DHT server (for bootstrap nodes with a public address)")
	flag.BoolVar(&cfg.MDNS, "mdns", true, "find peers on the local network")
	flag.BoolVar(&cfg.UPnP, "upnp", true, "ask the router (UPnP/NAT-PMP) to forward the p2p port so peers can reach us directly")
	flag.IntVar(&cfg.MaxTokens, "max-tokens", 32768, "most new tokens a peer may ask for in one request (0: no limit)")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.BoolVar(&cfg.Open, "open", false, "serve uninvited nodes like members (any free slot, --free-daily per node), not only when idle")
	flag.Int64Var(&cfg.FreeDaily, "free-daily", 200_000, "credits each member may use per UTC day on this server (contributors: no limit; 0: no limit)")
	flag.Parse()
	if *showVersion {
		fmt.Println("bitllm", Version)
		return
	}

	cfg.Upstream = strings.TrimRight(cfg.Upstream, "/")
	cfg.Bootstrap = boot
	cfg.Roots = DefaultRoots
	if len(roots) > 0 {
		cfg.Roots = roots
	}
	if len(boot) == 0 && !cfg.DHTServer {
		cfg.Bootstrap = DefaultBootstrap
	}
	cfg.Private = !cfg.DHTServer
	cfg.AnnounceEvery = 10 * time.Second
	cfg.Listen = []string{
		fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port),
		fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", port),
		fmt.Sprintf("/ip6/::/tcp/%d", port),
		fmt.Sprintf("/ip6/::/udp/%d/quic-v1", port),
	}
	if wsPort > 0 {
		// Peers arriving through the tunnel all come from 127.0.0.1.
		cfg.RelayBehindProxy = true
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
	log.Printf("bitllm %s, peer id %s", Version, n.host.ID())
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
