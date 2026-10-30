package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	Rendezvous    = "llmpool/v1"
	AnnounceTopic = "llmpool/announce/1"
)

type Config struct {
	Upstream      string            // OpenAI-compatible base URL to share; empty = client only
	UpstreamKey   string            //
	Models        map[string]string // public name -> upstream name; empty = discover
	Concurrency   int               //
	Listen        []string          // libp2p listen multiaddrs
	Bootstrap     []string          // multiaddrs with /p2p/ID
	DataDir       string            //
	DHTServer     bool              // force DHT server mode (bootstrap nodes, tests)
	MDNS          bool              //
	UPnP          bool              // ask the router to forward our port (exposes the node publicly)
	Private       bool              // assume we are behind NAT: reserve a slot on the bootstrap relays
	FreeCredits   int64             // how far into debt a stranger may go with us
	AnnounceEvery time.Duration     //
}

// Announce is what a serving node gossips about itself.
type Announce struct {
	Models []string `json:"models"`
	Free   int      `json:"free"`
	Max    int      `json:"max"`
}

type peerInfo struct {
	Announce
	seen time.Time
}

type Node struct {
	cfg      Config
	host     host.Host
	dht      *dht.IpfsDHT
	topic    *pubsub.Topic
	ledger   *Ledger
	relays   []peer.AddrInfo // bootstrap nodes, which also relay
	http     *http.Client
	inflight atomic.Int32

	mu    sync.Mutex
	peers map[peer.ID]*peerInfo
}

func loadKey(path string) (crypto.PrivKey, error) {
	if b, err := os.ReadFile(path); err == nil {
		return crypto.UnmarshalPrivateKey(b)
	}
	k, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		return nil, err
	}
	b, _ := crypto.MarshalPrivateKey(k)
	return k, os.WriteFile(path, b, 0o600)
}

func NewNode(ctx context.Context, cfg Config) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, err
	}
	key, err := loadKey(filepath.Join(cfg.DataDir, "identity.key"))
	if err != nil {
		return nil, err
	}
	var bootstrap []peer.AddrInfo
	for _, s := range cfg.Bootstrap {
		ai, err := peer.AddrInfoFromString(s)
		if err != nil {
			return nil, fmt.Errorf("bootstrap %q: %w", s, err)
		}
		bootstrap = append(bootstrap, *ai)
	}

	n := &Node{
		cfg:    cfg,
		ledger: OpenLedger(filepath.Join(cfg.DataDir, "ledger.json")),
		http:   &http.Client{},
		peers:  map[peer.ID]*peerInfo{},
		relays: bootstrap,
	}
	mode := dht.ModeAutoServer
	if cfg.DHTServer {
		mode = dht.ModeServer
	}
	opts := []libp2p.Option{
		libp2p.Identity(key),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		// Public nodes relay for NATed ones until hole punching upgrades the link.
		// Inference streams are long, so the relay must not cut them off.
		libp2p.EnableRelayService(relay.WithInfiniteLimits()),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			var err error
			n.dht, err = dht.New(h, dht.Mode(mode), dht.ProtocolPrefix("/llmpool"),
				dht.BootstrapPeers(bootstrap...))
			return n.dht, err
		}),
	}
	if len(cfg.Listen) > 0 {
		opts = append(opts, libp2p.ListenAddrStrings(cfg.Listen...))
	} else {
		opts = append(opts, libp2p.NoListenAddrs)
	}
	// AutoNAT needs several peers to vouch before it calls us private, which a
	// small network may never have; without that we would never take a relay
	// slot and nobody could reach us. So non-bootstrap nodes just assume NAT.
	// Bootstrap nodes are public by definition, and the relay service only
	// starts once we know we are public.
	if cfg.Private {
		opts = append(opts, libp2p.ForceReachabilityPrivate())
	} else if cfg.DHTServer {
		opts = append(opts, libp2p.ForceReachabilityPublic())
	}
	if cfg.UPnP {
		opts = append(opts, libp2p.NATPortMap())
	}
	if len(bootstrap) > 0 {
		opts = append(opts, libp2p.EnableAutoRelayWithStaticRelays(bootstrap,
			autorelay.WithMinCandidates(1), autorelay.WithBootDelay(0)))
	}
	if n.host, err = libp2p.New(opts...); err != nil {
		return nil, err
	}

	if cfg.Upstream != "" {
		if len(n.cfg.Models) == 0 {
			if n.cfg.Models, err = n.discoverModels(ctx); err != nil {
				return nil, fmt.Errorf("upstream %s: %w", cfg.Upstream, err)
			}
		}
		n.host.SetStreamHandler(InferProto, n.handleInfer)
	}

	ps, err := pubsub.NewGossipSub(ctx, n.host)
	if err != nil {
		return nil, err
	}
	if n.topic, err = ps.Join(AnnounceTopic); err != nil {
		return nil, err
	}
	sub, err := n.topic.Subscribe()
	if err != nil {
		return nil, err
	}
	go n.readAnnounces(ctx, sub)

	for _, ai := range bootstrap {
		if err := n.host.Connect(ctx, ai); err != nil {
			log.Printf("bootstrap %s: %v", ai.ID, err)
		}
	}
	if err := n.dht.Bootstrap(ctx); err != nil {
		return nil, err
	}
	if cfg.MDNS {
		mdns.NewMdnsService(n.host, Rendezvous, n).Start()
	}
	go n.discoverLoop(ctx)
	if cfg.Upstream != "" {
		go n.announceLoop(ctx)
	}
	return n, nil
}

// HandlePeerFound is the mDNS callback.
func (n *Node) HandlePeerFound(ai peer.AddrInfo) {
	if ai.ID != n.host.ID() {
		n.host.Connect(context.Background(), ai)
	}
}

func (n *Node) discoverLoop(ctx context.Context) {
	disc := drouting.NewRoutingDiscovery(n.dht)
	dutil.Advertise(ctx, disc, Rendezvous)
	for {
		if ch, err := disc.FindPeers(ctx, Rendezvous); err == nil {
			for ai := range ch {
				if ai.ID != n.host.ID() && n.host.Network().Connectedness(ai.ID) != network.Connected {
					n.host.Connect(ctx, ai)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
	}
}

func (n *Node) announceLoop(ctx context.Context) {
	models := make([]string, 0, len(n.cfg.Models))
	for m := range n.cfg.Models {
		models = append(models, m)
	}
	sort.Strings(models)
	for {
		free := n.cfg.Concurrency - int(n.inflight.Load())
		b, _ := json.Marshal(Announce{Models: models, Free: max(free, 0), Max: n.cfg.Concurrency})
		n.topic.Publish(ctx, b)
		select {
		case <-ctx.Done():
			return
		case <-time.After(n.cfg.AnnounceEvery):
		}
	}
}

func (n *Node) readAnnounces(ctx context.Context, sub *pubsub.Subscription) {
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		from := msg.GetFrom() // origin (signed), not the forwarding hop
		if from == n.host.ID() {
			continue
		}
		var a Announce
		if json.Unmarshal(msg.Data, &a) != nil {
			continue
		}
		n.mu.Lock()
		n.peers[from] = &peerInfo{Announce: a, seen: time.Now()}
		n.mu.Unlock()
	}
}

// alive returns announces seen within three announce periods.
func (n *Node) alive() map[peer.ID]Announce {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := map[peer.ID]Announce{}
	for p, pi := range n.peers {
		if time.Since(pi.seen) < 3*n.cfg.AnnounceEvery+5*time.Second {
			out[p] = pi.Announce
		} else {
			delete(n.peers, p)
		}
	}
	return out
}

// candidates lists remote peers serving model, best first.
func (n *Node) candidates(model string) []peer.ID {
	type c struct {
		id   peer.ID
		free int
		rtt  time.Duration
	}
	var cs []c
	for p, a := range n.alive() {
		for _, m := range a.Models {
			if m == model {
				cs = append(cs, c{p, a.Free, n.host.Peerstore().LatencyEWMA(p)})
				break
			}
		}
	}
	sort.Slice(cs, func(i, j int) bool {
		if (cs[i].free > 0) != (cs[j].free > 0) {
			return cs[i].free > 0
		}
		return cs[i].rtt < cs[j].rtt
	})
	out := make([]peer.ID, len(cs))
	for i := range cs {
		out[i] = cs[i].id
	}
	return out
}

// addRelayPaths records a route to p through every bootstrap relay. autorelay
// only advertises circuit addresses built from a relay's public addresses, and
// a peer we only heard of over gossip may have no address at all, so we build
// the routes ourselves. Direct addresses, when known, are still dialed first,
// and hole punching upgrades a relayed connection when it can.
func (n *Node) addRelayPaths(p peer.ID) {
	for _, r := range n.relays {
		if r.ID == p {
			continue
		}
		circuit := ma.StringCast("/p2p/" + r.ID.String() + "/p2p-circuit")
		for _, a := range r.Addrs {
			n.host.Peerstore().AddAddr(p, a.Encapsulate(circuit), peerstore.TempAddrTTL)
		}
	}
}

func (n *Node) discoverModels(ctx context.Context) (map[string]string, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", n.cfg.Upstream+"/models", nil)
	n.upstreamAuth(req)
	resp, err := n.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		Data []struct{ ID string } `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, m := range body.Data {
		out[m.ID] = m.ID
	}
	return out, nil
}

func (n *Node) upstreamAuth(req *http.Request) {
	if n.cfg.UpstreamKey != "" {
		req.Header.Set("Authorization", "Bearer "+n.cfg.UpstreamKey)
	}
}

func (n *Node) Addrs() []string {
	var out []string
	for _, a := range n.host.Addrs() {
		out = append(out, a.Encapsulate(ma.StringCast("/p2p/"+n.host.ID().String())).String())
	}
	return out
}
