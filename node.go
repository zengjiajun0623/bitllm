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
	Rendezvous    = "bitllm/v1"
	AnnounceTopic = "bitllm/announce/1"
)

type Config struct {
	Upstream         string            // OpenAI-compatible base URL to share; empty = client only
	UpstreamKey      string            //
	Models           map[string]string // public name -> upstream name; empty = discover
	Concurrency      int               //
	Listen           []string          // libp2p listen multiaddrs
	Bootstrap        []string          // multiaddrs with /p2p/ID
	DataDir          string            //
	DHTServer        bool              // force DHT server mode (bootstrap nodes, tests)
	MDNS             bool              //
	UPnP             bool              // ask the router to forward our port (exposes the node publicly)
	Private          bool              // assume we are behind NAT: reserve a slot on the bootstrap relays
	RelayBehindProxy bool              // peers reach our relay through a tunnel, all from one IP
	Announce         []string          // extra public addrs, e.g. /dns4/boot.example.com/tcp/443/wss behind a tunnel
	FreeDaily        int64             // credits a member may use per UTC day on this server; contributors are unlimited
	Roots            []string          // accounts at the top of the invite tree; none = membership off
	Open             bool              // serve uninvited nodes like members (any free slot, quota per node)
	OpenSlots        int               // most slots uninvited nodes may hold on an open server; 0 = half
	OpenDaily        int64             // credits all uninvited nodes together may use per UTC day on an open server; 0 = no limit
	MaxTokens        int               // cap on new tokens per request we serve; 0 = none
	AnnounceEvery    time.Duration     //
}

// Announce is what a serving node gossips about itself.
type Announce struct {
	Version string   `json:"v,omitempty"` // program version, so a future release can tell old nodes apart
	Models  []string `json:"models"`
	Free    int      `json:"free"`
	Max     int      `json:"max"`
}

type peerInfo struct {
	Announce
	seen time.Time
}

type Node struct {
	cfg          Config
	host         host.Host
	dht          *dht.IpfsDHT
	topic        *pubsub.Topic
	ledger       *Ledger
	receipts     *Receipts
	claims       *Claims
	claimTop     *pubsub.Topic
	nodeKey      crypto.PrivKey
	members      *Members
	memTop       *pubsub.Topic
	memRate      originRate // membership gossip, per origin
	quota        *Quota
	accessMu     sync.Mutex
	accessAt     time.Time
	access       *MemberSet // cached, see memberSet
	servedMu     sync.Mutex // own lock: memberSet holds accessMu while Compute calls servedSets
	servedAt     time.Time
	servedEver   map[string]bool // cached, see servedSets
	servedRecent map[string]bool
	annRate      originRate // announce gossip, per origin
	claimRate    originRate // claim gossip, per origin
	receiptSeed  []byte
	relays       []peer.AddrInfo // bootstrap nodes, which also relay
	http         *http.Client
	inflight     atomic.Int32
	openInflight atomic.Int32 // slots held by uninvited nodes on an open server

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
	// Several addresses of one peer (tcp, quic...) must become one AddrInfo, or
	// the same node is dialed, synced and used as a relay once per address.
	var baddrs []ma.Multiaddr
	for _, s := range cfg.Bootstrap {
		a, err := ma.NewMultiaddr(s)
		if err != nil {
			return nil, fmt.Errorf("bootstrap %q: %w", s, err)
		}
		baddrs = append(baddrs, a)
	}
	bootstrap, err := peer.AddrInfosFromP2pAddrs(baddrs...)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: %w", err)
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
		libp2p.EnableRelayService(relay.WithResources(relayResources(cfg.RelayBehindProxy))),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			var err error
			n.dht, err = dht.New(h, dht.Mode(mode), dht.ProtocolPrefix("/bitllm"),
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
	if len(cfg.Announce) > 0 {
		var extra []ma.Multiaddr
		for _, a := range cfg.Announce {
			m, err := ma.NewMultiaddr(a)
			if err != nil {
				return nil, fmt.Errorf("announce %q: %w", a, err)
			}
			extra = append(extra, m)
		}
		opts = append(opts, libp2p.AddrsFactory(func(as []ma.Multiaddr) []ma.Multiaddr {
			return append(as, extra...)
		}))
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

	n.nodeKey = key
	if n.receiptSeed, err = loadSeed(filepath.Join(cfg.DataDir, "receipt.seed")); err != nil {
		return nil, err
	}
	n.claims = OpenClaims(filepath.Join(cfg.DataDir, "claims.json"))
	n.quota = OpenQuota(filepath.Join(cfg.DataDir, "quota.json"))
	if n.receipts, err = OpenReceipts(filepath.Join(cfg.DataDir, "receipts.jsonl"), key, n.host.ID()); err != nil {
		return nil, err
	}
	// After receipts and claims: replaying saved invites needs to know who served.
	n.members = OpenMembers(filepath.Join(cfg.DataDir, "members.json"), cfg.Roots, n.servedSets)
	n.host.SetStreamHandler(ReceiptProto, n.handleReceipt)
	n.host.SetStreamHandler(SyncProto, n.handleSync)

	ps, err := pubsub.NewGossipSub(ctx, n.host)
	if err != nil {
		return nil, err
	}
	ps.RegisterTopicValidator(ReceiptTopic, func(_ context.Context, _ peer.ID, msg *pubsub.Message) bool {
		var sr SignedReceipt
		return json.Unmarshal(msg.Data, &sr) == nil && n.receipts.acceptGossip(msg.GetFrom(), &sr)
	})
	if n.receipts.topic, err = ps.Join(ReceiptTopic); err != nil {
		return nil, err
	}
	rsub, err := n.receipts.topic.Subscribe()
	if err != nil {
		return nil, err
	}
	go n.readReceipts(ctx, rsub)
	// A claim is gossiped by the node it names, at most once per 2 s (the node
	// re-gossips its claim 5 s after start and then every 5 min).
	n.claimRate.every = 2 * time.Second
	ps.RegisterTopicValidator(ClaimTopic, func(_ context.Context, _ peer.ID, msg *pubsub.Message) bool {
		var c Claim
		return json.Unmarshal(msg.Data, &c) == nil && c.Node == msg.GetFrom().String() &&
			c.Valid() && n.claimRate.allow(msg.GetFrom(), time.Now())
	})
	n.memRate.every = time.Second
	ps.RegisterTopicValidator(MembersTopic, func(_ context.Context, _ peer.ID, msg *pubsub.Message) bool {
		var it memberItem
		if json.Unmarshal(msg.Data, &it) != nil || !n.memRate.allow(msg.GetFrom(), time.Now()) {
			return false
		}
		// Cheap filter first: an invite from someone who is not a member (per
		// the 10 s cache) is dropped before the full check, which recomputes
		// the member set and costs CPU that outsiders must not be able to spend.
		if it.Invite != nil && n.members.Enabled() && !n.memberSet().Member[it.Invite.Inviter] {
			return false
		}
		return n.members.known(it) || n.members.check(it) == nil
	})
	if n.memTop, err = ps.Join(MembersTopic); err != nil {
		return nil, err
	}
	msub, err := n.memTop.Subscribe()
	if err != nil {
		return nil, err
	}
	go n.readMembers(ctx, msub)
	n.annRate.every = cfg.AnnounceEvery / 2
	ps.RegisterTopicValidator(AnnounceTopic, func(_ context.Context, _ peer.ID, msg *pubsub.Message) bool {
		var a Announce
		return json.Unmarshal(msg.Data, &a) == nil && validAnnounce(&a) && n.annRate.allow(msg.GetFrom(), time.Now())
	})
	if n.claimTop, err = ps.Join(ClaimTopic); err != nil {
		return nil, err
	}
	csub, err := n.claimTop.Subscribe()
	if err != nil {
		return nil, err
	}
	go n.readClaims(ctx, csub)
	go n.claimLoop(ctx)
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
			continue
		}
		if added, err := n.syncReceipts(ctx, ai.ID); err != nil {
			log.Printf("receipt sync from %s: %v", ai.ID, err)
		} else if added > 0 {
			log.Printf("synced %d receipts from %s", added, ai.ID)
		}
	}
	if err := n.dht.Bootstrap(ctx); err != nil {
		return nil, err
	}
	if cfg.MDNS {
		mdns.NewMdnsService(n.host, Rendezvous, n).Start()
	}
	go n.discoverLoop(ctx)
	go n.syncLoop(ctx)
	if cfg.Upstream != "" {
		go n.announceLoop(ctx)
	}
	return n, nil
}

func (n *Node) readClaims(ctx context.Context, sub *pubsub.Subscription) {
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		var c Claim
		if json.Unmarshal(msg.Data, &c) == nil {
			n.claims.add(c)
		}
	}
}

// claimLoop re-gossips our own claim now and then, so peers that missed it
// (or joined after) learn who owns this node.
func (n *Node) claimLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
		if c, ok := n.claims.get(n.host.ID().String()); ok {
			b, _ := json.Marshal(c)
			n.claimTop.Publish(ctx, b)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Minute):
		}
	}
}

// Link records a claim of this node by owner, given the owner's signature
// over the claim message, countersigns it, and gossips it.
func (n *Node) Link(owner, name string, t int64, ownerSig string) (*Claim, error) {
	c, err := newClaim(n.host.ID(), owner, name)
	if err != nil {
		return nil, err
	}
	c.Time, c.OwnerSig = t, ownerSig
	if err := signClaimAsNode(c, n.nodeKey); err != nil {
		return nil, err
	}
	if !n.claims.add(*c) {
		return nil, fmt.Errorf("claim rejected (invalid, or older than the current one)")
	}
	b, _ := json.Marshal(c)
	n.claimTop.Publish(context.Background(), b)
	return c, nil
}

// relayResources: no per-circuit time or byte limit (inference streams are
// long), but bounded slots so a public bootstrap is not an open-ended relay.
// The default cap of 8 reservations per source IP is the only sybil guard on a
// public-IP relay, but behind a tunnel every peer arrives from 127.0.0.1, so
// there it would let only 8 NATed nodes in the whole network be reachable.
func relayResources(behindProxy bool) relay.Resources {
	rc := relay.DefaultResources()
	rc.Limit = nil
	rc.MaxReservations = 1024     // NATed peers that can be reached through us
	rc.MaxReservationsPerPeer = 2 // default 1; 2 tolerates a reconnect overlap
	rc.MaxCircuits = 64           // open relayed connections per peer
	if behindProxy {
		rc.MaxReservationsPerIP = rc.MaxReservations
	}
	return rc
}

// syncLoop re-syncs receipts, claims and invites from the bootstrap nodes
// now and then, so anything a node missed on gossip arrives eventually.
func (n *Node) syncLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Minute):
		}
		for _, ai := range n.relays {
			if n.host.Network().Connectedness(ai.ID) == network.Connected {
				n.syncReceipts(ctx, ai.ID)
			}
		}
	}
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
		b, _ := json.Marshal(Announce{Version: Version, Models: models, Free: max(free, 0), Max: n.cfg.Concurrency})
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
		if _, ok := n.peers[from]; !ok && len(n.peers) >= maxPeers {
			oldest, at := peer.ID(""), time.Now()
			for p, pi := range n.peers {
				if pi.seen.Before(at) {
					oldest, at = p, pi.seen
				}
			}
			delete(n.peers, oldest)
		}
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
