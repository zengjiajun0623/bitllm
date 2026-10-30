package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Credits for serving are proven by receipts. After each job the client signs
// what it received (token counts and a hash of the exact response bytes), the
// server checks that against what it sent and countersigns, then gossips the
// receipt. Any node can verify both signatures and tally credits per server,
// so the record is public and checkable without a chain. Which receipts will
// count (self-dealing, unverified models) is decided later, from these facts.
//
// The client signs with a fresh key for every job, not its node key, so the
// public record says which server did how much work but not who asked for it.
const (
	ReceiptProto = "/bitllm/receipt/1.0.0"
	SyncProto    = "/bitllm/receipts-sync/1.0.0"
	ReceiptTopic = "bitllm/receipts/1"
)

type Receipt struct {
	Job        string `json:"job"`
	Model      string `json:"model"`
	Client     string `json:"client"`
	Server     string `json:"server"`
	Prompt     int64  `json:"prompt_tokens"`
	Completion int64  `json:"completion_tokens"`
	Credits    int64  `json:"credits"`
	Output     string `json:"output_sha256"`
	TS         int64  `json:"ts"`
}

type SignedReceipt struct {
	Receipt   Receipt `json:"receipt"`
	ClientSig []byte  `json:"client_sig"`
	ServerSig []byte  `json:"server_sig,omitempty"`
}

func (r Receipt) bytes() []byte { b, _ := json.Marshal(r); return b }

func verifySig(id string, data, sig []byte) bool {
	p, err := peer.Decode(id)
	if err != nil {
		return false
	}
	pk, err := p.ExtractPublicKey()
	if err != nil {
		return false
	}
	ok, err := pk.Verify(data, sig)
	return err == nil && ok
}

// maxJobTokens bounds the token counts of one receipt, so a pair of colluding
// keys cannot mint arbitrary credits in a single signed line.
const maxJobTokens = 4 << 20

// price is the credits for a job: prefill is cheap relative to decode.
func price(prompt, completion int64) int64 { return prompt/4 + completion }

// Valid checks both signatures and that the credits follow from the token counts.
func (sr *SignedReceipt) Valid() bool {
	r := sr.Receipt
	if r.Prompt < 0 || r.Completion < 0 || r.Prompt > maxJobTokens || r.Completion > maxJobTokens ||
		r.Credits != price(r.Prompt, r.Completion) || len(r.Job) > 64 || len(r.Model) > 128 {
		return false
	}
	b := r.bytes()
	return verifySig(r.Client, b, sr.ClientSig) && verifySig(r.Server, b, sr.ServerSig)
}

// pendingJob is what a server remembers about a job until the client's receipt arrives.
type pendingJob struct {
	client  peer.ID
	model   string
	m       meter
	expires time.Time
}

type ServerCredits struct {
	Credits int64 `json:"credits"`
	Jobs    int   `json:"jobs"`
}

// Receipts is every valid receipt this node has seen, appended to a file.
type Receipts struct {
	mu      sync.Mutex
	f       *os.File
	seen    map[string]bool
	all     []SignedReceipt
	pending map[string]*pendingJob
	rate    gossipRate
	topic   *pubsub.Topic
	key     crypto.PrivKey
	self    peer.ID
}

func OpenReceipts(path string, key crypto.PrivKey, self peer.ID) (*Receipts, error) {
	rs := &Receipts{seen: map[string]bool{}, pending: map[string]*pendingJob{}, key: key, self: self}
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(nil, 1<<20)
		for sc.Scan() {
			var sr SignedReceipt
			if json.Unmarshal(sc.Bytes(), &sr) == nil && sr.Valid() && !rs.seen[sr.Receipt.Job] {
				rs.seen[sr.Receipt.Job] = true
				rs.all = append(rs.all, sr)
			}
		}
		f.Close()
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	rs.f = f
	return rs, err
}

// add stores a receipt if it is valid and new; it reports whether it was.
func (rs *Receipts) add(sr SignedReceipt) bool {
	if !sr.Valid() {
		return false
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.seen[sr.Receipt.Job] {
		return false
	}
	rs.seen[sr.Receipt.Job] = true
	rs.all = append(rs.all, sr)
	b, _ := json.Marshal(sr)
	rs.f.Write(append(b, '\n'))
	return true
}

// Tally sums credits per server.
func (rs *Receipts) Tally() map[string]ServerCredits {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := map[string]ServerCredits{}
	for _, sr := range rs.all {
		r := sr.Receipt
		c := out[r.Server]
		c.Credits += r.Credits
		c.Jobs++
		out[r.Server] = c
	}
	return out
}

// Gossip limits. Valid receipts cost nothing to forge between two keys you
// own, so without limits anyone could fill every node's memory and disk.
// A receipt must be published by its own server, be recent, and fit under a
// per-server and a network-wide rate.
const (
	receiptMaxAge    = time.Hour
	receiptPerServer = 120 // per minute
	receiptPerMinute = 600 // per minute, all servers together
)

type gossipRate struct {
	mu     sync.Mutex
	minute int64
	total  int
	per    map[string]int
}

func (g *gossipRate) allow(server string, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if m := now.Unix() / 60; m != g.minute {
		g.minute, g.total, g.per = m, 0, map[string]int{}
	}
	if g.total >= receiptPerMinute || g.per[server] >= receiptPerServer {
		return false
	}
	g.total++
	g.per[server]++
	return true
}

// acceptGossip is the topic validator for receipts.
func (rs *Receipts) acceptGossip(from peer.ID, sr *SignedReceipt) bool {
	r, now := sr.Receipt, time.Now()
	if from.String() != r.Server || r.TS < now.Add(-receiptMaxAge).Unix() || r.TS > now.Add(10*time.Minute).Unix() {
		return false
	}
	return sr.Valid() && rs.rate.allow(r.Server, now)
}

// served is called by the server when a job finishes.
func (rs *Receipts) served(job string, client peer.ID, model string, m meter) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	now := time.Now()
	for id, p := range rs.pending {
		if now.After(p.expires) {
			delete(rs.pending, id)
		}
	}
	rs.pending[job] = &pendingJob{client, model, m, now.Add(10 * time.Minute)}
}

// countersign checks a client's receipt against the job we served and signs it.
func (rs *Receipts) countersign(sr *SignedReceipt, from peer.ID) error {
	r := sr.Receipt
	rs.mu.Lock()
	p := rs.pending[r.Job]
	delete(rs.pending, r.Job)
	rs.mu.Unlock()
	switch {
	case p == nil:
		return fmt.Errorf("unknown job")
	case r.Server != rs.self.String() || p.client != from:
		return fmt.Errorf("wrong parties")
	case !verifySig(r.Client, r.bytes(), sr.ClientSig):
		return fmt.Errorf("bad client signature")
	}
	if prompt, completion := p.m.tokens(); r.Model != p.model || r.Output != p.m.sum() ||
		r.Prompt != prompt || r.Completion != completion || r.Credits != p.m.credits() {
		return fmt.Errorf("receipt does not match the job served")
	}
	sig, err := rs.key.Sign(r.bytes())
	sr.ServerSig = sig
	return err
}

// handleReceipt: a client hands us its signed receipt for a job we served.
func (n *Node) handleReceipt(s network.Stream) {
	defer s.Close()
	var sr SignedReceipt
	if err := json.NewDecoder(io.LimitReader(s, 64<<10)).Decode(&sr); err != nil {
		return
	}
	if err := n.receipts.countersign(&sr, s.Conn().RemotePeer()); err != nil {
		log.Printf("receipt from %s rejected: %v", s.Conn().RemotePeer(), err)
		fmt.Fprintf(s, "%s\n", err)
		return
	}
	n.receipts.add(sr)
	b, _ := json.Marshal(sr)
	n.receipts.topic.Publish(context.Background(), b)
	s.Write([]byte("ok\n"))
}

// sendReceipt signs a receipt for a job p served us and gives it to p. It
// signs with a one-off key, so the gossiped receipt does not name this node.
func (n *Node) sendReceipt(p peer.ID, job, model string, m meter) error {
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		return err
	}
	client, _ := peer.IDFromPrivateKey(key)
	prompt, completion := m.tokens()
	r := Receipt{Job: job, Model: model, Client: client.String(), Server: p.String(),
		Prompt: prompt, Completion: completion, Credits: m.credits(), Output: m.sum(), TS: time.Now().Unix()}
	sig, err := key.Sign(r.bytes())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := n.host.NewStream(network.WithAllowLimitedConn(ctx, "receipt"), p, ReceiptProto)
	if err != nil {
		return err
	}
	defer s.Close()
	if err := json.NewEncoder(s).Encode(SignedReceipt{Receipt: r, ClientSig: sig}); err != nil {
		return err
	}
	s.CloseWrite()
	line, _ := bufio.NewReader(s).ReadString('\n')
	if line != "ok\n" {
		return fmt.Errorf("server refused receipt: %q", line)
	}
	return nil
}

// syncItem is one line of the sync stream: a receipt, or a node claim.
type syncItem struct {
	SignedReceipt
	Claim *Claim `json:"claim,omitempty"`
}

// handleSync streams every receipt and claim we know, so a new node can catch up.
func (n *Node) handleSync(s network.Stream) {
	defer s.Close()
	n.receipts.mu.Lock()
	all := append([]SignedReceipt(nil), n.receipts.all...)
	n.receipts.mu.Unlock()
	enc := json.NewEncoder(s)
	for _, sr := range all {
		if enc.Encode(sr) != nil {
			return
		}
	}
	for _, c := range n.claims.all() {
		if enc.Encode(map[string]any{"claim": c}) != nil {
			return
		}
	}
}

func (n *Node) syncReceipts(ctx context.Context, p peer.ID) (int, error) {
	s, err := n.host.NewStream(network.WithAllowLimitedConn(ctx, "sync"), p, SyncProto)
	if err != nil {
		return 0, err
	}
	defer s.Close()
	s.CloseWrite()
	dec, added := json.NewDecoder(s), 0
	for {
		var it syncItem
		if err := dec.Decode(&it); err != nil {
			if err == io.EOF {
				err = nil
			}
			return added, err
		}
		if it.Claim != nil {
			n.claims.add(*it.Claim)
		} else if n.receipts.add(it.SignedReceipt) {
			added++
		}
	}
}

func (n *Node) readReceipts(ctx context.Context, sub *pubsub.Subscription) {
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		var sr SignedReceipt
		if json.Unmarshal(msg.Data, &sr) == nil {
			n.receipts.add(sr)
		}
	}
}

// creditsTable is the tally grouped by owner, for the gateway.
func (n *Node) creditsTable() []OwnerCredits {
	return ownerTable(n.receipts.Tally(), n.claims)
}
