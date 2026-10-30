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

// Valid checks both signatures.
func (sr *SignedReceipt) Valid() bool {
	b := sr.Receipt.bytes()
	return verifySig(sr.Receipt.Client, b, sr.ClientSig) && verifySig(sr.Receipt.Server, b, sr.ServerSig)
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
	Clients int   `json:"clients"`
}

// Receipts is every valid receipt this node has seen, appended to a file.
type Receipts struct {
	mu      sync.Mutex
	f       *os.File
	seen    map[string]bool
	all     []SignedReceipt
	pending map[string]*pendingJob
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

func (rs *Receipts) Tally() map[string]ServerCredits {
	out, _ := rs.tally()
	return out
}

// tally sums credits per server and also returns each server's set of clients.
func (rs *Receipts) tally() (map[string]ServerCredits, map[string]map[string]bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := map[string]ServerCredits{}
	clients := map[string]map[string]bool{}
	for _, sr := range rs.all {
		r := sr.Receipt
		c := out[r.Server]
		c.Credits += r.Credits
		c.Jobs++
		if clients[r.Server] == nil {
			clients[r.Server] = map[string]bool{}
		}
		clients[r.Server][r.Client] = true
		c.Clients = len(clients[r.Server])
		out[r.Server] = c
	}
	return out, clients
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
	case r.Server != rs.self.String() || r.Client != from.String() || p.client != from:
		return fmt.Errorf("wrong parties")
	case !verifySig(r.Client, r.bytes(), sr.ClientSig):
		return fmt.Errorf("bad client signature")
	case r.Model != p.model || r.Output != p.m.sum() || r.Credits != p.m.credits() ||
		r.Prompt != p.m.prompt || r.Completion != p.m.completion:
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

// sendReceipt signs a receipt for a job p served us and gives it to p.
func (n *Node) sendReceipt(p peer.ID, job, model string, m meter) error {
	r := Receipt{Job: job, Model: model, Client: n.host.ID().String(), Server: p.String(),
		Prompt: m.prompt, Completion: m.completion, Credits: m.credits(), Output: m.sum(), TS: time.Now().Unix()}
	sig, err := n.receipts.key.Sign(r.bytes())
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
	perNode, clients := n.receipts.tally()
	return ownerTable(perNode, clients, n.claims)
}
