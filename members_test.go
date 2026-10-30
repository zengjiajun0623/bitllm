package main

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type ethKey struct {
	k    *secp256k1.PrivateKey
	addr string
}

func newEthKey(t *testing.T) ethKey {
	k, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	return ethKey{k, ethAddress(k.PubKey())}
}

func (e ethKey) invite(to string, at int64) memberItem {
	i := &Invite{Inviter: e.addr, Invitee: to, Time: at}
	i.Sig = hex.EncodeToString(ethSign(e.k, i.Message()))
	return memberItem{Invite: i}
}

func (e ethKey) revoke(acct string, at int64) memberItem {
	r := &Revoke{Account: acct, Root: e.addr, Time: at}
	r.Sig = hex.EncodeToString(ethSign(e.k, r.Message()))
	return memberItem{Revoke: r}
}

func TestMemberSet(t *testing.T) {
	root, a, b, c := newEthKey(t), newEthKey(t), newEthKey(t), newEthKey(t)
	ever, recent := map[string]bool{}, map[string]bool{}
	served := func() (map[string]bool, map[string]bool) { return ever, recent }
	m := OpenMembers(t.TempDir()+"/m.json", []string{root.addr}, served)
	add := func(it memberItem) {
		t.Helper()
		if err := m.check(it); err != nil || !m.add(it) {
			t.Fatalf("valid item refused: %v", err)
		}
	}
	add(root.invite(a.addr, 100))
	add(a.invite(b.addr, 200))
	if m.add(c.invite(a.addr, 50)) || m.add(c.invite(newEthKey(t).addr, 300)) {
		t.Fatal("stored an invite from a non-member")
	}
	ms := m.Compute()
	if !ms.Member[a.addr] || !ms.Member[b.addr] || ms.Member[c.addr] || ms.InvitedBy[b.addr] != a.addr {
		t.Fatalf("chain: %+v", ms)
	}

	// A member stores 3 invites; once it has served, 10.
	var extra []string
	tryInvites := func(from ethKey, n int, at int64) (stored int) {
		for i := range n {
			x := newEthKey(t).addr
			extra = append(extra, x)
			if m.add(from.invite(x, at+int64(i))) {
				stored++
			}
		}
		return
	}
	if got := tryInvites(b, 5, 300); got != memberInvites {
		t.Fatalf("member stored %d invites, want %d", got, memberInvites)
	}
	ever[b.addr] = true
	if got := tryInvites(b, 10, 400); got != contributorInvites-memberInvites {
		t.Fatalf("sharer stored %d more, want %d", got, contributorInvites-memberInvites)
	}
	members := func() (n int) {
		ms := m.Compute()
		for _, x := range extra {
			if ms.Member[x] {
				n++
			}
		}
		return
	}
	// Not serving lately (recent is empty) does not take invites back.
	if got := members(); got != contributorInvites {
		t.Fatalf("%d of b's invitees are members, want %d", got, contributorInvites)
	}
	if got := tryInvites(root, 12, 500); got != 12 {
		t.Fatalf("root stored %d of 12", got)
	}

	// Revoking a removes a, b (invited by a) and everyone b invited.
	d := newEthKey(t)
	add(a.invite(d.addr, 600))
	add(root.revoke(a.addr, 700))
	ms = m.Compute()
	if ms.Member[a.addr] || ms.Member[b.addr] || ms.Member[d.addr] {
		t.Fatal("revoke did not cascade")
	}
	for _, x := range extra[:15] { // b's invitees
		if ms.Member[x] {
			t.Fatal("revoke left a grandchild")
		}
	}
	// A revoked account cannot invite, not even backdated.
	if m.add(a.invite(newEthKey(t).addr, 150)) {
		t.Fatal("stored a backdated invite from a revoked account")
	}
	// Someone else can re-invite a person caught under a.
	add(root.invite(d.addr, 800))
	if !m.Compute().Member[d.addr] {
		t.Fatal("re-invite after revoke failed")
	}
	if m.add(b.revoke(root.addr, 900)) || m.add(root.revoke(root.addr, 900)) {
		t.Fatal("bad revoke accepted")
	}
	bad := root.invite(c.addr, 1000)
	bad.Invite.Inviter = d.addr
	if m.add(bad) {
		t.Fatal("forged invite accepted")
	}
	// A reload replays to the same state.
	before := m.Compute()
	again := OpenMembers(m.path, []string{root.addr}, served).Compute()
	if len(again.Member) != len(before.Member) || again.Member[a.addr] || !again.Member[d.addr] {
		t.Fatalf("reload: %d vs %d members", len(again.Member), len(before.Member))
	}
}

// TestInviteSpamCannotBlock: invites signed by outsiders are not stored, so
// they cannot fill the store and keep real invites out.
func TestInviteSpamCannotBlock(t *testing.T) {
	root := newEthKey(t)
	m := OpenMembers(t.TempDir()+"/m.json", []string{root.addr}, nil)
	for i := range 1000 {
		m.add(newEthKey(t).invite(newEthKey(t).addr, int64(i)))
	}
	if n := len(m.items()); n != 0 {
		t.Fatalf("stored %d junk invites", n)
	}
	guest := newEthKey(t)
	if !m.add(root.invite(guest.addr, 2000)) || !m.Compute().Member[guest.addr] {
		t.Fatal("real invite refused")
	}
}

// claimNode makes a node key owned by account and records the claim on n.
func claimNode(t *testing.T, n *Node, owner ethKey) (peer.ID, crypto.PrivKey) {
	nk, _, _ := crypto.GenerateEd25519Key(nil)
	id, _ := peer.IDFromPrivateKey(nk)
	c, err := newClaim(id, owner.addr, "")
	if err != nil {
		t.Fatal(err)
	}
	c.OwnerSig = hex.EncodeToString(ethSign(owner.k, c.Message()))
	if err := signClaimAsNode(c, nk); err != nil {
		t.Fatal(err)
	}
	if !n.claims.add(*c) {
		t.Fatal("claim refused")
	}
	return id, nk
}

func TestAccessRules(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root, member, contrib := newEthKey(t), newEthKey(t), newEthKey(t)
	n := testNode(t, ctx, Config{Roots: []string{root.addr}, FreeDaily: 30, Concurrency: 2})
	if !n.members.add(root.invite(member.addr, 1)) || !n.members.add(root.invite(contrib.addr, 2)) {
		t.Fatal("invites refused")
	}
	memberNode, _ := claimNode(t, n, member)
	contribNode, contribKey := claimNode(t, n, contrib)
	stranger := randomPeer(t)
	now := time.Now()

	// Rule 3: a contributor served a job recently (a receipt names its node as server).
	ck, _, _ := crypto.GenerateEd25519Key(nil)
	cid, _ := peer.IDFromPrivateKey(ck)
	r := Receipt{Job: "j1", Model: "m", Client: cid.String(), Server: contribNode.String(),
		Prompt: 4, Completion: 1, Credits: price(4, 1), Output: "x", TS: now.Unix()}
	sr := SignedReceipt{Receipt: r}
	sr.ClientSig, _ = ck.Sign(r.bytes())
	sr.ServerSig, _ = contribKey.Sign(r.bytes())
	if !n.receipts.add(sr) {
		t.Fatal("receipt refused")
	}
	n.invalidateMembers()

	release := func() { n.inflight.Add(-1) }

	// Rule 2: a member is served until the daily quota, then refused.
	s, _, tk := n.admit(memberNode, now)
	if s != 0 || tk.key != member.addr {
		t.Fatalf("member refused: %d %+v", s, tk)
	}
	n.done(tk, 30, now) // uses up the quota
	if s, msg, _ := n.admit(memberNode, now); s != 429 || msg != msgQuotaSpent {
		t.Fatalf("member over quota: %d %s", s, msg)
	}
	if s, _, _ := n.admit(memberNode, now.Add(24*time.Hour)); s != 0 {
		t.Fatal("quota did not reset the next UTC day")
	}
	release()

	// Rule 3: a contributor has no quota.
	n.quota.add(contrib.addr, 1_000_000, now)
	s, _, tk = n.admit(contribNode, now)
	if s != 0 || tk.key != "" {
		t.Fatalf("contributor: %d %+v", s, tk)
	}
	release()

	// Rule 1: uninvited only when idle.
	if s, _, _ := n.admit(stranger, now); s != 0 {
		t.Fatal("uninvited refused on an idle server")
	}
	if s, msg, _ := n.admit(stranger, now); s != 429 || !isBusy(s, msg) {
		t.Fatalf("uninvited served on a busy server: %d %s", s, msg)
	}
	release()
	// A member still gets the second slot while the stranger holds one.
	if s, _, _ := n.admit(stranger, now); s != 0 {
		t.Fatal("idle again")
	}
	if s, _, _ := n.admit(contribNode, now); s != 0 {
		t.Fatal("contributor refused a free slot")
	}
	release()
	release()
}

// TestInviteGossipAndSync: an invite made on one node reaches the bootstrap
// over gossip and a node that joins later through sync.
func TestInviteGossipAndSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root, guest := newEthKey(t), newEthKey(t)
	cfg := func() Config { return Config{Roots: []string{root.addr}} }
	boot := testNode(t, ctx, cfg())
	bs := boot.Addrs()[:1]
	c := cfg()
	c.Bootstrap = bs
	a := testNode(t, ctx, c)
	for deadline := time.Now().Add(15 * time.Second); ; {
		if a.publishMember(root.invite(guest.addr, time.Now().Unix())) || a.members.Compute().Member[guest.addr] {
			if boot.members.Compute().Member[guest.addr] {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("invite never reached the bootstrap")
		}
		time.Sleep(200 * time.Millisecond)
	}
	late := cfg()
	late.Bootstrap = bs
	if !testNode(t, ctx, late).members.Compute().Member[guest.addr] {
		t.Fatal("late joiner did not sync the invite")
	}
}

// TestOpenServer: --open serves uninvited nodes on spare slots (at most
// OpenSlots of them, so members always find room), with a per-node quota and
// a shared daily pool for all uninvited nodes together.
func TestOpenServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root, member := newEthKey(t), newEthKey(t)
	n := testNode(t, ctx, Config{Roots: []string{root.addr}, Open: true, OpenSlots: 2, OpenDaily: 100,
		FreeDaily: 30, Concurrency: 3})
	n.members.add(root.invite(member.addr, 1))
	memberNode, _ := claimNode(t, n, member)
	a, b, c := randomPeer(t), randomPeer(t), randomPeer(t)
	now := time.Now()

	s1, _, ta := n.admit(a, now)
	s2, _, tb := n.admit(b, now)
	if s1 != 0 || s2 != 0 || ta.key != "node:"+a.String() || !ta.open {
		t.Fatalf("open server refused uninvited on spare slots: %d %d %+v", s1, s2, ta)
	}
	// The third slot is kept for members.
	if s, msg, _ := n.admit(c, now); s != 429 || msg != msgUninvited {
		t.Fatalf("uninvited took a reserved slot: %d %s", s, msg)
	}
	s, _, tm := n.admit(memberNode, now)
	if s != 0 || tm.open {
		t.Fatalf("member refused the reserved slot: %d", s)
	}
	n.done(tm, 0, now)
	n.done(tb, 0, now)

	// Per-node quota: a has used 30, b has not.
	n.done(ta, 30, now)
	if s, msg, _ := n.admit(a, now); s != 429 || msg != msgQuotaSpent {
		t.Fatalf("per-node quota not applied: %d %s", s, msg)
	}
	s, _, tb = n.admit(b, now)
	if s != 0 {
		t.Fatal("one node's quota limited another")
	}
	// The shared pool (100) runs out after 30 + 70 = 100: fresh ids get idle-only.
	n.done(tb, 70, now)
	s, _, tc := n.admit(c, now) // idle server: served, but not as open
	if s != 0 || tc.open {
		t.Fatalf("pool spent: want idle-only service, got %d %+v", s, tc)
	}
	if s, msg, _ := n.admit(randomPeer(t), now); s != 429 || msg != msgUninvited {
		t.Fatalf("pool spent and busy: want refusal, got %d %s", s, msg)
	}
	n.done(tc, 0, now)
	if n.inflight.Load() != 0 || n.openInflight.Load() != 0 {
		t.Fatalf("slots leaked: %d %d", n.inflight.Load(), n.openInflight.Load())
	}
}
