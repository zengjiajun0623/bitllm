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
	m := OpenMembers(t.TempDir()+"/m.json", []string{root.addr})
	add := func(it memberItem) {
		t.Helper()
		if !m.add(it) {
			t.Fatal("valid item refused")
		}
	}
	add(root.invite(a.addr, 100))
	add(a.invite(b.addr, 200))
	add(c.invite(a.addr, 50)) // c is not a member: ignored
	ms := m.Compute(nil)
	if !ms.Member[a.addr] || !ms.Member[b.addr] || ms.Member[c.addr] || ms.InvitedBy[b.addr] != a.addr {
		t.Fatalf("chain: %+v", ms)
	}

	// A member invites 3, a contributor 10, a root without limit (time order).
	var extra []string
	for i := range 12 {
		x := newEthKey(t).addr
		extra = append(extra, x)
		add(b.invite(x, int64(300+i)))
	}
	count := func(ms *MemberSet) (n int) {
		for _, x := range extra {
			if ms.Member[x] {
				n++
			}
		}
		return
	}
	if got := count(m.Compute(nil)); got != memberInvites {
		t.Fatalf("member invited %d, want %d", got, memberInvites)
	}
	if got := count(m.Compute(map[string]bool{b.addr: true})); got != contributorInvites {
		t.Fatalf("contributor invited %d, want %d", got, contributorInvites)
	}
	if !m.Compute(nil).Member[extra[0]] || m.Compute(nil).Member[extra[3]] {
		t.Fatal("limit not applied in time order")
	}
	for i := range 12 {
		add(root.invite(newEthKey(t).addr, int64(400+i)))
	}
	if ms := m.Compute(nil); ms.InviteLimit(root.addr) != -1 || ms.Used[root.addr] != 13 {
		t.Fatalf("root limited: used %d", ms.Used[root.addr])
	}

	// Revoking a: a is out, b (invited before) stays, a's later invites do not count.
	d := newEthKey(t)
	add(a.invite(d.addr, 600))
	if !m.Compute(nil).Member[d.addr] {
		t.Fatal("pre-revoke setup")
	}
	add(root.revoke(a.addr, 500))
	ms = m.Compute(nil)
	if ms.Member[a.addr] || !ms.Member[b.addr] || ms.Member[d.addr] {
		t.Fatalf("revoke: a=%v b=%v d=%v", ms.Member[a.addr], ms.Member[b.addr], ms.Member[d.addr])
	}
	// Only a root can revoke.
	if m.add(b.revoke(root.addr, 700)) {
		t.Fatal("non-root revoke accepted")
	}
	// Forged invite.
	bad := root.invite(c.addr, 800)
	bad.Invite.Inviter = b.addr
	if m.add(bad) {
		t.Fatal("forged invite accepted")
	}
	// State survives a restart.
	if again := OpenMembers(m.path, []string{root.addr}).Compute(nil); again.Member[a.addr] || !again.Member[b.addr] {
		t.Fatal("reload lost state")
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
	n.members.add(root.invite(member.addr, 1))
	n.members.add(root.invite(contrib.addr, 2))
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
	s, _, key := n.admit(memberNode, now)
	if s != 0 || key != member.addr {
		t.Fatalf("member refused: %d %q", s, key)
	}
	release()
	n.quota.add(key, 30, now)
	if s, msg, _ := n.admit(memberNode, now); s != 429 || msg != msgQuotaSpent {
		t.Fatalf("member over quota: %d %s", s, msg)
	}
	if s, _, _ := n.admit(memberNode, now.Add(24*time.Hour)); s != 0 {
		t.Fatal("quota did not reset the next UTC day")
	}
	release()

	// Rule 3: a contributor has no quota.
	n.quota.add(contrib.addr, 1_000_000, now)
	s, _, key = n.admit(contribNode, now)
	if s != 0 || key != "" {
		t.Fatalf("contributor: %d %q", s, key)
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
		if a.publishMember(root.invite(guest.addr, time.Now().Unix())) || a.members.Compute(nil).Member[guest.addr] {
			if boot.members.Compute(nil).Member[guest.addr] {
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
	if !testNode(t, ctx, late).members.Compute(nil).Member[guest.addr] {
		t.Fatal("late joiner did not sync the invite")
	}
}
