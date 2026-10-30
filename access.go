package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Quota counts credits served per account (or per peer, for nodes with no
// account) during the current UTC day, on this server only.
type Quota struct {
	mu   sync.Mutex
	path string
	Day  int64            `json:"day"`
	Used map[string]int64 `json:"used"`
}

func utcDay(t time.Time) int64 { return t.Unix() / 86400 }

func OpenQuota(path string) *Quota {
	q := &Quota{path: path, Used: map[string]int64{}}
	if b, err := os.ReadFile(path); err == nil {
		json.Unmarshal(b, q)
	}
	if q.Used == nil {
		q.Used = map[string]int64{}
	}
	return q
}

func (q *Quota) roll(now time.Time) {
	if d := utcDay(now); d != q.Day {
		q.Day, q.Used = d, map[string]int64{}
	}
}

func (q *Quota) used(key string, now time.Time) int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.roll(now)
	return q.Used[key]
}

func (q *Quota) add(key string, credits int64, now time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.roll(now)
	q.Used[key] += credits
	b, _ := json.Marshal(q)
	tmp := q.path + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		os.Rename(tmp, q.path)
	}
}

// memberSet returns the current member set, recomputed at most every 10 s.
func (n *Node) memberSet() *MemberSet {
	n.accessMu.Lock()
	defer n.accessMu.Unlock()
	if n.access != nil && time.Since(n.accessAt) < 10*time.Second {
		return n.access
	}
	n.access, n.accessAt = n.members.Compute(), time.Now()
	return n.access
}

// invalidateMembers drops the cached member set after a change.
func (n *Node) invalidateMembers() {
	n.accessMu.Lock()
	n.access = nil
	n.accessMu.Unlock()
	n.servedMu.Lock()
	n.servedEver = nil
	n.servedMu.Unlock()
}

// servedSets are the accounts owning a node that has a receipt as server:
// ever (10 invites) and within the contributor window (no quota).
func (n *Node) servedSets() (ever, recent map[string]bool) {
	n.servedMu.Lock()
	defer n.servedMu.Unlock()
	if n.servedEver != nil && time.Since(n.servedAt) < 10*time.Second {
		return n.servedEver, n.servedRecent
	}
	since := time.Now().Add(-contributorWindow).Unix()
	n.receipts.mu.Lock()
	servers := map[string]bool{} // node -> served recently
	for _, sr := range n.receipts.all {
		r := sr.Receipt
		servers[r.Server] = servers[r.Server] || r.TS >= since
	}
	n.receipts.mu.Unlock()
	ever, recent = map[string]bool{}, map[string]bool{}
	for s, rec := range servers {
		if c, ok := n.claims.get(s); ok {
			ever[c.Owner] = true
			recent[c.Owner] = recent[c.Owner] || rec
		}
	}
	n.servedEver, n.servedRecent, n.servedAt = ever, recent, time.Now()
	return ever, recent
}

// accountOf is the account that owns node p, or "".
func (n *Node) accountOf(p peer.ID) string {
	if c, ok := n.claims.get(p.String()); ok {
		return c.Owner
	}
	return ""
}

// The three access rules, as the server applies them to a request from remote.
const (
	msgBusy       = "busy"
	msgUninvited  = "busy: this server serves uninvited nodes only when it is idle. Ask a member for an invite (bitllm invite), then link this node to your account"
	msgQuotaSpent = "daily free quota on this server used up (its owner set one). Another server may still answer; sharing a model removes the limit, or try again after 00:00 UTC"
)

// ticket is a taken serving slot and what the job will be charged to.
type ticket struct {
	key  string // daily quota key, "" when not counted (contributors, idle-only uninvited)
	open bool   // an uninvited node served because the server is --open
}

const openPool = "open-pool" // quota key shared by all uninvited nodes on an open server

// openSlots is how many slots uninvited nodes may hold at once on an open
// server, so members and contributors always find room.
func (n *Node) openSlots() int32 {
	if n.cfg.OpenSlots > 0 {
		return int32(min(n.cfg.OpenSlots, n.cfg.Concurrency))
	}
	return int32(max(1, n.cfg.Concurrency/2))
}

// admit takes a serving slot for remote, or says why not. The caller must
// call done with the ticket when the job ends.
func (n *Node) admit(remote peer.ID, now time.Time) (status int, msg string, t ticket) {
	account := n.accountOf(remote)
	member, contributor := true, false
	if n.members.Enabled() {
		ms := n.memberSet()
		member = account != "" && ms.Member[account]
		contributor = member && ms.Contributors[account]
	}
	// An open server serves uninvited nodes on spare slots, while the shared
	// pool for all of them lasts. Peer ids are free, so the per-node quota
	// alone would not bound it.
	open := !member && n.cfg.Open && (n.cfg.OpenDaily <= 0 || n.quota.used(openPool, now) < n.cfg.OpenDaily)
	if !member && !open {
		if n.inflight.Add(1) > 1 { // idle means nothing else is running
			n.inflight.Add(-1)
			return 429, msgUninvited, t
		}
		return 0, "", t
	}
	key := account
	if !member || key == "" { // uninvited on an open server, or membership off without an account
		key = "node:" + remote.String()
	}
	// Checked before the job and charged after, so parallel jobs started just
	// under the quota can overshoot it by at most concurrency x max-tokens.
	if !contributor && n.cfg.FreeDaily > 0 && n.quota.used(key, now) >= n.cfg.FreeDaily {
		return 429, msgQuotaSpent, t
	}
	if open {
		if n.openInflight.Add(1) > n.openSlots() {
			n.openInflight.Add(-1)
			return 429, msgUninvited, t
		}
	}
	if n.inflight.Add(1) > int32(n.cfg.Concurrency) {
		n.inflight.Add(-1)
		if open {
			n.openInflight.Add(-1)
		}
		return 429, msgBusy, t
	}
	if !contributor {
		t.key = key
	}
	t.open = open
	return 0, "", t
}

// done releases a ticket's slot and charges the job's credits.
func (n *Node) done(t ticket, credits int64, now time.Time) {
	n.inflight.Add(-1)
	if t.open {
		n.openInflight.Add(-1)
		n.quota.add(openPool, credits, now)
	}
	if t.key != "" {
		n.quota.add(t.key, credits, now)
	}
}

// isBusy reports whether a refusal is temporary, worth waiting out.
func isBusy(status int, msg string) bool {
	return status == 429 && strings.HasPrefix(msg, msgBusy)
}

func (n *Node) readMembers(ctx context.Context, sub *pubsub.Subscription) {
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		var it memberItem
		if json.Unmarshal(msg.Data, &it) == nil && n.members.add(it) {
			n.invalidateMembers()
		}
	}
}

// publishMember stores an invite or revocation and gossips it.
func (n *Node) publishMember(it memberItem) bool {
	if !n.members.add(it) {
		return false
	}
	n.invalidateMembers()
	b, _ := json.Marshal(it)
	n.memTop.Publish(context.Background(), b)
	// Gossip sent before the mesh has formed is lost; say it once more.
	time.AfterFunc(5*time.Second, func() { n.memTop.Publish(context.Background(), b) })
	return true
}

// MemberInfo is what the dashboard shows about an account.
type MemberInfo struct {
	Enabled      bool   `json:"enabled"`
	Account      string `json:"account,omitempty"`
	Member       bool   `json:"member"`
	Root         bool   `json:"root"`
	Contributor  bool   `json:"contributor"`
	InvitedBy    string `json:"invited_by,omitempty"`
	InvitesUsed  int    `json:"invites_used"`
	InvitesLimit int    `json:"invites_limit"` // -1: unlimited
	Members      int    `json:"members"`
}

func (n *Node) memberInfo(account string) MemberInfo {
	ms := n.memberSet()
	info := MemberInfo{Enabled: n.members.Enabled(), Account: account, Members: len(ms.Member)}
	if account == "" {
		return info
	}
	info.Member = ms.Member[account]
	info.Root = ms.roots[account]
	info.Contributor = ms.Contributors[account]
	if inv := ms.InvitedBy[account]; inv != "" {
		info.InvitedBy = checksumAddress(inv)
	}
	info.InvitesUsed = ms.Used[account]
	info.InvitesLimit = ms.InviteLimit(account)
	return info
}

func (n *Node) membersMe(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(n.memberInfo(n.accountOf(n.host.ID())))
}

// membersPost accepts a signed invite or revocation (from the CLI) and gossips it.
func (n *Node) membersPost(w http.ResponseWriter, r *http.Request) {
	var it memberItem
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&it); err != nil {
		jsonErr(w, 400, "bad json")
		return
	}
	if err := sigShapeErr(sigOf(it)); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	if err := n.members.check(it); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	if !n.publishMember(it) {
		jsonErr(w, 400, "already known")
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

func sigOf(it memberItem) string {
	switch {
	case it.Invite != nil:
		return it.Invite.Sig
	case it.Revoke != nil:
		return it.Revoke.Sig
	}
	return ""
}

// membersList shows every member with who invited it and how much its nodes
// served lately, so a root can spot a receipt farm and revoke it.
func (n *Node) membersList(w http.ResponseWriter, r *http.Request) {
	ms := n.memberSet()
	since := time.Now().Add(-contributorWindow).Unix()
	jobs := map[string]int{}
	n.receipts.mu.Lock()
	for _, sr := range n.receipts.all {
		if sr.Receipt.TS >= since {
			jobs[sr.Receipt.Server]++
		}
	}
	n.receipts.mu.Unlock()
	perAccount := map[string]int{}
	for node, j := range jobs {
		if c, ok := n.claims.get(node); ok {
			perAccount[c.Owner] += j
		}
	}
	type row struct {
		Account     string `json:"account"`
		InvitedBy   string `json:"invited_by,omitempty"`
		Root        bool   `json:"root,omitempty"`
		Sharer      bool   `json:"sharer"`
		Contributor bool   `json:"contributor"`
		Invited     int    `json:"invited"`
		Jobs7d      int    `json:"jobs_7d"`
	}
	out := []row{}
	for a := range ms.Member {
		rw := row{Account: checksumAddress(a), Root: ms.roots[a], Sharer: ms.Sharers[a],
			Contributor: ms.Contributors[a], Invited: ms.Used[a], Jobs7d: perAccount[a]}
		if inv := ms.InvitedBy[a]; inv != "" {
			rw.InvitedBy = checksumAddress(inv)
		}
		out = append(out, rw)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Jobs7d > out[j].Jobs7d })
	json.NewEncoder(w).Encode(out)
}
