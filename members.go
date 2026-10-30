package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Membership is an invite tree of Ethereum accounts, private-tracker style.
// Roots are members; so is anyone validly invited by a member. Every node
// computes the same member set from the same signed invites and revocations,
// so there is no membership server. The rules:
//
//  1. Members are served on any free slot; uninvited nodes only when the
//     server is idle, unless it runs with --open.
//  2. No daily quota by default; an owner can set one (--free-daily).
//  3. Contributors (served a job in the last 7 days) are never limited by a
//     quota, and an account that has ever served gets 10 invites, not 3.

const (
	MembersTopic       = "bitllm/invites/1"
	memberInvites      = 3
	contributorInvites = 10
	contributorWindow  = 7 * 24 * time.Hour
	maxInvites         = 50_000 // stored invites; ids are cheap, so bound the set
)

// Invite says inviter (a member) vouches for invitee.
type Invite struct {
	Inviter string `json:"inviter"`
	Invitee string `json:"invitee"`
	Time    int64  `json:"time"`
	Sig     string `json:"sig"` // personal_sign by inviter of Message()
}

func (i *Invite) Message() string {
	return fmt.Sprintf("BitLLM invite\ninviter: %s\ninvitee: %s\ntime: %d", i.Inviter, i.Invitee, i.Time)
}

func (i *Invite) id() string { return i.Inviter + ">" + i.Invitee }

func validAddr(a string) bool {
	if len(a) != 42 || !strings.HasPrefix(a, "0x") || a != strings.ToLower(a) {
		return false
	}
	_, err := hex.DecodeString(a[2:])
	return err == nil
}

// sigShapeErr explains a signature that is not a plain 65-byte ECDSA one,
// which is what smart-account wallets (ERC-1271/6492) return.
func sigShapeErr(sigHex string) error {
	sig, err := hex.DecodeString(strings.TrimPrefix(sigHex, "0x"))
	if err != nil {
		return fmt.Errorf("signature is not hex")
	}
	if len(sig) != 65 {
		return fmt.Errorf("this wallet returned a smart-account signature (%d bytes, not 65); switch to a standard account in your wallet and try again", len(sig))
	}
	return nil
}

func signedBy(msg, sigHex, addr string) bool {
	sig, err := hex.DecodeString(strings.TrimPrefix(sigHex, "0x"))
	if err != nil {
		return false
	}
	got, err := ethRecover(msg, sig)
	return err == nil && got == addr
}

func (i *Invite) Valid() bool {
	return validAddr(i.Inviter) && validAddr(i.Invitee) && i.Inviter != i.Invitee &&
		i.Time <= time.Now().Add(10*time.Minute).Unix() && signedBy(i.Message(), i.Sig, i.Inviter)
}

// Revoke is a root removing an account. It loses membership and so does
// everyone it invited, down the tree, unless someone else invited them too.
type Revoke struct {
	Account string `json:"account"`
	Root    string `json:"root"`
	Time    int64  `json:"time"`
	Sig     string `json:"sig"` // personal_sign by root of Message()
}

func (r *Revoke) Message() string {
	return fmt.Sprintf("BitLLM revoke\naccount: %s\nroot: %s\ntime: %d", r.Account, r.Root, r.Time)
}

func (r *Revoke) Valid() bool {
	return validAddr(r.Account) && validAddr(r.Root) &&
		r.Time <= time.Now().Add(10*time.Minute).Unix() && signedBy(r.Message(), r.Sig, r.Root)
}

// memberItem is one gossip or sync message on the membership topic.
type memberItem struct {
	Invite *Invite `json:"invite,omitempty"`
	Revoke *Revoke `json:"revoke,omitempty"`
}

// Members stores invites and revocations and derives the member set. It only
// stores (and so only forwards) an invite that counts at the time it arrives:
// from a member with invites left, for an account that is not a member yet.
// Signing keys are free, so storing every validly signed invite would let
// outsiders fill the store; this way it holds at most members x 10 invites.
type Members struct {
	mu      sync.Mutex
	path    string
	roots   map[string]bool
	invites map[string]Invite // by id
	revokes map[string]Revoke // by account
	// served reports which accounts have ever served a job and which did in
	// the contributor window. Set by the node; nil means nobody has.
	served func() (ever, recent map[string]bool)
}

// OpenMembers loads the store. Saved items are replayed through the same
// checks as new ones (revocations first, then invites in time order), so a
// store written by an older or buggy version cannot carry junk forward.
func OpenMembers(path string, roots []string, served func() (ever, recent map[string]bool)) *Members {
	m := &Members{path: path, roots: map[string]bool{}, invites: map[string]Invite{}, revokes: map[string]Revoke{}, served: served}
	for _, r := range roots {
		m.roots[strings.ToLower(r)] = true
	}
	if b, err := os.ReadFile(path); err == nil {
		var items []memberItem
		if json.Unmarshal(b, &items) == nil {
			sortItems(items)
			for _, it := range items {
				m.add(it)
			}
		}
	}
	return m
}

// sortItems orders revocations first, then invites by time, which is the order
// in which they can be checked (an inviter's own invite before the ones it signs).
func sortItems(items []memberItem) {
	key := func(it memberItem) (int, int64, string) {
		if it.Revoke != nil {
			return 0, it.Revoke.Time, it.Revoke.Account
		}
		return 1, it.Invite.Time, it.Invite.id()
	}
	sort.SliceStable(items, func(a, b int) bool {
		ka, ta, ia := key(items[a])
		kb, tb, ib := key(items[b])
		if ka != kb {
			return ka < kb
		}
		if ta != tb {
			return ta < tb
		}
		return ia < ib
	})
}

// Enabled reports whether membership is in force (some root is configured).
func (m *Members) Enabled() bool { return len(m.roots) > 0 }

// check says why an item would not be stored, or nil if it would.
func (m *Members) check(it memberItem) error {
	switch {
	case it.Invite != nil:
		i := it.Invite
		if !i.Valid() {
			return fmt.Errorf("invalid invite signature or addresses")
		}
		if !m.Enabled() {
			return nil
		}
		ms := m.Compute()
		switch lim := ms.InviteLimit(i.Inviter); {
		case !ms.Member[i.Inviter]:
			return fmt.Errorf("%s is not a member, so its invites do not count", checksumAddress(i.Inviter))
		case lim >= 0 && ms.Used[i.Inviter] >= lim:
			return fmt.Errorf("%s has used all %d invites", checksumAddress(i.Inviter), lim)
		case ms.Member[i.Invitee]:
			return fmt.Errorf("%s is already a member", checksumAddress(i.Invitee))
		case ms.revoked[i.Invitee]:
			return fmt.Errorf("%s was revoked by a root", checksumAddress(i.Invitee))
		}
		return nil
	case it.Revoke != nil:
		r := it.Revoke
		if !r.Valid() || !m.roots[r.Root] {
			return fmt.Errorf("only a root can revoke")
		}
		if m.roots[r.Account] {
			return fmt.Errorf("a root cannot be revoked")
		}
		return nil
	}
	return fmt.Errorf("empty item")
}

// add stores an invite or revocation if it passes check and is new; it
// reports whether it did.
func (m *Members) add(it memberItem) bool {
	if m.check(it) != nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case it.Invite != nil:
		i := *it.Invite
		if _, ok := m.invites[i.id()]; ok || len(m.invites) >= maxInvites {
			return false
		}
		m.invites[i.id()] = i
	case it.Revoke != nil:
		r := *it.Revoke
		if _, ok := m.revokes[r.Account]; ok {
			return false
		}
		m.revokes[r.Account] = r
	}
	b, _ := json.Marshal(m.itemsLocked())
	tmp := m.path + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		os.Rename(tmp, m.path)
	}
	return true
}

func (m *Members) itemsLocked() []memberItem {
	out := make([]memberItem, 0, len(m.invites)+len(m.revokes))
	for _, i := range m.invites {
		out = append(out, memberItem{Invite: &i})
	}
	for _, r := range m.revokes {
		out = append(out, memberItem{Revoke: &r})
	}
	sortItems(out)
	return out
}

// known reports whether exactly this item is already stored. Gossip checks
// a node's own messages too, and re-forwarding a known item is harmless.
func (m *Members) known(it memberItem) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case it.Invite != nil:
		old, ok := m.invites[it.Invite.id()]
		return ok && old == *it.Invite
	case it.Revoke != nil:
		old, ok := m.revokes[it.Revoke.Account]
		return ok && old == *it.Revoke
	}
	return false
}

// items returns everything stored, in replay order.
func (m *Members) items() []memberItem {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.itemsLocked()
}

// MemberSet is the derived state at one point in time.
type MemberSet struct {
	Member       map[string]bool
	InvitedBy    map[string]string
	Used         map[string]int  // invites used
	Sharers      map[string]bool // ever served a job: 10 invites
	Contributors map[string]bool // served in the contributor window: no quota
	roots        map[string]bool
	revoked      map[string]bool
}

// InviteLimit is how many accounts a member may invite; -1 means no limit.
// Having served once is enough for 10, so the limit never drops later and
// invites already made never become invalid.
func (s *MemberSet) InviteLimit(a string) int {
	switch {
	case s.roots[a]:
		return -1
	case !s.Member[a]:
		return 0
	case s.Sharers[a]:
		return contributorInvites
	}
	return memberInvites
}

// Compute derives the member set. Invites apply in time order; one counts
// only if its inviter was already a member and under its invite limit; the
// first such invite of an account wins. A revoked account counts as never
// invited and its invites count for nothing, so revoking an account also
// removes everyone it brought in (unless someone else invited them too).
func (m *Members) Compute() *MemberSet {
	m.mu.Lock()
	invites := make([]Invite, 0, len(m.invites))
	for _, i := range m.invites {
		invites = append(invites, i)
	}
	revoked := map[string]bool{}
	for a := range m.revokes {
		if !m.roots[a] {
			revoked[a] = true
		}
	}
	roots, served := m.roots, m.served
	m.mu.Unlock()

	ever, recent := map[string]bool{}, map[string]bool{}
	if served != nil {
		ever, recent = served()
	}
	sort.Slice(invites, func(a, b int) bool {
		if invites[a].Time != invites[b].Time {
			return invites[a].Time < invites[b].Time
		}
		return invites[a].id() < invites[b].id()
	})
	s := &MemberSet{Member: map[string]bool{}, InvitedBy: map[string]string{}, Used: map[string]int{},
		Sharers: ever, Contributors: recent, roots: roots, revoked: revoked}
	for r := range roots {
		s.Member[r] = true
	}
	for _, i := range invites {
		if s.Member[i.Invitee] || !s.Member[i.Inviter] || revoked[i.Inviter] || revoked[i.Invitee] {
			continue
		}
		if lim := s.InviteLimit(i.Inviter); lim >= 0 && s.Used[i.Inviter] >= lim {
			continue
		}
		s.Used[i.Inviter]++
		s.Member[i.Invitee] = true
		s.InvitedBy[i.Invitee] = i.Inviter
	}
	return s
}
