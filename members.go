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
//  1. Uninvited nodes are served only when the server is idle.
//  2. Members get a daily free quota on each server (--free-daily).
//  3. Contributors (accounts that served a job in the last 7 days) have no
//     quota and 10 invites instead of 3.

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

// Revoke is a root removing an account: it loses membership, and invites it
// signs from then on are invalid. Accounts it invited before stay members.
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

// Members stores invites and revocations and derives the member set.
type Members struct {
	mu      sync.Mutex
	path    string
	roots   map[string]bool
	invites map[string]Invite // by id
	revokes map[string]Revoke // by account, earliest valid
}

func OpenMembers(path string, roots []string) *Members {
	m := &Members{path: path, roots: map[string]bool{}, invites: map[string]Invite{}, revokes: map[string]Revoke{}}
	for _, r := range roots {
		m.roots[strings.ToLower(r)] = true
	}
	if b, err := os.ReadFile(path); err == nil {
		var items []memberItem
		if json.Unmarshal(b, &items) == nil {
			for _, it := range items {
				m.addLocked(it)
			}
		}
	}
	return m
}

// Enabled reports whether membership is in force (some root is configured).
func (m *Members) Enabled() bool { return len(m.roots) > 0 }

// add stores an invite or revocation if valid and new; it reports whether it did.
func (m *Members) add(it memberItem) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.addLocked(it) {
		return false
	}
	b, _ := json.Marshal(m.itemsLocked())
	tmp := m.path + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		os.Rename(tmp, m.path)
	}
	return true
}

func (m *Members) addLocked(it memberItem) bool {
	switch {
	case it.Invite != nil:
		i := *it.Invite
		if !i.Valid() || len(m.invites) >= maxInvites {
			return false
		}
		if old, ok := m.invites[i.id()]; ok && old.Time <= i.Time {
			return false
		}
		m.invites[i.id()] = i
		return true
	case it.Revoke != nil:
		r := *it.Revoke
		if !r.Valid() || !m.roots[r.Root] {
			return false
		}
		if old, ok := m.revokes[r.Account]; ok && old.Time <= r.Time {
			return false
		}
		m.revokes[r.Account] = r
		return true
	}
	return false
}

func (m *Members) itemsLocked() []memberItem {
	out := make([]memberItem, 0, len(m.invites)+len(m.revokes))
	for _, i := range m.invites {
		out = append(out, memberItem{Invite: &i})
	}
	for _, r := range m.revokes {
		out = append(out, memberItem{Revoke: &r})
	}
	return out
}

func (m *Members) items() []memberItem {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.itemsLocked()
}

// MemberSet is the derived state at one point in time.
type MemberSet struct {
	Member       map[string]bool
	InvitedBy    map[string]string
	Used         map[string]int // invites used
	Contributors map[string]bool
	roots        map[string]bool
}

// InviteLimit is how many accounts a member may invite; -1 means no limit.
func (s *MemberSet) InviteLimit(a string) int {
	switch {
	case s.roots[a]:
		return -1
	case !s.Member[a]:
		return 0
	case s.Contributors[a]:
		return contributorInvites
	}
	return memberInvites
}

// Compute derives the member set. Invites apply in time order; one counts
// only if its inviter was already a member, had not been revoked when it
// signed, and was under its invite limit. The first valid invite of an account
// wins. contributors are the accounts that served a job recently.
func (m *Members) Compute(contributors map[string]bool) *MemberSet {
	m.mu.Lock()
	invites := make([]Invite, 0, len(m.invites))
	for _, i := range m.invites {
		invites = append(invites, i)
	}
	revokedAt := map[string]int64{}
	for a, r := range m.revokes {
		revokedAt[a] = r.Time
	}
	roots := m.roots
	m.mu.Unlock()

	sort.Slice(invites, func(a, b int) bool {
		if invites[a].Time != invites[b].Time {
			return invites[a].Time < invites[b].Time
		}
		return invites[a].id() < invites[b].id()
	})
	s := &MemberSet{Member: map[string]bool{}, InvitedBy: map[string]string{}, Used: map[string]int{},
		Contributors: contributors, roots: roots}
	for r := range roots {
		s.Member[r] = true
	}
	for _, i := range invites {
		if s.Member[i.Invitee] || !s.Member[i.Inviter] {
			continue
		}
		if t, ok := revokedAt[i.Inviter]; ok && i.Time >= t && !roots[i.Inviter] {
			continue
		}
		if lim := s.InviteLimit(i.Inviter); lim >= 0 && s.Used[i.Inviter] >= lim {
			continue
		}
		s.Used[i.Inviter]++
		s.Member[i.Invitee] = true
		s.InvitedBy[i.Invitee] = i.Inviter
	}
	for a := range revokedAt {
		if !roots[a] {
			delete(s.Member, a)
		}
	}
	return s
}
