package main

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/tyler-smith/go-bip39"
	"golang.org/x/crypto/sha3"
)

// Accounts are Ethereum addresses. Only the signature scheme is borrowed
// (secp256k1, personal_sign, BIP39/BIP44 mnemonics): nothing here talks to a
// chain. An owner claims a node with a statement signed by both the owner's
// wallet and the node's own key, so nobody can claim someone else's node.
// Credits are then summed per owner across all their machines.

const ClaimTopic = "bitllm/claims/1"

func keccak(b ...[]byte) []byte {
	h := sha3.NewLegacyKeccak256()
	for _, x := range b {
		h.Write(x)
	}
	return h.Sum(nil)
}

func ethAddress(pub *secp256k1.PublicKey) string {
	return "0x" + hex.EncodeToString(keccak(pub.SerializeUncompressed()[1:])[12:])
}

// checksumAddress renders an address in EIP-55 mixed case, for display.
func checksumAddress(addr string) string {
	a := strings.ToLower(strings.TrimPrefix(addr, "0x"))
	h := hex.EncodeToString(keccak([]byte(a)))
	out := []byte(a)
	for i, c := range out {
		if c >= 'a' && h[i] >= '8' {
			out[i] = c - 32
		}
	}
	return "0x" + string(out)
}

func personalHash(msg string) []byte {
	return keccak([]byte("\x19Ethereum Signed Message:\n" + strconv.Itoa(len(msg)) + msg))
}

// ethSign returns a 65-byte r||s||v signature, as wallets produce for personal_sign.
func ethSign(key *secp256k1.PrivateKey, msg string) []byte {
	c := ecdsa.SignCompact(key, personalHash(msg), false) // [27+recid][r][s]
	return append(c[1:], c[0])
}

// ethRecover returns the lowercase address that signed msg.
func ethRecover(msg string, sig []byte) (string, error) {
	if len(sig) != 65 {
		return "", fmt.Errorf("signature must be 65 bytes")
	}
	v := sig[64]
	if v < 27 {
		v += 27
	}
	pub, _, err := ecdsa.RecoverCompact(append([]byte{v}, sig[:64]...), personalHash(msg))
	if err != nil {
		return "", err
	}
	return ethAddress(pub), nil
}

// keyFromMnemonic derives the first account of a standard Ethereum wallet, m/44'/60'/0'/0/0.
func keyFromMnemonic(mnemonic string) (*secp256k1.PrivateKey, error) {
	mnemonic = strings.Join(strings.Fields(mnemonic), " ")
	if !bip39.IsMnemonicValid(mnemonic) {
		return nil, fmt.Errorf("not a valid BIP39 mnemonic")
	}
	mac := hmac.New(sha512.New, []byte("Bitcoin seed"))
	mac.Write(bip39.NewSeed(mnemonic, ""))
	I := mac.Sum(nil)
	k, c := I[:32], I[32:]
	for _, i := range []uint32{44 | 1<<31, 60 | 1<<31, 0 | 1<<31, 0, 0} {
		var data []byte
		if i >= 1<<31 {
			data = append([]byte{0}, k...)
		} else {
			data = secp256k1.PrivKeyFromBytes(k).PubKey().SerializeCompressed()
		}
		data = binary.BigEndian.AppendUint32(data, i)
		mac := hmac.New(sha512.New, c)
		mac.Write(data)
		I := mac.Sum(nil)
		var il, kk secp256k1.ModNScalar
		if il.SetByteSlice(I[:32]) {
			return nil, fmt.Errorf("invalid derivation, try another mnemonic")
		}
		kk.SetByteSlice(k)
		kk.Add(&il)
		b := kk.Bytes()
		k, c = b[:], I[32:]
	}
	return secp256k1.PrivKeyFromBytes(k), nil
}

// Claim says that node belongs to owner, under a display name.
type Claim struct {
	Node     string `json:"node"`
	Owner    string `json:"owner"` // lowercase 0x address
	Name     string `json:"name"`
	Time     int64  `json:"time"`
	OwnerSig string `json:"owner_sig"` // hex, personal_sign of Message()
	NodeSig  []byte `json:"node_sig"`  // libp2p signature of Message()
}

// Message is the exact text the owner's wallet signs.
func (c *Claim) Message() string {
	return fmt.Sprintf("BitLLM node claim\nnode: %s\nowner: %s\nname: %s\ntime: %d", c.Node, c.Owner, c.Name, c.Time)
}

func validName(s string) bool {
	return len(s) <= 32 && !strings.ContainsAny(s, "\n\r")
}

func (c *Claim) Valid() bool {
	if !validName(c.Name) || c.Time > time.Now().Add(10*time.Minute).Unix() {
		return false
	}
	sig, err := hex.DecodeString(strings.TrimPrefix(c.OwnerSig, "0x"))
	if err != nil {
		return false
	}
	addr, err := ethRecover(c.Message(), sig)
	if err != nil || addr != c.Owner {
		return false
	}
	return verifySig(c.Node, []byte(c.Message()), c.NodeSig)
}

// Claims keeps the newest valid claim per node.
type Claims struct {
	mu     sync.Mutex
	path   string
	ByNode map[string]Claim
}

func OpenClaims(path string) *Claims {
	c := &Claims{path: path, ByNode: map[string]Claim{}}
	if b, err := os.ReadFile(path); err == nil {
		var m map[string]Claim
		if json.Unmarshal(b, &m) == nil {
			for k, v := range m {
				if v.Valid() && v.Node == k {
					c.ByNode[k] = v
				}
			}
		}
	}
	return c
}

// add stores c if it is valid and newer than what we have; it reports whether it did.
func (cs *Claims) add(c Claim) bool {
	if !c.Valid() {
		return false
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if old, ok := cs.ByNode[c.Node]; ok && old.Time >= c.Time {
		return false
	}
	cs.ByNode[c.Node] = c
	b, _ := json.MarshalIndent(cs.ByNode, "", "  ")
	tmp := cs.path + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		os.Rename(tmp, cs.path)
	}
	return true
}

func (cs *Claims) get(node string) (Claim, bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	c, ok := cs.ByNode[node]
	return c, ok
}

func (cs *Claims) all() []Claim {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	out := make([]Claim, 0, len(cs.ByNode))
	for _, c := range cs.ByNode {
		out = append(out, c)
	}
	return out
}

// newClaim builds an unsigned claim of this node for owner.
func newClaim(self peer.ID, owner, name string) (*Claim, error) {
	owner = strings.ToLower(strings.TrimSpace(owner))
	if len(owner) != 42 || !strings.HasPrefix(owner, "0x") {
		return nil, fmt.Errorf("owner must be a 0x address")
	}
	if _, err := hex.DecodeString(owner[2:]); err != nil {
		return nil, fmt.Errorf("owner must be a 0x address")
	}
	name = strings.TrimSpace(name)
	if !validName(name) {
		return nil, fmt.Errorf("name: one line, at most 32 bytes")
	}
	return &Claim{Node: self.String(), Owner: owner, Name: name, Time: time.Now().Unix()}, nil
}

// signClaimAsNode adds the node's signature after checking the owner's.
func signClaimAsNode(c *Claim, nodeKey crypto.PrivKey) error {
	sig, err := hex.DecodeString(strings.TrimPrefix(c.OwnerSig, "0x"))
	if err != nil {
		return fmt.Errorf("owner signature is not hex")
	}
	if addr, err := ethRecover(c.Message(), sig); err != nil || addr != c.Owner {
		return fmt.Errorf("owner signature does not match %s", c.Owner)
	}
	c.NodeSig, err = nodeKey.Sign([]byte(c.Message()))
	return err
}

// OwnerCredits is the credits table row for one owner (or one unclaimed node).
type OwnerCredits struct {
	Owner   string          `json:"owner,omitempty"`
	Name    string          `json:"name,omitempty"`
	Credits int64           `json:"credits"`
	Jobs    int             `json:"jobs"`
	Nodes   []NodeCreditRow `json:"nodes"`
}

type NodeCreditRow struct {
	Node    string `json:"node"`
	Credits int64  `json:"credits"`
	Jobs    int    `json:"jobs"`
}

// ownerTable groups per-node credits by each node's current owner.
func ownerTable(perNode map[string]ServerCredits, claims *Claims) []OwnerCredits {
	rows := map[string]*OwnerCredits{}
	for node, c := range perNode {
		key, owner, name := "node:"+node, "", ""
		if cl, ok := claims.get(node); ok {
			key, owner, name = cl.Owner, checksumAddress(cl.Owner), cl.Name
		}
		r := rows[key]
		if r == nil {
			r = &OwnerCredits{Owner: owner}
			rows[key] = r
		}
		if name != "" {
			r.Name = name
		}
		r.Credits += c.Credits
		r.Jobs += c.Jobs
		r.Nodes = append(r.Nodes, NodeCreditRow{node, c.Credits, c.Jobs})
	}
	out := make([]OwnerCredits, 0, len(rows))
	for _, r := range rows {
		sort.Slice(r.Nodes, func(i, j int) bool { return r.Nodes[i].Credits > r.Nodes[j].Credits })
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Credits > out[j].Credits })
	return out
}
