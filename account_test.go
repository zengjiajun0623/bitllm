package main

import (
	"encoding/hex"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// The well-known Hardhat/Anvil test mnemonic and its first account.
const testMnemonic = "test test test test test test test test test test test junk"
const testAddress = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

func TestMnemonicDerivation(t *testing.T) {
	k, err := keyFromMnemonic(testMnemonic)
	if err != nil {
		t.Fatal(err)
	}
	if got := checksumAddress(ethAddress(k.PubKey())); got != testAddress {
		t.Fatalf("address %s, want %s", got, testAddress)
	}
	if _, err := keyFromMnemonic("not a mnemonic at all"); err == nil {
		t.Fatal("accepted a bad mnemonic")
	}
}

// A signature made by ethers.js (wallet.signMessage("hello") with the Hardhat
// account 0 key), so our recovery matches what browser wallets produce.
func TestRecoverWalletSignature(t *testing.T) {
	k, _ := keyFromMnemonic(testMnemonic)
	sig := ethSign(k, "hello")
	if addr, err := ethRecover("hello", sig); err != nil || checksumAddress(addr) != testAddress {
		t.Fatalf("recover own sig: %s %v", addr, err)
	}
	ext, _ := hex.DecodeString("f16ea9a3478698f695fd1401bfe27e9e4a7e8e3da94aa72b021125e31fa899cc573c48ea3fe1d4ab61a9db10c19032026e3ed2dbccba5a178235ac27f94504311c")
	if addr, err := ethRecover("hello", ext); err != nil || checksumAddress(addr) != testAddress {
		t.Fatalf("recover ethers sig: %s %v", addr, err)
	}
}

func TestClaimNeedsBothSignatures(t *testing.T) {
	owner, _ := keyFromMnemonic(testMnemonic)
	nodeKey, _, _ := crypto.GenerateEd25519Key(nil)
	nodeID, _ := peer.IDFromPrivateKey(nodeKey)
	c, err := newClaim(nodeID, testAddress, "alice")
	if err != nil {
		t.Fatal(err)
	}
	c.OwnerSig = hex.EncodeToString(ethSign(owner, c.Message()))
	if c.Valid() {
		t.Fatal("valid without the node's signature")
	}
	if err := signClaimAsNode(c, nodeKey); err != nil {
		t.Fatal(err)
	}
	if !c.Valid() {
		t.Fatal("fully signed claim invalid")
	}
	// Someone else's wallet cannot claim this node.
	thief := *c
	thief.Owner = "0x000000000000000000000000000000000000dead"
	if thief.Valid() {
		t.Fatal("claim with a swapped owner is valid")
	}
	// Renaming invalidates both signatures.
	renamed := *c
	renamed.Name = "mallory"
	if renamed.Valid() {
		t.Fatal("renamed claim is valid")
	}
}
