package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// InferProto: the client writes one JSON line {path, body}, then closes its
// write side. The server answers with one JSON header line {status, error},
// then (if status is 200) the upstream response body verbatim: SSE lines for
// stream requests, a JSON document otherwise.
const InferProto = "/bitllm/infer/1.0.0"

const maxRequest = 16 << 20

// readTimeout bounds how long a peer may take to send its request line.
const readTimeout = 30 * time.Second

type inferReq struct {
	Job  string          `json:"job"` // client-chosen id, named in the receipt
	Path string          `json:"path"`
	Body json.RawMessage `json:"body"`
}

type inferHdr struct {
	Status int    `json:"status"`
	Error  string `json:"error,omitempty"`
}

type peerError struct {
	status int
	msg    string
}

func (e *peerError) Error() string { return fmt.Sprintf("%d: %s", e.status, e.msg) }

// inferPaths are the upstream endpoints a peer may call: OpenAI chat and
// completions, and Anthropic messages (Claude Code and other Anthropic clients).
var inferPaths = map[string]bool{"/chat/completions": true, "/completions": true, "/messages": true}

// meter counts tokens in a response to price it: prefill is cheap relative to decode.
type meter struct {
	prompt, completion int64
	chars              int64
	reported           bool
	out                hash.Hash // sha256 of the exact response bytes, for the receipt
}

func (m *meter) raw(b []byte) {
	if m.out == nil {
		m.out = sha256.New()
	}
	m.out.Write(b)
}

func (m *meter) sum() string {
	if m.out == nil {
		m.out = sha256.New()
	}
	return hex.EncodeToString(m.out.Sum(nil))
}

// usage holds token counts in either OpenAI or Anthropic shape.
type usage struct {
	Prompt     int64 `json:"prompt_tokens"`
	Completion int64 `json:"completion_tokens"`
	Input      int64 `json:"input_tokens"`
	Output     int64 `json:"output_tokens"`
}

func (m *meter) usage(u *usage) {
	if u == nil {
		return
	}
	if u.Prompt > 0 || u.Completion > 0 {
		m.prompt, m.completion = u.Prompt, u.Completion
	}
	// Anthropic streams input tokens in message_start and the running output
	// count in message_delta, so take each when present.
	if u.Input > 0 {
		m.prompt = u.Input
	}
	if u.Output > 0 {
		m.completion = u.Output
	}
	m.reported = true
}

type usageDoc struct {
	Usage   *usage `json:"usage"`
	Message *struct {
		Usage *usage `json:"usage"`
	} `json:"message"` // Anthropic message_start
	Delta struct {
		Text string `json:"text"`
	} `json:"delta"` // Anthropic content_block_delta
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}

func (m *meter) sseLine(line []byte) {
	data, ok := bytes.CutPrefix(bytes.TrimSpace(line), []byte("data:"))
	if !ok {
		return
	}
	var d usageDoc
	if json.Unmarshal(data, &d) != nil {
		return
	}
	m.usage(d.Usage)
	if d.Message != nil {
		m.usage(d.Message.Usage)
	}
	m.chars += int64(len(d.Delta.Text))
	for _, c := range d.Choices {
		m.chars += int64(len(c.Delta.Content))
	}
}

func (m *meter) body(b []byte) {
	var d usageDoc
	if json.Unmarshal(b, &d) == nil {
		m.usage(d.Usage)
	}
}

// tokens returns the counts to bill. An upstream that ignored include_usage
// is estimated at four characters per completion token.
func (m *meter) tokens() (prompt, completion int64) {
	if !m.reported {
		return 0, m.chars / 4
	}
	return m.prompt, m.completion
}

func (m *meter) credits() int64 { return price(m.tokens()) }

// pipe copies a response body to dst, line by line for SSE so each event is
// flushed as it arrives, and meters it on the way.
func pipe(dst io.Writer, flush func(), src io.Reader, stream bool, m *meter) error {
	if !stream {
		b, err := io.ReadAll(src)
		if err != nil {
			return err
		}
		m.body(b)
		m.raw(b)
		_, err = dst.Write(b)
		return err
	}
	r := bufio.NewReaderSize(src, 64<<10)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			m.sseLine(line)
			m.raw(line)
			if _, werr := dst.Write(line); werr != nil {
				return werr
			}
			if flush != nil {
				flush()
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// callUpstream sends an OpenAI or Anthropic request for a public model name to our own server.
func (n *Node) callUpstream(ctx context.Context, path string, raw []byte) (*http.Response, bool, error) {
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, false, &peerError{400, "bad json"}
	}
	model, _ := body["model"].(string)
	up, ok := n.cfg.Models[model]
	if !ok {
		return nil, false, &peerError{404, fmt.Sprintf("model %q not served here", model)}
	}
	body["model"] = up
	n.clamp(path, body)
	stream, _ := body["stream"].(bool)
	if stream && path != "/messages" { // Anthropic streams always carry usage
		so, _ := body["stream_options"].(map[string]any)
		if so == nil {
			so = map[string]any{}
		}
		so["include_usage"] = true
		body["stream_options"] = so
	}
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, "POST", n.cfg.Upstream+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	n.upstreamAuth(req)
	resp, err := n.http.Do(req)
	if err != nil {
		return nil, false, &peerError{502, "upstream: " + err.Error()}
	}
	if resp.StatusCode != 200 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, false, &peerError{resp.StatusCode, string(msg)}
	}
	return resp, stream, nil
}

// clamp keeps a peer from asking for more than one bounded answer: one
// choice, and at most MaxTokens new tokens (set when the client gave none,
// except on legacy /completions whose servers default to a short answer).
func (n *Node) clamp(path string, body map[string]any) {
	delete(body, "n")
	delete(body, "best_of")
	if n.cfg.MaxTokens <= 0 {
		return
	}
	limit := float64(n.cfg.MaxTokens)
	set := false
	for _, k := range []string{"max_tokens", "max_completion_tokens"} {
		if v, ok := body[k].(float64); ok {
			body[k] = min(v, limit)
			set = true
		} else if _, ok := body[k]; ok {
			delete(body, k)
		}
	}
	if !set && path != "/completions" {
		body["max_tokens"] = limit
	}
}

// admit takes a serving slot for remote, or returns why not. Strangers get
// the slots left after a reserve kept for peers that have served us more than
// we served them, and every peer is rate limited.
func (n *Node) admit(remote peer.ID, now time.Time) (status int, msg string) {
	if !n.reqLimit.allow(remote, now) {
		return 429, "too many requests from you; slow down"
	}
	limit := int32(n.cfg.Concurrency)
	if n.ledger.Balance(remote) <= 0 {
		limit -= int32(reservedSlots(n.cfg.Concurrency))
	}
	if n.inflight.Add(1) > limit {
		n.inflight.Add(-1)
		return 429, "busy"
	}
	return 0, ""
}

// handleInfer serves one inference request from a peer.
func (n *Node) handleInfer(s network.Stream) {
	defer s.Close()
	remote := s.Conn().RemotePeer()
	reply := func(status int, msg string) {
		b, _ := json.Marshal(inferHdr{status, msg})
		s.Write(append(b, '\n'))
	}
	s.SetReadDeadline(time.Now().Add(readTimeout))
	line, err := bufio.NewReader(io.LimitReader(s, maxRequest)).ReadBytes('\n')
	s.SetReadDeadline(time.Time{})
	var req inferReq
	if err != nil || json.Unmarshal(line, &req) != nil || !inferPaths[req.Path] {
		reply(400, "bad request")
		return
	}
	if bal := n.ledger.Balance(remote); bal < -n.cfg.FreeCredits {
		reply(402, fmt.Sprintf("you owe this node %d credits; share a model to earn", -bal))
		return
	}
	if status, msg := n.admit(remote, time.Now()); status != 0 {
		reply(status, msg)
		return
	}
	defer n.inflight.Add(-1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // a failed write to the peer stops the upstream generation too
	resp, stream, err := n.callUpstream(ctx, req.Path, req.Body)
	if err != nil {
		pe := err.(*peerError)
		reply(pe.status, pe.msg)
		return
	}
	defer resp.Body.Close()
	reply(200, "")
	var m meter
	pipe(s, nil, resp.Body, stream, &m)
	n.ledger.Served(remote, m.credits())
	if req.Job != "" {
		var model struct{ Model string }
		json.Unmarshal(req.Body, &model)
		n.receipts.served(req.Job, remote, model.Model, m)
	}
}

// infer runs a request on peer p and writes the response to w. started reports
// whether anything reached w; until then the caller may retry another peer.
func (n *Node) infer(ctx context.Context, p peer.ID, path string, body []byte, stream bool,
	w http.ResponseWriter) (started bool, err error) {
	n.addRelayPaths(p)
	s, err := n.host.NewStream(network.WithAllowLimitedConn(ctx, "infer"), p, InferProto)
	if err != nil {
		return false, err
	}
	defer s.Close()
	stop := context.AfterFunc(ctx, func() { s.Reset() })
	defer stop()

	job := rand.Text()
	b, _ := json.Marshal(inferReq{job, path, body})
	if _, err := s.Write(append(b, '\n')); err != nil {
		return false, err
	}
	s.CloseWrite()
	r := bufio.NewReaderSize(s, 64<<10)
	line, err := r.ReadBytes('\n')
	if err != nil {
		return false, err
	}
	var hdr inferHdr
	if err := json.Unmarshal(line, &hdr); err != nil {
		return false, err
	}
	if hdr.Status != 200 {
		return false, &peerError{hdr.Status, hdr.Error}
	}
	var m meter
	err = pipe(w, writeHead(w, stream, p), r, stream, &m)
	n.ledger.Received(p, m.credits())
	if err == nil {
		var model struct{ Model string }
		json.Unmarshal(body, &model)
		go func() {
			if err := n.sendReceipt(p, job, model.Model, m); err != nil {
				log.Printf("receipt to %s: %v", p, err)
			}
		}()
	}
	return true, err
}

// writeHead sets response headers and returns a flusher.
func writeHead(w http.ResponseWriter, stream bool, served peer.ID) func() {
	if stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
	if served != "" {
		w.Header().Set("X-Bitllm-Peer", served.String())
	}
	w.WriteHeader(200)
	f, _ := w.(http.Flusher)
	return func() {
		if f != nil {
			f.Flush()
		}
	}
}
