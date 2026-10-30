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

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// InferProto: the client writes one JSON line {path, body}, then closes its
// write side. The server answers with one JSON header line {status, error},
// then (if status is 200) the upstream response body verbatim: SSE lines for
// stream requests, a JSON document otherwise.
const InferProto = "/bitllm/infer/1.0.0"

const maxRequest = 64 << 20

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

func (m *meter) credits() int64 {
	if !m.reported { // upstream ignored include_usage: rough estimate
		return m.chars / 4
	}
	return m.prompt/4 + m.completion
}

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

// handleInfer serves one inference request from a peer.
func (n *Node) handleInfer(s network.Stream) {
	defer s.Close()
	remote := s.Conn().RemotePeer()
	reply := func(status int, msg string) {
		b, _ := json.Marshal(inferHdr{status, msg})
		s.Write(append(b, '\n'))
	}
	line, err := bufio.NewReader(io.LimitReader(s, maxRequest)).ReadBytes('\n')
	var req inferReq
	if err != nil || json.Unmarshal(line, &req) != nil || !inferPaths[req.Path] {
		reply(400, "bad request")
		return
	}
	if bal := n.ledger.Balance(remote); bal < -n.cfg.FreeCredits {
		reply(402, fmt.Sprintf("you owe this node %d credits; share a model to earn", -bal))
		return
	}
	if n.inflight.Add(1) > int32(n.cfg.Concurrency) {
		n.inflight.Add(-1)
		reply(429, "busy")
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
