package main

import (
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"
)

//go:embed ui.html
var uiHTML []byte

// busyWait is how long a request queues when every peer serving its model is busy.
var busyWait = 60 * time.Second

// Gateway is the local OpenAI-compatible endpoint. Point any agent at it; it
// answers from our own upstream when we serve the model, otherwise from a peer.
func (n *Node) Gateway() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", n.listModels)
	mux.HandleFunc("POST /v1/chat/completions", n.complete("/chat/completions"))
	mux.HandleFunc("POST /v1/completions", n.complete("/completions"))
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(uiHTML)
	})
	mux.HandleFunc("GET /status", n.status)
	mux.HandleFunc("GET /credits", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(n.creditsTable())
	})
	return mux
}

func jsonErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": msg}})
}

func (n *Node) listModels(w http.ResponseWriter, r *http.Request) {
	nodes := map[string]int{}
	for m := range n.cfg.Models {
		nodes[m]++
	}
	for _, a := range n.alive() {
		for _, m := range a.Models {
			nodes[m]++
		}
	}
	names := make([]string, 0, len(nodes))
	for m := range nodes {
		names = append(names, m)
	}
	sort.Strings(names)
	data := []map[string]any{}
	for _, m := range names {
		data = append(data, map[string]any{"id": m, "object": "model", "owned_by": "bitllm", "nodes": nodes[m]})
	}
	json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
}

func (n *Node) complete(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, maxRequest))
		if err != nil {
			jsonErr(w, 400, err.Error())
			return
		}
		var req struct {
			Model  string `json:"model"`
			Stream bool   `json:"stream"`
		}
		if json.Unmarshal(body, &req) != nil {
			jsonErr(w, 400, "bad json")
			return
		}

		// Our own model: private and free, no network hop.
		if _, ok := n.cfg.Models[req.Model]; ok {
			resp, stream, err := n.callUpstream(r.Context(), path, body)
			if err != nil {
				pe := err.(*peerError)
				jsonErr(w, pe.status, pe.msg)
				return
			}
			defer resp.Body.Close()
			pipe(w, writeHead(w, stream, ""), resp.Body, stream, &meter{})
			return
		}

		cands := n.candidates(req.Model)
		if len(cands) == 0 {
			jsonErr(w, 503, "no peer serves model "+req.Model+"; see GET /v1/models")
			return
		}
		// Try the best few peers; if every one of them is merely busy, queue
		// for a while rather than fail, since a small network has few servers.
		var last error
		for deadline := time.Now().Add(busyWait); ; {
			busy := true
			for _, p := range cands[:min(len(cands), 3)] {
				started, err := n.infer(r.Context(), p, path, body, req.Stream, w)
				if started {
					if err != nil {
						log.Printf("peer %s broke mid-response: %v", p, err)
					}
					return
				}
				var pe *peerError
				busy = busy && errors.As(err, &pe) && pe.status == 429
				if !busy {
					log.Printf("peer %s: %v", p, err)
				}
				last = err
			}
			if !busy || time.Now().After(deadline) || r.Context().Err() != nil {
				break
			}
			time.Sleep(time.Second)
			if c := n.candidates(req.Model); len(c) > 0 {
				cands = c
			}
		}
		var pe *peerError
		if errors.As(last, &pe) {
			jsonErr(w, pe.status, "all peers refused: "+pe.msg)
		} else {
			jsonErr(w, 502, "all peers failed: "+last.Error())
		}
	}
}

func (n *Node) status(w http.ResponseWriter, r *http.Request) {
	serving := []string{}
	for m := range n.cfg.Models {
		serving = append(serving, m)
	}
	sort.Strings(serving)
	conns := []map[string]any{}
	for _, c := range n.host.Network().Conns() {
		conns = append(conns, map[string]any{
			"peer":    c.RemotePeer().String(),
			"addr":    c.RemoteMultiaddr().String(),
			"dir":     c.Stat().Direction.String(),
			"relayed": strings.Contains(c.RemoteMultiaddr().String(), "/p2p-circuit"),
		})
	}
	json.NewEncoder(w).Encode(map[string]any{
		"id":       n.host.ID().String(),
		"addrs":    n.Addrs(),
		"conns":    conns,
		"serving":  serving,
		"inflight": n.inflight.Load(),
		"ledger":   n.ledger.Snapshot(),
	})
}
