# BitLLM

**BitTorrent for local LLMs.** Share your local model, use everyone's.

Every node runs the same binary. It can share an OpenAI-compatible server you already run
(vLLM, SGLang, llama.cpp, Ollama, LM Studio, MLX...), and it gives you a local gateway that
answers from whichever peer serves the model you ask for. There is no coordinator: peers find
each other through a Kademlia DHT and gossip, connect directly when they can, and go through a
relay plus hole punching when both sides are behind NAT.

## Share a model

```bash
./bitllm --upstream http://localhost:8000/v1 \
  --model <your-served-name>=deepseek-v4.1-flash-1017 \
  --concurrency 2 \
  --bootstrap <BOOTSTRAP_MULTIADDR>
```

- `--model local=public`: the name on the left is what your server calls the model, the name on
  the right is the network name. **Use the canonical names below** or nobody will find your model.
- Without `--model`, every model your server lists is shared under its own name.
- `--concurrency`: how many requests you serve at once. Extra requests queue on the client side.

### Canonical model names

| network name | weights |
|---|---|
| `deepseek-v4.1-flash-1017` | DeepSeek V4.1 Flash, 1017 release, official weights |
| `deepseek-v4.1-flash-1017-heretic` | same, heretic (uncensored) fine-tune |

Same name must mean same weights. Add a row (PR or message) before sharing a new model.

## Use the network

Every node, sharing or not, has a gateway at `http://127.0.0.1:7778/v1`:

```bash
./bitllm --bootstrap <BOOTSTRAP_MULTIADDR>      # use only, share nothing
curl localhost:7778/v1/models
curl localhost:7778/v1/chat/completions \
  -d '{"model":"deepseek-v4.1-flash-1017","stream":true,"messages":[{"role":"user","content":"hi"}]}'
curl localhost:7778/status                         # your peer id, connections, ledger
```

Point any agent at it: `OPENAI_BASE_URL=http://127.0.0.1:7778/v1`, any API key. If you share
a model yourself, your gateway answers it from your own server (free, never leaves your machine).
The response header `X-Bitllm-Peer` says which peer answered.

## Credits for serving

Every job you serve earns credits (prompt tokens / 4 + completion tokens), proven by a receipt:
the client signs what it received (model, token counts, SHA-256 of the exact response bytes),
your node checks it against what it sent and countersigns, and the receipt is gossiped to the
whole network. Any node can verify both signatures and recompute everyone's credits:

```bash
curl localhost:7778/credits      # credits per serving peer, from every receipt this node has seen
```

Receipts are stored in `~/.bitllm/receipts.jsonl`; a new node catches up from its bootstrap
peers. What credits will be worth is not decided yet. Which receipts count will be published
before it is: serving yourself through fake clients or serving a model other than the one named
will not count.

Separately, each node keeps a private ledger of work exchanged with each peer
(`~/.bitllm/ledger.json`). A peer may take up to `--free-credits` (50,000) more than it gave
before your node stops serving it.

## Run a bootstrap node

Any machine with a public IP: `./bitllm --dht-server --port 4001`, open TCP and UDP 4001.
It also relays for NATed peers until they hole-punch a direct connection.

## What you should know

- **The peer that serves you sees your prompt**, like any inference provider. Transport is
  encrypted (Noise/TLS/QUIC), but the serving machine reads the plaintext. Don't send secrets.
- **Peers you connect to directly see your IP address.**
- No verification yet: a node could serve a different model than it claims.
- `--upnp` (on by default) asks your router to forward the p2p port.

## Build and test

```bash
go test ./...          # discovery, relay-only path, busy queueing, ledger, debt limit
go build -o bitllm .
GOOS=darwin GOARCH=arm64 go build -o dist/bitllm-darwin-arm64 .
```

Protocol: `/bitllm/infer/1.0.0` (one JSON request line, one JSON header line, then the upstream
body verbatim), DHT prefix `/bitllm`, gossip topic `bitllm/announce/1`. The first, centralized
Python prototype is in `legacy/`.
