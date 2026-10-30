# /// script
# requires-python = ">=3.11"
# dependencies = ["aiohttp>=3.9"]
# ///
"""llmpool node agent: shares a local OpenAI-compatible server with a pool.

Dials out to the pool over WebSocket, so it works behind NAT with no open ports.

  uv run node.py --pool ws://POOL:7777/node --key lp-... \
      --upstream http://localhost:8080/v1 [--upstream-key K] \
      [--model local-name=public-name ...] [--concurrency 2]
"""
import argparse
import asyncio
import json

import aiohttp


async def run_job(session, args, ws_send, aliases, job):
    jid, body = job["id"], job["body"]
    body["model"] = aliases.get(body.get("model"), body.get("model"))
    headers = {"Authorization": f"Bearer {args.upstream_key}"} if args.upstream_key else {}
    url = args.upstream.rstrip("/") + job["path"]
    try:
        async with session.post(url, json=body, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=None, sock_read=600)) as r:
            if r.status != 200:
                text = await r.text()
                await ws_send({"type": "error", "id": jid, "status": r.status, "message": text[:2000]})
                return
            if not body.get("stream"):
                data = await r.json()
                await ws_send({"type": "done", "id": jid, "body": data, "usage": data.get("usage")})
                return
            usage, chars = None, 0
            async for raw in r.content:
                line = raw.decode()
                if line.startswith("data:") and "[DONE]" not in line:
                    try:
                        ev = json.loads(line[5:])
                        usage = ev.get("usage") or usage
                        for c in ev.get("choices", []):
                            chars += len((c.get("delta") or {}).get("content") or "")
                    except json.JSONDecodeError:
                        pass
                await ws_send({"type": "chunk", "id": jid, "data": line})
            if not usage:  # upstream ignored include_usage: rough estimate
                usage = {"prompt_tokens": 0, "completion_tokens": chars // 4, "estimated": True}
            await ws_send({"type": "done", "id": jid, "usage": usage})
    except Exception as e:
        await ws_send({"type": "error", "id": jid, "status": 502, "message": f"upstream: {e!r}"})


async def discover(session, args):
    headers = {"Authorization": f"Bearer {args.upstream_key}"} if args.upstream_key else {}
    async with session.get(args.upstream.rstrip("/") + "/models", headers=headers) as r:
        r.raise_for_status()
        return [m["id"] for m in (await r.json())["data"]]


async def main(args):
    async with aiohttp.ClientSession() as session:
        local = [m.split("=")[0] for m in args.model] or await discover(session, args)
        public = {}  # public name -> upstream name
        for m in args.model or local:
            loc, _, pub = m.partition("=")
            public[pub or loc] = loc
        backoff = 1
        while True:
            try:
                async with session.ws_connect(args.pool, heartbeat=20, max_msg_size=64 * 1024 * 1024,
                                              headers={"Authorization": f"Bearer {args.key}"}) as ws:
                    lock = asyncio.Lock()

                    async def send(msg):
                        async with lock:
                            await ws.send_json(msg)

                    await send({"type": "hello", "name": args.name, "models": sorted(public),
                                "max_concurrency": args.concurrency})
                    print(f"connected to {args.pool}, sharing {sorted(public)}", flush=True)
                    backoff = 1
                    tasks = set()
                    async for m in ws:
                        if m.type != aiohttp.WSMsgType.TEXT:
                            continue
                        msg = json.loads(m.data)
                        if msg["type"] == "job":
                            t = asyncio.create_task(run_job(session, args, send, public, msg))
                            tasks.add(t)
                            t.add_done_callback(tasks.discard)
                        elif msg["type"] == "welcome":
                            pass
            except (aiohttp.ClientError, OSError) as e:
                print(f"pool connection failed: {e!r}", flush=True)
            print(f"reconnecting in {backoff}s", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", required=True, help="ws://host:7777/node")
    ap.add_argument("--key", required=True, help="your pool API key (POST /v1/register)")
    ap.add_argument("--upstream", default="http://localhost:8080/v1")
    ap.add_argument("--upstream-key", default="")
    ap.add_argument("--model", action="append", default=[],
                    help="local-name or local-name=public-name; default: all upstream models")
    ap.add_argument("--concurrency", type=int, default=1)
    ap.add_argument("--name", default="node")
    asyncio.run(main(ap.parse_args()))
