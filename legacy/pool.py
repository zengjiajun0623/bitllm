# /// script
# requires-python = ">=3.11"
# dependencies = ["aiohttp>=3.9"]
# ///
"""llmpool coordinator: routes OpenAI-compatible requests to local-LLM nodes.

Nodes dial in over WebSocket (no open ports needed). One account = one API key,
used both to run a node (earn credits) and to call models (spend credits).
Prompts and completions are never stored; only token counts hit the ledger.
"""
import argparse
import asyncio
import json
import secrets
import sqlite3
import time
import uuid

from aiohttp import WSMsgType, web

STARTER_CREDITS = 20_000
PROMPT_WEIGHT = 0.25      # prefill is cheap relative to decode
COMPLETION_WEIGHT = 1.0
JOB_TIMEOUT = 600


def cost(usage):
    return int(usage.get("prompt_tokens", 0) * PROMPT_WEIGHT
               + usage.get("completion_tokens", 0) * COMPLETION_WEIGHT)


class Ledger:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("""CREATE TABLE IF NOT EXISTS accounts (
            key TEXT PRIMARY KEY, name TEXT, balance INTEGER,
            earned INTEGER DEFAULT 0, spent INTEGER DEFAULT 0, created REAL)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY, ts REAL, model TEXT, client TEXT, node TEXT,
            prompt_tokens INTEGER, completion_tokens INTEGER, credits INTEGER)""")
        self.db.commit()

    def register(self, name):
        key = "lp-" + secrets.token_urlsafe(24)
        self.db.execute("INSERT INTO accounts VALUES (?,?,?,0,0,?)",
                        (key, name, STARTER_CREDITS, time.time()))
        self.db.commit()
        return key

    def get(self, key):
        r = self.db.execute("SELECT name,balance,earned,spent FROM accounts WHERE key=?",
                            (key,)).fetchone()
        return r and dict(zip(("name", "balance", "earned", "spent"), r))

    def settle(self, job_id, model, client, node, usage):
        credits = cost(usage) if client != node else 0  # serving yourself earns nothing
        self.db.execute("UPDATE accounts SET balance=balance-?, spent=spent+? WHERE key=?",
                        (credits, credits, client))
        self.db.execute("UPDATE accounts SET balance=balance+?, earned=earned+? WHERE key=?",
                        (credits, credits, node))
        self.db.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?)",
                        (job_id, time.time(), model, client, node,
                         usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0), credits))
        self.db.commit()
        return credits


class Node:
    def __init__(self, ws, owner, name, models, max_concurrency):
        self.ws, self.owner, self.name = ws, owner, name
        self.models, self.max_concurrency = models, max_concurrency
        self.inflight = {}  # job id -> asyncio.Queue
        self.send_lock = asyncio.Lock()

    async def send(self, msg):
        async with self.send_lock:
            await self.ws.send_json(msg)


class Pool:
    def __init__(self, ledger):
        self.ledger = ledger
        self.nodes = set()

    def pick(self, model, client):
        cands = [n for n in self.nodes
                 if model in n.models and len(n.inflight) < n.max_concurrency]
        if not cands:
            return None
        # Prefer other people's nodes, then the least loaded.
        return min(cands, key=lambda n: (n.owner == client, len(n.inflight) / n.max_concurrency))

    def models(self):
        out = {}
        for n in self.nodes:
            for m in n.models:
                out[m] = out.get(m, 0) + 1
        return out


def auth(request):
    h = request.headers.get("Authorization", "")
    key = h.removeprefix("Bearer ").strip()
    if not key or not request.app["ledger"].get(key):
        raise web.HTTPUnauthorized(text=json.dumps({"error": {"message": "bad api key"}}),
                                   content_type="application/json")
    return key


def err(status, msg):
    return web.json_response({"error": {"message": msg}}, status=status)


async def register(request):
    body = await request.json() if request.can_read_body else {}
    name = str(body.get("name", "anon"))[:64]
    key = request.app["ledger"].register(name)
    return web.json_response({"api_key": key, "balance": STARTER_CREDITS})


async def me(request):
    return web.json_response(request.app["ledger"].get(auth(request)))


async def list_models(request):
    models = request.app["pool"].models()
    return web.json_response({"object": "list", "data": [
        {"id": m, "object": "model", "owned_by": "llmpool", "nodes": c}
        for m, c in sorted(models.items())]})


async def chat(request):
    key = auth(request)
    ledger, pool = request.app["ledger"], request.app["pool"]
    if ledger.get(key)["balance"] <= 0:
        return err(402, "out of credits: run a node to earn more")
    body = await request.json()
    model = body.get("model")
    node = pool.pick(model, key)
    if not node:
        return err(503, f"no free node serving model {model!r}; see GET /v1/models")

    stream = bool(body.get("stream"))
    if stream:
        body.setdefault("stream_options", {})["include_usage"] = True
    job_id = uuid.uuid4().hex
    q = asyncio.Queue()
    node.inflight[job_id] = q
    resp = None
    usage = None
    try:
        await node.send({"type": "job", "id": job_id, "path": "/chat/completions", "body": body})
        while True:
            msg = await asyncio.wait_for(q.get(), JOB_TIMEOUT)
            t = msg["type"]
            if t == "error":
                if resp is None:
                    return err(msg.get("status", 502), msg.get("message", "node error"))
                break
            if t == "chunk":  # one SSE data line, already framed
                if resp is None:
                    resp = web.StreamResponse(headers={"Content-Type": "text/event-stream",
                                                       "Cache-Control": "no-cache"})
                    await resp.prepare(request)
                await resp.write(msg["data"].encode())
            elif t == "done":
                usage = msg.get("usage") or {}
                if not stream:
                    return web.json_response(msg["body"])
                break
    except asyncio.TimeoutError:
        if resp is None:
            return err(504, "node timed out")
    except ConnectionResetError:
        pass  # client went away mid-stream; MVP does not bill partial output
    finally:
        node.inflight.pop(job_id, None)
        if usage is not None:
            ledger.settle(job_id, model, key, node.owner, usage)
    if resp is not None:
        await resp.write_eof()
        return resp
    return err(502, "node disconnected")


async def node_ws(request):
    key = auth(request)
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=64 * 1024 * 1024)
    await ws.prepare(request)
    hello = await ws.receive_json(timeout=30)
    if hello.get("type") != "hello" or not hello.get("models"):
        await ws.close(message=b"expected hello with models")
        return ws
    node = Node(ws, key, str(hello.get("name", "node"))[:64], set(hello["models"]),
                max(1, int(hello.get("max_concurrency", 1))))
    pool = request.app["pool"]
    pool.nodes.add(node)
    await ws.send_json({"type": "welcome", "models": sorted(node.models)})
    print(f"+ node {node.name} models={sorted(node.models)} conc={node.max_concurrency}", flush=True)
    try:
        async for m in ws:
            if m.type != WSMsgType.TEXT:
                continue
            msg = json.loads(m.data)
            q = node.inflight.get(msg.get("id"))
            if q:
                q.put_nowait(msg)
    finally:
        pool.nodes.discard(node)
        for q in node.inflight.values():
            q.put_nowait({"type": "error", "status": 502, "message": "node disconnected"})
        print(f"- node {node.name}", flush=True)
    return ws


def make_app(db_path):
    app = web.Application(client_max_size=64 * 1024 * 1024)
    app["ledger"] = Ledger(db_path)
    app["pool"] = Pool(app["ledger"])
    app.add_routes([
        web.post("/v1/register", register),
        web.get("/v1/me", me),
        web.get("/v1/models", list_models),
        web.post("/v1/chat/completions", chat),
        web.get("/node", node_ws),
    ])
    return app


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=7777)
    ap.add_argument("--db", default="pool.db")
    a = ap.parse_args()
    web.run_app(make_app(a.db), host=a.host, port=a.port)
