# /// script
# requires-python = ">=3.11"
# dependencies = ["aiohttp>=3.9"]
# ///
"""End-to-end check: mock upstream + pool + node agents + clients, all in-process.

  uv run test_e2e.py
"""
import argparse
import asyncio
import json
import os
import sys
import tempfile

import aiohttp
from aiohttp import web

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import node as node_mod  # noqa: E402
import pool as pool_mod  # noqa: E402

UP_PORT, POOL_PORT = 17801, 17802
POOL = f"http://127.0.0.1:{POOL_PORT}"


async def mock_upstream():
    async def models(_):
        return web.json_response({"data": [{"id": "local-qwen"}]})

    async def chat(req):
        body = await req.json()
        if req.headers.get("Authorization") != "Bearer upkey":
            return web.json_response({"error": "auth"}, status=401)
        assert body["model"] == "local-qwen", body["model"]
        await asyncio.sleep(float(body.get("delay", 0)))
        usage = {"prompt_tokens": 40, "completion_tokens": 10, "total_tokens": 50}
        if not body.get("stream"):
            return web.json_response({"choices": [{"message": {"role": "assistant", "content": "hello world"}}],
                                      "usage": usage})
        r = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await r.prepare(req)
        for w in ["hel", "lo ", "world"]:
            await r.write(f"data: {json.dumps({'choices': [{'delta': {'content': w}}]})}\n\n".encode())
        if body.get("stream_options", {}).get("include_usage"):
            await r.write(f"data: {json.dumps({'choices': [], 'usage': usage})}\n\n".encode())
        await r.write(b"data: [DONE]\n\n")
        await r.write_eof()
        return r

    app = web.Application()
    app.add_routes([web.get("/v1/models", models), web.post("/v1/chat/completions", chat)])
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", UP_PORT).start()


async def start_pool(db):
    runner = web.AppRunner(pool_mod.make_app(db))
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", POOL_PORT).start()


def start_node(key, models, conc=1):
    args = argparse.Namespace(pool=f"ws://127.0.0.1:{POOL_PORT}/node", key=key,
                              upstream=f"http://127.0.0.1:{UP_PORT}/v1", upstream_key="upkey",
                              model=models, concurrency=conc, name="test-node")
    return asyncio.create_task(node_mod.main(args))


async def main():
    failures = []

    def check(name, cond, detail=""):
        print(("PASS " if cond else "FAIL ") + name + (f"  ({detail})" if detail and not cond else ""))
        if not cond:
            failures.append(name)

    await mock_upstream()
    await start_pool(os.path.join(tempfile.mkdtemp(), "pool.db"))
    async with aiohttp.ClientSession() as s:
        async def post(path, key=None, body=None):
            h = {"Authorization": f"Bearer {key}"} if key else {}
            async with s.post(POOL + path, json=body or {}, headers=h) as r:
                ct = r.headers.get("Content-Type", "")
                return r.status, (await r.json() if "json" in ct else await r.text())

        async def get(path, key=None):
            h = {"Authorization": f"Bearer {key}"} if key else {}
            async with s.get(POOL + path, headers=h) as r:
                return r.status, await r.json()

        _, a = await post("/v1/register", body={"name": "alice"})
        _, b = await post("/v1/register", body={"name": "bob"})
        alice, bob = a["api_key"], b["api_key"]
        start = pool_mod.STARTER_CREDITS

        st, _ = await post("/v1/chat/completions", "lp-bogus", {"model": "x"})
        check("bad key -> 401", st == 401, st)

        node = start_node(alice, ["local-qwen=qwen-public"])
        await asyncio.sleep(0.5)
        _, ms = await get("/v1/models")
        check("model listed under public alias", [m["id"] for m in ms["data"]] == ["qwen-public"], ms)

        st, r = await post("/v1/chat/completions", bob, {"model": "qwen-public", "messages": []})
        check("non-stream completion", st == 200 and r["choices"][0]["message"]["content"] == "hello world", r)
        expect = int(40 * pool_mod.PROMPT_WEIGHT + 10 * pool_mod.COMPLETION_WEIGHT)
        _, bm = await get("/v1/me", bob)
        _, am = await get("/v1/me", alice)
        check("client charged", bm["balance"] == start - expect, bm)
        check("node owner paid", am["balance"] == start + expect and am["earned"] == expect, am)

        st, text = await post("/v1/chat/completions", bob,
                              {"model": "qwen-public", "messages": [], "stream": True})
        content = "".join(json.loads(l[5:])["choices"][0]["delta"]["content"]
                          for l in text.splitlines()
                          if l.startswith("data:") and "[DONE]" not in l and '"delta"' in l)
        check("stream completion", st == 200 and content == "hello world" and "[DONE]" in text, text)
        _, bm = await get("/v1/me", bob)
        check("stream billed via usage", bm["balance"] == start - 2 * expect, bm)

        _, am0 = await get("/v1/me", alice)
        await post("/v1/chat/completions", alice, {"model": "qwen-public", "messages": []})
        _, am1 = await get("/v1/me", alice)
        check("self-serve is free", am0["balance"] == am1["balance"], (am0, am1))

        st, _ = await post("/v1/chat/completions", bob, {"model": "nope", "messages": []})
        check("unknown model -> 503", st == 503, st)

        slow = asyncio.create_task(post("/v1/chat/completions", bob,
                                        {"model": "qwen-public", "messages": [], "delay": 1}))
        await asyncio.sleep(0.2)
        st, _ = await post("/v1/chat/completions", bob, {"model": "qwen-public", "messages": []})
        check("busy node (conc=1) -> 503", st == 503, st)
        st, _ = await slow
        check("slow job still completes", st == 200, st)

        node2 = start_node(bob, ["local-qwen=qwen-public"], conc=4)
        await asyncio.sleep(0.5)
        _, ms = await get("/v1/models")
        check("two nodes counted", ms["data"][0]["nodes"] == 2, ms)
        _, am0 = await get("/v1/me", alice)
        await post("/v1/chat/completions", alice, {"model": "qwen-public", "messages": []})
        _, am1 = await get("/v1/me", alice)
        check("router prefers someone else's node", am1["balance"] == am0["balance"] - expect, (am0, am1))

        node.cancel()
        node2.cancel()
        await asyncio.sleep(0.5)
        _, ms = await get("/v1/models")
        check("nodes gone after disconnect", ms["data"] == [], ms)

    print(f"\n{'ALL PASS' if not failures else 'FAILED: ' + ', '.join(failures)}")
    sys.exit(1 if failures else 0)


asyncio.run(main())
