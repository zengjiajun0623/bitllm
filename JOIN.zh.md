# 加入 BitLLM

BitLLM 是一个本地大模型的点对点网络：把你自己跑的模型分享出来，也能用到别人分享的模型。没有中心服务器，不用做端口转发，不用注册账号。

## 1. 下载

在 [Releases](../../releases) 下载对应系统的程序：

- Mac（Apple 芯片）：`bitllm-darwin-arm64`
- Linux：`bitllm-linux-amd64`（x86）或 `bitllm-linux-arm64`（ARM，例如 DGX Spark）
- Windows：`bitllm-windows-amd64.exe`

```bash
chmod +x bitllm-*      # Mac / Linux
```

Mac 第一次运行如果提示"无法验证开发者"，执行一次：`xattr -d com.apple.quarantine bitllm-darwin-arm64`

## 2. 分享你的 DeepSeek V4.1 Flash 1017

假设你用 vLLM 或 SGLang 在本机 8000 端口跑模型，先查一下它对外叫什么名字：

```bash
curl -s localhost:8000/v1/models     # 看 "id" 字段，比如 "deepseek-v4.1-flash"
```

然后把这个名字映射成网络里的统一名字：

```bash
./bitllm-linux-arm64 --upstream http://localhost:8000/v1 \
  --model deepseek-v4.1-flash=deepseek-v4.1-flash-1017 \
  --concurrency 2
```

- 等号左边是你本地的名字，右边是网络统一名字。**官方权重一律用 `deepseek-v4.1-flash-1017`**，去审查版用 `deepseek-v4.1-flash-1017-heretic`。名字不对，别人就找不到你。
- `--concurrency` 是同时接几个请求，按你机器的余量填。

看到 `sharing map[...]` 和 `gateway http://127.0.0.1:7778/v1` 就说明接上了。

## 3. 用网络里的模型

程序在本机开了一个 OpenAI 兼容接口 `http://127.0.0.1:7778/v1`，任何支持 OpenAI 接口的工具都能直接用（API key 随便填）：

```bash
curl localhost:7778/v1/models        # 网络里现在有哪些模型、各有几个节点
curl localhost:7778/v1/chat/completions \
  -d '{"model":"deepseek-v4.1-flash-1017","stream":true,"messages":[{"role":"user","content":"你好"}]}'
```

只想用不想分享，直接运行 `./bitllm-...` 不带参数即可。自己分享的模型会优先由自己的机器回答，不出本机。

## 4. 积分

你每帮别人跑一次推理，就得到一张双方签名的收据，记积分（prompt token ÷ 4 + 生成 token）。收据会广播到全网，任何人都能验证：

```bash
curl localhost:7778/credits
```

积分以后有什么用还没定。定之前会先公布计分规则：自己开小号刷自己、或者冒充模型的收据不算。

## 需要知道的

- **帮你跑推理的节点能看到你的 prompt 明文**（传输是加密的，但对方机器要读内容才能推理）。不要发敏感信息。
- 和你直连的节点能看到你的 IP。
- 目前还没有防作弊验证，节点理论上可以用别的模型冒充。
- 身份和账本存在 `~/.bitllm/`，换机器想保留积分就把整个目录带走。

有问题在群里问，或者在 GitHub 提 issue。
