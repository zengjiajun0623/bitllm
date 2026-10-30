# 加入 BitLLM

BitLLM 是一个本地大模型的点对点网络：把你自己跑的模型分享出来，也能用到别人分享的模型。没有中心服务器，不用做端口转发，不用注册账号。

## 1. 下载

复制对应系统的一行命令，下载、加执行权限、运行一步到位：

```bash
# Mac（Apple 芯片）
curl -fL -o bitllm https://github.com/zengjiajun0623/bitllm/releases/latest/download/bitllm-darwin-arm64 && chmod +x bitllm && ./bitllm
# Linux x86（ARM 机器比如 DGX Spark 用 bitllm-linux-arm64）
curl -fL -o bitllm https://github.com/zengjiajun0623/bitllm/releases/latest/download/bitllm-linux-amd64 && chmod +x bitllm && ./bitllm
# Windows（PowerShell）
iwr -OutFile bitllm.exe https://github.com/zengjiajun0623/bitllm/releases/latest/download/bitllm-windows-amd64.exe; .\bitllm.exe
```

然后浏览器打开 http://127.0.0.1:7778 ，选一个模型，复制给你的 agent 用的配置，点"发送测试消息"确认能用。用 curl 下载不会被 Mac 隔离，不会弹"无法验证开发者"。

下面的命令里 `./bitllm` 在 Windows 上换成 `.\bitllm.exe`。

## 2. 分享你的 DeepSeek V4.1 Flash 1017

假设你用 vLLM 或 SGLang 在本机 8000 端口跑模型，先查一下它对外叫什么名字：

```bash
curl -s localhost:8000/v1/models     # 看 "id" 字段，比如 "deepseek-v4.1-flash"
```

然后把这个名字映射成网络里的统一名字：

```bash
./bitllm --upstream http://localhost:8000/v1 \
  --model deepseek-v4.1-flash=deepseek-v4.1-flash-1017 \
  --concurrency 2
```

- 等号左边是你本地的名字，右边是网络统一名字。**官方权重一律用 `deepseek-v4.1-flash-1017`**，去审查版用 `deepseek-v4.1-flash-1017-heretic`。名字不对，别人就找不到你。
- `--concurrency` 是同时接几个请求，按你机器的余量填。

看到 `sharing map[...]` 和 `gateway http://127.0.0.1:7778/v1` 就说明接上了。

## 3. 在 agent 里用网络里的模型

浏览器打开 `http://127.0.0.1:7778`，页面上有 Claude Code、OpenAI 兼容工具、pi 的现成配置，点复制就行，还有一个"测试连接"按钮。

本机接口同时支持 OpenAI 格式（`/v1/chat/completions`）和 Anthropic 格式（`/v1/messages`），所以 Claude Code 可以直接接：

```bash
M=deepseek-v4.1-flash-1017
ANTHROPIC_BASE_URL=http://127.0.0.1:7778 ANTHROPIC_API_KEY=bitllm ANTHROPIC_MODEL=$M \
ANTHROPIC_DEFAULT_OPUS_MODEL=$M ANTHROPIC_DEFAULT_SONNET_MODEL=$M ANTHROPIC_DEFAULT_HAIKU_MODEL=$M \
CLAUDE_CODE_SUBAGENT_MODEL=$M claude
```

Cline、Continue、OpenCode、Aider 等 OpenAI 兼容工具：Base URL 填 `http://127.0.0.1:7778/v1`，API key 随便填。命令行试一下：

```bash
curl localhost:7778/v1/models        # 网络里现在有哪些模型、各有几个节点
curl localhost:7778/v1/chat/completions \
  -d '{"model":"deepseek-v4.1-flash-1017","stream":true,"messages":[{"role":"user","content":"你好"}]}'
```

只想用不想分享，直接运行 `./bitllm` 不带参数即可。自己分享的模型会优先由自己的机器回答，不出本机。

## 4. 访问规则（邀请制内测，PT 站式规则）

1. 有邀请才有免费额度。没被邀请的节点，只在服务方空闲时才会被服务。
2. 会员在每台服务节点上每天有免费额度（服务方自己设置，默认每天 200,000 积分）。
3. 分享算力就是贡献者：不限额度，邀请名额从 3 个变成 10 个。

会员 = 根账户，或者被会员邀请的账户。你的节点只要服务过一次任务，就永久拥有 10 个邀请名额；最近 7 天内服务过任务，就不受每日额度限制。根账户可以撤销一个账户，被撤销账户邀请进来的人也会一起失去资格（之后可以由别的会员重新邀请）。邀请由以太坊账户签名后全网广播，没有中心化的会员服务器。

怎么加入：把你的以太坊地址发给一位会员，请他邀请你，然后在控制台把你的节点关联到这个地址（见下面"积分"一节）。

邀请别人：用浏览器钱包的话，在控制台的 Invite 框里填对方地址；用助记词账户的话：

```bash
./bitllm invite 0x对方地址
```

## 5. 积分

你每帮别人跑一次推理，就得到一张双方签名的收据，记积分（prompt token ÷ 4 + 生成 token）。收据会广播到全网（收据里的客户端用的是每个服务节点一个、每 30 天更换的化名，不会暴露你是谁），任何人都能验证：

```bash
curl localhost:7778/credits
```

积分是记在节点上的。想把自己几台机器的积分合到一个账户下，就把每个节点关联到同一个以太坊地址。这只是签一个名：不发交易、不花 gas、不上链。

```bash
./bitllm account new --name 你的名字     # 生成新账户（12 个助记词，务必抄下来），并关联这台机器
./bitllm account link --name 你的名字    # 在另一台机器上，输入同一组助记词关联
./bitllm account                        # 查看这台机器归谁
```

也可以在控制台页面点"Link wallet"，用浏览器钱包（MetaMask、Rabby 等）签名关联。助记词就是账户本身，丢了就找不回来，别人拿到就能冒充你。

积分以后有什么用还没定。定之前会先公布计分规则：自己开小号刷自己、或者冒充模型的收据不算。

## 需要知道的

- **帮你跑推理的节点能看到你的 prompt 明文**（传输是加密的，但对方机器要读内容才能推理）。不要发敏感信息。
- 和你直连的节点能看到你的 IP，帮你推理的节点能看到你的节点 ID。在意的话，通过 VPN 使用。
- 你分享模型时，你服务的客户端也能看到你的 IP。
- 收据用每次推理临时生成的密钥签名，全网公开的记录里只有"哪个节点干了多少活"，没有"谁问的"。
- 目前还没有防作弊验证，节点理论上可以用别的模型冒充。
- 身份和账本存在 `~/.bitllm/`，换机器想保留积分就把整个目录带走。

有问题在群里问，或者在 GitHub 提 issue。
