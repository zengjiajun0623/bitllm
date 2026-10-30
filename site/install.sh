#!/bin/sh
# BitLLM installer for macOS and Linux. Downloads the latest release binary
# for this machine, checks it against SHA256SUMS when the release has one,
# and puts it in ~/.local/bin (override with BITLLM_DIR). No sudo needed.
#
#   curl -fsSL https://bitllm.org/install.sh | sh
set -eu

REPO=zengjiajun0623/bitllm
DIR=${BITLLM_DIR:-$HOME/.local/bin}
BASE=https://github.com/$REPO/releases/latest/download

os=$(uname -s)
arch=$(uname -m)
case "$os" in
  Darwin) os=darwin ;;
  Linux) os=linux ;;
  *) echo "bitllm: unsupported OS $os (on Windows use: irm https://bitllm.org/install.ps1 | iex)" >&2; exit 1 ;;
esac
case "$arch" in
  x86_64 | amd64) arch=amd64 ;;
  arm64 | aarch64) arch=arm64 ;;
  *) echo "bitllm: unsupported CPU $arch" >&2; exit 1 ;;
esac
if [ "$os-$arch" = darwin-amd64 ]; then
  echo "bitllm: Intel Macs have no build yet (Apple silicon only)" >&2; exit 1
fi
name=bitllm-$os-$arch

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
echo "downloading $name"
curl -fsSL -o "$tmp/bitllm" "$BASE/$name"
if curl -fsSL -o "$tmp/SHA256SUMS" "$BASE/SHA256SUMS" 2>/dev/null; then
  want=$(awk -v n="$name" '$2 == n || $2 == "*"n {print $1}' "$tmp/SHA256SUMS")
  got=$( (sha256sum "$tmp/bitllm" 2>/dev/null || shasum -a 256 "$tmp/bitllm") | awk '{print $1}')
  if [ -z "$want" ] || [ "$want" != "$got" ]; then
    echo "bitllm: checksum mismatch for $name, not installing" >&2; exit 1
  fi
  echo "checksum ok"
fi
mkdir -p "$DIR"
chmod +x "$tmp/bitllm"
mv "$tmp/bitllm" "$DIR/bitllm"
"$DIR/bitllm" --version

case ":$PATH:" in
  *":$DIR:"*) cmd=bitllm ;;
  *) cmd="$DIR/bitllm"; echo "note: $DIR is not on your PATH" ;;
esac
echo
echo "Installed. Start it with:  $cmd"
echo "Then open http://127.0.0.1:7778, or read https://bitllm.org/llms.txt (steps for agents)."
