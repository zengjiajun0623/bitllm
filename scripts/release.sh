#!/usr/bin/env bash
# Build release binaries into dist/. -trimpath keeps local paths (user name,
# toolchain layout) out of the binaries; -s -w drops symbol and debug tables.
#   scripts/release.sh v0.1.5
set -euo pipefail
V="${1:?usage: scripts/release.sh vX.Y.Z}"
cd "$(dirname "$0")/.."
rm -rf dist && mkdir dist
for t in darwin/arm64 linux/amd64 linux/arm64 windows/amd64; do
  os=${t%/*} arch=${t#*/} ext=""
  [ "$os" = windows ] && ext=.exe
  CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags "-s -w -X main.Version=$V" -o "dist/bitllm-$os-$arch$ext" .
done
(cd dist && sha256sum bitllm-* > SHA256SUMS)
if grep -l -a -e "$HOME" -e "$(whoami)@" dist/bitllm-* >/dev/null; then
  echo "local paths leaked into dist/" >&2; exit 1
fi
ls -la dist
