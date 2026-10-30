# BitLLM installer for Windows (PowerShell). Puts bitllm.exe in
# %LOCALAPPDATA%\bitllm. No admin rights needed.
#
#   irm https://bitllm.org/install.ps1 | iex
$ErrorActionPreference = "Stop"
$dir = Join-Path $env:LOCALAPPDATA "bitllm"
New-Item -ItemType Directory -Force -Path $dir | Out-Null
$exe = Join-Path $dir "bitllm.exe"
$base = "https://github.com/zengjiajun0623/bitllm/releases/latest/download"
Write-Host "downloading bitllm-windows-amd64.exe"
Invoke-WebRequest -UseBasicParsing -OutFile $exe "$base/bitllm-windows-amd64.exe"
# Verify against SHA256SUMS when the release has one.
# Saved to a file: release assets are octet-stream, which PowerShell 5.1 returns as bytes.
$sums = $null
$sumsFile = Join-Path $env:TEMP "bitllm-SHA256SUMS"
try { Invoke-WebRequest -UseBasicParsing -OutFile $sumsFile "$base/SHA256SUMS"; $sums = Get-Content -Raw $sumsFile } catch { }
if ($sums) {
  $want = ($sums -split "`n" | Where-Object { $_ -match "bitllm-windows-amd64\.exe" } | ForEach-Object { ($_ -split "\s+")[0] } | Select-Object -First 1)
  $got = (Get-FileHash -Algorithm SHA256 $exe).Hash.ToLower()
  if (-not $want -or $want -ne $got) { Remove-Item $exe; throw "bitllm: checksum mismatch, not installing" }
  Write-Host "checksum ok"
}
& $exe --version
Write-Host ""
Write-Host "Installed $exe"
Write-Host "Start it with:  & '$exe'"
Write-Host "Then open http://127.0.0.1:7778, or read https://bitllm.org/llms.txt (steps for agents)."
