#!/usr/bin/env bash
# Turn a fresh Ubuntu VM with a public IP into a BitLLM bootstrap + relay node.
#
#   curl -fsSL https://raw.githubusercontent.com/zengjiajun0623/bitllm/main/deploy/bootstrap-vps.sh | sudo bash
#   (or: sudo bash bootstrap-vps.sh [version], default: latest release)
#
# Oracle Cloud: the VM firewall is not the only one. Also open 4001 in the
# subnet's security list (VCN > Subnet > Security List > Ingress rules):
#   source 0.0.0.0/0, TCP, destination port 4001
#   source 0.0.0.0/0, UDP, destination port 4001
# Other clouds (Hetzner, Vultr, DO...) have a similar cloud firewall; open the same two.
set -euo pipefail

VERSION="${1:-latest}"
PORT=4001
case "$(uname -m)" in
  x86_64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "unsupported arch $(uname -m)" >&2; exit 1 ;;
esac
[ "$(id -u)" = 0 ] || { echo "run as root (sudo)" >&2; exit 1; }

if [ "$VERSION" = latest ]; then
  URL="https://github.com/zengjiajun0623/bitllm/releases/latest/download/bitllm-linux-$ARCH"
else
  URL="https://github.com/zengjiajun0623/bitllm/releases/download/$VERSION/bitllm-linux-$ARCH"
fi
echo "downloading $URL"
curl -fsSL -o /usr/local/bin/bitllm.new "$URL"
chmod 755 /usr/local/bin/bitllm.new
mv /usr/local/bin/bitllm.new /usr/local/bin/bitllm
/usr/local/bin/bitllm --version

id bitllm >/dev/null 2>&1 || useradd --system --home-dir /var/lib/bitllm --create-home --shell /usr/sbin/nologin bitllm

cat > /etc/systemd/system/bitllm-boot.service <<EOF
[Unit]
Description=BitLLM bootstrap and relay node
After=network-online.target
Wants=network-online.target

[Service]
User=bitllm
ExecStart=/usr/local/bin/bitllm --dht-server --port $PORT --mdns=false --upnp=false --data /var/lib/bitllm --gateway 127.0.0.1:7778
Restart=always
RestartSec=5
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/bitllm
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

# Host firewall. Oracle's Ubuntu images ship iptables rules that drop most inbound traffic.
if command -v ufw >/dev/null && ufw status | grep -q "Status: active"; then
  ufw allow $PORT/tcp
  ufw allow $PORT/udp
elif command -v iptables >/dev/null; then
  for proto in tcp udp; do
    iptables -C INPUT -p $proto --dport $PORT -j ACCEPT 2>/dev/null || iptables -I INPUT -p $proto --dport $PORT -j ACCEPT
    ip6tables -C INPUT -p $proto --dport $PORT -j ACCEPT 2>/dev/null || ip6tables -I INPUT -p $proto --dport $PORT -j ACCEPT
  done
  if command -v netfilter-persistent >/dev/null; then netfilter-persistent save; fi
fi

systemctl daemon-reload
systemctl enable --now bitllm-boot
sleep 4

ID=$(journalctl -u bitllm-boot -n 50 --no-pager | grep -o 'peer id [^ ]*' | tail -1 | awk '{print $3}')
IP4=$(curl -fsS4 --max-time 5 https://api.ipify.org || true)
IP6=$(curl -fsS6 --max-time 5 https://api6.ipify.org || true)
echo
echo "bootstrap running, peer id $ID"
echo "back up /var/lib/bitllm/identity.key: the peer id is baked into clients"
echo
if [ -z "$IP4" ]; then
  IP4=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1;i<NF;i++) if ($i=="src") print $(i+1)}' || true)
  echo "could not look up the public IP. ${IP4:+$IP4 is the address of this machine; on Oracle and most clouds it is private, so use the public IP from the cloud console instead.}"
  echo
fi
echo "add these to DefaultBootstrap in main.go (before the boot.bitllm.org entry):"
if [ -n "$IP4" ]; then
  echo "  \"/ip4/$IP4/tcp/$PORT/p2p/$ID\","
  echo "  \"/ip4/$IP4/udp/$PORT/quic-v1/p2p/$ID\","
fi
if [ -n "$IP6" ]; then
  echo "  \"/ip6/$IP6/tcp/$PORT/p2p/$ID\","
  echo "  \"/ip6/$IP6/udp/$PORT/quic-v1/p2p/$ID\","
fi
echo
echo "check from another machine: bitllm --bootstrap /ip4/${IP4:-PUBLIC_IP}/tcp/$PORT/p2p/$ID"
