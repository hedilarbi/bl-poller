#!/usr/bin/env bash
# Install bl-poller on a fresh Ubuntu 24.04 VPS, same layout as the production servers:
#   /opt/chauffeur/bl-poller + venv + Playwright Chromium + systemd service "chauffeur-poller".
# One VPS = one chauffeur: the .env is the same as on the other servers, only
# ASSIGNED_BOT_ID / ASSIGNED_TELEGRAM_ID change. Never assign the same chauffeur to two servers.
#
# Usage, as root on the new server:
#   bash setup_vps.sh <.env copied from another server> <ASSIGNED_BOT_ID> <ASSIGNED_TELEGRAM_ID>
set -euo pipefail

ENV_SRC="${1:?usage: setup_vps.sh <env file> <ASSIGNED_BOT_ID> <ASSIGNED_TELEGRAM_ID>}"
BOT_ID="${2:?missing ASSIGNED_BOT_ID}"
TG_ID="${3:?missing ASSIGNED_TELEGRAM_ID}"
APP=/opt/chauffeur/bl-poller
REPO=https://github.com/hedilarbi/bl-poller.git

[ "$(id -u)" -eq 0 ] || { echo "Run as root"; exit 1; }
[ -f "$ENV_SRC" ] || { echo "Env file not found: $ENV_SRC"; exit 1; }
ENV_SRC="$(realpath "$ENV_SRC")"

echo "==> System packages"
apt-get update -y
apt-get install -y git python3-venv python3-pip curl

echo "==> Code ($REPO, main)"
mkdir -p /opt/chauffeur
[ -d "$APP/.git" ] || git clone "$REPO" "$APP"
cd "$APP"
git pull --ff-only

echo "==> Python venv + dependencies"
python3 -m venv venv
venv/bin/pip install --upgrade pip
venv/bin/pip install -r requirements.txt
venv/bin/python -m playwright install --with-deps chromium   # browser + its system libraries

echo "==> .env (assigned chauffeur: $BOT_ID / $TG_ID)"
cp "$ENV_SRC" .env
for kv in "ASSIGNED_BOT_ID=$BOT_ID" "ASSIGNED_TELEGRAM_ID=$TG_ID"; do
    key="${kv%%=*}"
    if grep -q "^$key=" .env; then sed -i "s|^$key=.*|$kv|" .env; else echo "$kv" >> .env; fi
done
chmod 600 .env

echo "==> EC2 internal API reachable?"
EC2_API_URL="$(grep -E '^EC2_API_URL=' .env | cut -d= -f2- | tr -d '"')"
code="$(curl -s -o /dev/null -m 10 -w '%{http_code}' "$EC2_API_URL/internal/poller/users" || true)"
case "$code" in
    401|403|200) echo "    OK (HTTP $code)" ;;
    *) echo "    WARNING: $EC2_API_URL unreachable (HTTP $code) — check the EC2 firewall" ;;
esac

echo "==> systemd service"
cat > /etc/systemd/system/chauffeur-poller.service <<EOF
[Unit]
Description=Chauffeur Poller
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$APP
EnvironmentFile=$APP/.env
ExecStart=$APP/venv/bin/python main.py
Restart=always
RestartSec=3
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable chauffeur-poller
systemctl restart chauffeur-poller

echo
echo "Done. Logs: journalctl -u chauffeur-poller -f"
echo "Then delete the copied env file: rm $ENV_SRC"
