#!/bin/sh
# Check for root
if [[ $EUID -ne 0 ]]; then
    echo "This script must be run as root." >&2
    exit 1
fi
tee /yt/viewtube-update-build-run.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

CURRENT_APP_VERSION="0.1.0"
REPO_URL="https://github.com/Pingasmaster/viewtube.git"
APP_DIR="/www/newtube.com/"
SCREEN_NAME_ROUTINEUPDATE="routineupdate"
SCREEN_NAME_BACKEND="backend"
NGINX_SERVICE="nginx"

# Make sure PATH knows about cargo (adjust user/path if needed)
export PATH="$PATH:/root/.cargo/bin:/usr/local/bin"

# Clone or update the repo
echo "[*] Cloning repo..."
rm -rf "$APP_DIR"
git clone "$REPO_URL" "$APP_DIR"

cd / && cd "$APP_DIR"
# Remove uneeded files
./cleanup-repo.sh
CARGO_VERSION=$(grep -m1 '^version' Cargo.toml | sed -E 's/version\s*=\s*"([^"]+)"/\1/')
if [[ "$APP_VERSION" != "$CARGO_VERSION" ]]; then
    echo "Versions differ, running setup again..."
    ./setup-software.sh
    exit 0
fi
rm -f cleanup-repo.sh setup-software.sh 

echo "[*] Building with cargo (release)..."
cargo build --release
cp target/release/backend target/release/download_channel target/release/routine_update /yt && cargo clean

echo "[*] Stopping existing screen session for backend (if any)..."
if screen -list | grep -q "\.${SCREEN_NAME_BACKEND}"; then
    screen -S "$SCREEN_NAME_BACKEND" -X quit || true
fi

echo "[*] Stopping existing screen session for routine update (if any)..."
if screen -list | grep -q "\.${SCREEN_NAME_ROUTINEUPDATE}"; then
    screen -S "$SCREEN_NAME_ROUTINEUPDATE" -X quit || true
fi

echo "[*] Starting new screen session..."
screen -dmS "$SCREEN_NAME_BACKEND" /yt/backend
screen -dmS "$SCREEN_NAME_BACKEND" /yt/routine_update

echo "[*] Restarting nginx..."
systemctl restart "$NGINX_SERVICE"

echo "[*] Done."
EOF
tee /etc/systemd/system/software-updater.service <<'EOF'
[Unit]
Description=Update, build (cargo), run software in screen, then restart nginx
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=root
WorkingDirectory=/www/newtube.com/
ExecStart=/yt/viewtube-update-build-run.sh 

# Optional: give it more time for compiling
TimeoutStartSec=3600

[Install]
WantedBy=multi-user.target
EOF
tee /etc/systemd/system/software-updater.timer <<'EOF'
[Unit]
Description=Run software-updater.service daily

[Timer]
OnCalendar=*-*-* 03:00
Persistent=true
Unit=software-updater.service

[Install]
WantedBy=timers.target
EOF
chmod +x /yt/viewtube-update-build-run.sh 
systemctl daemon-reload
systemctl start software-updater.service
systemctl daemon-reload
systemctl enable --now software-updater.timer
# Check status
systemctl status software-updater.timer
# Validate when it last/next ran
systemctl list-timers | grep software-updater
