#!/usr/bin/env bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "This script must be run as root." >&2
    exit 1
fi

CONFIG_FILE="/etc/viewtube-env"
MEDIA_ROOT="${MEDIA_ROOT:-/yt}"
WWW_ROOT="${WWW_ROOT:-/www/newtube.com}"
APP_VERSION="${APP_VERSION:-0.1.0}"
HELPER_SCRIPT="$MEDIA_ROOT/viewtube-update-build-run.sh"

mkdir -p "$MEDIA_ROOT" "$WWW_ROOT"

cat <<EOF > "$CONFIG_FILE"
MEDIA_ROOT="$MEDIA_ROOT"
WWW_ROOT="$WWW_ROOT"
APP_VERSION="$APP_VERSION"
EOF

cat <<'SCRIPT' > "$HELPER_SCRIPT"
#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE="/etc/viewtube-env"

if [[ -f "$CONFIG_FILE" ]]; then
    # shellcheck source=/etc/viewtube-env
    . "$CONFIG_FILE"
else
    echo "Missing $CONFIG_FILE; cannot continue." >&2
    exit 1
fi

REPO_URL="https://github.com/Pingasmaster/viewtube.git"
SCREEN_NAME_ROUTINEUPDATE="routineupdate"
SCREEN_NAME_BACKEND="backend"
NGINX_SERVICE="nginx"

export PATH="$PATH:/root/.cargo/bin:/usr/local/bin"

APP_DIR="$WWW_ROOT"

cd "$APP_DIR"

echo "[*] Cloning repo..."
git pull
#rm -rf "$APP_DIR"
#git clone "$REPO_URL" "$APP_DIR"

cd "$APP_DIR"
./cleanup-repo.sh
#CARGO_VERSION=$(grep -m1 '^version' Cargo.toml | sed -E 's/version\s*=\s*"([^"]+)"/\1/')
#if [[ "$APP_VERSION" != "$CARGO_VERSION" ]]; then
#    echo "Detected version change ($APP_VERSION -> $CARGO_VERSION); refreshing ..."
#    cat <<EOF > "$CONFIG_FILE"
#MEDIA_ROOT="$MEDIA_ROOT"
#WWW_ROOT="$WWW_ROOT"
#APP_VERSION="$CARGO_VERSION"
#EOF
#fi
#rm -f cleanup-repo.sh setup-software.sh

echo "[*] Building with cargo (release)..."
cargo build --release
cp target/release/backend target/release/download_channel target/release/routine_update "$MEDIA_ROOT" && cargo clean

echo "[*] Stopping existing screen session for backend (if any)..."
if screen -list | grep -q "\.${SCREEN_NAME_BACKEND}"; then
    screen -S "$SCREEN_NAME_BACKEND" -X quit || true
fi

echo "[*] Stopping existing screen session for routine update (if any)..."
if screen -list | grep -q "\.${SCREEN_NAME_ROUTINEUPDATE}"; then
    screen -S "$SCREEN_NAME_ROUTINEUPDATE" -X quit || true
fi

echo "[*] Starting new screen sessions..."
screen -dmS "$SCREEN_NAME_BACKEND" "$MEDIA_ROOT/backend" --media-root "$MEDIA_ROOT"
screen -dmS "$SCREEN_NAME_ROUTINEUPDATE" "$MEDIA_ROOT/routine_update" --media-root "$MEDIA_ROOT" --www-root "$WWW_ROOT"

echo "[*] Restarting nginx..."
systemctl restart "$NGINX_SERVICE"

echo "[*] Done."
SCRIPT

chmod +x "$HELPER_SCRIPT"

cat <<EOF > /etc/systemd/system/software-updater.service
[Unit]
Description=Update, build (cargo), run software in screen, then restart nginx
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=root
WorkingDirectory=$WWW_ROOT
ExecStart=$HELPER_SCRIPT

# Optional: give it more time for compiling
TimeoutStartSec=3600

[Install]
WantedBy=multi-user.target
EOF

cat <<'EOF' > /etc/systemd/system/software-updater.timer
[Unit]
Description=Run software-updater.service daily

[Timer]
OnCalendar=*-*-* 03:00
Persistent=true
Unit=software-updater.service

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl restart software-updater.service
systemctl enable software-updater.timer
# Check status
#systemctl status software-updater.timer
# Validate when it last/next ran
#systemctl list-timers | grep software-updater
