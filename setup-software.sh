#!/usr/bin/env bash
set -euo pipefail

log() {
    printf '[setup] %s\n' "$*"
}

prompt_yes_no() {
    local prompt=$1
    local response
    while true; do
        if ! read -rp "$prompt [y/N]: " response; then
            echo "\nInput aborted." >&2
            exit 1
        fi
        response=${response,,}
        case "$response" in
            y|yes) return 0 ;;
            n|no|"") return 1 ;;
            *) echo "Please answer y or n." ;;
        esac
    done
}

prompt_for_domain() {
    local current_value=${DOMAIN_NAME:-}
    if [[ -n "$current_value" ]]; then
        if prompt_yes_no "Detected existing domain '$current_value'. Keep this value?"; then
            DOMAIN_NAME="$current_value"
            return
        fi
    fi

    while true; do
        local domain_input
        if ! read -rp "Enter the domain name serving Viewtube (e.g. example.com): " domain_input; then
            echo "\nInput aborted." >&2
            exit 1
        fi
        domain_input=$(printf '%s' "$domain_input" | xargs)
        domain_input=${domain_input#https://}
        domain_input=${domain_input#http://}
        domain_input=${domain_input#/}
        domain_input=${domain_input%/}
        domain_input=${domain_input,,}
        if [[ "$domain_input" =~ [[:space:]] ]]; then
            echo "Domain name cannot contain whitespace." >&2
            continue
        fi
        if [[ "$domain_input" == */* ]]; then
            echo "Domain name cannot contain path segments." >&2
            continue
        fi
        if [[ -n "$domain_input" ]]; then
            DOMAIN_NAME="$domain_input"
            break
        fi
        echo "Domain name cannot be empty." >&2
    done
}

detect_package_manager() {
    local managers=(apt-get apt dnf yum pacman apk zypper)
    local mgr
    for mgr in "${managers[@]}"; do
        if command -v "$mgr" >/dev/null 2>&1; then
            echo "$mgr"
            return 0
        fi
    done
    return 1
}

install_nginx_package() {
    local manager
    if ! manager=$(detect_package_manager); then
        echo "Could not detect a supported package manager. Please install nginx manually and rerun." >&2
        exit 1
    fi

    log "Installing nginx via $manager"
    case "$manager" in
        apt-get)
            apt-get update
            apt-get install -y nginx
            ;;
        apt)
            apt update
            apt install -y nginx
            ;;
        dnf)
            dnf install -y nginx
            ;;
        yum)
            yum install -y nginx
            ;;
        pacman)
            pacman -Sy --noconfirm nginx
            ;;
        apk)
            apk update
            apk add nginx
            ;;
        zypper)
            zypper refresh
            zypper install -y nginx
            ;;
        *)
            echo "Package manager $manager is not supported by this installer." >&2
            exit 1
            ;;
    esac

    systemctl enable --now "$NGINX_SERVICE"
}

ensure_nginx_installed() {
    if systemctl list-unit-files --type=service --all | grep -q "^${NGINX_SERVICE}\.service"; then
        log "Detected ${NGINX_SERVICE}.service"
        return
    fi

    log "${NGINX_SERVICE}.service not detected."
    if prompt_yes_no "Install nginx now"; then
        install_nginx_package
    else
        echo "nginx is required for this setup. Aborting." >&2
        exit 1
    fi
}

deploy_nginx_config() {
    local config_path symlink_path
    if [[ -d /etc/nginx/sites-available ]]; then
        config_path="/etc/nginx/sites-available/viewtube.conf"
        symlink_path="/etc/nginx/sites-enabled/viewtube.conf"
    else
        config_path="/etc/nginx/conf.d/viewtube.conf"
        symlink_path=""
    fi

    local action="create"
    if [[ -f "$config_path" ]]; then
        action="replace"
    fi

    if ! prompt_yes_no "Deploy the recommended nginx config to $config_path (will $action existing content)"; then
        log "Skipping nginx config deployment"
        return
    fi

    install -d "$(dirname "$config_path")"
    cat <<EOF > "$config_path"
server {
    listen 80;
    listen [::]:80;
    server_name $DOMAIN_NAME;

    return 301 https://$DOMAIN_NAME\$request_uri;
}

server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name $DOMAIN_NAME;

    ssl_certificate /etc/letsencrypt/live/$DOMAIN_NAME/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/$DOMAIN_NAME/privkey.pem;
    ssl_prefer_server_ciphers on;

    root $WWW_ROOT;
    index index.html;

    location / {
        try_files \$uri \$uri/ /index.html;
    }
}
EOF

    if [[ -n "$symlink_path" ]]; then
        install -d "$(dirname "$symlink_path")"
        ln -sf "$config_path" "$symlink_path"
    fi

    if command -v nginx >/dev/null 2>&1; then
        nginx -t
        systemctl reload "$NGINX_SERVICE"
    fi

    log "Deployed nginx config to $config_path"
}

if [[ $EUID -ne 0 ]]; then
    echo "This script must be run as root." >&2
    exit 1
fi

CONFIG_FILE="/etc/viewtube-env"
NGINX_SERVICE="nginx"

if [[ -f "$CONFIG_FILE" ]]; then
    # shellcheck disable=SC1091
    . "$CONFIG_FILE"
fi

MEDIA_ROOT="${MEDIA_ROOT:-/yt}"
WWW_ROOT="${WWW_ROOT:-/www/newtube.com}"
APP_VERSION="${APP_VERSION:-0.1.0}"
DOMAIN_NAME="${DOMAIN_NAME:-}"
HELPER_SCRIPT="$MEDIA_ROOT/viewtube-update-build-run.sh"

prompt_for_domain
log "Using domain: $DOMAIN_NAME"

log "Creating MEDIA_ROOT ($MEDIA_ROOT) and WWW_ROOT ($WWW_ROOT)"
mkdir -p "$MEDIA_ROOT" "$WWW_ROOT"

ensure_nginx_installed
deploy_nginx_config

log "Writing config to $CONFIG_FILE"
cat <<EOF > "$CONFIG_FILE"
MEDIA_ROOT="$MEDIA_ROOT"
WWW_ROOT="$WWW_ROOT"
APP_VERSION="$APP_VERSION"
DOMAIN_NAME="$DOMAIN_NAME"
EOF

log "Writing helper script to $HELPER_SCRIPT"
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

echo "[*] Syncing repo..."
if [[ -d "$APP_DIR/.git" ]]; then
    if ! git -C "$APP_DIR" pull; then
        echo "[!] git pull failed; recloning fresh copy..."
        rm -rf "$APP_DIR"
        git clone "$REPO_URL" "$APP_DIR"
    fi
else
    git clone "$REPO_URL" "$APP_DIR"
fi

cd "$APP_DIR"
./cleanup-repo.sh
CARGO_VERSION=$(grep -m1 '^version' Cargo.toml | sed -E 's/version\s*=\s*"([^"]+)"/\1/')
if [[ "$APP_VERSION" != "$CARGO_VERSION" ]]; then
    echo "Detected version change ($APP_VERSION -> $CARGO_VERSION); refreshing ..."
    cat <<EOF > "$CONFIG_FILE"
MEDIA_ROOT="$MEDIA_ROOT"
WWW_ROOT="$WWW_ROOT"
APP_VERSION="$CARGO_VERSION"
DOMAIN_NAME="$DOMAIN_NAME"
EOF
    SETUP_SCRIPT_PATH="$APP_DIR/setup-software.sh"
    if [[ -x "$SETUP_SCRIPT_PATH" ]]; then
        echo "[*] Re-running setup from updated repo..."
        APP_VERSION="$CARGO_VERSION" exec "$SETUP_SCRIPT_PATH"
    else
        echo "Missing $SETUP_SCRIPT_PATH; cannot re-run setup." >&2
        exit 1
    fi
fi
rm -f cleanup-repo.sh setup-software.sh

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

log "Installing systemd service: software-updater.service"
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

log "Installing systemd timer: software-updater.timer"
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

log "Reloading systemd units"
systemctl daemon-reload

log "Enabling and starting software-updater.timer"
systemctl enable --now software-updater.timer

log "Running initial helper script (this may take a while; see output below)"
"$HELPER_SCRIPT"

log "software-updater.timer status"
systemctl status software-updater.timer || true

log "Upcoming runs (systemctl list-timers)"
systemctl list-timers | grep software-updater || true

log "Setup complete"
