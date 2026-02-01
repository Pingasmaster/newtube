# Newtube

Newtube is a self-hosted YouTube library UI. The Rust backend serves a local media library (videos, shorts, subtitles, thumbnails, comments) and a simple JSON API. Downloads are performed by `yt-dlp`, and the frontend is a static SPA that stores user data locally (likes, playlists, history) with export/import.

Key points:
- Local media only. The UI never streams from YouTube directly; downloads happen ahead of time via `download_channel`.
- User data is localStorage-based with JSON export/import (no accounts).
- All configuration lives in a single `.env` file (source of truth). The Admin page writes back to `.env`.
- The SQLite metadata database is served at `/metadata.db` from `MEDIA_ROOT`.

## Docker Compose (recommended)

This is the default, multi-tier deployment: a frontend (nginx), backend API, and background workers.

1. Copy the example file and edit it:
   ```bash
   cp .env.example .env
   ```
2. Start the stack:
   ```bash
   docker compose up -d
   ```

Services:
- `frontend`: static UI + reverse proxy for `/api/*` and `/metadata.db`.
- `backend`: Rust API server (media + metadata). Not exposed publicly.
- `routine_update`: periodic refresh of all known channels.
- `downloader` (manual profile): on-demand downloads.

Download a channel:
```bash
docker compose run --rm downloader https://www.youtube.com/@LinusTechTips
```

### Configuration (.env)

All runtime configuration is read from `.env`.

Common variables:
- `MEDIA_ROOT`: where media and metadata are stored inside containers.
- `MEDIA_ROOT_HOST`: host path that is mounted to `MEDIA_ROOT`.
- `WWW_ROOT`: path to the static UI inside containers (backend uses this for manual deployments).
- `NEWTUBE_PORT`: backend API port inside the Docker network.
- `NEWTUBE_HOST`: backend bind host (use `0.0.0.0` inside containers).
- `NEWTUBE_PUBLIC_PORT`: host port for the frontend container.
- `NEWTUBE_MISSING_MEDIA_BEHAVIOR`: `404` (default) or `prompt` to show a download prompt.
- `NEWTUBE_DOWNLOAD_BIN`: optional override for the `download_channel` binary path (manual installs).

The Admin UI (`/admin`) updates `NEWTUBE_MISSING_MEDIA_BEHAVIOR` directly inside `.env`.
The Admin page has no authentication; protect it with your reverse proxy if the instance is public.

## Manual install (still supported)

1. Build the binaries:
   ```bash
   cargo build --release
   ```
2. Install them (keep the default names, or set `NEWTUBE_DOWNLOAD_BIN` if you rename):
   ```bash
   sudo install -m 755 target/release/backend /usr/local/bin/backend
   sudo install -m 755 target/release/download_channel /usr/local/bin/download_channel
   sudo install -m 755 target/release/routine_update /usr/local/bin/routine_update
   ```
3. Create a `.env` file in the working directory:
   ```bash
   cat > .env <<'EOF'
   MEDIA_ROOT=/var/lib/newtube/media
   WWW_ROOT=/var/lib/newtube/www
   NEWTUBE_PORT=8080
   NEWTUBE_HOST=0.0.0.0
   NEWTUBE_MISSING_MEDIA_BEHAVIOR=404
   EOF
   ```
4. Copy the frontend assets into `WWW_ROOT`:
   ```bash
   rsync -a --delete index.html app.js pageHome.js pageViewer.js pageAdmin.js userData.js styles.css sw.js Roboto-*.ttf /var/lib/newtube/www/
   ```
5. Run the backend:
   ```bash
   backend
   ```

Download a channel:
```bash
download_channel https://www.youtube.com/@LinusTechTips
```

Refresh all channels:
```bash
routine_update
```

## Reverse proxy examples (manual installs)

### Nginx
```
server {
    listen 80;
    server_name example.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### Apache (httpd)
```
<VirtualHost *:80>
    ServerName example.com

    ProxyPreserveHost On
    ProxyPass / http://127.0.0.1:8080/
    ProxyPassReverse / http://127.0.0.1:8080/
    RequestHeader set X-Forwarded-Proto "http"
</VirtualHost>
```

## Tests

Before running tests, install Node modules:
```bash
npm install
```

- `cargo test` covers the Rust backend (module `metadata.rs`).
- `npm run test` / `npm run test:unit` runs Jest with `fake-indexeddb` and `jsdom`.
- `npm run test:coverage` generates HTML/LCOV under `coverage/jest`.
- `npm run test:e2e` runs Cypress headless on port 4173.

## CI workflows

### GitHub Actions

- `.github/workflows/ci.yml` runs on pushes to `main`/`dev` and on pull requests. It checks Rust formatting/lints/tests and runs the frontend suite via `npm run test`, `npm run test:coverage`, and `npm run test:e2e`, with the Jest coverage uploaded as an artifact.
- `.github/workflows/security.yml` runs on a weekly schedule (Monday 02:00 UTC) or via manual dispatch. It executes `npm audit`, `cargo audit`, and a Trivy filesystem scan, and uploads the Trivy report artifact.

### GitLab CI

- `.gitlab-ci.yml` mirrors the GitHub workflows with `test` and `security` stages. `backend` and `frontend` run on merge requests and on pushes to `main`/`dev`, using the same Rust checks and npm test commands and keeping the Jest coverage as an artifact.
- Security jobs (`npm_audit`, `cargo_audit`, `trivy_scan`) run only on scheduled or manually-triggered pipelines and produce the same audit outputs, including the Trivy report artifact.

## Automating Ansible via CI

We automated deployments from GitLab CI (not github actions, only deviation between the two.):
For each variable, we store the base64 string in a GitLab File variable (Project settings -> CI/CD -> Variables,the file content is the base64).

Example to generate:
- SSH_PRIVATE_KEY_B64: base64 -w 0 ~/.ssh/id_rsa
- SSH_KNOWN_HOSTS_B64: ssh-keyscan -H your.vm.host | base64 -w 0
- DEPLOY_HOST_B64: echo -n "your.vm.host" | base64 -w 0
- DEPLOY_USER_B64: echo -n "ubuntu" | base64 -w 0

GitLab note: the repo ships a `deploy_prod` job in `.gitlab-ci.yml` that decodes base64 “file” variables for SSH (key + known_hosts) and the target host/user, builds an inventory on the fly, and runs the Ansible playbook against a VM in our proxmo>
It’s manual on `main`, so you control when deployments happen.

## Ansible deployment

A minimal Ansible configuration is included in `ansible/`. It installs Docker, clones the repo on a VM, writes the `.env`, and runs `docker compose up -d`.

Quick start:
```bash
cd ansible
cp inventory.example inventory
# edit inventory and group_vars/all.yml
ansible-playbook -i inventory playbook.yml
