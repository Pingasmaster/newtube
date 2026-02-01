# Newtube

## Docker Compose (recommended)

This is the recommended method to serve the web UI and have the complete software stack.

1. **Create a `.env` file** in this folder (same as the example below).
2. **Run it:**
   ```bash
   docker compose up -d
   ```

After the `.env` is set, `docker compose up -d` is the only command you need.

Example `.env`:
```bash
cat > .env <<'EOF'
MEDIA_ROOT="/data/media"
WWW_ROOT="/app/www"
NEWTUBE_PORT="8080"
NEWTUBE_HOST="0.0.0.0"
NEWTUBE_MISSING_MEDIA_BEHAVIOR="404"
EOF
```

`NEWTUBE_MISSING_MEDIA_BEHAVIOR` accepts `404` (default) or `prompt` to show an in-app download prompt for missing videos.

The compose stack serves the SPA + `/api/*` on port 8080 and keeps channels fresh in the background.

Admin settings live at `/admin` (no auth by default; protect it with your reverse proxy if needed).

A youtube frontend clone, entirely written from the gound up in HTML, CSS and javascript to be extra extra fast and almost pixel-perfect with the Youtube UI. Only exceptions are bad UI/UX decisions like the very recent icons and mobile-oriented style. The backend is fully written in safe rust and some bash scripts in order to clone entire youtube channels. When a video from them is first asked by a client, the backend downloads the entire channels videos.

There is no account system, but history and likes/dislikes still work. You can save your cookies via an ID which contains your likes/dislikes/playlists/history and is unique to you so you can erase your cookies and still have the same experience on all your devices. There is also no ad. It also is not in violation of youtube copyright as all icons are taken from material UI and open-licensed, and it does NOT serve videos from youtube directly or indirectly, therefore there is no violation of youtube's TOS as this makes NO calls to youtube.com or any google-owned subdomains.

The Javascript caches pages and loads them only one time via a service worker to have instant subsequent loading times of non video-related assets for maximum speed and responsiveness. Pages are drawn into a container and which is then deleted and recreated when changing pages to keep everything in the same page. Page structure is mainly in the javascript files, which manipulate the HTML in real time.

## Install (manual / old school)

1. **Clone and build:**
   ```bash
   git clone https://github.com/Pingasmaster/newtube.git
   cd newtube
   cargo build --release
   ```
2. **Copy the binaries where you want to run them:**
   ```bash
   sudo install -m 755 target/release/backend /usr/local/bin/newtube-backend
   sudo install -m 755 target/release/download_channel /usr/local/bin/newtube-download
   sudo install -m 755 target/release/routine_update /usr/local/bin/newtube-routine
   ```
3. **Create a `.env` file** in the working directory you will run the binaries from:
   ```bash
   cat > .env <<'EOF'
   MEDIA_ROOT="/var/lib/newtube/media"
   WWW_ROOT="/var/lib/newtube/www"
NEWTUBE_PORT="8080"
NEWTUBE_HOST="0.0.0.0"
NEWTUBE_MISSING_MEDIA_BEHAVIOR="404"
EOF
```
4. **Place the web UI files in `WWW_ROOT`:**
   ```bash
   rsync -a --delete index.html app.js pageHome.js pageViewer.js pageAdmin.js userData.js styles.css sw.js Roboto-*.ttf /var/lib/newtube/www/
   ```

### Run the backend (serves UI + API)

```bash
newtube-backend
```

The backend serves the SPA and the `/api/*` endpoints from the same HTTP port.

### Download a channel

```bash
newtube-download https://www.youtube.com/@LinusTechTips
```

### Refresh all channels

```bash
newtube-routine
```

Schedule `newtube-routine` with cron/systemd if you want nightly content refreshes.

## Docker (single container)

Build and run:

```bash
docker build -t newtube .
docker run --rm -p 8080:8080 -v /srv/newtube/media:/data/media newtube
```

Notes:
- The image bundles the static web UI; only the media library needs a volume.
- The container runs as a non-root user (uid 10001). Ensure the media volume is readable by that uid.
- To override settings, mount your own `.env` at `/app/.env` or pass environment variables.

## Docker Compose (details / extras)

A `docker-compose.yml` is included. With a `.env` in this folder:

```bash
docker compose up -d
```

This brings up:
- `backend`: serves the UI + API on port 8080.
- `routine_update`: runs `routine_update` in a loop for periodic refreshes.
- `downloader` (profile `manual`): on-demand channel downloads.

Download a channel via compose:

```bash
docker compose run --rm downloader https://www.youtube.com/@LinusTechTips
```

## Reverse proxy examples

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

## Program reference

Every Rust binary is produced by `cargo build --release`. The binaries read `.env` from the current working directory and use its values unless you override via flags.

### `backend`

- Purpose: Axum HTTP server that serves the web UI and `/api/*` routes.
- Flags:
  - `--media-root <path>`: override `MEDIA_ROOT`.
  - `--www-root <path>`: override `WWW_ROOT`.
  - `--port <port>`: override `NEWTUBE_PORT`.
  - `--host <ip>`: override `NEWTUBE_HOST`.
- Usage example:
  ```bash
  newtube-backend --port 9090
  ```

### `download_channel`

- Purpose: clones an entire YouTube channel (videos, Shorts, comments, subtitles, thumbnails) into the local library and keeps the SQLite database fresh.
- Dependencies: `yt-dlp` must be on the `PATH`, plus optional `cookies.txt` in `MEDIA_ROOT` when you need to access members-only/private feeds.
- Behaviour:
  - Creates `{MEDIA_ROOT}/{videos,shorts,subtitles,thumbnails,comments}` as needed.
  - Downloads muxed video formats, subtitles (auto + manual), thumbnails, `.info.json`, `.description`, and the latest ~500 comments per video.
  - Writes/updates `{MEDIA_ROOT}/download-archive.txt` so future runs skip duplicates.
  - Inserts/updates rows inside `{MEDIA_ROOT}/metadata.db` so the backend sees the new content immediately.
- Flags:
  - `--media-root <path>`: store media + metadata under a custom directory.
  - `--www-root <path>`: controls where the static frontend directory lives.
- Usage example:
  ```bash
  newtube-download --media-root /data/yt --www-root /srv/www https://www.youtube.com/@LinusTechTips
  ```

### `routine_update`

- Purpose: cron-friendly helper that re-runs `download_channel` for every channel already present under `MEDIA_ROOT`.
- Behaviour:
  - Walks `{MEDIA_ROOT}/videos/**` and `{MEDIA_ROOT}/shorts/**` looking for `<video_id>.info.json` files.
  - Extracts the original `channel_url`/`uploader_url` from those JSON blobs and deduplicates them.
  - Sequentially invokes `download_channel <channel_url>` so each channel gets refreshed with the latest uploads/comments.
- Flags:
  - `--media-root <path>`: matches the library root passed to `download_channel`/`backend`.
  - `--www-root <path>`: forwarded to each `download_channel` call so the helper can rebuild the same site directory.
- Usage example:
  ```bash
  newtube-routine
  ```

All binaries share the same Rust crate (`newtube_tools`), so adding new metadata fields or config knobs only requires updating the shared structs once.

# Tests

Before runing any tests, you need to run `npm install` to install modules.

`cargo test` covers the Rust backend (module `metadata.rs`)

`npm run test` / `npm run test:unit` : launches Jest with `fake-indexeddb`, `jsdom` and validates front helpers (normalisation vidéo, opérations IndexedDB, API client, stockage user). Les fichiers concernés se trouvent dans `tests/js/*.test.js`

`npm run test:coverage` : même suite Jest que ci-dessus mais enregistre un rapport HTML/LCOV sous `coverage/jest`

`npm run test:e2e` : launches Cypress on port 4173. It now covers **both** `cypress/e2e/home.cy.js` (home grid + sidebar states per desktop/tablet/mobile rules from `cypress/fixtures/bootstrap.json`) and `cypress/e2e/watch.cy.js` (video player metadata, comments rendering and like/dislike/subscription toggles with mocked API responses)
