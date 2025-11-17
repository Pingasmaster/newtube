# Newtube

A youtube frontend clone, entirely written from the gound up in HTMlL, CSS and javascript to be extra extra fast and almost pixel-perfect with the Youtube UI. Only exceptions are bad UI/UX decisions like the very recent icons and mobile-oriented style. The backend is fully written in safe rust and some bash scripts in order to clone entire youtube channels. When a video from them is first asked by a client, the 

There is no account system, but history and likes/dislikes still work. You can save your cookies via an ID which contains your likes/dislikes/playlists/history and is unique to you so you can erase your cookies and still have the same experience on all your devices. There is also no ad. It also is not in violation of youtube copyright as all icons are taken from material UI and open-licensed, and it does NOT serve videos from youtube directly or indirectly, therefore there is no violation of youtube's TOS as this makes NO calls to youtube.com or any google-owned subdomains.

The Javascript caches pages and loads them only one time via a service worker to have instant subsequent loading times of non video-related assets for maximum speed and responsiveness. Pages are drawn into a container and which is then deleted and recreated when changing pages to keep everything in the same page. Page structure is mainly in the javascript files, which manipulate the HTML in real time.

## Install

1. **Clone and build once:**
   ```bash
   git clone https://github.com/Pingasmaster/newtube.git
   cd newtube
   cargo build --release
   ```
2. **Run the installer with the published public key:**
   ```bash
   sudo ./target/release/installer \
     --domain example.com \
     --trusted-pubkey release-public-key.json
   ```
   The repository now ships `release-public-key.json`, so you no longer have to copy the verifier to every host. The installer copies that key into `<WWW_ROOT>/release-public-key.json` (derived from your config) and all services default to that location—`/etc/newtube-release.pub` is no longer used. Add `--release-repo <owner/repo>` if you are tracking a fork instead of `Pingasmaster/newtube`.
   (Fork maintainers: overwrite that file with your own Ed25519 public key before installing.)
3. **Answer the prompts** (media root defaults to `/yt`, www root to `/www/newtube.com`, backend port `8080`). The installer writes everything to `/etc/newtube-env`, deploys nginx, copies the freshly built binaries to `/opt/newtube/bin`, and enables the systemd services (`newtube-backend`, `newtube-routine`, plus the nightly updater).

From that point on the machine keeps itself current: every night at 03:00 the updater downloads the latest *signed source archive*, verifies it with the bundled public key, rebuilds the Rust binaries locally, swaps `/opt/newtube/bin`, refreshes the static assets under the www root, and restarts the services. No `git pull` or writable shell scripts are involved.

## Automatic updates & signed releases

- **Two release artifacts per tag.** GitHub Actions (see `.github/workflows/release.yml`) produces `newtube-src-<tag>.tar.gz` (full repo tree) and `newtube-bin-<tag>.tar.gz` (prebuilt binaries + static assets). Each archive ships with a `.sig` file containing a BLAKE3 digest and an Ed25519 signature.
- **Only the signed source archive feeds automation.** `installer --auto-update` (and the nightly `software-updater.timer`) downloads the latest source tarball + signature from GitHub Releases, verifies them with `<WWW_ROOT>/release-public-key.json`, rebuilds the binaries locally, replaces `/opt/newtube/bin`, refreshes the static assets, and restarts `newtube-backend` + `newtube-routine`. The binary tarball is there for reproducibility/mirrors but is never executed automatically.
- **Manual/offline updates** use the same verification flow. Download the source tarball and signature and run:
  ```bash
  sudo /opt/newtube/bin/installer \
    --apply-archive \
    --source-archive /tmp/newtube-src-vX.Y.Z.tar.gz \
    --source-signature /tmp/newtube-src-vX.Y.Z.tar.gz.sig \
    --trusted-pubkey release-public-key.json \
    --config /etc/newtube-env
  ```
  The command checks the signature, rebuilds, and restarts everything.
- **BLAKE3 everywhere.** Older SHA-256 digests are gone; signatures now cover `digest` (BLAKE3 hex) plus the version string, so tampering is detected before any compilation step.

## For maintainers and forks

1. **Generate a key pair:** `cargo run --bin installer -- --keygen --key-dir ./release-key`. Overwrite `release-public-key.json` with the contents of `release-key/newtube-release.pub` (the file in the repo is only a placeholder). Keep `newtube-release.key` private.
2. **Expose the private key to CI:** `base64 -w0 release-key/newtube-release.key` and store the result as the `RELEASE_SIGNING_KEY` GitHub secret (the release workflow decodes it into `$RUNNER_TEMP/signing-key.json`).
3. **Publish releases:** tag commits (`git tag v0.3.0 && git push origin v0.3.0`) and create a GitHub Release; `.github/workflows/release.yml` packages the signed source/binary tarballs plus `.sig` files automatically.
4. **Install servers against your repo:** `sudo ./target/release/installer --release-repo yourname/yourfork --trusted-pubkey release-public-key.json ...`. The nightly updater now follows your releases, verifies them with your key, and rebuilds from source locally.

Because the verifying key lives in the repo, bootstrapping a new server is now “clone → run installer → point at `release-public-key.json`”; no more copying keys by hand to every machine.

## Running the Rust backend manually

The installer already builds and copies `backend`, `download_channel`, `routine_update`, and `installer` into `/opt/newtube/bin`, but you can still run them manually if you want to experiment or develop locally:

```bash
git clone https://github.com/Pingasmaster/newtube.git && cd newtube
cargo build --release
./target/release/backend --config /etc/newtube-env --port 9090
```

Each binary loads `MEDIA_ROOT`, `WWW_ROOT`, `NEWTUBE_PORT`, and `RELEASE_REPO` from `/etc/newtube-env` unless you override them with the usual CLI flags (`--config`, `--media-root`, `--www-root`, `--port`, etc.). The installer keeps the services running under systemd, so there is no longer a need for ad-hoc `screen` sessions or writable helper scripts.

`download_channel` still downloads entire channels (videos, Shorts, comments, subtitles, thumbnails) into the media root, and `routine_update` walks the library to refresh every subscribed channel. Both binaries share the same config loader as the backend.

This software needs a `media root` and a `www root` directory, which are used to store youtube videos/shorts/metadata and serve web content respectively. The `www root` is also by default the place where the github will be cloned into by `installer`.

- Videos + muxed formats live under `/yt/videos/<video_id>/`.
- Shorts live under `/yt/shorts/<video_id>/`.
- Thumbnails and subtitles live under `/yt/thumbnails/<video_id>/` and `/yt/subtitles/<video_id>/` respectively.
- The SQLite metadata database resides at `/yt/metadata.db`, website should be served via a nginx reverse proxy pointed to `/www/newtube.com/index.html` which is the app's entry point. 

Example of such a reverse proxy:

```
server {
    listen 80;
    server_name domain.com;

    return 301 https://domain.com$request_uri;
}

server {
    listen 443 ssl;   # match the URL you redirect to
    server_name domain.com;
    http2 on;

    ssl_certificate /etc/letsencrypt/live/domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/domain.com/privkey.pem;
    ssl_prefer_server_ciphers on;

    root /www/newtube.com;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }
}
```

Start or inspect the API server under systemd:

```bash
sudo systemctl status newtube-backend
sudo journalctl -fu newtube-backend   # follow logs
```

If you want to run it in the foreground for debugging, use `./target/release/backend --config /etc/newtube-env --port 8080`. The runtime knobs (`MEDIA_ROOT`, `WWW_ROOT`, `NEWTUBE_PORT`, `RELEASE_REPO`) all live in `/etc/newtube-env` and can still be overridden per command.

## Program reference

Every Rust binary lives under `target/release/`. Unless you pass overrides, they all read `/etc/newtube-env` (written by the installer) to discover `MEDIA_ROOT`, `WWW_ROOT`, and `NEWTUBE_PORT`.

### `installer`

- Purpose: one-stop setup/teardown tool that also enforces the signed-release workflow. It writes `/etc/newtube-env`, deploys nginx, copies binaries into `/opt/newtube/bin`, installs the systemd units (`newtube-backend`, `newtube-routine`, `software-updater.service/.timer`), and verifies every update using the public key at `<WWW_ROOT>/release-public-key.json`. Root is required for install/uninstall/reinstall (only `--cleanup` is non-root).
- Behaviour:
  - Prompts for/creates the media root (stores downloads + metadata) and www root (served by nginx), rebuilds the project, and copies fresh binaries into `/opt/newtube/bin`.
  - Deploys a Let’s Encrypt-friendly nginx config for the supplied domain and reloads nginx automatically.
  - Registers a nightly timer that runs `installer --auto-update`, which downloads the latest signed source tarball, verifies it via BLAKE3+Ed25519, compiles from source locally, and restarts the services.
  - Stores `MEDIA_ROOT`, `WWW_ROOT`, `NEWTUBE_PORT`, `DOMAIN_NAME`, `APP_VERSION`, and `RELEASE_REPO` inside `/etc/newtube-env` so subsequent runs keep the same defaults.
- Useful flags:
  - `-c`, `--cleanup`: delete `node_modules`, `coverage`, stray binaries, and run `cargo clean` in the repo.
  - `-u`, `--uninstall`: remove the systemd units/config; combine with `--reinstall` for a clean reinstall.
  - `-r`, `--reinstall`: uninstall then install again with the same prompts/overrides.
  - `--media-dir`, `--www-dir`, `--port`, `--domain`: override the stored defaults during installation.
  - `--release-repo owner/repo`: trust a different GitHub repo (defaults to `Pingasmaster/newtube`).
  - `--auto-update`: run one update cycle immediately instead of waiting for the nightly timer.
  - `--apply-archive`: verify + apply a local source tarball and signature (no network needed).
  - `--package-release`, `--release-tag`, `--output-dir`, `--signing-key`: build the signed source/binary tarballs used on GitHub Releases (the CI workflow calls this).
- Usage example:
  ```bash
  sudo ./target/release/installer --domain example.com --trusted-pubkey release-public-key.json
  sudo ./target/release/installer --auto-update --trusted-pubkey release-public-key.json
  ```

### `backend`

- Purpose: lightweight Axum HTTP server that exposes `/api/*` routes consumed by the web UI.
- Inputs: reads metadata from `/yt/metadata.db` and streams files out of `/yt` (videos, shorts, subtitles, thumbnails).
- Caching: keeps a read-through cache in memory so hot feeds do not hammer SQLite; restart the process to clear the cache.
- Flags:
  - `--config <path>`: read runtime values from another env file instead of `/etc/newtube-env`.
  - `--media-root <path>`: override `MEDIA_ROOT` for metadata/filesystem lookups.
  - `--port <port>`: override `NEWTUBE_PORT` (defaults to 8080) if you need to bind the Axum server somewhere else.
- Usage example:
  ```bash
  ./backend --config /etc/newtube-env --port 9090
  # -> API server listening on http://0.0.0.0:9090
  ```

### `download_channel`

- Purpose: clones an entire YouTube channel (all versions of each video or Shorts + thumbnail + metadata + subtitles + comments) into the local library and keeps the SQLite database fresh.
- Dependencies: `yt-dlp` must be on the `PATH`, plus optional `cookies.txt` in `/yt` when you need to access members-only/private feeds.
- Behaviour:
  - Creates `/yt/{videos,shorts,subtitles,thumbnails,comments}` as needed.
  - Downloads *all* muxed video formats, subtitles (auto + manual), thumbnails, `.info.json`, `.description`, and the latest ~500 comments per video.
  - Writes/updates `/yt/download-archive.txt` so future runs skip duplicates.
  - Inserts/updates rows inside `/yt/metadata.db` so the backend sees the new content immediately.
- Flags:
  - `--config <path>`: load `MEDIA_ROOT`/`WWW_ROOT` defaults from a specific env file rather than `/etc/newtube-env`.
  - `--media-root <path>` stores media + metadata under a custom directory instead of `/yt`.
  - `--www-root <path>` controls where the static frontend directory is created (defaults to `/www/newtube.com`).
- Usage example:
  ```bash
  ./download_channel --media-root /data/yt --www-root /srv/www https://www.youtube.com/@LinusTechTips
  ```
  The program prints progress for each video, clearly separating long-form uploads and Shorts.

### `routine_update`

- Purpose: cron-friendly helper that re-runs `download_channel` for every channel already present under `/yt`.
- Behaviour:
  - Walks `/yt/videos/**` and `/yt/shorts/**` looking for `<video_id>.info.json` files.
  - Extracts the original `channel_url`/`uploader_url` from those JSON blobs and deduplicates them.
  - Sequentially invokes `download_channel <channel_url>` so each channel gets refreshed with the latest uploads/comments.
- Flags:
  - `--config <path>`: use a different env file for defaults and to forward into the downloader.
  - `--media-root <path>` matches the library root passed to `download_channel`/`backend` (default `/yt`).
  - `--www-root <path>` mirrors the downloader flag; forwarded to each `download_channel` call so the helper can rebuild the same site directory.
- Usage example:
  ```bash
  ./routine_update --config /etc/newtube-env
  ```
  Combine it with a scheduler (cron/systemd timers) to keep your library synced overnight without manual intervention.

All four binaries share the same Rust crate (`newtube_tools`), so adding new metadata fields or config knobs only requires updating the shared structs once.

# Tests

Before runing any tests, you need to run `npm install` to install modules.

`cargo test` covers the Rust backend (module `metadata.rs`)

`npm run test` / `npm run test:unit` : launches Jest with `fake-indexeddb`, `jsdom` and validates front helpers (normalisation vidéo, opérations IndexedDB, API client, stockage user). Les fichiers concernés se trouvent dans `tests/js/*.test.js`

`npm run test:coverage` : même suite Jest que ci-dessus mais enregistre un rapport HTML/LCOV sous `coverage/jest`

`npm run test:e2e` : launches Cypress on port 4173. It now covers **both** `cypress/e2e/home.cy.js` (home grid + sidebar states per desktop/tablet/mobile rules from `cypress/fixtures/bootstrap.json`) and `cypress/e2e/watch.cy.js` (video player metadata, comments rendering and like/dislike/subscription toggles with mocked API responses)
