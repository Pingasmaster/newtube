# Viewtube

A youtube frontend clone, entirely written from the gound up in HTMlL, CSS and javascript to be extra extra fast and almost pixel-perfect with the Youtube UI. Only exceptions are bad UI/UX decisions like the very recent icons and mobile-oriented style. The backend is fully written in safe rust and some bash scripts in order to clone entire youtube channels. When a video from them is first asked by a client, the 

There is no account system, but history and likes/dislikes still work. You can save your cookies via an ID which contains your likes/dislikes/playlists/history and is unique to you so you can erase your cookies and still have the same experience on all your devices. There is also no ad. It also is not in violation of youtube copyright as all icons are taken from material UI and open-licensed, and it does NOT serve videos from youtube directly or indirectly, therefore there is no violation of youtube's TOS as this makes NO calls to youtube.com or any google-owned subdomains.

The Javascript caches pages and loads them only one time via a service worker to have instant subsequent loading times of non video-related assets for maximum speed and responsiveness. Pages are drawn into a container and which is then deleted and recreated when changing pages to keep everything in the same page. Page structure is mainly in the javascript files, which manipulate the HTML in real time.

## Install

A one-liner will install everything you need, including auto-update scripts, and launch the backend. You just have to wait for the users to come and it will start downloading content automatically or download things yourself using `/yt/download_channel https://youtube.com/@LinustechTips`.

```bash
git clone https://github.com/Pingasmaster/viewtube.git && cd viewtube && cargo build --release && sudo ./target/release/installer && rm -rf ./viewtube
```

This software needs a `media root` and a `www root` directory, it will ask you where you want them while you install the software. By default they are `/yt/` for the media and `/www/newtube.com` for `www root`. During the same prompt session the installer also asks which TCP port the backend should listen on (stored as `NEWTUBE_PORT`, default `8080`). All three answers live in the default config file `/etc/viewtube-env` so future runs automatically pick them up.

Nginx is installed if it's not already and the correct config for the website is automatically put there when you run the `./installer`.

## Using the Rust Backend

Compile and get the binaries in the current directory (change `MEDIA_ROOT`/`WWW_ROOT`/`NEWTUBE_PORT` in `/etc/viewtube-env` *before* running the `setup-script.sh` helper if you want something else than `/yt` + `/www/newtube.com` + `8080`):

To compile manually:

```bash
# Clone and build
git clone https://github.com/Pingasmaster/viewtube.git && cd viewtube
cargo build --release
# Copy needed executables under /yt/ (or your media root directory).
cp target/release/installer target/release/backend target/release/download_channel target/release/routine_update /yt/
```

`installer` can be used to install, uninstall, reinstall (manual forced update), and clean the www root of build artifacts. It is meant to run once, at the first install, and then never again except if you need to clean the www-root directory and remove junk files made by a manual build maybe.

It checks if you have nginx and screen installed, prompt to install them if not, and puts the good nginx config in place if you wish (it asks for the domain name). It then clones the repo to the `www root` and installs a systemd service for the updater, which is a bash script that pulls the git repo under `www root` and sees if theres any update, if so it rebuilds the binaries and replace them and changes the software version in the config file. Its run at 3AM every single day. It also runs `routine_update` to download any new content from any channel already downloaded.

`backend` is the backend api. Takes things under the media root directory (/yt/ by default). It's automatically run in the background by the command `screen` if you used the installer. You can also run it manually; by default it reads `MEDIA_ROOT`, `WWW_ROOT`, and `NEWTUBE_PORT` from `/etc/viewtube-env` (override with `--config`, `--media-root`, or `--port` if needed).

`download_channel` takes a youtube channel full url and downloads every single video and short from that channel. It also downloads the comments of these videos alongside metadata and subtitles. Like the backend, it prefers loading `MEDIA_ROOT`/`WWW_ROOT` from `/etc/viewtube-env` unless you explicitly pass overrides.

`routine_update` takes every single channel you already downloaded and retries to download them all, but remembers thanks to an archive what videos were already downloaded. Theres a metadata update mode which only redownloads metadata and subtitles and comments from a video which you can trigger manually. Right now the metadata mode is never trigger automatically. The binary now shares the same `--config` parsing logic so it picks up the exact same directories that the downloader/backends use.

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

Start the API server:

The runtime knobs are the port of the api and the directories www root and media root which can be customized upon first `./installer` run or in `/etc/viewtubeconfig`

```
screen -S "backend" ./backend
```

CTRL+A and CTRL+D to exit.

The software is not meant to be run manually like this though. A simple execution of ./setup-script.sh will get you up and running.

## Bakend implementation details

### `backend`

- Purpose: lightweight Axum HTTP server that exposes `/api/*` routes consumed by the web UI.
- Inputs: reads metadata from `/yt/metadata.db` and streams files out of `/yt` (videos, shorts, subtitles, thumbnails).
- Caching: keeps a read-through cache in memory so hot feeds do not hammer SQLite; restart the process to clear the cache.
- Flags:
  - `--media-root <path>` overrides the default `/yt` library root (the metadata database is read from `<path>/metadata.db`).
- Usage example:
  ```bash
  ./backend
  # -> API server listening on http://0.0.0.0:8080
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
  - `--media-root <path>` stores media + metadata under a custom directory instead of `/yt`.
  - `--www-root <path>` controls where the static frontend directory is created (defaults to `/www/newtube.com`).
- Usage example:
  ```bash
  ./download_channel https://www.youtube.com/@LinusTechTips
  ```
  The program prints progress for each video, clearly separating long-form uploads and Shorts.

### `routine_update`

- Purpose: cron-friendly helper that re-runs `download_channel` for every channel already present under `/yt`.
- Behaviour:
  - Walks `/yt/videos/**` and `/yt/shorts/**` looking for `<video_id>.info.json` files.
  - Extracts the original `channel_url`/`uploader_url` from those JSON blobs and deduplicates them.
  - Sequentially invokes `download_channel <channel_url>` so each channel gets refreshed with the latest uploads/comments.
- Flags:
  - `--media-root <path>` matches the library root passed to `download_channel`/`backend` (default `/yt`).
  - `--www-root <path>` mirrors the downloader flag for consistency, letting you document the static site root if it lives somewhere else.
- Usage example:
  ```bash
  ./routine_update
  ```
  Combine it with a scheduler (cron/systemd timers) to keep your library synced overnight without manual intervention.

All three utilities share the same Rust crate (`newtube_tools`), so adding new metadata fields only requires updating the structs once.

# Tests

Before runing any tests, you need to run `npm install` to install modules.

`cargo test` covers the Rust backend (module `metadata.rs`)

`npm run test` / `npm run test:unit` : launches Jest with `fake-indexeddb`, `jsdom` and validates front helpers (normalisation vidéo, opérations IndexedDB, API client, stockage user). Les fichiers concernés se trouvent dans `tests/js/*.test.js`

`npm run test:coverage` : même suite Jest que ci-dessus mais enregistre un rapport HTML/LCOV sous `coverage/jest`

`npm run test:e2e` : launches Cypress on port 4173. It now covers **both** `cypress/e2e/home.cy.js` (home grid + sidebar states per desktop/tablet/mobile rules from `cypress/fixtures/bootstrap.json`) and `cypress/e2e/watch.cy.js` (video player metadata, comments rendering and like/dislike/subscription toggles with mocked API responses)
