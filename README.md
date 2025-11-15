# Viewtube

A youtube frontend clone, entirely written from the gound up in HTMlL, CSS and javascript to be extra extra fast and almost pixel-perfect with the Youtube UI. Only exceptions are bad UI/UX decisions like the very recent icons and mobile-oriented style. The backend is fully written in safe rust and some bash scripts in order to clone entire youtube channels. When a video from them is first asked by a client, the 

There is no account system, but history and likes/dislikes still work. You can save your cookies via an ID which contains your likes/dislikes/playlists/history and is unique to you so you can erase your cookies and still have the same experience on all your devices. There is also no ad. It also is not in violation of youtube copyright as all icons are taken from material UI and open-licensed, and it does NOT serve videos from youtube directly or indirectly, therefore there is no violation of youtube's TOS as this makes NO calls to youtube.com or any google-owned subdomains.

The Javascript caches pages and loads them only one time via a service worker to have instant subsequent loading times of non video-related assets for maximum speed and responsiveness. Pages are drawn into a container and which is then deleted and recreated when changing pages to keep everything in the same page. Page structure is mainly in the javascript files, which manipulate the HTML in real time.

## Using the Rust Backend

Compile and get the binaries in the current directory:

```
cargo clean && cargo build --release
cp target/release/backend target/release/download_channel target/release/routine_update .
```

Make sure the downloader has created the directory layout expected by the server:

 - Videos + muxed formats live under `/yt/videos/<video_id>/`.
 - Shorts live under `/yt/shorts/<video_id>/`.
 - Thumbnails and subtitles live under `/yt/thumbnails/<video_id>/` and `/yt/subtitles/<video_id>/` respectively.
- The SQLite metadata database resides at `/yt/metadata.db`, website should be served via a nginx reverse proxy pointed to `/www/newtube.com/index.html` which is the app's entry point.

Start the API server:

The only runtime knob is the port with `NEWTUBE_PORT=9090` (default `8080`).

```
screen -S "backend" ./backend
```

CTRL+A and CTRL+D to exit.

## Bakend implementation details

### `backend`

- Purpose: lightweight Axum HTTP server that exposes `/api/*` routes consumed by the web UI.
- Inputs: reads metadata from `/yt/metadata.db` and streams files out of `/yt` (videos, shorts, subtitles, thumbnails).
- Caching: keeps a read-through cache in memory so hot feeds do not hammer SQLite; restart the process to clear the cache.
- Flags:
  - `--media-root <path>` overrides the default `/yt` library root (the metadata database is read from `<path>/metadata.db`).
- Usage example:
  ```bash
  NEWTUBE_PORT=9000 ./backend
  # -> API server listening on http://0.0.0.0:9000
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

`npm run test` : launches Jest with `fake-indexeddb`, `jsdom` and validates front helpers with et valide les helpers front (normalisation vidéo, opérations IndexedDB, API client, stockage user). Les fichiers concernés se trouvent dans `tests/js/*.test.js`

`npm run test:e2e` launches Cypress on port 4173 and runs `cypress/e2e/home.cy.js` who verifies the proper render of the Home video grid and sidebar switch between versions (normal / minimal/ none) according to desktop/tablet/mobile rules from `cypress/fixtures/bootstrap.json`
