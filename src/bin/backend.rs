//! Minimal Axum backend that serves already-downloaded ViewTube assets.
//!
//! Incoming requests never touch YouTube. We only expose the SQLite metadata
//! plus the media files stored locally on disk. The number of comments in here
//! is intentionally high, per project request, to make future maintenance easy.

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use mime_guess::{MimeGuess, mime::Mime};
use newtube_tools::metadata::{
    CommentRecord, MetadataReader, SubtitleCollection, VideoRecord, VideoSource,
};
use parking_lot::RwLock;
use serde::Serialize;
use tokio::{fs::File, signal, task};
use tokio_util::io::ReaderStream;

// Directory layout constants. Keeping them centralized means the same values
// can be used when serving both long-form and short-form videos.
const BASE_DIR: &str = "/yt";
const VIDEOS_SUBDIR: &str = "videos";
const SHORTS_SUBDIR: &str = "shorts";
const THUMBNAILS_SUBDIR: &str = "thumbnails";
const SUBTITLES_SUBDIR: &str = "subtitles";

// Network defaults. Both can be overridden through env vars/env but in most
// deployments we bind to every interface on the host.
const DEFAULT_HOST: &str = "0.0.0.0";
const DEFAULT_PORT: u16 = 8080;

// SQLite database location that the downloader keeps up to date.
const METADATA_DB_PATH: &str = "/www/newtube.com/metadata.db";

#[derive(Clone, Copy)]
enum MediaCategory {
    Video,
    Short,
}

/// Shared state injected into every Axum handler.
///
/// * `reader` performs blocking SQLite reads via `spawn_blocking`.
/// * `cache` prevents repeated deserialization for hot endpoints such as the
///   homepage feed.
/// * `files` knows where audio/video/subtitle payloads live on disk.
#[derive(Clone)]
struct AppState {
    reader: Arc<MetadataReader>,
    cache: Arc<ApiCache>,
    files: Arc<FilePaths>,
}

/// Very small in-memory cache to avoid re-querying SQLite on every request.
///
/// This keeps the backend stateless enough for systemd restarts yet vastly
/// reduces IO for repeated playback of the same assets.
struct ApiCache {
    videos: RwLock<Option<Vec<VideoRecord>>>,
    shorts: RwLock<Option<Vec<VideoRecord>>>,
    video_details: RwLock<HashMap<String, VideoRecord>>,
    short_details: RwLock<HashMap<String, VideoRecord>>,
    comments: RwLock<HashMap<String, Vec<CommentRecord>>>,
    subtitles: RwLock<HashMap<String, SubtitleCollection>>,
    bootstrap: RwLock<Option<Arc<BootstrapPayload>>>,
}

impl ApiCache {
    /// Creates an empty cache. RwLocks allow parallel readers while writes
    /// remain extremely short-lived (single assignment).
    fn new() -> Self {
        Self {
            videos: RwLock::new(None),
            shorts: RwLock::new(None),
            video_details: RwLock::new(HashMap::new()),
            short_details: RwLock::new(HashMap::new()),
            comments: RwLock::new(HashMap::new()),
            subtitles: RwLock::new(HashMap::new()),
            bootstrap: RwLock::new(None),
        }
    }

    fn media_list(&self, category: MediaCategory) -> &RwLock<Option<Vec<VideoRecord>>> {
        match category {
            MediaCategory::Video => &self.videos,
            MediaCategory::Short => &self.shorts,
        }
    }

    fn media_details(&self, category: MediaCategory) -> &RwLock<HashMap<String, VideoRecord>> {
        match category {
            MediaCategory::Video => &self.video_details,
            MediaCategory::Short => &self.short_details,
        }
    }
}

/// Materialized file-system locations used at runtime.
struct FilePaths {
    videos: PathBuf,
    shorts: PathBuf,
    thumbnails: PathBuf,
    subtitles: PathBuf,
}

impl FilePaths {
    /// Builds the folder structure from the constants at the top of the file.
    fn new() -> Self {
        let base = PathBuf::from(BASE_DIR);
        Self {
            videos: base.join(VIDEOS_SUBDIR),
            shorts: base.join(SHORTS_SUBDIR),
            thumbnails: base.join(THUMBNAILS_SUBDIR),
            subtitles: base.join(SUBTITLES_SUBDIR),
        }
    }

    /// Chooses either the `videos` or `shorts` directory.
    fn media_dir(&self, category: MediaCategory) -> &Path {
        match category {
            MediaCategory::Video => &self.videos,
            MediaCategory::Short => &self.shorts,
        }
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    /// Creates a 404 error with the provided message.
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    /// Creates a 500 error with the provided message.
    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let body = serde_json::json!({
            "error": self.message,
        });
        (self.status, headers, Json(body)).into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

#[tokio::main]
async fn main() -> Result<()> {
    // Allow overriding the port via environment variable while retaining the
    // easy default for local testing.
    let port = std::env::var("NEWTUBE_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);

    let reader = MetadataReader::new(METADATA_DB_PATH).context("initializing metadata reader")?;

    let state = AppState {
        reader: Arc::new(reader),
        cache: Arc::new(ApiCache::new()),
        files: Arc::new(FilePaths::new()),
    };

    // Each route is extremely small; helpers supplement anything that is shared
    // between videos and shorts.
    let app = Router::new()
        .route("/api/bootstrap", get(bootstrap))
        .route("/api/videos", get(list_videos))
        .route("/api/videos/{id}", get(get_video))
        .route("/api/videos/{id}/comments", get(get_video_comments))
        .route("/api/videos/{id}/subtitles", get(list_video_subtitles))
        .route(
            "/api/videos/{id}/subtitles/{code}",
            get(download_video_subtitle),
        )
        .route(
            "/api/videos/{id}/thumbnails/{file}",
            get(download_video_thumbnail),
        )
        .route("/api/videos/{id}/streams/{format}", get(stream_video_file))
        .route("/api/shorts", get(list_shorts))
        .route("/api/shorts/{id}", get(get_short))
        .route("/api/shorts/{id}/comments", get(get_video_comments))
        .route("/api/shorts/{id}/subtitles", get(list_short_subtitles))
        .route(
            "/api/shorts/{id}/subtitles/{code}",
            get(download_short_subtitle),
        )
        .route(
            "/api/shorts/{id}/thumbnails/{file}",
            get(download_short_thumbnail),
        )
        .route("/api/shorts/{id}/streams/{format}", get(stream_short_file))
        .with_state(state);

    let addr = SocketAddr::new(DEFAULT_HOST.parse()?, port);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding to {}", addr))?;
    println!("API server listening on http://{}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("running API server")?;

    Ok(())
}

async fn shutdown_signal() {
    // We do not propagate this error up because it only affects graceful
    // shutdown; the process still terminates when Ctrl+C fires.
    if let Err(err) = signal::ctrl_c().await {
        eprintln!("Failed to install Ctrl+C handler: {}", err);
    }
}

async fn bootstrap(State(state): State<AppState>) -> ApiResult<Json<BootstrapPayload>> {
    let payload = state.get_bootstrap().await?;
    Ok(Json((*payload).clone()))
}

async fn list_videos(State(state): State<AppState>) -> ApiResult<Json<Vec<VideoRecord>>> {
    let videos = state.get_media_list(MediaCategory::Video).await?;
    Ok(Json(videos))
}

async fn list_shorts(State(state): State<AppState>) -> ApiResult<Json<Vec<VideoRecord>>> {
    let shorts = state.get_media_list(MediaCategory::Short).await?;
    Ok(Json(shorts))
}

async fn get_video(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<VideoRecord>> {
    let record = state.get_media(MediaCategory::Video, &id).await?;
    Ok(Json(record))
}

async fn get_short(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<VideoRecord>> {
    let record = state.get_media(MediaCategory::Short, &id).await?;
    Ok(Json(record))
}

async fn get_video_comments(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<Vec<CommentRecord>>> {
    let comments = state.get_comments(&id).await?;
    Ok(Json(comments))
}

async fn list_video_subtitles(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<Vec<SubtitleInfo>>> {
    list_subtitles(state, id, "videos").await
}

async fn list_short_subtitles(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<Vec<SubtitleInfo>>> {
    list_subtitles(state, id, "shorts").await
}

async fn list_subtitles(
    state: AppState,
    id: String,
    slug: &'static str,
) -> ApiResult<Json<Vec<SubtitleInfo>>> {
    // Build lightweight DTOs that point the frontend to the download
    // endpoints; the actual subtitle JSON remains cached server side.
    let mut response = Vec::new();
    if let Some(collection) = state.get_subtitles(&id).await? {
        for track in collection.languages {
            let url = format!("/api/{slug}/{}/subtitles/{}", id, track.code);
            response.push(SubtitleInfo {
                code: track.code,
                name: track.name,
                url,
            });
        }
    }

    Ok(Json(response))
}

async fn download_video_subtitle(
    State(state): State<AppState>,
    AxumPath((id, code)): AxumPath<(String, String)>,
) -> ApiResult<Response> {
    download_subtitle(state, id, code).await
}

async fn download_short_subtitle(
    State(state): State<AppState>,
    AxumPath((id, code)): AxumPath<(String, String)>,
) -> ApiResult<Response> {
    download_subtitle(state, id, code).await
}

async fn download_subtitle(state: AppState, id: String, code: String) -> ApiResult<Response> {
    let subtitles = state
        .get_subtitles(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("subtitles not available"))?;

    let track = subtitles
        .languages
        .into_iter()
        .find(|track| track.code == code)
        .ok_or_else(|| ApiError::not_found("subtitle track not found"))?;

    // Prefer the explicit filesystem path recorded during download, but fall
    // back to the standard `videoid/lang` layout when missing.
    let path = track.path.map(PathBuf::from).unwrap_or_else(|| {
        state
            .files
            .subtitles
            .join(&id)
            .join(format!("{}.{}.vtt", id, code))
    });

    stream_file(path, Some("text/vtt".parse().unwrap())).await
}

async fn download_video_thumbnail(
    State(state): State<AppState>,
    AxumPath((id, file)): AxumPath<(String, String)>,
) -> ApiResult<Response> {
    download_thumbnail(state, id, file).await
}

async fn download_short_thumbnail(
    State(state): State<AppState>,
    AxumPath((id, file)): AxumPath<(String, String)>,
) -> ApiResult<Response> {
    download_thumbnail(state, id, file).await
}

async fn download_thumbnail(state: AppState, id: String, file: String) -> ApiResult<Response> {
    let path = state.files.thumbnails.join(&id).join(&file);
    stream_file(path, None).await
}

async fn stream_video_file(
    State(state): State<AppState>,
    AxumPath((id, format)): AxumPath<(String, String)>,
) -> ApiResult<Response> {
    stream_media(state, MediaCategory::Video, id, format).await
}

async fn stream_short_file(
    State(state): State<AppState>,
    AxumPath((id, format)): AxumPath<(String, String)>,
) -> ApiResult<Response> {
    stream_media(state, MediaCategory::Short, id, format).await
}

async fn stream_media(
    state: AppState,
    category: MediaCategory,
    id: String,
    format: String,
) -> ApiResult<Response> {
    // We load metadata first so we can map the requested format slug to a file
    // path and mime type before hitting the disk.
    let record = state.get_media(category, &id).await?;

    let source = record
        .sources
        .iter()
        .find(|source| source_key(source).as_deref() == Some(format.as_str()))
        .ok_or_else(|| ApiError::not_found("requested format not found"))?;

    let path = match &source.path {
        Some(path) => PathBuf::from(path),
        None => {
            let ext = source.ext.as_deref().unwrap_or("mp4");
            state
                .files
                .media_dir(category)
                .join(&id)
                .join(format!("{}_{}.{}", id, format, ext))
        }
    };

    stream_file(
        path,
        source.mime_type.as_ref().and_then(|mime| mime.parse().ok()),
    )
    .await
}

/// Lightweight response that exposes a download URL for each subtitle track.
#[derive(serde::Serialize)]
struct SubtitleInfo {
    code: String,
    name: String,
    url: String,
}

/// Payload returned by `/api/bootstrap` so the client can hydrate offline.
#[derive(Clone, Serialize)]
struct BootstrapPayload {
    videos: Vec<VideoRecord>,
    shorts: Vec<VideoRecord>,
    subtitles: Vec<SubtitleCollection>,
    comments: Vec<CommentRecord>,
}

impl AppState {
    /// Returns a cached snapshot containing everything the SPA needs to boot
    /// without hitting follow-up endpoints (videos, shorts, subtitles,
    /// comments). The heavy lifting runs in a blocking task because SQLite is a
    /// synchronous API.
    async fn get_bootstrap(&self) -> ApiResult<Arc<BootstrapPayload>> {
        if let Some(cached) = self.cache.bootstrap.read().clone() {
            return Ok(cached);
        }

        let reader = self.reader.clone();
        let payload = task::spawn_blocking(move || -> Result<BootstrapPayload> {
            let videos = reader.list_videos()?;
            let shorts = reader.list_shorts()?;
            let subtitles = reader.list_subtitles()?;
            let comments = reader.list_all_comments()?;
            Ok(BootstrapPayload {
                videos,
                shorts,
                subtitles,
                comments,
            })
        })
        .await
        .map_err(|err| ApiError::internal(format!("task join error: {err}")))?
        .map_err(|err| ApiError::internal(err.to_string()))?;

        let payload = Arc::new(payload);
        self.cache.bootstrap.write().replace(payload.clone());
        Ok(payload)
    }

    /// Retrieves every video/short record, memoizing both the list and the
    /// individual details map for quick follow-up lookups.
    async fn get_media_list(&self, category: MediaCategory) -> ApiResult<Vec<VideoRecord>> {
        if let Some(cached) = self.cache.media_list(category).read().clone() {
            return Ok(cached);
        }

        let reader = self.reader.clone();
        let records = task::spawn_blocking(move || match category {
            MediaCategory::Video => reader.list_videos(),
            MediaCategory::Short => reader.list_shorts(),
        })
        .await
        .map_err(|err| ApiError::internal(format!("task join error: {err}")))?
        .map_err(|err| ApiError::internal(err.to_string()))?;

        self.cache
            .media_list(category)
            .write()
            .replace(records.clone());

        let mut details = self.cache.media_details(category).write();
        for record in &records {
            details.insert(record.videoid.clone(), record.clone());
        }

        Ok(records)
    }

    /// Loads metadata for a single video or short, preferring the cache before
    /// falling back to SQLite.
    async fn get_media(&self, category: MediaCategory, videoid: &str) -> ApiResult<VideoRecord> {
        if let Some(record) = self
            .cache
            .media_details(category)
            .read()
            .get(videoid)
            .cloned()
        {
            return Ok(record);
        }

        let reader = self.reader.clone();
        let result = task::spawn_blocking({
            let videoid = videoid.to_owned();
            move || match category {
                MediaCategory::Video => reader.get_video(&videoid),
                MediaCategory::Short => reader.get_short(&videoid),
            }
        })
        .await
        .map_err(|err| ApiError::internal(format!("task join error: {err}")))?
        .map_err(|err| ApiError::internal(err.to_string()))?;

        let record = result.ok_or_else(|| ApiError::not_found("video not found"))?;

        self.cache
            .media_details(category)
            .write()
            .insert(videoid.to_owned(), record.clone());

        Ok(record)
    }

    /// Lazy-loads comment threads; we store them keyed by id because comment
    /// payloads are far smaller than video blobs.
    async fn get_comments(&self, videoid: &str) -> ApiResult<Vec<CommentRecord>> {
        if let Some(cached) = self.cache.comments.read().get(videoid).cloned() {
            return Ok(cached);
        }

        let reader = self.reader.clone();
        let comments = task::spawn_blocking({
            let videoid = videoid.to_owned();
            move || reader.get_comments(&videoid)
        })
        .await
        .map_err(|err| ApiError::internal(format!("task join error: {err}")))?
        .map_err(|err| ApiError::internal(err.to_string()))?;

        self.cache
            .comments
            .write()
            .insert(videoid.to_owned(), comments.clone());

        Ok(comments)
    }

    /// Provides subtitle metadata if available. Not every video has subtitles
    /// so the API returns an Option.
    async fn get_subtitles(&self, videoid: &str) -> ApiResult<Option<SubtitleCollection>> {
        if let Some(cached) = self.cache.subtitles.read().get(videoid).cloned() {
            return Ok(Some(cached));
        }

        let reader = self.reader.clone();
        let result = task::spawn_blocking({
            let videoid = videoid.to_owned();
            move || reader.get_subtitles(&videoid)
        })
        .await
        .map_err(|err| ApiError::internal(format!("task join error: {err}")))?
        .map_err(|err| ApiError::internal(err.to_string()))?;

        if let Some(collection) = &result {
            self.cache
                .subtitles
                .write()
                .insert(videoid.to_owned(), collection.clone());
        }

        Ok(result)
    }
}

/// Normalizes a VideoSource URL by keeping only the trailing segment. During
/// download we store files named `{videoid}_{format}` and the format parameter
/// is the only piece users need to specify.
fn source_key(source: &VideoSource) -> Option<String> {
    source.url.rsplit('/').next().map(|value| value.to_owned())
}

async fn stream_file(path: PathBuf, mime: Option<Mime>) -> ApiResult<Response> {
    let file = File::open(&path)
        .await
        .map_err(|_| ApiError::not_found("file not found"))?;

    // Either use the explicit mime provided by the VideoSource or infer it from
    // the file extension. Setting CONTENT_TYPE hints allows browsers to stream
    // video without sniffing.
    let guessed = mime.or_else(|| MimeGuess::from_path(&path).first());
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);
    let mut response = body.into_response();
    if let Some(mime) = guessed {
        if let Ok(value) = mime.to_string().parse() {
            response.headers_mut().insert(header::CONTENT_TYPE, value);
        }
    }

    Ok(response)
}
