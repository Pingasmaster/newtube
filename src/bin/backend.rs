#![forbid(unsafe_code)]

//! Minimal Axum backend that serves already-downloaded NewTube assets.
//!
//! Incoming requests never touch YouTube. We only expose the SQLite metadata
//! plus the media files stored locally on disk. The number of comments in here
//! is intentionally high, per project request, to make future maintenance easy.

use std::{
    collections::HashMap,
    fs,
    net::{IpAddr, SocketAddr},
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use mime_guess::{MimeGuess, mime::Mime};
use newtube_tools::config::{
    DEFAULT_ENV_PATH, RuntimeOverrides, read_env_file, resolve_runtime_paths,
};
use newtube_tools::metadata::{
    CommentRecord, MetadataReader, SubtitleCollection, VideoRecord, VideoSource,
};
#[cfg(test)]
use newtube_tools::metadata::{MetadataStore, SubtitleTrack};
use newtube_tools::security::ensure_not_root;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use serde_json::json;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    signal,
};
use tokio_util::io::ReaderStream;

// Directory layout defaults. Keeping them centralized means the same values
// can be used when serving both long-form and short-form videos.
const VIDEOS_SUBDIR: &str = "videos";
const SHORTS_SUBDIR: &str = "shorts";
const THUMBNAILS_SUBDIR: &str = "thumbnails";
const SUBTITLES_SUBDIR: &str = "subtitles";

// SQLite database file relative to the media root.
const METADATA_DB_FILE: &str = "metadata.db";
const SETTINGS_FILE: &str = "instance_settings.json";
const DOWNLOADS_DIR: &str = "downloads";

#[derive(Debug, Clone)]
struct BackendArgs {
    media_root: PathBuf,
    www_root: PathBuf,
    newtube_port: u16,
    listen_host: IpAddr,
}

impl BackendArgs {
    fn parse() -> Result<Self> {
        Self::from_iter(std::env::args().skip(1))
    }

    fn from_iter<I>(iter: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut media_root_override: Option<PathBuf> = None;
        let mut www_root_override: Option<PathBuf> = None;
        let mut port_override: Option<u16> = None;
        let mut host_override: Option<IpAddr> = None;
        let mut args = iter.into_iter();
        while let Some(arg) = args.next() {
            if let Some(value) = arg.strip_prefix("--media-root=") {
                media_root_override = Some(PathBuf::from(value));
                continue;
            }
            if let Some(value) = arg.strip_prefix("--www-root=") {
                www_root_override = Some(PathBuf::from(value));
                continue;
            }
            if let Some(value) = arg.strip_prefix("--port=") {
                port_override = Some(parse_port_arg(value)?);
                continue;
            }
            if let Some(value) = arg.strip_prefix("--host=") {
                host_override = Some(parse_host_arg(value)?);
                continue;
            }

            match arg.as_str() {
                "--media-root" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--media-root requires a value"))?;
                    media_root_override = Some(PathBuf::from(value));
                }
                "--www-root" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--www-root requires a value"))?;
                    www_root_override = Some(PathBuf::from(value));
                }
                "--port" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--port requires a value"))?;
                    port_override = Some(parse_port_arg(&value)?);
                }
                "--host" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--host requires a value"))?;
                    host_override = Some(parse_host_arg(&value)?);
                }
                _ => return Err(anyhow!("unknown argument: {arg}")),
            }
        }

        let runtime_paths = resolve_runtime_paths(RuntimeOverrides {
            media_root: media_root_override.clone(),
            www_root: www_root_override.clone(),
            ..RuntimeOverrides::default()
        })?;
        let runtime_host = parse_host_arg(&runtime_paths.newtube_host)?;
        let media_root = media_root_override.unwrap_or(runtime_paths.media_root);
        let www_root = www_root_override.unwrap_or(runtime_paths.www_root);
        let newtube_port = port_override.unwrap_or(runtime_paths.newtube_port);
        let listen_host = host_override.unwrap_or(runtime_host);

        Ok(Self {
            media_root,
            www_root,
            newtube_port,
            listen_host,
        })
    }
}

fn parse_port_arg(value: &str) -> Result<u16> {
    value
        .parse::<u16>()
        .context("expected a numeric port between 0 and 65535")
}

fn parse_host_arg(value: &str) -> Result<IpAddr> {
    value
        .parse::<IpAddr>()
        .context("expected a valid IPv4 or IPv6 address for --host/NEWTUBE_HOST")
}

#[derive(Clone, Copy)]
enum MediaCategory {
    Video,
    Short,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MissingMediaBehavior {
    NotFound,
    Prompt,
}

impl MissingMediaBehavior {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "404" | "not_found" | "notfound" => Some(Self::NotFound),
            "prompt" | "download" | "ask" => Some(Self::Prompt),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstanceSettings {
    missing_media_behavior: MissingMediaBehavior,
}

impl InstanceSettings {
    fn from_env(file_vars: &HashMap<String, String>) -> Self {
        let raw = env_or_file_value("NEWTUBE_MISSING_MEDIA_BEHAVIOR", file_vars);
        let missing_media_behavior = raw
            .as_deref()
            .and_then(MissingMediaBehavior::parse)
            .unwrap_or(MissingMediaBehavior::NotFound);

        Self {
            missing_media_behavior,
        }
    }
}

struct SettingsStore {
    path: PathBuf,
    current: RwLock<InstanceSettings>,
}

impl SettingsStore {
    fn load(media_root: &Path, defaults: InstanceSettings) -> Self {
        let path = media_root.join(SETTINGS_FILE);
        let current = match fs::read_to_string(&path) {
            Ok(raw) => serde_json::from_str(&raw).unwrap_or(defaults),
            Err(_) => defaults,
        };

        Self {
            path,
            current: RwLock::new(current),
        }
    }

    fn get(&self) -> InstanceSettings {
        self.current.read().clone()
    }

    fn update(&self, settings: InstanceSettings) -> Result<InstanceSettings> {
        write_json_atomic(&self.path, &settings)?;
        *self.current.write() = settings.clone();
        Ok(settings)
    }
}

#[derive(Clone)]
struct DownloadManager {
    inner: Arc<DownloadManagerInner>,
}

struct DownloadManagerInner {
    jobs: Mutex<HashMap<String, DownloadJob>>,
    counter: AtomicUsize,
    media_root: PathBuf,
    www_root: PathBuf,
    downloader: Option<PathBuf>,
}

#[derive(Clone)]
struct DownloadJob {
    id: String,
    status: DownloadStatus,
    progress_file: PathBuf,
    message: String,
}

#[derive(Clone, Copy, Debug)]
enum DownloadStatus {
    Queued,
    Running,
    Success,
    Failed,
}

impl DownloadStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Success => "completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DownloadJobResponse {
    id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DownloadJobStatus {
    id: String,
    status: String,
    progress: u8,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DownloadVideoRequest {
    video_id: String,
    media_kind: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DownloadChannelRequest {
    video_id: String,
    media_kind: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProgressReport {
    progress: u8,
    message: String,
}

impl DownloadManager {
    fn new(media_root: PathBuf, www_root: PathBuf) -> Self {
        let downloader = find_download_channel_executable().ok();
        Self {
            inner: Arc::new(DownloadManagerInner {
                jobs: Mutex::new(HashMap::new()),
                counter: AtomicUsize::new(1),
                media_root,
                www_root,
                downloader,
            }),
        }
    }

    fn start_video_download(&self, video_id: String, media_kind: MediaCategory) -> Result<String> {
        let downloader = self
            .inner
            .downloader
            .clone()
            .ok_or_else(|| anyhow!("download_channel binary not found"))?;
        let job_id = self.next_job_id();
        let progress_file = self.progress_file_path(&job_id);
        write_progress_report(&progress_file, 0, "Queued download");

        self.inner.jobs.lock().insert(
            job_id.clone(),
            DownloadJob {
                id: job_id.clone(),
                status: DownloadStatus::Queued,
                progress_file: progress_file.clone(),
                message: "Queued".to_string(),
            },
        );

        let inner = self.inner.clone();
        let job_id_clone = job_id.clone();
        tokio::spawn(async move {
            update_job_status(&inner, &job_id_clone, DownloadStatus::Running, "Running");
            let inner_for_run = inner.clone();
            let progress_for_run = progress_file.clone();
            let status = tokio::task::spawn_blocking(move || {
                let args = vec![
                    "--media-root".to_string(),
                    inner_for_run.media_root.to_string_lossy().into_owned(),
                    "--www-root".to_string(),
                    inner_for_run.www_root.to_string_lossy().into_owned(),
                    "--progress-file".to_string(),
                    progress_for_run.to_string_lossy().into_owned(),
                    "--video-id".to_string(),
                    video_id,
                    "--media-kind".to_string(),
                    media_kind_label(media_kind).to_string(),
                ];
                run_download_channel(&downloader, args)
            })
            .await;

            match status {
                Ok(Ok(())) => {
                    update_job_status(&inner, &job_id_clone, DownloadStatus::Success, "Done")
                }
                Ok(Err(err)) => {
                    write_progress_report(&progress_file, 100, "Download failed");
                    update_job_status(
                        &inner,
                        &job_id_clone,
                        DownloadStatus::Failed,
                        &format!("Failed: {err}"),
                    );
                }
                Err(err) => {
                    write_progress_report(&progress_file, 100, "Download failed");
                    update_job_status(
                        &inner,
                        &job_id_clone,
                        DownloadStatus::Failed,
                        &format!("Failed: {err}"),
                    );
                }
            }
        });

        Ok(job_id)
    }

    fn start_channel_download(
        &self,
        video_id: String,
        media_kind: MediaCategory,
    ) -> Result<String> {
        let downloader = self
            .inner
            .downloader
            .clone()
            .ok_or_else(|| anyhow!("download_channel binary not found"))?;
        let job_id = self.next_job_id();
        let progress_file = self.progress_file_path(&job_id);
        write_progress_report(&progress_file, 0, "Resolving channel");

        self.inner.jobs.lock().insert(
            job_id.clone(),
            DownloadJob {
                id: job_id.clone(),
                status: DownloadStatus::Queued,
                progress_file: progress_file.clone(),
                message: "Queued".to_string(),
            },
        );

        let inner = self.inner.clone();
        let job_id_clone = job_id.clone();
        tokio::spawn(async move {
            update_job_status(
                &inner,
                &job_id_clone,
                DownloadStatus::Running,
                "Resolving channel",
            );
            let video_id_for_lookup = video_id.clone();
            let channel_result = tokio::task::spawn_blocking(move || {
                resolve_channel_url(&video_id_for_lookup, media_kind)
            })
            .await;

            let channel_url = match channel_result {
                Ok(Ok(url)) => url,
                Ok(Err(err)) => {
                    write_progress_report(&progress_file, 100, "Channel lookup failed");
                    update_job_status(
                        &inner,
                        &job_id_clone,
                        DownloadStatus::Failed,
                        &format!("Failed: {err}"),
                    );
                    return;
                }
                Err(err) => {
                    write_progress_report(&progress_file, 100, "Channel lookup failed");
                    update_job_status(
                        &inner,
                        &job_id_clone,
                        DownloadStatus::Failed,
                        &format!("Failed: {err}"),
                    );
                    return;
                }
            };

            let inner_for_run = inner.clone();
            let progress_for_run = progress_file.clone();
            let status = tokio::task::spawn_blocking(move || {
                let args = vec![
                    "--media-root".to_string(),
                    inner_for_run.media_root.to_string_lossy().into_owned(),
                    "--www-root".to_string(),
                    inner_for_run.www_root.to_string_lossy().into_owned(),
                    "--progress-file".to_string(),
                    progress_for_run.to_string_lossy().into_owned(),
                    channel_url,
                ];
                run_download_channel(&downloader, args)
            })
            .await;

            match status {
                Ok(Ok(())) => {
                    update_job_status(&inner, &job_id_clone, DownloadStatus::Success, "Done")
                }
                Ok(Err(err)) => {
                    write_progress_report(&progress_file, 100, "Download failed");
                    update_job_status(
                        &inner,
                        &job_id_clone,
                        DownloadStatus::Failed,
                        &format!("Failed: {err}"),
                    );
                }
                Err(err) => {
                    write_progress_report(&progress_file, 100, "Download failed");
                    update_job_status(
                        &inner,
                        &job_id_clone,
                        DownloadStatus::Failed,
                        &format!("Failed: {err}"),
                    );
                }
            }
        });

        Ok(job_id)
    }

    fn get_status(&self, job_id: &str) -> Option<DownloadJobStatus> {
        let job = self.inner.jobs.lock().get(job_id).cloned()?;
        let progress = read_progress_report(&job.progress_file);

        let (progress_value, message) = progress
            .map(|report| (report.progress, report.message))
            .unwrap_or((0, job.message.clone()));

        Some(DownloadJobStatus {
            id: job.id,
            status: job.status.as_str().to_string(),
            progress: progress_value,
            message,
        })
    }

    fn next_job_id(&self) -> String {
        let id = self.inner.counter.fetch_add(1, Ordering::Relaxed);
        format!("download-{id}")
    }

    fn progress_file_path(&self, job_id: &str) -> PathBuf {
        self.inner
            .media_root
            .join(DOWNLOADS_DIR)
            .join(format!("{job_id}.json"))
    }
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
    www_root: Arc<PathBuf>,
    settings: Arc<SettingsStore>,
    downloads: DownloadManager,
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
    last_db_version: RwLock<Option<i64>>,
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
            last_db_version: RwLock::new(None),
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

    fn clear(&self) {
        self.videos.write().take();
        self.shorts.write().take();
        self.video_details.write().clear();
        self.short_details.write().clear();
        self.comments.write().clear();
        self.subtitles.write().clear();
        self.bootstrap.write().take();
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
    /// Builds the folder structure based on the provided media root.
    fn new(media_root: &Path) -> Self {
        Self {
            videos: media_root.join(VIDEOS_SUBDIR),
            shorts: media_root.join(SHORTS_SUBDIR),
            thumbnails: media_root.join(THUMBNAILS_SUBDIR),
            subtitles: media_root.join(SUBTITLES_SUBDIR),
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

#[cfg(test)]
impl FilePaths {
    fn for_base(path: &Path) -> Self {
        let paths = Self::new(path);
        std::fs::create_dir_all(&paths.videos).unwrap();
        std::fs::create_dir_all(&paths.shorts).unwrap();
        std::fs::create_dir_all(&paths.thumbnails).unwrap();
        std::fs::create_dir_all(&paths.subtitles).unwrap();
        paths
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
    let BackendArgs {
        media_root,
        www_root,
        newtube_port,
        listen_host,
    } = BackendArgs::parse()?;

    ensure_not_root("backend")?;

    // Allow overriding the port via environment variable while retaining the
    // easy default for local testing.
    let port = std::env::var("NEWTUBE_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(newtube_port);

    let host = match std::env::var("NEWTUBE_HOST") {
        Ok(value) if !value.trim().is_empty() => parse_host_arg(value.trim())?,
        _ => listen_host,
    };

    let metadata_path = media_root.join(METADATA_DB_FILE);
    let reader = MetadataReader::new(&metadata_path)
        .await
        .context("initializing metadata reader")?;

    let env_vars = read_env_file(Path::new(DEFAULT_ENV_PATH)).unwrap_or_default();
    let settings_defaults = InstanceSettings::from_env(&env_vars);
    let settings_store = Arc::new(SettingsStore::load(&media_root, settings_defaults));
    let downloads = DownloadManager::new(media_root.clone(), www_root.clone());

    let state = AppState {
        reader: Arc::new(reader),
        cache: Arc::new(ApiCache::new()),
        files: Arc::new(FilePaths::new(&media_root)),
        www_root: Arc::new(www_root),
        settings: settings_store,
        downloads,
    };

    // Each route is extremely small; helpers supplement anything that is shared
    // between videos and shorts.
    let app = Router::new()
        .route("/api/settings", get(get_settings).put(update_settings))
        .route("/api/downloads/video", post(start_video_download))
        .route("/api/downloads/channel", post(start_channel_download))
        .route("/api/downloads/{id}", get(get_download_status))
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
        .fallback(static_fallback)
        .with_state(state);

    let addr = SocketAddr::new(host, port);
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

async fn static_fallback(State(state): State<AppState>, req: Request<Body>) -> Response {
    let path = req.uri().path();
    if path == "/api" || path.starts_with("/api/") {
        return ApiError::not_found("endpoint not found").into_response();
    }

    match serve_www_path(&state.www_root, path).await {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

async fn get_settings(State(state): State<AppState>) -> ApiResult<Json<InstanceSettings>> {
    Ok(Json(state.settings.get()))
}

async fn update_settings(
    State(state): State<AppState>,
    Json(payload): Json<InstanceSettings>,
) -> ApiResult<Json<InstanceSettings>> {
    let updated = state
        .settings
        .update(payload)
        .map_err(|err| ApiError::internal(err.to_string()))?;
    Ok(Json(updated))
}

async fn start_video_download(
    State(state): State<AppState>,
    Json(payload): Json<DownloadVideoRequest>,
) -> ApiResult<Json<DownloadJobResponse>> {
    let kind = parse_media_kind(payload.media_kind.as_deref());
    let job_id = state
        .downloads
        .start_video_download(payload.video_id, kind)
        .map_err(|err| ApiError::internal(err.to_string()))?;
    Ok(Json(DownloadJobResponse { id: job_id }))
}

async fn start_channel_download(
    State(state): State<AppState>,
    Json(payload): Json<DownloadChannelRequest>,
) -> ApiResult<Json<DownloadJobResponse>> {
    let kind = parse_media_kind(payload.media_kind.as_deref());
    let job_id = state
        .downloads
        .start_channel_download(payload.video_id, kind)
        .map_err(|err| ApiError::internal(err.to_string()))?;
    Ok(Json(DownloadJobResponse { id: job_id }))
}

async fn get_download_status(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<DownloadJobStatus>> {
    let status = state
        .downloads
        .get_status(&id)
        .ok_or_else(|| ApiError::not_found("download not found"))?;
    Ok(Json(status))
}

async fn serve_www_path(root: &Path, request_path: &str) -> ApiResult<Response> {
    let target = resolve_www_path(root, request_path)?;
    let metadata = tokio::fs::metadata(&target).await;

    match metadata {
        Ok(meta) if meta.is_dir() => {
            let index = root.join("index.html");
            stream_file(index, None, None).await
        }
        Ok(_) => stream_file(target, None, None).await,
        Err(_) => {
            if should_fallback_to_index(request_path) {
                let index = root.join("index.html");
                stream_file(index, None, None).await
            } else {
                Err(ApiError::not_found("file not found"))
            }
        }
    }
}

fn resolve_www_path(root: &Path, request_path: &str) -> ApiResult<PathBuf> {
    let trimmed = request_path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok(root.join("index.html"));
    }
    let candidate = Path::new(trimmed);
    if candidate
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ApiError::not_found("file not found"));
    }
    Ok(root.join(candidate))
}

fn should_fallback_to_index(request_path: &str) -> bool {
    let trimmed = request_path.trim_start_matches('/');
    if trimmed.is_empty() {
        return true;
    }
    let candidate = Path::new(trimmed);
    let has_extension = candidate.extension().is_some();
    !has_extension
}

async fn bootstrap(State(state): State<AppState>) -> ApiResult<Json<BootstrapPayload>> {
    let payload = state.get_bootstrap().await?;
    Ok(Json((*payload).clone()))
}

async fn list_videos(State(state): State<AppState>) -> ApiResult<Json<Vec<VideoRecord>>> {
    let videos = state.get_media_list(MediaCategory::Video).await?;
    Ok(Json(sanitize_video_records(&videos)))
}

async fn list_shorts(State(state): State<AppState>) -> ApiResult<Json<Vec<VideoRecord>>> {
    let shorts = state.get_media_list(MediaCategory::Short).await?;
    Ok(Json(sanitize_video_records(&shorts)))
}

async fn get_video(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<VideoRecord>> {
    let record = state.get_media(MediaCategory::Video, &id).await?;
    Ok(Json(sanitize_video_record(&record)))
}

async fn get_short(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<VideoRecord>> {
    let record = state.get_media(MediaCategory::Short, &id).await?;
    Ok(Json(sanitize_video_record(&record)))
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
    ensure_safe_path_segment(&id)?;
    ensure_safe_path_segment(&code)?;

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
    let path = if let Some(path) = track.path {
        let path = PathBuf::from(path);
        let base = state.files.subtitles.join(&id);
        if path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
            || !path.starts_with(&base)
        {
            return Err(ApiError::not_found("subtitle track not found"));
        }
        path
    } else {
        find_subtitle_file(&state.files.subtitles, &id, &code).await?
    };

    let mime = MimeGuess::from_path(&path).first();
    stream_file(path, mime, None).await
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
    ensure_safe_path_segment(&id)?;
    ensure_safe_path_segment(&file)?;
    let path = state.files.thumbnails.join(&id).join(&file);
    stream_file(path, None, None).await
}

async fn stream_video_file(
    State(state): State<AppState>,
    AxumPath((id, format)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    stream_media(state, MediaCategory::Video, id, format, &headers).await
}

async fn stream_short_file(
    State(state): State<AppState>,
    AxumPath((id, format)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    stream_media(state, MediaCategory::Short, id, format, &headers).await
}

async fn stream_media(
    state: AppState,
    category: MediaCategory,
    id: String,
    format: String,
    headers: &HeaderMap,
) -> ApiResult<Response> {
    ensure_safe_path_segment(&id)?;
    ensure_safe_path_segment(&format)?;

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
        Some(headers),
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
    async fn ensure_fresh_cache(&self) -> ApiResult<()> {
        let version = self
            .reader
            .data_version()
            .await
            .map_err(|err| ApiError::internal(err.to_string()))?;

        let mut last = self.cache.last_db_version.write();
        if let Some(previous) = *last
            && version != previous
        {
            self.cache.clear();
        }
        *last = Some(version);
        Ok(())
    }

    /// Returns a cached snapshot containing everything the SPA needs to boot
    /// without hitting follow-up endpoints (videos, shorts, subtitles,
    /// comments). The heavy lifting runs in a blocking task because SQLite is a
    /// synchronous API.
    async fn get_bootstrap(&self) -> ApiResult<Arc<BootstrapPayload>> {
        self.ensure_fresh_cache().await?;
        if let Some(cached) = self.cache.bootstrap.read().clone() {
            return Ok(cached);
        }

        let videos = self
            .reader
            .list_videos()
            .await
            .map_err(|err| ApiError::internal(err.to_string()))?;
        let shorts = self
            .reader
            .list_shorts()
            .await
            .map_err(|err| ApiError::internal(err.to_string()))?;
        let subtitles = self
            .reader
            .list_subtitles()
            .await
            .map_err(|err| ApiError::internal(err.to_string()))?
            .into_iter()
            .map(sanitize_subtitle_collection)
            .collect();
        let comments = self
            .reader
            .list_all_comments()
            .await
            .map_err(|err| ApiError::internal(err.to_string()))?;
        let payload = BootstrapPayload {
            videos: sanitize_video_records(&videos),
            shorts: sanitize_video_records(&shorts),
            subtitles,
            comments,
        };

        let payload = Arc::new(payload);
        self.cache.bootstrap.write().replace(payload.clone());
        Ok(payload)
    }

    /// Retrieves every video/short record, memoizing both the list and the
    /// individual details map for quick follow-up lookups.
    async fn get_media_list(&self, category: MediaCategory) -> ApiResult<Vec<VideoRecord>> {
        self.ensure_fresh_cache().await?;
        if let Some(cached) = self.cache.media_list(category).read().clone() {
            return Ok(cached);
        }

        let records = match category {
            MediaCategory::Video => self
                .reader
                .list_videos()
                .await
                .map_err(|err| ApiError::internal(err.to_string()))?,
            MediaCategory::Short => self
                .reader
                .list_shorts()
                .await
                .map_err(|err| ApiError::internal(err.to_string()))?,
        };

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
        self.ensure_fresh_cache().await?;
        if let Some(record) = self
            .cache
            .media_details(category)
            .read()
            .get(videoid)
            .cloned()
        {
            return Ok(record);
        }

        let result = match category {
            MediaCategory::Video => self
                .reader
                .get_video(videoid)
                .await
                .map_err(|err| ApiError::internal(err.to_string()))?,
            MediaCategory::Short => self
                .reader
                .get_short(videoid)
                .await
                .map_err(|err| ApiError::internal(err.to_string()))?,
        };

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
        self.ensure_fresh_cache().await?;
        if let Some(cached) = self.cache.comments.read().get(videoid).cloned() {
            return Ok(cached);
        }

        let comments = self
            .reader
            .get_comments(videoid)
            .await
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
        self.ensure_fresh_cache().await?;
        if let Some(cached) = self.cache.subtitles.read().get(videoid).cloned() {
            return Ok(Some(cached));
        }

        let result = self
            .reader
            .get_subtitles(videoid)
            .await
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
    let format_id = source.format_id.trim();
    if format_id.is_empty() {
        return None;
    }
    Some(normalize_format_id(format_id))
}

fn normalize_format_id(value: &str) -> String {
    value
        .chars()
        .map(|c| match c {
            '/' | ':' | ' ' => '_',
            _ => c,
        })
        .collect()
}

/// Validates that a single dynamic path segment never escapes its base folder.
fn ensure_safe_path_segment(value: &str) -> ApiResult<()> {
    if value.is_empty()
        || Path::new(value)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ApiError::not_found("file not found"));
    }

    Ok(())
}

async fn find_subtitle_file(subtitles_root: &Path, id: &str, code: &str) -> ApiResult<PathBuf> {
    let dir = subtitles_root.join(id);
    let mut entries = tokio::fs::read_dir(&dir)
        .await
        .map_err(|_| ApiError::not_found("subtitle track not found"))?;
    let prefix = format!("{id}.{code}.");
    let mut best: Option<(usize, PathBuf)> = None;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|_| ApiError::not_found("subtitle track not found"))?
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = entry
            .file_name()
            .into_string()
            .unwrap_or_else(|os| os.to_string_lossy().into_owned());
        if name.starts_with(&prefix) {
            let ext = Path::new(&name)
                .extension()
                .and_then(|ext| ext.to_str())
                .unwrap_or("");
            let rank = subtitle_extension_rank(ext);
            if rank >= 10 {
                continue;
            }
            match best {
                Some((best_rank, _)) if rank >= best_rank => {}
                _ => best = Some((rank, path)),
            }
        }
    }
    best.map(|(_, path)| path)
        .ok_or_else(|| ApiError::not_found("subtitle track not found"))
}

fn subtitle_extension_rank(ext: &str) -> usize {
    match ext.to_ascii_lowercase().as_str() {
        "vtt" => 0,
        "srv3" => 1,
        "srv2" => 2,
        "srv1" => 3,
        "srt" => 4,
        "ttml" => 5,
        "xml" => 6,
        "ass" => 7,
        _ => 10,
    }
}

async fn stream_file(
    path: PathBuf,
    mime: Option<Mime>,
    headers: Option<&HeaderMap>,
) -> ApiResult<Response> {
    let mut file = File::open(&path)
        .await
        .map_err(|_| ApiError::not_found("file not found"))?;
    let metadata = file
        .metadata()
        .await
        .map_err(|_| ApiError::not_found("file not found"))?;
    let size = metadata.len();

    let guessed = mime.or_else(|| MimeGuess::from_path(&path).first());
    let range = headers
        .and_then(|headers| headers.get(header::RANGE))
        .and_then(|value| parse_range_header(value, size));

    let mut response = if let Some((start, end)) = range {
        if start >= size {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                format!("bytes */{}", size).parse().unwrap(),
            );
            response
        } else {
            let end = end.min(size.saturating_sub(1));
            let length = end - start + 1;
            file.seek(std::io::SeekFrom::Start(start))
                .await
                .map_err(|_| ApiError::not_found("file not found"))?;
            let stream = ReaderStream::new(file.take(length));
            let body = Body::from_stream(stream);
            let mut response = body.into_response();
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", start, end, size).parse().unwrap(),
            );
            response
                .headers_mut()
                .insert(header::CONTENT_LENGTH, length.to_string().parse().unwrap());
            response
        }
    } else {
        let stream = ReaderStream::new(file);
        let body = Body::from_stream(stream);
        body.into_response()
    };

    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
    if let Some(mime) = guessed
        && let Ok(value) = mime.to_string().parse()
    {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }

    Ok(response)
}

fn sanitize_video_records(records: &[VideoRecord]) -> Vec<VideoRecord> {
    records.iter().map(sanitize_video_record).collect()
}

fn sanitize_video_record(record: &VideoRecord) -> VideoRecord {
    let mut clone = record.clone();
    for source in &mut clone.sources {
        source.path = None;
    }
    clone
}

fn sanitize_subtitle_collection(collection: SubtitleCollection) -> SubtitleCollection {
    let mut clone = collection;
    for track in &mut clone.languages {
        track.path = None;
    }
    clone
}

fn parse_range_header(value: &header::HeaderValue, size: u64) -> Option<(u64, u64)> {
    let value = value.to_str().ok()?;
    let value = value.trim();
    let mut parts = value.split('=');
    let unit = parts.next()?.trim();
    if unit != "bytes" {
        return None;
    }
    let range = parts.next()?.trim();
    if range.is_empty() {
        return None;
    }
    let (start_str, end_str) = range.split_once('-')?;

    if start_str.is_empty() {
        // Suffix range: "-N" means last N bytes.
        let suffix_len: u64 = end_str.parse().ok()?;
        if suffix_len == 0 {
            return None;
        }
        if suffix_len >= size {
            return Some((0, size.saturating_sub(1)));
        }
        return Some((size - suffix_len, size.saturating_sub(1)));
    }

    let start: u64 = start_str.parse().ok()?;
    let end = if end_str.is_empty() {
        size.saturating_sub(1)
    } else {
        end_str.parse().ok()?
    };
    if end < start {
        return None;
    }
    Some((start, end))
}

fn env_or_file_value(key: &str, file_vars: &HashMap<String, String>) -> Option<String> {
    std::env::var(key)
        .ok()
        .and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .or_else(|| file_vars.get(key).cloned())
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let tmp_path = path.with_extension("tmp");
    let payload = serde_json::to_vec_pretty(value)?;
    fs::write(&tmp_path, payload)?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

fn write_progress_report(path: &Path, progress: u8, message: &str) {
    let report = ProgressReport {
        progress: progress.min(100),
        message: message.to_string(),
    };
    if let Err(err) = write_json_atomic(path, &report) {
        eprintln!("Failed to write progress report: {err}");
    }
}

fn read_progress_report(path: &Path) -> Option<ProgressReport> {
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

fn update_job_status(
    inner: &DownloadManagerInner,
    job_id: &str,
    status: DownloadStatus,
    message: &str,
) {
    if let Some(job) = inner.jobs.lock().get_mut(job_id) {
        job.status = status;
        job.message = message.to_string();
    }
}

fn media_kind_label(kind: MediaCategory) -> &'static str {
    match kind {
        MediaCategory::Video => "video",
        MediaCategory::Short => "short",
    }
}

fn parse_media_kind(value: Option<&str>) -> MediaCategory {
    match value.map(|value| value.trim().to_ascii_lowercase()) {
        Some(ref value) if value == "short" || value == "shorts" => MediaCategory::Short,
        _ => MediaCategory::Video,
    }
}

fn find_download_channel_executable() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("NEWTUBE_DOWNLOAD_BIN") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            let candidate = PathBuf::from(trimmed);
            if candidate.exists() {
                return Ok(candidate);
            }
        }
    }

    if let Ok(path) = std::env::var("CARGO_BIN_EXE_download_channel") {
        let candidate = PathBuf::from(path);
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    let docker_path = PathBuf::from("/usr/local/bin/download_channel");
    if docker_path.exists() {
        return Ok(docker_path);
    }

    let mut sibling = std::env::current_exe().context("locating backend executable")?;
    sibling.set_file_name("download_channel");
    if sibling.exists() {
        return Ok(sibling);
    }

    bail!("download_channel binary not found");
}

fn run_download_channel(binary: &Path, args: Vec<String>) -> Result<()> {
    let status = std::process::Command::new(binary)
        .args(args)
        .status()
        .context("launching download_channel")?;
    if status.success() {
        Ok(())
    } else {
        bail!("download_channel exited with {}", status)
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    fn iter(&self) -> Box<dyn Iterator<Item = &T> + '_> {
        match self {
            OneOrMany::One(value) => Box::new(std::iter::once(value)),
            OneOrMany::Many(values) => Box::new(values.iter()),
        }
    }
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(untagged)]
enum CreatorInfo {
    Name(String),
    Object {
        url: Option<String>,
        channel_url: Option<String>,
        uploader_url: Option<String>,
    },
}

impl CreatorInfo {
    fn url(&self) -> Option<&str> {
        match self {
            CreatorInfo::Name(_) => None,
            CreatorInfo::Object {
                url,
                channel_url,
                uploader_url,
            } => url
                .as_deref()
                .or(channel_url.as_deref())
                .or(uploader_url.as_deref()),
        }
    }
}

#[derive(Deserialize)]
struct MinimalInfo {
    channel_url: Option<OneOrMany<String>>,
    uploader_url: Option<OneOrMany<String>>,
    #[serde(default)]
    creators: Option<Vec<CreatorInfo>>,
    channel: Option<OneOrMany<CreatorInfo>>,
    uploader: Option<OneOrMany<CreatorInfo>>,
}

fn first_channel_url(info: &MinimalInfo) -> Option<&str> {
    if let Some(channel_urls) = &info.channel_url {
        for url in channel_urls.iter() {
            if !url.trim().is_empty() {
                return Some(url);
            }
        }
    }
    if let Some(uploader_urls) = &info.uploader_url {
        for url in uploader_urls.iter() {
            if !url.trim().is_empty() {
                return Some(url);
            }
        }
    }
    if let Some(channel) = &info.channel {
        for creator in channel.iter() {
            if let Some(url) = creator.url()
                && !url.trim().is_empty()
            {
                return Some(url);
            }
        }
    }
    if let Some(uploader) = &info.uploader {
        for creator in uploader.iter() {
            if let Some(url) = creator.url()
                && !url.trim().is_empty()
            {
                return Some(url);
            }
        }
    }
    if let Some(creators) = &info.creators {
        for creator in creators {
            if let Some(url) = creator.url()
                && !url.trim().is_empty()
            {
                return Some(url);
            }
        }
    }
    None
}

fn video_url_for_kind(video_id: &str, kind: MediaCategory) -> String {
    match kind {
        MediaCategory::Video => format!("https://www.youtube.com/watch?v={video_id}"),
        MediaCategory::Short => format!("https://www.youtube.com/shorts/{video_id}"),
    }
}

fn resolve_channel_url(video_id: &str, kind: MediaCategory) -> Result<String> {
    let video_url = video_url_for_kind(video_id, kind);
    let output = std::process::Command::new("yt-dlp")
        .arg("--dump-single-json")
        .arg("--skip-download")
        .arg("--no-warnings")
        .arg("--no-progress")
        .arg(&video_url)
        .output()
        .with_context(|| format!("fetching metadata for {video_url}"))?;

    if !output.status.success() {
        bail!("yt-dlp failed for {} (status {})", video_url, output.status);
    }

    let info: MinimalInfo =
        serde_json::from_slice(&output.stdout).context("parsing yt-dlp metadata response")?;
    let url = first_channel_url(&info).ok_or_else(|| anyhow!("channel url not found"))?;
    Ok(url.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;
    use axum::{body::to_bytes, extract::State as AxumState};
    use libsql::{Builder, params};
    use serde_json::Value;
    use std::sync::Mutex;
    use std::{env, path::PathBuf, sync::Arc};
    use tempfile::tempdir;

    struct BackendTestContext {
        _temp: tempfile::TempDir,
        db_path: PathBuf,
        store: MetadataStore,
        state: AppState,
    }

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env_file(vars: &[(&str, &str)], f: impl FnOnce()) {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempdir().unwrap();
        let mut contents = String::new();
        for (key, value) in vars {
            contents.push_str(&format!("{key}=\"{value}\"\n"));
        }
        std::fs::write(dir.path().join(".env"), contents).unwrap();
        let cwd = env::current_dir().unwrap();
        env::set_current_dir(dir.path()).unwrap();
        f();
        env::set_current_dir(cwd).unwrap();
    }

    impl BackendTestContext {
        async fn new() -> Self {
            let temp = tempdir().unwrap();
            let db_path = temp.path().join("metadata.db");
            let store = MetadataStore::open(&db_path).await.unwrap();
            let reader = MetadataReader::new(&db_path).await.unwrap();
            let files = FilePaths::for_base(temp.path());
            let www_root = temp.path().join("www");
            std::fs::create_dir_all(&www_root).unwrap();

            Self {
                state: AppState {
                    reader: Arc::new(reader),
                    cache: Arc::new(ApiCache::new()),
                    files: Arc::new(files),
                    www_root: Arc::new(www_root),
                    settings: Arc::new(SettingsStore::load(
                        temp.path(),
                        InstanceSettings {
                            missing_media_behavior: MissingMediaBehavior::NotFound,
                        },
                    )),
                    downloads: DownloadManager::new(
                        temp.path().to_path_buf(),
                        temp.path().join("www"),
                    ),
                },
                db_path,
                store,
                _temp: temp,
            }
        }

        async fn insert_video(&mut self, id: &str) {
            self.store.upsert_video(&sample_video(id)).await.unwrap();
        }

        async fn insert_short(&mut self, id: &str) {
            self.store.upsert_short(&sample_video(id)).await.unwrap();
        }

        async fn insert_subtitles(&mut self, id: &str, tracks: Vec<SubtitleTrack>) {
            self.store
                .upsert_subtitles(&SubtitleCollection {
                    videoid: id.into(),
                    languages: tracks,
                })
                .await
                .unwrap();
        }

        async fn insert_comments(&mut self, id: &str, comments: Vec<CommentRecord>) {
            self.store
                .replace_comments(id, &comments)
                .await
                .expect("comments persisted");
        }

        async fn delete_by_videoid(&self, table: &str, value: &str) {
            let db = Builder::new_local(&self.db_path).build().await.unwrap();
            let conn = db.connect().unwrap();
            conn.execute(
                &format!("DELETE FROM {table} WHERE videoid = ?1"),
                params![value],
            )
            .await
            .unwrap();
        }
    }

    fn sample_video(id: &str) -> VideoRecord {
        VideoRecord {
            videoid: id.into(),
            title: format!("Video {id}"),
            description: "desc".into(),
            likes: Some(1),
            dislikes: Some(0),
            views: Some(10),
            upload_date: Some("2024-01-01T00:00:00Z".into()),
            author: Some("Channel".into()),
            subscriber_count: Some(100),
            duration: Some(60),
            duration_text: Some("1:00".into()),
            channel_url: Some("https://example.test/channel".into()),
            thumbnail_url: Some("/thumb.jpg".into()),
            tags: vec![],
            thumbnails: vec![],
            extras: json!(null),
            sources: vec![VideoSource {
                format_id: "1080p".into(),
                quality_label: Some("1080p".into()),
                width: Some(1920),
                height: Some(1080),
                fps: Some(30.0),
                mime_type: Some("video/mp4".into()),
                ext: Some("mp4".into()),
                file_size: Some(1024),
                url: format!("/api/videos/{id}/streams/1080p"),
                path: None,
            }],
        }
    }

    fn sample_comment(id: &str, videoid: &str) -> CommentRecord {
        CommentRecord {
            id: id.into(),
            videoid: videoid.into(),
            author: "tester".into(),
            text: "hello world".into(),
            likes: Some(1),
            time_posted: Some("2024-01-01T00:00:00Z".into()),
            parent_comment_id: None,
            status_likedbycreator: false,
            reply_count: Some(0),
        }
    }

    fn parse_backend_args(env_values: &[(&str, &str)], extra: &[&str]) -> BackendArgs {
        let argv = extra
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let mut parsed = None;
        with_env_file(env_values, || {
            parsed = Some(BackendArgs::from_iter(argv.clone()).expect("parsed args"));
        });
        parsed.expect("args set")
    }

    #[test]
    fn backend_args_default_media_root() {
        let args = parse_backend_args(
            &[
                ("MEDIA_ROOT", "/yt/test"),
                ("WWW_ROOT", "/www/test"),
                ("NEWTUBE_PORT", "4242"),
                ("NEWTUBE_HOST", "127.0.0.1"),
            ],
            &[],
        );
        assert_eq!(args.media_root, PathBuf::from("/yt/test"));
        assert_eq!(args.www_root, PathBuf::from("/www/test"));
        assert_eq!(args.newtube_port, 4242);
    }

    #[test]
    fn backend_args_override_media_root() {
        let args = parse_backend_args(
            &[
                ("MEDIA_ROOT", "/yt/test"),
                ("WWW_ROOT", "/www/test"),
                ("NEWTUBE_PORT", "4242"),
                ("NEWTUBE_HOST", "127.0.0.1"),
            ],
            &["--media-root", "/custom/media"],
        );
        assert_eq!(args.media_root, PathBuf::from("/custom/media"));
    }

    #[test]
    fn backend_args_override_www_root() {
        let args = parse_backend_args(
            &[
                ("MEDIA_ROOT", "/yt/test"),
                ("WWW_ROOT", "/www/test"),
                ("NEWTUBE_PORT", "4242"),
                ("NEWTUBE_HOST", "127.0.0.1"),
            ],
            &["--www-root", "/custom/www"],
        );
        assert_eq!(args.www_root, PathBuf::from("/custom/www"));
    }

    #[test]
    fn backend_args_override_port() {
        let args = parse_backend_args(
            &[
                ("MEDIA_ROOT", "/yt/test"),
                ("WWW_ROOT", "/www/test"),
                ("NEWTUBE_PORT", "4242"),
                ("NEWTUBE_HOST", "127.0.0.1"),
            ],
            &["--port", "9000"],
        );
        assert_eq!(args.newtube_port, 9000);
    }

    #[test]
    fn backend_args_override_host() {
        let args = parse_backend_args(
            &[
                ("MEDIA_ROOT", "/yt/test"),
                ("WWW_ROOT", "/www/test"),
                ("NEWTUBE_PORT", "4242"),
                ("NEWTUBE_HOST", "127.0.0.1"),
            ],
            &["--host", "0.0.0.0"],
        );
        assert_eq!(args.listen_host, "0.0.0.0".parse::<IpAddr>().unwrap());
    }

    #[tokio::test]
    async fn bootstrap_caches_payload() {
        let mut ctx = BackendTestContext::new().await;
        ctx.insert_video("alpha").await;
        ctx.insert_short("beta").await;
        ctx.insert_subtitles(
            "alpha",
            vec![SubtitleTrack {
                code: "en".into(),
                name: "English".into(),
                url: "/api/videos/alpha/subtitles/en".into(),
                path: None,
            }],
        )
        .await;
        ctx.insert_comments("alpha", vec![sample_comment("1", "alpha")])
            .await;

        let first = ctx.state.get_bootstrap().await.unwrap();
        assert_eq!(first.videos.len(), 1);

        ctx.insert_video("gamma").await;
        let second = ctx.state.get_bootstrap().await.unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(second.videos.iter().any(|video| video.videoid == "gamma"));
    }

    #[tokio::test]
    async fn media_list_populates_cache() {
        let mut ctx = BackendTestContext::new().await;
        ctx.insert_video("alpha").await;

        let list = ctx
            .state
            .get_media_list(MediaCategory::Video)
            .await
            .unwrap();
        assert_eq!(list.len(), 1);
        ctx.delete_by_videoid("videos", "alpha").await;

        let cached = ctx
            .state
            .get_media_list(MediaCategory::Video)
            .await
            .unwrap();
        assert_eq!(cached.len(), 0);
    }

    #[tokio::test]
    async fn api_responses_strip_file_paths() {
        let ctx = BackendTestContext::new().await;
        let mut video = sample_video("alpha");
        video.sources[0].path = Some("/yt/videos/alpha/secret.mp4".into());
        ctx.store.upsert_video(&video).await.unwrap();

        let Json(videos) = super::list_videos(AxumState(ctx.state.clone()))
            .await
            .unwrap();
        assert!(videos[0].sources[0].path.is_none());

        let Json(single) = super::get_video(AxumState(ctx.state.clone()), AxumPath("alpha".into()))
            .await
            .unwrap();
        assert!(single.sources[0].path.is_none());

        let bootstrap = ctx.state.get_bootstrap().await.unwrap();
        assert!(bootstrap.videos[0].sources[0].path.is_none());
    }

    #[tokio::test]
    async fn media_lookup_prefers_cache() {
        let mut ctx = BackendTestContext::new().await;
        ctx.insert_video("alpha").await;
        let record = ctx
            .state
            .get_media(MediaCategory::Video, "alpha")
            .await
            .unwrap();
        assert_eq!(record.videoid, "alpha");

        ctx.delete_by_videoid("videos", "alpha").await;
        let err = ctx
            .state
            .get_media(MediaCategory::Video, "alpha")
            .await
            .unwrap_err();
        assert_eq!(err.status, StatusCode::NOT_FOUND);

        let err = ctx
            .state
            .get_media(MediaCategory::Video, "ghost")
            .await
            .unwrap_err();
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn comments_and_subtitles_cache() {
        let mut ctx = BackendTestContext::new().await;
        ctx.insert_video("alpha").await;
        ctx.insert_comments("alpha", vec![sample_comment("1", "alpha")])
            .await;
        ctx.insert_subtitles(
            "alpha",
            vec![SubtitleTrack {
                code: "en".into(),
                name: "English".into(),
                url: "/sub".into(),
                path: None,
            }],
        )
        .await;

        let first_comments = ctx.state.get_comments("alpha").await.unwrap();
        assert_eq!(first_comments.len(), 1);
        ctx.delete_by_videoid("comments", "alpha").await;
        let cached_comments = ctx.state.get_comments("alpha").await.unwrap();
        assert_eq!(cached_comments.len(), 0);

        let first_subtitles = ctx.state.get_subtitles("alpha").await.unwrap();
        assert!(first_subtitles.is_some());
        ctx.delete_by_videoid("subtitles", "alpha").await;
        let cached_subtitles = ctx.state.get_subtitles("alpha").await.unwrap();
        assert!(cached_subtitles.is_none());
    }

    #[tokio::test]
    async fn list_subtitles_includes_download_urls() {
        let mut ctx = BackendTestContext::new().await;
        ctx.insert_video("alpha").await;
        ctx.insert_subtitles(
            "alpha",
            vec![SubtitleTrack {
                code: "en".into(),
                name: "English".into(),
                url: "/api/videos/alpha/subtitles/en".into(),
                path: None,
            }],
        )
        .await;

        let Json(payload) = super::list_subtitles(ctx.state.clone(), "alpha".into(), "videos")
            .await
            .unwrap();
        assert_eq!(payload.len(), 1);
        assert!(payload[0].url.contains("/videos/alpha/subtitles/en"));
    }

    #[tokio::test]
    async fn download_subtitle_uses_fallback_path() {
        let mut ctx = BackendTestContext::new().await;
        ctx.insert_video("alpha").await;
        ctx.insert_subtitles(
            "alpha",
            vec![SubtitleTrack {
                code: "en".into(),
                name: "English".into(),
                url: "/api/videos/alpha/subtitles/en".into(),
                path: None,
            }],
        )
        .await;

        let subtitle_dir = ctx.state.files.subtitles.join("alpha");
        std::fs::create_dir_all(&subtitle_dir).unwrap();
        std::fs::write(subtitle_dir.join("alpha.en.vtt"), "WEBVTT").unwrap();

        let response = download_subtitle(ctx.state.clone(), "alpha".into(), "en".into())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn download_thumbnail_serves_local_files() {
        let ctx = BackendTestContext::new().await;
        let thumb_dir = ctx.state.files.thumbnails.join("alpha");
        std::fs::create_dir_all(&thumb_dir).unwrap();
        std::fs::write(thumb_dir.join("poster.png"), b"PNG").unwrap();

        let response = download_thumbnail(ctx.state.clone(), "alpha".into(), "poster.png".into())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"PNG");
    }

    #[tokio::test]
    async fn download_thumbnail_rejects_path_traversal() {
        let ctx = BackendTestContext::new().await;
        let err = download_thumbnail(ctx.state.clone(), "alpha".into(), "../secret.txt".into())
            .await
            .unwrap_err();
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn stream_media_uses_custom_path() {
        let ctx = BackendTestContext::new().await;
        let mut video = sample_video("alpha");
        let custom = ctx.state.files.videos.join("custom.mp4");
        std::fs::create_dir_all(custom.parent().unwrap()).unwrap();
        std::fs::write(&custom, "bytes").unwrap();
        video.sources[0].path = Some(custom.to_string_lossy().into_owned());
        ctx.store.upsert_video(&video).await.unwrap();

        let response = stream_media(
            ctx.state.clone(),
            MediaCategory::Video,
            "alpha".into(),
            "1080p".into(),
            &HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "video/mp4"
        );
    }

    #[tokio::test]
    async fn stream_media_builds_default_path() {
        let ctx = BackendTestContext::new().await;
        let mut video = sample_video("alpha");
        video.sources[0].path = None;
        ctx.store.upsert_video(&video).await.unwrap();
        let media_dir = ctx
            .state
            .files
            .media_dir(MediaCategory::Video)
            .join("alpha");
        std::fs::create_dir_all(&media_dir).unwrap();
        std::fs::write(media_dir.join("alpha_1080p.mp4"), "bytes").unwrap();

        let response = stream_media(
            ctx.state.clone(),
            MediaCategory::Video,
            "alpha".into(),
            "1080p".into(),
            &HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn stream_media_missing_format_errors() {
        let mut ctx = BackendTestContext::new().await;
        ctx.insert_video("alpha").await;
        let err = stream_media(
            ctx.state.clone(),
            MediaCategory::Video,
            "alpha".into(),
            "4k".into(),
            &HeaderMap::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn api_error_serializes_json() {
        let response = ApiError::not_found("missing").into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["error"], "missing");
    }
}
