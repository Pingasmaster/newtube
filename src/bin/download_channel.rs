#![forbid(unsafe_code)]

//! Command-line helper that downloads whole YouTube channels and builds the
//! on-disk cache that the ViewTube backend serves.
//!
//! The binary intentionally documents every moving piece: directory layout,
//! yt-dlp invocations, and metadata normalization. This makes it trivial to
//! tweak behaviour without re-reading the entire file.

use anyhow::{Context, Result, bail};
use chrono::{NaiveDate, Utc};
use newtube_tools::config::{DEFAULT_CONFIG_PATH, load_runtime_paths_from};
use newtube_tools::metadata::{
    CommentRecord, MetadataStore, SubtitleCollection, SubtitleTrack, VideoRecord, VideoSource,
};
use newtube_tools::security::ensure_not_root;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
#[cfg(test)]
use std::sync::{Mutex, MutexGuard};

#[cfg(test)]
const DEFAULT_MEDIA_ROOT: &str = "/yt";
const VIDEOS_SUBDIR: &str = "videos";
const SHORTS_SUBDIR: &str = "shorts";
const SUBTITLES_SUBDIR: &str = "subtitles";
const THUMBNAILS_SUBDIR: &str = "thumbnails";
const COMMENTS_SUBDIR: &str = "comments";
const ARCHIVE_FILE: &str = "download-archive.txt";
const COOKIES_FILE: &str = "cookies.txt";
#[cfg(test)]
const DEFAULT_WWW_ROOT: &str = "/www/newtube.com";
const METADATA_DB_FILE: &str = "metadata.db";

#[cfg(test)]
static YT_DLP_STUB: Mutex<Option<PathBuf>> = Mutex::new(None);
#[cfg(test)]
static STUB_USE_LOCK: Mutex<()> = Mutex::new(());

fn yt_dlp_command() -> Command {
    #[cfg(test)]
    {
        if let Some(path) = YT_DLP_STUB.lock().unwrap().clone() {
            return Command::new(path);
        }
    }
    Command::new("yt-dlp")
}

#[cfg(test)]
fn set_ytdlp_stub_path(path: PathBuf) -> YtDlpStubGuard {
    let guard = STUB_USE_LOCK.lock().unwrap();
    {
        let mut lock = YT_DLP_STUB.lock().unwrap();
        *lock = Some(path);
    }
    YtDlpStubGuard { lock: Some(guard) }
}

#[cfg(test)]
struct YtDlpStubGuard {
    lock: Option<MutexGuard<'static, ()>>,
}

#[cfg(test)]
impl Drop for YtDlpStubGuard {
    fn drop(&mut self) {
        *YT_DLP_STUB.lock().unwrap() = None;
        self.lock.take();
    }
}

/// Convenience wrapper around every filesystem location this binary touches.
struct Paths {
    base: PathBuf,
    videos: PathBuf,
    shorts: PathBuf,
    subtitles: PathBuf,
    thumbnails: PathBuf,
    comments: PathBuf,
    archive: PathBuf,
    cookies: PathBuf,
    www_root: PathBuf,
    metadata_db: PathBuf,
}

#[derive(Debug, Clone)]
struct DownloaderArgs {
    channel_url: String,
    media_root: PathBuf,
    www_root: PathBuf,
}

impl DownloaderArgs {
    fn parse() -> Result<Self> {
        Self::from_iter(env::args().skip(1))
    }

    #[cfg(test)]
    fn from_slice(values: &[&str]) -> Result<Self> {
        Self::from_iter(values.iter().map(|value| value.to_string()))
    }

    fn from_iter<I>(iter: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut media_root_override: Option<PathBuf> = None;
        let mut www_root_override: Option<PathBuf> = None;
        let mut config_path = PathBuf::from(DEFAULT_CONFIG_PATH);
        let mut channel_url: Option<String> = None;
        let mut args = iter.into_iter();

        while let Some(arg) = args.next() {
            if arg == "--" {
                for value in args {
                    Self::set_channel(&mut channel_url, value)?;
                }
                break;
            }

            if let Some(value) = arg.strip_prefix("--media-root=") {
                media_root_override = Some(PathBuf::from(value));
                continue;
            }
            if let Some(value) = arg.strip_prefix("--www-root=") {
                www_root_override = Some(PathBuf::from(value));
                continue;
            }
            if let Some(value) = arg.strip_prefix("--config=") {
                config_path = PathBuf::from(value);
                continue;
            }

            match arg.as_str() {
                "--media-root" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--media-root requires a value"))?;
                    media_root_override = Some(PathBuf::from(value));
                }
                "--www-root" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--www-root requires a value"))?;
                    www_root_override = Some(PathBuf::from(value));
                }
                "--config" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--config requires a value"))?;
                    config_path = PathBuf::from(value);
                }
                _ if arg.starts_with('-') => {
                    bail!("unknown argument: {arg}");
                }
                _ => {
                    Self::set_channel(&mut channel_url, arg)?;
                }
            }
        }

        let channel_url = channel_url.ok_or_else(|| {
            anyhow::anyhow!(
                "Usage: download_channel [--config <path>] [--media-root <path>] [--www-root <path>] <channel_url>"
            )
        })?;

        let runtime_paths = load_runtime_paths_from(&config_path)?;
        let media_root = media_root_override.unwrap_or_else(|| runtime_paths.media_root.clone());
        let www_root = www_root_override.unwrap_or_else(|| runtime_paths.www_root.clone());

        Ok(Self {
            channel_url,
            media_root,
            www_root,
        })
    }

    fn set_channel(target: &mut Option<String>, value: String) -> Result<()> {
        if target.is_some() {
            bail!("channel URL specified multiple times");
        }
        *target = Some(value);
        Ok(())
    }
}

/// Minimal version of yt-dlp's `info.json` just to extract available formats.
#[derive(Deserialize)]
struct InfoJson {
    #[serde(default)]
    formats: Vec<FormatEntry>,
}

#[derive(Deserialize)]
struct FormatEntry {
    #[serde(rename = "format_id")]
    format_id: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
/// Full `yt-dlp --dump-single-json` payload. Only a subset of fields are read
/// but everything is left optional because older videos may lack metadata.
struct VideoInfo {
    id: String,
    title: Option<String>,
    fulltitle: Option<String>,
    description: Option<String>,
    like_count: Option<i64>,
    dislike_count: Option<i64>,
    view_count: Option<i64>,
    upload_date: Option<String>,
    #[serde(default)]
    release_timestamp: Option<i64>,
    uploader: Option<String>,
    channel: Option<String>,
    channel_id: Option<String>,
    channel_url: Option<String>,
    #[serde(rename = "channel_follower_count")]
    channel_follower_count: Option<i64>,
    duration: Option<i64>,
    #[serde(rename = "duration_string")]
    duration_string: Option<String>,
    thumbnails: Option<Vec<ThumbnailInfo>>,
    tags: Option<Vec<String>>,
    comment_count: Option<i64>,
    #[serde(default)]
    subtitles: Option<HashMap<String, Vec<SubtitleInfo>>>,
    #[serde(default, rename = "automatic_captions")]
    automatic_captions: Option<HashMap<String, Vec<SubtitleInfo>>>,
    formats: Option<Vec<FormatInfo>>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ThumbnailInfo {
    url: Option<String>,
    width: Option<i64>,
    height: Option<i64>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct SubtitleInfo {
    url: Option<String>,
    ext: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FormatInfo {
    #[serde(rename = "format_id")]
    format_id: Option<String>,
    format_note: Option<String>,
    width: Option<i64>,
    height: Option<i64>,
    fps: Option<f64>,
    ext: Option<String>,
    vcodec: Option<String>,
    acodec: Option<String>,
    filesize: Option<i64>,
    #[serde(rename = "filesize_approx")]
    filesize_approx: Option<i64>,
    #[serde(rename = "dynamic_range")]
    dynamic_range: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct RawComment {
    id: String,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    like_count: Option<i64>,
    #[serde(default)]
    timestamp: Option<i64>,
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    author_is_uploader: bool,
    #[serde(default)]
    author_is_channel_owner: bool,
    #[serde(default)]
    is_favorited: bool,
    #[serde(default)]
    reply_count: Option<i64>,
    #[serde(default)]
    time_text: Option<String>,
}

/// Distinguishes long-form uploads from Shorts so we can route files to the
/// right directory and API slug.
#[derive(Clone, Copy)]
enum MediaKind {
    Video,
    Short,
}

/// CLI entry point. Validates prerequisites, prepares directories, and kicks
/// off downloads for both standard uploads and Shorts.
fn main() -> Result<()> {
    ensure_not_root("download_channel")?;

    let DownloaderArgs {
        channel_url,
        media_root,
        www_root,
    } = DownloaderArgs::parse()?;

    ensure_program_available("yt-dlp")?;

    let paths = Paths::with_roots(&media_root, &www_root);
    paths.prepare()?;
    let mut metadata =
        MetadataStore::open(&paths.metadata_db).context("initializing metadata database")?;

    println!("===================================");
    println!("YouTube Channel Downloader");
    println!("===================================");
    println!("Channel: {}", channel_url);
    println!("Base directory: {}", paths.base.display());
    println!("WWW root: {}", paths.www_root.display());
    println!();

    println!("Starting download process...");
    println!();

    let mut archive = load_archive(&paths.archive)?;

    download_collection(
        "regular videos",
        format!("{}/videos", &channel_url),
        Some("!is_live & original_url!*=/shorts/"),
        &paths,
        &mut archive,
        MediaKind::Video,
        &mut metadata,
    )?;

    download_collection(
        "shorts",
        format!("{}/shorts", &channel_url),
        Some("original_url*=/shorts/"),
        &paths,
        &mut archive,
        MediaKind::Short,
        &mut metadata,
    )?;

    println!();
    println!("===================================");
    println!("Download complete!");
    println!("===================================");
    println!("Videos: {}", paths.videos.display());
    println!("Shorts: {}", paths.shorts.display());
    println!("Subtitles: {}", paths.subtitles.display());
    println!("Thumbnails: {}", paths.thumbnails.display());
    println!("Archive: {}", paths.archive.display());
    println!();
    println!("Metadata files:");
    println!("  - <video_id>.info.json (video metadata)");
    println!("  - <video_id>.description (video description)");
    println!("  - <video_id>.jpg (thumbnail)");
    println!();
    println!("Next steps:");
    println!("1. Download likes/dislikes data separately");
    println!("2. Download comments data separately");
    println!("3. Process .info.json files to populate IndexedDB");

    Ok(())
}

impl Paths {
    /// Builds the struct using the provided media and www roots.
    fn with_roots(media_root: &Path, www_root: &Path) -> Self {
        let base = media_root.to_path_buf();
        let videos = base.join(VIDEOS_SUBDIR);
        let shorts = base.join(SHORTS_SUBDIR);
        let subtitles = base.join(SUBTITLES_SUBDIR);
        let thumbnails = base.join(THUMBNAILS_SUBDIR);
        let comments = base.join(COMMENTS_SUBDIR);
        let archive = base.join(ARCHIVE_FILE);
        let cookies = base.join(COOKIES_FILE);
        let www_root = www_root.to_path_buf();
        let metadata_db = base.join(METADATA_DB_FILE);

        Self {
            base,
            videos,
            shorts,
            subtitles,
            thumbnails,
            comments,
            archive,
            cookies,
            www_root,
            metadata_db,
        }
    }

    /// Creates every directory we might write to. This allows subsequent steps
    /// to assume the filesystem exists.
    fn prepare(&self) -> Result<()> {
        fs::create_dir_all(&self.videos)
            .with_context(|| format!("creating {}", self.videos.display()))?;
        fs::create_dir_all(&self.shorts)
            .with_context(|| format!("creating {}", self.shorts.display()))?;
        fs::create_dir_all(&self.subtitles)
            .with_context(|| format!("creating {}", self.subtitles.display()))?;
        fs::create_dir_all(&self.thumbnails)
            .with_context(|| format!("creating {}", self.thumbnails.display()))?;
        fs::create_dir_all(&self.comments)
            .with_context(|| format!("creating {}", self.comments.display()))?;
        fs::create_dir_all(&self.www_root)
            .with_context(|| format!("creating {}", self.www_root.display()))?;
        Ok(())
    }

    /// Returns the on-disk directory for the provided media kind.
    fn media_dir(&self, kind: MediaKind) -> &Path {
        match kind {
            MediaKind::Video => &self.videos,
            MediaKind::Short => &self.shorts,
        }
    }
}

#[cfg(test)]
impl Paths {
    fn from_base(base: &Path) -> Self {
        let www_root = base.join("www");
        Self::with_roots(base, &www_root)
    }
}

/// Runs `<name> --version` to fail loudly when dependencies such as yt-dlp are
/// missing.
fn ensure_program_available(name: &str) -> Result<()> {
    let status = Command::new(name)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(_) => bail!("{} is installed but returned a failure status", name),
        Err(err) => bail!("{} is not installed or not in PATH: {}", name, err),
    }
}

/// Parses yt-dlp's archive file to avoid duplicate downloads.
fn load_archive(path: &Path) -> Result<HashSet<String>> {
    if !path.exists() {
        return Ok(HashSet::new());
    }

    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut entries = HashSet::new();

    for line in reader.lines() {
        let line = line?;
        if let Some(id) = line.split_whitespace().last()
            && !id.is_empty()
        {
            entries.insert(id.to_owned());
        }
    }

    Ok(entries)
}

/// Mirrors yt-dlp's archive format by writing `youtube <id>` per line.
fn append_to_archive(path: &Path, video_id: &str) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening archive {}", path.display()))?;
    writeln!(file, "youtube {}", video_id)
        .with_context(|| format!("writing archive entry for {}", video_id))?;
    Ok(())
}

/// Given a playlist (videos, Shorts, etc.), download each entry and refresh its
/// metadata.
fn download_collection(
    label: &str,
    list_url: String,
    filter: Option<&str>,
    paths: &Paths,
    archive: &mut HashSet<String>,
    media_kind: MediaKind,
    metadata: &mut MetadataStore,
) -> Result<()> {
    println!("Getting list of {}...", label);

    let ids = get_video_ids(&list_url, filter)?;

    if ids.is_empty() {
        println!("No {} found", label);
        println!();
        return Ok(());
    }

    let total = ids.len();
    println!("Found {} {}", total, label);
    println!();

    for (index, video_id) in ids.iter().enumerate() {
        let current = index + 1;
        if let Err(err) = process_media_entry(
            video_id, current, total, paths, archive, media_kind, metadata,
        ) {
            eprintln!("  Warning: failed to process {}: {}", video_id, err);
        }
    }

    println!();
    println!(
        "{} download complete!",
        label
            .chars()
            .next()
            .map(|c| c.to_uppercase().to_string() + &label[1..])
            .unwrap_or_else(|| label.to_string()),
    );
    println!();

    Ok(())
}

/// Handles a single video/short: download media if missing, then refresh all
/// metadata artifacts.
fn process_media_entry(
    video_id: &str,
    current: usize,
    total: usize,
    paths: &Paths,
    archive: &mut HashSet<String>,
    media_kind: MediaKind,
    metadata: &mut MetadataStore,
) -> Result<()> {
    let output_dir = paths.media_dir(media_kind);
    // Archive entries let us skip heavy downloads when the file tree already
    // contains every muxed format. We still refresh metadata because stats can
    // change over time.
    let already_downloaded = archive.contains(video_id);
    let video_url = format!("https://www.youtube.com/watch?v={video_id}");

    if already_downloaded {
        println!(
            "[{}/{}] Refreshing metadata for {}",
            current, total, video_id
        );
    } else {
        println!(
            "[{}/{}] Downloading and indexing {}",
            current, total, video_id
        );
        if let Err(err) = download_video_all_formats(video_id, output_dir, paths) {
            eprintln!("  Warning: failed to download {}: {}", video_id, err);
        } else {
            append_to_archive(&paths.archive, video_id)?;
            archive.insert(video_id.to_owned());
        }
    }

    if let Err(err) = refresh_metadata(
        video_id, &video_url, output_dir, paths, media_kind, metadata,
    ) {
        eprintln!(
            "  Warning: metadata refresh failed for {}: {}",
            video_id, err
        );
    }

    Ok(())
}

/// Fetches info JSON, updates DB rows, and syncs subtitles/comments.
fn refresh_metadata(
    video_id: &str,
    video_url: &str,
    output_dir: &Path,
    paths: &Paths,
    media_kind: MediaKind,
    metadata: &mut MetadataStore,
) -> Result<()> {
    let info = fetch_video_info(video_id, video_url, output_dir, paths)?;
    let record = build_video_record(video_id, &info, output_dir, media_kind, paths)?;

    match media_kind {
        MediaKind::Video => metadata.upsert_video(&record)?,
        MediaKind::Short => metadata.upsert_short(&record)?,
    }

    let subtitles = collect_subtitles(video_id, &info, paths, media_kind)?;
    metadata.upsert_subtitles(&subtitles)?;

    let comments = fetch_comments(video_id, video_url, paths)?;
    metadata.replace_comments(video_id, &comments)?;

    Ok(())
}

/// Runs `yt-dlp --dump-single-json` and caches the response alongside the
/// downloaded assets.
fn fetch_video_info(
    video_id: &str,
    video_url: &str,
    output_dir: &Path,
    paths: &Paths,
) -> Result<VideoInfo> {
    let mut command = yt_dlp_command();
    command
        .arg("--dump-single-json")
        .arg("--skip-download")
        .arg("--no-warnings")
        .arg("--no-progress")
        .arg(video_url);

    if paths.cookies.exists() {
        command
            .arg("--cookies")
            .arg(paths.cookies.to_string_lossy().to_string());
    }

    let output = command
        .output()
        .with_context(|| format!("fetching metadata for {}", video_url))?;

    if !output.status.success() {
        bail!(
            "metadata command failed for {} (status {})",
            video_url,
            output.status
        );
    }

    let raw_json =
        String::from_utf8(output.stdout).context("parsing metadata JSON response as UTF-8")?;
    let info: VideoInfo = serde_json::from_str(&raw_json).context("deserializing metadata JSON")?;

    let info_dir = output_dir.join(video_id);
    fs::create_dir_all(&info_dir)
        .with_context(|| format!("ensuring info directory {}", info_dir.display()))?;

    let info_path = info_dir.join(format!("{}.info.json", video_id));
    fs::write(&info_path, raw_json).with_context(|| format!("writing {}", info_path.display()))?;

    if let Some(description) = &info.description {
        let desc_path = info_dir.join(format!("{}.description", video_id));
        fs::write(&desc_path, description)
            .with_context(|| format!("writing {}", desc_path.display()))?;
    }

    Ok(info)
}

/// Translates `VideoInfo` from yt-dlp into the structured `VideoRecord` that
/// the backend expects.
fn build_video_record(
    video_id: &str,
    info: &VideoInfo,
    output_dir: &Path,
    media_kind: MediaKind,
    paths: &Paths,
) -> Result<VideoRecord> {
    let title = info
        .fulltitle
        .as_deref()
        .or(info.title.as_deref())
        .filter(|t| !t.is_empty())
        .unwrap_or(video_id);

    let description = info.description.clone().unwrap_or_default();

    let upload_date = info
        .upload_date
        .as_deref()
        .and_then(upload_date_to_iso)
        .or_else(|| info.release_timestamp.and_then(timestamp_to_iso));

    let duration = info.duration;
    let duration_text = info
        .duration_string
        .clone()
        .or_else(|| duration.map(format_duration));

    let author = info.channel.clone().or_else(|| info.uploader.clone());

    let slug = media_kind_slug(media_kind);

    let thumbnails = collect_thumbnails(video_id, paths, slug)?;
    let thumbnail_url = thumbnails.first().cloned();

    let sources = collect_sources(video_id, info, output_dir, slug)?;

    let extras = json!({
        "channelId": info.channel_id,
        "commentCount": info.comment_count,
    });

    Ok(VideoRecord {
        videoid: video_id.to_owned(),
        title: title.to_owned(),
        description,
        likes: info.like_count,
        dislikes: info.dislike_count,
        views: info.view_count,
        upload_date,
        author,
        subscriber_count: info.channel_follower_count,
        duration,
        duration_text,
        channel_url: info.channel_url.clone(),
        thumbnail_url,
        tags: info.tags.clone().unwrap_or_default(),
        thumbnails,
        extras,
        sources,
    })
}

/// Gathers subtitle tracks saved locally, falling back to the remote URL when
/// nothing has been downloaded yet.
fn collect_subtitles(
    video_id: &str,
    info: &VideoInfo,
    paths: &Paths,
    media_kind: MediaKind,
) -> Result<SubtitleCollection> {
    let slug = media_kind_slug(media_kind);
    let subtitles_dir = paths.subtitles.join(video_id);
    let mut tracks = Vec::new();
    let display_names = subtitle_name_map(info);

    if subtitles_dir.exists() {
        for entry in fs::read_dir(&subtitles_dir)
            .with_context(|| format!("reading subtitles dir {}", subtitles_dir.display()))?
        {
            let entry = entry?;
            if !entry.path().is_file() {
                continue;
            }

            let file_name = entry
                .file_name()
                .into_string()
                .unwrap_or_else(|os| os.to_string_lossy().into_owned());

            let (without_ext, _ext) = match file_name.rsplit_once('.') {
                Some(parts) => parts,
                None => continue,
            };

            let prefix = format!("{video_id}.");
            let code = match without_ext.strip_prefix(&prefix) {
                Some(code) => code,
                None => continue,
            };

            let name = display_names
                .get(code)
                .cloned()
                .unwrap_or_else(|| code.to_ascii_uppercase());

            tracks.push(SubtitleTrack {
                code: code.to_owned(),
                name,
                url: format!("/api/{slug}/{}/subtitles/{}", video_id, code),
                path: Some(entry.path().to_string_lossy().into_owned()),
            });
        }
    }

    if tracks.is_empty() {
        // If nothing was saved locally we still return the first remote track so
        // the frontend can show at least a single caption option.
        if let Some(remote) = first_remote_subtitle(info) {
            tracks.push(remote);
        }
    }

    Ok(SubtitleCollection {
        videoid: video_id.to_owned(),
        languages: tracks,
    })
}

/// Builds a mapping of language code -> display name using both manual and
/// automatic subtitle entries.
fn subtitle_name_map(info: &VideoInfo) -> HashMap<String, String> {
    let mut names = HashMap::new();
    if let Some(subs) = &info.subtitles {
        for (code, entries) in subs {
            if let Some(entry) = entries.first()
                && let Some(name) = &entry.name
            {
                names.insert(code.to_owned(), name.to_owned());
            }
        }
    }
    if let Some(auto) = &info.automatic_captions {
        for (code, entries) in auto {
            if let Some(entry) = entries.first()
                && let Some(name) = &entry.name
            {
                names
                    .entry(code.to_owned())
                    .or_insert_with(|| name.to_owned());
            }
        }
    }
    names
}

/// Helper that returns the first remote subtitle entry so the frontend can
/// still offer captions even if local downloads failed.
fn first_remote_subtitle(info: &VideoInfo) -> Option<SubtitleTrack> {
    let iter = info.subtitles.iter().chain(info.automatic_captions.iter());

    for map in iter {
        for (code, entries) in map {
            if let Some(entry) = entries.first()
                && let Some(url) = &entry.url
            {
                let name = entry
                    .name
                    .clone()
                    .unwrap_or_else(|| code.to_ascii_uppercase());
                return Some(SubtitleTrack {
                    code: code.to_owned(),
                    name,
                    url: url.clone(),
                    path: None,
                });
            }
        }
    }

    None
}

/// Returns a sorted list of thumbnail URLs served via the backend.
fn collect_thumbnails(video_id: &str, paths: &Paths, slug: &str) -> Result<Vec<String>> {
    let thumb_dir = paths.thumbnails.join(video_id);
    if !thumb_dir.exists() {
        return Ok(Vec::new());
    }

    let mut thumbs = Vec::new();
    for entry in fs::read_dir(&thumb_dir)
        .with_context(|| format!("reading thumbnails dir {}", thumb_dir.display()))?
    {
        let entry = entry?;
        if !entry.path().is_file() {
            continue;
        }
        let file_name = entry
            .file_name()
            .into_string()
            .unwrap_or_else(|os| os.to_string_lossy().into_owned());
        thumbs.push(file_name);
    }

    thumbs.sort();
    Ok(thumbs
        .into_iter()
        .map(|name| format!("/api/{slug}/{}/thumbnails/{name}", video_id))
        .collect())
}

/// Builds the list of transcodings that exist on disk for a given video so the
/// API can expose them as playable streams.
fn collect_sources(
    video_id: &str,
    info: &VideoInfo,
    output_dir: &Path,
    slug: &str,
) -> Result<Vec<VideoSource>> {
    let mut sources = Vec::new();
    let base_dir = output_dir.join(video_id);
    if !base_dir.exists() {
        return Ok(sources);
    }

    if let Some(formats) = &info.formats {
        for format in formats {
            let format_id = match format.format_id.as_deref() {
                Some(id) => id,
                None => continue,
            };

            // Skip pure audio or video-only streams because the frontend
            // expects ready-to-play muxed files.
            if format
                .vcodec
                .as_deref()
                .is_some_and(|codec| codec.eq_ignore_ascii_case("none"))
                || format
                    .acodec
                    .as_deref()
                    .is_some_and(|codec| codec.eq_ignore_ascii_case("none"))
            {
                continue;
            }

            let sanitized = sanitize_format_id(format_id);
            let ext = format.ext.as_deref().unwrap_or("mp4");
            let mut path = base_dir.join(format!("{video_id}_{sanitized}"));
            path.set_extension(ext);

            if !path.exists() {
                continue;
            }

            let quality_label = format
                .format_note
                .clone()
                .or_else(|| format_quality_label(format.height, format.dynamic_range.as_deref()));

            let mime_type = Some(mime_from_extension(ext));
            let file_size = format.filesize.or(format.filesize_approx);

            sources.push(VideoSource {
                format_id: format_id.to_owned(),
                quality_label,
                width: format.width,
                height: format.height,
                fps: format.fps,
                mime_type,
                ext: Some(ext.to_owned()),
                file_size,
                url: format!("/api/{slug}/{}/streams/{}", video_id, sanitized),
                path: Some(path.to_string_lossy().into_owned()),
            });
        }
    }

    Ok(sources)
}

/// Downloads every available comment via yt-dlp, writes them to disk, and then
/// normalizes into `CommentRecord` rows while removing duplicates.
fn fetch_comments(video_id: &str, video_url: &str, paths: &Paths) -> Result<Vec<CommentRecord>> {
    let comments_dir = paths.comments.join(video_id);
    fs::create_dir_all(&comments_dir)
        .with_context(|| format!("creating comments dir {}", comments_dir.display()))?;

    let output_pattern = comments_dir.join(video_id);
    let mut command = yt_dlp_command();
    command
        .arg("--skip-download")
        .arg("--write-comments")
        .arg("--no-warnings")
        .arg("--no-progress")
        .arg("--force-overwrites")
        .arg("--output")
        .arg(output_pattern.to_string_lossy().to_string())
        .arg(video_url);

    if paths.cookies.exists() {
        command
            .arg("--cookies")
            .arg(paths.cookies.to_string_lossy().to_string());
    }

    match command.status() {
        Ok(status) if status.success() => {}
        Ok(status) => {
            eprintln!(
                "  Warning: comment extraction failed for {} (status {})",
                video_id, status
            );
        }
        Err(err) => {
            eprintln!(
                "  Warning: unable to execute comment extraction for {}: {}",
                video_id, err
            );
        }
    }

    let comments_path = comments_dir.join(format!("{}.comments.json", video_id));
    if !comments_path.exists() {
        return Ok(Vec::new());
    }

    let file = File::open(&comments_path)
        .with_context(|| format!("opening {}", comments_path.display()))?;
    let reader = BufReader::new(file);
    let json_value: Value = serde_json::from_reader(reader)
        .with_context(|| format!("parsing {}", comments_path.display()))?;

    let comments_array = match json_value {
        Value::Array(arr) => arr,
        Value::Object(mut map) => match map.remove("comments") {
            Some(Value::Array(arr)) => arr,
            Some(other) => serde_json::from_value::<Vec<Value>>(other).unwrap_or_default(),
            None => Vec::new(),
        },
        _ => Vec::new(),
    };

    let mut comments = Vec::new();
    let mut seen_ids = HashSet::new();
    for value in comments_array {
        match serde_json::from_value::<RawComment>(value) {
            Ok(raw) => {
                if !seen_ids.insert(raw.id.clone()) {
                    continue;
                }
                let time_posted = raw
                    .timestamp
                    .and_then(timestamp_to_iso)
                    .or_else(|| raw.time_text.clone())
                    .or_else(|| Some(Utc::now().to_rfc3339()));

                comments.push(CommentRecord {
                    id: raw.id,
                    videoid: video_id.to_owned(),
                    author: raw.author.unwrap_or_default(),
                    text: raw.text.unwrap_or_default(),
                    likes: raw.like_count,
                    time_posted,
                    parent_comment_id: raw.parent,
                    status_likedbycreator: raw.author_is_channel_owner || raw.author_is_uploader,
                    reply_count: raw.reply_count,
                });
            }
            Err(err) => {
                eprintln!("  Warning: could not parse comment entry: {}", err);
            }
        }
    }

    Ok(comments)
}

/// Creates a human-friendly label such as `1080p HDR` when the metadata is
/// present.
fn format_quality_label(height: Option<i64>, dynamic_range: Option<&str>) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(h) = height {
        parts.push(format!("{h}p"));
    }
    if let Some(range) = dynamic_range
        && !range.is_empty()
    {
        parts.push(range.to_owned());
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}

/// Guesses the MIME type for each downloaded file based on its extension.
fn mime_from_extension(ext: &str) -> String {
    match ext {
        "mp4" => "video/mp4".to_owned(),
        "mkv" => "video/x-matroska".to_owned(),
        "webm" => "video/webm".to_owned(),
        other => format!("video/{other}"),
    }
}

/// Maps the enum to the slug portion used in API URLs and folder names.
fn media_kind_slug(kind: MediaKind) -> &'static str {
    match kind {
        MediaKind::Video => "videos",
        MediaKind::Short => "shorts",
    }
}

/// Converts yt-dlp's `YYYYMMDD` upload date format into ISO-8601.
fn upload_date_to_iso(value: &str) -> Option<String> {
    if value.len() != 8 {
        return None;
    }
    let year = &value[0..4];
    let month = &value[4..6];
    let day = &value[6..8];
    let naive = NaiveDate::from_ymd_opt(year.parse().ok()?, month.parse().ok()?, day.parse().ok()?);
    let naive = naive?.and_hms_opt(0, 0, 0)?;
    Some(format!("{}Z", naive.format("%Y-%m-%dT%H:%M:%S")))
}

/// Converts epoch seconds into an ISO-8601 string.
fn timestamp_to_iso(timestamp: i64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp(timestamp, 0).map(|datetime| datetime.to_rfc3339())
}

/// Renders durations as `H:MM:SS` or `M:SS` for short clips.
fn format_duration(duration: i64) -> String {
    let hours = duration / 3600;
    let minutes = (duration % 3600) / 60;
    let seconds = duration % 60;

    if hours > 0 {
        format!("{hours}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes}:{seconds:02}")
    }
}

/// Lists all video IDs in a playlist/channel, optionally applying a yt-dlp
/// `--match-filter` (used to split Shorts vs. regular uploads).
fn get_video_ids(list_url: &str, filter: Option<&str>) -> Result<Vec<String>> {
    let mut command = yt_dlp_command();
    command
        .arg("--flat-playlist")
        .arg("--get-id")
        .arg("--ignore-errors");

    if let Some(filter) = filter {
        command.arg("--match-filter").arg(filter);
    }

    command.arg(list_url);

    let output = command
        .output()
        .with_context(|| format!("retrieving playlist from {}", list_url))?;

    if !output.status.success() {
        bail!(
            "failed to list videos for {} (status: {})",
            list_url,
            output.status
        );
    }

    let content = String::from_utf8_lossy(&output.stdout);
    let ids = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|id| id.to_owned())
        .collect();

    Ok(ids)
}

/// Downloads every available muxed format for the provided video id, skipping
/// streams we already grabbed.
fn download_video_all_formats(video_id: &str, output_dir: &Path, paths: &Paths) -> Result<()> {
    let video_url = format!("https://www.youtube.com/watch?v={}", video_id);
    let video_dir = output_dir.join(video_id);
    fs::create_dir_all(&video_dir).with_context(|| format!("creating {}", video_dir.display()))?;

    let base_output = video_dir.join(video_id);
    let base_output_pattern = base_output.to_string_lossy().to_string();
    let info_json_path = base_output.with_extension("info.json");

    println!("Processing video: {}", video_id);

    run_metadata_command(&video_url, &base_output_pattern, &paths.cookies);
    run_subtitle_command(video_id, &video_url, &paths.subtitles, &paths.cookies);
    run_thumbnail_command(video_id, &video_url, &paths.thumbnails, &paths.cookies);

    let formats = collect_format_ids(&info_json_path, &video_url)?;

    if formats.is_empty() {
        println!("  No downloadable formats found for {}", video_id);
        return Ok(());
    }

    for format_id in formats {
        let safe_format_id = sanitize_format_id(&format_id);
        let mut output_path = video_dir.join(format!("{}_{}", video_id, safe_format_id));
        output_path.set_extension("%(ext)s");

        println!("  Downloading format: {}", format_id);

        let mut command = yt_dlp_command();
        command
            .arg("--format")
            .arg(&format_id)
            .arg("--output")
            .arg(output_path.to_string_lossy().to_string())
            .arg("--no-embed-metadata")
            .arg("--no-embed-subs")
            .arg("--no-embed-thumbnail")
            .arg("--no-overwrites")
            .arg("--continue")
            .arg("--ignore-errors")
            .arg("--no-warnings")
            .arg(&video_url);

        if paths.cookies.exists() {
            command
                .arg("--cookies")
                .arg(paths.cookies.to_string_lossy().to_string());
        }

        match command.status() {
            Ok(status) if status.success() => {}
            Ok(_) => {
                eprintln!("    Failed to download format {}", format_id);
            }
            Err(err) => {
                eprintln!("    Failed to download format {}: {}", format_id, err);
            }
        }
    }

    println!("  Completed: {}", video_id);

    Ok(())
}

/// Wrapper for the metadata/description/thumbnail yt-dlp call.
fn run_metadata_command(video_url: &str, output_pattern: &str, cookies: &Path) {
    let mut command = yt_dlp_command();
    command
        .arg("--write-info-json")
        .arg("--write-description")
        .arg("--write-thumbnail")
        .arg("--skip-download")
        .arg("--output")
        .arg(output_pattern)
        .arg(video_url);

    if cookies.exists() {
        command
            .arg("--cookies")
            .arg(cookies.to_string_lossy().to_string());
    }

    run_silent(command, "metadata");
}

/// Downloads subtitles (manual+auto) into a per-video directory.
fn run_subtitle_command(video_id: &str, video_url: &str, subtitles_dir: &Path, cookies: &Path) {
    let target_dir = subtitles_dir.join(video_id);
    if let Err(err) = fs::create_dir_all(&target_dir) {
        eprintln!(
            "  Warning: could not create subtitles directory {}: {}",
            target_dir.display(),
            err
        );
        return;
    }

    let output_pattern = target_dir.join(video_id).to_string_lossy().to_string();

    let mut command = yt_dlp_command();
    command
        .arg("--write-sub")
        .arg("--write-auto-sub")
        .arg("--sub-langs")
        .arg("all")
        .arg("--skip-download")
        .arg("--output")
        .arg(output_pattern)
        .arg(video_url);

    if cookies.exists() {
        command
            .arg("--cookies")
            .arg(cookies.to_string_lossy().to_string());
    }

    run_silent(command, "subtitles");
}

/// Ensures we have the highest quality thumbnails for offline use.
fn run_thumbnail_command(video_id: &str, video_url: &str, thumbnails_dir: &Path, cookies: &Path) {
    let target_dir = thumbnails_dir.join(video_id);
    if let Err(err) = fs::create_dir_all(&target_dir) {
        eprintln!(
            "  Warning: could not create thumbnails directory {}: {}",
            target_dir.display(),
            err
        );
        return;
    }

    let output_pattern = target_dir.join(video_id).to_string_lossy().to_string();

    let mut command = yt_dlp_command();
    command
        .arg("--write-thumbnail")
        .arg("--skip-download")
        .arg("--output")
        .arg(output_pattern)
        .arg(video_url);

    if cookies.exists() {
        command
            .arg("--cookies")
            .arg(cookies.to_string_lossy().to_string());
    }

    run_silent(command, "thumbnails");
}

/// Executes a command and only logs warnings, keeping stdout noise minimal.
fn run_silent(mut command: Command, label: &str) {
    match command.status() {
        Ok(status) if status.success() => {}
        Ok(status) => {
            eprintln!("  Warning: {} command exited with status {}", label, status);
        }
        Err(err) => {
            eprintln!("  Warning: {} command failed: {}", label, err);
        }
    }
}

/// Reads format IDs from the downloaded `.info.json`. If the file is missing or
/// incomplete we fall back to invoking `yt-dlp -F`.
fn collect_format_ids(info_json_path: &Path, video_url: &str) -> Result<Vec<String>> {
    let mut formats = BTreeSet::new();

    if info_json_path.exists()
        && let Ok(file) = File::open(info_json_path)
    {
        let reader = BufReader::new(file);
        match serde_json::from_reader::<_, InfoJson>(reader) {
            Ok(info) => {
                for entry in info.formats {
                    if let Some(id) = entry.format_id {
                        let trimmed = id.trim();
                        if !trimmed.is_empty() {
                            formats.insert(trimmed.to_owned());
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!(
                    "  Warning: could not parse {}: {}",
                    info_json_path.display(),
                    err
                );
            }
        }
    }

    if formats.is_empty() {
        println!("  Could not read formats from metadata, falling back to format listing");
        let output = yt_dlp_command()
            .arg("-F")
            .arg(video_url)
            .output()
            .with_context(|| format!("listing formats for {}", video_url))?;

        if !output.status.success() {
            eprintln!(
                "  Warning: format listing failed for {} (status: {})",
                video_url, output.status
            );
        } else {
            // Parse the human-readable yt-dlp table by grabbing the first token
            // on each non-empty line (skipping header rows like `format code`).
            let listing = String::from_utf8_lossy(&output.stdout);
            for line in listing.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                if let Some(first) = trimmed.split_whitespace().next() {
                    if first.eq_ignore_ascii_case("format") || first.eq_ignore_ascii_case("code") {
                        continue;
                    }
                    if first
                        .chars()
                        .next()
                        .is_some_and(|c| c.is_ascii_alphanumeric())
                    {
                        formats.insert(first.to_owned());
                    }
                }
            }
        }
    }

    Ok(formats.into_iter().collect())
}

/// Normalizes yt-dlp format identifiers so they become safe filenames.
fn sanitize_format_id(format_id: &str) -> String {
    format_id
        .chars()
        .map(|c| match c {
            '/' | ':' | ' ' => '_',
            _ => c,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use newtube_tools::metadata::MetadataReader;
    use std::collections::{HashMap, HashSet};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::{fs, io::Write, path::PathBuf};
    use tempfile::{NamedTempFile, tempdir};

    fn write_runtime_config(media: &str, www: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            "MEDIA_ROOT=\"{media}\"\nWWW_ROOT=\"{www}\"\nNEWTUBE_PORT=\"8080\"\nNEWTUBE_HOST=\"127.0.0.1\"\nAPP_VERSION=\"1.0\"\nDOMAIN_NAME=\"example.com\"\n"
        )
        .unwrap();
        file
    }

    fn temp_paths() -> (tempfile::TempDir, Paths) {
        let dir = tempdir().unwrap();
        let paths = Paths::from_base(dir.path());
        (dir, paths)
    }

    #[test]
    fn downloader_args_use_defaults() {
        let config = write_runtime_config(DEFAULT_MEDIA_ROOT, DEFAULT_WWW_ROOT);
        let args = DownloaderArgs::from_slice(&[
            "--config",
            config.path().to_str().unwrap(),
            "https://www.youtube.com/@Channel",
        ])
        .unwrap();
        assert_eq!(args.channel_url, "https://www.youtube.com/@Channel");
        assert_eq!(args.media_root, PathBuf::from(DEFAULT_MEDIA_ROOT));
        assert_eq!(args.www_root, PathBuf::from(DEFAULT_WWW_ROOT));
    }

    #[test]
    fn downloader_args_override_roots() {
        let config = write_runtime_config(DEFAULT_MEDIA_ROOT, DEFAULT_WWW_ROOT);
        let args = DownloaderArgs::from_slice(&[
            "--config",
            config.path().to_str().unwrap(),
            "--media-root",
            "/data/media",
            "--www-root",
            "/srv/www",
            "https://www.youtube.com/@Channel",
        ])
        .unwrap();

        assert_eq!(args.media_root, PathBuf::from("/data/media"));
        assert_eq!(args.www_root, PathBuf::from("/srv/www"));
    }

    fn sample_video_info() -> VideoInfo {
        VideoInfo {
            id: "abc".into(),
            title: Some("Sample Title".into()),
            fulltitle: Some("Sample Title".into()),
            description: Some("desc".into()),
            like_count: Some(1),
            dislike_count: Some(0),
            view_count: Some(10),
            upload_date: Some("20240101".into()),
            release_timestamp: None,
            uploader: None,
            channel: Some("Channel".into()),
            channel_id: Some("channel123".into()),
            channel_url: Some("https://example.com/channel".into()),
            channel_follower_count: Some(100),
            duration: Some(120),
            duration_string: None,
            thumbnails: Some(vec![ThumbnailInfo {
                url: Some("https://img/1.jpg".into()),
                width: Some(100),
                height: Some(100),
            }]),
            tags: Some(vec!["tech".into()]),
            comment_count: Some(5),
            subtitles: Some(HashMap::new()),
            automatic_captions: Some(HashMap::new()),
            formats: Some(Vec::new()),
        }
    }

    fn sample_format(id: &str, ext: &str) -> FormatInfo {
        FormatInfo {
            format_id: Some(id.into()),
            format_note: None,
            width: Some(1920),
            height: Some(1080),
            fps: Some(30.0),
            ext: Some(ext.into()),
            vcodec: Some("avc1".into()),
            acodec: Some("mp4a".into()),
            filesize: Some(1234),
            filesize_approx: None,
            dynamic_range: Some("HDR".into()),
        }
    }

    fn install_ytdlp_stub(dir: &Path) -> Result<PathBuf> {
        let script_path = dir.join("yt-dlp");
        let script = r#"#!/usr/bin/env bash
set -eu
args=("$@")
output=""
format_id=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)
      shift
      output="$1"
      ;;
    --format)
      shift
      format_id="$1"
      ;;
  esac
  shift
done

json_payload='{
  "id": "alpha",
  "fulltitle": "Alpha Title",
  "description": "Sample description",
  "like_count": 1,
  "dislike_count": 0,
  "view_count": 10,
  "upload_date": "20240101",
  "channel": "Channel",
  "channel_id": "chan123",
  "channel_url": "https://youtube.com/@Channel",
  "channel_follower_count": 100,
  "duration": 120,
  "formats": [
    {
      "format_id": "1080p",
      "width": 1920,
      "height": 1080,
      "fps": 30,
      "ext": "mp4",
      "vcodec": "avc1",
      "acodec": "mp4a",
      "filesize": 1024
    }
  ],
  "tags": ["tech"],
  "comment_count": 2
}'

format_listing='sb3 mhtml 48x27        1    |                  mhtml | images                                storyboard
sb2 mhtml 80x45        1    |                  mhtml | images                                storyboard
sb1 mhtml 160x90       1    |                  mhtml | images                                storyboard
sb0 mhtml 320x180      1    |                  mhtml | images                                storyboard
139 m4a   audio only      2 |    1.04MiB   49k https | audio only        mp4a.40.5   49k 22k [en] low, m4a_dash
249 webm  audio only      2 |    1.15MiB   54k https | audio only        opus        54k 48k [en] low, webm_dash
140 m4a   audio only      2 |    2.75MiB  130k https | audio only        mp4a.40.2  130k 44k [en] medium, m4a_dash
251 webm  audio only      2 |    2.98MiB  140k https | audio only        opus       140k 48k [en] medium, webm_dash
91  mp4   256x144     25    | ~  3.62MiB  171k m3u8  | avc1.4D400C       mp4a.40.5           [en]
160 mp4   256x144     25    |    1.25MiB   59k https | avc1.4d400c   59k video only          144p, mp4_dash
278 webm  256x144     25    |  465.49KiB   21k https | vp9           21k video only          144p, webm_dash
92  mp4   426x240     25    | ~  6.85MiB  323k m3u8  | avc1.4D4015       mp4a.40.5           [en]
133 mp4   426x240     25    |    3.12MiB  147k https | avc1.4d4015  147k video only          240p, mp4_dash
242 webm  426x240     25    |  712.56KiB   33k https | vp9           33k video only          240p, webm_dash
93  mp4   640x360     25    | ~  6.08MiB  287k m3u8  | avc1.4D401E       mp4a.40.2           [en]
134 mp4   640x360     25    |    2.14MiB  101k https | avc1.4d401e  101k video only          360p, mp4_dash
18  mp4   640x360     25  2 |    3.93MiB  185k https | avc1.42001E       mp4a.40.2       44k [en] 360p
243 webm  640x360     25    |    1.53MiB   72k https | vp9           72k video only          360p, webm_dash
94  mp4   854x480     25    | ~  9.84MiB  464k m3u8  | avc1.4D401E       mp4a.40.2           [en]
135 mp4   854x480     25    |    4.60MiB  217k https | avc1.4d401e  217k video only          480p, mp4_dash
244 webm  854x480     25    |    2.89MiB  136k https | vp9          136k video only          480p, webm_dash
95  mp4   1280x720    25    | ~ 19.69MiB  928k m3u8  | avc1.4D401F       mp4a.40.2           [en]
136 mp4   1280x720    25    |   11.17MiB  526k https | avc1.4d401f  526k video only          720p, mp4_dash
247 webm  1280x720    25    |    8.27MiB  390k https | vp9          390k video only          720p, webm_dash
96  mp4   1920x1080   25    | ~ 45.98MiB 2167k m3u8  | avc1.640028       mp4a.40.2           [en]
137 mp4   1920x1080   25    |   29.26MiB 1379k https | avc1.640028 1379k video only          1080p, mp4_dash
248 webm  1920x1080   25    |   15.79MiB  744k https | vp9          744k video only          1080p, webm_dash
271 webm  2560x1440   25    |   51.65MiB 2434k https | vp9         2434k video only          1440p, webm_dash
313 webm  3840x2160   25    |  147.52MiB 6950k https | vp9         6950k video only          2160p, webm_dash'

if printf '%s\n' "${args[@]}" | grep -q -- '--flat-playlist'; then
  echo "alpha"
  exit 0
fi

if printf '%s\n' "${args[@]}" | grep -q -- '--dump-single-json'; then
  printf '%s\n' "$json_payload"
  exit 0
fi

if printf '%s\n' "${args[@]}" | grep -q -- '--write-info-json'; then
  mkdir -p "$(dirname "$output")"
  printf '%s\n' "$json_payload" > "${output}.info.json"
  echo "desc" > "${output}.description"
  echo "thumb" > "${output}.jpg"
  exit 0
fi

if printf '%s\n' "${args[@]}" | grep -q -- '--write-comments'; then
  mkdir -p "$(dirname "$output")"
cat <<'JSON' > "${output}.comments.json"
[
  {"id":"c1","text":"first","timestamp":1700000000,"author_is_channel_owner":true,"like_count":1},
  {"id":"c1","text":"duplicate","timestamp":1700000100},
  {"id":"c2","text":"second","time_text":"2024-01-01","author_is_uploader":true}
]
JSON
  exit 0
fi

if printf '%s\n' "${args[@]}" | grep -q -- '--write-sub'; then
  mkdir -p "$(dirname "$output")"
  echo "WEBVTT" > "${output}.en.vtt"
  exit 0
fi

if printf '%s\n' "${args[@]}" | grep -q -- '--write-thumbnail'; then
  mkdir -p "$(dirname "$output")"
  echo "thumb" > "${output}.jpg"
  exit 0
fi

if [[ -n "$format_id" ]]; then
  target="${output//%(ext)s/mp4}"
  mkdir -p "$(dirname "$target")"
  echo "video" > "$target"
  exit 0
fi

if printf '%s\n' "${args[@]}" | grep -q -- '^-F$'; then
  printf '%s\n' "$format_listing"
  exit 0
fi

exit 0
"#;
        fs::write(&script_path, script)?;
        #[cfg(unix)]
        {
            let mut perms = fs::metadata(&script_path)?.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&script_path, perms)?;
        }
        Ok(script_path)
    }

    #[test]
    fn paths_prepare_creates_directories() -> Result<()> {
        let (_temp, paths) = temp_paths();
        paths.prepare()?;
        assert!(paths.videos.exists());
        assert!(paths.shorts.exists());
        assert!(paths.subtitles.exists());
        assert!(paths.thumbnails.exists());
        assert!(paths.comments.exists());
        assert!(paths.www_root.exists());
        Ok(())
    }

    #[test]
    fn archive_roundtrip_loads_ids() -> Result<()> {
        let dir = tempdir()?;
        let archive_path = dir.path().join("archive.txt");
        append_to_archive(&archive_path, "abc123")?;
        append_to_archive(&archive_path, "abc123")?;
        append_to_archive(&archive_path, "def456")?;

        let entries = load_archive(&archive_path)?;
        assert_eq!(entries.len(), 2);
        assert!(entries.contains("abc123"));
        assert!(entries.contains("def456"));
        Ok(())
    }

    #[test]
    fn build_video_record_populates_fields() -> Result<()> {
        let (_temp, paths) = temp_paths();
        paths.prepare()?;
        let media_dir = paths.media_dir(MediaKind::Video).join("abc");
        fs::create_dir_all(&media_dir)?;
        fs::write(media_dir.join("abc_1080p.mp4"), "bytes")?;
        let thumbs_dir = paths.thumbnails.join("abc");
        fs::create_dir_all(&thumbs_dir)?;
        fs::write(thumbs_dir.join("first.jpg"), "1")?;
        fs::write(thumbs_dir.join("second.jpg"), "1")?;

        let mut info = sample_video_info();
        info.fulltitle = Some("Fancy Title".into());
        info.title = None;
        info.duration = Some(125);
        info.duration_string = None;
        info.formats = Some(vec![sample_format("1080p", "mp4")]);

        let record = build_video_record(
            "abc",
            &info,
            paths.media_dir(MediaKind::Video),
            MediaKind::Video,
            &paths,
        )?;
        assert_eq!(record.title, "Fancy Title");
        assert_eq!(record.duration_text.as_deref(), Some("2:05"));
        assert_eq!(
            record.thumbnail_url.as_deref(),
            Some("/api/videos/abc/thumbnails/first.jpg")
        );
        assert_eq!(record.sources.len(), 1);
        assert_eq!(
            record.sources[0].url,
            "/api/videos/abc/streams/1080p".to_string()
        );
        Ok(())
    }

    #[test]
    fn collect_subtitles_prefers_local_files() -> Result<()> {
        let (_temp, paths) = temp_paths();
        let mut info = sample_video_info();
        let mut subs = HashMap::new();
        subs.insert(
            "en".into(),
            vec![SubtitleInfo {
                url: Some("https://remote/en.vtt".into()),
                ext: Some("vtt".into()),
                name: Some("English".into()),
            }],
        );
        info.subtitles = Some(subs);
        let subtitle_dir = paths.subtitles.join("abc");
        fs::create_dir_all(&subtitle_dir)?;
        fs::write(subtitle_dir.join("abc.en.vtt"), "WEBVTT")?;

        let collection = collect_subtitles("abc", &info, &paths, MediaKind::Video)?;
        assert_eq!(collection.languages.len(), 1);
        let track = &collection.languages[0];
        assert!(track.path.as_deref().unwrap().ends_with("abc.en.vtt"));
        assert!(track.url.contains("/api/videos/abc/subtitles/en"));
        Ok(())
    }

    #[test]
    fn collect_subtitles_falls_back_to_remote_track() -> Result<()> {
        let (_temp, paths) = temp_paths();
        let mut info = sample_video_info();
        let mut subs = HashMap::new();
        subs.insert(
            "en".into(),
            vec![SubtitleInfo {
                url: Some("https://remote/en.vtt".into()),
                ext: Some("vtt".into()),
                name: Some("English".into()),
            }],
        );
        info.subtitles = Some(subs);

        let collection = collect_subtitles("abc", &info, &paths, MediaKind::Video)?;
        assert_eq!(collection.languages.len(), 1);
        let track = &collection.languages[0];
        assert!(track.path.is_none());
        assert_eq!(track.url, "https://remote/en.vtt");
        Ok(())
    }

    #[test]
    fn collect_sources_skips_audio_only_formats() -> Result<()> {
        let (_temp, paths) = temp_paths();
        let video_dir = paths.media_dir(MediaKind::Video).join("abc");
        fs::create_dir_all(&video_dir)?;
        let sanitized = sanitize_format_id("f/1");
        fs::write(video_dir.join(format!("abc_{sanitized}.mp4")), "bytes")?;
        let mut info = sample_video_info();
        info.formats = Some(vec![
            FormatInfo {
                format_id: Some("f/1".into()),
                format_note: None,
                width: Some(1920),
                height: Some(1080),
                fps: Some(30.0),
                ext: Some("mp4".into()),
                vcodec: Some("avc1".into()),
                acodec: Some("mp4a".into()),
                filesize: Some(100),
                filesize_approx: None,
                dynamic_range: Some("HDR".into()),
            },
            FormatInfo {
                format_id: Some("audio".into()),
                format_note: None,
                width: None,
                height: None,
                fps: None,
                ext: Some("m4a".into()),
                vcodec: Some("none".into()),
                acodec: Some("mp4a".into()),
                filesize: Some(50),
                filesize_approx: None,
                dynamic_range: None,
            },
        ]);

        let sources = collect_sources("abc", &info, paths.media_dir(MediaKind::Video), "videos")?;
        assert_eq!(sources.len(), 1);
        assert!(sources[0].url.contains("f_1"));
        assert_eq!(sources[0].quality_label.as_deref(), Some("1080p HDR"));
        Ok(())
    }

    #[test]
    fn format_helpers_cover_edge_cases() {
        assert_eq!(
            format_quality_label(Some(2160), Some("HDR")),
            Some("2160p HDR".into())
        );
        assert_eq!(format_quality_label(None, None), None);
        assert_eq!(mime_from_extension("webm"), "video/webm");
        assert_eq!(mime_from_extension("foo"), "video/foo");
        assert_eq!(
            upload_date_to_iso("20240102"),
            Some("2024-01-02T00:00:00Z".into())
        );
        assert!(upload_date_to_iso("2024").is_none());
        assert_eq!(
            timestamp_to_iso(0).as_deref(),
            Some("1970-01-01T00:00:00+00:00")
        );
        assert_eq!(format_duration(65), "1:05");
        assert_eq!(format_duration(3725), "1:02:05");
    }

    #[test]
    fn collect_format_ids_reads_json() -> Result<()> {
        let dir = tempdir()?;
        let info_path = dir.path().join("info.json");
        let json = serde_json::json!({
            "formats": [
                { "format_id": " 136 " },
                { "format_id": "249" },
                { "format_id": null }
            ]
        });
        fs::write(&info_path, serde_json::to_vec(&json)?)?;
        let ids = collect_format_ids(&info_path, "https://example.com/video")?;
        assert_eq!(ids, vec!["136".to_string(), "249".to_string()]);
        Ok(())
    }

    #[test]
    fn sanitize_format_id_replaces_delimiters() {
        assert_eq!(sanitize_format_id("http/1080p:60"), "http_1080p_60");
        assert_eq!(sanitize_format_id("abc def"), "abc_def");
    }

    #[test]
    fn process_entry_refreshes_metadata_even_when_archived() -> Result<()> {
        let (temp, paths) = temp_paths();
        let stub = install_ytdlp_stub(temp.path())?;
        let _guard = set_ytdlp_stub_path(stub);
        paths.prepare()?;

        let media_dir = paths.media_dir(MediaKind::Video).join("alpha");
        fs::create_dir_all(&media_dir)?;
        fs::write(media_dir.join("alpha_1080p.mp4"), "video-bytes")?;
        let subtitle_dir = paths.subtitles.join("alpha");
        fs::create_dir_all(&subtitle_dir)?;
        fs::write(subtitle_dir.join("alpha.en.vtt"), "WEBVTT")?;

        let mut metadata = MetadataStore::open(&paths.metadata_db)?;
        let mut archive = HashSet::from([String::from("alpha")]);
        process_media_entry(
            "alpha",
            1,
            1,
            &paths,
            &mut archive,
            MediaKind::Video,
            &mut metadata,
        )?;

        let reader = MetadataReader::new(&paths.metadata_db)?;
        let video = reader.get_video("alpha")?.expect("video stored");
        assert_eq!(video.title, "Alpha Title");
        let comments = reader.get_comments("alpha")?;
        assert_eq!(comments.len(), 2);
        assert!(comments.iter().any(|c| c.status_likedbycreator));
        Ok(())
    }

    #[test]
    fn fetch_comments_dedupes_and_sets_flags() -> Result<()> {
        let (temp, paths) = temp_paths();
        let stub = install_ytdlp_stub(temp.path())?;
        let _guard = set_ytdlp_stub_path(stub);
        let comments = fetch_comments("alpha", "https://youtube.com/watch?v=alpha", &paths)?;
        assert_eq!(comments.len(), 2);
        assert!(
            comments[0]
                .time_posted
                .as_ref()
                .unwrap()
                .starts_with("2023")
        );
        assert!(comments.iter().any(|c| c.status_likedbycreator));
        Ok(())
    }

    #[test]
    fn download_collection_downloads_new_entries() -> Result<()> {
        let (temp, paths) = temp_paths();
        let stub = install_ytdlp_stub(temp.path())?;
        let _guard = set_ytdlp_stub_path(stub);
        paths.prepare()?;
        let mut metadata = MetadataStore::open(&paths.metadata_db)?;
        let mut archive = HashSet::new();
        download_collection(
            "test videos",
            "https://example.com/channel/videos".to_string(),
            None,
            &paths,
            &mut archive,
            MediaKind::Video,
            &mut metadata,
        )?;
        let reader = MetadataReader::new(&paths.metadata_db)?;
        assert!(reader.get_video("alpha")?.is_some());
        let media_file = paths
            .media_dir(MediaKind::Video)
            .join("alpha")
            .join("alpha_1080p.mp4");
        assert!(media_file.exists());
        Ok(())
    }

    fn expected_format_ids() -> Vec<String> {
        vec![
            "133", "134", "135", "136", "137", "139", "140", "160", "18", "242", "243", "244",
            "247", "248", "249", "251", "271", "278", "313", "91", "92", "93", "94", "95", "96",
            "sb0", "sb1", "sb2", "sb3",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn collect_format_ids_matches_known_listing() -> Result<()> {
        let (temp, _paths) = temp_paths();
        let stub = install_ytdlp_stub(temp.path())?;
        let _guard = set_ytdlp_stub_path(stub);
        let info_path = temp.path().join("empty.json");
        fs::write(&info_path, r#"{"formats":[]}"#)?;
        let actual = collect_format_ids(&info_path, "https://www.youtube.com/watch?v=6QZz04e6gqE")?;
        assert_eq!(actual, expected_format_ids());
        Ok(())
    }
}
