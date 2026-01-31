#![forbid(unsafe_code)]

//! Helper binary that re-runs the downloader for every channel already present
//! on disk. Acts like a nightly cron job.

use anyhow::{Context, Result, bail};
use newtube_tools::{
    config::{RuntimeOverrides, resolve_runtime_paths},
    metadata::MetadataStore,
    security::ensure_not_root,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(test)]
use std::sync::Mutex;
use walkdir::WalkDir;

const VIDEOS_SUBDIR: &str = "videos";
const SHORTS_SUBDIR: &str = "shorts";
const METADATA_DB_FILE: &str = "metadata.db";

#[derive(Debug, Clone)]
struct RoutineArgs {
    media_root: PathBuf,
    www_root: PathBuf,
}

impl RoutineArgs {
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
                _ => {
                    bail!("unknown argument: {arg}");
                }
            }
        }

        let runtime_paths = resolve_runtime_paths(RuntimeOverrides {
            media_root: media_root_override.clone(),
            www_root: www_root_override.clone(),
            ..RuntimeOverrides::default()
        })?;
        let media_root = media_root_override.unwrap_or(runtime_paths.media_root);
        let www_root = www_root_override.unwrap_or(runtime_paths.www_root);

        Ok(Self {
            media_root,
            www_root,
        })
    }
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
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
                .or_else(|| channel_url.as_deref())
                .or_else(|| uploader_url.as_deref()),
        }
    }
}

/// Only grab the small subset of fields we need from `.info.json`.
#[derive(Deserialize)]
struct MinimalInfo {
    channel_url: Option<OneOrMany<String>>,
    uploader_url: Option<OneOrMany<String>>,
    #[serde(default)]
    creators: Option<Vec<CreatorInfo>>,
    channel: Option<OneOrMany<CreatorInfo>>,
    uploader: Option<OneOrMany<CreatorInfo>>,
}

/// Scans on-disk metadata, identifies unique channels, and launches
/// `download_channel` for each.
#[tokio::main]
async fn main() -> Result<()> {
    ensure_not_root("routine_update")?;

    let RoutineArgs {
        media_root,
        www_root,
    } = RoutineArgs::parse()?;

    let metadata_path = media_root.join(METADATA_DB_FILE);
    let _metadata =
        MetadataStore::open(&metadata_path)
            .await
            .context("initializing metadata database")?;

    println!("Library root: {}", media_root.display());
    println!("WWW root: {}", www_root.display());

    let base_dir = media_root.clone();
    let videos_dir = base_dir.join(VIDEOS_SUBDIR);
    let shorts_dir = base_dir.join(SHORTS_SUBDIR);

    let mut channels = BTreeMap::new();
    collect_channels(&videos_dir, &mut channels)?;
    collect_channels(&shorts_dir, &mut channels)?;

    if channels.is_empty() {
        println!(
            "No previously downloaded channels found in {}.",
            base_dir.display()
        );
        return Ok(());
    }

    let downloader = find_download_channel_executable()?;

    let scheduled: Vec<String> = channels.values().cloned().collect();
    println!("Found {} channel(s) to update.", scheduled.len());
    println!("Channels queued for refresh:");
    for channel in &scheduled {
        println!("  - {}", channel);
    }

    for (index, channel) in scheduled.iter().enumerate() {
        let current = index + 1;
        println!();
        println!(
            "[{}/{}] Updating channel: {}",
            current,
            scheduled.len(),
            channel
        );

        match Command::new(&downloader)
            .arg("--media-root")
            .arg(&media_root)
            .arg("--www-root")
            .arg(&www_root)
            .arg(channel)
            .status()
        {
            Ok(status) if status.success() => {
                println!("  Completed update for {}", channel);
            }
            Ok(status) => {
                eprintln!(
                    "  Warning: downloader exited with status {} for {}",
                    status, channel
                );
            }
            Err(err) => {
                eprintln!(
                    "  Warning: failed to run downloader for {}: {}",
                    channel, err
                );
            }
        }
    }

    println!();
    println!("All channel updates complete.");

    Ok(())
}

/// Walks a directory tree looking for `*.info.json` files and extracts the
/// original channel URL so we can re-run downloads later.
fn collect_channels(root: &Path, channels: &mut BTreeMap<String, String>) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }

    for entry in WalkDir::new(root)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
    {
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        if !entry.file_name().to_string_lossy().ends_with(".info.json") {
            continue;
        }

        // Each `.info.json` contains the original uploader metadata, so we read
        // just enough fields to recover a canonical channel URL.
        if let Some(url) = extract_channel_url(entry.path())? {
            let canonical = canonicalize_channel_url(&url);
            channels.entry(canonical).or_insert(url);
        }
    }

    Ok(())
}

/// Reads the minimal metadata needed to figure out which channel a video
/// belongs to.
fn extract_channel_url(path: &Path) -> Result<Option<String>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("  Warning: could not open {}: {}", path.display(), err);
            return Ok(None);
        }
    };
    let reader = BufReader::new(file);

    match serde_json::from_reader::<_, MinimalInfo>(reader) {
        Ok(info) => {
            if let Some(url) = first_url(&info) {
                return Ok(Some(url.trim().to_owned()));
            }
            Ok(None)
        }
        Err(err) => {
            eprintln!("  Warning: could not parse {}: {}", path.display(), err);
            Ok(None)
        }
    }
}

fn first_url(info: &MinimalInfo) -> Option<&str> {
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

/// Returns a lowercase, slash-normalized version of the channel URL for
/// deduplication.
fn canonicalize_channel_url(url: &str) -> String {
    let trimmed = url.trim();
    let without_fragment = trimmed.split('#').next().unwrap_or(trimmed);
    let without_query = without_fragment.split('?').next().unwrap_or(without_fragment);
    let without_slash = without_query.trim_end_matches('/');
    without_slash.to_ascii_lowercase()
}

/// Finds the `download_channel` executable either via Cargo's env var or by
/// looking next to the current binary (assuming `cargo install`/`cargo build`).
#[cfg(test)]
static DOWNLOAD_CHANNEL_STUB: Mutex<Option<PathBuf>> = Mutex::new(None);

#[cfg(test)]
fn set_download_channel_stub(path: PathBuf) {
    *DOWNLOAD_CHANNEL_STUB.lock().unwrap() = Some(path);
}

fn find_download_channel_executable() -> Result<PathBuf> {
    #[cfg(test)]
    {
        if let Some(path) = DOWNLOAD_CHANNEL_STUB.lock().unwrap().clone()
            && path.exists()
        {
            return Ok(path);
        }
    }

    if let Ok(path) = env::var("CARGO_BIN_EXE_download_channel") {
        let path = PathBuf::from(path);
        if path.exists() {
            return Ok(path);
        }
    }

    let mut sibling = env::current_exe().context("locating routine_update executable")?;
    sibling.set_file_name("download_channel");
    if sibling.exists() {
        return Ok(sibling);
    }

    bail!("download_channel binary not found. Build it with `cargo build --bin download_channel`.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::io::Write;
    use std::{
        fs::{self, File},
        path::PathBuf,
    };
    use std::sync::Mutex;
    use tempfile::tempdir;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env_file(vars: &[(&str, &str)], f: impl FnOnce()) {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempdir().unwrap();
        let mut contents = String::new();
        for (key, value) in vars {
            contents.push_str(&format!("{key}=\"{value}\"\n"));
        }
        fs::write(dir.path().join(".env"), contents).unwrap();
        let cwd = env::current_dir().unwrap();
        env::set_current_dir(dir.path()).unwrap();
        f();
        env::set_current_dir(cwd).unwrap();
    }

    #[test]
    fn routine_args_default_paths() {
        let mut parsed = None;
        with_env_file(
            &[
                ("MEDIA_ROOT", "/yt"),
                ("WWW_ROOT", "/www/newtube.com"),
            ],
            || {
                parsed = Some(RoutineArgs::from_slice(&[]).unwrap());
            },
        );
        let args = parsed.unwrap();
        assert_eq!(args.media_root, PathBuf::from("/yt"));
        assert_eq!(args.www_root, PathBuf::from("/www/newtube.com"));
    }

    #[test]
    fn routine_args_override_paths() {
        let mut parsed = None;
        with_env_file(
            &[
                ("MEDIA_ROOT", "/yt"),
                ("WWW_ROOT", "/www/newtube.com"),
            ],
            || {
                parsed = Some(
                    RoutineArgs::from_slice(&[
                        "--media-root",
                        "/data/yt",
                        "--www-root",
                        "/srv/site",
                    ])
                    .unwrap(),
                );
            },
        );
        let args = parsed.unwrap();
        assert_eq!(args.media_root, PathBuf::from("/data/yt"));
        assert_eq!(args.www_root, PathBuf::from("/srv/site"));
    }

    #[test]
    fn collect_channels_dedupes_entries() -> Result<()> {
        let temp = tempdir()?;
        let videos_dir = temp.path().join("videos");
        fs::create_dir_all(&videos_dir)?;
        let info_path = videos_dir.join("sample.info.json");
        File::create(&info_path)?.write_all(br#"{"channel_url":"HTTPS://YouTube.com/@Test/"}"#)?;
        let mut map = BTreeMap::new();
        collect_channels(&videos_dir, &mut map)?;
        assert_eq!(map.len(), 1);
        assert_eq!(map.values().next().unwrap(), "HTTPS://YouTube.com/@Test/");
        Ok(())
    }

    #[test]
    fn extract_channel_url_prefers_channel_field() -> Result<()> {
        let temp = tempdir()?;
        let file_path = temp.path().join("a.info.json");
        File::create(&file_path)?.write_all(
            br#"{"channel_url":"https://example.com","uploader_url":"https://other"}"#,
        )?;
        let url = extract_channel_url(&file_path)?.expect("url parsed");
        assert_eq!(url, "https://example.com");
        Ok(())
    }

    #[test]
    fn canonicalize_channel_url_strips_trailing_slash() {
        assert_eq!(
            canonicalize_channel_url("HTTPS://Example.com/Channel/"),
            "https://example.com/channel"
        );
    }

    #[test]
    fn find_download_channel_uses_stub_path() -> Result<()> {
        let temp = tempdir()?;
        let fake = temp.path().join("download_channel");
        File::create(&fake)?;
        set_download_channel_stub(fake.clone());
        let path = find_download_channel_executable()?;
        assert_eq!(path, fake);
        Ok(())
    }
}
