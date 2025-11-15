#![forbid(unsafe_code)]

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use blake3::Hasher;
use clap::{ArgGroup, Parser};
use ed25519_dalek::{Signature, SigningKey, Signer, VerifyingKey};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use newtube_tools::config::{
    DEFAULT_CONFIG_PATH, DEFAULT_NEWTUBE_HOST, DEFAULT_NEWTUBE_PORT, DEFAULT_RELEASE_REPO,
    EnvConfig, read_env_config, load_runtime_paths_from,
};
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::{self, File},
    io::{self, Read, Write},
    net::IpAddr,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::Command,
};
use tar::Builder;
use tempfile::TempDir;
use ureq::{Agent, Response};
use walkdir::WalkDir;

const DEFAULT_MEDIA_DIR: &str = "/yt";
const DEFAULT_WWW_DIR: &str = "/www/newtube.com";
const BIN_ROOT: &str = "/opt/viewtube/bin";
const DEFAULT_PUBLIC_KEY_PATH: &str = "/etc/viewtube-release.pub";
const RELEASE_SIG_VERSION: u32 = 1;
const RELEASE_SIG_PREFIX: &str = "viewtube-release";
const SOURCE_ARCHIVE_PREFIX: &str = "viewtube-src";
const BINARY_ARCHIVE_PREFIX: &str = "viewtube-bin";
const SOURCE_ROOT_DIR: &str = "source";
const BINARY_ROOT_DIR: &str = "bundle";
const GITHUB_API_BASE: &str = "https://api.github.com";
const SOFTWARE_SERVICE: &str = "software-updater.service";
const SOFTWARE_TIMER: &str = "software-updater.timer";
const NGINX_SERVICE: &str = "nginx";
const BACKEND_SERVICE: &str = "viewtube-backend.service";
const ROUTINE_SERVICE: &str = "viewtube-routine.service";
const VIEWTUBE_GROUP: &str = "viewtube";
const BACKEND_USER: &str = "viewtube-backend";
const DOWNLOADER_USER: &str = "viewtube-downloader";
const BACKEND_HOME: &str = "/var/lib/viewtube-backend";
const DOWNLOADER_HOME: &str = "/var/lib/viewtube-downloader";
const FRONTEND_SKIP_ENTRIES: &[&str] = &[
    ".git",
    ".github",
    "node_modules",
    "coverage",
    "cypress",
    "tests",
    "target",
    "src",
    "Cargo.lock",
    "Cargo.toml",
    "package.json",
    "package-lock.json",
    "README.md",
    "LICENSE",
];

#[derive(Parser, Debug)]
#[command(author, version, about = "Install and manage ViewTube services.")]
#[command(group(
    ArgGroup::new("mode")
        .args([
            "uninstall",
            "cleanup",
            "reinstall",
            "auto_update",
            "package_release",
            "keygen",
            "apply_archive"
        ])
        .multiple(false)
))]
struct Cli {
    #[arg(short = 'u', long = "uninstall", help = "Uninstall the service")]
    uninstall: bool,
    #[arg(short = 'c', long = "cleanup", help = "Cleanup build and runtime artifacts in the repo")]
    cleanup: bool,
    #[arg(short = 'r', long = "reinstall", help = "Uninstall then install the latest version")]
    reinstall: bool,
    #[arg(long = "media-dir", value_name = "PATH", help = "Override the media directory (default /yt)")]
    media_dir: Option<PathBuf>,
    #[arg(long = "www-dir", value_name = "PATH", help = "Override the www directory (default /www/newtube.com)")]
    www_dir: Option<PathBuf>,
    #[arg(long = "port", value_name = "PORT", help = "Override the backend API port (default 8080)")]
    port: Option<u16>,
    #[arg(long = "host", value_name = "IP", help = "Override the backend listen address (default 127.0.0.1)")]
    host: Option<String>,
    #[arg(long = "domain", value_name = "NAME", help = "Domain name serving ViewTube (e.g., example.com)")]
    domain: Option<String>,
    #[arg(long = "release-repo", value_name = "OWNER/REPO", help = "GitHub repository used for signed releases")]
    release_repo: Option<String>,
    #[arg(long = "config", value_name = "PATH", default_value = DEFAULT_CONFIG_PATH, help = "Path to the config file")]
    config: PathBuf,
    #[arg(short = 'y', long = "assume-yes", help = "Automatically answer yes to prompts")]
    assume_yes: bool,
    #[arg(long = "auto-update", help = "Download, verify, and build the latest signed release from GitHub")]
    auto_update: bool,
    #[arg(long = "github-token-file", value_name = "PATH", help = "Optional file containing a GitHub token used for release downloads")]
    github_token_file: Option<PathBuf>,
    #[arg(long = "apply-archive", help = "Apply a local signed source archive (offline update)")]
    apply_archive: bool,
    #[arg(long = "source-archive", value_name = "PATH", requires = "apply_archive", help = "Path to the signed source tarball (.tar.gz)")]
    source_archive: Option<PathBuf>,
    #[arg(long = "source-signature", value_name = "PATH", requires = "apply_archive", help = "Path to the detached signature for the source tarball")]
    source_signature: Option<PathBuf>,
    #[arg(long = "package-release", help = "Build and sign release archives (source + binary bundles)")]
    package_release: bool,
    #[arg(long = "release-tag", value_name = "TAG", requires = "package_release", help = "Release tag (e.g., v0.2.0) used for artifact naming")]
    release_tag: Option<String>,
    #[arg(long = "output-dir", value_name = "DIR", requires = "package_release", help = "Directory where release artifacts should be written")]
    output_dir: Option<PathBuf>,
    #[arg(long = "signing-key", value_name = "PATH", requires = "package_release", help = "Path to the Ed25519 signing key generated via --keygen")]
    signing_key: Option<PathBuf>,
    #[arg(long = "keygen", help = "Generate an Ed25519 signing keypair used for release packaging")]
    keygen: bool,
    #[arg(long = "key-dir", value_name = "DIR", requires = "keygen", help = "Directory where signing key files should be written")]
    key_dir: Option<PathBuf>,
    #[arg(long = "trusted-pubkey", value_name = "PATH", default_value = DEFAULT_PUBLIC_KEY_PATH, help = "Path to the trusted release public key used for verification")]
    trusted_pubkey: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let repo_root = env::current_dir().context("Failed to determine current directory")?;

    if cli.cleanup {
        cleanup_repo(&repo_root)?;
        return Ok(());
    }

    if cli.keygen {
        generate_signing_keypair(&cli)?;
        return Ok(());
    }

    if cli.package_release {
        package_release_artifacts(&repo_root, &cli)?;
        return Ok(());
    }

    ensure_root()?;

    if cli.apply_archive {
        apply_signed_source_archive(
            &cli.config,
            cli.source_archive.as_ref().expect("source archive required"),
            cli.source_signature.as_ref().expect("source signature required"),
            &cli.trusted_pubkey,
            None,
        )?;
        return Ok(());
    }

    if cli.auto_update {
        let token = load_optional_token(cli.github_token_file.as_deref())?;
        auto_update_from_github(&cli.config, &cli.trusted_pubkey, token.as_deref())?;
        return Ok(());
    }

    let existing_env = read_env_config(&cli.config)?;
    let needs_prompt = !cli.uninstall || cli.reinstall;
    let port_arg = cli.port;
    let host_arg = cli.host.clone();
    let (media_root, www_root, newtube_port, newtube_host, release_repo) = if needs_prompt {
        (
            resolve_media_root(
                cli.media_dir.clone(),
                existing_env.as_ref().and_then(|cfg| cfg.media_root.clone()),
                cli.assume_yes,
            )?,
            resolve_www_root(
                cli.www_dir.clone(),
                existing_env.as_ref().and_then(|cfg| cfg.www_root.clone()),
                cli.assume_yes,
            )?,
            resolve_port(
                port_arg,
                existing_env.as_ref().and_then(|cfg| cfg.newtube_port),
                cli.assume_yes,
            )?,
            resolve_host(
                host_arg.clone(),
                existing_env
                    .as_ref()
                    .and_then(|cfg| cfg.newtube_host.clone()),
                cli.assume_yes,
            )?,
            resolve_release_repo(
                cli.release_repo.clone(),
                existing_env.as_ref().and_then(|cfg| cfg.release_repo.clone()),
                cli.assume_yes,
            )?,
        )
    } else {
        (
            cli.media_dir
                .clone()
                .or_else(|| existing_env.as_ref().and_then(|cfg| cfg.media_root.clone()))
                .unwrap_or_else(|| PathBuf::from(DEFAULT_MEDIA_DIR)),
            cli.www_dir
                .clone()
                .or_else(|| existing_env.as_ref().and_then(|cfg| cfg.www_root.clone()))
                .unwrap_or_else(|| PathBuf::from(DEFAULT_WWW_DIR)),
            port_arg
                .or_else(|| existing_env.as_ref().and_then(|cfg| cfg.newtube_port))
                .unwrap_or(DEFAULT_NEWTUBE_PORT),
            host_arg
                .clone()
                .or_else(|| {
                    existing_env
                        .as_ref()
                        .and_then(|cfg| cfg.newtube_host.clone())
                })
                .unwrap_or_else(|| DEFAULT_NEWTUBE_HOST.to_string()),
            cli.release_repo
                .clone()
                .or_else(|| existing_env.as_ref().and_then(|cfg| cfg.release_repo.clone()))
                .unwrap_or_else(|| DEFAULT_RELEASE_REPO.to_string()),
        )
    };
    let app_version = determine_version(&repo_root)?;

    let domain = if cli.uninstall && !cli.reinstall {
        None
    } else {
        Some(resolve_domain(
            cli.domain.clone(),
            existing_env.as_ref(),
            cli.assume_yes,
        )?)
    };

    if cli.reinstall {
        uninstall(&media_root, &cli.config)?;
        let install_config = InstallConfig {
            media_root,
            www_root,
            newtube_port,
            newtube_host: newtube_host.clone(),
            config_path: cli.config.clone(),
            domain_name: domain.expect("domain required"),
            app_version,
             release_repo: release_repo.clone(),
            assume_yes: cli.assume_yes,
        };
        install(install_config, &repo_root, &cli.trusted_pubkey)?;
        return Ok(());
    }

    if cli.uninstall {
        uninstall(&media_root, &cli.config)?;
        return Ok(());
    }

    let install_config = InstallConfig {
        media_root,
        www_root,
        newtube_port,
        newtube_host,
        config_path: cli.config,
        domain_name: domain.expect("domain required"),
        app_version,
        release_repo,
        assume_yes: cli.assume_yes,
    };

    install(install_config, &repo_root, &cli.trusted_pubkey)
}

#[derive(Clone, Debug)]
struct InstallConfig {
    media_root: PathBuf,
    www_root: PathBuf,
    newtube_port: u16,
    newtube_host: String,
    config_path: PathBuf,
    domain_name: String,
    app_version: String,
    release_repo: String,
    assume_yes: bool,
}

fn install(cfg: InstallConfig, repo_root: &Path, pubkey_source: &Path) -> Result<()> {
    log_info("Starting installation");
    fs::create_dir_all(&cfg.media_root)
        .with_context(|| format!("Creating media dir {}", cfg.media_root.display()))?;
    fs::create_dir_all(&cfg.www_root)
        .with_context(|| format!("Creating www dir {}", cfg.www_root.display()))?;
    ensure_directory(Path::new(BIN_ROOT), 0o750)?;

    ensure_service_accounts(&cfg)?;
    ensure_nginx_installed(cfg.assume_yes)?;
    deploy_nginx_config(&cfg.domain_name, &cfg.www_root, cfg.assume_yes)?;

    write_env_config(&cfg)?;
    install_trusted_pubkey(pubkey_source)?;
    install_systemd_units(&cfg)?;
    build_from_workspace(repo_root, &cfg)?;

    run_command("systemctl", &["daemon-reload"])?;
    run_command("systemctl", &["enable", "--now", BACKEND_SERVICE])?;
    run_command("systemctl", &["enable", "--now", ROUTINE_SERVICE])?;
    run_command("systemctl", &["enable", "--now", SOFTWARE_TIMER])?;
    show_status()?;

    Ok(())
}

fn uninstall(_media_root: &Path, config_path: &Path) -> Result<()> {
    log_info("Stopping timer and removing files");
    let _ = run_command_allow_fail("systemctl", &["disable", "--now", SOFTWARE_TIMER]);
    let _ = run_command_allow_fail("systemctl", &["disable", "--now", SOFTWARE_SERVICE]);
    let _ = run_command_allow_fail("systemctl", &["disable", "--now", BACKEND_SERVICE]);
    let _ = run_command_allow_fail("systemctl", &["disable", "--now", ROUTINE_SERVICE]);

    let systemd_dir = PathBuf::from("/etc/systemd/system");
    remove_path_if_exists(&systemd_dir.join(SOFTWARE_SERVICE))?;
    remove_path_if_exists(&systemd_dir.join(SOFTWARE_TIMER))?;
    remove_path_if_exists(&systemd_dir.join(BACKEND_SERVICE))?;
    remove_path_if_exists(&systemd_dir.join(ROUTINE_SERVICE))?;

    run_command("systemctl", &["daemon-reload"])?;

    if config_path.exists() {
        fs::remove_file(config_path)
            .with_context(|| format!("Removing {}", config_path.display()))?;
    }
    if Path::new(BIN_ROOT).exists() {
        fs::remove_dir_all(BIN_ROOT)
            .with_context(|| format!("Removing {}", BIN_ROOT))?;
    }
    log_info("Uninstall complete");
    Ok(())
}

fn cleanup_repo(repo_root: &Path) -> Result<()> {
    log_info("Cleaning repo artifacts");
    for dir in ["node_modules", "coverage"] {
        let path = repo_root.join(dir);
        if path.exists() {
            fs::remove_dir_all(&path).with_context(|| format!("Removing {}", path.display()))?;
        }
    }
    for file in ["download_channel", "backend", "routine-update"] {
        let path = repo_root.join(file);
        if path.exists() {
            if path.is_dir() {
                fs::remove_dir_all(&path)
                    .with_context(|| format!("Removing {}", path.display()))?;
            } else {
                fs::remove_file(&path).with_context(|| format!("Removing {}", path.display()))?;
            }
        }
    }
    run_command_in_dir("cargo", &["clean"], repo_root)?;
    Ok(())
}

fn ensure_directory(path: &Path, mode: u32) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("Creating {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}

fn install_trusted_pubkey(source: &Path) -> Result<()> {
    let dest = Path::new(DEFAULT_PUBLIC_KEY_PATH);
    if dest.exists() {
        return Ok(());
    }
    if !source.exists() {
        bail!(
            "Trusted public key not found at {}. Provide --trusted-pubkey pointing to the signer public key or copy one to {}.",
            source.display(),
            dest.display()
        );
    }
    fs::copy(source, dest)
        .with_context(|| format!("Copying {} to {}", source.display(), dest.display()))?;
    fs::set_permissions(dest, fs::Permissions::from_mode(0o640))?;
    Ok(())
}

fn build_from_workspace(repo_root: &Path, cfg: &InstallConfig) -> Result<()> {
    log_info("Building release binaries from working tree");
    run_command_in_dir("cargo", &["build", "--release"], repo_root)?;
    install_release_binaries(repo_root, Path::new(BIN_ROOT))?;
    copy_frontend_assets(repo_root, &cfg.www_root)?;
    ensure_media_permissions(&cfg.media_root)?;
    Ok(())
}

fn install_release_binaries(build_root: &Path, dest_dir: &Path) -> Result<()> {
    let target_dir = build_root.join("target").join("release");
    let binaries = ["backend", "download_channel", "routine_update", "installer"];
    for bin in binaries {
        let src = target_dir.join(bin);
        if !src.exists() {
            bail!("Missing compiled binary {}. Run cargo build --release first.", src.display());
        }
        let dest = dest_dir.join(bin);
        copy_executable(&src, &dest)?;
    }
    Ok(())
}

fn copy_executable(src: &Path, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(src, dest)
        .with_context(|| format!("Copying {} to {}", src.display(), dest.display()))?;
    fs::set_permissions(dest, fs::Permissions::from_mode(0o750))?;
    chown_to("root", VIEWTUBE_GROUP, dest)?;
    Ok(())
}

fn chown_to(owner: &str, group: &str, path: &Path) -> Result<()> {
    let status = Command::new("chown")
        .arg(format!("{}:{}", owner, group))
        .arg(path)
        .status()
        .with_context(|| format!("Updating ownership on {}", path.display()))?;
    if !status.success() {
        bail!("chown failed for {}", path.display());
    }
    Ok(())
}

fn copy_frontend_assets(src_root: &Path, dest_root: &Path) -> Result<()> {
    if dest_root.exists() {
        fs::remove_dir_all(dest_root)
            .with_context(|| format!("Removing stale assets at {}", dest_root.display()))?;
    }
    fs::create_dir_all(dest_root)?;
    for entry in fs::read_dir(src_root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else { continue };
        if FRONTEND_SKIP_ENTRIES.contains(&name_str) {
            continue;
        }
        let destination = dest_root.join(name_str);
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_directory_recursive(&entry.path(), &destination)?;
        } else if file_type.is_file() {
            copy_file_with_mode(&entry.path(), &destination, 0o644)?;
        }
    }
    Ok(())
}

fn copy_directory_recursive(src: &Path, dest: &Path) -> Result<()> {
    for entry in WalkDir::new(src) {
        let entry = entry?;
        let rel = match entry.path().strip_prefix(src) {
            Ok(rel) if rel.as_os_str().is_empty() => continue,
            Ok(rel) => rel,
            Err(_) => continue,
        };
        let target = dest.join(rel);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&target)?;
            fs::set_permissions(&target, fs::Permissions::from_mode(0o755))?;
        } else if entry.file_type().is_file() {
            copy_file_with_mode(entry.path(), &target, 0o644)?;
        }
    }
    Ok(())
}

fn copy_file_with_mode(src: &Path, dest: &Path, mode: u32) -> Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(src, dest)
        .with_context(|| format!("Copying {} to {}", src.display(), dest.display()))?;
    fs::set_permissions(dest, fs::Permissions::from_mode(mode))?;
    Ok(())
}

fn ensure_media_permissions(media_root: &Path) -> Result<()> {
    if !media_root.exists() {
        return Ok(());
    }
    let owner = format!("{}:{}", DOWNLOADER_USER, VIEWTUBE_GROUP);
    let status = Command::new("chown")
        .arg("-R")
        .arg(&owner)
        .arg(media_root)
        .status()
        .with_context(|| format!("Setting ownership on {}", media_root.display()))?;
    if !status.success() {
        bail!("chown failed for {}", media_root.display());
    }
    let chmod_status = Command::new("chmod")
        .arg("-R")
        .arg("g+rwX")
        .arg(media_root)
        .status()
        .with_context(|| format!("Setting permissions on {}", media_root.display()))?;
    if !chmod_status.success() {
        bail!("chmod failed for {}", media_root.display());
    }
    Ok(())
}

fn ensure_root() -> Result<()> {
    let output = Command::new("id")
        .arg("-u")
        .output()
        .context("Failed to run id -u")?;
    if !output.status.success() {
        bail!("id -u reported failure");
    }
    let uid: u32 = String::from_utf8(output.stdout)
        .context("id output not UTF-8")?
        .trim()
        .parse()
        .context("id output not a number")?;
    if uid != 0 {
        bail!("This installer must be run as root");
    }
    Ok(())
}

fn load_optional_token(path: Option<&Path>) -> Result<Option<String>> {
    if let Some(file) = path {
        if file.exists() {
            let token = fs::read_to_string(file)?;
            let trimmed = token.trim().to_string();
            if !trimmed.is_empty() {
                return Ok(Some(trimmed));
            }
        } else {
            log_info(format!(
                "GitHub token file {} not found; continuing without one",
                file.display()
            ));
        }
    }
    if let Ok(env_token) = env::var("GITHUB_TOKEN") {
        let trimmed = env_token.trim();
        if !trimmed.is_empty() {
            return Ok(Some(trimmed.to_string()));
        }
    }
    Ok(None)
}

fn write_env_config(cfg: &InstallConfig) -> Result<()> {
    let content = format!(
        "MEDIA_ROOT=\"{}\"\nWWW_ROOT=\"{}\"\nNEWTUBE_PORT=\"{}\"\nNEWTUBE_HOST=\"{}\"\nAPP_VERSION=\"{}\"\nDOMAIN_NAME=\"{}\"\nRELEASE_REPO=\"{}\"\n",
        cfg.media_root.display(),
        cfg.www_root.display(),
        cfg.newtube_port,
        cfg.newtube_host,
        cfg.app_version,
        cfg.domain_name,
        cfg.release_repo
    );
    fs::write(&cfg.config_path, content)
        .with_context(|| format!("Writing {}", cfg.config_path.display()))?;
    fs::set_permissions(&cfg.config_path, fs::Permissions::from_mode(0o640))?;
    let owner = format!("root:{}", VIEWTUBE_GROUP);
    let target = cfg.config_path.to_string_lossy().into_owned();
    let args = [owner.as_str(), target.as_str()];
    run_command("chown", &args)?;
    Ok(())
}

fn env_to_install_config(env: EnvConfig, config_path: PathBuf) -> Result<InstallConfig> {
    Ok(InstallConfig {
        media_root: env
            .media_root
            .ok_or_else(|| anyhow!("MEDIA_ROOT missing from {}", config_path.display()))?,
        www_root: env
            .www_root
            .ok_or_else(|| anyhow!("WWW_ROOT missing from {}", config_path.display()))?,
        newtube_port: env.newtube_port.unwrap_or(DEFAULT_NEWTUBE_PORT),
        newtube_host: env
            .newtube_host
            .unwrap_or_else(|| DEFAULT_NEWTUBE_HOST.to_string()),
        config_path,
        domain_name: env
            .domain_name
            .ok_or_else(|| anyhow!("DOMAIN_NAME missing from config"))?,
        app_version: env.app_version.unwrap_or_else(|| "unknown".into()),
        release_repo: env
            .release_repo
            .unwrap_or_else(|| DEFAULT_RELEASE_REPO.to_string()),
        assume_yes: true,
    })
}

fn resolve_media_root(
    cli_value: Option<PathBuf>,
    existing: Option<PathBuf>,
    assume_yes: bool,
) -> Result<PathBuf> {
    if let Some(path) = cli_value {
        return Ok(path);
    }
    if let Some(ref existing_path) = existing
        && (assume_yes
            || prompt_yes_no(
                &format!(
                    "Use detected media root '{}' (stores downloaded videos and metadata)?",
                    existing_path.display()
                ),
                true,
            )?)
    {
        return Ok(existing_path.clone());
    }
    if assume_yes {
        log_info(format!(
            "Using default media root {} due to --assume-yes",
            DEFAULT_MEDIA_DIR
        ));
        return Ok(PathBuf::from(DEFAULT_MEDIA_DIR));
    }
    prompt_for_media_root(existing)
}

fn resolve_www_root(
    cli_value: Option<PathBuf>,
    existing: Option<PathBuf>,
    assume_yes: bool,
) -> Result<PathBuf> {
    if let Some(path) = cli_value {
        return Ok(path);
    }
    if let Some(ref existing_path) = existing
        && (assume_yes
            || prompt_yes_no(
                &format!(
                    "Use detected WWW root '{}' (nginx will serve the web UI from here)?",
                    existing_path.display()
                ),
                true,
            )?)
    {
        return Ok(existing_path.clone());
    }
    if assume_yes {
        log_info(format!(
            "Using default WWW root {} due to --assume-yes",
            DEFAULT_WWW_DIR
        ));
        return Ok(PathBuf::from(DEFAULT_WWW_DIR));
    }
    prompt_for_www_root(existing)
}

fn resolve_port(cli_value: Option<u16>, existing: Option<u16>, assume_yes: bool) -> Result<u16> {
    if let Some(port) = cli_value {
        return Ok(port);
    }
    if let Some(existing_port) = existing
        && (assume_yes
            || prompt_yes_no(
                &format!("Use detected NEWTUBE_PORT '{existing_port}' (backend API listens here)?"),
                true,
            )?)
    {
        return Ok(existing_port);
    }
    if assume_yes {
        log_info(format!(
            "Using default NEWTUBE_PORT {} due to --assume-yes",
            DEFAULT_NEWTUBE_PORT
        ));
        return Ok(DEFAULT_NEWTUBE_PORT);
    }
    prompt_for_port(existing)
}

fn resolve_host(
    cli_value: Option<String>,
    existing: Option<String>,
    assume_yes: bool,
) -> Result<String> {
    if let Some(host) = cli_value {
        return validate_host(&host);
    }
    if let Some(ref existing_host) = existing
        && (assume_yes
            || prompt_yes_no(
                &format!("Use detected NEWTUBE_HOST '{existing_host}' (backend API binds here)?"),
                true,
            )?)
    {
        return Ok(existing_host.clone());
    }
    if assume_yes {
        log_info(format!(
            "Using default NEWTUBE_HOST {} due to --assume-yes",
            DEFAULT_NEWTUBE_HOST
        ));
        return Ok(DEFAULT_NEWTUBE_HOST.to_string());
    }
    prompt_for_host(existing)
}

fn resolve_release_repo(
    cli_value: Option<String>,
    existing: Option<String>,
    assume_yes: bool,
) -> Result<String> {
    if let Some(repo) = cli_value {
        return normalize_release_repo(&repo);
    }
    if let Some(ref saved) = existing
        && (assume_yes
            || prompt_yes_no(&format!("Use detected release repo '{saved}'?"), true)?)
    {
        return Ok(saved.clone());
    }
    if assume_yes {
        log_info(format!(
            "Using default release repo {} due to --assume-yes",
            DEFAULT_RELEASE_REPO
        ));
        return Ok(DEFAULT_RELEASE_REPO.to_string());
    }
    prompt_for_release_repo(existing)
}

fn prompt_for_media_root(default: Option<PathBuf>) -> Result<PathBuf> {
    println!();
    println!(
        "Media root is the directory where ViewTube stores every downloaded video, audio file, subtitle, thumbnail, and the metadata database."
    );
    println!(
        "Pick a location with ample free space; hundreds of gigabytes may be required depending on your library."
    );
    let suggested = default.unwrap_or_else(|| PathBuf::from(DEFAULT_MEDIA_DIR));
    prompt_for_path("Enter the full path for the media root", suggested)
}

fn prompt_for_www_root(default: Option<PathBuf>) -> Result<PathBuf> {
    println!();
    println!(
        "WWW root is the folder nginx serves to browsers. It will contain the built ViewTube frontend assets (index.html, JS, CSS)."
    );
    println!(
        "Choose the directory you want to expose via HTTPS; the installer will deploy the nginx config pointing here."
    );
    let suggested = default.unwrap_or_else(|| PathBuf::from(DEFAULT_WWW_DIR));
    prompt_for_path("Enter the full path for the WWW root", suggested)
}

fn prompt_for_port(default: Option<u16>) -> Result<u16> {
    println!();
    println!("NEWTUBE_PORT controls which TCP port the backend API listens on.");
    println!(
        "Keep 8080 unless another service already uses it or you plan to front it with a reverse proxy."
    );
    let suggested = default.unwrap_or(DEFAULT_NEWTUBE_PORT);
    loop {
        print!("Enter the port for the backend API [{}]: ", suggested);
        io::stdout().flush().ok();
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Ok(suggested);
        }
        match trimmed.parse::<u16>() {
            Ok(value @ 1..=65535) => return Ok(value),
            Ok(_) => println!("Port must be between 1 and 65535."),
            Err(_) => println!("Please enter a valid number between 1 and 65535."),
        }
    }
}

fn prompt_for_host(default: Option<String>) -> Result<String> {
    println!();
    println!("NEWTUBE_HOST controls which network interface the backend binds to.");
    println!(
        "Use 127.0.0.1 when placing nginx in front of the API so it stays unreachable from the public internet."
    );
    let suggested = default.unwrap_or_else(|| DEFAULT_NEWTUBE_HOST.to_string());
    loop {
        print!("Enter the listen address for the backend [{}]: ", suggested);
        io::stdout().flush().ok();
        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            bail!("Failed to read listen address");
        }
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Ok(suggested.clone());
        }
        match validate_host(trimmed) {
            Ok(value) => return Ok(value),
            Err(err) => eprintln!("{err}"),
        }
    }
}

fn prompt_for_release_repo(default: Option<String>) -> Result<String> {
    println!();
    println!("Enter the GitHub repository (owner/repo) that publishes signed releases.");
    let suggested = default.unwrap_or_else(|| DEFAULT_RELEASE_REPO.to_string());
    loop {
        print!("Release repository [{}]: ", suggested);
        io::stdout().flush().ok();
        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            bail!("Failed to read release repository input");
        }
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Ok(suggested.clone());
        }
        match normalize_release_repo(trimmed) {
            Ok(repo) => return Ok(repo),
            Err(err) => eprintln!("{err}"),
        }
    }
}

fn normalize_release_repo(input: &str) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("Repository cannot be empty");
    }
    if !trimmed.contains('/') {
        bail!("Repository must be in owner/repo format");
    }
    let mut parts = trimmed.split('/').filter(|part| !part.is_empty());
    let owner = parts
        .next()
        .ok_or_else(|| anyhow!("Missing repository owner"))?;
    let repo = parts
        .next()
        .ok_or_else(|| anyhow!("Missing repository name"))?;
    if parts.next().is_some() {
        bail!("Repository must be in owner/repo format");
    }
    Ok(format!("{}/{}", owner, repo))
}

fn validate_host(input: &str) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("Listen address cannot be empty");
    }
    trimmed
        .parse::<IpAddr>()
        .with_context(|| format!("Invalid listen address '{trimmed}'"))?;
    Ok(trimmed.to_string())
}

fn prompt_for_path(prompt: &str, default_path: PathBuf) -> Result<PathBuf> {
    let default_choice = default_path;
    loop {
        print!("{prompt} [{}]: ", default_choice.display());
        io::stdout().flush().ok();
        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            bail!("Failed to read input");
        }
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Ok(default_choice.clone());
        }
        let candidate = PathBuf::from(trimmed);
        if candidate.is_absolute() {
            return Ok(candidate);
        }
        println!("Please enter an absolute path (e.g., /srv/viewtube).");
    }
}

fn resolve_domain(
    cli_domain: Option<String>,
    existing: Option<&EnvConfig>,
    assume_yes: bool,
) -> Result<String> {
    if let Some(domain) = cli_domain {
        return normalize_domain(&domain);
    }
    if let Some(existing_domain) = existing.and_then(|cfg| cfg.domain_name.clone())
        && (assume_yes
            || prompt_yes_no(&format!("Use detected domain '{existing_domain}'?"), false)?)
    {
        return Ok(existing_domain);
    }
    prompt_for_domain(assume_yes)
}

fn prompt_for_domain(assume_yes: bool) -> Result<String> {
    if assume_yes {
        bail!("--domain must be provided when --assume-yes is used and no saved domain exists");
    }
    loop {
        print!("Enter the domain name serving ViewTube (e.g. example.com): ");
        io::stdout().flush().ok();
        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            bail!("Failed to read domain input");
        }
        match normalize_domain(&input) {
            Ok(domain) => return Ok(domain),
            Err(err) => eprintln!("{err}"),
        }
    }
}

fn normalize_domain(input: &str) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("Domain name cannot be empty");
    }
    let lower = trimmed.to_lowercase();
    let stripped = lower
        .strip_prefix("https://")
        .or_else(|| lower.strip_prefix("http://"))
        .unwrap_or(&lower)
        .trim_matches('/')
        .to_string();
    if stripped.is_empty() {
        bail!("Domain name cannot be empty");
    }
    if stripped.contains('/') {
        bail!("Domain name cannot contain path segments");
    }
    if stripped.contains(char::is_whitespace) {
        bail!("Domain name cannot contain whitespace");
    }
    Ok(stripped)
}

fn prompt_yes_no(prompt: &str, default_yes: bool) -> Result<bool> {
    let default_indicator = if default_yes { "[Y/n]" } else { "[y/N]" };
    loop {
        print!("{prompt} {default_indicator} ");
        io::stdout().flush().ok();
        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            bail!("Input aborted");
        }
        let value = input.trim().to_lowercase();
        if value.is_empty() {
            return Ok(default_yes);
        }
        match value.as_str() {
            "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => println!("Please answer y or n."),
        }
    }
}

fn ensure_nginx_installed(assume_yes: bool) -> Result<()> {
    if service_exists(NGINX_SERVICE)? {
        log_info("nginx service detected");
        return Ok(());
    }
    log_info("nginx service not detected");
    if assume_yes || prompt_yes_no("Install nginx via package manager?", false)? {
        install_nginx_package().context("Unable to install nginx")?;
    } else {
        bail!("nginx is required for setup");
    }
    Ok(())
}

fn ensure_service_accounts(cfg: &InstallConfig) -> Result<()> {
    ensure_group_exists(VIEWTUBE_GROUP)?;
    ensure_user_exists(BACKEND_USER, VIEWTUBE_GROUP, BACKEND_HOME)?;
    ensure_user_exists(DOWNLOADER_USER, VIEWTUBE_GROUP, DOWNLOADER_HOME)?;
    ensure_media_permissions(&cfg.media_root)?;
    Ok(())
}

fn ensure_group_exists(name: &str) -> Result<()> {
    match Command::new("getent").args(["group", name]).status() {
        Ok(status) if status.success() => return Ok(()),
        Ok(_) | Err(_) => {}
    }
    run_command("groupadd", &["--system", name])
}

fn ensure_user_exists(user: &str, group: &str, home: &str) -> Result<()> {
    match Command::new("id").args(["-u", user]).status() {
        Ok(status) if status.success() => {
            fs::create_dir_all(home).with_context(|| format!("Creating home directory {home}"))?;
            return Ok(());
        }
        Ok(_) | Err(_) => {}
    }

    fs::create_dir_all(home).with_context(|| format!("Creating home directory {home}"))?;
    let home_owned = home.to_string();
    let args = [
        "--system",
        "--create-home",
        "--home",
        home_owned.as_str(),
        "--shell",
        "/usr/sbin/nologin",
        "--gid",
        group,
        user,
    ];
    run_command("useradd", &args)
}

fn service_exists(name: &str) -> Result<bool> {
    let stdout = run_command_capture("systemctl", &["list-unit-files", "--type=service", "--all"])?;
    let needle = format!("{name}.service");
    Ok(stdout
        .lines()
        .map(str::trim_start)
        .any(|line| line.starts_with(&needle)))
}

fn install_nginx_package() -> Result<()> {
    let manager = detect_package_manager()
        .ok_or_else(|| anyhow!("Could not detect a supported package manager"))?;
    match manager {
        "apt-get" => {
            run_command("apt-get", &["update"])?;
            run_command("apt-get", &["install", "-y", "nginx"])?;
        }
        "apt" => {
            run_command("apt", &["update"])?;
            run_command("apt", &["install", "-y", "nginx"])?;
        }
        "dnf" => run_command("dnf", &["install", "-y", "nginx"])?,
        "yum" => run_command("yum", &["install", "-y", "nginx"])?,
        "pacman" => run_command("pacman", &["-Sy", "--noconfirm", "nginx"])?,
        "apk" => {
            run_command("apk", &["update"])?;
            run_command("apk", &["add", "nginx"])?;
        }
        "zypper" => {
            run_command("zypper", &["refresh"])?;
            run_command("zypper", &["install", "-y", "nginx"])?;
        }
        other => bail!("Unsupported package manager {other}"),
    }
    run_command("systemctl", &["enable", "--now", NGINX_SERVICE])?;
    Ok(())
}

fn detect_package_manager() -> Option<&'static str> {
    let managers = ["apt-get", "apt", "dnf", "yum", "pacman", "apk", "zypper"];
    managers.into_iter().find(|mgr| command_exists(mgr))
}

fn command_exists(bin: &str) -> bool {
    if let Some(paths) = env::var_os("PATH") {
        for path in env::split_paths(&paths) {
            if path.join(bin).exists() {
                return true;
            }
        }
    }
    false
}

fn deploy_nginx_config(domain: &str, www_root: &Path, assume_yes: bool) -> Result<()> {
    let (config_path, symlink_path) = if Path::new("/etc/nginx/sites-available").is_dir() {
        (
            PathBuf::from("/etc/nginx/sites-available/viewtube.conf"),
            Some(PathBuf::from("/etc/nginx/sites-enabled/viewtube.conf")),
        )
    } else {
        (PathBuf::from("/etc/nginx/conf.d/viewtube.conf"), None)
    };
    let action = if config_path.exists() {
        "replace"
    } else {
        "create"
    };
    let prompt = format!(
        "Deploy the recommended nginx config to {} (will {action} existing content)?",
        config_path.display()
    );
    if !(assume_yes || prompt_yes_no(&prompt, false)?) {
        log_info("Skipping nginx config deployment");
        return Ok(());
    }
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let server_block = format!(
        "server {{\n    listen 80;\n    listen [::]:80;\n    server_name {domain};\n\n    return 301 https://{domain}$request_uri;\n}}\n\nserver {{\n    listen 443 ssl http2;\n    listen [::]:443 ssl http2;\n    server_name {domain};\n\n    ssl_certificate /etc/letsencrypt/live/{domain}/fullchain.pem;\n    ssl_certificate_key /etc/letsencrypt/live/{domain}/privkey.pem;\n    ssl_prefer_server_ciphers on;\n\n    root {www};\n    index index.html;\n\n    location / {{\n        try_files $uri $uri/ /index.html;\n    }}\n}}\n",
        domain = domain,
        www = www_root.display()
    );
    fs::write(&config_path, server_block)?;
    if let Some(symlink_dest) = symlink_path {
        if let Some(parent) = symlink_dest.parent() {
            fs::create_dir_all(parent)?;
        }
        if symlink_dest.exists() {
            fs::remove_file(&symlink_dest)?;
        }
        symlink(&config_path, symlink_dest)?;
    }
    run_command("nginx", &["-t"])?;
    run_command("systemctl", &["reload", NGINX_SERVICE])?;
    Ok(())
}


fn install_systemd_units(cfg: &InstallConfig) -> Result<()> {
    let systemd_dir = PathBuf::from("/etc/systemd/system");
    fs::create_dir_all(&systemd_dir)?;

    let updater_service = systemd_dir.join(SOFTWARE_SERVICE);
    let timer_path = systemd_dir.join(SOFTWARE_TIMER);
    let backend_service = systemd_dir.join(BACKEND_SERVICE);
    let routine_service = systemd_dir.join(ROUTINE_SERVICE);

    let installer_exec = escape_systemd_path(&Path::new(BIN_ROOT).join("installer"))?;
    let pubkey_path = escape_systemd_path(Path::new(DEFAULT_PUBLIC_KEY_PATH))?;
    let config_path = escape_systemd_path(&cfg.config_path)?;
    let updater_contents = format!(
        "[Unit]\nDescription=Fetch and build signed ViewTube releases\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=oneshot\nUser=root\nWorkingDirectory=/\nExecStart={exec} --auto-update --config {config} --trusted-pubkey {pubkey}\nTimeoutStartSec=3600\n\n[Install]\nWantedBy=multi-user.target\n",
        exec = installer_exec,
        config = config_path,
        pubkey = pubkey_path
    );
    fs::write(&updater_service, updater_contents)?;

    let timer_contents = "[Unit]\nDescription=Scan for signed ViewTube releases nightly\n\n[Timer]\nOnCalendar=*-*-* 03:00\nPersistent=true\nUnit=software-updater.service\n\n[Install]\nWantedBy=timers.target\n";
    fs::write(&timer_path, timer_contents)?;

    let media_work_dir = escape_systemd_path(&cfg.media_root)?;
    let backend_exec = escape_systemd_path(&Path::new(BIN_ROOT).join("backend"))?;
    let backend_contents = format!(
        "[Unit]\nDescription=ViewTube backend API\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nUser={user}\nGroup={group}\nWorkingDirectory={work}\nExecStart={exec} --config {config}\nRestart=on-failure\nRestartSec=2\nAmbientCapabilities=\nCapabilityBoundingSet=\nNoNewPrivileges=yes\nProtectSystem=full\nProtectHome=read-only\nPrivateTmp=yes\nRestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX\nRestrictSUIDSGID=yes\nRestrictRealtime=yes\nLockPersonality=yes\nUMask=0027\nReadWritePaths={work}\n\n[Install]\nWantedBy=multi-user.target\n",
        user = BACKEND_USER,
        group = VIEWTUBE_GROUP,
        work = media_work_dir,
        exec = backend_exec,
        config = config_path
    );
    fs::write(&backend_service, backend_contents)?;

    let routine_exec = escape_systemd_path(&Path::new(BIN_ROOT).join("routine_update"))?;
    let www_dir = escape_systemd_path(&cfg.www_root)?;
    let routine_contents = format!(
        "[Unit]\nDescription=ViewTube nightly channel refresh\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=oneshot\nUser={user}\nGroup={group}\nWorkingDirectory={work}\nExecStart={exec} --config {config} --media-root {work} --www-root {www}\nAmbientCapabilities=\nCapabilityBoundingSet=\nNoNewPrivileges=yes\nProtectSystem=full\nProtectHome=read-only\nPrivateTmp=yes\nRestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX\nRestrictSUIDSGID=yes\nRestrictRealtime=yes\nLockPersonality=yes\nUMask=0027\nReadWritePaths={work}\n\n[Install]\nWantedBy=multi-user.target\n",
        user = DOWNLOADER_USER,
        group = VIEWTUBE_GROUP,
        work = media_work_dir,
        exec = routine_exec,
        config = config_path,
        www = www_dir
    );
    fs::write(&routine_service, routine_contents)?;
    Ok(())
}

fn generate_signing_keypair(cli: &Cli) -> Result<()> {
    let dir = cli
        .key_dir
        .as_ref()
        .ok_or_else(|| anyhow!("--key-dir is required when using --keygen"))?;
    fs::create_dir_all(dir)?;
    let private_path = dir.join("viewtube-release.key");
    let public_path = dir.join("viewtube-release.pub");
    if private_path.exists() || public_path.exists() {
        bail!("Signing key already exists in {}", dir.display());
    }
    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();
    let private_file = SerializedPrivateKey {
        algorithm: "ed25519".into(),
        private_key: BASE64.encode(signing_key.to_bytes()),
        public_key: BASE64.encode(verifying_key.to_bytes()),
    };
    let public_file = SerializedPublicKey {
        algorithm: "ed25519".into(),
        public_key: BASE64.encode(verifying_key.to_bytes()),
    };
    fs::write(&private_path, serde_json::to_vec_pretty(&private_file)?)?;
    fs::set_permissions(&private_path, fs::Permissions::from_mode(0o600))?;
    fs::write(&public_path, serde_json::to_vec_pretty(&public_file)?)?;
    fs::set_permissions(&public_path, fs::Permissions::from_mode(0o644))?;
    println!("Generated signing key: {}", private_path.display());
    println!("Generated public key: {}", public_path.display());
    Ok(())
}

fn package_release_artifacts(repo_root: &Path, cli: &Cli) -> Result<()> {
    let tag = cli
        .release_tag
        .as_ref()
        .ok_or_else(|| anyhow!("--release-tag is required"))?;
    let output_dir = cli
        .output_dir
        .as_ref()
        .ok_or_else(|| anyhow!("--output-dir is required"))?;
    let signing_key_path = cli
        .signing_key
        .as_ref()
        .ok_or_else(|| anyhow!("--signing-key is required"))?;

    fs::create_dir_all(output_dir)?;
    run_command_in_dir("cargo", &["build", "--release"], repo_root)?;

    let src_name = format!("{SOURCE_ARCHIVE_PREFIX}-{tag}.tar.gz");
    let bin_name = format!("{BINARY_ARCHIVE_PREFIX}-{tag}.tar.gz");
    let src_path = output_dir.join(&src_name);
    let bin_path = output_dir.join(&bin_name);

    package_source_archive(repo_root, &src_path)?;
    package_binary_archive(repo_root, &bin_path)?;

    let signing_key = load_signing_key(signing_key_path)?;
    sign_release_file(&src_path, &signature_path_for(&src_path), &signing_key, tag)?;
    sign_release_file(&bin_path, &signature_path_for(&bin_path), &signing_key, tag)?;

    log_info(format!(
        "Release artifacts written to {}",
        output_dir.display()
    ));
    Ok(())
}

fn signature_path_for(archive: &Path) -> PathBuf {
    archive.with_extension("tar.gz.sig")
}

fn package_source_archive(repo_root: &Path, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(dest)?;
    let encoder = GzEncoder::new(file, Compression::best());
    let mut builder = Builder::new(encoder);
    for entry in WalkDir::new(repo_root).into_iter().filter_map(|e| e.ok()) {
        let rel = match entry.path().strip_prefix(repo_root) {
            Ok(rel) if rel.as_os_str().is_empty() => continue,
            Ok(rel) => rel,
            Err(_) => continue,
        };
        if should_skip_source_entry(rel) {
            continue;
        }
        let target = Path::new(SOURCE_ROOT_DIR).join(rel);
        if entry.file_type().is_dir() {
            builder.append_dir(target, entry.path())?;
        } else if entry.file_type().is_file() {
            builder.append_path_with_name(entry.path(), target)?;
        }
    }
    builder.finish()?;
    builder.into_inner()?.finish()?;
    Ok(())
}

fn should_skip_source_entry(rel: &Path) -> bool {
    let components: Vec<&str> = rel
        .components()
        .filter_map(|comp| comp.as_os_str().to_str())
        .collect();
    if components.is_empty() {
        return false;
    }
    matches!(components[0], ".git" | "target" | "node_modules" | "coverage")
}

fn package_binary_archive(repo_root: &Path, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let stage = TempDir::new()?;
    let bundle_root = stage.path().join(BINARY_ROOT_DIR);
    let bin_stage = bundle_root.join("bin");
    let www_stage = bundle_root.join("www");
    fs::create_dir_all(&bin_stage)?;
    fs::create_dir_all(&www_stage)?;
    copy_release_binaries_for_archive(repo_root, &bin_stage)?;
    copy_frontend_assets(repo_root, &www_stage)?;

    let file = File::create(dest)?;
    let encoder = GzEncoder::new(file, Compression::best());
    let mut builder = Builder::new(encoder);
    builder.append_dir_all(Path::new(BINARY_ROOT_DIR).join("bin"), &bin_stage)?;
    builder.append_dir_all(Path::new(BINARY_ROOT_DIR).join("www"), &www_stage)?;
    builder.finish()?;
    builder.into_inner()?.finish()?;
    Ok(())
}

fn copy_release_binaries_for_archive(repo_root: &Path, dest_dir: &Path) -> Result<()> {
    let target_dir = repo_root.join("target").join("release");
    let binaries = ["backend", "download_channel", "routine_update", "installer"];
    for bin in binaries {
        let src = target_dir.join(bin);
        if !src.exists() {
            bail!("Missing compiled binary {}", src.display());
        }
        let dest = dest_dir.join(bin);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(&src, &dest)
            .with_context(|| format!("Copying {} to {}", src.display(), dest.display()))?;
        fs::set_permissions(&dest, fs::Permissions::from_mode(0o750))?;
    }
    Ok(())
}

fn sign_release_file(
    artifact: &Path,
    signature_path: &Path,
    signing_key: &SigningKey,
    version: &str,
) -> Result<()> {
    let digest = compute_blake3_hex(artifact)?;
    let message = signature_message(version, &digest);
    let signature = signing_key.sign(&message);
    let payload = ReleaseSignature {
        format: RELEASE_SIG_VERSION,
        version: version.into(),
        digest,
        signature: BASE64.encode(signature.to_bytes()),
    };
    fs::write(signature_path, serde_json::to_vec_pretty(&payload)?)?;
    Ok(())
}

fn auto_update_from_github(config_path: &Path, pubkey_path: &Path, token: Option<&str>) -> Result<()> {
    let env_cfg = read_env_config(config_path)?.ok_or_else(|| {
        anyhow!(
            "Missing env config at {}. Install ViewTube before running auto-update",
            config_path.display()
        )
    })?;
    let current_version = env_cfg.app_version.clone().unwrap_or_default();
    let release_repo = env_cfg
        .release_repo
        .clone()
        .unwrap_or_else(|| DEFAULT_RELEASE_REPO.to_string());

    let agent = Agent::new();
    let release = fetch_latest_release(&agent, &release_repo, token)?;
    if !current_version.is_empty() && current_version == release.tag_name {
        log_info(format!(
            "Already running latest release {}; skipping update",
            release.tag_name
        ));
        return Ok(());
    }

    let src_name = format!("{SOURCE_ARCHIVE_PREFIX}-{}.tar.gz", release.tag_name);
    let sig_name = format!("{SOURCE_ARCHIVE_PREFIX}-{}.tar.gz.sig", release.tag_name);
    let src_asset = release
        .assets
        .iter()
        .find(|asset| asset.name == src_name)
        .ok_or_else(|| anyhow!("Source archive {src_name} not found in latest release"))?;
    let sig_asset = release
        .assets
        .iter()
        .find(|asset| asset.name == sig_name)
        .ok_or_else(|| anyhow!("Signature {sig_name} not found in latest release"))?;

    let temp = TempDir::new()?;
    let src_path = temp.path().join(&src_name);
    let sig_path = temp.path().join(&sig_name);
    download_asset(&agent, &src_asset.browser_download_url, token, &src_path)?;
    download_asset(&agent, &sig_asset.browser_download_url, token, &sig_path)?;

    apply_signed_source_archive(
        config_path,
        &src_path,
        &sig_path,
        pubkey_path,
        Some(&release.tag_name),
    )
}

fn fetch_latest_release(agent: &Agent, repo: &str, token: Option<&str>) -> Result<GithubRelease> {
    let url = format!("{GITHUB_API_BASE}/repos/{repo}/releases/latest");
    let response = github_get(agent, &url, token)?;
    response
        .into_json::<GithubRelease>()
        .map_err(|err| anyhow!("Failed to parse release JSON: {err}"))
}

fn download_asset(agent: &Agent, url: &str, token: Option<&str>, dest: &Path) -> Result<()> {
    let mut request = agent.get(url).set("User-Agent", "viewtube-installer");
    if let Some(token) = token {
        request = request.set("Authorization", &format!("token {token}"));
    }
    let response = request
        .call()
        .map_err(|err| anyhow!("Failed to download asset {url}: {err}"))?;
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(dest)?;
    std::io::copy(&mut response.into_reader(), &mut file)?;
    Ok(())
}

fn github_get(agent: &Agent, url: &str, token: Option<&str>) -> Result<Response> {
    let mut request = agent.get(url).set("User-Agent", "viewtube-installer");
    if let Some(token) = token {
        request = request.set("Authorization", &format!("token {token}"));
    }
    let response = request.call().map_err(|err| anyhow!("GitHub request failed: {err}"))?;
    if !(200..300).contains(&response.status()) {
        bail!(
            "GitHub API returned status {} for {}",
            response.status(),
            url
        );
    }
    Ok(response)
}

#[derive(Deserialize)]
struct GithubRelease {
    tag_name: String,
    assets: Vec<GithubAsset>,
}

#[derive(Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
}

fn apply_signed_source_archive(
    config_path: &Path,
    artifact: &Path,
    signature: &Path,
    pubkey_path: &Path,
    expected_version: Option<&str>,
) -> Result<()> {
    let verifying_key = load_public_key(pubkey_path)?;
    let metadata = verify_release_signature(artifact, signature, &verifying_key)?;
    if let Some(expected) = expected_version {
        if expected != metadata.version {
            bail!(
                "Release signature reports version {} but updater expected {}",
                metadata.version, expected
            );
        }
    }

    log_info(format!(
        "Applying release {} (digest {})",
        metadata.version, metadata.digest
    ));

    let temp = TempDir::new()?;
    let decoder = GzDecoder::new(File::open(artifact)?);
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(temp.path())?;

    let source_root = temp.path().join(SOURCE_ROOT_DIR);
    if !source_root.is_dir() {
        bail!("Release archive missing '{}' directory", SOURCE_ROOT_DIR);
    }

    run_command_in_dir("cargo", &["build", "--release"], &source_root)?;
    install_release_binaries(&source_root, Path::new(BIN_ROOT))?;

    let runtime = load_runtime_paths_from(config_path)?;
    copy_frontend_assets(&source_root, &runtime.www_root)?;
    ensure_media_permissions(&runtime.media_root)?;

    let env_cfg = read_env_config(config_path)?.ok_or_else(|| {
        anyhow!(
            "Missing env config at {} when updating release",
            config_path.display()
        )
    })?;
    let mut snapshot = env_to_install_config(env_cfg, config_path.to_path_buf())?;
    snapshot.app_version = metadata.version.clone();
    write_env_config(&snapshot)?;

    run_command("systemctl", &["restart", BACKEND_SERVICE])?;
    run_command("systemctl", &["restart", ROUTINE_SERVICE])?;
    run_command("systemctl", &["reload", NGINX_SERVICE])?;
    Ok(())
}

fn verify_release_signature(
    artifact: &Path,
    signature_path: &Path,
    verifying_key: &VerifyingKey,
) -> Result<ReleaseSignature> {
    let payload: ReleaseSignature = serde_json::from_slice(&fs::read(signature_path)?)?;
    if payload.format != RELEASE_SIG_VERSION {
        bail!(
            "Unsupported release signature format {}",
            payload.format
        );
    }
    let digest = compute_blake3_hex(artifact)?;
    if digest != payload.digest {
        bail!("Release checksum mismatch (expected {}, got {})", payload.digest, digest);
    }
    let signature_bytes: [u8; 64] = BASE64
        .decode(payload.signature.as_bytes())?
        .try_into()
        .map_err(|_| anyhow!("Invalid signature length"))?;
    let signature = Signature::from_bytes(&signature_bytes);
    let message = signature_message(&payload.version, &payload.digest);
    verifying_key
        .verify_strict(&message, &signature)
        .map_err(|_| anyhow!("Signature verification failed"))?;
    Ok(payload)
}

fn load_signing_key(path: &Path) -> Result<SigningKey> {
    let data: SerializedPrivateKey = serde_json::from_slice(&fs::read(path)?)?;
    if data.algorithm != "ed25519" {
        bail!("Unsupported signing key algorithm {}", data.algorithm);
    }
    let secret_bytes: [u8; 32] = BASE64
        .decode(data.private_key.as_bytes())?
        .try_into()
        .map_err(|_| anyhow!("Invalid private key length"))?;
    Ok(SigningKey::from_bytes(&secret_bytes))
}

fn load_public_key(path: &Path) -> Result<VerifyingKey> {
    let data: SerializedPublicKey = serde_json::from_slice(&fs::read(path)?)?;
    if data.algorithm != "ed25519" {
        bail!("Unsupported public key algorithm {}", data.algorithm);
    }
    let public_bytes: [u8; 32] = BASE64
        .decode(data.public_key.as_bytes())?
        .try_into()
        .map_err(|_| anyhow!("Invalid public key length"))?;
    VerifyingKey::from_bytes(&public_bytes).map_err(|err| anyhow!("{err}"))
}

fn signature_message(version: &str, digest_hex: &str) -> Vec<u8> {
    format!(
        "{}|v{}|{}|{}",
        RELEASE_SIG_PREFIX, RELEASE_SIG_VERSION, version, digest_hex
    )
    .into_bytes()
}

fn compute_blake3_hex(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Hasher::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

#[derive(Serialize, Deserialize)]
struct SerializedPrivateKey {
    algorithm: String,
    private_key: String,
    public_key: String,
}

#[derive(Serialize, Deserialize)]
struct SerializedPublicKey {
    algorithm: String,
    public_key: String,
}

#[derive(Serialize, Deserialize)]
struct ReleaseSignature {
    format: u32,
    version: String,
    digest: String,
    signature: String,
}

fn show_status() -> Result<()> {
    let _ = run_command_allow_fail("systemctl", &["status", BACKEND_SERVICE]);
    let _ = run_command_allow_fail("systemctl", &["status", SOFTWARE_TIMER]);
    let _ = run_command_allow_fail("systemctl", &["list-timers"]);
    Ok(())
}

fn run_command(cmd: &str, args: &[&str]) -> Result<()> {
    let printable = format_command(cmd, args);
    log_info(format!("Running: {printable}"));
    let status = Command::new(cmd)
        .args(args)
        .status()
        .with_context(|| format!("Failed to run {cmd}"))?;
    if !status.success() {
        bail!("Command {cmd} failed with status {status}");
    }
    Ok(())
}

fn run_command_in_dir(cmd: &str, args: &[&str], dir: &Path) -> Result<()> {
    let status = Command::new(cmd)
        .args(args)
        .current_dir(dir)
        .status()
        .with_context(|| format!("Failed to run {cmd}"))?;
    if !status.success() {
        bail!("Command {cmd} failed with status {status}");
    }
    Ok(())
}

fn run_command_capture(cmd: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(cmd)
        .args(args)
        .output()
        .with_context(|| format!("Failed to run {cmd}"))?;
    if !output.status.success() {
        bail!("Command {cmd} failed");
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn run_command_allow_fail(cmd: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(cmd).args(args).status()?;
    if !status.success() {
        eprintln!(
            "[installer] Warning: {} exited with status {status}",
            format_command(cmd, args)
        );
    }
    Ok(())
}

fn remove_path_if_exists(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_file(path).with_context(|| format!("Removing {}", path.display()))?;
    }
    Ok(())
}

fn format_command(cmd: &str, args: &[&str]) -> String {
    if args.is_empty() {
        return cmd.to_string();
    }
    let mut printable = String::from(cmd);
    for arg in args {
        printable.push(' ');
        printable.push_str(arg);
    }
    printable
}

fn escape_systemd_path(path: &Path) -> Result<String> {
    let display = path.to_string_lossy().to_string();
    if !display.contains(' ') {
        return Ok(display);
    }
    let output = Command::new("systemd-escape")
        .arg("--path")
        .arg(path)
        .output();
    match output {
        Ok(result) if result.status.success() => {
            Ok(String::from_utf8_lossy(&result.stdout).trim().to_string())
        }
        _ => Ok(display.replace(' ', "\\x20")),
    }
}

fn determine_version(repo_root: &Path) -> Result<String> {
    let cargo = fs::read_to_string(repo_root.join("Cargo.toml"))?;
    let value: toml::Value = toml::from_str(&cargo)?;
    value
        .get("package")
        .and_then(|pkg| pkg.get("version"))
        .and_then(|version| version.as_str())
        .ok_or_else(|| anyhow!("Failed to read version from Cargo.toml"))
        .map(|s| s.to_string())
}

fn log_info(msg: impl AsRef<str>) {
    println!("[installer] {}", msg.as_ref());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Write, path::Path};
    use tempfile::NamedTempFile;

    #[test]
    fn normalize_domain_strips_protocols() {
        assert_eq!(
            normalize_domain("https://Example.com/").unwrap(),
            "example.com"
        );
    }

    #[test]
    fn normalize_domain_rejects_whitespace() {
        assert!(normalize_domain("foo bar").is_err());
    }

    #[test]
    fn read_env_config_parses_values() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            "MEDIA_ROOT=\"/yt\"\nWWW_ROOT=\"/www\"\nAPP_VERSION=\"1.2.3\"\nDOMAIN_NAME=\"demo.example\"\n"
        )
        .unwrap();
        let cfg = read_env_config(file.path()).unwrap().unwrap();
        assert_eq!(cfg.media_root.unwrap(), PathBuf::from("/yt"));
        assert_eq!(cfg.www_root.unwrap(), PathBuf::from("/www"));
        assert_eq!(cfg.app_version.unwrap(), "1.2.3");
        assert_eq!(cfg.domain_name.unwrap(), "demo.example");
    }
}
