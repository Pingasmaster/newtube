#![forbid(unsafe_code)]

use anyhow::{Context, Result, anyhow, bail};
use clap::{ArgGroup, Parser};
use newtube_tools::config::{
    DEFAULT_CONFIG_PATH, DEFAULT_NEWTUBE_HOST, DEFAULT_NEWTUBE_PORT, EnvConfig, read_env_config,
};
use std::{
    env, fs,
    io::{self, Write},
    net::IpAddr,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::Command,
};

const DEFAULT_MEDIA_DIR: &str = "/yt";
const DEFAULT_WWW_DIR: &str = "/www/newtube.com";
const HELPER_SCRIPT_NAME: &str = "viewtube-update-build-run.sh";
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

#[derive(Parser, Debug)]
#[command(author, version, about = "Install and manage ViewTube services.")]
#[command(group(
    ArgGroup::new("mode")
        .args(["uninstall", "cleanup", "reinstall"])
        .multiple(false)
))]
struct Cli {
    #[arg(short = 'u', long = "uninstall", help = "Uninstall the service")]
    uninstall: bool,
    #[arg(
        short = 'c',
        long = "cleanup",
        help = "Cleanup build and runtime artifacts in the repo"
    )]
    cleanup: bool,
    #[arg(
        short = 'r',
        long = "reinstall",
        help = "Uninstall then install the latest version"
    )]
    reinstall: bool,
    #[arg(
        long = "media-dir",
        value_name = "PATH",
        help = "Override the media directory (default /yt)"
    )]
    media_dir: Option<PathBuf>,
    #[arg(
        long = "www-dir",
        value_name = "PATH",
        help = "Override the www directory (default /www/newtube.com)"
    )]
    www_dir: Option<PathBuf>,
    #[arg(
        long = "port",
        value_name = "PORT",
        help = "Override the backend API port (default 8080)"
    )]
    port: Option<u16>,
    #[arg(
        long = "host",
        value_name = "IP",
        help = "Override the backend listen address (default 127.0.0.1)"
    )]
    host: Option<String>,
    #[arg(
        long = "domain",
        value_name = "NAME",
        help = "Domain name serving ViewTube (e.g., example.com)"
    )]
    domain: Option<String>,
    #[arg(long = "config", value_name = "PATH", default_value = DEFAULT_CONFIG_PATH, help = "Path to the config file")]
    config: PathBuf,
    #[arg(
        short = 'y',
        long = "assume-yes",
        help = "Automatically answer yes to prompts"
    )]
    assume_yes: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let repo_root = env::current_dir().context("Failed to determine current directory")?;

    if cli.cleanup {
        cleanup_repo(&repo_root)?;
        return Ok(());
    }

    ensure_root()?;

    let existing_env = read_env_config(&cli.config)?;
    let needs_prompt = !cli.uninstall || cli.reinstall;
    let port_arg = cli.port;
    let host_arg = cli.host.clone();
    let (media_root, www_root, newtube_port, newtube_host) = if needs_prompt {
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
            assume_yes: cli.assume_yes,
        };
        install(install_config)?;
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
        assume_yes: cli.assume_yes,
    };

    install(install_config)
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
    assume_yes: bool,
}

fn install(cfg: InstallConfig) -> Result<()> {
    log_info("Starting installation");
    fs::create_dir_all(&cfg.media_root)
        .with_context(|| format!("Creating media dir {}", cfg.media_root.display()))?;
    fs::create_dir_all(&cfg.www_root)
        .with_context(|| format!("Creating www dir {}", cfg.www_root.display()))?;

    ensure_service_accounts(&cfg)?;
    ensure_nginx_installed(cfg.assume_yes)?;
    deploy_nginx_config(&cfg.domain_name, &cfg.www_root, cfg.assume_yes)?;

    write_env_config(&cfg)?;
    write_helper_script(&cfg)?;
    install_systemd_units(&cfg)?;

    run_command("systemctl", &["daemon-reload"])?;
    run_command("systemctl", &["enable", BACKEND_SERVICE])?;
    run_command("systemctl", &["enable", "--now", SOFTWARE_TIMER])?;

    run_helper_script(&cfg.media_root.join(HELPER_SCRIPT_NAME))?;
    show_status()?;

    Ok(())
}

fn uninstall(media_root: &Path, config_path: &Path) -> Result<()> {
    log_info("Stopping timer and removing files");
    let script_path = media_root.join(HELPER_SCRIPT_NAME);
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

    if script_path.exists() {
        fs::remove_file(&script_path)
            .with_context(|| format!("Removing {}", script_path.display()))?;
    }
    if config_path.exists() {
        fs::remove_file(config_path)
            .with_context(|| format!("Removing {}", config_path.display()))?;
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

fn write_env_config(cfg: &InstallConfig) -> Result<()> {
    let content = format!(
        "MEDIA_ROOT=\"{}\"\nWWW_ROOT=\"{}\"\nNEWTUBE_PORT=\"{}\"\nNEWTUBE_HOST=\"{}\"\nAPP_VERSION=\"{}\"\nDOMAIN_NAME=\"{}\"\n",
        cfg.media_root.display(),
        cfg.www_root.display(),
        cfg.newtube_port,
        cfg.newtube_host,
        cfg.app_version,
        cfg.domain_name
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
    apply_media_permissions(&cfg.media_root)?;
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

fn apply_media_permissions(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let owner = format!("{}:{}", DOWNLOADER_USER, VIEWTUBE_GROUP);
    let target = path.to_string_lossy().into_owned();
    let chown_args = ["-R", owner.as_str(), target.as_str()];
    run_command("chown", &chown_args)?;
    let chmod_args = ["-R", "g+rwX", target.as_str()];
    run_command("chmod", &chmod_args)
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

fn write_helper_script(cfg: &InstallConfig) -> Result<()> {
    let helper_path = cfg.media_root.join(HELPER_SCRIPT_NAME);
    let content = helper_script_contents(cfg);
    fs::write(&helper_path, content)?;
    fs::set_permissions(&helper_path, fs::Permissions::from_mode(0o755))?;
    Ok(())
}

fn helper_script_contents(cfg: &InstallConfig) -> String {
    let config_path = shell_quote(&cfg.config_path);
    let app_dir = shell_quote(&cfg.www_root);
    const TEMPLATE: &str = r#"#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE=__CONFIG_PATH__

if [[ -f "$CONFIG_FILE" ]]; then
    . "$CONFIG_FILE"
else
    echo "Missing $CONFIG_FILE; cannot continue." >&2
    exit 1
fi

if [[ -z "${NEWTUBE_PORT:-}" ]]; then
    NEWTUBE_PORT="8080"
fi

if [[ -z "${NEWTUBE_HOST:-}" ]]; then
    NEWTUBE_HOST="127.0.0.1"
fi

REPO_URL="https://github.com/Pingasmaster/viewtube.git"
BACKEND_SERVICE="viewtube-backend.service"
ROUTINE_SERVICE="viewtube-routine.service"
SERVICE_GROUP="viewtube"
DOWNLOADER_USER="viewtube-downloader"
NGINX_SERVICE="nginx"

export PATH="$PATH:/root/.cargo/bin:/usr/local/bin"

APP_DIR=__APP_DIR__

echo "[*] Syncing repo..."
if [[ -d "$APP_DIR/.git" ]]; then
    if ! git -C "$APP_DIR" pull; then
        echo "[!] git pull failed; recloning fresh copy..."
        rm -rf "$APP_DIR"
        git clone "$REPO_URL" "$APP_DIR"
    fi
else
    rm -rf "$APP_DIR"
    git clone "$REPO_URL" "$APP_DIR"
fi

cd "$APP_DIR"

cleanup_repo() {
    rm -rf node_modules coverage
    rm -f download_channel backend routine-update
    cargo clean
}

cleanup_repo
CARGO_VERSION=$(grep -m1 '^version' Cargo.toml | sed -E 's/version\s*=\s*"([^"]+)"/\1/')
if [[ "$APP_VERSION" != "$CARGO_VERSION" ]]; then
    cat <<EOF > "$CONFIG_FILE"
MEDIA_ROOT="$MEDIA_ROOT"
WWW_ROOT="$WWW_ROOT"
NEWTUBE_PORT="$NEWTUBE_PORT"
NEWTUBE_HOST="$NEWTUBE_HOST"
APP_VERSION="$CARGO_VERSION"
DOMAIN_NAME="$DOMAIN_NAME"
EOF
    INSTALLER_BIN="$APP_DIR/target/release/installer"
    if [[ -x "$INSTALLER_BIN" ]]; then
        echo "[*] Detected version change ($APP_VERSION -> $CARGO_VERSION); rerunning installer..."
        exec "$INSTALLER_BIN" -y --config "$CONFIG_FILE" --media-dir "$MEDIA_ROOT" --www-dir "$WWW_ROOT" --port "$NEWTUBE_PORT" --host "$NEWTUBE_HOST" --domain "$DOMAIN_NAME"
    else
        echo "Missing $INSTALLER_BIN; cannot re-run installer." >&2
        exit 1
    fi
fi

echo "[*] Building with cargo (release)..."
cargo build --release
cp target/release/backend target/release/download_channel target/release/routine_update "$MEDIA_ROOT" && cargo clean

echo "[*] Resetting media directory permissions..."
chown -R "$DOWNLOADER_USER:$SERVICE_GROUP" "$MEDIA_ROOT"
chmod -R g+rwX "$MEDIA_ROOT"

echo "[*] Restarting backend service..."
systemctl restart "$BACKEND_SERVICE"

echo "[*] Launching routine update unit..."
systemctl start "$ROUTINE_SERVICE"

echo "[*] Restarting nginx..."
systemctl restart "$NGINX_SERVICE"

echo "[*] Done."
"#;

    TEMPLATE
        .replace("__CONFIG_PATH__", &config_path)
        .replace("__APP_DIR__", &app_dir)
}

fn shell_quote(path: &Path) -> String {
    let value = path.to_string_lossy();
    let mut quoted = String::from("'");
    for ch in value.chars() {
        if ch == '\'' {
            quoted.push_str("'\"'\"'");
        } else {
            quoted.push(ch);
        }
    }
    quoted.push('\'');
    quoted
}

fn install_systemd_units(cfg: &InstallConfig) -> Result<()> {
    let systemd_dir = PathBuf::from("/etc/systemd/system");
    fs::create_dir_all(&systemd_dir)?;

    let updater_service = systemd_dir.join(SOFTWARE_SERVICE);
    let timer_path = systemd_dir.join(SOFTWARE_TIMER);
    let backend_service = systemd_dir.join(BACKEND_SERVICE);
    let routine_service = systemd_dir.join(ROUTINE_SERVICE);

    let www_working_dir = escape_systemd_path(&cfg.www_root)?;
    let helper_exec = escape_systemd_path(&cfg.media_root.join(HELPER_SCRIPT_NAME))?;
    let updater_contents = format!(
        "[Unit]\nDescription=Update, rebuild, and restart ViewTube services\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=oneshot\nUser=root\nWorkingDirectory={}\nExecStart={}\nTimeoutStartSec=3600\n\n[Install]\nWantedBy=multi-user.target\n",
        www_working_dir, helper_exec
    );
    fs::write(&updater_service, updater_contents)?;

    let timer_contents = "[Unit]\nDescription=Run software-updater.service daily\n\n[Timer]\nOnCalendar=*-*-* 03:00\nPersistent=true\nUnit=software-updater.service\n\n[Install]\nWantedBy=timers.target\n";
    fs::write(&timer_path, timer_contents)?;

    let media_work_dir = escape_systemd_path(&cfg.media_root)?;
    let backend_exec = escape_systemd_path(&cfg.media_root.join("backend"))?;
    let config_path = escape_systemd_path(&cfg.config_path)?;
    let backend_contents = format!(
        "[Unit]\nDescription=ViewTube backend API\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nUser={user}\nGroup={group}\nWorkingDirectory={work}\nExecStart={exec} --config {config}\nRestart=on-failure\nRestartSec=2\nAmbientCapabilities=\nCapabilityBoundingSet=\nNoNewPrivileges=yes\nProtectSystem=full\nProtectHome=read-only\nPrivateTmp=yes\nRestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX\nRestrictSUIDSGID=yes\nRestrictRealtime=yes\nLockPersonality=yes\nUMask=0027\nReadWritePaths={work}\n\n[Install]\nWantedBy=multi-user.target\n",
        user = BACKEND_USER,
        group = VIEWTUBE_GROUP,
        work = media_work_dir,
        exec = backend_exec,
        config = config_path
    );
    fs::write(&backend_service, backend_contents)?;

    let routine_exec = escape_systemd_path(&cfg.media_root.join("routine_update"))?;
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

fn run_helper_script(path: &Path) -> Result<()> {
    log_info("Running helper script (may take a while)");
    let status = Command::new(path)
        .status()
        .with_context(|| format!("Failed to run {}", path.display()))?;
    if !status.success() {
        bail!("Helper script exited with status {status}");
    }
    Ok(())
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
    fn shell_quote_handles_quotes() {
        assert_eq!(shell_quote(Path::new("/tmp/foo bar")), "'/tmp/foo bar'");
        assert_eq!(shell_quote(Path::new("/tmp/it")), "'/tmp/it'");
        assert_eq!(shell_quote(Path::new("/tmp/it'")), "'/tmp/it'\"'\"''");
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

    #[test]
    fn helper_script_contains_cleanup_logic() {
        let cfg = InstallConfig {
            media_root: PathBuf::from("/yt"),
            www_root: PathBuf::from("/www"),
            newtube_port: 8080,
            newtube_host: "127.0.0.1".into(),
            config_path: PathBuf::from("/etc/viewtube-env"),
            domain_name: "example.com".into(),
            app_version: "1.0.0".into(),
            assume_yes: false,
        };
        let contents = helper_script_contents(&cfg);
        assert!(contents.contains("cleanup_repo"));
        assert!(contents.contains("NEWTUBE_HOST"));
        assert!(contents.contains("--host \"$NEWTUBE_HOST\""));
        assert!(contents.contains("systemctl restart \"$BACKEND_SERVICE\""));
    }
}
