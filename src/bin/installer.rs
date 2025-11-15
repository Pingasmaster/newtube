#![forbid(unsafe_code)]

use anyhow::{Context, Result, anyhow, bail};
use clap::{ArgGroup, Parser};
use std::{
    env, fs,
    io::{self, Write},
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::Command,
};

const DEFAULT_MEDIA_DIR: &str = "/yt";
const DEFAULT_WWW_DIR: &str = "/www/newtube.com";
const DEFAULT_CONFIG_PATH: &str = "/etc/viewtube-env";
const HELPER_SCRIPT_NAME: &str = "viewtube-update-build-run.sh";
const SOFTWARE_SERVICE: &str = "software-updater.service";
const SOFTWARE_TIMER: &str = "software-updater.timer";
const NGINX_SERVICE: &str = "nginx";

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
    let media_root = cli
        .media_dir
        .clone()
        .or_else(|| existing_env.as_ref().and_then(|cfg| cfg.media_root.clone()))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_MEDIA_DIR));
    let www_root = cli
        .www_dir
        .clone()
        .or_else(|| existing_env.as_ref().and_then(|cfg| cfg.www_root.clone()))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_WWW_DIR));
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
    config_path: PathBuf,
    domain_name: String,
    app_version: String,
    assume_yes: bool,
}

#[derive(Debug, Clone, Default)]
struct EnvConfig {
    media_root: Option<PathBuf>,
    www_root: Option<PathBuf>,
    app_version: Option<String>,
    domain_name: Option<String>,
}

fn install(cfg: InstallConfig) -> Result<()> {
    log_info("Starting installation");
    fs::create_dir_all(&cfg.media_root)
        .with_context(|| format!("Creating media dir {}", cfg.media_root.display()))?;
    fs::create_dir_all(&cfg.www_root)
        .with_context(|| format!("Creating www dir {}", cfg.www_root.display()))?;

    ensure_nginx_installed(cfg.assume_yes)?;
    deploy_nginx_config(&cfg.domain_name, &cfg.www_root, cfg.assume_yes)?;

    write_env_config(&cfg)?;
    write_helper_script(&cfg)?;
    install_systemd_units(&cfg)?;

    run_command("systemctl", &["daemon-reload"])?;
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

    let systemd_dir = PathBuf::from("/etc/systemd/system");
    remove_path_if_exists(&systemd_dir.join(SOFTWARE_SERVICE))?;
    remove_path_if_exists(&systemd_dir.join(SOFTWARE_TIMER))?;

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

fn read_env_config(path: &Path) -> Result<Option<EnvConfig>> {
    if !path.exists() {
        return Ok(None);
    }
    let content =
        fs::read_to_string(path).with_context(|| format!("Reading {}", path.display()))?;
    let mut cfg = EnvConfig::default();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some((key, value_raw)) = trimmed.split_once('=') {
            let value = value_raw.trim().trim_matches('"');
            match key {
                "MEDIA_ROOT" => cfg.media_root = Some(PathBuf::from(value)),
                "WWW_ROOT" => cfg.www_root = Some(PathBuf::from(value)),
                "APP_VERSION" => cfg.app_version = Some(value.to_string()),
                "DOMAIN_NAME" => cfg.domain_name = Some(value.to_string()),
                _ => {}
            }
        }
    }
    Ok(Some(cfg))
}

fn write_env_config(cfg: &InstallConfig) -> Result<()> {
    let content = format!(
        "MEDIA_ROOT=\"{}\"\nWWW_ROOT=\"{}\"\nAPP_VERSION=\"{}\"\nDOMAIN_NAME=\"{}\"\n",
        cfg.media_root.display(),
        cfg.www_root.display(),
        cfg.app_version,
        cfg.domain_name
    );
    fs::write(&cfg.config_path, content)
        .with_context(|| format!("Writing {}", cfg.config_path.display()))?;
    fs::set_permissions(&cfg.config_path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

fn resolve_domain(
    cli_domain: Option<String>,
    existing: Option<&EnvConfig>,
    assume_yes: bool,
) -> Result<String> {
    if let Some(domain) = cli_domain {
        return normalize_domain(&domain);
    }
    if let Some(existing_domain) = existing.and_then(|cfg| cfg.domain_name.clone()) {
        if assume_yes || prompt_yes_no(&format!("Use detected domain '{existing_domain}'?"), false)?
        {
            return Ok(existing_domain);
        }
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
    format!(
        "#!/usr/bin/env bash\nset -euo pipefail\n\nCONFIG_FILE={config_path}\n\nif [[ -f \"$CONFIG_FILE\" ]]; then\n    . \"$CONFIG_FILE\"\nelse\n    echo \"Missing $CONFIG_FILE; cannot continue.\" >&2\n    exit 1\nfi\n\nREPO_URL=\"https://github.com/Pingasmaster/viewtube.git\"\nSCREEN_NAME_ROUTINEUPDATE=\"routineupdate\"\nSCREEN_NAME_BACKEND=\"backend\"\nNGINX_SERVICE=\"nginx\"\n\nexport PATH=\"$PATH:/root/.cargo/bin:/usr/local/bin\"\n\nAPP_DIR={app_dir}\n\necho \"[*] Syncing repo...\"\nif [[ -d \"$APP_DIR/.git\" ]]; then\n    if ! git -C \"$APP_DIR\" pull; then\n        echo \"[!] git pull failed; recloning fresh copy...\"\n        rm -rf \"$APP_DIR\"\n        git clone \"$REPO_URL\" \"$APP_DIR\"\n    fi\nelse\n    rm -rf \"$APP_DIR\"\n    git clone \"$REPO_URL\" \"$APP_DIR\"\nfi\n\ncd \"$APP_DIR\"\n\ncleanup_repo() {{\n    rm -rf node_modules coverage\n    rm -f download_channel backend routine-update\n    cargo clean\n}}\n\ncleanup_repo\nCARGO_VERSION=$(grep -m1 '^version' Cargo.toml | sed -E 's/version\\s*=\\s*\"([^\"]+)\"/\\1/')\nif [[ \"$APP_VERSION\" != \"$CARGO_VERSION\" ]]; then\n    cat <<EOF > \"$CONFIG_FILE\"\nMEDIA_ROOT=\"$MEDIA_ROOT\"\nWWW_ROOT=\"$WWW_ROOT\"\nAPP_VERSION=\"$CARGO_VERSION\"\nDOMAIN_NAME=\"$DOMAIN_NAME\"\nEOF\n    INSTALLER_BIN=\"$APP_DIR/target/release/installer\"\n    if [[ -x \"$INSTALLER_BIN\" ]]; then\n        echo \"[*] Detected version change ($APP_VERSION -> $CARGO_VERSION); rerunning installer...\"\n        exec \"$INSTALLER_BIN\" -y --media-dir \"$MEDIA_ROOT\" --www-dir \"$WWW_ROOT\" --domain \"$DOMAIN_NAME\"\n    else\n        echo \"Missing $INSTALLER_BIN; cannot re-run installer.\" >&2\n        exit 1\n    fi\nfi\n\necho \"[*] Building with cargo (release)...\"\ncargo build --release\ncp target/release/backend target/release/download_channel target/release/routine_update \"$MEDIA_ROOT\" && cargo clean\n\necho \"[*] Stopping existing screen session for backend (if any)...\"\nif screen -list | grep -q \"\\.$SCREEN_NAME_BACKEND\"; then\n    screen -S \"$SCREEN_NAME_BACKEND\" -X quit || true\nfi\n\necho \"[*] Stopping existing screen session for routine update (if any)...\"\nif screen -list | grep -q \"\\.$SCREEN_NAME_ROUTINEUPDATE\"; then\n    screen -S \"$SCREEN_NAME_ROUTINEUPDATE\" -X quit || true\nfi\n\necho \"[*] Starting new screen sessions...\"\nscreen -dmS \"$SCREEN_NAME_BACKEND\" \"$MEDIA_ROOT/backend\" --media-root \"$MEDIA_ROOT\"\nscreen -dmS \"$SCREEN_NAME_ROUTINEUPDATE\" \"$MEDIA_ROOT/routine_update\" --media-root \"$MEDIA_ROOT\" --www-root \"$WWW_ROOT\"\n\necho \"[*] Restarting nginx...\"\nsystemctl restart \"$NGINX_SERVICE\"\n\necho \"[*] Done.\"\n",
        config_path = config_path,
        app_dir = app_dir
    )
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
    let service_path = PathBuf::from("/etc/systemd/system").join(SOFTWARE_SERVICE);
    let timer_path = PathBuf::from("/etc/systemd/system").join(SOFTWARE_TIMER);
    if let Some(parent) = service_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let working_dir = escape_systemd_path(&cfg.www_root)?;
    let helper_exec = escape_systemd_path(&cfg.media_root.join(HELPER_SCRIPT_NAME))?;
    let service_contents = format!(
        "[Unit]\nDescription=Update, build (cargo), run software in screen, then restart nginx\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=oneshot\nUser=root\nWorkingDirectory={}\nExecStart={}\nTimeoutStartSec=3600\n\n[Install]\nWantedBy=multi-user.target\n",
        working_dir, helper_exec
    );
    fs::write(&service_path, service_contents)?;
    if let Some(parent) = timer_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let timer_contents = "[Unit]\nDescription=Run software-updater.service daily\n\n[Timer]\nOnCalendar=*-*-* 03:00\nPersistent=true\nUnit=software-updater.service\n\n[Install]\nWantedBy=timers.target\n";
    fs::write(&timer_path, timer_contents)?;
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
    let _ = run_command_allow_fail("systemctl", &["status", SOFTWARE_TIMER]);
    let _ = run_command_allow_fail("systemctl", &["list-timers"]);
    Ok(())
}

fn run_command(cmd: &str, args: &[&str]) -> Result<()> {
    let printable = format_command(cmd, args);
    log_info(&format!("Running: {printable}"));
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
            config_path: PathBuf::from("/etc/viewtube-env"),
            domain_name: "example.com".into(),
            app_version: "1.0.0".into(),
            assume_yes: false,
        };
        let contents = helper_script_contents(&cfg);
        assert!(contents.contains("cleanup_repo"));
        assert!(contents.contains("exec \"$INSTALLER_BIN\" -y"));
    }
}
