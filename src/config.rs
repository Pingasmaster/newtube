use anyhow::{Context, Result, anyhow};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub const DEFAULT_CONFIG_PATH: &str = "/etc/viewtube-env";
pub const DEFAULT_NEWTUBE_PORT: u16 = 8080;
pub const DEFAULT_NEWTUBE_HOST: &str = "127.0.0.1";

#[derive(Debug, Clone, Default)]
pub struct EnvConfig {
    pub media_root: Option<PathBuf>,
    pub www_root: Option<PathBuf>,
    pub app_version: Option<String>,
    pub domain_name: Option<String>,
    pub newtube_port: Option<u16>,
    pub newtube_host: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RuntimePaths {
    pub media_root: PathBuf,
    pub www_root: PathBuf,
    pub newtube_port: u16,
    pub newtube_host: String,
}

pub fn read_env_config(path: &Path) -> Result<Option<EnvConfig>> {
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
                "NEWTUBE_PORT" => {
                    let port: u16 = value
                        .parse()
                        .with_context(|| format!("Parsing NEWTUBE_PORT from {}", path.display()))?;
                    cfg.newtube_port = Some(port);
                }
                "NEWTUBE_HOST" => {
                    if !value.is_empty() {
                        cfg.newtube_host = Some(value.to_string());
                    }
                }
                _ => {}
            }
        }
    }
    Ok(Some(cfg))
}

pub fn load_runtime_paths() -> Result<RuntimePaths> {
    load_runtime_paths_from(Path::new(DEFAULT_CONFIG_PATH))
}

pub fn load_runtime_paths_from(path: impl AsRef<Path>) -> Result<RuntimePaths> {
    let path = path.as_ref();
    let cfg = read_env_config(path)?
        .ok_or_else(|| anyhow!("Missing config file at {}", path.display()))?;
    let media_root = cfg
        .media_root
        .ok_or_else(|| anyhow!("MEDIA_ROOT not set in {}", path.display()))?;
    let www_root = cfg
        .www_root
        .ok_or_else(|| anyhow!("WWW_ROOT not set in {}", path.display()))?;
    let newtube_port = cfg.newtube_port.unwrap_or(DEFAULT_NEWTUBE_PORT);
    let newtube_host = cfg
        .newtube_host
        .clone()
        .unwrap_or_else(|| DEFAULT_NEWTUBE_HOST.to_string());
    Ok(RuntimePaths {
        media_root,
        www_root,
        newtube_port,
        newtube_host,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn make_config(contents: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "{}", contents).unwrap();
        file
    }

    #[test]
    fn read_env_config_extracts_port() {
        let cfg = make_config("MEDIA_ROOT=\"/yt\"\nWWW_ROOT=\"/www\"\nNEWTUBE_PORT=\"4242\"\n");
        let parsed = read_env_config(cfg.path()).unwrap().unwrap();
        assert_eq!(parsed.newtube_port, Some(4242));
    }

    #[test]
    fn load_runtime_paths_defaults_missing_port() {
        let cfg = make_config("MEDIA_ROOT=\"/m\"\nWWW_ROOT=\"/w\"\n");
        let runtime = load_runtime_paths_from(cfg.path()).unwrap();
        assert_eq!(runtime.newtube_port, DEFAULT_NEWTUBE_PORT);
        assert_eq!(runtime.media_root, PathBuf::from("/m"));
        assert_eq!(runtime.www_root, PathBuf::from("/w"));
        assert_eq!(runtime.newtube_host, DEFAULT_NEWTUBE_HOST);
    }

    #[test]
    fn load_runtime_paths_reads_host() {
        let cfg = make_config("MEDIA_ROOT=\"/m\"\nWWW_ROOT=\"/w\"\nNEWTUBE_HOST=\"0.0.0.0\"\n");
        let runtime = load_runtime_paths_from(cfg.path()).unwrap();
        assert_eq!(runtime.newtube_host, "0.0.0.0");
    }
}
