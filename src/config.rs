use anyhow::{Context, Result, anyhow};
use std::{
    collections::HashMap,
    env,
    fs,
    path::{Path, PathBuf},
};

pub const DEFAULT_ENV_PATH: &str = ".env";
pub const DEFAULT_NEWTUBE_PORT: u16 = 8080;
pub const DEFAULT_NEWTUBE_HOST: &str = "127.0.0.1";

#[derive(Debug, Clone)]
pub struct RuntimePaths {
    pub media_root: PathBuf,
    pub www_root: PathBuf,
    pub newtube_port: u16,
    pub newtube_host: String,
}

pub fn load_runtime_paths() -> Result<RuntimePaths> {
    resolve_runtime_paths(RuntimeOverrides::default())
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeOverrides {
    pub media_root: Option<PathBuf>,
    pub www_root: Option<PathBuf>,
    pub newtube_port: Option<u16>,
    pub newtube_host: Option<String>,
    pub env_path: Option<PathBuf>,
}

pub fn resolve_runtime_paths(overrides: RuntimeOverrides) -> Result<RuntimePaths> {
    let env_path = overrides
        .env_path
        .as_deref()
        .unwrap_or_else(|| Path::new(DEFAULT_ENV_PATH));
    let file_vars = read_env_file(env_path)?;
    build_runtime_paths_with_overrides(&file_vars, env_var_string, overrides)
}

#[cfg(test)]
fn build_runtime_paths(
    file_vars: &HashMap<String, String>,
    env_lookup: impl Fn(&str) -> Option<String>,
) -> Result<RuntimePaths> {
    build_runtime_paths_with_overrides(
        file_vars,
        env_lookup,
        RuntimeOverrides::default(),
    )
}

fn build_runtime_paths_with_overrides(
    file_vars: &HashMap<String, String>,
    env_lookup: impl Fn(&str) -> Option<String>,
    overrides: RuntimeOverrides,
) -> Result<RuntimePaths> {
    let media_root = overrides
        .media_root
        .map(|path| path.to_string_lossy().into_owned())
        .or_else(|| lookup_value("MEDIA_ROOT", file_vars, &env_lookup))
        .ok_or_else(|| anyhow!("MEDIA_ROOT not set"))?;
    let www_root = overrides
        .www_root
        .map(|path| path.to_string_lossy().into_owned())
        .or_else(|| lookup_value("WWW_ROOT", file_vars, &env_lookup))
        .ok_or_else(|| anyhow!("WWW_ROOT not set"))?;
    let newtube_port = overrides
        .newtube_port
        .or_else(|| {
            lookup_value("NEWTUBE_PORT", file_vars, &env_lookup)
                .and_then(|value| value.parse::<u16>().ok())
        })
        .unwrap_or(DEFAULT_NEWTUBE_PORT);
    let newtube_host = overrides
        .newtube_host
        .and_then(|value| {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        })
        .or_else(|| lookup_value("NEWTUBE_HOST", file_vars, &env_lookup))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_NEWTUBE_HOST.to_string());
    Ok(RuntimePaths {
        media_root: PathBuf::from(media_root),
        www_root: PathBuf::from(www_root),
        newtube_port,
        newtube_host,
    })
}

fn env_var_string(key: &str) -> Option<String> {
    env::var(key).ok().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn lookup_value(
    key: &str,
    file_vars: &HashMap<String, String>,
    env_lookup: &impl Fn(&str) -> Option<String>,
) -> Option<String> {
    env_lookup(key).or_else(|| file_vars.get(key).cloned())
}

fn read_env_file(path: &Path) -> Result<HashMap<String, String>> {
    let mut vars = HashMap::new();
    if !path.exists() {
        return Ok(vars);
    }
    let content = fs::read_to_string(path).with_context(|| format!("Reading {}", path.display()))?;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let line = trimmed.strip_prefix("export ").unwrap_or(trimmed);
        let Some((key, value_raw)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        let value = value_raw.trim();
        let value = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .or_else(|| value.strip_prefix('\'').and_then(|value| value.strip_suffix('\'')))
            .unwrap_or(value);
        vars.insert(key.to_string(), value.to_string());
    }
    Ok(vars)
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

    fn runtime_from(contents: &str) -> RuntimePaths {
        let cfg = make_config(contents);
        let vars = read_env_file(cfg.path()).unwrap();
        build_runtime_paths(&vars, |_| None).unwrap()
    }

    #[test]
    fn load_runtime_paths_reads_port() {
        let runtime = runtime_from("MEDIA_ROOT=\"/yt\"\nWWW_ROOT=\"/www\"\nNEWTUBE_PORT=\"4242\"\n");
        assert_eq!(runtime.newtube_port, 4242);
    }

    #[test]
    fn load_runtime_paths_defaults_missing_port() {
        let runtime = runtime_from("MEDIA_ROOT=\"/m\"\nWWW_ROOT=\"/w\"\n");
        assert_eq!(runtime.newtube_port, DEFAULT_NEWTUBE_PORT);
        assert_eq!(runtime.media_root, PathBuf::from("/m"));
        assert_eq!(runtime.www_root, PathBuf::from("/w"));
        assert_eq!(runtime.newtube_host, DEFAULT_NEWTUBE_HOST);
    }

    #[test]
    fn load_runtime_paths_reads_host() {
        let runtime =
            runtime_from("MEDIA_ROOT=\"/m\"\nWWW_ROOT=\"/w\"\nNEWTUBE_HOST=\"0.0.0.0\"\n");
        assert_eq!(runtime.newtube_host, "0.0.0.0");
    }

    #[test]
    fn read_env_file_parses_values() {
        let cfg = make_config("MEDIA_ROOT=\"/x\"\nWWW_ROOT=\"/y\"\nNEWTUBE_PORT=\"9090\"\n");
        let vars = read_env_file(cfg.path()).unwrap();
        let runtime = build_runtime_paths(&vars, |_| None).unwrap();
        assert_eq!(runtime.media_root, PathBuf::from("/x"));
        assert_eq!(runtime.www_root, PathBuf::from("/y"));
        assert_eq!(runtime.newtube_port, 9090);
    }

    #[test]
    fn build_runtime_paths_prefers_env_over_file() {
        let vars = read_env_file(make_config("MEDIA_ROOT=\"/file\"\nWWW_ROOT=\"/www\"\n").path())
            .unwrap();
        let runtime = build_runtime_paths(&vars, |key| {
            if key == "MEDIA_ROOT" {
                Some("/env".to_string())
            } else {
                None
            }
        })
        .unwrap();
        assert_eq!(runtime.media_root, PathBuf::from("/env"));
    }
}
