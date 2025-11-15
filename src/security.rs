#![forbid(unsafe_code)]

//! Shared security helpers used by the ViewTube binaries.

use anyhow::{Result, bail};
use nix::unistd::Uid;

/// Fails fast when a binary is started as root. All services are expected to
/// run under the dedicated, unprivileged accounts provisioned by the
/// installer. Guarding binaries themselves ensures that manual invocations do
/// not silently revert to insecure defaults.
pub fn ensure_not_root(process: &str) -> Result<()> {
    if Uid::current().is_root() {
        bail!("{process} must not be run as root; please use the viewtube-* service accounts");
    }
    Ok(())
}
