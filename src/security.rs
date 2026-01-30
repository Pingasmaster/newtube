#![forbid(unsafe_code)]

//! Shared security helpers used by the newtube binaries.

use anyhow::{Result, bail};
use nix::unistd::Uid;

/// Fails fast when a binary is started as root. Running as a regular
/// unprivileged user keeps local installs predictable and avoids accidental
/// writes into system directories.
pub fn ensure_not_root(process: &str) -> Result<()> {
    if Uid::current().is_root() {
        bail!("{process} must not be run as root; use a regular user or a dedicated service account");
    }
    Ok(())
}
