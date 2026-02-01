#![forbid(unsafe_code)]

//! Shared security helpers used by the newtube binaries.

use anyhow::{Result, bail};
use nix::unistd::Uid;

/// Fails fast when a binary is started as root. Running as a regular
/// unprivileged user keeps local installs predictable and avoids accidental
/// writes into system directories.
pub fn ensure_not_root(process: &str) -> Result<()> {
    ensure_not_root_for(Uid::current(), process)
}

fn ensure_not_root_for(uid: Uid, process: &str) -> Result<()> {
    if uid.is_root() {
        bail!(
            "{process} must not be run as root; use a regular user or a dedicated service account"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::unistd::Uid;

    #[test]
    fn ensure_not_root_allows_unprivileged_uid() {
        let uid = Uid::from_raw(1000);
        assert!(ensure_not_root_for(uid, "tester").is_ok());
    }

    #[test]
    fn ensure_not_root_rejects_root_uid() {
        let uid = Uid::from_raw(0);
        let err = ensure_not_root_for(uid, "tester").unwrap_err();
        assert!(err.to_string().contains("must not be run as root"));
    }
}
