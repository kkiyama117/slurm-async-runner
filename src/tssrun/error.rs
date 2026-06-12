//! Structured errors for the tssrun backend, mirroring
//! [`crate::sbatch::error`] so both backends expose the same failure-mode
//! granularity (issue #16 item 1).
//!
//! Display strings deliberately reproduce the pre-structured `anyhow!`
//! messages ("no persisted handle matched …", "not owner of the child /
//! already waited", …) so Python callers matching on `RuntimeError` text
//! and existing log greps keep working.

use uuid::Uuid;

/// Errors that can occur while spawning the tssrun child.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum TssrunSpawnError {
    /// The [`crate::tssrun::cmd::TssrunCmd`] spec could not be turned into
    /// an argv (validation failure — no process was started).
    #[error("failed to build tssrun argv: {0}")]
    ArgvBuild(#[source] anyhow::Error),

    /// The dispatcher failed to start the child process (binary missing,
    /// permission denied, …) — no process was started.
    #[error("failed to spawn tssrun child: {source}")]
    SpawnFailed {
        #[source]
        source: anyhow::Error,
    },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Errors that can occur while attaching to a previously persisted handle.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum TssrunAttachError {
    /// `key` is the human-readable lookup description, e.g. `"uuid <v7>"`,
    /// `"pid 4242"`, `"jobid 999"`.
    #[error("no persisted handle matched {key}")]
    NotFound { key: String },

    #[error("io error during attach: {0}")]
    Io(#[from] anyhow::Error),
}

/// Errors that can occur in the owner-only [`wait`].
///
/// [`wait`]: crate::tssrun::handle::TssrunJobHandle::wait
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum TssrunWaitError {
    /// The handle was attached (never owned the child) or `wait()` was
    /// already consumed once.
    #[error("not owner of the child / already waited")]
    NotOwner,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Errors that can occur in [`refresh`] / [`wait_terminal`].
///
/// [`refresh`]: crate::tssrun::handle::TssrunJobHandle::refresh
/// [`wait_terminal`]: crate::tssrun::handle::TssrunJobHandle::wait_terminal
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum TssrunRefreshError {
    /// The handle was constructed without a [`crate::store::JobStateStore`],
    /// so there is nothing to re-read the snapshot from.
    #[error("no store on this handle")]
    NoStore,

    #[error("uuid {uuid} not found in store")]
    NotFound { uuid: Uuid },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_argv_build_carries_source_message() {
        let e = TssrunSpawnError::ArgvBuild(anyhow::anyhow!("program path is empty"));
        let msg = e.to_string();
        assert!(msg.contains("failed to build tssrun argv"), "got: {msg}");
        assert!(msg.contains("program path is empty"), "got: {msg}");
    }

    #[test]
    fn spawn_failed_carries_dispatcher_message() {
        let e = TssrunSpawnError::SpawnFailed {
            source: anyhow::anyhow!("No such file or directory (os error 2)"),
        };
        assert!(e.to_string().contains("No such file or directory"));
    }

    #[test]
    fn attach_not_found_reproduces_legacy_message_for_each_key_kind() {
        for key in ["uuid 0198-abc", "pid 4242", "jobid 999"] {
            let e = TssrunAttachError::NotFound {
                key: key.to_string(),
            };
            let msg = e.to_string();
            assert_eq!(msg, format!("no persisted handle matched {key}"));
        }
    }

    #[test]
    fn wait_not_owner_reproduces_legacy_message() {
        assert_eq!(
            TssrunWaitError::NotOwner.to_string(),
            "not owner of the child / already waited"
        );
    }

    #[test]
    fn refresh_no_store_reproduces_legacy_message() {
        assert_eq!(
            TssrunRefreshError::NoStore.to_string(),
            "no store on this handle"
        );
    }

    #[test]
    fn refresh_not_found_carries_uuid() {
        let uuid = Uuid::now_v7();
        let e = TssrunRefreshError::NotFound { uuid };
        let msg = e.to_string();
        assert!(msg.contains(&uuid.to_string()), "got: {msg}");
        assert!(msg.contains("not found in store"), "got: {msg}");
    }

    #[test]
    fn refresh_other_converts_to_anyhow_and_back_for_trait_paths() {
        // The JobHandleCommon trait keeps anyhow::Result, so the typed
        // error must convert losslessly into anyhow::Error via `?`.
        fn as_anyhow(e: TssrunRefreshError) -> anyhow::Error {
            e.into()
        }
        let msg = as_anyhow(TssrunRefreshError::NoStore).to_string();
        assert_eq!(msg, "no store on this handle");
    }
}
