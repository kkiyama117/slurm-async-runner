//! Path utilities shared across submission backends (`tssrun::cmd`,
//! `sbatch::cmd`, `manager`). Phase 2 P1 consolidates three duplicate
//! implementations of `absolutize` into this single source.

use anyhow::{Context, Result};
use std::path::Path;

/// Convert a possibly-relative path to its absolute UTF-8 string form.
///
/// Returns an error if `std::path::absolute` fails (e.g. CWD unreadable)
/// or if the resulting path is not valid UTF-8.
// Phase 2 P1 Task 1 introduces this helper; Task 2 migrates the three
// existing call sites to it. The `allow(dead_code)` is removed in Task 2.
#[allow(dead_code)]
pub(crate) fn absolutize(p: &Path) -> Result<String> {
    let abs =
        std::path::absolute(p).with_context(|| format!("failed to absolutize {}", p.display()))?;
    abs.into_os_string()
        .into_string()
        .map_err(|os| anyhow::anyhow!("non-UTF8 path: {os:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn absolute_path_roundtrips() {
        let abs = absolutize(Path::new("/tmp/foo")).unwrap();
        assert_eq!(abs, "/tmp/foo");
    }

    #[test]
    fn relative_path_is_made_absolute() {
        let abs = absolutize(Path::new("foo.sh")).unwrap();
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(abs, format!("{}/foo.sh", cwd.display()));
    }

    #[test]
    fn handles_dot_segments() {
        let abs = absolutize(Path::new("./bar")).unwrap();
        let cwd = std::env::current_dir().unwrap();
        assert!(
            abs.starts_with(&cwd.display().to_string()),
            "abs={abs} should start with {cwd:?}"
        );
        let _ = PathBuf::from(&abs);
    }
}
