//! Path utilities shared across submission backends (`tssrun::cmd`,
//! `sbatch::cmd`, `manager`). Phase 2 P1 consolidates three duplicate
//! implementations of `absolutize` into this single source.

use anyhow::{Context, Result};
use std::path::Path;

/// Convert a possibly-relative path to its absolute UTF-8 string form.
///
/// Returns an error if `std::path::absolute` fails (e.g. CWD unreadable)
/// or if the resulting path is not valid UTF-8.
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

    #[test]
    #[cfg(unix)]
    fn absolute_path_roundtrips() {
        let abs = absolutize(Path::new("/tmp/foo")).unwrap();
        assert_eq!(abs, "/tmp/foo");
    }

    #[test]
    fn relative_path_is_made_absolute() {
        let abs = absolutize(Path::new("foo.sh")).unwrap();
        let cwd = std::env::current_dir().unwrap();
        let expected = cwd.join("foo.sh").to_str().unwrap().to_owned();
        assert_eq!(abs, expected);
    }

    #[test]
    fn handles_dot_segments() {
        let abs = absolutize(Path::new("./bar")).unwrap();
        let cwd = std::env::current_dir().unwrap();
        let expected = cwd.join("bar").to_str().unwrap().to_owned();
        assert_eq!(abs, expected, "dot segment should be normalized away");
    }
}
