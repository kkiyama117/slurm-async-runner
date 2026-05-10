//! Pure parsers / formatters for the sbatch module — no I/O.

use std::path::PathBuf;

/// Parse the jobid from an `sbatch` submission's stdout.
///
/// Typical output: `Submitted batch job 12345`. The line may be embedded
/// among other lines (warnings, multi-cluster output `Submitted batch job
/// 12345 on cluster X`, array form `Submitted batch job 12345_0`).
/// First match wins; trailing non-digit chars are stripped, so for array
/// forms the parent jobid is returned (Phase 1 simplification).
/// Returns `None` if no line matches.
pub fn parse_submitted_jobid(stdout: &str) -> Option<u64> {
    for line in stdout.lines() {
        let line = line.trim();
        let prefix = "Submitted batch job ";
        if let Some(rest) = line.strip_prefix(prefix) {
            let id_str: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            if !id_str.is_empty()
                && let Ok(id) = id_str.parse::<u64>()
            {
                return Some(id);
            }
        }
    }
    None
}

/// Lenient SLURM `-o`/`-e` template substitution.
///
/// Phase 1 expands `%j` (jobid) and, when `job_name` is `Some`, `%x`.
/// Other tokens (`%A`, `%a`, `%u`, `%N`) are preserved verbatim — caller
/// can detect "still has unresolved variables" by checking for `%` in the
/// returned path.
pub fn resolve_log_path(template: &str, jobid: u64, job_name: Option<&str>) -> PathBuf {
    let mut s = template.to_string();
    s = s.replace("%j", &jobid.to_string());
    if let Some(name) = job_name {
        s = s.replace("%x", name);
    }
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- parse_submitted_jobid ----

    #[test]
    fn parses_clean_single_line() {
        assert_eq!(
            parse_submitted_jobid("Submitted batch job 12345\n"),
            Some(12345)
        );
    }

    #[test]
    fn parses_with_leading_warning() {
        let out = "\
sbatch: warning: ...
Submitted batch job 67890
";
        assert_eq!(parse_submitted_jobid(out), Some(67890));
    }

    #[test]
    fn parses_multi_cluster_form() {
        let out = "Submitted batch job 42 on cluster cluster1\n";
        assert_eq!(parse_submitted_jobid(out), Some(42));
    }

    #[test]
    fn parses_array_form_takes_parent_id() {
        let out = "Submitted batch job 12345_0\n";
        assert_eq!(parse_submitted_jobid(out), Some(12345));
    }

    #[test]
    fn returns_none_when_no_match() {
        assert_eq!(parse_submitted_jobid(""), None);
        assert_eq!(parse_submitted_jobid("error: bad partition\n"), None);
    }

    // ---- resolve_log_path ----

    #[test]
    fn resolve_substitutes_jobid_only() {
        let p = resolve_log_path("slurm-%j.out", 12345, None);
        assert_eq!(p, PathBuf::from("slurm-12345.out"));
    }

    #[test]
    fn resolve_substitutes_jobname_when_some() {
        let p = resolve_log_path("%x-%j.out", 12345, Some("g09run"));
        assert_eq!(p, PathBuf::from("g09run-12345.out"));
    }

    #[test]
    fn resolve_leaves_jobname_token_when_none() {
        let p = resolve_log_path("%x-%j.out", 12345, None);
        assert_eq!(p, PathBuf::from("%x-12345.out"));
    }

    #[test]
    fn resolve_leaves_unsupported_tokens_raw() {
        let p = resolve_log_path("%A_%a-%u-%N-%j.out", 999, Some("nm"));
        assert_eq!(p, PathBuf::from("%A_%a-%u-%N-999.out"));
    }
}
