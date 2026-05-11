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
/// Substitutes the following tokens:
/// - `%j` and `%A` — the jobid (`%A` is SLURM's "master jobid" alias on
///   array submissions; for single jobs the two are identical).
/// - `%x` — `job_name` if `Some`, else preserved raw.
/// - `%a` — `array_task_id` if `Some`, else preserved raw.
/// - `%u` — `USER` env var (empty string if unset).
/// - `%N` — `HOSTNAME` env var (empty string if unset). For pending
///   array tasks SLURM normally fills in the compute node name; the
///   spawn-time `HOSTNAME` is a best-effort placeholder for the login
///   node. We do NOT update `%N` retroactively.
///
/// Tokens NOT in the list above (e.g. `%5j` width modifiers, `%t` task id)
/// are preserved verbatim — caller can detect "still has unresolved
/// variables" by checking for `%` in the returned path.
pub fn resolve_log_path(
    template: &str,
    jobid: u64,
    array_task_id: Option<u32>,
    job_name: Option<&str>,
) -> PathBuf {
    let mut s = template.to_string();
    // Substitute %A first (master jobid alias) so it does not collide with %a.
    let jobid_str = jobid.to_string();
    s = s.replace("%A", &jobid_str);
    s = s.replace("%j", &jobid_str);
    if let Some(idx) = array_task_id {
        s = s.replace("%a", &idx.to_string());
    }
    if let Some(name) = job_name {
        s = s.replace("%x", name);
    }
    let user = std::env::var("USER").unwrap_or_default();
    s = s.replace("%u", &user);
    let hostname = std::env::var("HOSTNAME").unwrap_or_default();
    s = s.replace("%N", &hostname);
    PathBuf::from(s)
}

/// Parse sacct's `ExitCode` column ("<exit>:<signal>") into an i32 exit code.
///
/// Slurm の sacct は次のような形を返す:
/// - `"0:0"` — 正常終了
/// - `"139:0"` — exit code 139（プロセスが直接 exit 139 を返した）
/// - `"0:9"` — シグナル SIGKILL で終了。shell convention で 128+9=137 が exit
/// - `"139:11"` — シグナル SIGSEGV、shell convention で 128+11=139
///
/// シグナル成分 (`:<signal>`) が **非ゼロ** のときは shell convention に従い
/// `128 + signal` を返す。両成分がゼロまたは exit のみ非ゼロなら exit を返す。
/// 形式不正は `None`。
pub(crate) fn parse_sacct_exit_code(field: &str) -> Option<i32> {
    let (exit_s, signal_s) = field.split_once(':')?;
    let exit = exit_s.parse::<i32>().ok()?;
    let signal = signal_s.parse::<i32>().ok()?;
    if signal != 0 {
        Some(128 + signal)
    } else {
        Some(exit)
    }
}

/// Enumerate every task index covered by a `SlurmArraySpec`.
///
/// `max_concurrent` (the `%N` suffix) is deliberately ignored — it
/// constrains runtime concurrency at SLURM, not the set of tasks
/// submitted. Indices are returned in declaration order (`Vec` order).
#[allow(dead_code)]
pub(crate) fn expand_array_indices(spec: &crate::entities::slurm::SlurmArraySpec) -> Vec<u32> {
    use crate::entities::slurm::ArrayIndex;
    let mut out = Vec::new();
    for entry in &spec.indices {
        match *entry {
            ArrayIndex::Single(i) => out.push(i),
            ArrayIndex::Range { start, end } => {
                for i in start..=end {
                    out.push(i);
                }
            }
            ArrayIndex::Stepped { start, end, step } => {
                let mut i = start;
                while i <= end {
                    out.push(i);
                    match i.checked_add(step) {
                        Some(next) => i = next,
                        None => break,
                    }
                }
            }
        }
    }
    out
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
        let p = resolve_log_path("slurm-%j.out", 12345, None, None);
        assert_eq!(p, PathBuf::from("slurm-12345.out"));
    }

    #[test]
    fn resolve_substitutes_jobname_when_some() {
        let p = resolve_log_path("%x-%j.out", 12345, None, Some("g09run"));
        assert_eq!(p, PathBuf::from("g09run-12345.out"));
    }

    #[test]
    fn resolve_leaves_jobname_token_when_none() {
        let p = resolve_log_path("%x-%j.out", 12345, None, None);
        assert_eq!(p, PathBuf::from("%x-12345.out"));
    }

    #[test]
    fn resolve_leaves_unsupported_array_token_when_none() {
        // %a stays raw when array_task_id is None; %A still expands.
        let p = resolve_log_path("%A_%a-%j.out", 999, None, Some("nm"));
        assert_eq!(p, PathBuf::from("999_%a-999.out"));
    }

    #[test]
    fn resolve_leaves_truly_unsupported_tokens_raw() {
        let p = resolve_log_path("%5j-%t-%j.out", 999, None, None);
        assert_eq!(p, PathBuf::from("%5j-%t-999.out"));
    }

    #[test]
    fn resolve_substitutes_master_jobid_via_capital_a() {
        let p = resolve_log_path("slurm-%A.out", 12345, None, None);
        assert_eq!(p, PathBuf::from("slurm-12345.out"));
    }

    #[test]
    fn resolve_substitutes_array_task_id_via_lowercase_a() {
        let p = resolve_log_path("slurm-%A_%a.out", 12345, Some(7), None);
        assert_eq!(p, PathBuf::from("slurm-12345_7.out"));
    }

    #[test]
    fn resolve_leaves_array_task_id_token_when_none() {
        let p = resolve_log_path("slurm-%A_%a.out", 12345, None, None);
        assert_eq!(p, PathBuf::from("slurm-12345_%a.out"));
    }

    #[test]
    fn resolve_substitutes_user_env() {
        let prev = std::env::var("USER").ok();
        // SAFETY: single-threaded test, no other threads observing env.
        unsafe {
            std::env::set_var("USER", "alice");
        }
        let p = resolve_log_path("/home/%u/out-%j.log", 999, None, None);
        assert_eq!(p, PathBuf::from("/home/alice/out-999.log"));
        // SAFETY: restore previous USER.
        match prev {
            Some(v) => unsafe { std::env::set_var("USER", v) },
            None => unsafe { std::env::remove_var("USER") },
        }
    }

    #[test]
    fn resolve_substitutes_hostname_env() {
        let prev = std::env::var("HOSTNAME").ok();
        // SAFETY: single-threaded test.
        unsafe {
            std::env::set_var("HOSTNAME", "loginnode");
        }
        let p = resolve_log_path("%N-%j.out", 42, None, None);
        assert_eq!(p, PathBuf::from("loginnode-42.out"));
        match prev {
            Some(v) => unsafe { std::env::set_var("HOSTNAME", v) },
            None => unsafe { std::env::remove_var("HOSTNAME") },
        }
    }

    // ---- parse_sacct_exit_code ----

    #[test]
    fn parses_clean_zero_exit() {
        assert_eq!(parse_sacct_exit_code("0:0"), Some(0));
    }

    #[test]
    fn parses_nonzero_exit_no_signal() {
        assert_eq!(parse_sacct_exit_code("139:0"), Some(139));
    }

    #[test]
    fn parses_signal_kill_with_zero_exit() {
        // SIGKILL = 9 -> shell convention 128 + 9 = 137
        assert_eq!(parse_sacct_exit_code("0:9"), Some(137));
    }

    #[test]
    fn parses_signal_segv_with_nonzero_exit() {
        // SIGSEGV = 11 -> shell convention 128 + 11 = 139.
        // Slurm sometimes emits "139:11" — signal field is authoritative.
        assert_eq!(parse_sacct_exit_code("139:11"), Some(139));
    }

    #[test]
    fn rejects_garbled_field() {
        assert_eq!(parse_sacct_exit_code(""), None);
        assert_eq!(parse_sacct_exit_code("abc"), None);
        assert_eq!(parse_sacct_exit_code(":0"), None);
        assert_eq!(parse_sacct_exit_code("0:"), None);
        assert_eq!(parse_sacct_exit_code("0"), None);
    }

    // ---- expand_array_indices ----

    #[test]
    fn expand_single_value() {
        let spec: crate::entities::slurm::SlurmArraySpec = "5".parse().unwrap();
        assert_eq!(expand_array_indices(&spec), vec![5]);
    }

    #[test]
    fn expand_simple_range() {
        let spec: crate::entities::slurm::SlurmArraySpec = "0-3".parse().unwrap();
        assert_eq!(expand_array_indices(&spec), vec![0, 1, 2, 3]);
    }

    #[test]
    fn expand_stepped_range_even() {
        let spec: crate::entities::slurm::SlurmArraySpec = "0-8:2".parse().unwrap();
        assert_eq!(expand_array_indices(&spec), vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn expand_stepped_range_odd_endpoint() {
        // 0-10:4 -> 0, 4, 8 (10 NOT included since (10-0)%4 != 0)
        let spec: crate::entities::slurm::SlurmArraySpec = "0-10:4".parse().unwrap();
        assert_eq!(expand_array_indices(&spec), vec![0, 4, 8]);
    }

    #[test]
    fn expand_mixed_entries_preserves_order() {
        let spec: crate::entities::slurm::SlurmArraySpec = "0,2,5-7".parse().unwrap();
        assert_eq!(expand_array_indices(&spec), vec![0, 2, 5, 6, 7]);
    }

    #[test]
    fn expand_ignores_max_concurrent() {
        let spec: crate::entities::slurm::SlurmArraySpec = "0-3%2".parse().unwrap();
        assert_eq!(expand_array_indices(&spec), vec![0, 1, 2, 3]);
    }
}
