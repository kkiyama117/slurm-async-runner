//! Async SLURM job-status batch query, ported from the original Python
//! `slurm_async_runner.runner.SlurmManager.query_job_states_batch`.
//!
//! Issues at most one `squeue` (active queue) and one `sacct` (history)
//! subprocess per call. Returns a map keyed by **every** input jobid;
//! ids absent from both backends map to
//! `JobStatus { state: Unknown, reason: None }` so callers can rely on
//! every input id being present.
//!
//! The state token is parsed via `JobState::parse` (24 SLURM long forms
//! plus compact codes plus trailing-context tolerance) and the reason
//! via `JobReason::parse` (~80 SLURM reason strings plus `Other(String)`
//! forward-compat).

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::dispatcher::{JobDispatcher, TokioDispatcher};
use crate::{JobReason, JobState, JobStatus};

/// Bulk-query SLURM for the `(state, reason)` of every jobid in the input.
/// Default-flavored wrapper around [`query_job_states_batch_with`] that
/// uses [`TokioDispatcher`] for actual subprocess work.
pub async fn query_job_states_batch(jobids: &[u64]) -> Result<HashMap<u64, JobStatus>> {
    query_job_states_batch_with(&TokioDispatcher, jobids).await
}

/// Bulk-query SLURM via a custom [`JobDispatcher`].
///
/// Empty input short-circuits to `Ok(HashMap::new())` without dispatching.
/// Duplicate ids are de-duplicated for the actual SLURM calls but every
/// input id (duplicates included) appears as a key in the returned map.
/// Ids absent from both backends map to `JobStatus::default()`
/// (state=Unknown, reason=None).
pub async fn query_job_states_batch_with<D: JobDispatcher>(
    dispatcher: &D,
    jobids: &[u64],
) -> Result<HashMap<u64, JobStatus>> {
    if jobids.is_empty() {
        return Ok(HashMap::new());
    }

    let unique = dedupe_preserving_order(jobids);
    let id_csv = csv_join(&unique);

    let squeue_argv = vec![
        "squeue".to_string(),
        "-h".to_string(),
        "-j".to_string(),
        id_csv,
        "-o".to_string(),
        "%i %T %r".to_string(),
    ];
    let (_, squeue_out) = dispatcher.capture(&squeue_argv).await?;
    let active = parse_squeue(&squeue_out);

    let missing: Vec<u64> = unique
        .iter()
        .copied()
        .filter(|j| !active.contains_key(j))
        .collect();

    let history = if missing.is_empty() {
        HashMap::new()
    } else {
        let sacct_argv = vec![
            "sacct".to_string(),
            "-P".to_string(),
            "-n".to_string(),
            "-j".to_string(),
            csv_join(&missing),
            "-o".to_string(),
            "JobID,State,Reason".to_string(),
        ];
        let (_, sacct_out) = dispatcher.capture(&sacct_argv).await?;
        parse_sacct(&sacct_out)
    };

    Ok(merge_results(jobids, &active, &history))
}

/// Like [`query_job_states_batch_with`] but additionally captures sacct's
/// `ExitCode` column and returns it as part of [`JobOutcome`].
///
/// Phase 2 P1 introduces this so `SbatchJobHandle::refresh_with_sacct` can
/// persist the exit code into `FinishedInfo::exit_code`.
///
/// One squeue + at most one sacct call per invocation; jobids still active
/// in squeue do not trigger sacct (mirrors the legacy function's policy).
///
/// Mirrors [`query_job_states_batch_with`]'s "every input id is present in
/// the returned map" contract: ids absent from both squeue and sacct
/// receive `JobOutcome { status: JobStatus::default(), exit_code: None }`.
pub async fn query_job_states_with_exit_code_with<D: JobDispatcher>(
    dispatcher: &D,
    jobids: &[u64],
) -> Result<HashMap<u64, JobOutcome>> {
    if jobids.is_empty() {
        return Ok(HashMap::new());
    }

    let unique = dedupe_preserving_order(jobids);
    let id_csv = csv_join(&unique);

    let squeue_argv = vec![
        "squeue".to_string(),
        "-h".to_string(),
        "-j".to_string(),
        id_csv,
        "-o".to_string(),
        "%i %T %r".to_string(),
    ];
    let (_, squeue_out) = dispatcher.capture(&squeue_argv).await?;
    let active = parse_squeue(&squeue_out);

    let missing: Vec<u64> = unique
        .iter()
        .copied()
        .filter(|j| !active.contains_key(j))
        .collect();

    let history: HashMap<u64, JobOutcome> = if missing.is_empty() {
        HashMap::new()
    } else {
        let sacct_argv = vec![
            "sacct".to_string(),
            "-P".to_string(),
            "-n".to_string(),
            "-j".to_string(),
            csv_join(&missing),
            "-o".to_string(),
            "JobID,State,Reason,ExitCode".to_string(),
        ];
        let (_, sacct_out) = dispatcher.capture(&sacct_argv).await?;
        parse_sacct_with_exit_code(&sacct_out)
    };

    let mut out: HashMap<u64, JobOutcome> = HashMap::with_capacity(jobids.len());
    for jid in jobids.iter().copied() {
        if let Some(status) = active.get(&jid) {
            out.insert(
                jid,
                JobOutcome {
                    status: status.clone(),
                    exit_code: None,
                },
            );
        } else if let Some(oc) = history.get(&jid) {
            out.insert(jid, oc.clone());
        } else {
            out.insert(
                jid,
                JobOutcome {
                    status: JobStatus::default(),
                    exit_code: None,
                },
            );
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------- helpers

fn dedupe_preserving_order(ids: &[u64]) -> Vec<u64> {
    let mut seen = HashSet::with_capacity(ids.len());
    ids.iter().copied().filter(|j| seen.insert(*j)).collect()
}

fn csv_join(ids: &[u64]) -> String {
    let mut out = String::with_capacity(ids.len() * 8);
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&id.to_string());
    }
    out
}

/// Parse `%i %T %r` rows from `squeue -h`.
///
/// Lines without at least 2 whitespace-separated tokens (jobid + state)
/// are skipped. The 3rd token is the reason; if missing it defaults to
/// `JobReason::None`. Unparseable jobids (non-numeric first token) are
/// also skipped — `squeue` does not emit such rows in practice but the
/// parser stays forward-compatible with future format changes.
pub(crate) fn parse_squeue(text: &str) -> HashMap<u64, JobStatus> {
    let mut out = HashMap::new();
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let Some(jid_str) = parts.next() else {
            continue;
        };
        let Some(state_str) = parts.next() else {
            continue;
        };
        let reason_str = parts.next().unwrap_or("");
        let Ok(jid) = jid_str.parse::<u64>() else {
            continue;
        };
        out.insert(
            jid,
            JobStatus {
                state: JobState::parse(state_str),
                reason: JobReason::parse(reason_str),
            },
        );
    }
    out
}

/// Parse `JobID|State|Reason` rows from `sacct -P -n`.
///
/// Step rows (`12345.batch`, `12345.extern`, `12345.0`) carry the same
/// state as the parent and are filtered out so the base id wins
/// deterministically. Lines without a `|` are ignored. Reason column is
/// optional — older sacct may emit only 2 fields (`12345|COMPLETED`).
pub(crate) fn parse_sacct(text: &str) -> HashMap<u64, JobStatus> {
    let mut out = HashMap::new();
    for line in text.lines() {
        let mut parts = line.splitn(3, '|');
        let Some(jid_str) = parts.next() else {
            continue;
        };
        let Some(state_str) = parts.next() else {
            continue;
        };
        let reason_str = parts.next().unwrap_or("");
        if jid_str.contains('.') {
            continue;
        }
        let Ok(jid) = jid_str.parse::<u64>() else {
            continue;
        };
        out.insert(
            jid,
            JobStatus {
                state: JobState::parse(state_str),
                reason: JobReason::parse(reason_str),
            },
        );
    }
    out
}

/// Outcome of a sacct query for one jobid: status plus optional exit code.
///
/// Phase 2 P1 introduces this richer return type so `refresh_with_sacct`
/// can persist `FinishedInfo::exit_code`. The legacy
/// `query_job_states_batch_with` keeps its `HashMap<u64, JobStatus>`
/// signature for backward compatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobOutcome {
    pub status: JobStatus,
    pub exit_code: Option<i32>,
}

/// Parse `JobID|State|Reason|ExitCode` rows from `sacct -P -n`.
///
/// Behaves like [`parse_sacct`] for the first three fields, plus extracts
/// the optional fourth `ExitCode` column via
/// [`crate::sbatch::parse::parse_sacct_exit_code`].
///
/// Step rows (`12345.batch`, `12345.0`) are filtered. If the fourth field
/// is missing or unparseable, `JobOutcome::exit_code` is `None`.
pub(crate) fn parse_sacct_with_exit_code(text: &str) -> HashMap<u64, JobOutcome> {
    use crate::sbatch::parse::parse_sacct_exit_code;
    let mut out = HashMap::new();
    for line in text.lines() {
        let mut parts = line.splitn(4, '|');
        let Some(jid_str) = parts.next() else {
            continue;
        };
        let Some(state_str) = parts.next() else {
            continue;
        };
        let reason_str = parts.next().unwrap_or("");
        let exit_field = parts.next();
        if jid_str.contains('.') {
            continue;
        }
        let Ok(jid) = jid_str.parse::<u64>() else {
            continue;
        };
        let exit_code = exit_field.and_then(parse_sacct_exit_code);
        out.insert(
            jid,
            JobOutcome {
                status: JobStatus {
                    state: JobState::parse(state_str),
                    reason: JobReason::parse(reason_str),
                },
                exit_code,
            },
        );
    }
    out
}

/// Parse `qgroup -l` output into a `{jobid: JobStatus}` map.
///
/// Expected layout (KUDPC; the pipe-separated form is the current shape
/// observed in production):
/// ```text
///  QUEUE    USER     JOBID    | STAT  SUBMIT_AT        | RSC:core | PROC CORE    MEM       ELAPSE
///  gr10641a b39027   7519503  | FINI  2026-05-12 01:29 |        1 |    1    1  1070M     00:01:00
/// ```
///
/// Earlier KUDPC layouts emitted the same columns without `|` separators
/// (`QUEUE USER JOBID STATUS …`); both forms are accepted because the
/// parser ignores standalone `|` tokens.
///
/// Behaviour:
/// - Whitespace-split each line and drop `|` tokens (KUDPC column dividers).
/// - First numeric token is taken as JOBID; the next non-`|` token after it
///   is taken as STATUS.
/// - Lines without a parseable JOBID are skipped (header rows, blanks, the
///   per-queue and per-user summary rows where the JOBID column is absent).
/// - State strings are forwarded to `JobState::parse` (handles "RUN", "QUE",
///   "CMP", "FINI", and SLURM long forms thanks to forward-compat fallbacks).
/// - Reason is set to `JobReason::None` (qgroup -l does not surface reasons).
pub fn parse_qgroup_l(stdout: &str) -> HashMap<u64, JobStatus> {
    let mut out = HashMap::new();
    for line in stdout.lines() {
        // Drop standalone `|` column dividers (current KUDPC layout);
        // the older pipe-less layout passes through unchanged.
        let mut tokens = line.split_whitespace().filter(|t| *t != "|");
        let _queue = tokens.next();
        let _user = tokens.next();
        let Some(jobid_str) = tokens.next() else {
            continue;
        };
        let Some(state_str) = tokens.next() else {
            continue;
        };
        let Ok(jobid) = jobid_str.parse::<u64>() else {
            continue;
        };
        // SLURM/KUDPC jobids are positive; a literal `0` here means the
        // line is a summary row (`QUEUE SYS | RUN PEND OTHER …`) whose
        // third token happens to be a count, not a real jobid.
        if jobid == 0 {
            continue;
        }
        out.insert(
            jobid,
            JobStatus {
                state: JobState::parse(state_str),
                reason: JobReason::None,
            },
        );
    }
    out
}

/// Bulk-query KUDPC's `qgroup -l` for the given `jobids`. Cheap (KUDPC
/// docs do not flag it as system-intensive). Returns only the jobids that
/// `qgroup -l` reports; missing ids are simply absent from the map (caller
/// decides whether to fall back to squeue / sacct).
pub async fn query_job_states_via_qgroup_with<D: JobDispatcher>(
    dispatcher: &D,
    jobids: &[u64],
) -> Result<HashMap<u64, JobStatus>> {
    if jobids.is_empty() {
        return Ok(HashMap::new());
    }
    let argv = vec!["qgroup".to_string(), "-l".to_string()];
    let (_, stdout) = dispatcher.capture(&argv).await?;
    let all = parse_qgroup_l(&stdout);
    let wanted: HashSet<u64> = jobids.iter().copied().collect();
    Ok(all
        .into_iter()
        .filter(|(k, _)| wanted.contains(k))
        .collect())
}

/// Like [`query_job_states_batch_with`] but **squeue only**, no sacct
/// fallback. Returns only the jobids squeue reports; missing ids are
/// absent from the map.
pub async fn query_job_states_squeue_only_with<D: JobDispatcher>(
    dispatcher: &D,
    jobids: &[u64],
) -> Result<HashMap<u64, JobStatus>> {
    if jobids.is_empty() {
        return Ok(HashMap::new());
    }
    let unique = dedupe_preserving_order(jobids);
    let argv = vec![
        "squeue".to_string(),
        "-h".to_string(),
        "-j".to_string(),
        csv_join(&unique),
        "-o".to_string(),
        "%i %T %r".to_string(),
    ];
    let (_, out) = dispatcher.capture(&argv).await?;
    Ok(parse_squeue(&out))
}

/// Query a single array task by its SLURM `<master>_<idx>` key via
/// `squeue`. Returns the task's [`JobStatus`] if squeue still has the
/// task in the active queue, or `None` if squeue reports no rows for
/// that key (the task has left the active listing — caller may follow
/// up with [`query_array_task_outcome_with`] to consult sacct).
///
/// This is the per-task analogue of [`query_job_states_squeue_only_with`]
/// for handles whose `array_task_id.is_some()`. KUDPC's `qgroup -l`
/// returns the array master summary (one row per submission, not per
/// task), so the per-task refresh path skips qgroup and goes straight
/// to squeue.
///
/// See spec §5.5 (sbatch Phase 2 design) and issue #8 A5.
pub async fn query_array_task_state_with<D: JobDispatcher>(
    dispatcher: &D,
    master_jobid: u64,
    array_task_id: u32,
) -> Result<Option<JobStatus>> {
    let key = format!("{master_jobid}_{array_task_id}");
    let argv = vec![
        "squeue".to_string(),
        "-h".to_string(),
        "-j".to_string(),
        key,
        "-o".to_string(),
        "%T %r".to_string(),
    ];
    let (_, out) = dispatcher.capture(&argv).await?;
    Ok(parse_squeue_array_task(&out))
}

/// Per-task heavyweight finalizer. Issues `sacct -P -n -j <master>_<idx>`
/// and returns a [`JobOutcome`] (status + exit code). Caller is expected
/// to have already verified via [`query_array_task_state_with`] that
/// the task has vanished from the active queue; this function does not
/// short-circuit on a still-active task.
///
/// Returns `None` if sacct reports no parent row for the task (purged
/// from history, never made it past the controller, …). Step rows
/// (`<master>_<idx>.batch`, `<master>_<idx>.0`) are filtered out — only
/// the parent row contributes to the returned outcome.
///
/// See spec §5.5 (sbatch Phase 2 design) and issue #8 A5.
pub async fn query_array_task_outcome_with<D: JobDispatcher>(
    dispatcher: &D,
    master_jobid: u64,
    array_task_id: u32,
) -> Result<Option<JobOutcome>> {
    let key = format!("{master_jobid}_{array_task_id}");
    let argv = vec![
        "sacct".to_string(),
        "-P".to_string(),
        "-n".to_string(),
        "-j".to_string(),
        key,
        "-o".to_string(),
        "JobID,State,Reason,ExitCode".to_string(),
    ];
    let (_, out) = dispatcher.capture(&argv).await?;
    Ok(parse_sacct_array_task_with_exit_code(&out))
}

/// Parse a single-row `squeue -o "%T %r"` output for one array task.
///
/// The `%i` column is omitted in the argv because we already know which
/// task we asked for, and including it would force the parser to also
/// recognise the `<master>_<idx>` syntax (which is not a `u64`). Empty
/// output (no row) is the "task has vanished" signal — returns `None`.
///
/// Reason is optional; missing reason defaults to [`JobReason::None`].
pub(crate) fn parse_squeue_array_task(text: &str) -> Option<JobStatus> {
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let state_str = match parts.next() {
            Some(s) => s,
            None => continue,
        };
        let reason_str = parts.next().unwrap_or("");
        return Some(JobStatus {
            state: JobState::parse(state_str),
            reason: JobReason::parse(reason_str),
        });
    }
    None
}

/// Parse `sacct -P -n -j <master>_<idx> -o "JobID|State|Reason|ExitCode"`
/// output for one array task. Returns the parent row (without a `.` in
/// the JobID column); step rows (`<master>_<idx>.batch`, `.0`, …) are
/// silently skipped.
///
/// The JobID column itself is only used as a discriminator between
/// parent and step rows — its `<master>_<idx>` value is never parsed as
/// a number, so we sidestep the "u64 parse failure" that the
/// summary-mode [`parse_sacct_with_exit_code`] would hit on array task
/// rows.
pub(crate) fn parse_sacct_array_task_with_exit_code(text: &str) -> Option<JobOutcome> {
    use crate::sbatch::parse::parse_sacct_exit_code;
    for line in text.lines() {
        let mut parts = line.splitn(4, '|');
        let Some(jid_str) = parts.next() else {
            continue;
        };
        // Step rows (`<master>_<idx>.batch` etc.) are filtered out — only
        // the parent row contributes the canonical state/exit code.
        if jid_str.contains('.') {
            continue;
        }
        let Some(state_str) = parts.next() else {
            continue;
        };
        let reason_str = parts.next().unwrap_or("");
        let exit_field = parts.next();
        let exit_code = exit_field.and_then(parse_sacct_exit_code);
        return Some(JobOutcome {
            status: JobStatus {
                state: JobState::parse(state_str),
                reason: JobReason::parse(reason_str),
            },
            exit_code,
        });
    }
    None
}

/// Resolve every input id from `(active → history → Unknown)`.
///
/// Uses `JobStatus::default()` (state=Unknown, reason=None) for ids that
/// neither backend reported. Input duplicates produce duplicate keys in
/// the returned map collapsing onto the same value — same as the original
/// Python `{jid: ... for jid in jobids}` dict-comprehension semantics.
pub(crate) fn merge_results(
    jobids: &[u64],
    active: &HashMap<u64, JobStatus>,
    history: &HashMap<u64, JobStatus>,
) -> HashMap<u64, JobStatus> {
    jobids
        .iter()
        .map(|jid| {
            let status = active
                .get(jid)
                .or_else(|| history.get(jid))
                .cloned()
                .unwrap_or_default();
            (*jid, status)
        })
        .collect()
}

// --------------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;

    // ---- parse_squeue ----

    #[test]
    fn parse_squeue_three_field_rows() {
        let text = "100 PENDING Priority\n200 RUNNING None\n";
        let m = parse_squeue(text);
        assert_eq!(m.len(), 2);
        assert_eq!(
            m[&100],
            JobStatus::with_reason(JobState::Pending, JobReason::Priority)
        );
        assert_eq!(m[&200], JobStatus::new(JobState::Running));
    }

    #[test]
    fn parse_squeue_compact_state_codes() {
        let text = "1 PD Priority\n2 R None\n3 OOM OutOfMemory\n";
        let m = parse_squeue(text);
        assert_eq!(m[&1].state, JobState::Pending);
        assert_eq!(m[&2].state, JobState::Running);
        assert_eq!(m[&3].state, JobState::OutOfMemory);
        assert_eq!(m[&3].reason, JobReason::OutOfMemory);
    }

    #[test]
    fn parse_squeue_missing_reason_defaults_to_none() {
        let text = "42 RUNNING\n";
        let m = parse_squeue(text);
        assert_eq!(m[&42], JobStatus::new(JobState::Running));
    }

    #[test]
    fn parse_squeue_skips_malformed_and_empty() {
        let text = "\n\
                    notanid PENDING Priority\n\
                    just-one-token\n\
                    \n\
                    777 RUNNING None\n";
        let m = parse_squeue(text);
        assert_eq!(m.len(), 1);
        assert!(m.contains_key(&777));
    }

    #[test]
    fn parse_squeue_unknown_state_falls_back() {
        // Forward-compat: a future SLURM version emits a state we
        // don't know yet — must round-trip to JobState::Unknown.
        let text = "999 FUTURE_STATE FutureReason\n";
        let m = parse_squeue(text);
        assert_eq!(m[&999].state, JobState::Unknown);
        assert!(matches!(
            m[&999].reason,
            JobReason::Other(ref s) if s == "FutureReason"
        ));
    }

    // ---- parse_sacct ----

    #[test]
    fn parse_sacct_three_field_rows() {
        let text = "100|COMPLETED|None\n200|FAILED|NonZeroExitCode\n";
        let m = parse_sacct(text);
        assert_eq!(m.len(), 2);
        assert_eq!(m[&100], JobStatus::new(JobState::Completed));
        assert_eq!(
            m[&200],
            JobStatus::with_reason(JobState::Failed, JobReason::NonZeroExitCode)
        );
    }

    #[test]
    fn parse_sacct_filters_step_rows() {
        let text = "100|COMPLETED|None\n\
                    100.batch|COMPLETED|None\n\
                    100.extern|COMPLETED|None\n\
                    100.0|COMPLETED|None\n";
        let m = parse_sacct(text);
        assert_eq!(m.len(), 1);
        assert!(m.contains_key(&100));
    }

    #[test]
    fn parse_sacct_handles_trailing_context_in_state() {
        // sacct prints `CANCELLED by 1234` for the State column. The
        // upstream JobState::parse trims to the first whitespace token.
        let text = "100|CANCELLED by 1234|None\n";
        let m = parse_sacct(text);
        assert_eq!(m[&100].state, JobState::Cancelled);
    }

    #[test]
    fn parse_sacct_two_field_rows_default_reason_to_none() {
        let text = "42|COMPLETED\n";
        let m = parse_sacct(text);
        assert_eq!(m[&42], JobStatus::new(JobState::Completed));
    }

    #[test]
    fn parse_sacct_skips_lines_without_pipe() {
        let text = "no-pipe-here\n100|COMPLETED|None\n";
        let m = parse_sacct(text);
        assert_eq!(m.len(), 1);
        assert!(m.contains_key(&100));
    }

    // ---- parse_sacct_with_exit_code ----

    #[test]
    fn parse_sacct_with_exit_code_three_fields_completed() {
        let text = "12345|COMPLETED|None|0:0\n";
        let m = parse_sacct_with_exit_code(text);
        let oc = m.get(&12345).expect("jobid present");
        assert_eq!(oc.status.state, JobState::Completed);
        assert_eq!(oc.exit_code, Some(0));
    }

    #[test]
    fn parse_sacct_with_exit_code_signaled() {
        let text = "12345|CANCELLED by 1001|None|0:9\n";
        let m = parse_sacct_with_exit_code(text);
        let oc = m.get(&12345).expect("jobid present");
        assert_eq!(oc.status.state, JobState::Cancelled);
        assert_eq!(oc.exit_code, Some(137));
    }

    #[test]
    fn parse_sacct_with_exit_code_filters_step_rows() {
        let text = "12345|COMPLETED|None|0:0\n12345.batch|COMPLETED|None|0:0\n";
        let m = parse_sacct_with_exit_code(text);
        assert_eq!(m.len(), 1);
        assert!(m.contains_key(&12345));
    }

    #[test]
    fn parse_sacct_with_exit_code_handles_missing_exit_field() {
        let text = "12345|COMPLETED|None\n";
        let m = parse_sacct_with_exit_code(text);
        let oc = m.get(&12345).expect("jobid present");
        assert_eq!(oc.exit_code, None);
        assert_eq!(oc.status.state, JobState::Completed);
    }

    // ---- query_job_states_with_exit_code_with ----

    #[tokio::test]
    async fn query_with_exit_code_squeue_only_reports_no_exit_code() {
        struct D;
        impl crate::dispatcher::JobDispatcher for D {
            async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
                unimplemented!()
            }
            async fn capture(&self, argv: &[String]) -> anyhow::Result<(i32, String)> {
                let bin = argv[0].as_str();
                let out = if bin == "squeue" {
                    "12345 RUNNING None\n".to_string()
                } else {
                    String::new()
                };
                Ok((0, out))
            }
        }
        let m = query_job_states_with_exit_code_with(&D, &[12345])
            .await
            .unwrap();
        let oc = m.get(&12345).unwrap();
        assert_eq!(oc.status.state, JobState::Running);
        assert_eq!(oc.exit_code, None);
    }

    #[tokio::test]
    async fn query_with_exit_code_sacct_supplies_exit_code() {
        struct D;
        impl crate::dispatcher::JobDispatcher for D {
            async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
                unimplemented!()
            }
            async fn capture(&self, argv: &[String]) -> anyhow::Result<(i32, String)> {
                let bin = argv[0].as_str();
                let out = if bin == "squeue" {
                    String::new()
                } else if bin == "sacct" {
                    let format_idx = argv.iter().position(|a| a == "-o").unwrap();
                    assert!(
                        argv[format_idx + 1].contains("ExitCode"),
                        "sacct argv must include ExitCode column, got: {:?}",
                        argv
                    );
                    "12345|COMPLETED|None|0:0\n".to_string()
                } else {
                    String::new()
                };
                Ok((0, out))
            }
        }
        let m = query_job_states_with_exit_code_with(&D, &[12345])
            .await
            .unwrap();
        let oc = m.get(&12345).unwrap();
        assert_eq!(oc.status.state, JobState::Completed);
        assert_eq!(oc.exit_code, Some(0));
    }

    #[tokio::test]
    async fn query_with_exit_code_missing_id_defaults_to_unknown() {
        struct D;
        impl crate::dispatcher::JobDispatcher for D {
            async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
                unimplemented!()
            }
            async fn capture(&self, _argv: &[String]) -> anyhow::Result<(i32, String)> {
                // Both squeue and sacct return empty — id 99999 is unknown to both
                Ok((0, String::new()))
            }
        }
        let m = query_job_states_with_exit_code_with(&D, &[99999])
            .await
            .unwrap();
        let oc = m
            .get(&99999)
            .expect("missing id must still appear in returned map");
        assert_eq!(oc.status, JobStatus::default());
        assert_eq!(oc.status.state, JobState::Unknown);
        assert_eq!(oc.status.reason, JobReason::None);
        assert_eq!(oc.exit_code, None);
    }

    // ---- merge_results ----

    #[test]
    fn merge_active_wins_over_history() {
        let active = HashMap::from([(
            1,
            JobStatus::with_reason(JobState::Running, JobReason::None),
        )]);
        let history = HashMap::from([(
            1,
            JobStatus::with_reason(JobState::Completed, JobReason::None),
        )]);
        let m = merge_results(&[1], &active, &history);
        assert_eq!(m[&1].state, JobState::Running);
    }

    #[test]
    fn merge_falls_back_to_history() {
        let active = HashMap::new();
        let history = HashMap::from([(7, JobStatus::new(JobState::Completed))]);
        let m = merge_results(&[7], &active, &history);
        assert_eq!(m[&7].state, JobState::Completed);
    }

    #[test]
    fn merge_unknown_for_missing() {
        let m = merge_results(&[42], &HashMap::new(), &HashMap::new());
        assert_eq!(m[&42], JobStatus::default());
        assert_eq!(m[&42].state, JobState::Unknown);
        assert_eq!(m[&42].reason, JobReason::None);
    }

    #[test]
    fn merge_returns_every_input_id() {
        // Contract: every input id appears as a key in the result.
        let active = HashMap::from([(1, JobStatus::new(JobState::Running))]);
        let history = HashMap::from([(2, JobStatus::new(JobState::Completed))]);
        let m = merge_results(&[1, 2, 3], &active, &history);
        assert_eq!(m.len(), 3);
        assert_eq!(m[&1].state, JobState::Running);
        assert_eq!(m[&2].state, JobState::Completed);
        assert_eq!(m[&3].state, JobState::Unknown);
    }

    // ---- dedupe / csv_join ----

    #[test]
    fn dedupe_preserves_first_occurrence_order() {
        assert_eq!(
            dedupe_preserving_order(&[3, 1, 2, 1, 3, 4]),
            vec![3, 1, 2, 4]
        );
    }

    #[test]
    fn csv_join_basic() {
        assert_eq!(csv_join(&[]), "");
        assert_eq!(csv_join(&[1]), "1");
        assert_eq!(csv_join(&[1, 2, 3]), "1,2,3");
    }

    // ---- end-to-end (no subprocess) ----

    #[tokio::test]
    async fn empty_input_short_circuits() {
        let m = query_job_states_batch(&[]).await.unwrap();
        assert!(m.is_empty());
    }

    // ---- test fixtures for async query functions ----

    struct PanicDispatcher;
    impl JobDispatcher for PanicDispatcher {
        async fn run(&self, _argv: &[String]) -> Result<i32> {
            panic!("PanicDispatcher.run called")
        }
        async fn capture(&self, _argv: &[String]) -> Result<(i32, String)> {
            panic!("PanicDispatcher.capture called")
        }
    }

    struct MockCapture {
        expected_argv: Vec<String>,
        stdout: String,
    }
    impl JobDispatcher for MockCapture {
        async fn run(&self, _argv: &[String]) -> Result<i32> {
            panic!("not used")
        }
        async fn capture(&self, argv: &[String]) -> Result<(i32, String)> {
            assert_eq!(argv, self.expected_argv.as_slice());
            Ok((0, self.stdout.clone()))
        }
    }

    // ---- query_job_states_via_qgroup_with ----

    #[tokio::test]
    async fn query_via_qgroup_short_circuits_on_empty_input() {
        // No dispatcher call should happen for empty input.
        let m = super::query_job_states_via_qgroup_with(&PanicDispatcher, &[])
            .await
            .unwrap();
        assert!(m.is_empty());
    }

    #[tokio::test]
    async fn query_squeue_only_short_circuits_on_empty_input() {
        let m = super::query_job_states_squeue_only_with(&PanicDispatcher, &[])
            .await
            .unwrap();
        assert!(m.is_empty());
    }

    #[tokio::test]
    async fn query_via_qgroup_filters_to_requested_jobids() {
        // qgroup returns jobs 1, 2, 3; we only ask for 2 — only 2 should come back.
        let stdout = "\
queue user 1 RUN 1 1 1M 0:0:1(0:1:0)
queue user 2 RUN 1 1 1M 0:0:1(0:1:0)
queue user 3 RUN 1 1 1M 0:0:1(0:1:0)
";
        let mock = MockCapture {
            expected_argv: vec!["qgroup".into(), "-l".into()],
            stdout: stdout.to_string(),
        };
        let m = super::query_job_states_via_qgroup_with(&mock, &[2])
            .await
            .unwrap();
        assert_eq!(m.len(), 1);
        assert!(m.contains_key(&2));
    }

    // ---- parse_qgroup_l ----

    #[test]
    fn parse_qgroup_l_extracts_jobid_and_state() {
        let out = "\
QUEUE     USER     JOBID          STATUS  PROC  CORE    MEM    ELAPSE(    limit)
gr19999b  b59999   12345          RUN        4     1  4570M  00:00:07( 01:00:00)
gr19999b  b59999   12346          QUE        1     1   100M  00:00:00( 00:30:00)
";
        let map = super::parse_qgroup_l(out);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&12345).unwrap().state, JobState::Running);
        assert_eq!(map.get(&12346).unwrap().state, JobState::Pending);
    }

    #[test]
    fn parse_qgroup_l_skips_blank_and_short_lines() {
        let out = "\
QUEUE USER JOBID STATUS

short
gr19999b u 9999 RUN 1 1 1M 0:0:1(0:1:0)
";
        let map = super::parse_qgroup_l(out);
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&9999));
    }

    #[test]
    fn parse_qgroup_l_handles_completed_state() {
        let out = "\
QUEUE USER JOBID STATUS PROC
gr19999b u 5555 CMP 1
";
        let map = super::parse_qgroup_l(out);
        assert_eq!(map.get(&5555).map(|s| s.state), Some(JobState::Completed));
    }

    /// Current KUDPC `qgroup -l` output uses `|` as column dividers and
    /// emits `FINI` as the "completed" status token. Regression guard for
    /// the bug where `wait_terminal` timed out at 180 s even though the
    /// job had finished, because the previous parser took field index 3
    /// (`"|"`) as STATUS and `FINI` had no mapping in `JobState::parse`.
    #[test]
    fn parse_qgroup_l_handles_kudpc_pipe_separated_fini_row() {
        let out = "\
 QUEUE    SYS |   RUN  PEND OTHER | ALLOC ( MIN/ STD/ MAX)
----------------------------------------------------------------
 gr10641a  A  |     0     0     1 |     0 (   0/ 112/ 112)

 QUEUE    USER     |   RUN(ALLOC)  PEND(REQUEST) OTHER(REQUEST)
----------------------------------------------------------------
 gr10641a b39027   |     0(    0)     0(     0)     1(     1)

 QUEUE    USER     JOBID    | STAT  SUBMIT_AT        | RSC:core | PROC CORE    MEM       ELAPSE
------------------------------------------------------------------------------------------------
 gr10641a b39027   7519503  | FINI  2026-05-12 01:29 |        1 |    1    1  1070M     00:01:00
";
        let map = super::parse_qgroup_l(out);
        // Only the detail row carries a real jobid; the two summary
        // sections must be skipped.
        assert_eq!(map.len(), 1, "expected 1 detail row, got: {map:?}");
        assert_eq!(
            map.get(&7519503).map(|s| s.state),
            Some(JobState::Completed),
            "FINI must map to Completed so wait_terminal can exit"
        );
    }

    /// The pending-side equivalent of the KUDPC pipe layout — `QUE`
    /// should still resolve to `Pending`, confirming the new token
    /// stream filter works for non-terminal states too.
    #[test]
    fn parse_qgroup_l_handles_kudpc_pipe_separated_que_row() {
        let out = "\
 gr10641a b39027   7519600  | QUE   2026-05-12 02:00 |        1 |    1    1  1070M     00:00:00
";
        let map = super::parse_qgroup_l(out);
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&7519600).map(|s| s.state), Some(JobState::Pending));
    }

    // ---- parse_squeue_array_task ----

    #[test]
    fn parse_squeue_array_task_extracts_state_and_reason() {
        // squeue with -o "%T %r" gives just `STATE REASON` per row.
        let out = "RUNNING None\n";
        let s = super::parse_squeue_array_task(out).expect("Some");
        assert_eq!(s.state, JobState::Running);
        assert_eq!(s.reason, JobReason::None);
    }

    #[test]
    fn parse_squeue_array_task_returns_none_for_empty_output() {
        assert_eq!(super::parse_squeue_array_task(""), None);
        assert_eq!(super::parse_squeue_array_task("\n"), None);
    }

    #[test]
    fn parse_squeue_array_task_carries_pending_reason() {
        let out = "PENDING Priority\n";
        let s = super::parse_squeue_array_task(out).expect("Some");
        assert_eq!(s.state, JobState::Pending);
        assert_eq!(s.reason, JobReason::Priority);
    }

    // ---- parse_sacct_array_task_with_exit_code ----

    #[test]
    fn parse_sacct_array_task_returns_parent_row_skipping_steps() {
        // Real sacct emits the parent row plus step rows (`.batch`, `.0`).
        // Only the parent row should contribute.
        let out = "\
12345_3|COMPLETED|None|0:0
12345_3.batch|COMPLETED|None|0:0
12345_3.extern|COMPLETED|None|0:0
";
        let oc = super::parse_sacct_array_task_with_exit_code(out).expect("Some");
        assert_eq!(oc.status.state, JobState::Completed);
        assert_eq!(oc.exit_code, Some(0));
    }

    #[test]
    fn parse_sacct_array_task_recovers_nonzero_exit_code() {
        let out = "12345_7|FAILED|NonZeroExitCode|2:0\n";
        let oc = super::parse_sacct_array_task_with_exit_code(out).expect("Some");
        assert_eq!(oc.status.state, JobState::Failed);
        assert_eq!(oc.status.reason, JobReason::NonZeroExitCode);
        assert_eq!(oc.exit_code, Some(2));
    }

    #[test]
    fn parse_sacct_array_task_returns_none_for_only_step_rows() {
        // Defensive: if sacct somehow returns only step rows (purged
        // parent, …), the parser must not synthesize a parent.
        let out = "12345_3.batch|COMPLETED|None|0:0\n";
        assert_eq!(super::parse_sacct_array_task_with_exit_code(out), None);
    }

    #[test]
    fn parse_sacct_array_task_returns_none_for_empty_output() {
        assert_eq!(super::parse_sacct_array_task_with_exit_code(""), None);
    }

    // ---- query_array_task_state_with / query_array_task_outcome_with ----

    #[tokio::test]
    async fn query_array_task_state_uses_master_underscore_idx_squeue_key() {
        let mock = MockCapture {
            expected_argv: vec![
                "squeue".into(),
                "-h".into(),
                "-j".into(),
                "12345_3".into(),
                "-o".into(),
                "%T %r".into(),
            ],
            stdout: "RUNNING None\n".into(),
        };
        let s = super::query_array_task_state_with(&mock, 12345, 3)
            .await
            .unwrap()
            .expect("Some");
        assert_eq!(s.state, JobState::Running);
    }

    #[tokio::test]
    async fn query_array_task_state_returns_none_when_squeue_reports_no_row() {
        let mock = MockCapture {
            expected_argv: vec![
                "squeue".into(),
                "-h".into(),
                "-j".into(),
                "99999_0".into(),
                "-o".into(),
                "%T %r".into(),
            ],
            stdout: String::new(),
        };
        let r = super::query_array_task_state_with(&mock, 99999, 0)
            .await
            .unwrap();
        assert!(r.is_none());
    }

    #[tokio::test]
    async fn query_array_task_outcome_uses_master_underscore_idx_sacct_key() {
        let mock = MockCapture {
            expected_argv: vec![
                "sacct".into(),
                "-P".into(),
                "-n".into(),
                "-j".into(),
                "12345_3".into(),
                "-o".into(),
                "JobID,State,Reason,ExitCode".into(),
            ],
            stdout: "12345_3|COMPLETED|None|0:0\n".into(),
        };
        let oc = super::query_array_task_outcome_with(&mock, 12345, 3)
            .await
            .unwrap()
            .expect("Some");
        assert_eq!(oc.status.state, JobState::Completed);
        assert_eq!(oc.exit_code, Some(0));
    }
}
