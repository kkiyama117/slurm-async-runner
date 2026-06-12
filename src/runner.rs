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
//!
//! The full index of SLURM output formats understood by this crate lives
//! in [`crate::sbatch::parse`] (issue #16 item 5).

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::dispatcher::{CaptureOutput, JobDispatcher, TokioDispatcher};
use crate::{JobReason, JobState, JobStatus};

/// Fail with a uniform `` `{tool}` exited with {code}: {stderr} `` error
/// when a query subprocess reports a nonzero exit. Callers decide what a
/// nonzero exit *means* first (see [`squeue_reports_vanished`]) and only
/// route genuine failures here.
fn ensure_query_success(tool: &str, out: &CaptureOutput) -> anyhow::Result<()> {
    if out.success() {
        return Ok(());
    }
    anyhow::bail!(
        "`{tool}` exited with {}: {}",
        out.exit_code,
        out.stderr.trim()
    )
}

/// squeue exits non-zero with `Invalid job id specified` on stderr when
/// every queried jobid has left the queue (KUDPC purges terminated jobs
/// immediately). That is a *vanish* signal, not a failure.
fn squeue_reports_vanished(out: &CaptureOutput) -> bool {
    out.exit_code != 0 && out.stderr.contains("Invalid job id specified")
}

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
    let squeue_out = dispatcher.capture(&squeue_argv).await?;
    let active = if squeue_reports_vanished(&squeue_out) {
        // Every queried id has left the queue — fall through to sacct.
        HashMap::new()
    } else {
        ensure_query_success("squeue", &squeue_out)?;
        parse_squeue(&squeue_out.stdout)
    };

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
        let sacct_out = dispatcher.capture(&sacct_argv).await?;
        // sacct returns exit 0 + empty stdout for unknown jobids;
        // a nonzero exit is a genuine failure.
        ensure_query_success("sacct", &sacct_out)?;
        parse_sacct(&sacct_out.stdout)
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
    let squeue_out = dispatcher.capture(&squeue_argv).await?;
    let active = if squeue_reports_vanished(&squeue_out) {
        // Every queried id has left the queue — fall through to sacct.
        HashMap::new()
    } else {
        ensure_query_success("squeue", &squeue_out)?;
        parse_squeue(&squeue_out.stdout)
    };

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
        let sacct_out = dispatcher.capture(&sacct_argv).await?;
        // sacct returns exit 0 + empty stdout for unknown jobids;
        // a nonzero exit is a genuine failure.
        ensure_query_success("sacct", &sacct_out)?;
        parse_sacct_with_exit_code(&sacct_out.stdout)
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
    let out = dispatcher.capture(&argv).await?;
    // A nonzero qgroup exit is an error here; the refresh caller
    // (`SbatchJobHandle::refresh`) converts any qgroup `Err` into a
    // warn + miss + squeue fallback.
    ensure_query_success("qgroup", &out)?;
    let all = parse_qgroup_l(&out.stdout);
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
    let out = dispatcher.capture(&argv).await?;
    if squeue_reports_vanished(&out) {
        // Every queried id has left the queue — report an empty listing.
        return Ok(HashMap::new());
    }
    ensure_query_success("squeue", &out)?;
    Ok(parse_squeue(&out.stdout))
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
    let out = dispatcher.capture(&argv).await?;
    if squeue_reports_vanished(&out) {
        // The task has left the queue — the vanish signal, not a failure.
        return Ok(None);
    }
    ensure_query_success("squeue", &out)?;
    Ok(parse_squeue_array_task(&out.stdout))
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
        key.clone(),
        "-o".to_string(),
        "JobID,State,Reason,ExitCode".to_string(),
    ];
    let out = dispatcher.capture(&argv).await?;
    // sacct returns exit 0 + empty stdout for unknown jobids; a nonzero
    // exit is a genuine failure.
    ensure_query_success("sacct", &out)?;
    Ok(parse_sacct_array_task_with_exit_code(&out.stdout, &key))
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
/// output for one array task. Returns the parent row whose JobID column
/// equals `key` (the `<master>_<idx>` string that was queried); step rows
/// (`<master>_<idx>.batch`, `.0`, …) and rows for any other job are
/// silently skipped.
///
/// The exact-key match makes the parser safe against listings that
/// contain rows for more than the queried task (defense in depth today;
/// a prerequisite for ever batching array-task sacct queries). The
/// `<master>_<idx>` value is still never parsed as a number, so we
/// sidestep the "u64 parse failure" that the summary-mode
/// [`parse_sacct_with_exit_code`] would hit on array task rows.
pub(crate) fn parse_sacct_array_task_with_exit_code(text: &str, key: &str) -> Option<JobOutcome> {
    use crate::sbatch::parse::parse_sacct_exit_code;
    for line in text.lines() {
        let mut parts = line.splitn(4, '|');
        let Some(jid_str) = parts.next() else {
            continue;
        };
        // Only the queried task's parent row contributes the canonical
        // state/exit code. Step rows (`<master>_<idx>.batch` etc.) and
        // rows for other jobs fail the equality check.
        if jid_str != key {
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
mod tests;
