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

/// Parse `qgroup -l` output into a `{jobid: JobStatus}` map.
///
/// Expected layout (KUDPC):
/// ```text
/// QUEUE     USER     JOBID          STATUS  PROC  CORE    MEM    ELAPSE(    limit)
/// gr19999b  b59999   12345          RUN        4     1  4570M  00:00:07( 01:00:00)
/// ```
///
/// Behaviour:
/// - Whitespace-split each line; take field index 2 as JOBID and 3 as STATUS.
/// - Lines without at least 4 fields are skipped (header, blanks).
/// - Lines whose JOBID field is not a valid u64 are skipped.
/// - State strings are forwarded to `JobState::parse` (handles "RUN", "QUE",
///   "CMP", and SLURM long forms thanks to forward-compat fallbacks).
/// - Reason is set to `JobReason::None` (qgroup -l does not surface reasons).
pub fn parse_qgroup_l(stdout: &str) -> HashMap<u64, JobStatus> {
    let mut out = HashMap::new();
    for line in stdout.lines() {
        let mut fields = line.split_whitespace();
        let _queue = fields.next();
        let _user = fields.next();
        let jobid_str = match fields.next() {
            Some(s) => s,
            None => continue,
        };
        let state_str = match fields.next() {
            Some(s) => s,
            None => continue,
        };
        let Ok(jobid) = jobid_str.parse::<u64>() else {
            continue;
        };
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
}
