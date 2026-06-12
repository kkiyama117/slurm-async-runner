//! Batching single-flight TTL cache for `sacct` exit-code queries.
//!
//! `sacct` is the heaviest of the three status commands this crate
//! spawns: unlike `squeue` / `qgroup` (controller / sampled-listing
//! reads), every sacct invocation queries the slurmdbd accounting
//! database. The KUDPC manual asks users to refrain from repeating status
//! commands mechanically because it overloads the system — and sacct is
//! exactly where the load spikes, because handles call
//! `refresh_with_sacct()` when their job *finishes*, and batch workloads
//! finish in bursts: N jobs submitted together reach their finalizer
//! within the same few poll cycles, spawning N near-simultaneous sacct
//! processes. Accounting flush lag makes it worse: a job whose row has
//! not reached sacct yet resolves nothing, so callers retry the finalizer
//! and spawn sacct again.
//!
//! This module is the sacct instantiation of the generic
//! [`crate::sbatch::query_cache`] (issue #16 item 3) — the third use case
//! that motivated extracting the primitive. sacct accepts a
//! comma-separated id list on `-j` (the multi-id form is already
//! exercised by `runner.rs::query_job_states_batch_with`), so N finishing
//! handles of one manager collapse to one slurmdbd query per
//! `poll_interval`, exactly like the squeue summary batching one layer
//! up. Replay semantics are preserved bit-for-bit by the same argument
//! as the squeue cache: callers do keyed lookups into the parsed map
//! (`map.get(&jobid)`), extra rows for other batched jobs are ignored,
//! and a missing row already means "no usable accounting row yet" —
//! exactly what the subset-replay rule guarantees it can only mean
//! (a key is never served a listing that did not ask about it).
//!
//! Scope is deliberately limited to the exit-code query shape
//! `sacct -P -n -j <u64[,u64…]> -o JobID,State,Reason,ExitCode` issued by
//! `refresh_with_sacct()`'s single-job path — the burst source described
//! above. Passed through untouched:
//! - array-task finalizers (`-j <master>_<idx>`): their key is not a
//!   `u64`, and mixing array keys into a batched `-j` list is unverified
//!   on KUDPC (the squeue batching was only built after live
//!   verification; hold array-key batching to the same bar)
//! - the legacy 3-column listing (`-o JobID,State,Reason`) used by the
//!   one-shot `query_job_states_batch` API: not a polling path, and its
//!   output shape differs from the cached one
//! - `sbatch` / `scancel` / everything else: mutating or out of scope

use crate::sbatch::query_cache::{QueryCacheState, QueryCachingDispatcher, QueryShape};

/// The `-o` column list of the batchable exit-code query — must stay in
/// sync with the argv built in `runner.rs`
/// (`query_job_states_with_exit_code_with`).
const EXIT_CODE_COLUMNS: &str = "JobID,State,Reason,ExitCode";

/// The batchable exit-code query
/// `sacct -P -n -j <u64[,u64…]> -o JobID,State,Reason,ExitCode`, as a
/// [`QueryShape`].
#[derive(Default)]
pub(crate) struct SacctExitCodeShape;

impl QueryShape for SacctExitCodeShape {
    type Key = u64;

    /// Returns the requested jobids iff `argv` is exactly the batchable
    /// exit-code query. Array-task finalizers fail the key parse (their
    /// `-j` key `<master>_<idx>` is not a u64) and the legacy 3-column
    /// listing fails the column check.
    fn parse(&self, argv: &[String]) -> Option<Vec<u64>> {
        if argv.len() != 7
            || argv[0] != "sacct"
            || argv[1] != "-P"
            || argv[2] != "-n"
            || argv[3] != "-j"
            || argv[5] != "-o"
            || argv[6] != EXIT_CODE_COLUMNS
        {
            return None;
        }
        argv[4].split(',').map(|t| t.parse::<u64>().ok()).collect()
    }

    fn build(&self, batch: &[u64]) -> Vec<String> {
        let csv = batch
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        vec![
            "sacct".into(),
            "-P".into(),
            "-n".into(),
            "-j".into(),
            csv,
            "-o".into(),
            EXIT_CODE_COLUMNS.into(),
        ]
    }

    fn replay_marker(&self) -> &'static str {
        "(replayed from sacct batch cache)"
    }
}

/// Shared cache slot — one per [`crate::sbatch::manager::SbatchManager`],
/// shared by every handle that manager creates (spawn and attach alike).
pub(crate) type SacctCacheState = QueryCacheState<SacctExitCodeShape>;

/// Dispatcher wrapper that serves sacct exit-code queries from a shared
/// batched TTL cache; every other argv passes through to the inner
/// dispatcher.
pub(crate) type SacctBatchingDispatcher = QueryCachingDispatcher<SacctExitCodeShape>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcher::{CaptureOutput, JobDispatcher, into_dyn};
    use anyhow::Result;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// Inner dispatcher that records every `capture` argv and answers an
    /// exit-code query with a parent row plus `.batch` / `.extern` step
    /// rows per requested id — the realistic sacct output shape.
    struct RecordingInner {
        argvs: Arc<Mutex<Vec<Vec<String>>>>,
    }
    impl JobDispatcher for RecordingInner {
        async fn run(&self, _argv: &[String]) -> Result<i32> {
            unimplemented!()
        }
        async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
            self.argvs.lock().unwrap().push(argv.to_vec());
            let stdout = if argv.len() == 7 && argv[0] == "sacct" {
                argv[4]
                    .split(',')
                    .map(|id| {
                        format!(
                            "{id}|COMPLETED|None|0:0\n{id}.batch|COMPLETED||0:0\n{id}.extern|COMPLETED||0:0\n"
                        )
                    })
                    .collect()
            } else {
                String::new()
            };
            Ok(CaptureOutput {
                exit_code: 0,
                stdout,
                stderr: String::new(),
            })
        }
    }

    fn exit_code_argv(csv: &str) -> Vec<String> {
        vec![
            "sacct".into(),
            "-P".into(),
            "-n".into(),
            "-j".into(),
            csv.into(),
            "-o".into(),
            "JobID,State,Reason,ExitCode".into(),
        ]
    }

    fn batching(argvs: Arc<Mutex<Vec<Vec<String>>>>, ttl: Duration) -> SacctBatchingDispatcher {
        SacctBatchingDispatcher::new(
            into_dyn(RecordingInner { argvs }),
            Arc::new(SacctCacheState::new(ttl)),
        )
    }

    fn jlist(argvs: &Arc<Mutex<Vec<Vec<String>>>>, call: usize) -> String {
        argvs.lock().unwrap()[call][4].clone()
    }

    fn calls(argvs: &Arc<Mutex<Vec<Vec<String>>>>) -> usize {
        argvs.lock().unwrap().len()
    }

    /// The burst scenario this cache exists for: N handles finishing
    /// together each issue their single-id finalizer query, and all but
    /// the first are served from the shared batched listing.
    #[tokio::test]
    async fn finishing_burst_coalesces_to_one_sacct_spawn() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(argvs.clone(), Duration::from_secs(60));

        d.capture(&exit_code_argv("100")).await.unwrap();
        let out = d.capture(&exit_code_argv("200")).await.unwrap();
        // 200's first sight forces a live re-batch covering both ids…
        assert_eq!(calls(&argvs), 2);
        assert_eq!(jlist(&argvs, 1), "100,200");
        assert!(out.stdout.contains("200|COMPLETED"));

        // …after which every handle replays the shared listing.
        d.capture(&exit_code_argv("100")).await.unwrap();
        d.capture(&exit_code_argv("200")).await.unwrap();
        d.capture(&exit_code_argv("100,200")).await.unwrap();
        assert_eq!(
            calls(&argvs),
            2,
            "all subset queries within TTL must replay the batched listing"
        );
    }

    /// Accounting-lag retries: the same handle re-running its finalizer
    /// within TTL must not respawn sacct — it sees the same listing and
    /// retries live only after the TTL window.
    #[tokio::test]
    async fn retry_within_ttl_is_replayed_not_respawned() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(argvs.clone(), Duration::from_secs(60));

        let first = d.capture(&exit_code_argv("100")).await.unwrap();
        let second = d.capture(&exit_code_argv("100")).await.unwrap();

        assert_eq!(
            calls(&argvs),
            1,
            "a lag retry within TTL must not hit sacct"
        );
        assert_eq!(first, second);
    }

    /// Array-task finalizers and the legacy 3-column listing must pass
    /// through untouched — see the module doc for why each is excluded.
    #[tokio::test]
    async fn array_task_and_legacy_column_queries_bypass_the_cache() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(argvs.clone(), Duration::from_secs(60));

        let array: Vec<String> = vec![
            "sacct".into(),
            "-P".into(),
            "-n".into(),
            "-j".into(),
            "123_4".into(),
            "-o".into(),
            "JobID,State,Reason,ExitCode".into(),
        ];
        d.capture(&array).await.unwrap();
        d.capture(&array).await.unwrap();
        assert_eq!(
            calls(&argvs),
            2,
            "array-task sacct queries must never be coalesced or cached"
        );
        assert_eq!(
            jlist(&argvs, 0),
            "123_4",
            "the array key must reach sacct verbatim, never merged into a batch"
        );

        let legacy: Vec<String> = vec![
            "sacct".into(),
            "-P".into(),
            "-n".into(),
            "-j".into(),
            "100".into(),
            "-o".into(),
            "JobID,State,Reason".into(),
        ];
        d.capture(&legacy).await.unwrap();
        d.capture(&legacy).await.unwrap();
        assert_eq!(
            calls(&argvs),
            4,
            "the 3-column listing must never enter the exit-code cache"
        );
    }

    #[tokio::test]
    async fn ttl_zero_disables_caching_and_batching() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(argvs.clone(), Duration::ZERO);

        d.capture(&exit_code_argv("100")).await.unwrap();
        d.capture(&exit_code_argv("100")).await.unwrap();
        d.capture(&exit_code_argv("200")).await.unwrap();

        assert_eq!(calls(&argvs), 3, "TTL zero must disable caching");
        assert_eq!(
            jlist(&argvs, 2),
            "200",
            "TTL zero must also disable batching (zero registry window)"
        );
    }

    /// The subset-replay rule, sacct edition: a listing that never asked
    /// about an id must not be replayed to it — the absent row would read
    /// as "no usable accounting row" and (correctly) leave `finished`
    /// unset, but it would do so from fabricated evidence. The unseen id
    /// must force a live re-batch.
    #[tokio::test]
    async fn cached_listing_never_replays_to_an_unqueried_id() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(argvs.clone(), Duration::from_secs(60));

        d.capture(&exit_code_argv("100")).await.unwrap();
        let out = d.capture(&exit_code_argv("300")).await.unwrap();

        assert_eq!(
            calls(&argvs),
            2,
            "an unqueried id must go live, never read the cached listing"
        );
        assert_eq!(jlist(&argvs, 1), "100,300");
        assert!(
            out.stdout.contains("300|COMPLETED"),
            "id 300 must observe its own row, got: {}",
            out.stdout
        );
    }
}
