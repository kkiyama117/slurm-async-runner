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
//! Scope covers the exit-code query shape
//! `sacct -P -n -j <key[,key…]> -o JobID,State,Reason,ExitCode` issued by
//! `refresh_with_sacct()` — the burst source described above. Array-task
//! finalizers arrive keyed by their **master** jobid (sacct expands a
//! master-keyed query into per-task parent rows — KUDPC live-verified
//! 2026-06-12, jobid 7815414), so all N tasks of one array share a single
//! cache key: one slurmdbd query per TTL window serves the whole array,
//! even when tasks finish in different poll cycles. The shape still
//! accepts explicit `<master>_<idx>` keys (KUDPC-verified mixed `-j`
//! lists, jobids 7815400/7815401) as defense in depth and for symmetry
//! with the squeue shape, where per-task keys remain the live form.
//! Passed through untouched:
//! - the legacy 3-column listing (`-o JobID,State,Reason`) used by the
//!   one-shot `query_job_states_batch` API: not a polling path, and its
//!   output shape differs from the cached one
//! - `sbatch` / `scancel` / everything else: mutating or out of scope

use crate::sbatch::query_cache::{JobKey, QueryCacheState, QueryCachingDispatcher, QueryShape};

/// The `-o` column list of the batchable exit-code query — must stay in
/// sync with the argv built in `runner.rs`
/// (`query_job_states_with_exit_code_with`).
const EXIT_CODE_COLUMNS: &str = "JobID,State,Reason,ExitCode";

/// The batchable exit-code query
/// `sacct -P -n -j <key[,key…]> -o JobID,State,Reason,ExitCode`, as a
/// [`QueryShape`]. Keys are plain jobids and array tasks (see [`JobKey`]).
#[derive(Default)]
pub(crate) struct SacctExitCodeShape;

impl QueryShape for SacctExitCodeShape {
    type Key = JobKey;

    /// Returns the requested keys iff `argv` is exactly the batchable
    /// exit-code query. The legacy 3-column listing fails the column
    /// check; step suffixes and aggregate tokens fail the key parse.
    fn parse(&self, argv: &[String]) -> Option<Vec<JobKey>> {
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
        JobKey::parse_csv(&argv[4])
    }

    fn build(&self, batch: &[JobKey]) -> Vec<String> {
        vec![
            "sacct".into(),
            "-P".into(),
            "-n".into(),
            "-j".into(),
            JobKey::build_csv(batch),
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

    /// The load win that motivated keying array finalizers by their
    /// master jobid: tasks of one array finishing in *different* poll
    /// cycles still issue the identical master-keyed query, so every
    /// finalizer after the first replays the cached listing — one sacct
    /// spawn per TTL window for the whole array. Per-task keys would
    /// pay a fresh live re-batch for each newly-finishing task.
    #[tokio::test]
    async fn staggered_array_finalizers_share_one_master_keyed_spawn() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(argvs.clone(), Duration::from_secs(60));

        // Task 0 finishes first; its finalizer queries the master key.
        d.capture(&exit_code_argv("12345")).await.unwrap();
        // Tasks 1 and 2 finish in later poll cycles within the TTL —
        // identical argv, so both replay the shared listing.
        d.capture(&exit_code_argv("12345")).await.unwrap();
        d.capture(&exit_code_argv("12345")).await.unwrap();

        assert_eq!(
            calls(&argvs),
            1,
            "all task finalizers of one array must share one sacct spawn"
        );
        assert_eq!(jlist(&argvs, 0), "12345");
    }

    /// Explicit `<master>_<idx>` keys still enter the shared batch:
    /// KUDPC-verified (2026-06-12) that sacct accepts the mixed `-j`
    /// list and answers per-task parent rows the keyed array parser can
    /// match on. The runner's array finalizer no longer emits this form
    /// (it keys by the master — see
    /// `crate::runner::query_array_task_outcome_with`), but the shape
    /// keeps accepting it as defense in depth.
    #[tokio::test]
    async fn array_task_finalizers_join_the_shared_batch() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(argvs.clone(), Duration::from_secs(60));

        d.capture(&exit_code_argv("100")).await.unwrap();
        let out = d.capture(&exit_code_argv("123_4")).await.unwrap();
        assert_eq!(calls(&argvs), 2);
        assert_eq!(
            jlist(&argvs, 1),
            "100,123_4",
            "the array key's first sight must re-batch with the plain key"
        );
        assert!(out.stdout.contains("123_4|COMPLETED"));

        d.capture(&exit_code_argv("123_4")).await.unwrap();
        d.capture(&exit_code_argv("100")).await.unwrap();
        assert_eq!(
            calls(&argvs),
            2,
            "both key kinds must replay the shared listing within TTL"
        );
    }

    /// Subset-replay, array edition: a listing that never asked about an
    /// array key must not be replayed to it — the absent parent row would
    /// fabricate "no usable accounting row" for that task. The unseen
    /// array key must force a live re-batch.
    #[tokio::test]
    async fn cached_listing_never_replays_to_an_unqueried_array_key() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(argvs.clone(), Duration::from_secs(60));

        d.capture(&exit_code_argv("100")).await.unwrap();
        let out = d.capture(&exit_code_argv("200_7")).await.unwrap();

        assert_eq!(
            calls(&argvs),
            2,
            "an unqueried array key must go live, never read the cached listing"
        );
        assert_eq!(jlist(&argvs, 1), "100,200_7");
        assert!(out.stdout.contains("200_7|COMPLETED"));
    }

    /// The legacy 3-column listing must pass through untouched — see the
    /// module doc for why it is excluded.
    #[tokio::test]
    async fn legacy_column_queries_bypass_the_cache() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(argvs.clone(), Duration::from_secs(60));

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
            2,
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
