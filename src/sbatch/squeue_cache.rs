//! Batching single-flight TTL cache for `squeue` summary queries.
//!
//! After a `qgroup -l` miss, every single-job `SbatchJobHandle::refresh()`
//! issues `squeue -h -j <jobid> -o "%i %T %r"` — one subprocess per handle
//! per poll cycle. squeue accepts a comma-separated id list on `-j`, so N
//! handles of the same manager can share one listing per `poll_interval`,
//! exactly like the `qgroup -l` cache one layer above (see
//! [`crate::sbatch::qgroup_cache`]).
//!
//! This module is the squeue instantiation of the generic
//! [`crate::sbatch::query_cache`] (issue #16 item 3); the TTL slot,
//! single-flight locking, subset-replay rule and 2 × ttl key registry all
//! live there. What remains here is the squeue-specific argv shape.
//!
//! Live-verified on KUDPC (2026-06-12) before this was built: a multi-id
//! `squeue -j id1,id2,…` always exits 0 and prints rows for the
//! still-listed ids only, no matter how many of the queried ids are
//! already purged (even all of them). A missing row therefore carries the
//! same meaning as the single-id "empty listing" / `Invalid job id
//! specified` outcomes: the job has left the active queue. Callers
//! already treat "my id is absent from the parsed map" as that vanish
//! signal and ignore rows for other jobs (keyed lookups in
//! `SbatchJobHandle::refresh` and the `runner.rs` merge helpers), so
//! serving one shared listing to every handle preserves refresh
//! semantics bit-for-bit.
//!
//! Scope is deliberately limited to the exact summary-query argv shape:
//! array-task probes (`squeue -j <master>_<idx> -o "%T %r"`) parse a
//! single positional row and would mis-read a batched listing, and
//! `sbatch` / `scancel` are mutating calls where replaying a stale result
//! would be wrong. All of those pass through untouched (`sacct` exit-code
//! queries have their own shape — see [`crate::sbatch::sacct_cache`]).

use crate::sbatch::query_cache::{QueryCacheState, QueryCachingDispatcher, QueryShape};

/// The `-o` format of the batchable summary query — must stay in sync
/// with the argv built in `runner.rs` (`query_job_states_squeue_only_with`
/// and the two batch query functions).
const SUMMARY_FORMAT: &str = "%i %T %r";

/// The batchable summary query `squeue -h -j <u64[,u64…]> -o "%i %T %r"`,
/// as a [`QueryShape`].
#[derive(Default)]
pub(crate) struct SqueueSummaryShape;

impl QueryShape for SqueueSummaryShape {
    type Key = u64;

    /// Returns the requested jobids iff `argv` is exactly the batchable
    /// summary query. Array-task probes fail twice here (their `-j` key
    /// `<master>_<idx>` is not a u64 and their format is `%T %r`), and
    /// anything that is not squeue fails the prefix check.
    fn parse(&self, argv: &[String]) -> Option<Vec<u64>> {
        if argv.len() != 6
            || argv[0] != "squeue"
            || argv[1] != "-h"
            || argv[2] != "-j"
            || argv[4] != "-o"
            || argv[5] != SUMMARY_FORMAT
        {
            return None;
        }
        argv[3].split(',').map(|t| t.parse::<u64>().ok()).collect()
    }

    fn build(&self, batch: &[u64]) -> Vec<String> {
        let csv = batch
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        vec![
            "squeue".into(),
            "-h".into(),
            "-j".into(),
            csv,
            "-o".into(),
            SUMMARY_FORMAT.into(),
        ]
    }

    fn replay_marker(&self) -> &'static str {
        "(replayed from squeue batch cache)"
    }
}

/// Shared cache slot — one per [`crate::sbatch::manager::SbatchManager`],
/// shared by every handle that manager creates (spawn and attach alike).
pub(crate) type SqueueCacheState = QueryCacheState<SqueueSummaryShape>;

/// Dispatcher wrapper that serves squeue summary queries from a shared
/// batched TTL cache; every other argv passes through to the inner
/// dispatcher.
pub(crate) type SqueueBatchingDispatcher = QueryCachingDispatcher<SqueueSummaryShape>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcher::{CaptureOutput, JobDispatcher, into_dyn};
    use anyhow::Result;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// Inner dispatcher that records every `capture` argv and answers a
    /// summary query with one `<id> RUNNING None` row per requested id.
    /// An optional delay (real time) makes the single-flight test
    /// exercise genuine lock contention.
    struct RecordingInner {
        argvs: Arc<Mutex<Vec<Vec<String>>>>,
        fail: bool,
        delay: Duration,
    }
    impl RecordingInner {
        fn ok(argvs: Arc<Mutex<Vec<Vec<String>>>>) -> Self {
            Self {
                argvs,
                fail: false,
                delay: Duration::ZERO,
            }
        }
    }
    impl JobDispatcher for RecordingInner {
        async fn run(&self, _argv: &[String]) -> Result<i32> {
            unimplemented!()
        }
        async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
            self.argvs.lock().unwrap().push(argv.to_vec());
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            if self.fail {
                anyhow::bail!("No such file or directory (os error 2)")
            }
            let stdout = if argv.len() == 6 && argv[0] == "squeue" {
                argv[3]
                    .split(',')
                    .map(|id| format!("{id} RUNNING None\n"))
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

    fn summary_argv(csv: &str) -> Vec<String> {
        vec![
            "squeue".into(),
            "-h".into(),
            "-j".into(),
            csv.into(),
            "-o".into(),
            "%i %T %r".into(),
        ]
    }

    fn batching(inner: RecordingInner, ttl: Duration) -> SqueueBatchingDispatcher {
        SqueueBatchingDispatcher::new(into_dyn(inner), Arc::new(SqueueCacheState::new(ttl)))
    }

    fn jlist(argvs: &Arc<Mutex<Vec<Vec<String>>>>, call: usize) -> String {
        argvs.lock().unwrap()[call][3].clone()
    }

    fn calls(argvs: &Arc<Mutex<Vec<Vec<String>>>>) -> usize {
        argvs.lock().unwrap().len()
    }

    #[tokio::test]
    async fn second_query_for_same_id_within_ttl_is_replayed() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(RecordingInner::ok(argvs.clone()), Duration::from_secs(60));

        let first = d.capture(&summary_argv("100")).await.unwrap();
        let second = d.capture(&summary_argv("100")).await.unwrap();

        assert_eq!(
            calls(&argvs),
            1,
            "second query within TTL must not spawn squeue again"
        );
        assert_eq!(first, second, "cached replay must equal the live output");
    }

    /// The correctness rule: a listing that never asked about an id must
    /// not be replayed to it — the absent row would read as a vanish.
    /// The miss re-batches with every recently-seen id, after which both
    /// ids share the new listing.
    #[tokio::test]
    async fn unseen_id_forces_live_rebatch_with_union_of_recent_ids() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(RecordingInner::ok(argvs.clone()), Duration::from_secs(60));

        d.capture(&summary_argv("100")).await.unwrap();
        let out = d.capture(&summary_argv("200")).await.unwrap();

        assert_eq!(calls(&argvs), 2, "an unseen id must force a live query");
        assert_eq!(
            jlist(&argvs, 1),
            "100,200",
            "the re-batched query must cover both the cached and the new id"
        );
        assert!(
            out.stdout.contains("200 RUNNING"),
            "the new id's row must be present, got: {}",
            out.stdout
        );

        d.capture(&summary_argv("100")).await.unwrap();
        d.capture(&summary_argv("200")).await.unwrap();
        assert_eq!(
            calls(&argvs),
            2,
            "both ids must replay from the shared listing afterwards"
        );
    }

    #[tokio::test]
    async fn multi_id_subset_of_cached_batch_is_replayed() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(RecordingInner::ok(argvs.clone()), Duration::from_secs(60));

        d.capture(&summary_argv("100,200")).await.unwrap();
        d.capture(&summary_argv("200")).await.unwrap();
        d.capture(&summary_argv("100,200")).await.unwrap();

        assert_eq!(
            calls(&argvs),
            1,
            "subset queries must be served from the cached batch"
        );
    }

    #[tokio::test]
    async fn non_summary_argv_bypasses_cache_entirely() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(RecordingInner::ok(argvs.clone()), Duration::from_secs(60));

        // Array-task probe: `<master>_<idx>` key + `%T %r` format.
        let array: Vec<String> = vec![
            "squeue".into(),
            "-h".into(),
            "-j".into(),
            "123_4".into(),
            "-o".into(),
            "%T %r".into(),
        ];
        d.capture(&array).await.unwrap();
        d.capture(&array).await.unwrap();
        assert_eq!(
            calls(&argvs),
            2,
            "array-task probes must never be coalesced or cached"
        );

        let sacct: Vec<String> = vec![
            "sacct".into(),
            "-P".into(),
            "-n".into(),
            "-j".into(),
            "100".into(),
            "-o".into(),
            "JobID,State,Reason".into(),
        ];
        d.capture(&sacct).await.unwrap();
        d.capture(&sacct).await.unwrap();
        assert_eq!(calls(&argvs), 4, "sacct must never enter the squeue cache");
    }

    #[tokio::test]
    async fn ttl_zero_disables_caching_and_batches_only_the_requested_ids() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(RecordingInner::ok(argvs.clone()), Duration::ZERO);

        d.capture(&summary_argv("100")).await.unwrap();
        d.capture(&summary_argv("100")).await.unwrap();
        d.capture(&summary_argv("200")).await.unwrap();

        assert_eq!(calls(&argvs), 3, "TTL zero must disable caching");
        assert_eq!(jlist(&argvs, 1), "100");
        assert_eq!(
            jlist(&argvs, 2),
            "200",
            "TTL zero must also disable batching (zero registry window)"
        );
    }

    #[tokio::test]
    async fn spawn_failure_is_cached_and_replayed_as_error() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(
            RecordingInner {
                argvs: argvs.clone(),
                fail: true,
                delay: Duration::ZERO,
            },
            Duration::from_secs(60),
        );

        let first = d.capture(&summary_argv("100")).await.unwrap_err();
        let second = d.capture(&summary_argv("100")).await.unwrap_err();

        assert_eq!(
            calls(&argvs),
            1,
            "a failing squeue must not be respawned within TTL"
        );
        assert!(first.to_string().contains("No such file or directory"));
        assert!(
            second
                .to_string()
                .contains("(replayed from squeue batch cache)"),
            "replayed error must be marked as a replay, got: {second:#}"
        );
    }

    /// The 50 ms inner delay guarantees the second `join!` branch reaches
    /// the slot lock while the winner still holds it inside
    /// `inner.capture` — i.e. the test exercises real lock contention,
    /// not accidental sequential execution.
    #[tokio::test]
    async fn concurrent_same_id_queries_are_single_flight() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = batching(
            RecordingInner {
                argvs: argvs.clone(),
                fail: false,
                delay: Duration::from_millis(50),
            },
            Duration::from_secs(60),
        );
        let argv = summary_argv("100");

        let (a, b) = tokio::join!(d.capture(&argv), d.capture(&argv));

        assert_eq!(
            calls(&argvs),
            1,
            "concurrent queries must coalesce into one squeue spawn"
        );
        assert_eq!(a.unwrap(), b.unwrap());
    }

    /// Paused-clock test: an id whose handle stopped polling (terminal)
    /// must fall out of the batch after 2 × ttl so the `-j` list does not
    /// grow without bound in long-lived managers.
    #[tokio::test(start_paused = true)]
    async fn ids_not_requested_for_two_ttls_age_out_of_the_batch() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let ttl = Duration::from_secs(60);
        let d = batching(RecordingInner::ok(argvs.clone()), ttl);

        d.capture(&summary_argv("100")).await.unwrap();

        // Entry expired (61 s > ttl) but id 100 is still inside the 120 s
        // registry window — the re-batch must keep it.
        tokio::time::advance(Duration::from_secs(61)).await;
        d.capture(&summary_argv("200")).await.unwrap();
        assert_eq!(jlist(&argvs, 1), "100,200");

        // Now 100 was last requested 182 s ago (> 120 s) — pruned.
        tokio::time::advance(Duration::from_secs(121)).await;
        d.capture(&summary_argv("200")).await.unwrap();
        assert_eq!(
            jlist(&argvs, 2),
            "200",
            "an id beyond the 2 × ttl window must leave the batch"
        );
    }

    /// Regression guard for the subset-replay rule against the sharpest
    /// edge: a cached *single-id* vanish outcome (rc = 1 `Invalid job id
    /// specified`, the live-verified shape for a fully-purged lone id)
    /// must never be replayed to a different jobid — that would fabricate
    /// the vanish signal for a job squeue was never asked about.
    #[tokio::test]
    async fn cached_vanish_for_one_id_never_replays_to_another() {
        /// First capture answers with the single-id purged shape; every
        /// later capture answers rc = 0 with a RUNNING row for id 200.
        struct VanishThenRunning {
            argvs: Arc<Mutex<Vec<Vec<String>>>>,
        }
        impl JobDispatcher for VanishThenRunning {
            async fn run(&self, _argv: &[String]) -> Result<i32> {
                unimplemented!()
            }
            async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
                let mut argvs = self.argvs.lock().unwrap();
                argvs.push(argv.to_vec());
                if argvs.len() == 1 {
                    Ok(CaptureOutput {
                        exit_code: 1,
                        stdout: String::new(),
                        stderr: "slurm_load_jobs error: Invalid job id specified\n".into(),
                    })
                } else {
                    Ok(CaptureOutput {
                        exit_code: 0,
                        stdout: "200 RUNNING None\n".into(),
                        stderr: String::new(),
                    })
                }
            }
        }

        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = SqueueBatchingDispatcher::new(
            into_dyn(VanishThenRunning {
                argvs: argvs.clone(),
            }),
            Arc::new(SqueueCacheState::new(Duration::from_secs(60))),
        );

        // id 100 is fully purged — its rc = 1 vanish outcome gets cached.
        let vanish = d.capture(&summary_argv("100")).await.unwrap();
        assert_eq!(vanish.exit_code, 1);

        // id 200 must NOT be served that cached vanish: the subset rule
        // forces a live re-batch covering both ids, and 200 sees its own
        // RUNNING row.
        let out = d.capture(&summary_argv("200")).await.unwrap();
        assert_eq!(
            calls(&argvs),
            2,
            "id 200 must go live, never replay id 100's vanish outcome"
        );
        assert_eq!(jlist(&argvs, 1), "100,200");
        assert_eq!(out.exit_code, 0);
        assert!(
            out.stdout.contains("200 RUNNING"),
            "id 200 must observe its own row, got: {}",
            out.stdout
        );
    }
}
