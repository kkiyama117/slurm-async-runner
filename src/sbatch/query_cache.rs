//! Generic single-flight TTL batch cache for SLURM status queries
//! (issue #16 item 3).
//!
//! Three read-only status commands share the same load problem: N handles
//! of one manager polling concurrently spawn N identical (or batchable)
//! subprocesses per poll cycle, and the KUDPC manual explicitly asks users
//! to "refrain from repeating the command mechanically because it
//! overloads the system". The same single-flight TTL-cache algorithm
//! collapses that to one spawn per `poll_interval` per manager; this
//! module is that algorithm, factored out of the two original
//! implementations once the third use case arrived:
//!
//! - [`crate::sbatch::qgroup_cache`] — `qgroup -l` (global listing, the
//!   degenerate zero-key shape)
//! - [`crate::sbatch::squeue_cache`] — `squeue -h -j <ids> -o "%i %T %r"`
//! - [`crate::sbatch::sacct_cache`] — `sacct -P -n -j <ids> -o
//!   JobID,State,Reason,ExitCode`
//!
//! A [`QueryShape`] describes one batchable argv form: how to recognize it
//! and extract the queried keys, how to rebuild the argv for a merged
//! batch, and how to mark replayed errors. Everything else — TTL slot,
//! single-flight locking, the subset-replay rule, the self-aging key
//! registry — is shared here and tested once.
//!
//! Correctness rule (inherited from the squeue cache, where it was
//! live-verified on KUDPC 2026-06-12): a cached listing may only be
//! replayed to a request whose keys are a **subset** of the keys the
//! cached query asked for. Replaying it to a key the batch never queried
//! would fabricate the "no row = job vanished / no usable row" signal for
//! that key. A new key's first query therefore always goes live —
//! re-batched with every recently-seen key — and joins the shared listing
//! from the next cycle on.
//!
//! The key registry is self-maintaining: each intercepted request stamps
//! its keys, and keys not requested for 2 × ttl are dropped from future
//! batches. Handles stop polling once terminal, so their keys age out on
//! their own.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::Instant;

use crate::dispatcher::{CaptureOutput, DynJobDispatcher, JobDispatcher};

/// One batchable argv form. Implementations are zero-sized markers; the
/// `Default` bound lets [`QueryCacheState::new`] construct them without a
/// second parameter.
pub(crate) trait QueryShape: Default + Send + Sync + 'static {
    /// The unit of batching — a jobid for squeue/sacct, unused (`u64`)
    /// for the keyless qgroup listing.
    type Key: Eq + Hash + Ord + Clone + Send + Sync;

    /// `Some(keys)` iff `argv` is exactly this shape's batchable query.
    /// Anything else (other commands, other option/format variants,
    /// non-parseable keys) must return `None` and passes through to the
    /// inner dispatcher untouched.
    fn parse(&self, argv: &[String]) -> Option<Vec<Self::Key>>;

    /// Rebuild the query argv for a sorted, deduped batch of keys.
    fn build(&self, batch: &[Self::Key]) -> Vec<String>;

    /// Suffix appended to replayed error messages so an incident log can
    /// tell "the command spawned and failed just now" apart from "a
    /// failure up to TTL ago is being replayed".
    fn replay_marker(&self) -> &'static str;
}

/// Shared cache slot — one per [`crate::sbatch::manager::SbatchManager`],
/// shared by every handle that manager creates (spawn and attach alike).
pub(crate) struct QueryCacheState<S: QueryShape> {
    shape: S,
    ttl: Duration,
    slot: TokioMutex<Slot<S::Key>>,
}

struct Slot<K> {
    entry: Option<CachedEntry<K>>,
    /// key → last time some handle asked for it. This is the
    /// self-maintaining batch registry: keys older than 2 × ttl are
    /// pruned when the next live query is assembled.
    recent: HashMap<K, Instant>,
}

// Manual impl: `#[derive(Default)]` would add a spurious `K: Default`
// bound.
impl<K> Default for Slot<K> {
    fn default() -> Self {
        Self {
            entry: None,
            recent: HashMap::new(),
        }
    }
}

struct CachedEntry<K> {
    at: Instant,
    /// The keys the cached query actually asked about. Replay is only
    /// allowed for requests whose keys are a subset of this — see the
    /// module-level correctness rule.
    keys: HashSet<K>,
    /// `Ok`: raw output, any exit code (a nonzero exit is replayed as-is
    /// so caller-side classification — squeue vanish, qgroup fallback —
    /// fires identically for every handle). `Err`: spawn failure message
    /// — replayed as an error so every handle observes the same outage
    /// within one poll cycle instead of hammering a struggling
    /// controller.
    result: Result<CaptureOutput, String>,
}

impl<S: QueryShape> QueryCacheState<S> {
    /// `ttl == Duration::ZERO` effectively disables both caching and
    /// batching: every entry is already expired when read back, and the
    /// 2 × ttl registry window is also zero so each live query asks for
    /// exactly the requested keys — byte-for-byte the uncached behaviour.
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            shape: S::default(),
            ttl,
            slot: TokioMutex::new(Slot::default()),
        }
    }
}

/// Dispatcher wrapper that serves one [`QueryShape`]'s queries from a
/// shared batched TTL cache; every other argv passes through to `inner`.
pub(crate) struct QueryCachingDispatcher<S: QueryShape> {
    inner: Arc<dyn DynJobDispatcher>,
    cache: Arc<QueryCacheState<S>>,
}

impl<S: QueryShape> QueryCachingDispatcher<S> {
    /// The only construction path — keeps every wrapper tied to a cache
    /// that is actually shared (the manager's), never an ad-hoc one.
    pub(crate) fn new(inner: Arc<dyn DynJobDispatcher>, cache: Arc<QueryCacheState<S>>) -> Self {
        Self { inner, cache }
    }
}

fn replay(result: &Result<CaptureOutput, String>, marker: &str) -> Result<CaptureOutput> {
    match result {
        Ok(out) => Ok(out.clone()),
        Err(msg) => Err(anyhow::anyhow!("{msg} {marker}")),
    }
}

impl<S: QueryShape> JobDispatcher for QueryCachingDispatcher<S> {
    async fn run(&self, argv: &[String]) -> Result<i32> {
        self.inner.run(argv).await
    }

    async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
        let Some(requested) = self.cache.shape.parse(argv) else {
            return self.inner.capture(argv).await;
        };
        // The slot lock is held across the inner call on purpose:
        // concurrent queries queue here and the losers are served from
        // the entry the winner just stored (single-flight).
        //
        // Cancellation: this is NOT cancellation-safe in the
        // single-flight sense. If the winner's future is dropped
        // mid-`inner.capture` (`tokio::time::timeout`, `select!`, Python
        // `asyncio.wait_for`), tokio's Mutex releases the lock on drop,
        // the slot stays unwritten, and the next waiter simply issues a
        // fresh live query. Worst case under repeated cancellation is one
        // spawn per attempt — i.e. graceful degradation to the uncached
        // behaviour, never a deadlock and never a stale-entry replay.
        let mut slot = self.cache.slot.lock().await;
        let now = Instant::now();
        for key in &requested {
            slot.recent.insert(key.clone(), now);
        }
        if let Some(entry) = slot.entry.as_ref()
            && entry.at.elapsed() < self.cache.ttl
            && requested.iter().all(|key| entry.keys.contains(key))
        {
            return replay(&entry.result, self.cache.shape.replay_marker());
        }
        // 2 × ttl, not 1 ×: an actively-polling handle re-stamps its key
        // once per ttl, so a 1 × window would race poll jitter right at
        // the boundary and keep evicting live subscribers (forcing a
        // warm-up miss every cycle). Two missed polls means the handle
        // stopped polling — terminal or dropped — and its key can go.
        let window = self.cache.ttl.saturating_mul(2);
        slot.recent.retain(|_, at| at.elapsed() < window);
        let mut batch: Vec<S::Key> = slot.recent.keys().cloned().collect();
        // With ttl == ZERO the retain above empties the registry, so the
        // requested keys must be re-added unconditionally. This is also
        // what keeps `build()` from ever seeing an empty batch for keyed
        // shapes: `requested` is only empty for the zero-key qgroup
        // shape, whose `build()` ignores its argument.
        batch.extend(requested.iter().cloned());
        batch.sort_unstable();
        batch.dedup();
        let fetched = self.inner.capture(&self.cache.shape.build(&batch)).await;
        slot.entry = Some(CachedEntry {
            at: Instant::now(),
            keys: batch.into_iter().collect(),
            result: match &fetched {
                Ok(out) => Ok(out.clone()),
                // anyhow::Error is not Clone; keep the full context chain
                // as a string for replay.
                Err(e) => Err(format!("{e:#}")),
            },
        });
        fetched
    }
}

/// Generic-layer tests against a purpose-built shape, pinning the core
/// mechanics independently of the three production shapes (whose test
/// modules cover their argv specifics and end-to-end semantics). A
/// fourth shape gets this regression net for free.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcher::into_dyn;
    use std::sync::Mutex;

    /// Minimal keyed shape: `test -k <u64[,u64…]>`.
    #[derive(Default)]
    struct TestShape;
    impl QueryShape for TestShape {
        type Key = u64;
        fn parse(&self, argv: &[String]) -> Option<Vec<u64>> {
            if argv.len() != 3 || argv[0] != "test" || argv[1] != "-k" {
                return None;
            }
            argv[2].split(',').map(|t| t.parse::<u64>().ok()).collect()
        }
        fn build(&self, batch: &[u64]) -> Vec<String> {
            let csv = batch
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            vec!["test".into(), "-k".into(), csv]
        }
        fn replay_marker(&self) -> &'static str {
            "(replayed from test cache)"
        }
    }

    struct RecordingInner {
        argvs: Arc<Mutex<Vec<Vec<String>>>>,
        fail: bool,
    }
    impl JobDispatcher for RecordingInner {
        async fn run(&self, _argv: &[String]) -> Result<i32> {
            unimplemented!()
        }
        async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
            self.argvs.lock().unwrap().push(argv.to_vec());
            if self.fail {
                anyhow::bail!("inner boom")
            }
            Ok(CaptureOutput {
                exit_code: 0,
                stdout: argv.join(" "),
                stderr: String::new(),
            })
        }
    }

    fn cached(
        argvs: Arc<Mutex<Vec<Vec<String>>>>,
        fail: bool,
        ttl: Duration,
    ) -> QueryCachingDispatcher<TestShape> {
        QueryCachingDispatcher::new(
            into_dyn(RecordingInner { argvs, fail }),
            Arc::new(QueryCacheState::new(ttl)),
        )
    }

    fn argv(csv: &str) -> Vec<String> {
        vec!["test".into(), "-k".into(), csv.into()]
    }

    fn calls(argvs: &Arc<Mutex<Vec<Vec<String>>>>) -> usize {
        argvs.lock().unwrap().len()
    }

    #[tokio::test]
    async fn subset_replays_and_superset_goes_live_rebatched() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = cached(argvs.clone(), false, Duration::from_secs(60));

        d.capture(&argv("1,2")).await.unwrap();
        d.capture(&argv("2")).await.unwrap();
        assert_eq!(calls(&argvs), 1, "subset must replay the cached entry");

        d.capture(&argv("3")).await.unwrap();
        assert_eq!(calls(&argvs), 2, "an unseen key must go live");
        assert_eq!(
            argvs.lock().unwrap()[1][2],
            "1,2,3",
            "the live re-batch must union the registry with the request"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expired_entry_misses_and_registry_ages_out_after_two_ttls() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let ttl = Duration::from_secs(60);
        let d = cached(argvs.clone(), false, ttl);

        d.capture(&argv("1")).await.unwrap();
        tokio::time::advance(Duration::from_secs(61)).await;
        d.capture(&argv("2")).await.unwrap();
        assert_eq!(
            argvs.lock().unwrap()[1][2],
            "1,2",
            "a key inside the 2 x ttl window must stay in the batch"
        );

        tokio::time::advance(Duration::from_secs(121)).await;
        d.capture(&argv("2")).await.unwrap();
        assert_eq!(
            argvs.lock().unwrap()[2][2],
            "2",
            "a key idle beyond 2 x ttl must leave the batch"
        );
    }

    #[tokio::test]
    async fn ttl_zero_disables_caching_and_batching() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = cached(argvs.clone(), false, Duration::ZERO);

        d.capture(&argv("1")).await.unwrap();
        d.capture(&argv("1")).await.unwrap();
        d.capture(&argv("2")).await.unwrap();

        assert_eq!(calls(&argvs), 3);
        assert_eq!(argvs.lock().unwrap()[2][2], "2");
    }

    #[tokio::test]
    async fn replayed_error_carries_the_shape_marker() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = cached(argvs.clone(), true, Duration::from_secs(60));

        let first = d.capture(&argv("1")).await.unwrap_err();
        let second = d.capture(&argv("1")).await.unwrap_err();

        assert_eq!(calls(&argvs), 1, "a cached failure must not respawn");
        assert!(!first.to_string().contains("replayed from test cache"));
        assert!(second.to_string().contains("(replayed from test cache)"));
    }

    #[tokio::test]
    async fn non_matching_argv_passes_through_uncached() {
        let argvs = Arc::new(Mutex::new(Vec::new()));
        let d = cached(argvs.clone(), false, Duration::from_secs(60));
        let other: Vec<String> = vec!["other".into(), "-k".into(), "1".into()];

        d.capture(&other).await.unwrap();
        d.capture(&other).await.unwrap();

        assert_eq!(calls(&argvs), 2, "foreign argv must never be cached");
        assert_eq!(argvs.lock().unwrap()[0], other, "argv must pass verbatim");
    }
}
