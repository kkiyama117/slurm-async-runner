//! Single-flight TTL cache for the KUDPC `qgroup -l` listing.
//!
//! Every single-job `SbatchJobHandle::refresh()` starts with a `qgroup -l`
//! probe. The listing is global (no jobid argument), so N handles polling
//! concurrently spawn N identical subprocesses per poll cycle — 100 handles
//! at a 5 s interval is 72 000 spawns/hour. Layering this cache into the
//! dispatcher handed to handles collapses that to one spawn per
//! `poll_interval` shared by every handle of the same `SbatchManager`,
//! without touching the refresh logic itself.
//!
//! This is the degenerate zero-key instantiation of the generic
//! [`crate::sbatch::query_cache`] (issue #16 item 3): the listing takes no
//! jobid argument, so [`QueryShape::parse`] returns an empty key vector,
//! the subset-replay rule is trivially satisfied by any unexpired entry,
//! and the key registry stays empty. `squeue` summary queries and `sacct`
//! exit-code queries are handled by their own shapes (see
//! [`crate::sbatch::squeue_cache`] / [`crate::sbatch::sacct_cache`]);
//! `sbatch` / `scancel` are mutating calls where replaying a stale result
//! would be wrong, and pass through untouched.
//!
//! Staleness bound: a cached listing is served for at most `ttl`
//! (= the manager's `poll_interval`), which matches the latency callers
//! already accept by polling at that interval. KUDPC's qgroup data is
//! itself sampled (~30 s), so the cache adds no new order of staleness.

use crate::sbatch::query_cache::{QueryCacheState, QueryCachingDispatcher, QueryShape};

/// The exact `qgroup -l` listing argv, as a zero-key [`QueryShape`].
#[derive(Default)]
pub(crate) struct QgroupListingShape;

impl QueryShape for QgroupListingShape {
    /// Unused — the listing is global. `u64` keeps the generic machinery
    /// happy with a zero-cost placeholder.
    type Key = u64;

    fn parse(&self, argv: &[String]) -> Option<Vec<u64>> {
        (argv.len() == 2 && argv[0] == "qgroup" && argv[1] == "-l").then(Vec::new)
    }

    fn build(&self, _batch: &[u64]) -> Vec<String> {
        vec!["qgroup".into(), "-l".into()]
    }

    fn replay_marker(&self) -> &'static str {
        "(replayed from qgroup cache)"
    }
}

/// Shared cache slot — one per [`crate::sbatch::manager::SbatchManager`],
/// shared by every handle that manager creates (spawn and attach alike).
pub(crate) type QgroupCacheState = QueryCacheState<QgroupListingShape>;

/// Dispatcher wrapper that serves exactly `qgroup -l` from the shared TTL
/// cache; every other argv passes through to the inner dispatcher.
pub(crate) type QgroupCachingDispatcher = QueryCachingDispatcher<QgroupListingShape>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcher::{CaptureOutput, JobDispatcher, into_dyn};
    use anyhow::Result;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Inner dispatcher that counts `capture` calls and returns a canned
    /// success or a spawn-style error. An optional delay (real time)
    /// makes the single-flight test deterministic.
    struct CountingInner {
        calls: Arc<AtomicUsize>,
        fail: bool,
        delay: Duration,
    }
    impl CountingInner {
        fn ok(calls: Arc<AtomicUsize>) -> Self {
            Self {
                calls,
                fail: false,
                delay: Duration::ZERO,
            }
        }
    }
    impl JobDispatcher for CountingInner {
        async fn run(&self, _argv: &[String]) -> Result<i32> {
            unimplemented!()
        }
        async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            if self.fail {
                anyhow::bail!("No such file or directory (os error 2)")
            }
            Ok(CaptureOutput {
                exit_code: 0,
                stdout: format!("queue user 101 RUN 1 ({})\n", argv.join(" ")),
                stderr: String::new(),
            })
        }
    }

    fn qgroup_argv() -> Vec<String> {
        vec!["qgroup".into(), "-l".into()]
    }

    fn cached(inner: CountingInner, ttl: Duration) -> QgroupCachingDispatcher {
        QgroupCachingDispatcher::new(into_dyn(inner), Arc::new(QgroupCacheState::new(ttl)))
    }

    #[tokio::test]
    async fn second_qgroup_capture_within_ttl_is_served_from_cache() {
        let calls = Arc::new(AtomicUsize::new(0));
        let d = cached(CountingInner::ok(calls.clone()), Duration::from_secs(60));
        let argv = qgroup_argv();

        let first = d.capture(&argv).await.unwrap();
        let second = d.capture(&argv).await.unwrap();

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "second capture within TTL must not spawn qgroup again"
        );
        assert_eq!(first, second, "cached replay must equal the live output");
    }

    #[tokio::test]
    async fn non_qgroup_argv_bypasses_cache_entirely() {
        let calls = Arc::new(AtomicUsize::new(0));
        let d = cached(CountingInner::ok(calls.clone()), Duration::from_secs(60));
        let squeue: Vec<String> = vec!["squeue".into(), "-h".into(), "-j".into(), "1".into()];

        d.capture(&squeue).await.unwrap();
        d.capture(&squeue).await.unwrap();

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "squeue must never be coalesced or cached"
        );
    }

    #[tokio::test]
    async fn expired_ttl_refetches_from_inner() {
        let calls = Arc::new(AtomicUsize::new(0));
        // TTL zero == every entry is already expired when read back.
        let d = cached(CountingInner::ok(calls.clone()), Duration::ZERO);
        let argv = qgroup_argv();

        d.capture(&argv).await.unwrap();
        d.capture(&argv).await.unwrap();

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "an expired entry must trigger a live refetch"
        );
    }

    #[tokio::test]
    async fn spawn_failure_is_cached_and_replayed_as_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let d = cached(
            CountingInner {
                calls: calls.clone(),
                fail: true,
                delay: Duration::ZERO,
            },
            Duration::from_secs(60),
        );
        let argv = qgroup_argv();

        let first = d.capture(&argv).await.unwrap_err();
        let second = d.capture(&argv).await.unwrap_err();

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a failing qgroup binary must not be respawned within TTL"
        );
        assert!(first.to_string().contains("No such file or directory"));
        assert!(
            second.to_string().contains("No such file or directory"),
            "replayed error must carry the original message, got: {second:#}"
        );
    }

    /// The 50 ms inner delay guarantees the second `join!` branch reaches
    /// the slot lock while the winner still holds it inside
    /// `inner.capture` — i.e. the test exercises real lock contention,
    /// not accidental sequential execution.
    #[tokio::test]
    async fn concurrent_qgroup_captures_are_single_flight() {
        let calls = Arc::new(AtomicUsize::new(0));
        let d = cached(
            CountingInner {
                calls: calls.clone(),
                fail: false,
                delay: Duration::from_millis(50),
            },
            Duration::from_secs(60),
        );
        let argv = qgroup_argv();

        let (a, b) = tokio::join!(d.capture(&argv), d.capture(&argv));

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "concurrent captures must coalesce into one qgroup spawn"
        );
        assert_eq!(a.unwrap(), b.unwrap());
    }
}
