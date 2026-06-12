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
//! Scope is deliberately limited to the exact `qgroup -l` argv:
//! - `squeue` summary queries are handled by their own batching cache
//!   one layer below (see [`crate::sbatch::squeue_cache`], added after
//!   the mixed live/purged `-j` list behaviour was verified on KUDPC).
//! - `sbatch` / `scancel` / `sacct` are mutating or heavyweight opt-in
//!   calls where replaying a stale result would be wrong.
//!
//! Staleness bound: a cached listing is served for at most `ttl`
//! (= the manager's `poll_interval`), which matches the latency callers
//! already accept by polling at that interval. KUDPC's qgroup data is
//! itself sampled (~30 s), so the cache adds no new order of staleness.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::Instant;

use crate::dispatcher::{CaptureOutput, DynJobDispatcher, JobDispatcher};

/// Shared cache slot — one per [`crate::sbatch::manager::SbatchManager`],
/// shared by every handle that manager creates (spawn and attach alike).
pub(crate) struct QgroupCacheState {
    ttl: Duration,
    slot: TokioMutex<Option<CachedEntry>>,
}

struct CachedEntry {
    at: Instant,
    /// `Ok`: raw qgroup output, any exit code (a nonzero exit is replayed
    /// as-is so the per-handle warn + squeue fallback in `refresh()` fires
    /// for every handle). `Err`: spawn failure message (binary missing on
    /// non-KUDPC clusters) — replayed as an error for the same reason.
    result: Result<CaptureOutput, String>,
}

impl QgroupCacheState {
    /// `ttl == Duration::ZERO` effectively disables caching: every read
    /// sees `elapsed() < ZERO == false` and refetches live (the slot is
    /// still overwritten each time, which is harmless).
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            slot: TokioMutex::new(None),
        }
    }
}

/// Dispatcher wrapper that serves exactly `qgroup -l` from the shared TTL
/// cache; every other argv passes through to `inner` untouched.
pub(crate) struct QgroupCachingDispatcher {
    inner: Arc<dyn DynJobDispatcher>,
    cache: Arc<QgroupCacheState>,
}

impl QgroupCachingDispatcher {
    /// The only construction path — keeps every wrapper tied to a cache
    /// that is actually shared (the manager's), never an ad-hoc one.
    pub(crate) fn new(inner: Arc<dyn DynJobDispatcher>, cache: Arc<QgroupCacheState>) -> Self {
        Self { inner, cache }
    }
}

fn is_qgroup_listing(argv: &[String]) -> bool {
    argv.len() == 2 && argv[0] == "qgroup" && argv[1] == "-l"
}

fn replay(result: &Result<CaptureOutput, String>) -> Result<CaptureOutput> {
    match result {
        Ok(out) => Ok(out.clone()),
        // The suffix deliberately marks the error as a cache replay so a
        // KUDPC incident log can tell "qgroup spawned and failed just now"
        // apart from "a failure up to TTL ago is being replayed".
        Err(msg) => Err(anyhow::anyhow!("{msg} (replayed from qgroup cache)")),
    }
}

impl JobDispatcher for QgroupCachingDispatcher {
    async fn run(&self, argv: &[String]) -> Result<i32> {
        self.inner.run(argv).await
    }

    async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
        if !is_qgroup_listing(argv) {
            return self.inner.capture(argv).await;
        }
        // The slot lock is held across the inner call on purpose:
        // concurrent qgroup captures queue here and the losers are served
        // from the entry the winner just stored (single-flight).
        //
        // Cancellation: this is NOT cancellation-safe in the single-flight
        // sense. If the winner's future is dropped mid-`inner.capture`
        // (`tokio::time::timeout`, `select!`, Python `asyncio.wait_for`),
        // tokio's Mutex releases the lock on drop, the slot stays
        // unwritten, and the next waiter simply issues a fresh live query.
        // Worst case under repeated cancellation is one qgroup spawn per
        // attempt — i.e. graceful degradation to today's uncached
        // behaviour, never a deadlock and never a stale-entry replay.
        let mut slot = self.cache.slot.lock().await;
        if let Some(entry) = slot.as_ref()
            && entry.at.elapsed() < self.cache.ttl
        {
            return replay(&entry.result);
        }
        let fetched = self.inner.capture(argv).await;
        *slot = Some(CachedEntry {
            at: Instant::now(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcher::into_dyn;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        QgroupCachingDispatcher {
            inner: into_dyn(inner),
            cache: Arc::new(QgroupCacheState::new(ttl)),
        }
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
    /// `slot.lock().await` while the winner still holds the lock inside
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
