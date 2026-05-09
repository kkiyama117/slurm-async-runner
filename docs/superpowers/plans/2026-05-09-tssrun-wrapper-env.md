# tssrun wrapper with background execution and env inspection — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Rust+Python API to launch Kyoto-U ECCS `tssrun` as a non-blocking child, inspect its environment / SLURM allocation metadata at any time, and persist that state to disk so a separate process can re-attach for read-only inspection.

**Architecture:** New `src/tssrun/` module tree (cmd / parse / log / handle / manager) reusing the existing `runner.rs` for `sacct` polling and the existing `JobDispatcher` plumbing for `capture()`. A new additive trait `BackgroundDispatcher` (with `TokioBackgroundDispatcher`) provides the non-blocking `spawn()`. State is held in a `tokio::sync::watch` channel and optionally mirrored to `{state_dir}/{pid}.json` via atomic rename. Logs are routed through a `JobLogSink` trait so callers can choose stdout / file / in-memory / future sqlite without touching the manager.

**Tech Stack:** Rust 2024, tokio (`process`, `sync::watch`, `task`, `fs`, `io::BufReader::lines`), serde + serde_json, tempfile (atomic rename), tracing (warn-only logging), pyo3 0.28 + pyo3-async-runtimes (tokio runtime), pyo3-stub-gen + hand-written `.pyi` stubs, pytest + maturin develop on the Python side.

**Spec reference:** `docs/superpowers/specs/2026-05-09-tssrun-wrapper-env-design.md`

---

## File structure summary

```
src/
├── lib.rs                          MODIFY: add pub mod tssrun; + re-exports
├── dispatcher.rs                   MODIFY: add SpawnedChild, BackgroundDispatcher trait,
│                                    TokioBackgroundDispatcher
├── tssrun/
│   ├── mod.rs                      CREATE: pub re-exports
│   ├── parse.rs                    CREATE: parse_salloc_jobid, parse_salloc_node
│   ├── cmd.rs                      CREATE: Resource, TssrunCmd, build_argv
│   ├── log.rs                      CREATE: LogStream, JobLogSink, Null/Std/InMemory/FileLogSink
│   ├── handle.rs                   CREATE: LogLocations, FinishedInfo, JobHandleSnapshot, JobHandle
│   └── manager.rs                  CREATE: AttachKey, TssrunManager
└── py_export/
    ├── mod.rs                      MODIFY: wire tssrun submodule
    └── tssrun.rs                   CREATE: pyo3 bindings

tests/
└── tssrun_integration.rs           CREATE: end-to-end with bash mock

python/
├── slurm_async_runner/_core/
│   └── tssrun.pyi                  CREATE: hand-written stubs
└── tests/
    └── test_tssrun.py              CREATE: pytest using bash mock

Cargo.toml                          MODIFY: add serde_json, tempfile; expand tokio features
README.md                           MODIFY: add tssrun section
CHANGELOG.md                        MODIFY: add unreleased entry
```

---

## Task 0: Dependencies + empty module skeleton

**Files:**
- Modify: `Cargo.toml`
- Create: `src/tssrun/mod.rs`
- Create: `src/tssrun/parse.rs`
- Create: `src/tssrun/cmd.rs`
- Create: `src/tssrun/log.rs`
- Create: `src/tssrun/handle.rs`
- Create: `src/tssrun/manager.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Inspect existing transitive deps**

Run: `cargo tree --depth 1 | grep -E '(serde_json|tempfile)' || echo MISSING`
Expected: `MISSING` (we add both).

- [ ] **Step 2: Modify `Cargo.toml`**

Replace the `tokio = ...` line and append the two new deps. The relevant `[dependencies]` block becomes:

```toml
# Asynchronous runtime and utilities
futures = "0.3"
tokio = { version = "1.0", features = [
  "macros",
  "process",
  "rt-multi-thread",
  "sync",       # watch, Mutex
  "fs",         # tokio::fs::File / read
  "io-util",    # AsyncBufReadExt::lines
  "time",       # tokio::time::timeout in tests
] }

chrono = { version = "0.4", features = ["serde"] }
# Serialization
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
tempfile = "3.10"
```

(Leave every other line untouched.)

- [ ] **Step 3: Create empty module files**

`src/tssrun/mod.rs`:
```rust
//! tssrun wrapper with background execution and env inspection.
//!
//! See `docs/superpowers/specs/2026-05-09-tssrun-wrapper-env-design.md`.

pub mod cmd;
pub mod handle;
pub mod log;
pub mod manager;
pub mod parse;
```

`src/tssrun/parse.rs`:
```rust
//! Pure parsers for `salloc:` lines emitted by `tssrun` on allocation.
```

`src/tssrun/cmd.rs`:
```rust
//! Spec types: `Resource` and `TssrunCmd` with argv builder. No I/O.
```

`src/tssrun/log.rs`:
```rust
//! Pluggable log sinks for tee-ed stdout/stderr.
```

`src/tssrun/handle.rs`:
```rust
//! `JobHandleSnapshot` (Serde) and `JobHandle` (in-process state).
```

`src/tssrun/manager.rs`:
```rust
//! `TssrunManager` orchestrates spawn / attach / query_state.
```

- [ ] **Step 4: Wire module into `src/lib.rs`**

Insert after the existing `pub mod runner;` line:

```rust
pub mod tssrun;
```

- [ ] **Step 5: Verify compile**

Run: `cargo check --all-targets`
Expected: success.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml src/lib.rs src/tssrun/
git commit -m "chore(tssrun): add empty module skeleton + deps (serde_json, tempfile, tokio sync/fs/io-util/time)"
```

---

## Task 1: Pure `salloc:` parsers (`tssrun/parse.rs`)

**Files:**
- Modify: `src/tssrun/parse.rs`

- [ ] **Step 1: Write the failing tests**

Append to `src/tssrun/parse.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_jobid_from_granted_line() {
        assert_eq!(
            parse_salloc_jobid("salloc: Granted job allocation 102362"),
            Some(102362)
        );
    }

    #[test]
    fn rejects_jobid_when_prefix_missing() {
        assert_eq!(parse_salloc_jobid("Granted job allocation 102362"), None);
    }

    #[test]
    fn rejects_jobid_when_value_not_numeric() {
        assert_eq!(parse_salloc_jobid("salloc: Granted job allocation abc"), None);
    }

    #[test]
    fn parses_node_from_ready_line() {
        assert_eq!(
            parse_salloc_node("salloc: Nodes cnode3 are ready for job"),
            Some("cnode3".to_string())
        );
    }

    #[test]
    fn parses_multi_node_form_verbatim() {
        assert_eq!(
            parse_salloc_node("salloc: Nodes cnode[3-4] are ready for job"),
            Some("cnode[3-4]".to_string())
        );
    }

    #[test]
    fn rejects_node_when_marker_absent() {
        assert_eq!(
            parse_salloc_node("salloc: Nodes cnode3 are still pending"),
            None
        );
    }
}
```

- [ ] **Step 2: Run tests — expect FAIL**

Run: `cargo test --lib tssrun::parse -- --nocapture`
Expected: compile error `cannot find function parse_salloc_jobid`.

- [ ] **Step 3: Implement parsers**

Replace the contents of `src/tssrun/parse.rs` with:

```rust
//! Pure parsers for `salloc:` lines emitted by `tssrun` on allocation.
//!
//! These intentionally match exact prefixes — the kudpc manual prints
//! `"salloc: Granted job allocation N"` and
//! `"salloc: Nodes <node> are ready for job"`. Site-specific banner
//! changes break these parsers on purpose so the failure is visible.

/// Returns `Some(jobid)` when `line` is exactly the SLURM
/// "Granted job allocation N" message.
pub fn parse_salloc_jobid(line: &str) -> Option<u64> {
    line.strip_prefix("salloc: Granted job allocation ")
        .map(str::trim)
        .and_then(|s| s.parse::<u64>().ok())
}

/// Returns `Some(node_spec)` when `line` is the SLURM
/// "Nodes <spec> are ready for job" message. The node spec is preserved
/// verbatim (e.g. `"cnode3"` or `"cnode[3-4]"`).
pub fn parse_salloc_node(line: &str) -> Option<String> {
    let rest = line.strip_prefix("salloc: Nodes ")?;
    let (node, _) = rest.split_once(" are ready for job")?;
    Some(node.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_jobid_from_granted_line() {
        assert_eq!(
            parse_salloc_jobid("salloc: Granted job allocation 102362"),
            Some(102362)
        );
    }

    #[test]
    fn rejects_jobid_when_prefix_missing() {
        assert_eq!(parse_salloc_jobid("Granted job allocation 102362"), None);
    }

    #[test]
    fn rejects_jobid_when_value_not_numeric() {
        assert_eq!(parse_salloc_jobid("salloc: Granted job allocation abc"), None);
    }

    #[test]
    fn parses_node_from_ready_line() {
        assert_eq!(
            parse_salloc_node("salloc: Nodes cnode3 are ready for job"),
            Some("cnode3".to_string())
        );
    }

    #[test]
    fn parses_multi_node_form_verbatim() {
        assert_eq!(
            parse_salloc_node("salloc: Nodes cnode[3-4] are ready for job"),
            Some("cnode[3-4]".to_string())
        );
    }

    #[test]
    fn rejects_node_when_marker_absent() {
        assert_eq!(
            parse_salloc_node("salloc: Nodes cnode3 are still pending"),
            None
        );
    }
}
```

- [ ] **Step 4: Run tests — expect PASS**

Run: `cargo test --lib tssrun::parse`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/tssrun/parse.rs
git commit -m "feat(tssrun): add pure parsers for salloc Granted/Nodes lines"
```

---

## Task 2: `Resource` type with `--rsc` rendering (`tssrun/cmd.rs`)

**Files:**
- Modify: `src/tssrun/cmd.rs`

- [ ] **Step 1: Write failing tests**

Append to `src/tssrun/cmd.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_default_renders_none() {
        assert_eq!(Resource::default().render(), None);
    }

    #[test]
    fn resource_full_renders_in_order() {
        let r = Resource {
            processes: Some(4),
            threads: Some(8),
            cores: Some(8),
            memory: Some("2G".into()),
            gpus: Some(1),
        };
        assert_eq!(r.render().as_deref(), Some("p=4:t=8:c=8:m=2G:g=1"));
    }

    #[test]
    fn resource_partial_skips_none_keys() {
        let r = Resource {
            processes: Some(4),
            memory: Some("2G".into()),
            ..Default::default()
        };
        assert_eq!(r.render().as_deref(), Some("p=4:m=2G"));
    }
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test --lib tssrun::cmd`
Expected: compile error `cannot find type Resource`.

- [ ] **Step 3: Implement `Resource`**

Replace `src/tssrun/cmd.rs` with:

```rust
//! Spec types: [`Resource`] and `TssrunCmd` with argv builder. No I/O.

/// Resource spec passed to `tssrun --rsc p=:t=:c=:m=:g=`.
///
/// All fields are optional; `render` only emits keys whose value is `Some`.
/// Order is fixed: `p`, `t`, `c`, `m`, `g`. The `memory` field is a free
/// string (`"2G"`, `"512M"`, etc.) since SLURM accepts unit suffixes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resource {
    pub processes: Option<u32>,
    pub threads: Option<u32>,
    pub cores: Option<u32>,
    pub memory: Option<String>,
    pub gpus: Option<u32>,
}

impl Resource {
    /// Renders the colon-joined `p=…:t=…:m=…:g=…` string.
    /// Returns `None` if every field is `None`.
    pub fn render(&self) -> Option<String> {
        let mut parts: Vec<String> = Vec::with_capacity(5);
        if let Some(p) = self.processes {
            parts.push(format!("p={p}"));
        }
        if let Some(t) = self.threads {
            parts.push(format!("t={t}"));
        }
        if let Some(c) = self.cores {
            parts.push(format!("c={c}"));
        }
        if let Some(m) = &self.memory {
            parts.push(format!("m={m}"));
        }
        if let Some(g) = self.gpus {
            parts.push(format!("g={g}"));
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join(":"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_default_renders_none() {
        assert_eq!(Resource::default().render(), None);
    }

    #[test]
    fn resource_full_renders_in_order() {
        let r = Resource {
            processes: Some(4),
            threads: Some(8),
            cores: Some(8),
            memory: Some("2G".into()),
            gpus: Some(1),
        };
        assert_eq!(r.render().as_deref(), Some("p=4:t=8:c=8:m=2G:g=1"));
    }

    #[test]
    fn resource_partial_skips_none_keys() {
        let r = Resource {
            processes: Some(4),
            memory: Some("2G".into()),
            ..Default::default()
        };
        assert_eq!(r.render().as_deref(), Some("p=4:m=2G"));
    }
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test --lib tssrun::cmd`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/tssrun/cmd.rs
git commit -m "feat(tssrun): add Resource type with --rsc render"
```

---

## Task 3: `TssrunCmd::build_argv` (`tssrun/cmd.rs`)

**Files:**
- Modify: `src/tssrun/cmd.rs`

- [ ] **Step 1: Add failing tests**

Add to the `tests` module in `src/tssrun/cmd.rs`:

```rust
    #[test]
    fn cmd_minimal_argv_is_bin_then_program() {
        let c = TssrunCmd::new("/work/job.sh");
        let argv = c.build_argv().unwrap();
        assert_eq!(argv, vec!["tssrun".to_string(), "/work/job.sh".to_string()]);
    }

    #[test]
    fn cmd_relative_program_is_absolutized() {
        let c = TssrunCmd::new("job.sh");
        let argv = c.build_argv().unwrap();
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(argv[0], "tssrun");
        assert_eq!(argv[1], format!("{}/job.sh", cwd.display()));
    }

    #[test]
    fn cmd_full_flags_in_documented_order() {
        let mut c = TssrunCmd::new("/work/job.sh");
        c.queue = Some("gr19999b".into());
        c.time_limit = Some("1:0:0".into());
        c.rsc = Some(Resource {
            processes: Some(4),
            threads: Some(8),
            cores: Some(8),
            memory: Some("2G".into()),
            gpus: Some(1),
        });
        c.x11 = true;
        c.args = vec!["--flag".into(), "value".into()];
        let argv = c.build_argv().unwrap();
        assert_eq!(
            argv,
            vec![
                "tssrun".to_string(),
                "-p".to_string(),
                "gr19999b".to_string(),
                "-t".to_string(),
                "1:0:0".to_string(),
                "--rsc".to_string(),
                "p=4:t=8:c=8:m=2G:g=1".to_string(),
                "--x11".to_string(),
                "/work/job.sh".to_string(),
                "--flag".to_string(),
                "value".to_string(),
            ]
        );
    }

    #[test]
    fn cmd_rsc_with_only_some_keys() {
        let mut c = TssrunCmd::new("/work/job.sh");
        c.rsc = Some(Resource { processes: Some(4), memory: Some("2G".into()), ..Default::default() });
        let argv = c.build_argv().unwrap();
        assert!(argv.contains(&"--rsc".to_string()));
        assert!(argv.contains(&"p=4:m=2G".to_string()));
    }

    #[test]
    fn cmd_rsc_all_none_omits_flag_entirely() {
        let mut c = TssrunCmd::new("/work/job.sh");
        c.rsc = Some(Resource::default());
        let argv = c.build_argv().unwrap();
        assert!(!argv.contains(&"--rsc".to_string()));
    }
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test --lib tssrun::cmd`
Expected: compile error `cannot find struct TssrunCmd`.

- [ ] **Step 3: Implement `TssrunCmd::build_argv`**

Insert above the `#[cfg(test)]` block in `src/tssrun/cmd.rs`:

```rust
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Spec for a single `tssrun` invocation. Pure data + an argv builder —
/// no subprocess work. Mirrors [`crate::manager::SlurmCmd`] in spirit but
/// represents the kudpc-manual options as typed fields.
#[derive(Debug, Clone)]
pub struct TssrunCmd {
    pub tssrun_bin: String,
    pub queue: Option<String>,
    pub time_limit: Option<String>,
    pub rsc: Option<Resource>,
    pub x11: bool,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub cwd: Option<PathBuf>,
}

impl TssrunCmd {
    /// Construct with defaults: `tssrun_bin = "tssrun"`, no flags, given program.
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            tssrun_bin: "tssrun".to_string(),
            queue: None,
            time_limit: None,
            rsc: None,
            x11: false,
            program: program.into(),
            args: Vec::new(),
            env: HashMap::new(),
            cwd: None,
        }
    }

    /// Build the argv: `[bin, -p QUEUE?, -t TIME?, --rsc SPEC?, --x11?, program_abs, args…]`.
    pub fn build_argv(&self) -> Result<Vec<String>> {
        let mut argv: Vec<String> = Vec::with_capacity(8 + self.args.len());
        argv.push(self.tssrun_bin.clone());

        if let Some(q) = &self.queue {
            argv.push("-p".to_string());
            argv.push(q.clone());
        }
        if let Some(t) = &self.time_limit {
            argv.push("-t".to_string());
            argv.push(t.clone());
        }
        if let Some(r) = &self.rsc {
            if let Some(spec) = r.render() {
                argv.push("--rsc".to_string());
                argv.push(spec);
            }
        }
        if self.x11 {
            argv.push("--x11".to_string());
        }

        argv.push(absolutize(&self.program)?);

        for a in &self.args {
            argv.push(a.clone());
        }
        Ok(argv)
    }
}

fn absolutize(p: &Path) -> Result<String> {
    let abs = std::path::absolute(p)
        .with_context(|| format!("failed to absolutize {}", p.display()))?;
    abs.into_os_string()
        .into_string()
        .map_err(|os| anyhow::anyhow!("non-UTF8 program path: {os:?}"))
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test --lib tssrun::cmd`
Expected: 8 passed (3 Resource + 5 TssrunCmd).

- [ ] **Step 5: Commit**

```bash
git add src/tssrun/cmd.rs
git commit -m "feat(tssrun): add TssrunCmd::build_argv with kudpc flag ordering"
```

---

## Task 4: `LogStream` + `JobLogSink` trait + `NullLogSink` (`tssrun/log.rs`)

**Files:**
- Modify: `src/tssrun/log.rs`

- [ ] **Step 1: Write failing tests**

Append to `src/tssrun/log.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn null_sink_append_is_noop() {
        let s = NullLogSink;
        s.append(LogStream::Stdout, "hi").await.unwrap();
        s.append(LogStream::Stderr, "bye").await.unwrap();
        s.flush().await.unwrap();
    }
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test --lib tssrun::log`
Expected: compile error `cannot find type LogStream`.

- [ ] **Step 3: Implement trait + NullLogSink**

Replace `src/tssrun/log.rs` with:

```rust
//! Pluggable log sinks for tee-ed stdout/stderr lines from a tssrun child.
//!
//! Sinks are invoked once per `\n`-delimited line (no trailing newline).
//! Implementations must be `Send + Sync` so the tee task can be spawned
//! on the multi-threaded tokio runtime.

use std::future::Future;

use anyhow::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

pub trait JobLogSink: Send + Sync {
    fn append(&self, stream: LogStream, line: &str)
        -> impl Future<Output = Result<()>> + Send;
    fn flush(&self) -> impl Future<Output = Result<()>> + Send;
}

/// Drops every line. Use when stdout is irrelevant (still parses for jobid).
#[derive(Debug, Default, Clone, Copy)]
pub struct NullLogSink;

impl JobLogSink for NullLogSink {
    async fn append(&self, _stream: LogStream, _line: &str) -> Result<()> {
        Ok(())
    }
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn null_sink_append_is_noop() {
        let s = NullLogSink;
        s.append(LogStream::Stdout, "hi").await.unwrap();
        s.append(LogStream::Stderr, "bye").await.unwrap();
        s.flush().await.unwrap();
    }
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test --lib tssrun::log`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add src/tssrun/log.rs
git commit -m "feat(tssrun): add JobLogSink trait + NullLogSink"
```

---

## Task 5: `InMemoryLogSink` (`tssrun/log.rs`)

**Files:**
- Modify: `src/tssrun/log.rs`

- [ ] **Step 1: Add failing test**

Add to the `tests` module in `src/tssrun/log.rs`:

```rust
    #[tokio::test]
    async fn in_memory_sink_preserves_order() {
        let s = InMemoryLogSink::default();
        s.append(LogStream::Stdout, "a").await.unwrap();
        s.append(LogStream::Stderr, "b").await.unwrap();
        s.append(LogStream::Stdout, "c").await.unwrap();
        let v = s.snapshot();
        assert_eq!(
            v,
            vec![
                (LogStream::Stdout, "a".to_string()),
                (LogStream::Stderr, "b".to_string()),
                (LogStream::Stdout, "c".to_string()),
            ]
        );
    }
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test --lib tssrun::log`
Expected: compile error `cannot find type InMemoryLogSink`.

- [ ] **Step 3: Implement `InMemoryLogSink`**

Insert immediately above the `#[cfg(test)]` block in `src/tssrun/log.rs`:

```rust
use std::sync::Mutex;

/// Captures every appended line in memory. Test/diagnostics oriented.
#[derive(Debug, Default)]
pub struct InMemoryLogSink {
    buf: Mutex<Vec<(LogStream, String)>>,
}

impl InMemoryLogSink {
    pub fn snapshot(&self) -> Vec<(LogStream, String)> {
        self.buf.lock().unwrap().clone()
    }
}

impl JobLogSink for InMemoryLogSink {
    async fn append(&self, stream: LogStream, line: &str) -> Result<()> {
        self.buf.lock().unwrap().push((stream, line.to_string()));
        Ok(())
    }
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test --lib tssrun::log`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/tssrun/log.rs
git commit -m "feat(tssrun): add InMemoryLogSink"
```

---

## Task 6: `StdLogSink` (`tssrun/log.rs`)

**Files:**
- Modify: `src/tssrun/log.rs`

- [ ] **Step 1: Add failing test**

Add to the `tests` module:

```rust
    #[tokio::test]
    async fn std_sink_does_not_panic() {
        // Validates the type and trait wiring; the actual stdout/stderr
        // bytes go to the test harness output and are not assertable here.
        let s = StdLogSink;
        s.append(LogStream::Stdout, "alpha").await.unwrap();
        s.append(LogStream::Stderr, "beta").await.unwrap();
        s.flush().await.unwrap();
    }
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test --lib tssrun::log`
Expected: compile error `cannot find type StdLogSink`.

- [ ] **Step 3: Implement `StdLogSink`**

Insert above the `#[cfg(test)]` block:

```rust
/// Forwards stdout lines to the parent's stdout, stderr lines to stderr.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdLogSink;

impl JobLogSink for StdLogSink {
    async fn append(&self, stream: LogStream, line: &str) -> Result<()> {
        match stream {
            LogStream::Stdout => println!("{line}"),
            LogStream::Stderr => eprintln!("{line}"),
        }
        Ok(())
    }
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test --lib tssrun::log`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/tssrun/log.rs
git commit -m "feat(tssrun): add StdLogSink"
```

---

## Task 7: `FileLogSink` (`tssrun/log.rs`)

**Files:**
- Modify: `src/tssrun/log.rs`

- [ ] **Step 1: Add failing test**

Add to the `tests` module:

```rust
    #[tokio::test]
    async fn file_sink_writes_lines_to_correct_streams() {
        use std::path::PathBuf;
        let tmp = tempfile::tempdir().unwrap();
        let stdout_path: PathBuf = tmp.path().join("o.log");
        let stderr_path: PathBuf = tmp.path().join("e.log");
        let s = FileLogSink::create(stdout_path.clone(), stderr_path.clone())
            .await
            .unwrap();
        s.append(LogStream::Stdout, "hello").await.unwrap();
        s.append(LogStream::Stderr, "world").await.unwrap();
        s.flush().await.unwrap();

        let o = std::fs::read_to_string(&stdout_path).unwrap();
        let e = std::fs::read_to_string(&stderr_path).unwrap();
        assert_eq!(o, "hello\n");
        assert_eq!(e, "world\n");
    }
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test --lib tssrun::log`
Expected: compile error `cannot find type FileLogSink`.

- [ ] **Step 3: Implement `FileLogSink`**

Insert above the `#[cfg(test)]` block:

```rust
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

/// Append-only file sink. Each `append` writes one line followed by `\n`.
pub struct FileLogSink {
    stdout: tokio::sync::Mutex<tokio::fs::File>,
    stderr: tokio::sync::Mutex<tokio::fs::File>,
    paths: (PathBuf, PathBuf),
}

impl FileLogSink {
    pub async fn create(stdout: PathBuf, stderr: PathBuf) -> Result<Self> {
        let stdout_f = tokio::fs::OpenOptions::new()
            .create(true).append(true).open(&stdout).await?;
        let stderr_f = tokio::fs::OpenOptions::new()
            .create(true).append(true).open(&stderr).await?;
        Ok(Self {
            stdout: tokio::sync::Mutex::new(stdout_f),
            stderr: tokio::sync::Mutex::new(stderr_f),
            paths: (stdout, stderr),
        })
    }
    pub fn paths(&self) -> &(PathBuf, PathBuf) {
        &self.paths
    }
}

impl JobLogSink for FileLogSink {
    async fn append(&self, stream: LogStream, line: &str) -> Result<()> {
        let mut f = match stream {
            LogStream::Stdout => self.stdout.lock().await,
            LogStream::Stderr => self.stderr.lock().await,
        };
        f.write_all(line.as_bytes()).await?;
        f.write_all(b"\n").await?;
        Ok(())
    }
    async fn flush(&self) -> Result<()> {
        self.stdout.lock().await.flush().await?;
        self.stderr.lock().await.flush().await?;
        Ok(())
    }
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test --lib tssrun::log`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/tssrun/log.rs
git commit -m "feat(tssrun): add FileLogSink with tokio::fs append"
```

---

## Task 8: `BackgroundDispatcher` trait + `TokioBackgroundDispatcher` (`dispatcher.rs`)

**Files:**
- Modify: `src/dispatcher.rs`

- [ ] **Step 1: Add failing tests**

Append to the existing `tests` module in `src/dispatcher.rs`:

```rust
    #[tokio::test]
    async fn tokio_background_spawn_runs_to_zero() {
        use std::collections::HashMap;
        let d = TokioBackgroundDispatcher;
        let argv = vec!["bash".to_string(), "-c".to_string(), "exit 0".to_string()];
        let mut spawned = d.spawn(&argv, &HashMap::new(), None).await.unwrap();
        assert!(spawned.pid > 0);
        let status = spawned.child.wait().await.unwrap();
        assert_eq!(status.code(), Some(0));
    }

    #[tokio::test]
    async fn tokio_background_spawn_missing_binary_errors() {
        use std::collections::HashMap;
        let d = TokioBackgroundDispatcher;
        let err = d
            .spawn(
                &["definitely-not-a-real-binary-xyz".to_string()],
                &HashMap::new(),
                None,
            )
            .await
            .unwrap_err();
        let s = err.to_string();
        assert!(
            s.contains("failed to spawn") || s.contains("No such file"),
            "unexpected error: {s}"
        );
    }
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test --lib dispatcher::tests::tokio_background`
Expected: compile error `cannot find type TokioBackgroundDispatcher`.

- [ ] **Step 3: Implement extension**

Append to `src/dispatcher.rs` (above the existing `#[cfg(test)]` block):

```rust
use std::collections::HashMap;
use std::path::Path;

/// A child spawned but **not awaited**. Caller owns the `Child` and must
/// `wait()` it (or move it into a task that does) to avoid zombies.
pub struct SpawnedChild {
    pub pid: u32,
    pub child: tokio::process::Child,
}

/// Non-blocking variant of [`JobDispatcher`]: returns immediately with a
/// child handle whose stdout/stderr are piped.
pub trait BackgroundDispatcher: JobDispatcher {
    fn spawn(
        &self,
        argv: &[String],
        env: &HashMap<String, String>,
        cwd: Option<&Path>,
    ) -> impl std::future::Future<Output = anyhow::Result<SpawnedChild>> + Send;
}

/// Production [`BackgroundDispatcher`] backed by `tokio::process::Command`.
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioBackgroundDispatcher;

impl JobDispatcher for TokioBackgroundDispatcher {
    async fn run(&self, argv: &[String]) -> anyhow::Result<i32> {
        TokioDispatcher.run(argv).await
    }
    async fn capture(&self, argv: &[String]) -> anyhow::Result<(i32, String)> {
        TokioDispatcher.capture(argv).await
    }
}

impl BackgroundDispatcher for TokioBackgroundDispatcher {
    async fn spawn(
        &self,
        argv: &[String],
        env: &HashMap<String, String>,
        cwd: Option<&Path>,
    ) -> anyhow::Result<SpawnedChild> {
        use anyhow::Context as _;
        let (program, args) = argv
            .split_first()
            .context("TokioBackgroundDispatcher::spawn called with empty argv")?;
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .envs(env);
        if let Some(d) = cwd {
            cmd.current_dir(d);
        }
        let child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn `{program}`"))?;
        let pid = child.id().context("spawned child has no pid")?;
        Ok(SpawnedChild { pid, child })
    }
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test --lib dispatcher`
Expected: all existing tests still pass + 2 new ones.

- [ ] **Step 5: Commit**

```bash
git add src/dispatcher.rs
git commit -m "feat(dispatcher): add BackgroundDispatcher trait + TokioBackgroundDispatcher"
```

---

## Task 9: `JobHandleSnapshot` + serde round-trip (`tssrun/handle.rs`)

**Files:**
- Modify: `src/tssrun/handle.rs`

- [ ] **Step 1: Write failing tests**

Append to `src/tssrun/handle.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn snap_running() -> JobHandleSnapshot {
        JobHandleSnapshot {
            pid: 31415,
            argv: vec!["tssrun".into(), "/work/job.sh".into()],
            sent_env: HashMap::from([("OMP_NUM_THREADS".into(), "8".into())]),
            cwd: Some(PathBuf::from("/work")),
            started_at_unix: 1746345600,
            log_locations: LogLocations::Files {
                stdout: PathBuf::from("/var/log/x/o"),
                stderr: PathBuf::from("/var/log/x/e"),
            },
            jobid: Some(102362),
            node: Some("cnode3".into()),
            finished: None,
        }
    }

    #[test]
    fn snapshot_round_trip_running() {
        let s = snap_running();
        let json = serde_json::to_string(&s).unwrap();
        let back: JobHandleSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn snapshot_round_trip_finished_none_loglocations() {
        let mut s = snap_running();
        s.log_locations = LogLocations::None;
        s.finished = Some(FinishedInfo { exit_code: Some(0), finished_at_unix: 1746349200 });
        let json = serde_json::to_string(&s).unwrap();
        let back: JobHandleSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test --lib tssrun::handle`
Expected: compile error `cannot find type JobHandleSnapshot`.

- [ ] **Step 3: Implement snapshot types**

Replace the contents of `src/tssrun/handle.rs` with:

```rust
//! `JobHandleSnapshot` (Serde) and `JobHandle` (in-process state).

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Where the tee task is writing the child's logs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogLocations {
    /// Either no sink is attached or the sink is non-file-backed.
    None,
    /// Two append-only files on the local filesystem.
    Files { stdout: PathBuf, stderr: PathBuf },
    // Future: Sqlite { db_path: PathBuf, run_id: u64 }
}

/// Recorded once the child exits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FinishedInfo {
    pub exit_code: Option<i32>,
    pub finished_at_unix: i64,
}

/// Persistable snapshot of a tssrun job. Updated by the tee task as the
/// `salloc:` lines arrive and by the wait task on child exit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobHandleSnapshot {
    pub pid: u32,
    pub argv: Vec<String>,
    pub sent_env: HashMap<String, String>,
    pub cwd: Option<PathBuf>,
    pub started_at_unix: i64,
    pub log_locations: LogLocations,
    pub jobid: Option<u64>,
    pub node: Option<String>,
    pub finished: Option<FinishedInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn snap_running() -> JobHandleSnapshot {
        JobHandleSnapshot {
            pid: 31415,
            argv: vec!["tssrun".into(), "/work/job.sh".into()],
            sent_env: HashMap::from([("OMP_NUM_THREADS".into(), "8".into())]),
            cwd: Some(PathBuf::from("/work")),
            started_at_unix: 1746345600,
            log_locations: LogLocations::Files {
                stdout: PathBuf::from("/var/log/x/o"),
                stderr: PathBuf::from("/var/log/x/e"),
            },
            jobid: Some(102362),
            node: Some("cnode3".into()),
            finished: None,
        }
    }

    #[test]
    fn snapshot_round_trip_running() {
        let s = snap_running();
        let json = serde_json::to_string(&s).unwrap();
        let back: JobHandleSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn snapshot_round_trip_finished_none_loglocations() {
        let mut s = snap_running();
        s.log_locations = LogLocations::None;
        s.finished = Some(FinishedInfo { exit_code: Some(0), finished_at_unix: 1746349200 });
        let json = serde_json::to_string(&s).unwrap();
        let back: JobHandleSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test --lib tssrun::handle`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/tssrun/handle.rs
git commit -m "feat(tssrun): add JobHandleSnapshot + LogLocations + FinishedInfo (Serde)"
```

---

## Task 10: `JobHandle` lifecycle (spawn → tee/parse → wait)

**Files:**
- Modify: `src/tssrun/handle.rs`

This task is the core. It writes the integration glue between
`SpawnedChild`, `JobLogSink`, the watch channel, and the parsers.

- [ ] **Step 1: Add failing test**

Add to the `tests` module in `src/tssrun/handle.rs`:

```rust
    use crate::dispatcher::{BackgroundDispatcher, TokioBackgroundDispatcher};
    use crate::tssrun::log::InMemoryLogSink;
    use std::sync::Arc;

    #[tokio::test]
    async fn handle_from_spawn_parses_jobid_node_and_waits_zero() {
        let argv = vec![
            "bash".to_string(),
            "-c".to_string(),
            r#"echo "salloc: Granted job allocation 999"
echo "salloc: Nodes node-x are ready for job"
sleep 0.05
echo done"#.into(),
        ];
        let env = std::collections::HashMap::new();
        let spawned = TokioBackgroundDispatcher.spawn(&argv, &env, None).await.unwrap();

        let typed_sink: Arc<InMemoryLogSink> = Arc::new(InMemoryLogSink::default());
        let sink_for_handle: Arc<dyn crate::tssrun::log::JobLogSink> =
            Arc::clone(&typed_sink) as Arc<dyn crate::tssrun::log::JobLogSink>;

        let init = JobHandleSnapshot {
            pid: spawned.pid,
            argv: argv.clone(),
            sent_env: env,
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid: None,
            node: None,
            finished: None,
        };
        let mut handle =
            JobHandle::from_spawn(spawned, init, sink_for_handle, None).await.unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, 0);

        let snap = handle.snapshot();
        assert_eq!(snap.jobid, Some(999));
        assert_eq!(snap.node.as_deref(), Some("node-x"));
        assert!(snap.finished.is_some());

        let lines: Vec<String> =
            typed_sink.snapshot().into_iter().map(|(_, l)| l).collect();
        assert!(lines.iter().any(|l| l == "done"), "sink lines: {lines:?}");
    }
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test --lib tssrun::handle::tests::handle_from_spawn`
Expected: compile error `cannot find type JobHandle`.

- [ ] **Step 3: Implement `JobHandle`**

Insert immediately above the `#[cfg(test)]` block in `src/tssrun/handle.rs`:

```rust
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::ChildStderr;
use tokio::process::ChildStdout;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use crate::dispatcher::SpawnedChild;
use crate::tssrun::log::{JobLogSink, LogStream};
use crate::tssrun::parse::{parse_salloc_jobid, parse_salloc_node};

/// In-process handle to a spawned `tssrun` child plus the tee/wait tasks
/// that keep its [`JobHandleSnapshot`] up to date.
pub struct JobHandle {
    snapshot_rx: watch::Receiver<JobHandleSnapshot>,
    snapshot_tx: watch::Sender<JobHandleSnapshot>,
    wait_handle: Option<JoinHandle<Result<i32>>>,
    persist_path: Option<std::path::PathBuf>,
}

impl JobHandle {
    /// Build a handle from a freshly spawned child. Spawns the tee tasks
    /// for stdout/stderr and the wait task for `child.wait()`.
    pub async fn from_spawn(
        mut spawned: SpawnedChild,
        init: JobHandleSnapshot,
        log_sink: Arc<dyn JobLogSink>,
        persist_path: Option<std::path::PathBuf>,
    ) -> Result<Self> {
        let (tx, rx) = watch::channel(init);

        if let Some(p) = &persist_path {
            if let Err(e) = write_atomic_json(p, &*tx.borrow()) {
                tracing::warn!(error = %e, path = %p.display(), "initial persist failed");
            }
        }

        let stdout = spawned
            .child
            .stdout
            .take()
            .context("BackgroundDispatcher returned a child without piped stdout")?;
        let stderr = spawned
            .child
            .stderr
            .take()
            .context("BackgroundDispatcher returned a child without piped stderr")?;

        tokio::spawn(tee_stdout(
            stdout,
            log_sink.clone(),
            tx.clone(),
            persist_path.clone(),
        ));
        tokio::spawn(tee_stderr(
            stderr,
            log_sink.clone(),
            tx.clone(),
            persist_path.clone(),
        ));

        let child_arc = Arc::new(Mutex::new(spawned.child));
        let wait_handle = {
            let child = child_arc.clone();
            let tx = tx.clone();
            let persist_path = persist_path.clone();
            let log_sink = log_sink.clone();
            tokio::spawn(async move {
                let status = child.lock().await.wait().await?;
                let code = status.code();
                tx.send_modify(|s| {
                    s.finished = Some(FinishedInfo {
                        exit_code: code,
                        finished_at_unix: now_unix(),
                    });
                });
                if let Some(p) = &persist_path {
                    if let Err(e) = write_atomic_json(p, &*tx.borrow()) {
                        tracing::warn!(error = %e, path = %p.display(), "post-exit persist failed");
                    }
                }
                let _ = log_sink.flush().await;
                Ok(code.unwrap_or(0))
            })
        };

        Ok(Self {
            snapshot_rx: rx,
            snapshot_tx: tx,
            wait_handle: Some(wait_handle),
            persist_path,
        })
    }

    /// Build a read-only handle from a previously persisted snapshot.
    pub fn attach_snapshot(
        snap: JobHandleSnapshot,
        persist_path: Option<std::path::PathBuf>,
    ) -> Self {
        let (tx, rx) = watch::channel(snap);
        Self {
            snapshot_rx: rx,
            snapshot_tx: tx,
            wait_handle: None,
            persist_path,
        }
    }

    pub fn snapshot(&self) -> JobHandleSnapshot {
        self.snapshot_rx.borrow().clone()
    }
    pub fn pid(&self) -> u32 {
        self.snapshot_rx.borrow().pid
    }
    pub fn jobid(&self) -> Option<u64> {
        self.snapshot_rx.borrow().jobid
    }
    pub fn node(&self) -> Option<String> {
        self.snapshot_rx.borrow().node.clone()
    }
    pub fn sent_env(&self) -> std::collections::HashMap<String, String> {
        self.snapshot_rx.borrow().sent_env.clone()
    }
    pub fn is_running(&self) -> bool {
        self.snapshot_rx.borrow().finished.is_none()
    }
    pub fn exit_code(&self) -> Option<i32> {
        self.snapshot_rx
            .borrow()
            .finished
            .as_ref()
            .and_then(|f| f.exit_code)
    }

    /// Wait for the child to exit and return its exit code. Errors when
    /// invoked on an attached handle (no owned child) or after a previous
    /// `wait()` already consumed the join handle.
    pub async fn wait(&mut self) -> Result<i32> {
        let h = self
            .wait_handle
            .take()
            .ok_or_else(|| anyhow!("not owner of the child / already waited"))?;
        h.await?
    }

    /// Re-read the persisted snapshot from disk and broadcast it.
    pub async fn refresh_from_disk(&self) -> Result<()> {
        let p = self
            .persist_path
            .as_ref()
            .ok_or_else(|| anyhow!("no persist_path on this handle"))?;
        let bytes = tokio::fs::read(p).await?;
        let snap: JobHandleSnapshot = serde_json::from_slice(&bytes)?;
        let _ = self.snapshot_tx.send(snap);
        Ok(())
    }

    /// Read `/proc/<pid>/environ` (Linux only). Returns `Ok(None)` on
    /// other platforms or when the directory is gone.
    pub async fn live_env(&self) -> Result<Option<std::collections::HashMap<String, String>>> {
        if !cfg!(target_os = "linux") {
            return Ok(None);
        }
        let pid = self.pid();
        let path = format!("/proc/{pid}/environ");
        match tokio::fs::read(&path).await {
            Ok(bytes) => Ok(Some(parse_environ(&bytes))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

fn parse_environ(bytes: &[u8]) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    for raw in bytes.split(|&b| b == 0) {
        if raw.is_empty() {
            continue;
        }
        if let Ok(s) = std::str::from_utf8(raw) {
            if let Some((k, v)) = s.split_once('=') {
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}

async fn tee_stdout(
    stdout: ChildStdout,
    sink: Arc<dyn JobLogSink>,
    tx: watch::Sender<JobHandleSnapshot>,
    persist_path: Option<std::path::PathBuf>,
) {
    tee_lines(stdout, LogStream::Stdout, sink, tx, persist_path).await;
}

async fn tee_stderr(
    stderr: ChildStderr,
    sink: Arc<dyn JobLogSink>,
    tx: watch::Sender<JobHandleSnapshot>,
    persist_path: Option<std::path::PathBuf>,
) {
    tee_lines(stderr, LogStream::Stderr, sink, tx, persist_path).await;
}

async fn tee_lines<R>(
    stream: R,
    stream_kind: LogStream,
    sink: Arc<dyn JobLogSink>,
    tx: watch::Sender<JobHandleSnapshot>,
    persist_path: Option<std::path::PathBuf>,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut lines = BufReader::new(stream).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if let Err(e) = sink.append(stream_kind, &line).await {
                    tracing::warn!(error = %e, "log sink append failed");
                }
                let mut updated = false;
                if let Some(jid) = parse_salloc_jobid(&line) {
                    tx.send_modify(|s| {
                        if s.jobid.is_none() {
                            s.jobid = Some(jid);
                            updated = true;
                        }
                    });
                }
                if let Some(node) = parse_salloc_node(&line) {
                    tx.send_modify(|s| {
                        if s.node.is_none() {
                            s.node = Some(node);
                            updated = true;
                        }
                    });
                }
                if updated {
                    if let Some(p) = &persist_path {
                        if let Err(e) = write_atomic_json(p, &*tx.borrow()) {
                            tracing::warn!(error = %e, "persist after parse failed");
                        }
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(error = %e, "tee_lines read error");
                break;
            }
        }
    }
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn write_atomic_json(path: &Path, snap: &JobHandleSnapshot) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("persist_path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to mkdir -p {}", parent.display()))?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut tmp, snap)?;
    tmp.persist(path)
        .map_err(|e| anyhow!("persist rename failed: {e}"))?;
    Ok(())
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test --lib tssrun::handle`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/tssrun/handle.rs
git commit -m "feat(tssrun): add JobHandle (watch-based snapshot + tee/wait tasks)"
```

---

## Task 11: `attach_snapshot` wait error + `live_env` (`tssrun/handle.rs`)

**Files:**
- Modify: `src/tssrun/handle.rs`

- [ ] **Step 1: Add tests**

Add to the `tests` module:

```rust
    #[tokio::test]
    async fn attached_handle_wait_errors_with_not_owner() {
        let snap = snap_running();
        let mut h = JobHandle::attach_snapshot(snap, None);
        let err = h.wait().await.unwrap_err().to_string();
        assert!(err.contains("not owner"), "unexpected: {err}");
    }

    #[tokio::test]
    async fn live_env_returns_some_for_running_self() {
        if !cfg!(target_os = "linux") {
            return;
        }
        let argv = vec![
            "bash".to_string(),
            "-c".to_string(),
            "sleep 0.5".into(),
        ];
        let mut env = std::collections::HashMap::new();
        env.insert("MY_TEST_VAR".to_string(), "ok".to_string());
        let spawned = crate::dispatcher::TokioBackgroundDispatcher
            .spawn(&argv, &env, None).await.unwrap();
        let init = JobHandleSnapshot {
            pid: spawned.pid,
            argv,
            sent_env: env,
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid: None, node: None, finished: None,
        };
        let sink: std::sync::Arc<dyn crate::tssrun::log::JobLogSink> =
            std::sync::Arc::new(crate::tssrun::log::NullLogSink);
        let mut h = JobHandle::from_spawn(spawned, init, sink, None).await.unwrap();

        let live = h.live_env().await.unwrap();
        let live = live.expect("live env should be readable on linux for live child");
        assert_eq!(live.get("MY_TEST_VAR").map(String::as_str), Some("ok"));

        let _ = h.wait().await;
    }
```

- [ ] **Step 2: Run — expect PASS**

These tests should already pass given the Task 10 implementation
(`attach_snapshot` returns a handle with `wait_handle: None`,
`live_env` reads `/proc/<pid>/environ`).

Run: `cargo test --lib tssrun::handle`
Expected: 5 passed.

- [ ] **Step 3: Commit**

```bash
git add src/tssrun/handle.rs
git commit -m "test(tssrun): cover attach wait-error + live_env happy path"
```

---

## Task 12: `TssrunManager` (`tssrun/manager.rs`)

**Files:**
- Modify: `src/tssrun/manager.rs`

- [ ] **Step 1: Write failing tests**

Append to `src/tssrun/manager.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::tssrun::cmd::TssrunCmd;
    use crate::tssrun::log::{InMemoryLogSink, JobLogSink};
    use std::sync::Arc;

    #[tokio::test]
    async fn manager_spawn_with_bash_mock_returns_running_handle() {
        let mut cmd = TssrunCmd::new("/bin/true");
        cmd.tssrun_bin = "bash".to_string();
        cmd.args = vec![
            "-c".into(),
            r#"echo "salloc: Granted job allocation 999"; echo "salloc: Nodes node-x are ready for job"; sleep 0.05; echo done"#.into(),
        ];
        let dispatcher = crate::dispatcher::TokioBackgroundDispatcher;
        let sink: Arc<dyn JobLogSink> = Arc::new(InMemoryLogSink::default());
        let manager = TssrunManager::new(cmd).with_log_sink(sink);

        let mut handle = manager.spawn_with(&dispatcher).await.unwrap();
        let code = handle.wait().await.unwrap();
        assert_eq!(code, 0);
        assert_eq!(handle.snapshot().jobid, Some(999));
        assert_eq!(handle.snapshot().node.as_deref(), Some("node-x"));
    }

    #[tokio::test]
    async fn query_state_with_no_jobid_returns_default() {
        let cmd = TssrunCmd::new("/bin/true");
        let manager = TssrunManager::new(cmd);
        let snap = JobHandleSnapshot {
            pid: 1,
            argv: vec![],
            sent_env: Default::default(),
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid: None,
            node: None,
            finished: None,
        };
        let h = JobHandle::attach_snapshot(snap, None);
        let st = manager.query_state(&h).await.unwrap();
        assert_eq!(st, JobStatus::default());
    }

    #[tokio::test]
    async fn attach_by_file_round_trips_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("42.json");
        let snap = JobHandleSnapshot {
            pid: 42,
            argv: vec!["tssrun".into(), "/x".into()],
            sent_env: Default::default(),
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid: Some(7),
            node: Some("nA".into()),
            finished: None,
        };
        tokio::fs::write(&path, serde_json::to_vec_pretty(&snap).unwrap())
            .await
            .unwrap();

        let manager = TssrunManager::new(TssrunCmd::new("/bin/true"))
            .with_state_dir(tmp.path().to_path_buf());
        let h = manager.attach(AttachKey::File(path)).await.unwrap();
        assert_eq!(h.pid(), 42);
        assert_eq!(h.jobid(), Some(7));
    }

    #[tokio::test]
    async fn attach_by_jobid_finds_correct_file() {
        let tmp = tempfile::tempdir().unwrap();
        for (pid, jid) in [(10u32, 100u64), (11, 101)] {
            let snap = JobHandleSnapshot {
                pid,
                argv: vec![],
                sent_env: Default::default(),
                cwd: None,
                started_at_unix: 0,
                log_locations: LogLocations::None,
                jobid: Some(jid),
                node: None,
                finished: None,
            };
            tokio::fs::write(
                tmp.path().join(format!("{pid}.json")),
                serde_json::to_vec_pretty(&snap).unwrap(),
            )
            .await
            .unwrap();
        }
        let manager = TssrunManager::new(TssrunCmd::new("/bin/true"))
            .with_state_dir(tmp.path().to_path_buf());
        let h = manager.attach(AttachKey::JobId(101)).await.unwrap();
        assert_eq!(h.pid(), 11);
    }
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cargo test --lib tssrun::manager`
Expected: compile error `cannot find type TssrunManager`.

- [ ] **Step 3: Implement `TssrunManager`**

Replace `src/tssrun/manager.rs` with:

```rust
//! `TssrunManager` orchestrates spawn / attach / query_state.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};

use crate::JobStatus;
use crate::dispatcher::{BackgroundDispatcher, TokioBackgroundDispatcher};
use crate::runner;
use crate::tssrun::cmd::TssrunCmd;
use crate::tssrun::handle::{JobHandle, JobHandleSnapshot, LogLocations};
use crate::tssrun::log::{JobLogSink, StdLogSink};

/// Identifies a previously-persisted handle to attach to.
#[derive(Debug, Clone)]
pub enum AttachKey {
    Pid(u32),
    JobId(u64),
    File(PathBuf),
}

/// Orchestrates one or more tssrun invocations sharing a common log sink
/// and (optional) state directory.
pub struct TssrunManager {
    pub cmd: TssrunCmd,
    pub state_dir: Option<PathBuf>,
    pub log_sink: Arc<dyn JobLogSink>,
}

impl TssrunManager {
    pub fn new(cmd: TssrunCmd) -> Self {
        Self {
            cmd,
            state_dir: None,
            log_sink: Arc::new(StdLogSink),
        }
    }

    pub fn with_state_dir(mut self, dir: PathBuf) -> Self {
        self.state_dir = Some(dir);
        self
    }

    pub fn with_log_sink(mut self, sink: Arc<dyn JobLogSink>) -> Self {
        self.log_sink = sink;
        self
    }

    /// Spawn the configured command via [`TokioBackgroundDispatcher`].
    pub async fn spawn(&self) -> Result<JobHandle> {
        self.spawn_with(&TokioBackgroundDispatcher).await
    }

    /// Spawn via an explicit dispatcher.
    pub async fn spawn_with<D: BackgroundDispatcher>(&self, dispatcher: &D) -> Result<JobHandle> {
        let argv = self.cmd.build_argv()?;
        let cwd = self.cmd.cwd.as_deref();
        let spawned = dispatcher.spawn(&argv, &self.cmd.env, cwd).await?;

        let persist_path = self
            .state_dir
            .as_ref()
            .map(|d| d.join(format!("{}.json", spawned.pid)));

        let init = JobHandleSnapshot {
            pid: spawned.pid,
            argv,
            sent_env: self.cmd.env.clone(),
            cwd: self.cmd.cwd.clone(),
            started_at_unix: now_unix(),
            log_locations: LogLocations::None,
            jobid: None,
            node: None,
            finished: None,
        };
        JobHandle::from_spawn(spawned, init, self.log_sink.clone(), persist_path).await
    }

    /// Re-attach to a previously persisted handle.
    pub async fn attach(&self, key: AttachKey) -> Result<JobHandle> {
        let path = match key {
            AttachKey::File(p) => p,
            AttachKey::Pid(pid) => {
                let dir = self
                    .state_dir
                    .as_ref()
                    .ok_or_else(|| anyhow!("attach by pid requires state_dir"))?;
                dir.join(format!("{pid}.json"))
            }
            AttachKey::JobId(jobid) => {
                let dir = self
                    .state_dir
                    .as_ref()
                    .ok_or_else(|| anyhow!("attach by jobid requires state_dir"))?;
                find_by_jobid(dir, jobid).await?
            }
        };
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        let snap: JobHandleSnapshot = serde_json::from_slice(&bytes)?;
        Ok(JobHandle::attach_snapshot(snap, Some(path)))
    }

    /// Look up the SLURM lifecycle state via `sacct`.
    /// Returns a default `JobStatus` when the handle has no parsed jobid.
    pub async fn query_state(&self, handle: &JobHandle) -> Result<JobStatus> {
        match handle.jobid() {
            None => Ok(JobStatus::default()),
            Some(jid) => {
                let states = runner::query_job_states_batch(&[jid]).await?;
                Ok(states.get(&jid).cloned().unwrap_or_default())
            }
        }
    }
}

async fn find_by_jobid(dir: &std::path::Path, jobid: u64) -> Result<PathBuf> {
    let mut rd = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        if let Ok(bytes) = tokio::fs::read(entry.path()).await {
            if let Ok(snap) = serde_json::from_slice::<JobHandleSnapshot>(&bytes) {
                if snap.jobid == Some(jobid) {
                    return Ok(entry.path());
                }
            }
        }
    }
    Err(anyhow!(
        "no persisted handle in {} matched jobid {jobid}",
        dir.display()
    ))
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tssrun::cmd::TssrunCmd;
    use crate::tssrun::log::{InMemoryLogSink, JobLogSink};
    use std::sync::Arc;

    #[tokio::test]
    async fn manager_spawn_with_bash_mock_returns_running_handle() {
        let mut cmd = TssrunCmd::new("/bin/true");
        cmd.tssrun_bin = "bash".to_string();
        cmd.args = vec![
            "-c".into(),
            r#"echo "salloc: Granted job allocation 999"; echo "salloc: Nodes node-x are ready for job"; sleep 0.05; echo done"#.into(),
        ];
        let dispatcher = crate::dispatcher::TokioBackgroundDispatcher;
        let sink: Arc<dyn JobLogSink> = Arc::new(InMemoryLogSink::default());
        let manager = TssrunManager::new(cmd).with_log_sink(sink);

        let mut handle = manager.spawn_with(&dispatcher).await.unwrap();
        let code = handle.wait().await.unwrap();
        assert_eq!(code, 0);
        assert_eq!(handle.snapshot().jobid, Some(999));
        assert_eq!(handle.snapshot().node.as_deref(), Some("node-x"));
    }

    #[tokio::test]
    async fn query_state_with_no_jobid_returns_default() {
        let cmd = TssrunCmd::new("/bin/true");
        let manager = TssrunManager::new(cmd);
        let snap = JobHandleSnapshot {
            pid: 1,
            argv: vec![],
            sent_env: Default::default(),
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid: None,
            node: None,
            finished: None,
        };
        let h = JobHandle::attach_snapshot(snap, None);
        let st = manager.query_state(&h).await.unwrap();
        assert_eq!(st, JobStatus::default());
    }

    #[tokio::test]
    async fn attach_by_file_round_trips_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("42.json");
        let snap = JobHandleSnapshot {
            pid: 42,
            argv: vec!["tssrun".into(), "/x".into()],
            sent_env: Default::default(),
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid: Some(7),
            node: Some("nA".into()),
            finished: None,
        };
        tokio::fs::write(&path, serde_json::to_vec_pretty(&snap).unwrap())
            .await
            .unwrap();

        let manager = TssrunManager::new(TssrunCmd::new("/bin/true"))
            .with_state_dir(tmp.path().to_path_buf());
        let h = manager.attach(AttachKey::File(path)).await.unwrap();
        assert_eq!(h.pid(), 42);
        assert_eq!(h.jobid(), Some(7));
    }

    #[tokio::test]
    async fn attach_by_jobid_finds_correct_file() {
        let tmp = tempfile::tempdir().unwrap();
        for (pid, jid) in [(10u32, 100u64), (11, 101)] {
            let snap = JobHandleSnapshot {
                pid,
                argv: vec![],
                sent_env: Default::default(),
                cwd: None,
                started_at_unix: 0,
                log_locations: LogLocations::None,
                jobid: Some(jid),
                node: None,
                finished: None,
            };
            tokio::fs::write(
                tmp.path().join(format!("{pid}.json")),
                serde_json::to_vec_pretty(&snap).unwrap(),
            )
            .await
            .unwrap();
        }
        let manager = TssrunManager::new(TssrunCmd::new("/bin/true"))
            .with_state_dir(tmp.path().to_path_buf());
        let h = manager.attach(AttachKey::JobId(101)).await.unwrap();
        assert_eq!(h.pid(), 11);
    }
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `cargo test --lib tssrun::manager`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/tssrun/manager.rs
git commit -m "feat(tssrun): add TssrunManager with spawn/attach/query_state"
```

---

## Task 13: lib.rs re-exports

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Add re-exports**

Append to `src/lib.rs` after the existing `pub use dispatcher::...` line:

```rust
pub use dispatcher::{BackgroundDispatcher, SpawnedChild, TokioBackgroundDispatcher};
pub use tssrun::cmd::{Resource, TssrunCmd};
pub use tssrun::handle::{FinishedInfo, JobHandle, JobHandleSnapshot, LogLocations};
pub use tssrun::log::{
    FileLogSink, InMemoryLogSink, JobLogSink, LogStream, NullLogSink, StdLogSink,
};
pub use tssrun::manager::{AttachKey, TssrunManager};
pub use tssrun::parse::{parse_salloc_jobid, parse_salloc_node};
```

- [ ] **Step 2: Verify**

Run: `cargo test --lib`
Expected: all tests pass; no `unused import` warnings.

- [ ] **Step 3: Run clippy**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: zero warnings.

- [ ] **Step 4: Commit**

```bash
git add src/lib.rs
git commit -m "feat(tssrun): re-export public API from crate root"
```

---

## Task 14: Integration test (`tests/tssrun_integration.rs`)

**Files:**
- Create: `tests/tssrun_integration.rs`

- [ ] **Step 1: Write the test**

Create `tests/tssrun_integration.rs`:

```rust
//! End-to-end test: TssrunManager.spawn → handle.wait → snapshot reflects
//! parsed jobid/node and FileLogSink captures the child's stdout.

use std::sync::Arc;

use slurm_async_runner::{
    AttachKey, FileLogSink, JobLogSink, Resource, TssrunCmd, TssrunManager,
};

const BANNER: &str = r#"echo "salloc: Granted job allocation 12345"
echo "salloc: Nodes node-int are ready for job"
echo "hello from child"
"#;

#[tokio::test]
async fn spawn_then_wait_then_snapshot_then_attach() {
    let tmp = tempfile::tempdir().unwrap();
    let stdout_log = tmp.path().join("o.log");
    let stderr_log = tmp.path().join("e.log");
    let state_dir = tmp.path().join("state");
    tokio::fs::create_dir_all(&state_dir).await.unwrap();

    let mut cmd = TssrunCmd::new("/bin/true");
    cmd.tssrun_bin = "bash".to_string();
    cmd.args = vec!["-c".into(), BANNER.to_string()];
    cmd.rsc = Some(Resource {
        processes: Some(1),
        memory: Some("128M".into()),
        ..Default::default()
    });

    let sink: Arc<dyn JobLogSink> =
        Arc::new(FileLogSink::create(stdout_log.clone(), stderr_log.clone()).await.unwrap());
    let manager = TssrunManager::new(cmd)
        .with_state_dir(state_dir.clone())
        .with_log_sink(sink);

    let mut handle = manager.spawn().await.unwrap();
    let pid = handle.pid();
    let code = handle.wait().await.unwrap();
    assert_eq!(code, 0);

    let snap = handle.snapshot();
    assert_eq!(snap.jobid, Some(12345));
    assert_eq!(snap.node.as_deref(), Some("node-int"));
    assert!(snap.finished.is_some());

    let stdout_content = tokio::fs::read_to_string(&stdout_log).await.unwrap();
    assert!(stdout_content.contains("hello from child"));

    let path = state_dir.join(format!("{pid}.json"));
    assert!(path.exists(), "missing persisted handle at {}", path.display());

    let attached = manager.attach(AttachKey::File(path)).await.unwrap();
    assert_eq!(attached.pid(), pid);
    assert_eq!(attached.jobid(), Some(12345));
    assert_eq!(attached.node().as_deref(), Some("node-int"));
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test tssrun_integration`
Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/tssrun_integration.rs
git commit -m "test(tssrun): integration test covers spawn→wait→snapshot→attach"
```

---

## Task 15: pyo3 bindings (`src/py_export/tssrun.rs`)

**Files:**
- Create: `src/py_export/tssrun.rs`
- Modify: `src/py_export/mod.rs`

- [ ] **Step 1: Create the bindings file**

Create `src/py_export/tssrun.rs`:

```rust
//! pyo3 wrappers for the `slurm_async_runner._core.tssrun` submodule.

#![cfg(feature = "pyo3")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

use crate::tssrun::cmd::{Resource, TssrunCmd};
use crate::tssrun::handle::JobHandle;
use crate::tssrun::log::{FileLogSink, JobLogSink, NullLogSink, StdLogSink};
use crate::tssrun::manager::{AttachKey, TssrunManager};

// ---------- Resource ----------

#[pyclass(name = "Resource", module = "slurm_async_runner._core.tssrun", frozen)]
#[derive(Clone)]
pub struct PyResource(pub Resource);

#[pymethods]
impl PyResource {
    #[new]
    #[pyo3(signature = (processes = None, threads = None, cores = None, memory = None, gpus = None))]
    fn new(
        processes: Option<u32>,
        threads: Option<u32>,
        cores: Option<u32>,
        memory: Option<String>,
        gpus: Option<u32>,
    ) -> Self {
        Self(Resource { processes, threads, cores, memory, gpus })
    }
    #[getter] fn processes(&self) -> Option<u32> { self.0.processes }
    #[getter] fn threads(&self)   -> Option<u32> { self.0.threads }
    #[getter] fn cores(&self)     -> Option<u32> { self.0.cores }
    #[getter] fn memory(&self)    -> Option<String> { self.0.memory.clone() }
    #[getter] fn gpus(&self)      -> Option<u32> { self.0.gpus }
}

// ---------- TssrunCmd ----------

#[pyclass(name = "TssrunCmd", module = "slurm_async_runner._core.tssrun")]
#[derive(Clone)]
pub struct PyTssrunCmd(pub TssrunCmd);

#[pymethods]
impl PyTssrunCmd {
    #[new]
    #[pyo3(signature = (
        program,
        args = Vec::new(),
        queue = None,
        time_limit = None,
        rsc = None,
        x11 = false,
        env = HashMap::new(),
        cwd = None,
        tssrun_bin = "tssrun".to_string(),
    ))]
    fn new(
        program: PathBuf,
        args: Vec<String>,
        queue: Option<String>,
        time_limit: Option<String>,
        rsc: Option<PyResource>,
        x11: bool,
        env: HashMap<String, String>,
        cwd: Option<PathBuf>,
        tssrun_bin: String,
    ) -> Self {
        Self(TssrunCmd {
            tssrun_bin,
            queue,
            time_limit,
            rsc: rsc.map(|r| r.0),
            x11,
            program,
            args,
            env,
            cwd,
        })
    }

    fn build_argv(&self) -> PyResult<Vec<String>> {
        self.0.build_argv().map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

// ---------- LogSink ----------

#[pyclass(name = "LogSink", module = "slurm_async_runner._core.tssrun", frozen)]
#[derive(Clone)]
pub struct PyLogSink(pub Arc<dyn JobLogSink>);

#[pyfunction]
#[pyo3(name = "null_log_sink")]
fn null_log_sink() -> PyLogSink {
    PyLogSink(Arc::new(NullLogSink))
}

#[pyfunction]
#[pyo3(name = "std_log_sink")]
fn std_log_sink() -> PyLogSink {
    PyLogSink(Arc::new(StdLogSink))
}

#[pyfunction]
#[pyo3(name = "file_log_sink")]
fn file_log_sink<'py>(py: Python<'py>, stdout: PathBuf, stderr: PathBuf) -> PyResult<Bound<'py, PyAny>> {
    future_into_py(py, async move {
        let sink = FileLogSink::create(stdout, stderr).await
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyLogSink(Arc::new(sink)))
    })
}

// ---------- JobHandle ----------

#[pyclass(name = "TssrunJobHandle", module = "slurm_async_runner._core.tssrun")]
pub struct PyTssrunJobHandle {
    inner: Arc<tokio::sync::Mutex<JobHandle>>,
}

#[pymethods]
impl PyTssrunJobHandle {
    #[getter]
    fn pid<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.pid()) })
    }

    #[getter]
    fn jobid<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.jobid()) })
    }

    #[getter]
    fn node<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.node()) })
    }

    #[getter]
    fn sent_env<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.sent_env()) })
    }

    fn live_env<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.lock().await.live_env().await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    fn is_running<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.is_running()) })
    }

    fn exit_code<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.exit_code()) })
    }

    fn wait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.lock().await.wait().await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }
}

// ---------- TssrunManager ----------

#[pyclass(name = "TssrunManager", module = "slurm_async_runner._core.tssrun")]
#[derive(Clone)]
pub struct PyTssrunManager(pub Arc<TssrunManager>);

#[pymethods]
impl PyTssrunManager {
    #[new]
    #[pyo3(signature = (cmd, state_dir = None, log_sink = None))]
    fn new(cmd: PyTssrunCmd, state_dir: Option<PathBuf>, log_sink: Option<PyLogSink>) -> Self {
        let mut m = TssrunManager::new(cmd.0);
        if let Some(d) = state_dir {
            m = m.with_state_dir(d);
        }
        if let Some(s) = log_sink {
            m = m.with_log_sink(s.0);
        }
        Self(Arc::new(m))
    }

    fn spawn<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let m = self.0.clone();
        future_into_py(py, async move {
            let handle = m.spawn().await.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTssrunJobHandle { inner: Arc::new(tokio::sync::Mutex::new(handle)) })
        })
    }

    fn attach_pid<'py>(&self, py: Python<'py>, pid: u32) -> PyResult<Bound<'py, PyAny>> {
        let m = self.0.clone();
        future_into_py(py, async move {
            let h = m.attach(AttachKey::Pid(pid)).await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTssrunJobHandle { inner: Arc::new(tokio::sync::Mutex::new(h)) })
        })
    }

    fn attach_jobid<'py>(&self, py: Python<'py>, jobid: u64) -> PyResult<Bound<'py, PyAny>> {
        let m = self.0.clone();
        future_into_py(py, async move {
            let h = m.attach(AttachKey::JobId(jobid)).await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTssrunJobHandle { inner: Arc::new(tokio::sync::Mutex::new(h)) })
        })
    }

    fn attach_file<'py>(&self, py: Python<'py>, path: PathBuf) -> PyResult<Bound<'py, PyAny>> {
        let m = self.0.clone();
        future_into_py(py, async move {
            let h = m.attach(AttachKey::File(path)).await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTssrunJobHandle { inner: Arc::new(tokio::sync::Mutex::new(h)) })
        })
    }
}

// ---------- submodule wiring ----------

#[pymodule]
#[pyo3(name = "tssrun")]
pub mod inner_module {
    #[pymodule_export]
    use super::PyResource;
    #[pymodule_export]
    use super::PyTssrunCmd;
    #[pymodule_export]
    use super::PyLogSink;
    #[pymodule_export]
    use super::PyTssrunJobHandle;
    #[pymodule_export]
    use super::PyTssrunManager;
    #[pymodule_export]
    use super::null_log_sink;
    #[pymodule_export]
    use super::std_log_sink;
    #[pymodule_export]
    use super::file_log_sink;
}
```

- [ ] **Step 2: Wire submodule into `src/py_export/mod.rs`**

Replace `src/py_export/mod.rs` with:

```rust
#![cfg(feature = "pyo3")]

use pyo3::prelude::*;

pub mod manager;
pub mod runner;
pub mod tssrun;

pyo3_stub_gen::define_stub_info_gatherer!(stub_info);

#[pymodule]
#[pyo3(name = "_core")]
mod slurm_async_runner {
    use super::*;
    const PYTHON_MODULE_NAME: &str = "slurm_async_runner._core";

    #[pymodule_export]
    use crate::py_export::sum_as_string;

    #[pymodule_export]
    use super::runner::inner_module as runner_module;

    #[pymodule_export]
    use super::manager::inner_module as manager_module;

    #[pymodule_export]
    use super::tssrun::inner_module as tssrun_module;

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        let py = m.py();
        py.import("sys")?
            .getattr("modules")?
            .set_item(PYTHON_MODULE_NAME, m)?;
        log::debug!("{} Rust module initialized", PYTHON_MODULE_NAME);
        Ok(())
    }
}

#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "slurm_async_runner._core")]
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}
```

- [ ] **Step 3: Build with maturin**

Run: `uv run maturin develop`
Expected: build succeeds.

- [ ] **Step 4: Smoke check from Python**

Run:
```bash
uv run python -c "from slurm_async_runner._core.tssrun import TssrunCmd, Resource, TssrunManager, null_log_sink; \
print(TssrunCmd(program='/bin/true').build_argv())"
```
Expected output: `['tssrun', '/bin/true']`.

- [ ] **Step 5: Commit**

```bash
git add src/py_export/tssrun.rs src/py_export/mod.rs
git commit -m "feat(py_export): expose tssrun submodule (Resource, TssrunCmd, JobHandle, TssrunManager, log sinks)"
```

---

## Task 16: Hand-written stubs (`python/slurm_async_runner/_core/tssrun.pyi`)

**Files:**
- Create: `python/slurm_async_runner/_core/tssrun.pyi`

- [ ] **Step 1: Write the stub file**

Create `python/slurm_async_runner/_core/tssrun.pyi`:

```python
# Hand-written stubs for slurm_async_runner._core.tssrun.
# pyo3-stub-gen does not derive stubs for pyclasses inside #[pymodule]
# sub-modules wired via #[pymodule_export], so this file is maintained
# by hand. Keep it in sync with src/py_export/tssrun.rs.
# ruff: noqa: E501, F401, F403, F405

import builtins
import os
from collections.abc import Awaitable
from typing import final

__all__ = [
    "Resource",
    "TssrunCmd",
    "LogSink",
    "TssrunJobHandle",
    "TssrunManager",
    "null_log_sink",
    "std_log_sink",
    "file_log_sink",
]

@final
class Resource:
    """Resource spec for ``tssrun --rsc p=:t=:c=:m=:g=``."""
    processes: builtins.int | None
    threads: builtins.int | None
    cores: builtins.int | None
    memory: builtins.str | None
    gpus: builtins.int | None

    def __init__(
        self,
        processes: builtins.int | None = ...,
        threads: builtins.int | None = ...,
        cores: builtins.int | None = ...,
        memory: builtins.str | None = ...,
        gpus: builtins.int | None = ...,
    ) -> None: ...

@final
class TssrunCmd:
    """Spec for one ``tssrun`` invocation. Pure data + ``build_argv``."""
    def __init__(
        self,
        program: builtins.str | os.PathLike[builtins.str],
        args: builtins.list[builtins.str] = ...,
        queue: builtins.str | None = ...,
        time_limit: builtins.str | None = ...,
        rsc: Resource | None = ...,
        x11: builtins.bool = ...,
        env: builtins.dict[builtins.str, builtins.str] = ...,
        cwd: builtins.str | os.PathLike[builtins.str] | None = ...,
        tssrun_bin: builtins.str = ...,
    ) -> None: ...
    def build_argv(self) -> builtins.list[builtins.str]: ...

@final
class LogSink:
    """Opaque handle to a Rust log sink. Construct via the factory helpers."""

def null_log_sink() -> LogSink: ...
def std_log_sink() -> LogSink: ...
def file_log_sink(stdout: builtins.str, stderr: builtins.str) -> Awaitable[LogSink]: ...

@final
class TssrunJobHandle:
    """In-process or attached handle to a ``tssrun`` child."""
    @property
    def pid(self) -> Awaitable[builtins.int]: ...
    @property
    def jobid(self) -> Awaitable[builtins.int | None]: ...
    @property
    def node(self) -> Awaitable[builtins.str | None]: ...
    @property
    def sent_env(self) -> Awaitable[builtins.dict[builtins.str, builtins.str]]: ...
    def live_env(self) -> Awaitable[builtins.dict[builtins.str, builtins.str] | None]: ...
    def is_running(self) -> Awaitable[builtins.bool]: ...
    def exit_code(self) -> Awaitable[builtins.int | None]: ...
    def wait(self) -> Awaitable[builtins.int]: ...

@final
class TssrunManager:
    """Orchestrates spawn / attach / query_state for a ``TssrunCmd``."""
    def __init__(
        self,
        cmd: TssrunCmd,
        state_dir: builtins.str | os.PathLike[builtins.str] | None = ...,
        log_sink: LogSink | None = ...,
    ) -> None: ...
    def spawn(self) -> Awaitable[TssrunJobHandle]: ...
    def attach_pid(self, pid: builtins.int) -> Awaitable[TssrunJobHandle]: ...
    def attach_jobid(self, jobid: builtins.int) -> Awaitable[TssrunJobHandle]: ...
    def attach_file(
        self, path: builtins.str | os.PathLike[builtins.str]
    ) -> Awaitable[TssrunJobHandle]: ...
```

- [ ] **Step 2: Format**

Run: `uv run ruff format python/slurm_async_runner/_core/tssrun.pyi`
Expected: no diff or minor whitespace fix.

- [ ] **Step 3: Commit**

```bash
git add python/slurm_async_runner/_core/tssrun.pyi
git commit -m "docs(stubs): hand-written .pyi for slurm_async_runner._core.tssrun"
```

---

## Task 17: Python integration tests (`python/tests/test_tssrun.py`)

**Files:**
- Create: `python/tests/test_tssrun.py`

- [ ] **Step 1: Write the tests**

Create `python/tests/test_tssrun.py`:

```python
"""Python-side tests for the tssrun submodule.

Uses bash as a stand-in for tssrun to emit the same ``salloc:`` banner.
"""

import asyncio
import tempfile
from pathlib import Path

from slurm_async_runner._core.tssrun import (
    Resource,
    TssrunCmd,
    TssrunManager,
    file_log_sink,
    null_log_sink,
    std_log_sink,
)

BANNER = (
    'echo "salloc: Granted job allocation 555"; '
    'echo "salloc: Nodes node-py are ready for job"; '
    'sleep 0.05; '
    'echo done'
)


def _bash_cmd() -> TssrunCmd:
    return TssrunCmd(
        program="/bin/true", args=["-c", BANNER], tssrun_bin="bash"
    )


def test_resource_render_via_argv() -> None:
    cmd = TssrunCmd(
        program="/bin/true",
        rsc=Resource(processes=4, memory="2G"),
    )
    argv = cmd.build_argv()
    assert "--rsc" in argv
    assert "p=4:m=2G" in argv


def test_null_log_sink_does_not_raise() -> None:
    null_log_sink()


def test_std_log_sink_does_not_raise() -> None:
    std_log_sink()


def test_manager_spawn_then_wait_then_jobid() -> None:
    async def run() -> None:
        manager = TssrunManager(_bash_cmd())
        h = await manager.spawn()
        assert (await h.pid) > 0
        code = await h.wait()
        assert code == 0
        assert (await h.jobid) == 555
        assert (await h.node) == "node-py"

    asyncio.run(run())


def test_manager_with_file_log_sink_persists_logs() -> None:
    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            o = Path(td) / "o.log"
            e = Path(td) / "e.log"
            sink = await file_log_sink(str(o), str(e))
            manager = TssrunManager(_bash_cmd(), log_sink=sink)
            h = await manager.spawn()
            await h.wait()
            assert "done" in o.read_text()

    asyncio.run(run())


def test_manager_attach_file_round_trip() -> None:
    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            manager = TssrunManager(_bash_cmd(), state_dir=td)
            h = await manager.spawn()
            pid = await h.pid
            await h.wait()
            path = Path(td) / f"{pid}.json"
            assert path.exists()
            attached = await manager.attach_file(str(path))
            assert (await attached.pid) == pid
            assert (await attached.jobid) == 555

    asyncio.run(run())
```

- [ ] **Step 2: Rebuild + run**

Run:
```bash
uv run maturin develop
uv run pytest python/tests/test_tssrun.py -v
```
Expected: all tests pass.

- [ ] **Step 3: Run the full Python test suite**

Run: `uv run pytest python/tests -v`
Expected: existing tests still pass.

- [ ] **Step 4: Commit**

```bash
git add python/tests/test_tssrun.py
git commit -m "test(tssrun): Python end-to-end tests for spawn/wait/file-log/attach"
```

---

## Task 18: README + CHANGELOG

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add README section**

Append to `README.md` immediately before the `## Development` section:

````markdown
## tssrun (background mode + env inspection)

For the Kyoto-U ECCS interactive batch frontend `tssrun`, the
`slurm_async_runner.tssrun` submodule offers a non-blocking spawn API
and snapshot-based environment inspection.

```python
import asyncio
from slurm_async_runner._core.tssrun import (
    Resource, TssrunCmd, TssrunManager, file_log_sink,
)

async def main():
    cmd = TssrunCmd(
        program="/work/job.sh",
        queue="gr19999b", time_limit="1:00:00",
        rsc=Resource(processes=4, memory="2G"),
    )
    sink = await file_log_sink("/tmp/job.out", "/tmp/job.err")
    manager = TssrunManager(cmd, state_dir="/var/lib/slurm-runner", log_sink=sink)

    handle = await manager.spawn()
    print("pid", await handle.pid, "jobid", await handle.jobid)
    code = await handle.wait()
    print("exit", code)

asyncio.run(main())
```

A separate process can later call ``await manager.attach_pid(pid)``
(or ``attach_jobid`` / ``attach_file``) to inspect the persisted snapshot
read-only.
````

- [ ] **Step 2: Add CHANGELOG entry**

Insert at the top of `CHANGELOG.md` (under the heading, above existing entries):

```markdown
## Unreleased

### Added

- `slurm_async_runner::tssrun` Rust module and `slurm_async_runner._core.tssrun`
  Python submodule. Provides `TssrunCmd` (typed `tssrun` argv builder),
  `TssrunManager` (background spawn / attach / state query), `JobHandle`
  with watch-based snapshot, and a pluggable `JobLogSink` trait
  (`Null/Std/InMemory/FileLogSink`).
- `BackgroundDispatcher` trait + `TokioBackgroundDispatcher` for
  non-blocking child spawn alongside the existing synchronous
  `JobDispatcher`.
```

- [ ] **Step 3: Commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs: README + CHANGELOG entry for tssrun wrapper"
```

---

## Task 19: Final verification gauntlet

**Files:** none — verification only.

- [ ] **Step 1: Full Rust suite**

Run: `cargo test --all-targets`
Expected: all tests pass.

- [ ] **Step 2: Clippy**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: zero warnings.

- [ ] **Step 3: Format check**

Run: `cargo fmt --all -- --check`
Expected: zero diff.

- [ ] **Step 4: Python suite**

Run: `uv run maturin develop && uv run pytest python/tests -v`
Expected: all tests pass.

- [ ] **Step 5: Ruff**

Run: `uv run ruff check python/`
Expected: zero issues.

- [ ] **Step 6: Stub regeneration sanity**

Run: `cargo run --bin stub_gen && uv run ruff format python/`
Expected: no behavioural diff against the hand-written stubs.

- [ ] **Step 7: Final format-fixup commit (if any diff)**

```bash
git status --porcelain
# If non-empty:
git add -A
git commit -m "chore: format pass after tssrun feature complete"
```
