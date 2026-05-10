# sbatch Phase 2 P1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Phase 2 Tier 1 の追加機能 5 件と DRY 化 1 件を additive・非破壊で実装する — sacct `ExitCode` パーサ、`--no-requeue` / `--comment` flags、`SbatchJobHandle::log_lines` / `read_log_to_end` ログ読み取り API、`absolutize` の `src/util/path.rs` への DRY 集約。

**Architecture:** 既存 Phase 1 の Spec/Runtime 二軸を維持。`runner.rs` の sacct 引数に `ExitCode` 列を追加し新関数 `query_job_states_with_exit_code_with` を提供（既存 `query_job_states_batch_with` は不変、後方互換維持）。ログ読み取りは `tokio::fs` ベースで `SbatchJobHandle` に non-async-locking メソッドとして追加。`absolutize` は新 `src/util/path.rs` に `pub(crate) fn` として集約し、`src/sbatch/cmd.rs` / `src/tssrun/cmd.rs` / `src/manager.rs` の 3 箇所が import で利用する。

**Tech Stack:** Rust 1.81+, tokio, pyo3 abi3-py312, anyhow, thiserror, serde / serde_json, chrono, uuid v7, pytest (Python smoke), cargo-llvm-cov

**Spec reference:** `docs/superpowers/specs/2026-05-10-sbatch-phase2-design.md` §4.1, §4.4, §4.5, §4.7, §4.9, §8 (P1)

**Phase 1 invariants to preserve (from `docs/attention_phase2.md` §2):** kind 文字列 `"sbatch"` 不変 / 新 snapshot フィールド `#[serde(default)]` / sacct は opt-in（`refresh()` には絶対入れない）/ async 内 lock は `tokio::sync::Mutex` / 公開 attach 経路は kind peek 必須 / `JobState` variant 追加禁止。

---

## File Structure

**新規作成:**
- `src/util/mod.rs` — module 宣言
- `src/util/path.rs` — `pub(crate) fn absolutize(p: &Path) -> Result<String>`

**修正:**
- `src/lib.rs` — `mod util;` 追加（pub にしない）
- `src/sbatch/cmd.rs` — 自前 `absolutize` 削除 → import 化、`SbatchCmd { no_requeue: bool, comment: Option<String> }` フィールド追加、`build_argv` 拡張、`SbatchCmd::new` で新フィールドを default 初期化
- `src/sbatch/parse.rs` — `pub(crate) fn parse_sacct_exit_code(field: &str) -> Option<i32>` 追加
- `src/sbatch/handle.rs` — `LogStream`/`LogReadError` 追加、`SbatchJobHandle::log_lines` / `read_log_to_end` 実装、`exit_code` の Phase 1 limitation doc 削除（3 箇所）、`refresh_with_sacct` 内で sacct 結果から `exit_code` を埋める
- `src/runner.rs` — 新関数 `query_job_states_with_exit_code_with`（sacct argv に `ExitCode` 列を追加、戻り値は `HashMap<u64, JobOutcome>` 形式）と `parse_sacct_with_exit_code`、`pub struct JobOutcome { status: JobStatus, exit_code: Option<i32> }` を追加。既存 `query_job_states_batch_with` は不変（後方互換）
- `src/tssrun/cmd.rs` — 自前 `absolutize` 削除 → import 化
- `src/manager.rs` — inline `std::path::absolute` 使用箇所を `util::path::absolutize` に置換
- `src/py_export/sbatch.rs` — `PySbatchCmd::new` の kwargs に `no_requeue` / `comment` 追加、`PySbatchJobHandle::log_lines` / `read_log_to_end` メソッド追加（pyo3-async）
- `python/slurm_async_runner/_core/sbatch.pyi` — 新 kwargs と log read API の type stub、3 箇所の Phase 1 limitation docstring 削除
- `python/tests/test_sbatch.py` または既存テストファイル — log read smoke test 追加
- `CHANGELOG.md` — `[Unreleased]` に Phase 2 P1 項目追記

**新規・拡張テスト:**
- `src/util/path.rs` 内 `#[cfg(test)] mod tests`
- `src/sbatch/parse.rs` 内 `parse_sacct_exit_code` のユニットテスト
- `src/sbatch/cmd.rs` 内 `--no-requeue`/`--comment` の argv テスト
- `src/sbatch/handle.rs` 内 `log_lines`/`read_log_to_end` のユニットテスト
- `src/runner.rs` 内 `parse_sacct_with_exit_code` のユニットテスト + `query_job_states_with_exit_code_with` の integration

---

## Task 1: `src/util/path.rs` に共有 `absolutize` を新設

**Files:**
- Create: `src/util/mod.rs`
- Create: `src/util/path.rs`
- Modify: `src/lib.rs` (top-level mod 宣言追加)

- [ ] **Step 1.1: `src/util/mod.rs` を作成**

```rust
//! Crate-internal utilities shared across submission backends.
//!
//! This module is `pub(crate)` only; it intentionally does not appear
//! in the public API surface.

pub(crate) mod path;
```

- [ ] **Step 1.2: `src/util/path.rs` を作成（テスト含む）**

```rust
//! Path utilities shared across submission backends (`tssrun::cmd`,
//! `sbatch::cmd`, `manager`). Phase 2 P1 consolidates three duplicate
//! implementations of `absolutize` into this single source.

use anyhow::{Context, Result};
use std::path::Path;

/// Convert a possibly-relative path to its absolute UTF-8 string form.
///
/// Returns an error if `std::path::absolute` fails (e.g. CWD unreadable)
/// or if the resulting path is not valid UTF-8.
pub(crate) fn absolutize(p: &Path) -> Result<String> {
    let abs = std::path::absolute(p)
        .with_context(|| format!("failed to absolutize {}", p.display()))?;
    abs.into_os_string()
        .into_string()
        .map_err(|os| anyhow::anyhow!("non-UTF8 path: {os:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn absolute_path_roundtrips() {
        let abs = absolutize(Path::new("/tmp/foo")).unwrap();
        assert_eq!(abs, "/tmp/foo");
    }

    #[test]
    fn relative_path_is_made_absolute() {
        let abs = absolutize(Path::new("foo.sh")).unwrap();
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(abs, format!("{}/foo.sh", cwd.display()));
    }

    #[test]
    fn handles_dot_segments() {
        let abs = absolutize(Path::new("./bar")).unwrap();
        let cwd = std::env::current_dir().unwrap();
        // std::path::absolute does not normalize "." but produces a valid
        // absolute path. Just assert prefix to remain robust.
        assert!(
            abs.starts_with(&cwd.display().to_string()),
            "abs={abs} should start with {cwd:?}"
        );
        // sanity-check it can be turned back into PathBuf
        let _ = PathBuf::from(&abs);
    }
}
```

- [ ] **Step 1.3: `src/lib.rs` に `mod util;` を追加**

`src/lib.rs` を開き、既存 `pub mod entities;` の直後、`pub mod error;` より前に以下を追加:

```rust
// Crate-internal utilities — not part of the public API.
mod util;
```

`pub mod` ではなく `mod` であることに注意（外部 API 表面を増やさない）。

- [ ] **Step 1.4: テスト実行 → 通過確認**

Run: `cargo test --lib --features pyo3 util::path::tests -- --nocapture`
Expected: `running 3 tests` のうち全 PASS。

- [ ] **Step 1.5: フォーマット・clippy**

Run: `cargo fmt --all && cargo clippy --all-targets --features pyo3 -- -D warnings`
Expected: 0 warnings, 0 changes from fmt.

- [ ] **Step 1.6: コミット**

```bash
git add src/util/ src/lib.rs
git commit -m "feat(util): add shared absolutize at src/util/path.rs"
```

---

## Task 2: 既存 `absolutize` 重複を 3 箇所で解消

**Files:**
- Modify: `src/sbatch/cmd.rs:96-102` (削除) + import 追加
- Modify: `src/tssrun/cmd.rs:97-103` (削除) + import 追加
- Modify: `src/manager.rs` (inline `std::path::absolute` 使用箇所)

- [ ] **Step 2.1: `src/sbatch/cmd.rs` を更新**

ファイル冒頭の `use crate::entities::slurm::{...};` 直後に以下を追加:

```rust
use crate::util::path::absolutize;
```

そして既存の `fn absolutize` 関数定義（line 96 付近の `fn absolutize(p: &Path) -> Result<String> { ... }` ブロック全体、約 7 行）を **削除**。

- [ ] **Step 2.2: `src/tssrun/cmd.rs` を更新**

同じく ファイル冒頭の use 句に `use crate::util::path::absolutize;` を追加し、既存の `fn absolutize(...)` を削除。

- [ ] **Step 2.3: `src/manager.rs` の inline 呼び出しを置換**

`src/manager.rs` の該当箇所を確認: `grep -n "std::path::absolute" src/manager.rs`。
該当する関数の中身を以下のパターンに置換:

```rust
// before
let abs = std::path::absolute(batch_file)
    .with_context(|| format!("failed to absolutize {}", batch_file.display()))?;
abs.into_os_string()
    .into_string()
    .map_err(|os| anyhow::anyhow!("non-UTF8 path: {os:?}"))

// after
crate::util::path::absolutize(batch_file)
```

ファイル冒頭で必要なら `use crate::util::path::absolutize;` を追加し、`absolutize(batch_file)` 呼び出しに簡素化。

- [ ] **Step 2.4: 全テスト実行 → 全 pass 確認**

Run: `cargo test --lib --features pyo3`
Expected: ALL PASS（Phase 1 のテストすべて含む）。回帰がないこと。

特に `cargo test --lib --features pyo3 sbatch::cmd::tests::full_flags_cpu_variant_argv_layout`、`tssrun` 系テストが pass すること。

- [ ] **Step 2.5: clippy + fmt 確認**

Run: `cargo fmt --all --check && cargo clippy --all-targets --features pyo3 -- -D warnings`
Expected: 0 warnings, fmt 差分なし。

- [ ] **Step 2.6: コミット**

```bash
git add src/sbatch/cmd.rs src/tssrun/cmd.rs src/manager.rs
git commit -m "refactor(util): consolidate absolutize duplicates into util::path

- Remove fn absolutize from sbatch/cmd.rs, tssrun/cmd.rs
- Replace inline std::path::absolute in manager.rs
- All three call sites now import crate::util::path::absolutize"
```

---

## Task 3: `parse_sacct_exit_code` を `src/sbatch/parse.rs` に追加

**Files:**
- Modify: `src/sbatch/parse.rs` (関数 + tests 追加)

- [ ] **Step 3.1: 失敗するテストを書く**

`src/sbatch/parse.rs` の末尾、既存 `#[cfg(test)] mod tests` ブロック内（最後の `}` の直前）に以下を追加:

```rust
    // ---- parse_sacct_exit_code ----

    #[test]
    fn parses_clean_zero_exit() {
        assert_eq!(parse_sacct_exit_code("0:0"), Some(0));
    }

    #[test]
    fn parses_nonzero_exit_no_signal() {
        assert_eq!(parse_sacct_exit_code("139:0"), Some(139));
    }

    #[test]
    fn parses_signal_kill_with_zero_exit() {
        // SIGKILL = 9 -> shell convention 128 + 9 = 137
        assert_eq!(parse_sacct_exit_code("0:9"), Some(137));
    }

    #[test]
    fn parses_signal_segv_with_nonzero_exit() {
        // SIGSEGV = 11 -> shell convention 128 + 11 = 139.
        // Slurm sometimes emits "139:11" — signal field is authoritative.
        assert_eq!(parse_sacct_exit_code("139:11"), Some(139));
    }

    #[test]
    fn rejects_garbled_field() {
        assert_eq!(parse_sacct_exit_code(""), None);
        assert_eq!(parse_sacct_exit_code("abc"), None);
        assert_eq!(parse_sacct_exit_code(":0"), None);
        assert_eq!(parse_sacct_exit_code("0:"), None);
        assert_eq!(parse_sacct_exit_code("0"), None);
    }
```

- [ ] **Step 3.2: テスト実行 → コンパイルエラー（関数未定義）で fail 確認**

Run: `cargo test --lib --features pyo3 sbatch::parse::tests -- --nocapture 2>&1 | head -20`
Expected: FAIL with `error[E0425]: cannot find function 'parse_sacct_exit_code'`

- [ ] **Step 3.3: 関数を実装**

`src/sbatch/parse.rs` の `resolve_log_path` 関数の **下**、`#[cfg(test)] mod tests` の **上** に以下を追加:

```rust
/// Parse sacct's `ExitCode` column ("<exit>:<signal>") into an i32 exit code.
///
/// Slurm の sacct は次のような形を返す:
/// - `"0:0"` — 正常終了
/// - `"139:0"` — exit code 139（プロセスが直接 exit 139 を返した）
/// - `"0:9"` — シグナル SIGKILL で終了。shell convention で 128+9=137 が exit
/// - `"139:11"` — シグナル SIGSEGV、shell convention で 128+11=139
///
/// シグナル成分 (`:<signal>`) が **非ゼロ** のときは shell convention に従い
/// `128 + signal` を返す。両成分がゼロまたは exit のみ非ゼロなら exit を返す。
/// 形式不正は `None`。
pub(crate) fn parse_sacct_exit_code(field: &str) -> Option<i32> {
    let (exit_s, signal_s) = field.split_once(':')?;
    let exit = exit_s.parse::<i32>().ok()?;
    let signal = signal_s.parse::<i32>().ok()?;
    if signal != 0 {
        Some(128 + signal)
    } else {
        Some(exit)
    }
}
```

- [ ] **Step 3.4: テスト再実行 → 全 pass 確認**

Run: `cargo test --lib --features pyo3 sbatch::parse::tests -- --nocapture`
Expected: 全 PASS（既存 `parse_submitted_jobid` 系 + 新規 `parse_sacct_exit_code` 系）。

- [ ] **Step 3.5: コミット**

```bash
git add src/sbatch/parse.rs
git commit -m "feat(sbatch): add parse_sacct_exit_code parser for sacct ExitCode column"
```

---

## Task 4: `runner.rs` に `query_job_states_with_exit_code_with` を追加

**Files:**
- Modify: `src/runner.rs`
- Modify: `src/lib.rs` (re-export 追加)

`refresh_with_sacct` から呼べる新しい sacct クエリを **既存 `query_job_states_batch_with` を壊さずに** 追加する。

- [ ] **Step 4.1: `JobOutcome` 構造体と新パーサのテストを書く**

`src/runner.rs` の `#[cfg(test)] mod tests` ブロック内、既存 `parse_sacct_*` テストの直後に以下を追加:

```rust
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
        // Older sacct may emit only 3 fields (no ExitCode column requested).
        let text = "12345|COMPLETED|None\n";
        let m = parse_sacct_with_exit_code(text);
        let oc = m.get(&12345).expect("jobid present");
        assert_eq!(oc.exit_code, None);
        assert_eq!(oc.status.state, JobState::Completed);
    }
```

- [ ] **Step 4.2: テスト実行 → fail 確認**

Run: `cargo test --lib --features pyo3 runner::tests::parse_sacct_with_exit_code -- --nocapture 2>&1 | head -20`
Expected: FAIL with `cannot find function 'parse_sacct_with_exit_code'`.

- [ ] **Step 4.3: `JobOutcome` と `parse_sacct_with_exit_code` を実装**

`src/runner.rs` の `parse_sacct` 関数の直後（既存 `parse_qgroup_l` の前）に以下を追加:

```rust
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
```

なお `parse_sacct::parse_sacct_exit_code` は `pub(crate)` で sbatch モジュール内、runner からは `use crate::sbatch::parse::parse_sacct_exit_code` で import 可能。

- [ ] **Step 4.4: テスト実行 → 全 pass 確認**

Run: `cargo test --lib --features pyo3 runner::tests::parse_sacct_with_exit_code -- --nocapture`
Expected: 4 tests PASS。

- [ ] **Step 4.5: 新クエリ関数 `query_job_states_with_exit_code_with` のテストを書く**

`src/runner.rs` の test mod 末尾に以下を追加:

```rust
    // ---- query_job_states_with_exit_code_with ----

    #[tokio::test]
    async fn query_with_exit_code_squeue_only_reports_no_exit_code() {
        // Job is still in squeue (active) -> sacct not called -> exit_code = None
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
        let m = query_job_states_with_exit_code_with(&D, &[12345]).await.unwrap();
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
                    String::new()  // missing -> sacct fallback
                } else if bin == "sacct" {
                    // Verify caller asked for ExitCode column
                    let format_idx = argv.iter().position(|a| a == "-o").unwrap();
                    assert!(argv[format_idx + 1].contains("ExitCode"),
                        "sacct argv must include ExitCode column, got: {:?}", argv);
                    "12345|COMPLETED|None|0:0\n".to_string()
                } else {
                    String::new()
                };
                Ok((0, out))
            }
        }
        let m = query_job_states_with_exit_code_with(&D, &[12345]).await.unwrap();
        let oc = m.get(&12345).unwrap();
        assert_eq!(oc.status.state, JobState::Completed);
        assert_eq!(oc.exit_code, Some(0));
    }
```

- [ ] **Step 4.6: テスト実行 → fail 確認**

Run: `cargo test --lib --features pyo3 runner::tests::query_with_exit_code -- --nocapture 2>&1 | head -20`
Expected: FAIL with `cannot find function 'query_job_states_with_exit_code_with'`

- [ ] **Step 4.7: `query_job_states_with_exit_code_with` を実装**

`src/runner.rs` の `query_job_states_batch_with` 関数の **直後** に以下を追加:

```rust
/// Like [`query_job_states_batch_with`] but additionally captures sacct's
/// `ExitCode` column and returns it as part of [`JobOutcome`].
///
/// Phase 2 P1 introduces this so `SbatchJobHandle::refresh_with_sacct` can
/// persist the exit code into `FinishedInfo::exit_code`.
///
/// One squeue + at most one sacct call per invocation; jobids still active
/// in squeue do not trigger sacct (mirrors the legacy function's policy).
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
        }
    }
    Ok(out)
}
```

- [ ] **Step 4.8: テスト再実行 → 全 pass 確認**

Run: `cargo test --lib --features pyo3 runner::tests -- --nocapture`
Expected: 既存 + 新規すべて PASS。

- [ ] **Step 4.9: lib.rs から re-export**

`src/lib.rs` の既存 `pub use runner::{query_job_states_batch, query_job_states_batch_with};` 行を以下に置換:

```rust
pub use runner::{
    query_job_states_batch, query_job_states_batch_with,
    query_job_states_with_exit_code_with, JobOutcome,
};
```

- [ ] **Step 4.10: clippy + fmt + コミット**

Run: `cargo fmt --all && cargo clippy --all-targets --features pyo3 -- -D warnings`
Expected: 0 warnings.

```bash
git add src/runner.rs src/lib.rs
git commit -m "feat(runner): add query_job_states_with_exit_code_with + JobOutcome

Captures sacct ExitCode column alongside State/Reason. Existing
query_job_states_batch_with API is unchanged for backward compatibility."
```

---

## Task 5: `refresh_with_sacct` に exit_code を流し込む + Phase 1 limitation doc 削除

**Files:**
- Modify: `src/sbatch/handle.rs`

- [ ] **Step 5.1: 失敗するテストを書く**

`src/sbatch/handle.rs` の `mod tests` 内、既存 `refresh_with_sacct_calls_sacct_once_after_vanish` テストの **直後** に以下を追加:

```rust
    #[tokio::test]
    async fn refresh_with_sacct_populates_exit_code_on_completed() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        // sacct now emits 4 columns (JobID|State|Reason|ExitCode)
        let canned = std::sync::Arc::new(CannedDispatcher::new("", "", "12345|COMPLETED|None|0:0\n"));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        let finished = after.lifecycle.finished.expect("finished should be Some");
        assert_eq!(finished.final_state, crate::JobState::Completed);
        assert_eq!(finished.exit_code, Some(0));
    }

    #[tokio::test]
    async fn refresh_with_sacct_populates_exit_code_on_signaled() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new("", "", "12345|CANCELLED|None|0:9\n"));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        let finished = after.lifecycle.finished.expect("finished should be Some");
        // SIGKILL = 9 -> 128 + 9 = 137
        assert_eq!(finished.exit_code, Some(137));
    }
```

- [ ] **Step 5.2: テスト実行 → fail 確認**

Run: `cargo test --lib --features pyo3 sbatch::handle::tests::refresh_with_sacct_populates -- --nocapture 2>&1 | head -30`
Expected: FAIL — `assertion failed: left: None, right: Some(0)` (exit_code が現状 None のまま)。

- [ ] **Step 5.3: `refresh_with_sacct` 内で新クエリを使うよう変更**

`src/sbatch/handle.rs` の `refresh_with_sacct` 関数（line 262 付近）を以下のように書き換える:

```rust
/// Heavyweight finalizer. Calls `refresh()` first; only invokes
/// sacct if the job has actually left both `qgroup -l` and `squeue`
/// **and** `lifecycle.finished` is still None. Otherwise behaves
/// identically to `refresh()`.
pub async fn refresh_with_sacct(&self) -> anyhow::Result<SbatchJobSnapshot> {
    let mut snap = self.refresh().await?;
    if snap.lifecycle.finished.is_some() {
        return Ok(snap);
    }
    if !snap.lifecycle.left_active_listing {
        return Ok(snap);
    }

    let inner = &*self.0;
    let _guard = inner.refresh_lock.lock().await;
    let view = crate::dispatcher::DynView(&*inner.dispatcher);

    // Phase 2 P1: switch to the exit-code-aware query so we can populate
    // FinishedInfo::exit_code instead of leaving it None.
    let map =
        crate::runner::query_job_states_with_exit_code_with(&view, &[snap.jobid]).await?;
    let outcome = map.get(&snap.jobid).cloned().unwrap_or(crate::runner::JobOutcome {
        status: Default::default(),
        exit_code: None,
    });
    snap.lifecycle.finished = Some(FinishedInfo {
        final_state: outcome.status.state,
        final_reason: outcome.status.reason,
        exit_code: outcome.exit_code,
        finished_at: chrono::Utc::now(),
    });
    inner.store.save(&snap).await?;
    let _ = inner.snapshot_tx.send(snap.clone());
    Ok(snap)
}
```

- [ ] **Step 5.4: 既存テスト `refresh_with_sacct_calls_sacct_once_after_vanish` の sacct fixture を 4 列形式に更新**

`src/sbatch/handle.rs` の同テスト内 `CannedDispatcher::new("", "", "12345|COMPLETED|None\n")` となっている箇所を **`"12345|COMPLETED|None|0:0\n"`** に変更。3 列でも fallback (`exit_code = None`) で通るが、新クエリの実フォーマットに合わせる。

- [ ] **Step 5.5: 3 箇所の Phase 1 limitation doc-comment を削除**

`src/sbatch/handle.rs` の以下 3 箇所のドキュメントコメントから `**Phase 1 limitation:** ...` の段落（4 行）を削除:

(a) `SbatchLifecycle::exit_code` (line 78-87 付近)

```rust
// before
/// Exit code if the child exited normally; `None` if killed by signal,
/// or if `finished` is not yet recorded.
///
/// **Phase 1 limitation:** `refresh_with_sacct()` does NOT currently
/// parse the sacct `ExitCode` column, so this method returns `None`
/// even after a successful `refresh_with_sacct()` call. A future
/// release will extend `parse_sacct` to capture exit codes.
pub fn exit_code(&self) -> Option<i32> {

// after
/// Exit code if the child exited normally; `None` if killed by signal,
/// or if `finished` is not yet recorded.
pub fn exit_code(&self) -> Option<i32> {
```

(b) `SbatchJobSnapshot::exit_code` (line 111-120 付近) — 同様に 4 行削除。

(c) `SbatchJobHandle::exit_code` (line 212-221 付近) — 同様に 4 行削除。

- [ ] **Step 5.6: テスト実行 → 全 pass 確認**

Run: `cargo test --lib --features pyo3 sbatch::handle::tests -- --nocapture`
Expected: ALL PASS（既存 + 新規 2 件）。

- [ ] **Step 5.7: clippy + fmt + コミット**

Run: `cargo fmt --all && cargo clippy --all-targets --features pyo3 -- -D warnings`

```bash
git add src/sbatch/handle.rs
git commit -m "feat(sbatch): wire sacct ExitCode into FinishedInfo.exit_code

- refresh_with_sacct now uses query_job_states_with_exit_code_with
- Drops 3 Phase 1 limitation doc-comments on exit_code methods
- Existing CannedDispatcher fixture updated to 4-column sacct output"
```

---

## Task 6: `SbatchCmd` に `--no-requeue` / `--comment` フィールド追加

**Files:**
- Modify: `src/sbatch/cmd.rs`

- [ ] **Step 6.1: 失敗するテストを書く**

`src/sbatch/cmd.rs` の `mod tests` 内、`gpu_variant_renders_g_flag` テストの **直後** に以下を追加:

```rust
    #[test]
    fn no_requeue_flag_is_emitted_when_true() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.no_requeue = true;
        let argv = cmd.build_argv().unwrap();
        assert!(argv.iter().any(|a| a == "--no-requeue"));
    }

    #[test]
    fn no_requeue_flag_is_omitted_when_false() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a == "--no-requeue"));
    }

    #[test]
    fn comment_flag_emits_value() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.comment = Some("post-deadline rerun".to_string());
        let argv = cmd.build_argv().unwrap();
        let i = argv.iter().position(|a| a == "--comment").expect("--comment present");
        assert_eq!(argv[i + 1], "post-deadline rerun");
    }

    #[test]
    fn comment_omitted_when_none() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a == "--comment"));
    }
```

- [ ] **Step 6.2: テスト実行 → fail 確認**

Run: `cargo test --lib --features pyo3 sbatch::cmd::tests -- --nocapture 2>&1 | head -30`
Expected: コンパイルエラー — `no field 'no_requeue' on type 'SbatchCmd'` および `'comment'`。

- [ ] **Step 6.3: フィールド追加 + `new()` + `build_argv()` を更新**

`src/sbatch/cmd.rs` の `SbatchCmd` 構造体定義を以下のように更新（既存フィールドの末尾、`script` の前に 2 フィールド追加）:

```rust
pub struct SbatchCmd {
    pub sbatch_bin: String,

    pub job_name: Option<String>,
    pub partition: Option<JobPartition>,

    pub time_limit: Option<JobTimeLimit>,
    pub rsc: Option<ResourceSpec>,

    pub output: Option<String>,
    pub error: Option<String>,
    pub chdir: Option<PathBuf>,

    pub env: HashMap<String, String>,

    /// `--no-requeue` flag. When `true`, the job is not requeued on node failure.
    pub no_requeue: bool,

    /// `--comment` flag value. When `Some`, emitted as `--comment <value>`.
    pub comment: Option<String>,

    pub script: PathBuf,
    pub args: Vec<String>,
}
```

`new()` に 2 フィールドの default 初期化を追加:

```rust
pub fn new(script: impl Into<PathBuf>) -> Self {
    Self {
        sbatch_bin: "sbatch".to_string(),
        job_name: None,
        partition: None,
        time_limit: None,
        rsc: None,
        output: None,
        error: None,
        chdir: None,
        env: HashMap::new(),
        no_requeue: false,
        comment: None,
        script: script.into(),
        args: Vec::new(),
    }
}
```

`build_argv()` の `--export` ブロックの **直後**、`argv.push(absolutize(&self.script)?);` の **直前** に以下を追加:

```rust
        if self.no_requeue {
            argv.push("--no-requeue".to_string());
        }
        if let Some(c) = &self.comment {
            argv.push("--comment".to_string());
            argv.push(c.clone());
        }
```

- [ ] **Step 6.4: テスト再実行 → 全 pass 確認**

Run: `cargo test --lib --features pyo3 sbatch::cmd::tests -- --nocapture`
Expected: 既存 + 新規 4 件すべて PASS。

- [ ] **Step 6.5: clippy + fmt + コミット**

```bash
git add src/sbatch/cmd.rs
git commit -m "feat(sbatch): add --no-requeue and --comment fields to SbatchCmd"
```

---

## Task 7: `SbatchJobHandle` に `log_lines` / `read_log_to_end` API を追加

**Files:**
- Modify: `src/sbatch/handle.rs`
- Modify: `src/lib.rs`
- Modify (if needed): `Cargo.toml` (dev-dependencies に tempfile)

- [ ] **Step 7.1: `LogStream` enum と `LogReadError` を追加**

`src/sbatch/handle.rs` の use 文末尾、既存 import の直後（line 22 付近）に以下を追加:

```rust
use thiserror::Error;
```

そして `SbatchAttachKey` 定義の **直前** （line 122 付近）に以下を追加:

```rust
/// Which job log stream to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

/// Errors that can occur while reading a job's log file.
#[derive(Debug, Error)]
pub enum LogReadError {
    #[error("log path not resolved on snapshot (template missing)")]
    PathNotResolved,
    #[error("io error reading log: {0}")]
    Io(#[from] std::io::Error),
}
```

注: `crate::tssrun::log::LogStream` という同名 enum が tssrun 側にすでに存在する場合は import 衝突を避けるため、本 enum はあくまで `crate::sbatch::handle::LogStream` として独立定義する（tssrun のものを reuse しない理由: sbatch のログは SLURM が直接書き込む別物で、tssrun の tee buffer とは ownership モデルが異なる。Phase 3 で trait 化を検討するときに統合）。

- [ ] **Step 7.2: 失敗するテストを書く**

`src/sbatch/handle.rs` の `mod tests` 末尾に以下を追加:

```rust
    // ---- log_lines / read_log_to_end ----

    use std::io::Write as _;

    fn snap_with_log_path(jobid: u64, stdout_path: &str, stderr_path: &str) -> SbatchJobSnapshot {
        let mut s = snap(jobid);
        s.log = LogPathSpec {
            output_template: Some(stdout_path.to_string()),
            error_template: Some(stderr_path.to_string()),
        };
        s
    }

    #[tokio::test]
    async fn log_lines_returns_empty_when_file_missing() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;
        let s = snap_with_log_path(12345, "/nonexistent/stdout-%j.out", "/nonexistent/stderr-%j.err");
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s, store, dispatcher);
        let lines = h.log_lines(LogStream::Stdout, 5).await.unwrap();
        assert_eq!(lines, Vec::<String>::new());
    }

    #[tokio::test]
    async fn log_lines_returns_path_not_resolved_when_no_template() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;
        let mut s = snap(12345);
        s.log = LogPathSpec::default();  // both templates None
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s, store, dispatcher);
        let err = h.log_lines(LogStream::Stdout, 5).await.unwrap_err();
        assert!(matches!(err, LogReadError::PathNotResolved));
    }

    #[tokio::test]
    async fn log_lines_returns_last_n_lines() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        let dir = tempfile::tempdir().unwrap();
        let stdout_path = dir.path().join("stdout-12345.out");
        let mut f = std::fs::File::create(&stdout_path).unwrap();
        for i in 0..20 {
            writeln!(f, "line {i}").unwrap();
        }
        drop(f);

        let s = snap_with_log_path(
            12345,
            stdout_path.to_str().unwrap(),
            "ignored.err",
        );
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s, store, dispatcher);

        let lines = h.log_lines(LogStream::Stdout, 5).await.unwrap();
        assert_eq!(lines, vec!["line 15", "line 16", "line 17", "line 18", "line 19"]);
    }

    #[tokio::test]
    async fn read_log_to_end_returns_full_content() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        let dir = tempfile::tempdir().unwrap();
        let stdout_path = dir.path().join("stdout-12345.out");
        std::fs::write(&stdout_path, "hello\nworld\n").unwrap();

        let s = snap_with_log_path(
            12345,
            stdout_path.to_str().unwrap(),
            "ignored.err",
        );
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s, store, dispatcher);

        let content = h.read_log_to_end(LogStream::Stdout).await.unwrap();
        assert_eq!(content, "hello\nworld\n");
    }
```

- [ ] **Step 7.3: 必要なら `tempfile` を `Cargo.toml` の dev-dependencies に追加**

`Cargo.toml` の `[dev-dependencies]` セクション (なければ作成) を確認:

Run: `grep -A 10 '\[dev-dependencies\]' Cargo.toml`

`tempfile = "3"` が無ければ以下を追加:

```toml
[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 7.4: テスト実行 → fail 確認**

Run: `cargo test --lib --features pyo3 sbatch::handle::tests::log_ -- --nocapture 2>&1 | head -30`
Expected: コンパイルエラー — `no method named 'log_lines' found for struct 'SbatchJobHandle'`。

- [ ] **Step 7.5: `log_lines` / `read_log_to_end` を実装**

`src/sbatch/handle.rs` の `SbatchJobHandle` impl ブロックに以下のメソッドを追加（`exit_code` getter の **直後**、`refresh` の **直前** に挿入）:

```rust
    // -------- Log read API (Phase 2 P1) --------

    /// Read the last `n` lines of the job's stdout/stderr log file.
    ///
    /// Returns an empty `Vec` if the log file does not yet exist (job
    /// pending or just submitted). Returns `LogReadError::PathNotResolved`
    /// if the snapshot does not carry the corresponding log template.
    /// Other I/O errors are propagated as `LogReadError::Io`.
    ///
    /// Phase 2 P1 implements this with a full read of the file followed
    /// by line splitting; for very large logs (> ~10MB) consider Phase 3
    /// optimization with reverse seek.
    pub async fn log_lines(
        &self,
        stream: LogStream,
        n: usize,
    ) -> Result<Vec<String>, LogReadError> {
        let snap = self.0.snapshot_tx.borrow().clone();
        let path = match stream {
            LogStream::Stdout => snap.output_path(),
            LogStream::Stderr => snap.error_path(),
        };
        let path = path.ok_or(LogReadError::PathNotResolved)?;
        match tokio::fs::read_to_string(&path).await {
            Ok(content) => {
                let lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
                let start = lines.len().saturating_sub(n);
                Ok(lines[start..].to_vec())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(LogReadError::Io(e)),
        }
    }

    /// Read the full contents of the job's stdout/stderr log file.
    ///
    /// Returns an empty string if the log file does not yet exist.
    /// Same error semantics as [`SbatchJobHandle::log_lines`] otherwise.
    pub async fn read_log_to_end(
        &self,
        stream: LogStream,
    ) -> Result<String, LogReadError> {
        let snap = self.0.snapshot_tx.borrow().clone();
        let path = match stream {
            LogStream::Stdout => snap.output_path(),
            LogStream::Stderr => snap.error_path(),
        };
        let path = path.ok_or(LogReadError::PathNotResolved)?;
        match tokio::fs::read_to_string(&path).await {
            Ok(content) => Ok(content),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
            Err(e) => Err(LogReadError::Io(e)),
        }
    }
```

- [ ] **Step 7.6: テスト実行 → 全 pass 確認**

Run: `cargo test --lib --features pyo3 sbatch::handle::tests -- --nocapture`
Expected: ALL PASS（log_* 4 件含む）。

- [ ] **Step 7.7: lib.rs に `LogStream`/`LogReadError` を re-export**

`src/lib.rs` の sbatch re-export セクションを確認:

Run: `grep -n "sbatch::handle\|sbatch::manager" src/lib.rs`

既存スタイルに合わせて以下を追加（tssrun 側の `LogStream` と衝突しない名前で）:

```rust
pub use sbatch::handle::{LogStream as SbatchLogStream, LogReadError as SbatchLogReadError};
```

注: tssrun 側で `LogStream` が re-export されている場合の名前衝突を避けるため、`SbatchLogStream` / `SbatchLogReadError` にリネーム re-export する。

- [ ] **Step 7.8: clippy + fmt + コミット**

```bash
git add src/sbatch/handle.rs src/lib.rs Cargo.toml
git commit -m "feat(sbatch): add log_lines and read_log_to_end on SbatchJobHandle

LogStream { Stdout, Stderr } enum + LogReadError { PathNotResolved, Io }.
Missing files map to Ok(empty), other IO errors propagate."
```

---

## Task 8: pyo3 binding に新フィールド + log read API を追加

**Files:**
- Modify: `src/py_export/sbatch.rs`

- [ ] **Step 8.1: `PySbatchCmd::new` の既存シグネチャを確認**

Run: `grep -A 35 'impl PySbatchCmd' src/py_export/sbatch.rs | head -45`

既存の引数名・順序を把握する（`#[pyo3(signature = (...))]` ブロックと `fn new(...)` 引数）。

- [ ] **Step 8.2: `PySbatchCmd::new` に `no_requeue` / `comment` kwargs を追加**

`src/py_export/sbatch.rs` の `PySbatchCmd::new` を以下のパターンに拡張する。**既存引数の順序は変えず、新引数を末尾に追加** すること（破壊回避）:

```rust
    #[new]
    #[pyo3(signature = (
        script,
        *,
        sbatch_bin = None,
        job_name = None,
        partition = None,
        time_limit = None,
        rsc = None,
        output = None,
        error = None,
        chdir = None,
        env = None,
        no_requeue = false,
        comment = None,
        args = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        script: PathBuf,
        sbatch_bin: Option<String>,
        job_name: Option<String>,
        partition: Option<String>,
        time_limit: Option<crate::JobTimeLimit>,
        rsc: Option<crate::ResourceSpec>,
        output: Option<String>,
        error: Option<String>,
        chdir: Option<PathBuf>,
        env: Option<HashMap<String, String>>,
        no_requeue: bool,
        comment: Option<String>,
        args: Option<Vec<String>>,
    ) -> Self {
        let mut cmd = SbatchCmd::new(script);
        if let Some(b) = sbatch_bin { cmd.sbatch_bin = b; }
        cmd.job_name = job_name;
        cmd.partition = partition;
        cmd.time_limit = time_limit;
        cmd.rsc = rsc;
        cmd.output = output;
        cmd.error = error;
        cmd.chdir = chdir;
        if let Some(e) = env { cmd.env = e; }
        cmd.no_requeue = no_requeue;
        cmd.comment = comment;
        if let Some(a) = args { cmd.args = a; }
        Self(cmd)
    }
```

実際の既存型 (`Option<crate::JobTimeLimit>` 等の path) は Step 8.1 の grep 結果に合わせること。

- [ ] **Step 8.3: `PySbatchJobHandle` に `log_lines` / `read_log_to_end` を追加**

`src/py_export/sbatch.rs` の `PySbatchJobHandle` impl ブロック (line 159 付近)、`wait_terminal` メソッドの **直後** に以下を追加:

```rust
    /// stream: 0 = stdout, 1 = stderr
    fn log_lines<'py>(
        &self,
        py: Python<'py>,
        stream: u8,
        n: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stream = match stream {
                0 => crate::sbatch::handle::LogStream::Stdout,
                1 => crate::sbatch::handle::LogStream::Stderr,
                other => {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "log_lines: stream must be 0 (stdout) or 1 (stderr), got {other}"
                    )));
                }
            };
            h.log_lines(stream, n).await.map_err(py_err)
        })
    }

    fn read_log_to_end<'py>(
        &self,
        py: Python<'py>,
        stream: u8,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stream = match stream {
                0 => crate::sbatch::handle::LogStream::Stdout,
                1 => crate::sbatch::handle::LogStream::Stderr,
                other => {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "read_log_to_end: stream must be 0 (stdout) or 1 (stderr), got {other}"
                    )));
                }
            };
            h.read_log_to_end(stream).await.map_err(py_err)
        })
    }
```

注: `pyo3_async_runtimes::tokio::future_into_py` の正確な path は既存 `refresh<'py>` / `wait_terminal<'py>` で使われているものに合わせる（`grep -n "future_into_py" src/py_export/sbatch.rs` で確認）。

- [ ] **Step 8.4: ビルド確認**

Run: `cargo build --features pyo3 2>&1 | head -40`
Expected: 成功。warnings なし。

- [ ] **Step 8.5: clippy + fmt**

Run: `cargo fmt --all && cargo clippy --all-targets --features pyo3 -- -D warnings`

- [ ] **Step 8.6: コミット**

```bash
git add src/py_export/sbatch.rs
git commit -m "feat(py): expose no_requeue/comment kwargs and log read methods"
```

---

## Task 9: Python type stub と pytest smoke を更新

**Files:**
- Modify: `python/slurm_async_runner/_core/sbatch.pyi`
- Modify or create: `python/tests/test_sbatch.py`

- [ ] **Step 9.1: 既存 stub の構造を確認**

Run: `cat python/slurm_async_runner/_core/sbatch.pyi`

`PySbatchCmd.__init__` のシグネチャ、`PySbatchJobHandle` のメソッド一覧、`exit_code` 関連の docstring を把握する。

- [ ] **Step 9.2: `PySbatchCmd.__init__` に新 kwargs を追加**

`PySbatchCmd.__init__` の type stub に以下の引数を **末尾** に追加（既存引数の順序は維持）:

```python
class PySbatchCmd:
    def __init__(
        self,
        script: str | os.PathLike[str],
        *,
        sbatch_bin: str | None = None,
        job_name: str | None = None,
        partition: str | None = None,
        time_limit: JobTimeLimit | None = None,
        rsc: ResourceSpec | None = None,
        output: str | None = None,
        error: str | None = None,
        chdir: str | os.PathLike[str] | None = None,
        env: dict[str, str] | None = None,
        no_requeue: bool = False,
        comment: str | None = None,
        args: list[str] | None = None,
    ) -> None: ...
```

実際の既存 stub の引数名・順序は `grep -A 20 "class PySbatchCmd" python/slurm_async_runner/_core/sbatch.pyi` で確認し、**変更せず追加だけ** すること。

- [ ] **Step 9.3: `PySbatchJobHandle` に log read メソッドの type stub を追加**

`class PySbatchJobHandle` の末尾、`wait_terminal` メソッドの直後に以下を追加:

```python
    async def log_lines(self, stream: int, n: int) -> list[str]:
        """Read the last `n` lines of the job's stdout (stream=0) or stderr (stream=1).

        Returns an empty list if the log file does not yet exist.
        Raises ValueError if `stream` is not 0 or 1.
        Raises a SbatchLogReadError-like exception if the snapshot has no
        log template, or on other I/O errors.
        """
        ...

    async def read_log_to_end(self, stream: int) -> str:
        """Read the full contents of the job's stdout (0) or stderr (1) log.

        Returns an empty string if the log file does not yet exist.
        Same error semantics as `log_lines`.
        """
        ...
```

- [ ] **Step 9.4: 3 箇所の Phase 1 limitation docstring を削除**

`exit_code` プロパティ／メソッドの docstring から「Phase 1 limitation: refresh_with_sacct() does NOT currently parse the sacct ExitCode column ...」の段落を削除（ファイル内 3 箇所、Rust 側と対応）。

Run: `grep -n "Phase 1 limitation" python/slurm_async_runner/_core/sbatch.pyi`
すべての該当行を含む docstring 段落を削除。

- [ ] **Step 9.5: pytest smoke を追加**

`python/tests/` ディレクトリ構造を確認:

Run: `ls python/tests/`

`test_sbatch.py` または `test_sbatch_smoke.py` が既存なら同ファイルに追加、無ければ既存の最も類似したファイルに追加。以下の smoke を追加:

```python
def test_sbatch_cmd_no_requeue_kwarg(tmp_path):
    """no_requeue=True kwarg should produce --no-requeue in argv."""
    from slurm_async_runner._core import sbatch as core_sbatch

    script = tmp_path / "job.sh"
    script.write_text("#!/bin/sh\necho hi\n")

    cmd = core_sbatch.PySbatchCmd(str(script), no_requeue=True)
    argv = cmd.build_argv()
    assert "--no-requeue" in argv


def test_sbatch_cmd_comment_kwarg(tmp_path):
    """comment kwarg should produce --comment <value> in argv."""
    from slurm_async_runner._core import sbatch as core_sbatch

    script = tmp_path / "job.sh"
    script.write_text("#!/bin/sh\necho hi\n")

    cmd = core_sbatch.PySbatchCmd(str(script), comment="phase 2 smoke")
    argv = cmd.build_argv()
    i = argv.index("--comment")
    assert argv[i + 1] == "phase 2 smoke"
```

注: `core_sbatch.PySbatchCmd` の正確な import path は既存 pytest を見て合わせる（`grep -rn "PySbatchCmd" python/tests/`）。

- [ ] **Step 9.6: pytest 実行 → pass 確認**

Run: `uv run pytest python/tests/ -k 'no_requeue or comment_kwarg' -v`
Expected: 2 PASS。

`uv` が無い環境なら `python -m pytest python/tests/ -k '...' -v`。

- [ ] **Step 9.7: 全 pytest 実行 → 回帰なし**

Run: `uv run pytest python/tests/ -v`
Expected: 全 PASS。

- [ ] **Step 9.8: コミット**

```bash
git add python/slurm_async_runner/_core/sbatch.pyi python/tests/
git commit -m "docs(py): sync .pyi for new kwargs and log read API; remove Phase 1 limitation notes"
```

---

## Task 10: CHANGELOG を更新

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 10.1: `[Unreleased]` セクションに Phase 2 P1 項目を追記**

`CHANGELOG.md` の `## [Unreleased]` セクションに以下のサブセクションを追加（既存 `### Changed (BREAKING)` の **後**、もしくは適切な位置）:

```markdown
### Added (Phase 2 P1)

- **sacct `ExitCode` parser.** `SbatchJobHandle::refresh_with_sacct()` now
  populates `FinishedInfo::exit_code` (and the `exit_code()` getter on
  `SbatchLifecycle` / `SbatchJobSnapshot` / `SbatchJobHandle`). Sacct is
  still opt-in. See `parse_sacct_exit_code` in `src/sbatch/parse.rs` and
  the new `query_job_states_with_exit_code_with` in `src/runner.rs`
  (the legacy `query_job_states_batch_with` is unchanged).
- **`SbatchCmd::no_requeue: bool`** — emits `--no-requeue` when `true`.
  Python: `PySbatchCmd(..., no_requeue=True)`.
- **`SbatchCmd::comment: Option<String>`** — emits `--comment <value>`
  when `Some`. Python: `PySbatchCmd(..., comment="...")`.
- **`SbatchJobHandle::log_lines` / `read_log_to_end`** — read job
  stdout/stderr via `LogStream { Stdout, Stderr }`. Missing files return
  empty (`Ok(vec![])` / `Ok(String::new())`); template missing returns
  `LogReadError::PathNotResolved`; other I/O via `LogReadError::Io`.
  Python: `PySbatchJobHandle.log_lines(stream: int, n: int)` and
  `read_log_to_end(stream: int)`.

### Refactor (Phase 2 P1)

- **DRY: `absolutize` consolidated to `src/util/path.rs`.** The duplicate
  `fn absolutize` in `src/sbatch/cmd.rs` and `src/tssrun/cmd.rs`, plus
  the inline `std::path::absolute` use in `src/manager.rs`, all now go
  through `crate::util::path::absolutize`. No public API change.

### Docs (Phase 2 P1)

- Removed Phase 1 "this returns None until Phase 2" limitation notes from
  `exit_code` doc-comments on `SbatchLifecycle`, `SbatchJobSnapshot`, and
  `SbatchJobHandle`, and from the corresponding Python `.pyi` docstrings.
```

- [ ] **Step 10.2: コミット**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): record Phase 2 P1 additions, refactor, and doc cleanup"
```

---

## Task 11: 最終検証 (全パイプライン)

**Files:** none (検証のみ)

- [ ] **Step 11.1: フォーマット**

Run: `cargo fmt --all --check`
Expected: 差分なし（exit 0）。差分が出たら `cargo fmt --all` で適用、再実行。

- [ ] **Step 11.2: clippy 全 target**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings`
Expected: warnings 0。

- [ ] **Step 11.3: 全 unit + integration test**

Run: `cargo test --lib --features pyo3`
Expected: ALL PASS。

- [ ] **Step 11.4: pytest**

Run: `uv run pytest python/tests/ -v`
Expected: ALL PASS。

- [ ] **Step 11.5: ビルド (release)**

Run: `cargo build --release --features pyo3`
Expected: 成功。

- [ ] **Step 11.6: optionally — coverage 確認**

Run: `cargo llvm-cov --lib --features pyo3 --summary-only` (cargo-llvm-cov インストール済みの場合)
Expected: 80%+。下回ったら不足箇所のテスト追加を検討。

- [ ] **Step 11.7: branch 状態のチェック**

Run: `git log --oneline ed8b15a..HEAD`
Expected: P1 のコミット 9〜10 件が並ぶこと（task 1〜10 各 1 commit）。

Run: `git status`
Expected: clean。

- [ ] **Step 11.8: PR 提出可能状態の確認**

spec §11 のチェックリスト（特に以下）を目視確認:
- [ ] vocab 重複なし: 本 PR では entities 側の新規追加なし（`SlurmSignalSpec` は P4）
- [ ] kind 文字列の追加なし
- [ ] 新 snapshot フィールドなし（P1 では snapshot は不変、`array_*` は P5）
- [ ] sacct 呼び出しは `refresh_with_sacct` 内のみ（`refresh()` には入れていない）
- [ ] async 内 lock は `tokio::sync::Mutex` のみ（log read は lock を取らない、snapshot_tx の `.borrow()` は lock-free）
- [ ] CHANGELOG `[Unreleased]` 更新済み
- [ ] `python/.../*.pyi` 同期 + Phase 1 limitation doc 削除済み
- [ ] 全テスト pass

問題なければ P1 plan 完了。`develop` への PR を作成する場合:

```bash
git push -u origin sbatch-module-phase2
gh pr create --base develop --title "Phase 2 P1: sacct ExitCode + small flags + log read + absolutize DRY" \
  --body "Implements docs/superpowers/plans/2026-05-10-sbatch-phase2-p1.md. See CHANGELOG."
```

---

## Self-Review notes (for the implementing engineer)

このプランの設計判断を理解するために `docs/superpowers/specs/2026-05-10-sbatch-phase2-design.md` の以下節を参照すること:

- §2.1 vocab 重複定義の禁止（本 P1 は entities 側変更なし — Phase 2 ルールの予習として読む）
- §4.1 sacct ExitCode parser
- §4.4–4.5 small flags
- §4.7 log read API
- §4.9 absolutize DRY
- §11 PR チェックリスト

`docs/attention_phase2.md` §2 の不変条件（特に §2.5 `JobState::is_running` / `is_terminal` の同期）を本 P1 では一切触らないこと。`JobState` 周りはいずれも Phase 1 で固まった public API。

---

## Plan dependencies

P1 の **後** に着手可能:
- P5 (`--array` 配列ジョブ): `parse::resolve_log_path` を `%A`/`%a`/`%u`/`%N` に拡張するときに本 P1 の log read API を array task per snapshot で再利用
- P6 (`run()` + `cancel()`): 本 P1 の sacct ExitCode を `mgr.run() -> FinishedInfo` の返却で使う

P1 と **並列実行可能** (依存なし):
- P2 (`--dependency` + `--mail-*`)
- P3 (`--export` validation)
- P4 (`SlurmSignalSpec` entity + 配線)
