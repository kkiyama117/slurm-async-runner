# sbatch モジュール Phase 2 設計

- **Date**: 2026-05-10
- **Status**: Draft (brainstorming 完了、ユーザレビュー待ち)
- **Targets**: `crate::sbatch::*` (Rust) + `slurm_async_runner._core.sbatch` (Python) + `crate::entities::slurm::sbatch_options::*` 拡張
- **Phase 1 baseline**: `develop` ブランチ `ed8b15a`（`docs/attention_phase2.md` 参照）
- **References**:
  - Phase 1 設計: `docs/superpowers/specs/2026-05-10-sbatch-module-design.md`
  - Phase 1 計画: `docs/superpowers/plans/2026-05-10-sbatch-module.md`
  - Phase 1 ハンドオーバ: `docs/attention_phase2.md`
  - KUDPC: <https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/batch>
  - KUDPC tips (array/dependency): <https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/tips>
  - SLURM: <https://slurm.schedmd.com/sbatch.html>

---

## 1. 背景

Phase 1 で `crate::sbatch` の **fire-and-forget + 後追い監視 (attach)** が確立した。Phase 2 は **Phase 1 が意図的に out of scope とした 8 項目 + クロスカット改善 2 項目** を additive・非破壊で導入する。

### 1.1 Phase 2 in-scope (10 項目)

| # | 項目 | Tier |
|---|---|---|
| 1 | sacct `ExitCode` parser | Tier 1 |
| 2 | `--array` (`-a`) 配列ジョブ | Tier 2 |
| 3 | `--dependency` (`-d`) 配線 | Tier 1 |
| 4 | `--mail-user` / `--mail-type` 配線 | Tier 1 |
| 5a | `--no-requeue` | Tier 1 |
| 5b | `--comment` | Tier 1 |
| 5c | `--signal` (新 entity 追加 + 配線) | Tier 1 |
| 6 | `sbatch --wait` 相当の `run()` | Tier 2 |
| 7 | ログ tail / read API | Tier 1 |
| 8 | `--export` 値バリデーション | Tier 1（ハンドオーバ §5.5） |
| 9 | `absolutize` の DRY 化（`src/util/path.rs` 新設） | Tier 1（ハンドオーバ §5.6） |

### 1.2 明示的に Phase 2 外（Phase 3 以降）

- `JobHandleCommon` trait（tssrun + sbatch 共通抽象）— 「naming 規律宣言」のみ本 spec に記載、実装は Phase 2 #2/#6 の着地観察後
- KUDPC マニュアルが禁止する `--nodes`/`--ntasks`/`--cpus-per-task`/`--mem`/`--gpus`/`--exclusive` 等のフィールド化（Phase 1 §4.1 と一貫、Phase 2 でも禁止維持）
- Python で `SbatchJobHandle` を共通 base class に移す抽象化

---

## 2. クロスカット設計原則（Phase 2 で **必ず**守る）

### 2.1 vocab 重複定義の禁止 ★最重要★

> Slurm/KUDPC が定義する `--*` フラグの値型は **必ず `crate::entities::slurm::sbatch_options::*` に置く**。`crate::sbatch::*` 側では import して使うのみ。

理由: `entities` 配下は Slurm 公式 + KUDPC 公式マニュアルを参照して厳密に作られており、parsing rule・serde・Display が一貫している。`crate::sbatch::*` 配下で簡易 enum を再定義すると **2 系統の vocab が併存** し、片方が公式仕様から逸れる事故が起きる。

Phase 1 で既にこの原則は確立されている — `SbatchCmd` は `JobPartition`/`JobTimeLimit`/`ResourceSpec` を `entities` から import している。Phase 2 もこのパターンを継承する。

#### 2.1.1 Phase 2 で **再利用** する既存 entities（新規定義禁止）

| Phase 2 機能 | 既存型 | 場所 |
|---|---|---|
| `--array` | `SlurmArraySpec`, `ArrayIndex { Single, Range, Stepped }` | `entities/slurm/sbatch_options/array_spec.rs` |
| `--dependency` | `SlurmDependency`, `DependencyClause`, `DependencyType`（7 種類）, `DependencyJobRef`, `DependencyJoin` | `entities/slurm/sbatch_options/dependency.rs` |
| `--mail-type` | `MailType { BEGIN, END, FAIL, REQUEUE, ALL }` (uppercase Slurm 形式), `MailTypeInput(Vec<MailType>)` | `entities/slurm/sbatch_options.rs` |
| `--mail-user` | `pub type MailAddress = String` | 同上 |
| `--time` | `JobTimeLimit` | `entities/slurm/sbatch_options/time_limit.rs`（Phase 1 で既に再利用） |
| `--rsc` | `ResourceSpec`, `ResourceSpecCPU`, `ResourceSpecGPU`, `Memory`, `MemoryUnit` | `entities/slurm/sbatch_options/resource_spec.rs`（Phase 1 で既に再利用） |
| `-p` | `JobPartition = String` | `entities/slurm/sbatch_options.rs`（Phase 1 で既に再利用） |

#### 2.1.2 Phase 2 で entities に **新規追加** する型（1 件のみ）

| 機能 | 追加先 | 命名 | 形式 |
|---|---|---|---|
| `--signal` | `entities/slurm/sbatch_options/signal.rs`（新設） | `SlurmSignalSpec` | Slurm BNF `[R:]<sig_num\|sig_name>[@<sec>]` を typed 化 |

加えて `MailTypeInput` には現状 `Display` が無いので、Phase 2 で `Display` impl を追加（既存 `TryFrom<String>` の逆方向、entities 内追記のみ、API 破壊なし）。

### 2.2 不変条件の継承（Phase 1 で焼き付いた制約）

ハンドオーバ §2 の 6 項目をそのまま継承する。本 spec の各 plan は §11 のチェックリストに照らして検証する。

要点:
- `JobSnapshot::kind()` の `"sbatch"` 文字列は永続化済み。Phase 2 でも変更しない。配列ジョブも `"sbatch"` のまま（§5 参照）
- 全新フィールドに `#[serde(default)]`
- `JobDispatcher` 新メソッド禁止、`DynJobDispatcher` 周辺の triplet 更新ルール継承
- `JobState` variant は追加しない（配列タスクの状態は既存 11 種類で表現可能）
- sacct 呼び出しは `refresh_with_sacct` と `run()` 内のみ。`refresh()` には絶対入れない
- 公開 attach 経路は kind peek 必須
- async 内 lock は `tokio::sync::Mutex`

### 2.3 Spec/Runtime 二軸パターン継承

Phase 1 で確立された 3 層を Phase 2 でも維持:

- **Spec 層** (`SbatchCmd`): pure data + `build_argv()`、I/O なし
- **Runtime 層** (`SbatchJobHandle` / `SbatchJobSnapshot` / `SbatchLifecycle`): Arc + `watch::Sender` + lock-free 読み取り
- **Coordinator 層** (`SbatchManager`): Spec を受け取り Runtime を返す

新オプションはまず Spec に追加。snapshot に出すべきものだけ Runtime にも反映。

### 2.4 lock-free snapshot 維持

`tokio::sync::watch::Sender` で snapshot を broadcast、getter は all lock-free、`refresh_lock: Mutex<()>` で並行 refresh のみ単一化。Phase 2 で新 getter を足すときも **`async` にしない、`Mutex` を持たせない**。

### 2.5 Pyclass Single Owner ルール

ハンドオーバ §3.2 を継承。Phase 2 で新規追加する pyo3 binding（`PySbatchManager::run` / `cancel` / `spawn_array`、`SlurmSignalSpec` の py wrapper、配列 task 用 `PyArrayJobHandle` 等）は **1 つの Rust struct を 2 個以上の pyclass が共有しない** 規律を厳守する。

- `Py<...>` で wrap、`from_py_object` で Rust に渡す
- 配列ジョブの `Vec<SbatchJobHandle>` を Python 側に返すときは `Py<PyList>` of `Py<PySbatchJobHandle>` 形式とし、各要素が独立した Rust ownership を持つ
- 共有が必要な場合は `Arc<RwLock<T>>` を 1 pyclass に置き、他は参照経由（Phase 1 の `PySbatchManager` 同様のパターン）

clone semantics 不明確な共有は Phase 1 で問題化（handover §3.2）したため、Phase 2 でも厳守。

---

## 3. モジュールレイアウト変更

```text
src/
├── util/path.rs                                        ← 新設 (#9)
│   └── pub(crate) fn absolutize(p: &Path) -> Result<String>
├── entities/slurm/sbatch_options/
│   ├── signal.rs                                       ← 新設 (#5c)
│   │   └── SlurmSignalSpec + FromStr/Display/serde + tests
│   └── (sbatch_options.rs に MailTypeInput::Display 追記)
├── sbatch/
│   ├── cmd.rs                                          ← 拡張 (Phase 2 全 #)
│   │   └── SbatchCmd 新フィールド + build_argv 拡張 + --export validation
│   ├── parse.rs                                        ← 拡張 (#1, #2, #7)
│   │   ├── parse_sacct_exit_code: ExitCode "<n>:<m>" を typed 化
│   │   └── resolve_log_path: %A / %a / %u / %N 追加
│   ├── handle.rs                                       ← 拡張 (#1, #2, #7)
│   │   ├── SbatchJobSnapshot { array_jobid, array_task_id }（serde default）
│   │   ├── log read API: log_lines / read_log_to_end
│   │   └── exit_code Phase 1 limitation doc 削除
│   ├── manager.rs                                      ← 拡張 (#2, #6)
│   │   ├── spawn_array(cmd, ...) -> Vec<SbatchJobHandle>
│   │   ├── run(cmd) -> Result<FinishedInfo, SbatchRunError>
│   │   └── cancel(jobid) -> Result<()>
│   └── error.rs                                        ← 拡張
│       ├── SbatchSpawnError::{InvalidExportValue, InvalidArraySubmit, ...}
│       └── SbatchRunError (新規)
├── tssrun/cmd.rs                                       ← 改 (#9)
│   └── 自前 absolutize を削除、util::path::absolutize を import
├── manager.rs                                          ← 改 (#9)
│   └── 同上
└── py_export/sbatch.rs                                 ← 拡張
    └── 全 Phase 2 機能の pyo3 binding
```

`scripts/test_sbatch_live.py`：Phase 2 機能ごとに live smoke path を追加（dependency / array / mail / run / signal / log read）。

---

## 4. Tier 1: 最小追加機能の詳細

### 4.1 sacct ExitCode parser (#1)

**現状**: `src/sbatch/handle.rs:281` 付近で「surface as None for now. Phase 2 may extend the parser.」のコメント付きで `FinishedInfo.exit_code = None`。

**変更**:

`src/sbatch/parse.rs` に `parse_sacct_exit_code(field: &str) -> Option<i32>` を追加。

```rust
/// Parse sacct's `ExitCode` column ("<exit>:<signal>") into an i32 exit code.
///
/// Slurm の sacct は `0:0`（正常終了）, `0:9`（SIGKILL で終了）, `139:11`
/// （SIGSEGV で終了、shell convention で 128+11=139 が exit）の形を返す。
/// シグナル成分が非ゼロのときは `128 + signal` を採用する shell convention に従う。
pub(crate) fn parse_sacct_exit_code(field: &str) -> Option<i32> {
    let (exit_s, signal_s) = field.split_once(':')?;
    let exit = exit_s.parse::<i32>().ok()?;
    let signal = signal_s.parse::<i32>().ok()?;
    if signal != 0 { Some(128 + signal) } else { Some(exit) }
}
```

`refresh_with_sacct` の中で `parse_sacct_exit_code` を呼び、`FinishedInfo::exit_code` に格納。

**Phase 1 limitation doc-comment の削除**: 以下 3 箇所:
- `src/sbatch/handle.rs` の `SbatchLifecycle::exit_code`
- `src/sbatch/handle.rs` の `SbatchJobSnapshot::exit_code`
- `src/sbatch/handle.rs` の `SbatchJobHandle::exit_code`
- `python/slurm_async_runner/_core/sbatch.pyi` の対応 docstring

**テスト**: `parse_sacct_exit_code` の境界値 (`"0:0"`, `"0:9"`, `"139:11"`, `"abc"`, `""`, `":0"`)。`refresh_with_sacct` integration test で `exit_code = Some(0)` / `Some(137)` / `None` 確認。

### 4.2 `--dependency` 配線 (#3)

**変更**: `SbatchCmd` に `pub dependency: Option<SlurmDependency>` を追加。`build_argv` で:

```rust
if let Some(dep) = &self.dependency {
    argv.push("-d".into());
    argv.push(dep.to_string());  // SlurmDependency::Display を使用
}
```

`SlurmDependency` は既に 7 種類 (`after`/`afterany`/`afterburstbuffer`/`aftercorr`/`afternotok`/`afterok`/`singleton`) と AND/OR join、`+<minutes>` delay を完全実装済み。**新規定義禁止**。

**テスト**: `cmd.dependency = Some("afterok:200,afterany:201".parse().unwrap())` → argv に `["-d", "afterok:200,afterany:201"]` が含まれる。

### 4.3 `--mail-user` / `--mail-type` 配線 (#4)

**変更**:
- `SbatchCmd` に `pub mail_user: Option<MailAddress>` と `pub mail_types: Option<MailTypeInput>` を追加
- `entities/slurm/sbatch_options.rs` の `MailTypeInput` に `Display` impl を追加（カンマ区切り出力）

`MailType` Variant の Slurm 文字列出力（`BEGIN`/`END`/`FAIL`/`REQUEUE`/`ALL`）は P2 plan で `as_slurm_str(self) -> &'static str` メソッドを `entities` 側に追加し `Display` で使う方針（`Debug` 経由よりも明示的）。

`build_argv`:

```rust
if let Some(addr) = &self.mail_user {
    argv.push("--mail-user".into());
    argv.push(addr.clone());
}
if let Some(mts) = &self.mail_types {
    argv.push("--mail-type".into());
    argv.push(mts.to_string());
}
```

**バリデーション**: `mail_types.is_some() && mail_user.is_none()` のときに warn ログ（KUDPC は環境変数フォールバックがあるが、明示性のため警告）。エラーにはしない。

### 4.4 `--no-requeue` (#5a)

**変更**: `SbatchCmd { pub no_requeue: bool }` を追加（default `false`）。`build_argv` で `if self.no_requeue { argv.push("--no-requeue".into()); }`。

新型不要、テストは bool 出力 on/off のみ。

### 4.5 `--comment` (#5b)

**変更**: `SbatchCmd { pub comment: Option<String> }` を追加。`build_argv` で `if let Some(c) = &self.comment { argv.push("--comment".into()); argv.push(c.clone()); }`。

新型不要。`,` を含むコメントは sbatch 側で正常にエスケープされる（CLI は positional パース）ので validation 不要。

### 4.6 `--signal` typed 化 (#5c)

**新規 entity**: `entities/slurm/sbatch_options/signal.rs`。

```rust
//! `--signal` spec for a Slurm batch submission.
//!
//! References:
//! - <https://slurm.schedmd.com/sbatch.html> (`--signal`)
//!
//! Slurm BNF: `[R:]<sig_num|sig_name>[@<sig_time>]`
//! - `R:` prefix — also signal a job that's running but already received
//!   the signal (allow re-signal)
//! - `sig_num` — POSIX signal number (1..=64)
//! - `sig_name` — `SIGINT`, `SIGTERM`, `SIGKILL`, `USR1`, etc.
//! - `@<sig_time>` — seconds before time limit to send the signal (1..=65535)

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlurmSignalSpec {
    pub allow_resignal: bool,        // R: prefix
    pub signal: SignalIdent,
    pub seconds_before_end: Option<u16>,  // @<sec>
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignalIdent {
    Number(u8),                      // 1..=64
    Name(String),                    // "SIGINT", "USR1", ...
}
```

`FromStr`/`Display`/`serde` を `SlurmDependency` 同様の流儀で実装。ボリューム小（80-120 行）。

**配線**: `SbatchCmd { pub signal: Option<SlurmSignalSpec> }`。`build_argv`:

```rust
if let Some(s) = &self.signal {
    argv.push("--signal".into());
    argv.push(s.to_string());
}
```

### 4.7 ログ tail / read API (#7)

**設計**: `SbatchJobHandle` に追加（async fn）:

```rust
impl SbatchJobHandle {
    /// Read the last `n` lines of the job's stdout/stderr.
    ///
    /// `LogStream::Stdout` は snapshot の `log_stdout`、`LogStream::Stderr` は
    /// `log_stderr` を読む。ファイル未生成（ジョブ未開始など）なら `Ok(vec![])`
    /// を返す（NotFound を Ok にマッピング）。
    pub async fn log_lines(
        &self,
        stream: LogStream,
        n: usize,
    ) -> Result<Vec<String>, LogReadError>;

    /// Read the entire log file into a String.
    pub async fn read_log_to_end(
        &self,
        stream: LogStream,
    ) -> Result<String, LogReadError>;
}

pub enum LogStream { Stdout, Stderr }

#[derive(Debug, thiserror::Error)]
pub enum LogReadError {
    #[error("log path not resolved on snapshot")]
    PathNotResolved,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
```

**実装**: `tokio::fs::read_to_string` ベース。`log_lines` は末尾 `n` 行を取りたいので、ファイル全読み → 行分割 → 末尾 `n` 取り出し（KUDPC ジョブのログは数 MB スケール想定なので reverse seek 最適化は P1 plan では不要、Phase 3 改善余地）。

`%j`/`%x`/`%A`/`%a` 含む raw log path は `parse::resolve_log_path` で展開済みのものを `snapshot.log_stdout`/`log_stderr` に持つ前提（既に Phase 1 で実装済み、Phase 2 で resolver を array 対応に拡張するだけ）。

### 4.8 `--export` 値バリデーション (#8)

**変更**: `SbatchCmd::build_argv` の `render_export` で値を検査。

```rust
fn render_export(env: &HashMap<String, String>) -> Result<String, SbatchSpawnError> {
    let mut keys: Vec<&String> = env.keys().collect();
    keys.sort();
    let mut out = String::from("ALL");
    for k in keys {
        let v = &env[k];
        if k.contains(',') || k.contains('=') {
            return Err(SbatchSpawnError::InvalidExportKey { key: k.clone() });
        }
        if v.contains(',') || v.contains('=') {
            return Err(SbatchSpawnError::InvalidExportValue { key: k.clone(), value: v.clone() });
        }
        out.push(',');
        out.push_str(k);
        out.push('=');
        out.push_str(v);
    }
    Ok(out)
}
```

**新エラー variant**:

```rust
#[derive(Debug, thiserror::Error)]
pub enum SbatchSpawnError {
    // ... 既存 variant ...

    #[error("--export key contains forbidden char (`,` or `=`): {key:?}")]
    InvalidExportKey { key: String },

    #[error("--export value for key {key:?} contains forbidden char (`,` or `=`): {value:?}")]
    InvalidExportValue { key: String, value: String },
}
```

**テスト**: `cmd.env.insert("FOO", "1,2")` → `build_argv` が `Err(InvalidExportValue { ... })`。

### 4.9 `absolutize` DRY 化 (#9)

**新ファイル**: `src/util/path.rs`

```rust
//! Path utilities shared across submission backends.

use anyhow::{Context, Result};
use std::path::Path;

/// Convert a possibly-relative path to its absolute UTF-8 string form.
///
/// Used by `tssrun::cmd`, `sbatch::cmd`, and `manager` to render absolute
/// paths into argv. Returns an error if the path is non-UTF-8.
pub(crate) fn absolutize(p: &Path) -> Result<String> {
    let abs = std::path::absolute(p)
        .with_context(|| format!("failed to absolutize {}", p.display()))?;
    abs.into_os_string()
        .into_string()
        .map_err(|os| anyhow::anyhow!("non-UTF8 path: {os:?}"))
}
```

**移行**:
- `src/tssrun/cmd.rs:97` の `fn absolutize` を削除、`use crate::util::path::absolutize` に置換
- `src/sbatch/cmd.rs:96` の `fn absolutize` を削除、同様に置換
- `src/manager.rs:62` の inline `std::path::absolute` 使用箇所も置換

`src/util/mod.rs` (既存ならそのまま、無ければ新設) に `pub(crate) mod path;` を追加。`src/lib.rs` には既存の `mod util;` がなければ追加。**`util::path` は public でも `pub(crate)` でも良いが、`pub(crate)` で隠して外部 API 表面を増やさない**。

---

## 5. Tier 2-A: 配列ジョブ (`--array`) 詳細設計

### 5.1 `SlurmArraySpec` 配線

`SbatchCmd { pub array_spec: Option<SlurmArraySpec> }` を追加。`build_argv`:

```rust
if let Some(a) = &self.array_spec {
    argv.push("-a".into());
    argv.push(a.to_string());  // 既存 Display 使用
}
```

### 5.2 snapshot model 拡張

```rust
// src/sbatch/handle.rs
pub struct SbatchJobSnapshot {
    // ... Phase 1 既存フィールド ...

    /// Array job の master jobid (sbatch が返す `Submitted batch job <N>` の N)。
    /// 単発ジョブでは `None`。
    #[serde(default)]
    pub array_jobid: Option<u64>,

    /// 配列タスクの index。`SlurmArraySpec.indices.expand()` の値。
    /// 単発ジョブでは `None`。
    #[serde(default)]
    pub array_task_id: Option<u32>,
}
```

**`kind()` 文字列**: `"sbatch"` を維持（不変条件 §2.2）。配列タスクと単発ジョブを `array_task_id.is_some()` で判別。

### 5.3 spawn_array フロー（C-6 採用案: spawn 時に全タスク snapshot 一括生成）

```rust
impl SbatchManager {
    pub async fn spawn_array(
        &self,
        mut cmd: SbatchCmd,
        array_spec: SlurmArraySpec,
    ) -> Result<Vec<SbatchJobHandle>, SbatchSpawnError> {
        // 1. cmd.array_spec を上書き（呼び出し元で既に入っていてもよい）
        cmd.array_spec = Some(array_spec.clone());

        // 2. sbatch 1 回呼び出し → master jobid
        let argv = cmd.build_argv()?;
        let stdout = self.dispatcher.capture(...).await?;
        let master_jobid = parse_submitted_jobid(&stdout)?;

        // 3. array_spec.expand() で task index 列挙
        let task_indices: Vec<u32> = expand_array_indices(&array_spec);

        // 4. 各 task に UUID v7 を発行、snapshot 作成、store に save
        let mut handles = Vec::with_capacity(task_indices.len());
        for idx in task_indices {
            let uuid = Uuid::now_v7();
            let snapshot = SbatchJobSnapshot {
                uuid,
                jobid: Some(master_jobid),       // SLURM の squeue では <master>_<idx> 表記だが、jobid フィールドは master を保持
                array_jobid: Some(master_jobid),
                array_task_id: Some(idx),
                state: JobState::Pending,
                log_stdout: resolve_log_path(&cmd.output, master_jobid, Some(idx), &cmd.job_name),
                log_stderr: resolve_log_path(&cmd.error,  master_jobid, Some(idx), &cmd.job_name),
                // ... 他フィールド ...
            };
            self.store.save(&snapshot).await?;
            handles.push(SbatchJobHandle::from_snapshot(snapshot, ...));
        }
        Ok(handles)
    }
}

// helper（src/sbatch/array.rs か parse.rs に置く）
fn expand_array_indices(spec: &SlurmArraySpec) -> Vec<u32> {
    let mut out = Vec::new();
    for entry in &spec.indices {
        match entry {
            ArrayIndex::Single(i) => out.push(*i),
            ArrayIndex::Range { start, end } => out.extend(*start..=*end),
            ArrayIndex::Stepped { start, end, step } => {
                let mut i = *start;
                while i <= *end { out.push(i); i += step; }
            }
        }
    }
    out
}
```

**`max_concurrent` の扱い**: SLURM 側の同時実行数制限なので、snapshot/store には記録しない（squeue で観測される実 state のみ反映）。

### 5.4 `resolve_log_path` の `%A`/`%a`/`%u`/`%N` 対応

`src/sbatch/parse.rs::resolve_log_path` を拡張。

```rust
pub(crate) fn resolve_log_path(
    template: &Option<String>,
    master_jobid: u64,
    array_task_id: Option<u32>,
    job_name: &Option<String>,
) -> Option<PathBuf> {
    let raw = template.as_ref()?;
    let user = std::env::var("USER").unwrap_or_default();
    let node = std::env::var("HOSTNAME").unwrap_or_default();  // best-effort
    let mut s = raw.clone();
    s = s.replace("%j", &master_jobid.to_string());
    s = s.replace("%A", &master_jobid.to_string());
    if let Some(t) = array_task_id { s = s.replace("%a", &t.to_string()); }
    s = s.replace("%u", &user);
    s = s.replace("%N", &node);
    if let Some(name) = job_name { s = s.replace("%x", name); }
    // 未知の %-token は raw のまま残す既存戦略を継承
    Some(PathBuf::from(s))
}
```

**注**: `%N` (node name) はジョブが pending のときは未確定。spawn 時点では `HOSTNAME` 環境変数（log-in node 名）でフォールバックしておき、refresh で squeue が node 名を返したらそれで上書き、までは Phase 2 では行わない（ベストエフォート、Phase 3 改善候補）。Phase 2 では「`%N` が含まれる log path は spawn 時点では未解決のまま」も許容する設計とし、戻り値型は既存 `LogPathSpec`（raw template + 解決済 path のペア）パターンを継承。

### 5.5 refresh フロー（配列ジョブ対応）

各 `SbatchJobHandle.refresh()` は `array_task_id.is_some()` のとき:
1. `qgroup -l` で master jobid を引き
2. squeue で `<master>_<idx>` 単独行を探す（既存 `query_*_via_qgroup` を拡張、または squeue を別途 `JOBID==<master>_<idx>` でフィルタ）
3. 該当 task の state を取得し snapshot を更新

複数 task を一気に refresh する `SbatchManager::refresh_array(handles: &[SbatchJobHandle])` も提供（squeue を 1 回で叩いて分配、効率化）。

### 5.6 attach 経路

既存の `attach_uuid` / `attach_jobid` / `attach_file` で配列タスクの handle を個別に取得可能。`attach_jobid(master_jobid)` は **master を `array_jobid` として持つ snapshot を複数返す可能性**が出るため:

- 新メソッド `attach_array_jobid(master_jobid) -> Vec<SbatchJobHandle>` を追加（task index 昇順で返す）
- 既存 `attach_jobid(jobid) -> SbatchJobHandle` は単発 snapshot のみ返す挙動を維持。複数マッチ時は新エラー `SbatchAttachError::MultipleMatch { jobid, count }` を返す（破壊的だが、Phase 1 では起き得なかったケースなので互換性影響なし）

**kind peek の遵守**（ハンドオーバ §2.4 必須要件）: `attach_array_jobid` も **既存 attach 経路と同じく on-disk JSON の `kind` フィールドを peek し、`"sbatch"` 以外を silent skip する**。`FileSystemStateStore::find_*_by_array_jobid` 系の新スキャンメソッドは Phase 1 の既存 scan と同一の skip ロジックを継承する。kind 不一致拒否のテストを `attach_array_jobid` にも追加すること（handover §4 の "attach_file が kind チェックを忘れていた" 教訓を継承）。

---

## 6. Tier 2-B: `sbatch --wait` 相当 `run()` (#6) 詳細

### 6.0 handover §5.3 からの逸脱と理由（明示）

**handover §5.3 のタイトル**は "`sbatch --wait based run()`" であり、`--wait` フラグを使った同期 sbatch 起動を前提としていた。本 spec は **意図的に `--wait` を使わず、`spawn → wait_terminal` ポーリング実装を採用** する。

**逸脱の理由**:

1. **接続断による孤児ジョブのリスクが構造的に回避できない** — handover §5.3 自身も「コネクション切断 (timeout) で残骸ジョブが残るリスクあり」と警告。`--wait` を保持する Rust プロセスが SIGKILL/OOM/SSH 切断で死亡した場合、`Drop` impl が走らないので scancel が発火しない。KUDPC は長時間ジョブ（hours/days）が日常で SSH 切断が発生しやすく、構造的脆弱性となる。
2. **Phase 1 の "subprocess は短命、state は永続 snapshot" 不変条件と整合** — `--wait` は数時間 subprocess を保持する設計で、Phase 1 が確立した「sbatch は瞬時に jobid を返して終了 → snapshot は disk に永続 → 別プロセスから attach 可能」モデルと逆行する。Poll 実装なら spawn 直後に snapshot 永続化され、Rust プロセスが死んでも jobid で再 attach できる。
3. **KUDPC 負荷ガイドラインへの整合** — handover §5.1 が sacct について "ライセンス的に重い" と明記している通り、KUDPC は CLI 呼び出し負荷を懸念事項として挙げている。`--wait` で sbatch CLI を hours オーダーで保持するより、squeue ポーリング（既存 `wait_terminal` と同等、追加負荷ゼロ）の方が KUDPC 流儀に沿う。
4. **timeout 制御の単純さ** — `tokio::time::timeout(dur, mgr.run(cmd))` でユーザが包めるため、API 表面は最小。`--wait` 採用なら subprocess kill + scancel + drop 連携の重ね合わせで複雑化する。

**handover §5.3 の意図の解釈**: handover の bullet 2 「`timeout / cancel-on-drop の挙動を明記`」は **「--wait のリスクを書け」と読めると同時に、「リスク回避設計を選ぶ自由度」も含意する**。本 spec はこの自由度を行使し、§6.1 の poll 実装を採用する。

### 6.1 設計（C-7 採用案: tokio::time::timeout でユーザが包む）

```rust
impl SbatchManager {
    /// Submit a job and block until terminal state, then return FinishedInfo.
    ///
    /// Internally: `spawn(cmd) → handle.wait_terminal() → handle.refresh_with_sacct()`。
    /// `--wait` flag は使わない（KUDPC で接続切れ時の残骸ジョブリスクを避けるため）。
    ///
    /// Timeout が必要なら caller が `tokio::time::timeout(dur, mgr.run(cmd))` で包む。
    /// Timeout 後にジョブを止めたいなら `mgr.cancel(jobid)` を別途呼ぶ。
    pub async fn run(
        &self,
        cmd: SbatchCmd,
    ) -> Result<FinishedInfo, SbatchRunError>;

    /// Send `scancel <jobid>` for a job. 明示的取り消し API。
    /// Drop 時の auto-cancel は行わない（C-3 / handover §5.3 採用案）。
    pub async fn cancel(
        &self,
        jobid: u64,
    ) -> Result<(), SbatchCancelError>;
}
```

### 6.2 `SbatchRunError`

```rust
#[derive(Debug, thiserror::Error)]
pub enum SbatchRunError {
    #[error("spawn failed: {0}")]
    Spawn(#[from] SbatchSpawnError),

    #[error("wait_terminal io error: {0}")]
    Wait(std::io::Error),

    #[error("sacct refresh failed: {0}")]
    Sacct(String),

    #[error("job ended in failed state: {state:?}, exit_code={exit_code:?}")]
    JobFailed { state: JobState, exit_code: Option<i32> },

    #[error("array submission is not supported by run(); use spawn_array() instead")]
    ArrayNotSupported,
}
```

### 6.3 `cancel()`

`scancel <jobid>` を `JobDispatcher::capture` 経由で実行。失敗時は stderr を `SbatchCancelError::Scancel(String)` に格納。冪等（既に終了済みジョブへの cancel は no-op として `Ok(())`）。

### 6.4 `run()` と配列ジョブの組み合わせ

`run()` は単発ジョブ専用（`cmd.array_spec.is_some()` なら `Err(SbatchRunError::ArrayNotSupported)`）。配列ジョブの全タスク終了待ちは Phase 3 で `run_array()` として別途検討。

### 6.5 Drop semantics（再掲、C-3）

`SbatchJobHandle` の `Drop` impl は **`scancel` を呼ばない**。代わりに `tracing::warn!("dropped SbatchJobHandle for jobid={} without explicit cancel", ...)` のみ出力（任意機能、冪等）。

---

## 7. Tier 3: `JobHandleCommon` trait（Phase 2 では実装しない）

### 7.1 naming 規律宣言

Phase 2 で tssrun と sbatch の handle 双方が以下のシグネチャを持つことを **明文化**する（実 trait 化は Phase 3）。命名は **handover §5.4 由来のコア 5 names** と、**spec §7.2 の trait 化条件達成時に検討する拡張 2 names** の 2 段階に区別する:

```rust
// 規律宣言のみ — concrete trait は Phase 3
trait JobHandleNaming {
    // ─── handover §5.4 由来のコア 5 names（Phase 2 で必ず収斂させる） ───
    fn uuid(&self) -> Uuid;
    fn jobid(&self) -> Option<u64>;
    fn is_running(&self) -> bool;
    fn is_finished(&self) -> bool;
    fn exit_code(&self) -> Option<i32>;

    // ─── spec §7.2 の trait 化条件達成時に追加検討（Phase 2 では命名整合のみ） ───
    async fn wait_terminal(&self) -> Result<()>;
    async fn refresh(&self) -> Result<()>;
}
```

**Phase 2 で必ず守る範囲**: コア 5 names は `SbatchJobHandle` と `TssrunJobHandle` の両方で **同一シグネチャ**を持つこと。`wait_terminal` / `refresh` の async fn は Phase 2 では命名一致のみ（戻り値型の細部は handle ごとに異なってよい — 例: tssrun には sacct 概念がないので戻り値の error variant が異なる）。

Phase 2 PR レビューで既存・新規 handle メソッドがこの命名から逸れる場合は指摘し修正する。handover §5.4 が明記する **「sbatch の log path / sacct opt-in / array task は tssrun に対応物がない → trait に含めない」** 原則も継承し、Phase 2 でこれらを共通 trait に含めようとしない。

### 7.2 trait 化を実施する条件

- 3 つ目の handle 種別（例: `srun` 同期 handle、外部 scheduler 連携）が必要になったとき
- または `Box<dyn JobHandleCommon>` を必要とするユースケース（例: 統一 dashboard）が出現したとき

それまでは concrete handle 型のままで運用する。

---

## 8. Plan 分割（umbrella spec → 6 plans）

| Plan | 含まれる項目 | 想定 LOC | 主要ファイル |
|---|---|---|---|
| **P1** | #1 sacct ExitCode, #5a `--no-requeue`, #5b `--comment`, #7 ログ tail/read, #9 `absolutize` DRY | 中 (≈400) | `parse.rs`, `handle.rs`, `cmd.rs`, `util/path.rs` |
| **P2** | #3 `--dependency`, #4 `--mail-*`, `MailTypeInput::Display` 追加, `MailType::as_slurm_str` 追加 | 小 (≈200) | `cmd.rs`, `entities/slurm/sbatch_options.rs` |
| **P3** | #8 `--export` validation | 小 (≈100) | `cmd.rs`, `error.rs` |
| **P4** | #5c `SlurmSignalSpec` 新規 entity + 配線 | 中 (≈300) | `entities/slurm/sbatch_options/signal.rs` (新), `cmd.rs` |
| **P5** | #2 `--array` 配列ジョブ全機能（snapshot 拡張、`spawn_array`, `resolve_log_path` %A/%a/%u/%N、`refresh_array`、attach 拡張） | 大 (≈800) | `handle.rs`, `manager.rs`, `parse.rs`, `cmd.rs`, `py_export/sbatch.rs` |
| **P6** | #6 `run()` + `cancel()` + `SbatchRunError` | 中 (≈400) | `manager.rs`, `error.rs`, `py_export/sbatch.rs` |

**依存関係**: P1〜P4 は独立、並列可能。P5 は P1（log path resolver）に依存、P6 は P1（sacct ExitCode）に依存。

PR は plan 単位 → develop マージ。Plan ごとに `cargo test --lib --features pyo3 / clippy / fmt / pytest` 全 pass を要件とする。

---

## 9. テスト戦略

### 9.1 unit / integration

ハンドオーバ §6.1 の Phase 1 規律をそのまま継承:

- 各モジュール同居の `#[cfg(test)] mod tests`
- `tests/` 配下に integration test
- `MoveDispatcher` / `PanicDispatcher` / `CannedDispatcher` / `MockCapture` 流用、新 fake は導入しない

### 9.2 entities 側のテスト

`SlurmSignalSpec` は entities 標準のテストパターンを踏襲:
- FromStr / Display roundtrip
- 各 Slurm BNF 形式の境界値
- serde TOML roundtrip
- 不正入力の rejection

### 9.3 配列ジョブのテスト

- `expand_array_indices` の境界値（empty にならない、step なし、step あり、複合）
- `spawn_array` で task 数だけ snapshot が save される
- `resolve_log_path` の `%A`/`%a` 展開
- attach: `attach_array_jobid(master)` が全 task を返す、`attach_jobid(master)` が `MultipleMatch` を返す

### 9.4 live smoke

`scripts/test_sbatch_live.py` に Phase 2 機能ごとに smoke path を追加:
- `test_dependency_chain` — afterok で 2 ジョブ
- `test_array_job` — `-a 0-2` で 3 タスク投入、全 task 終了確認
- `test_mail_does_not_break_submission` — KUDPC で実メール送信は確認できないので submission が通ることのみ確認
- `test_run_blocks_until_terminal` — `mgr.run()` が終端で returns
- `test_signal_passes_through` — `--signal=USR1@60` で submission 成功
- `test_log_read` — `wait_terminal` 後に `log_lines(Stdout, 10)` で末尾 10 行取得

### 9.5 coverage

`cargo llvm-cov --lib --features pyo3` で 80% 維持。Phase 1 同等。

---

## 10. Migration & 後方互換

### 10.1 on-disk JSON 互換

- `kind = "sbatch"` のまま、新フィールド `array_jobid: Option<u64>` / `array_task_id: Option<u32>` を `#[serde(default)]` で追加。
- 既存 Phase 1 で書き出された snapshot ファイルは migration なしでロード可能（フィールドが無ければ `None`）。
- 配列タスク 1 個あたり 1 snapshot ファイル。`{root}/<uuid>.json` の名前空間は変えない。

### 10.2 公開 API 互換

- `SbatchCmd` の新フィールドは public field 直追加（既存パターン継承）。Phase 1 の `SbatchCmd::new(script)` constructor は **新フィールドを default 値で初期化する** よう更新。構造体リテラル構築する既存コードは Phase 1 から無いため破壊なし。
- Python pyo3 binding (`PySbatchCmd`) のキーワード引数はすべて optional で追加。Python 既存ユーザに破壊なし。
- `attach_jobid` の挙動は単発ジョブに対しては不変。複数マッチ時のみ `MultipleMatch` を返す（配列ジョブ環境のみ影響、Phase 1 では起き得なかった）。

### 10.3 `MailTypeInput::Display` / `MailType::as_slurm_str` 追加

`entities/slurm/sbatch_options.rs` への追記のみ、API 破壊なし。

### 10.4 backup ブランチ

ハンドオーバ §7.3 の `sbatch-module-backup` (`cedbedb`) は **Phase 2 の develop merge が origin に push されるまで残す**。Phase 2 の各 plan PR が develop にマージされて初めて Phase 1 の merge を含む `develop` が安定したと判断する。

---

## 11. クイック・チェックリスト（各 Plan PR 提出前）

ハンドオーバ §8 + 本 spec §2.1 の重複定義禁止を加えたもの:

- [ ] `develop` から切り出している
- [ ] **vocab 重複なし: `--*` 値型は `entities/slurm/sbatch_options/*` のみに置いた**
- [ ] 既存 entities 型 (`SlurmArraySpec` / `SlurmDependency` / `MailType` / `MailTypeInput` / `MailAddress`) を再利用している
- [ ] kind 文字列の追加なし（`"sbatch"` のまま、配列タスクも同一 kind）
- [ ] 新 snapshot フィールドに `#[serde(default)]`
- [ ] 新 `JobDispatcher` メソッドなし、`DynJobDispatcher` 周辺の更新不要
- [ ] 新 `JobState` variant なし
- [ ] sacct 呼び出しは `refresh_with_sacct` と `run()` 内のみ（`refresh()` には絶対入れない）
- [ ] 公開 attach 経路に kind peek あり
- [ ] async 内 lock は `tokio::sync::Mutex`
- [ ] CHANGELOG `[Unreleased]` 更新
- [ ] `python/.../*.pyi` 同期 + 該当 Phase 1 limitation doc 削除
- [ ] `cargo test --lib --features pyo3` / `cargo clippy --all-targets --features pyo3 -- -D warnings` / `cargo fmt --all --check` / `uv run pytest python/tests` 全 pass
- [ ] Live smoke（KUDPC で実行可能なら）
- [ ] Plan の依存関係（P1→P5/P6）を尊重
- [ ] **Phase 1 の「計画見落とし」教訓 6 項目（handover §4）への照合済み**:
  - [ ] trait/method の存在は `grep` で実存確認してから計画に書いた
  - [ ] 入力データのバリエーション（KUDPC `RUN`/`QUE`/`CMP` 等）を実物で確認しテストに再現
  - [ ] 公開 alias 変更は `grep -r` で全 usage 走査済み
  - [ ] dyn-safe にしたい trait は専用 wrapper trait + 明示 constructor 経由（blanket impl 禁止）
  - [ ] 公開 attach 経路は kind peek + 拒否テスト追加済み（spec §5.6 含む）
  - [ ] async 文脈の lock は `tokio::sync::Mutex` のみ

---

## 12. オープンな決定事項（実装中に判断、spec では確定しない）

- `log_lines` の reverse seek 最適化（Phase 3 候補、Phase 2 では full read）
- `attach_array_jobid` の戻り値 ordering（task index 昇順を採用）
- `cancel()` が未終了 master jobid に対して全配列タスクを止めるか個別 task のみか — P6 plan（Phase 2 では master 一括 scancel を採用予定、`scancel <master>` で SLURM が全 task 取消）

---

## 13. 連絡先 / リファレンス

- Phase 1 設計: `docs/superpowers/specs/2026-05-10-sbatch-module-design.md`
- Phase 1 計画: `docs/superpowers/plans/2026-05-10-sbatch-module.md`
- Phase 1 ハンドオーバ: `docs/attention_phase2.md`
- Phase 1 backup: `sbatch-module-backup` ブランチ (`cedbedb`)
- 統合 baseline: `develop` ブランチ (`ed8b15a`)
- Phase 2 作業ブランチ: `sbatch-module-phase2` (本 spec のコミット先)
