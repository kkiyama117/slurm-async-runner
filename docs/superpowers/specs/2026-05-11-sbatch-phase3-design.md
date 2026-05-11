# sbatch / tssrun Phase 3 設計 — `JobHandleCommon` trait 抽出

- **Date**: 2026-05-11
- **Status**: Draft (Phase 2 P1–P6 マージ前提、umbrella spec)
- **Targets**: `crate::handle` 新設 (Rust) + `crate::sbatch::*` / `crate::tssrun::*` (impl 追加) + `crate::py_export::*` (任意の type-erased facade)
- **Phase 2 baseline**: `sbatch-module-phase2` ブランチ (`6838d94`) を base にした worktree で作業する。`develop` への merge は Phase 3 各 PR と並行して進む可能性があるため、Phase 3 PR の base は **その時点で develop に Phase 2 がマージされていれば `develop`、未マージなら `sbatch-module-phase2`** とする (PR ごとに判断)
- **References**:
  - Phase 1 設計: `docs/superpowers/specs/2026-05-10-sbatch-module-design.md`
  - Phase 2 設計: `docs/superpowers/specs/2026-05-10-sbatch-phase2-design.md` §7 (本 Phase 3 の前提宣言)
  - Phase 1 ハンドオーバ: `docs/attention_phase2.md` §5.4 (trait 化前提条件)
  - 既存 `JobSnapshot` trait: `src/store.rs:18`
  - 既存 `JobDispatcher` / `DynJobDispatcher` パターン: `src/runner.rs`

---

## 1. 背景

Phase 2 §7.1 で **「コア 5 names は `SbatchJobHandle` と `TssrunJobHandle` の両方で同一シグネチャを持つ」** という命名規律が宣言され、Phase 2 終了時点で実装上もこれが満たされている (両 handle に `uuid` / `jobid` / `is_running` / `is_finished` / `exit_code` が同一シグネチャで存在)。

Phase 3 は **この命名規律を実 trait 化** する。abstract Box-able handle が出ることで、複数 backend を横断する dashboard / orchestrator / 統一 attach UI といった上位レイヤを書けるようになる。

### 1.1 Phase 3 in-scope

| # | 項目 | Tier |
|---|---|---|
| 1 | `JobHandleCommon` trait 定義 (`crate::handle` 新設) | Tier 1 ★本命★ |
| 2 | `SbatchJobHandle` / `TssrunJobHandle` への trait 実装 | Tier 1 |
| 3 | `tssrun::JobHandleSnapshot` の rename → `TssrunJobSnapshot` (sbatch 命名と対称化) | Tier 1 |
| 4 | `tssrun::TssrunJobHandle::refresh()` 戻り値を `Result<TssrunJobSnapshot>` に揃える (sbatch と parity) | Tier 1 |
| 5 | `DynJobHandleCommon` 型消去 facade (`Arc<dyn DynJobHandleCommon>`) + `into_dyn()` | Tier 1 |
| 6 | `JobHandleCommon` への associated `type Snapshot: JobSnapshot` バインド | Tier 1 |
| 7 | Python 側 `PyJobHandle` ABC / `Protocol` (任意、`runtime_checkable`) | Tier 2 |

### 1.2 明示的に Phase 3 外 (Phase 4 以降)

- `log_lines` の reverse-seek 最適化 (Phase 2 spec §4.7 改善余地) — log API は sbatch 固有なので common trait に含めない方針 (handover §5.4)
- `%N` (node name) の HOSTNAME フォールバック → refresh 上書き (Phase 2 spec §5.4 改善候補)
- `run_array()` (Phase 2 spec §6.4 言及) — 配列ジョブ全 task 終了待ち API
- Python 側 `SbatchJobHandle` / `TssrunJobHandle` の共通 base class 化 (Phase 2 spec §1.2 で deferred 済)
- 第 3 の handle 種別 (`srun` 同期 handle, 外部 scheduler 連携) — そのとき trait の十分性が試される

---

## 2. クロスカット設計原則 (Phase 1/2 から継承)

### 2.1 vocab 重複定義の禁止 ★継承★

Phase 2 §2.1 をそのまま継承。本 Phase 3 では `--*` flag の追加はないので新規 entity 追加なし。

### 2.2 不変条件の継承

ハンドオーバ §2 + Phase 2 §2.2 をそのまま継承:

- **`JobSnapshot::kind()` の永続化文字列 (`"sbatch"` / `"tssrun"`) を変更しない** — Snapshot struct を rename しても kind の戻り値は固定
- 全新フィールドに `#[serde(default)]`
- `JobDispatcher` 新メソッド禁止 (handle trait は `JobDispatcher` とは独立、§3 参照)
- `JobState` variant は追加しない
- sacct 呼び出しは `refresh_with_sacct` と `run()` 内のみ
- 公開 attach 経路は kind peek 必須
- async 内 lock は `tokio::sync::Mutex`

### 2.3 dyn-safe trait 設計の規律 ★Phase 1 学習★

ハンドオーバ §4 で記録された教訓:

> RPITIT trait の `dyn` 化で blanket impl が E0034 ambiguity → dyn-safe にしたいなら **専用 wrapper trait + 明示 constructor**

Phase 3 でも `JobHandleCommon` を dyn-safe にしたい。そのため:

- `JobHandleCommon` (associated type 保持の本体 trait) と `DynJobHandleCommon` (object-safe な平坦化版) を **2 段に分ける**
- `into_dyn(self) -> Arc<dyn DynJobHandleCommon>` を **明示的に定義**、blanket impl は禁止
- 両 trait の各メソッド triplet (本体 + Dyn + Dyn を呼ぶ adapter) を Phase 1 `DynJobDispatcher` パターン (`src/runner.rs`) に倣う

### 2.4 lock-free snapshot 維持 ★継承★

Phase 1/2 で確立済み。`watch::Receiver<S::Snapshot>` を返す `watch()` getter は `JobHandleCommon` trait の必須メソッドとし、内部で `Mutex` を取らない。

### 2.5 Pyclass Single Owner ルール ★継承★

ハンドオーバ §3.2、Phase 2 §2.5。Python binding を追加する場合 (項目 7) も `Py<...>` で wrap、`from_py_object` で Rust に渡す。1 つの Rust struct を 2 個以上の pyclass が共有しない。

---

## 3. Trait 設計

### 3.1 場所と命名

- **新規モジュール**: `src/handle.rs` (top-level、tssrun と sbatch の両方から参照可能)
- **trait 名**:
  - `JobHandleCommon` — 本体 trait (associated type 保持、async fn 含む)
  - `DynJobHandleCommon` — type-erased object-safe 版

`crate::JobHandleCommon` で公開する (`lib.rs` で re-export)。

### 3.2 `JobHandleCommon` trait 定義 (本体)

```rust
// src/handle.rs
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::watch;
use uuid::Uuid;

use crate::store::JobSnapshot;

#[async_trait::async_trait]
pub trait JobHandleCommon: Send + Sync + 'static {
    /// On-disk snapshot 型。`JobSnapshot` (uuid / jobid / kind) を満たす。
    type Snapshot: JobSnapshot;

    // ─── handover §5.4 / Phase 2 §7.1 由来のコア 5 sync getters ───
    fn uuid(&self) -> Uuid;
    fn jobid(&self) -> Option<u64>;
    fn is_running(&self) -> bool;
    fn is_finished(&self) -> bool;
    fn exit_code(&self) -> Option<i32>;

    // ─── snapshot lock-free 読み取り ───
    /// 現在の snapshot のクローン。lock-free。
    fn snapshot(&self) -> Self::Snapshot;
    /// snapshot 更新を購読する watch::Receiver。
    fn watch(&self) -> watch::Receiver<Self::Snapshot>;

    // ─── async fn 共通 ───
    /// SLURM/scheduler に問い合わせて snapshot を更新し、最新を返す。
    /// sbatch: qgroup -l → squeue。tssrun: 子プロセス状態 + qgroup -l → squeue。
    /// **sacct を呼んではいけない** (handover §2 不変条件)。
    async fn refresh(&self) -> Result<Self::Snapshot>;
}
```

### 3.3 `DynJobHandleCommon` trait 定義 (object-safe)

associated type を持つ trait は dyn-safe ではない。ハンドオーバ §4 の Phase 1 教訓どおり、**erase された snapshot を `Arc<dyn JobSnapshot>` ではなく serde JSON `serde_json::Value` で平坦化** する戦略を採る (snapshot 多態は serialize で十分、type erasure は将来 backend 数の増加に頑健)。

```rust
// src/handle.rs (続き)
#[async_trait::async_trait]
pub trait DynJobHandleCommon: Send + Sync + 'static {
    fn uuid(&self) -> Uuid;
    fn jobid(&self) -> Option<u64>;
    fn is_running(&self) -> bool;
    fn is_finished(&self) -> bool;
    fn exit_code(&self) -> Option<i32>;
    /// kind の peek (e.g. `"sbatch"` / `"tssrun"`)
    fn kind(&self) -> &'static str;
    /// snapshot を JSON value として返す (type-erased)
    fn snapshot_json(&self) -> serde_json::Value;
    /// refresh 後の snapshot を JSON value で返す
    async fn refresh_json(&self) -> Result<serde_json::Value>;
}
```

`watch::Receiver<S>` は `S` 多態のままでは dyn 経路に乗らないため、`DynJobHandleCommon` には含めない (本体 trait からのみアクセス可能)。dashboard ユースケースは `snapshot_json()` を polling すれば十分。

### 3.4 `into_dyn()` 明示 constructor

ハンドオーバ §4 教訓「blanket impl は追加しない」を遵守。各 concrete handle に `into_dyn(self)` メソッドを足す (bridge struct `DynHandleAdapter<H>` 経由):

```rust
// src/handle.rs (続き)
pub struct DynHandleAdapter<H: JobHandleCommon> {
    inner: H,
}

impl<H: JobHandleCommon> DynHandleAdapter<H> {
    pub fn new(inner: H) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<H: JobHandleCommon> DynJobHandleCommon for DynHandleAdapter<H> {
    fn uuid(&self) -> Uuid { self.inner.uuid() }
    fn jobid(&self) -> Option<u64> { self.inner.jobid() }
    fn is_running(&self) -> bool { self.inner.is_running() }
    fn is_finished(&self) -> bool { self.inner.is_finished() }
    fn exit_code(&self) -> Option<i32> { self.inner.exit_code() }
    fn kind(&self) -> &'static str { <H::Snapshot as JobSnapshot>::kind() }
    fn snapshot_json(&self) -> serde_json::Value {
        serde_json::to_value(self.inner.snapshot()).expect("JobSnapshot must serialize")
    }
    async fn refresh_json(&self) -> Result<serde_json::Value> {
        let snap = self.inner.refresh().await?;
        Ok(serde_json::to_value(snap)?)
    }
}

pub fn into_dyn<H: JobHandleCommon>(h: H) -> Arc<dyn DynJobHandleCommon> {
    Arc::new(DynHandleAdapter::new(h))
}
```

`SbatchJobHandle` / `TssrunJobHandle` は `JobHandleCommon` を impl すれば `into_dyn(handle)` で消去された Arc が得られる。`SbatchJobHandle::into_dyn(self)` のような shim は提供しない (一貫した entry point は free function `crate::handle::into_dyn` のみ)。

### 3.5 `Sized` 制約と Arc 共有

`SbatchJobHandle` も `TssrunJobHandle` も内部が `Arc<...>` で wrap されており、両方 `Clone` 実装あり。`JobHandleCommon: Send + Sync + 'static` で十分、`Sized` 制約は明示しない (trait object 化を妨げない)。

---

## 4. tssrun 側の整合性合わせ (additive migration)

### 4.1 `JobHandle` / `JobHandleSnapshot` の rename

| 現状 | Phase 3 後 | 備考 |
|---|---|---|
| `tssrun::JobHandleSnapshot` (`src/tssrun/handle.rs:81`) | `tssrun::TssrunJobSnapshot` | `JobSnapshot::kind()` は `"tssrun"` のまま。on-disk JSON `kind` 文字列は不変 |
| `tssrun::JobHandle` (`src/tssrun/handle.rs:114`) | `tssrun::TssrunJobHandle` | sbatch 側 `SbatchJobHandle` と命名対称化 |
| `sbatch::SbatchJobSnapshot` / `sbatch::SbatchJobHandle` | (変更なし) | 既に Tssrun 命名と対称 |

**破壊的変更**: 公開シンボル名が変わる (`crate::JobHandle` / `crate::JobHandleSnapshot` の re-export が `src/lib.rs:48` にあるため downstream にも見える)。

代替案 (より保守的): `pub use TssrunJobHandle as JobHandle;` / `pub use TssrunJobSnapshot as JobHandleSnapshot;` で alias を残す。Phase 3 では rename + alias 両方を提供、Phase 4 で alias 削除。

→ **採用**: rename + alias。alias には `#[deprecated(since = "x.x.x", note = "use TssrunJobHandle/TssrunJobSnapshot")]` を付ける。`src/lib.rs` の re-export も新名 + alias 並列で出す。

### 4.2 `tssrun::TssrunJobHandle::refresh()` の戻り値変更

| 現状 | Phase 3 後 |
|---|---|
| `pub async fn refresh(&self) -> Result<()>` | `pub async fn refresh(&self) -> Result<TssrunJobSnapshot>` |

**変更タイプ**: 既存戻り値が `()` なので、return 値を **足す** だけ。`?` で受けていた既存 caller は壊れない (`let _ = handle.refresh().await?;` で済む)。返り値を使っていない既存 call site の compile error は出ない (Rust は `Result<T>` の `Ok(T)` を捨てても warn しない)。

ただし dead-code lint (`#[must_use]`) を `JobHandleCommon::refresh` の戻り値に付けないことに注意 — 既存コードを silent-break させない。

### 4.3 `sbatch::SbatchJobHandle::refresh()` の戻り値

現状既に `Result<SbatchJobSnapshot>` なので Phase 3 で変更なし。

### 4.4 `wait_terminal()` の trait 含有可否

Phase 2 §7.1 の `wait_terminal` は **「Phase 2 では命名一致のみ、戻り値型は handle ごとに異なってよい」** と緩く宣言された。Phase 3 で trait に含めるかは以下のとおり:

- sbatch: `pub async fn wait_terminal(self, poll_interval: Duration) -> ...` (consume self、master jobid 終端待ち)
- tssrun: 該当する **active polling** メソッドが存在しない (`wait(&mut self)` は子プロセス wait であり Slurm queue 終端ではない)

→ **Phase 3 では trait に含めない**。trait に含めると tssrun 側に新規 `wait_terminal` 実装が必要になり scope が膨らむ。次の項に切り出す:

### 4.5 `tssrun::TssrunJobHandle::wait_terminal()` 追加 (Phase 3 範囲内)

sbatch と命名 / 振る舞いを揃えるため、tssrun 側にも以下を追加:

```rust
// src/tssrun/handle.rs に追加
pub async fn wait_terminal(
    &self,
    poll_interval: std::time::Duration,
) -> Result<TssrunJobSnapshot> {
    loop {
        let snap = self.refresh().await?;
        if snap.is_finished() {
            return Ok(snap);
        }
        tokio::time::sleep(poll_interval).await;
    }
}
```

これで `JobHandleCommon` trait に `async fn wait_terminal` を含めることが可能。

→ **追加採用**: Phase 3 で trait に `wait_terminal` も含める。

### 4.6 既存 `tssrun::JobHandleSnapshot::is_running` / `is_finished` / `exit_code` impl

`src/tssrun/handle.rs:96,101,107` は snapshot 上の helper として既に存在。trait impl on `TssrunJobHandle` の getter はこれらを delegate する形で良い (Phase 2 sbatch と同パターン)。

---

## 5. 既存 attach 経路との関係

### 5.1 attach は変更しない

`SbatchAttachKey` / `attach_uuid` / `attach_jobid` / `attach_file` / `attach_array_jobid` は **concrete handle 型を返し続ける**。multi-backend dashboard 用の `attach_dyn(uuid)` のような統合 entry point は Phase 3 では作らない (kind 多態の attach は store layer の peek を要し、scope が大きい → Phase 4 候補)。

### 5.2 Trait は handle "を持っている" 利用者向け

Phase 3 の `JobHandleCommon` trait は **すでに concrete handle を取得済み** のコードが「型を消して扱いたい」場合のためのもの。attach 段階の type-erasure は対象外。

---

## 6. Python 側の対応 (Tier 2、任意)

### 6.1 `Protocol` (推奨、scope 小)

Python 側で sbatch / tssrun handle を duck-type で受ける利用者向けに、`runtime_checkable` Protocol を提供:

```python
# python/slurm_async_runner/_slurm_async_runner_core/__init__.pyi
from typing import Protocol, runtime_checkable
from uuid import UUID

@runtime_checkable
class JobHandleCommon(Protocol):
    """Common methods on PySbatchJobHandle and PyTssrunJobHandle."""
    def uuid(self) -> UUID: ...
    def jobid(self) -> int | None: ...
    def is_running(self) -> bool: ...
    def is_finished(self) -> bool: ...
    def exit_code(self) -> int | None: ...
    async def refresh(self) -> object: ...  # Snapshot (concrete type per backend)
    async def wait_terminal(self, poll_interval_seconds: float) -> object: ...
```

- `Protocol` だけ stub に書く (実体なし、duck typing)
- `pyo3` 側に新規 `pyclass` は **追加しない** (Pyclass Single Owner ルール継承)
- 既存 `PySbatchJobHandle` / `PyTssrunJobHandle` がメソッドを提供すれば自動的に Protocol を満たす

### 6.2 `PyDynJobHandleCommon` (見送り)

Rust の `Arc<dyn DynJobHandleCommon>` を pyclass として公開する選択肢。**Phase 3 ではやらない**。理由:

- snapshot を JSON で返す `snapshot_json()` のみ pyo3 で表現する場合、Python 側で deserialize が必要 (面倒)
- 既存 concrete `PySbatchJobHandle` / `PyTssrunJobHandle` の方が typed snapshot を返せて使いやすい
- 統一 dashboard が必要になったとき初めて検討 (handover §5.4 trait 化条件と同型の判断)

---

## 7. Plan 分割 (umbrella spec → 4 plans)

| Plan | 含まれる項目 | 想定 LOC | 主要ファイル |
|---|---|---|---|
| **P1** | tssrun handle/snapshot rename (`JobHandle` → `TssrunJobHandle`, `JobHandleSnapshot` → `TssrunJobSnapshot`) + `#[deprecated]` alias 2 種 + `src/lib.rs` re-export 更新 + 全 caller 追従 | 小 (≈250) | `src/tssrun/handle.rs`, `src/tssrun/manager.rs`, `src/lib.rs`, `src/py_export/tssrun.rs`, `python/.../tssrun.pyi`, テスト |
| **P2** | `tssrun::TssrunJobHandle::refresh()` の戻り値を `Result<TssrunJobSnapshot>` 化 + `wait_terminal()` 追加 | 小 (≈200) | `src/tssrun/handle.rs`, `python/.../tssrun.pyi`, テスト |
| **P3** | `crate::handle` 新設 + `JobHandleCommon` trait + `Sbatch`/`Tssrun` への impl + lib.rs re-export | 中 (≈400) | `src/handle.rs` (新), `src/sbatch/handle.rs`, `src/tssrun/handle.rs`, `src/lib.rs`, テスト |
| **P4** | `DynJobHandleCommon` + `DynHandleAdapter<H>` + `into_dyn()` + Python `Protocol` stub | 中 (≈300) | `src/handle.rs`, `python/.../__init__.pyi`, テスト |

**依存関係**: **P1 → P2 → P3 → P4** で完全に直列。P1 が rename して P2 が新シグネチャを追加、P3 が trait を当て、P4 が dyn 化する。並列化の余地は無い (各 plan が前の plan の出力に乗る)。

PR は plan 単位 → develop マージ。Plan ごとに `cargo test --lib --features pyo3 / clippy / fmt / pytest` 全 pass を要件とする。

---

## 8. テスト戦略

### 8.1 unit / integration

Phase 1/2 の規律をそのまま継承:

- 各モジュール同居の `#[cfg(test)] mod tests`
- `tests/` 配下に integration test (新規 `tests/job_handle_common.rs` を P3 で追加)
- `MoveDispatcher` / `PanicDispatcher` / `CannedDispatcher` / `MockCapture` 流用

### 8.2 trait 実装の対称性テスト (P3)

`tests/job_handle_common.rs` で **同じテストフローを sbatch / tssrun 両 handle に対して実行** し、コア 5 names が同一挙動を返すことを確認:

```rust
async fn assert_handle_common_contract<H: JobHandleCommon>(handle: H) {
    // jobid=None when not yet known
    // is_running ⇔ snapshot.is_running の同期
    // is_finished ⇔ snapshot.is_finished の同期
    // exit_code consistency
    // refresh() succeeds and returns snapshot
}

#[tokio::test]
async fn sbatch_handle_satisfies_common_contract() {
    let h = build_sbatch_handle_for_test().await;
    assert_handle_common_contract(h).await;
}

#[tokio::test]
async fn tssrun_handle_satisfies_common_contract() {
    let h = build_tssrun_handle_for_test().await;
    assert_handle_common_contract(h).await;
}
```

### 8.3 dyn 経路のテスト (P4)

`into_dyn(handle)` 経由でも 5 sync getters と `kind()` が正しく返ることを確認。`snapshot_json()` の serde 往復テスト。

### 8.4 Python Protocol テスト (P4 任意)

```python
from slurm_async_runner._slurm_async_runner_core import JobHandleCommon, SbatchJobHandle, TssrunJobHandle

def test_sbatch_handle_satisfies_protocol(...):
    h: SbatchJobHandle = ...
    assert isinstance(h, JobHandleCommon)

def test_tssrun_handle_satisfies_protocol(...):
    h: TssrunJobHandle = ...
    assert isinstance(h, JobHandleCommon)
```

### 8.5 deprecated alias 動作確認 (P1)

```rust
#[allow(deprecated)]
#[test]
fn deprecated_jobhandlesnapshot_alias_still_works() {
    let _: tssrun::JobHandleSnapshot = build_test_snapshot();
}
```

### 8.6 coverage

`cargo llvm-cov --lib --features pyo3` で 80% 維持。

---

## 9. Migration & 後方互換

### 9.1 on-disk JSON 互換 ★最重要★

- `kind = "tssrun"` は **絶対に変更しない** (`TssrunJobSnapshot::kind()` も `"tssrun"` を返す)
- snapshot struct の serde フィールドは追加しない (Phase 3 はメソッド/型名/trait のみ追加)
- 既存 Phase 1/2 で書き出された snapshot ファイルは migration なしでロード可能

### 9.2 公開 API 互換

- `tssrun::JobHandleSnapshot` 削除はせず alias で残す → P1 で破壊的変更なし
- `tssrun::TssrunJobHandle::refresh()` 戻り値の `()` → `Snapshot` 変更は **既存 caller 非破壊** (戻り値を捨てるだけ、Rust は `Ok(T)` 破棄を warn しない)
- `JobHandleCommon` trait の追加は additive
- `into_dyn` / `DynJobHandleCommon` の追加は additive

### 9.3 Python pyo3 binding

P1 は Python 側でも `JobHandleSnapshot` を rename することになる (`tssrun.pyi`)。alias を `.pyi` でも提供:

```python
# tssrun.pyi
class TssrunJobSnapshot:
    ...

JobHandleSnapshot = TssrunJobSnapshot  # deprecated alias
```

### 9.4 CHANGELOG `[Unreleased]`

```markdown
### Phase 3 P1
- BREAKING (alias 提供): `tssrun::JobHandle` → `TssrunJobHandle`, `tssrun::JobHandleSnapshot` → `TssrunJobSnapshot` に rename。互換 alias 2 種を `#[deprecated]` で残す。`crate::JobHandle` / `crate::JobHandleSnapshot` の re-export も新名 + alias 並列。

### Phase 3 P2
- `tssrun::TssrunJobHandle::refresh()` の戻り値を `Result<()>` から `Result<TssrunJobSnapshot>` に変更 (既存 caller 非破壊)。
- `tssrun::TssrunJobHandle::wait_terminal()` を追加。

### Phase 3 P3
- `crate::handle::JobHandleCommon` trait を新設。`SbatchJobHandle` / `TssrunJobHandle` の両方に impl。

### Phase 3 P4
- `crate::handle::DynJobHandleCommon` + `crate::handle::into_dyn()` を追加 (type-erased facade)。
- Python: `runtime_checkable Protocol` `JobHandleCommon` を `__init__.pyi` に追加。
```

---

## 10. クイック・チェックリスト (各 Plan PR 提出前)

ハンドオーバ §8 + Phase 2 §11 + Phase 3 固有を統合:

- [ ] base ブランチが正しい (`develop` に Phase 2 マージ済なら `develop`、未マージなら `sbatch-module-phase2`)
- [ ] vocab 重複なし (`--*` 値型は entities/slurm/sbatch_options/* のみ。Phase 3 では新規追加なし)
- [ ] kind 文字列の追加・変更なし (`"sbatch"` / `"tssrun"` 不変)
- [ ] 新 snapshot フィールドなし (Phase 3 は struct rename のみ)
- [ ] 新 `JobDispatcher` メソッドなし
- [ ] 新 `JobState` variant なし
- [ ] sacct 呼び出しは `refresh_with_sacct` と `run()` 内のみ (Phase 3 trait の `refresh()` は sacct を呼ばない)
- [ ] 公開 attach 経路に kind peek あり (Phase 3 で attach は変更しないが念のため確認)
- [ ] async 内 lock は `tokio::sync::Mutex`
- [ ] CHANGELOG `[Unreleased]` 更新
- [ ] `python/.../*.pyi` 同期
- [ ] `cargo test --lib --features pyo3` / `cargo clippy --all-targets --features pyo3 -- -D warnings` / `cargo fmt --all --check` / `uv run pytest python/tests` 全 pass
- [ ] Live smoke (KUDPC で実行可能なら)
- [ ] **Phase 3 固有**:
  - [ ] `JobHandleCommon` の dyn 化は `DynHandleAdapter<H>` + `into_dyn()` 経由 (blanket impl 禁止)
  - [ ] trait 実装の対称性テスト (sbatch / tssrun 両方で同一 contract)
  - [ ] `tssrun::JobHandleSnapshot` の deprecated alias が compile/動作
  - [ ] `TssrunJobHandle::refresh()` の戻り値変更で既存 call site が壊れないことを `cargo build --all-features` で確認
  - [ ] Plan の依存関係 (P1→P2→P3→P4) を尊重 (PR base は順番に develop)

---

## 11. オープンな決定事項 (実装中に判断、spec では確定しない)

- `DynJobHandleCommon` の `kind()` は `&'static str` で良いか、`Cow<'static, str>` にすべきか — 現状の `JobSnapshot::kind()` が `&'static str` なので合わせる方針
- `Protocol` を Python に出すか出さないか — 出さない場合 P4 は Rust 側のみ。利用予定が見えない場合は **見送り** (YAGNI)
- `into_dyn()` を `pub fn into_dyn<H: JobHandleCommon>(h: H)` の free function にするか、`H::into_dyn(self)` のメソッドにするか — Phase 3 では free function を採用 (既存 `Arc::new(MockManager::with_dispatcher_adapter(...))` のような明示構築パターンに揃える)
- `wait_terminal` の poll_interval default 値 — sbatch は 30 s、tssrun も同じ default で揃えるか別にするか (P2 plan で決定)

---

## 12. 連絡先 / リファレンス

- Phase 1 設計: `docs/superpowers/specs/2026-05-10-sbatch-module-design.md`
- Phase 2 設計: `docs/superpowers/specs/2026-05-10-sbatch-phase2-design.md`
- Phase 1 ハンドオーバ: `docs/attention_phase2.md`
- Phase 2 PR: #6 (`sbatch-module-phase2` → `develop`)
- 統合 baseline: `sbatch-module-phase2` ブランチ (`6838d94`) — `develop` への Phase 2 マージ完了後は `develop` に切替
