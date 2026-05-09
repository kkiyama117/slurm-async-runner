# Phase C2 で発覚: クロス cdylib pyo3 type identity 問題

> **Status:** 未解決。後日ブレスト用にメモを残す。
> **発見日:** 2026-05-09
> **関連 plan:** `docs/superpowers/plans/2026-05-09-merge-tssrun-resourcespec.md`
> **関連 spec:** `docs/superpowers/specs/2026-05-09-merge-tssrun-resourcespec-design.md`

## 1. 何が起きたか

Phase A (`gaussian-job-shared2` の `relax-resource-spec-and-feature-split` branch @ `299d3e8`) と Phase B (このリポジトリの `merge-tssrun-struct` branch @ `0eda5cd`) を完了し、Phase C で両 wheel をビルドして cross-package smoke test を実行したところ、Python が **`ResourceSpec` という名前の型オブジェクトを 2 つ別個に保持してしまい、pyo3 の引数 isinstance チェックに失敗** する事象が発生した。

### 1.1 観測されたエラー

`/tmp/smoke_resourcespec.py` で `from gaussian_job_shared._gaussian_job_shared_core...` と `from slurm_async_runner._slurm_async_runner_core.tssrun` の双方から `ResourceSpec` を import:

```python
from gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options import (
    Memory, ResourceSpec, JobTimeLimit,
)
from slurm_async_runner._slurm_async_runner_core.tssrun import (
    ResourceSpec as ResourceSpec2,
)
assert ResourceSpec is ResourceSpec2  # → AssertionError
```

両者の `__module__` 文字列は同一だが Python type id が異なる:

```
A: <class '...sbatch_options.ResourceSpec'> id=93840473794096 module=gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options
B: <class '...sbatch_options.ResourceSpec'> id=93840473860976 module=gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options
A is B: False
A == B: False
```

`shared2` 由来の `ResourceSpec` インスタンスを SAR の `TssrunCmd.__new__` に渡すと:

```
TypeError: argument 'rsc': 'ResourceSpec' object is not an instance of 'ResourceSpec'
TypeError: argument 'memory': 'Memory' object is not an instance of 'Memory'
TypeError: argument 'time_limit': 'JobTimeLimit' object is not an instance of 'JobTimeLimit'
```

### 1.2 C1 は PASS

linker symbol レベルでは衝突なし:
- `gaussian_job_shared.so` → `PyInit__gaussian_job_shared_core` のみ
- `slurm_async_runner.so` → `PyInit__slurm_async_runner_core` のみ
- bare `PyInit__core` は両 wheel で 0 件

つまり「リンカ衝突回避」という Phase A4 + B6 の **下位の目的は達成できているが**、Phase B が暗黙的に依存していた「Python 型同一性」という上位の目的が崩れている。

## 2. 根本原因

`pyo3-types` cargo feature を経由して shared2 の `#[pyclass]` 実装 (`PyResourceSpec`, `PyJobTimeLimit`, `PyMemory`, ...) が **両方の cdylib にコンパイルされる** ため、各 cdylib が独自の `LazyTypeObject<T>` static を持つ。

- shared2 cdylib (`_gaussian_job_shared_core.abi3.so`) には pyclass impls + module entry がある。
- SAR cdylib (`_slurm_async_runner_core.abi3.so`) も `features = ["pyo3-types"]` 経由で **同じソースコードを別コンパイル** した pyclass impls を持つ。
- Python が両 module を import すると、各 cdylib の static type 登録が独立に発火し、**異なる Python type object が 2 つ生成される**。
- pyo3 の `FromPyObject` 派生 (`#[pyclass(... from_py_object)]`) は自分の cdylib の static type ID と比較するため、別 cdylib の同名インスタンスを reject する。

これは pyo3 の cdylib モデル由来の根源的な制約であり、`#[pymodule_export] use other_crate::PyType` は **再エクスポートではなく重複登録** として作用する。

### 2.1 plan の前提誤り

plan の以下 2 箇所が pyo3 のセマンティクスを誤解していた:

- **`spec §3.5 / Cargo feature graph`**: 「`pyo3-types` を有効にすれば downstream は pyclass 定義だけ取り込む」と書いたが、実際には pyclass 定義は **取り込んだ側の cdylib に登録される** ため独立 type が増える。
- **`spec §3.6 / pymodule rename`** + **`plan B4 Step 4`**: SAR の `tssrun` pymodule に `#[pymodule_export] use gaussian_job_shared::...PyResourceSpec` を書くことで「shared2 と同じ型」が露出すると想定したが、これは別コピーを作るだけで Python 識別子としては別物。

### 2.2 Phase B5 の integration tests がなぜ捕まえなかったか

Phase B5 のテスト (`tests/tssrun_integration.rs`) は SAR の cdylib 単体内で完結しており、shared2 由来の Python 型インスタンスを SAR の関数に渡すパスを **一度も exercise していなかった**。すなわち in-process の Rust 側型整合性しか確認していなかったため、cross-cdylib 境界の不整合を検出できなかった。

## 3. 中間状態 (commit 履歴)

両 repo の commit は破棄せず、後の修正方針決定後に再利用できる状態で温存している。

### gaussian-job-shared2 — branch `relax-resource-spec-and-feature-split` (push 済)
- `e7b1a26` — `feat(slurm)!: allow partial ResourceSpecCPU per KUDPC manual` (A1+A2 combined)
- `8a4445b` — `feat(py)!: positional/kwargs ResourceSpec.__new__ + from_str classmethod` (A3)
- `cf99abd` — `refactor: split pyo3 feature into pyo3-types + pymodule-entry` (A4)
- `81a4543` — `fix(features): enforce pymodule-entry implies pyo3-types via Cargo` (A4 fixup)
- `ed79faf` — `refactor!: rename pymodule _core to _gaussian_job_shared_core` (A5)
- `299d3e8` — `fix(test): adopt ResourceSpec.from_str(...) in test_all.py` (A3 follow-up; HEAD)

### slurm-async-runner2 — branch `merge-tssrun-struct`
- `918ba98` — `deps(shared2): pin to relax-resource-spec branch with pyo3-types` (B1)
- `3dc894a` — `refactor(tssrun)!: replace local Resource with shared2 ResourceSpec` (B2-B5 combined)
- `293b387` — `refactor!: rename pymodule _core to _slurm_async_runner_core` (B6)
- `0eda5cd` — `fix: update stale gaussian_job_shared._core docstrings + UPSTREAM_STATUS_MODULE` (HEAD)

両 repo で `cargo test` (134 + 81 pass) と `cargo clippy --all-targets --all-features -- -D warnings` は clean。`maturin build --release` も両 repo で通る。**唯一の壊れ方は「複数 wheel を同一 Python プロセスにロードしたときの cross-cdylib 型不整合」のみ**。

## 4. 修正候補 (ブレスト用 stub)

### A. Rust 側を `Bound<PyAny>` ベースに再設計
- SAR の dep を `default-features = false` に戻し、shared2 の pyclass impls を SAR cdylib から完全に切り離す
- `PyTssrunCmd::__new__(rsc: Option<&Bound<PyAny>>, time_limit: Option<&Bound<PyAny>>, ...)` に変更
- shared2 側に `__rust_inner_str__` のような extraction API を追加するか、Display/FromStr roundtrip で Rust 型を取り出す
- 課題: empty CPU の Display=`""` が FromStr で reject される非対称をどう扱うか
- 規模: 中。Phase B4 を再設計し、shared2 にも extraction support を追加

### B. Python facade で型変換
- SAR の `python/slurm_async_runner/__init__.py` に Python ラッパを置き、ユーザが渡した shared2 `ResourceSpec` を `from_str(str(spec))` で SAR-cdylib の `ResourceSpec` に変換してから Rust 呼び出し
- 課題:
  - 変換オーバーヘッドが発生
  - empty CPU (`ResourceSpec()` → `str(...) = ""`) で `FromStr` が err を返す。SAR 側でこの空文字列ケースを特別扱いする必要
  - SAR cdylib が依然として `Memory`, `JobTimeLimit` などを内包する必要があるため重複登録自体は残る
- 規模: 軽量だが workaround 多め

### C. SAR から shared2 型を露出しない
- SAR の `tssrun` pymodule から `#[pymodule_export] use gaussian_job_shared::...` を削除
- Python ユーザは `from gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options import ResourceSpec` で直接 import
- SAR cdylib にも独自の `ResourceSpec`/`JobTimeLimit` 型が登録され、これは SAR 内部用とする
- 課題: 「import path 統一」という plan §3.6 の設計目的を諦める形になる
- 規模: 最小

### D. 別の cdylib モデルを検討
- 1 つの workspace で 1 つの cdylib に集約 (`slurm_async_runner` を `gaussian_job_shared` の subcrate にする等)
- 規模: 最大。リポジトリ構造の変更を要する

### E. type sharing via abi3 + Python sys.modules patching
- shared2 の pyclass を SAR cdylib に **コンパイルしない** ように feature を制御 (pyo3-types を更に細分化)
- SAR のコードは shared2 の Python 型を `Py::import("...").getattr("ResourceSpec")` で動的に取得
- 規模: 中〜大。pyo3 の `dep:` インターフェースをかなり細かく扱う必要

## 5. 残タスク

1. **plan + spec 更新** — Phase A4/A5/B4/B6 の前提を訂正し、C2 で発覚した事象を §「リスク」「設計トレードオフ」節に反映
2. **修正方針の選定** — 上記 A〜E をブレストし、user の優先順位 (API ergonomics vs リファクタ規模 vs パフォーマンス) に合わせて選択
3. **(選定後) 修正実装** — 新方針で B4 相当 (もしくは shared2 側追加変更) を実装
4. **C2 smoke test の再設計** — `assert ResourceSpec is ResourceSpec2` のような cross-cdylib 同一性前提を外し、選定した方針に合わせた smoke 内容に書き換える
5. **B5 integration tests に cross-cdylib カバレッジを追加** — Python レベルで shared2 と SAR を同時 load する test を最低 1 本入れて、同種の regression が再発しないようにする

## 6. 参考リンク

- pyo3 Issue #1444 (sharing pyclasses between multiple Rust packages): <https://github.com/PyO3/pyo3/issues/1444> — **2021 年に open、2026 年現在も未解決 / 未実装**。`pyclass_export!` / `pyclass_import!` 案も merge されていない。
- pyo3-polars `FromPyObject` 実装 (業界標準の duck-typing 抽出): <https://github.com/pola-rs/pyo3-polars/blob/main/pyo3-polars/src/types.rs>
- pyo3-arrow: PyCapsule Interface による cross-cdylib データ受け渡し: <https://docs.rs/pyo3-arrow/>
- KUDPC `--rsc` manual: <https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/resource#rscoption>

## 7. 業界標準調査 (2026-05-09)

主要 pyo3 ライブラリ群を調査した結果、**「複数 cdylib 間で `#[pyclass]` の型同一性を共有する」ことを正面から解決した実装は存在しない**。pyo3 issue #1444 は 5 年以上 open のまま放置されており、これは pyo3 の cdylib モデル由来の **根源的な制約** として受け止めるのが現実的。

代わりに業界標準として確立しているパターンは **Duck-typing + Protocol-based extraction in `FromPyObject`**。

### 7.1 pyo3-polars の前例 (`pyo3-polars/src/types.rs`)

`PySeries::extract_bound` は **pyclass downcast に依存しない**:

```rust
impl<'a> FromPyObject<'a> for PySeries {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let ob = ob.call_method0("rechunk")?;             // duck-typing
        let name = ob.getattr("name")?;                   // duck-typing
        let py_name = name.str()?;
        let name = py_name.to_cow()?;
        let kwargs = PyDict::new(ob.py());
        if let Ok(compat_level) = ob.call_method0("_newest_compat_level") {
            let compat_level = compat_level.extract().unwrap();
            let compat_level = CompatLevel::with_level(compat_level)
                .unwrap_or(CompatLevel::newest());
            kwargs.set_item("compat_level", compat_level.get_level())?;
        }
        let arr = ob.call_method("to_arrow", (), Some(&kwargs))?;  // Arrow C interface
        let arr = ffi::to_rust::array_to_rust(&arr)?;
        let name = name.as_ref();
        Ok(PySeries(
            Series::try_from((PlSmallStr::from(name), arr)).map_err(PyPolarsErr::from)?,
        ))
    }
}
```

ポイント:
- 入力 `ob: &Bound<'a, PyAny>` は **どの cdylib 由来でもよい**
- `call_method0("rechunk")` / `getattr("name")` / `call_method("to_arrow", ...)` は Python レベルの protocol → cdylib 境界を超える
- Arrow C interface で zero-copy に Rust 側 `Series` を再構築
- `PyDataFrame` も同様 (`get_columns` + `width` を duck-typing 抽出)
- `PyLazyFrame` は `__getstate__()` の bytes を deserialize (binary protocol)

これにより、pyo3-polars を使った plugin cdylib は **本家 polars wheel の `Series` インスタンスをそのまま受け取れる**。pyclass identity に一切依存していない。

### 7.2 pyo3-arrow / Arrow PyCapsule Interface

Arrow ecosystem は `__arrow_c_schema__` / `__arrow_c_array__` / `__arrow_c_stream__` の **3 つの dunder** を protocol として標準化。`FromPyObject` はこれらを `getattr` → `call0` し、PyCapsule からポインタを取り出す。`#[pyclass]` 派生型に一切依存しない。

### 7.3 結論

「pyclass を共有する」のではなく「**Python オブジェクトを protocol として扱う**」のが pyo3 ecosystem 5 年の蓄積による answer。我々の `error.md §4` 候補は **A の発展形** が最有力 — ただし polars 前例に倣えば `Bound<PyAny>` 引数として露出させる必要はなく、**普通に `PyTssrunCmd::__new__(rsc: Option<RscBridge>)` と書いて pyo3 に自動で `FromPyObject` を呼ばせれば良い**。

## 8. 採用推奨候補: F. Duck-typing `FromPyObject` Bridge (Polars-style)

### 概要

shared2 の pyclass impls を **SAR cdylib に一切コンパイルしない** (= `pyo3-types` feature を外して `default-features = false` に戻す)。代わりに SAR 側に **bridge 型** (`RscBridge`, `JobTimeLimitBridge`, `MemoryBridge`) を定義し、独自の `FromPyObject` を **getattr ベースの duck-typing** で実装する。

### コード骨子 (SAR `src/py_export/bridge.rs`)

```rust
// shared2 の Rust 型 (pyclass ではない素の enum/struct) を内包
pub(crate) struct RscBridge(pub gaussian_job_shared::ResourceSpec);

impl<'py> FromPyObject<'py> for RscBridge {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        // Duck-typing: shared2 PyResourceSpec が公開している getter と同じ名前
        let processes: Option<u32> = ob.getattr(intern!(py, "processes"))?.extract()?;
        let threads:   Option<u32> = ob.getattr(intern!(py, "threads"))?.extract()?;
        let cores:     Option<u32> = ob.getattr(intern!(py, "cores"))?.extract()?;
        let gpus:      Option<u32> = ob.getattr(intern!(py, "gpus"))?.extract()?;
        let memory_any = ob.getattr(intern!(py, "memory"))?;
        let memory = if memory_any.is_none() {
            None
        } else {
            Some(MemoryBridge::extract_bound(&memory_any)?.0)
        };
        // Rust 型として再構築 (shared2 の pub fn from_parts(...) を使う)
        let spec = ResourceSpec::from_parts(processes, threads, cores, memory, gpus)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self(spec))
    }
}
```

`PyTssrunCmd::__new__` は普通の named 引数で受ける:

```rust
#[new]
fn new(
    cmd: String,
    partition: Option<String>,
    time_limit: Option<JobTimeLimitBridge>,
    rsc: Option<RscBridge>,
) -> PyResult<Self> { ... }
```

ユーザ視点では `PyTssrunCmd(rsc=shared2.ResourceSpec(processes=4))` と書ける。**shared2 の `ResourceSpec` インスタンスがそのまま渡せる**。

### shared2 側の追加要件

- `ResourceSpec::from_parts(processes, threads, cores, memory, gpus) -> Result<ResourceSpec>` を **Rust 公開 API** として export (現状 `PyResourceSpec::new` の中にしか同等ロジックがない)。これ自体は §A3 の改修と一貫しており、Rust 側の API 整合性も向上する。
- `pyo3-types` feature は維持 (shared2 自身の wheel build のため) だが、SAR は使わなくなる。

### 候補 A〜E 比較表

| 候補 | 規模 | API 一貫性 | ユーザ ergonomics | shared2 への変更 | cdylib サイズ |
|------|------|-----------|-------------------|----------------|---------------|
| A (Bound<PyAny>) | 中 | △ (型なし露出) | △ | 中 | 小 |
| B (Python facade) | 小 | △ | ○ | なし | 大のまま |
| C (露出諦め) | 最小 | × (path 二系統) | △ | なし | 大のまま |
| D (cdylib 統合) | 最大 | ○ | ○ | 構造変更 | 大 |
| E (動的 import) | 中〜大 | ○ | ○ | feature 細分化 | 小 |
| **F (本案)** | **中** | **○** | **◎** | **小 (`from_parts` 公開のみ)** | **小** |

候補 F が **規模・ergonomics・cdylib サイズの全項目でベスト**。前例 (polars) の実証が 5 年あり、リスクも低い。

### 適用後の予想構成

- `gaussian_job_shared/Cargo.toml`: 変更なし (現状の feature split は維持)
- `slurm_async_runner/Cargo.toml`: `gaussian_job_shared = { ..., default-features = false }` に戻す (`features = ["pyo3-types"]` 削除)
- `slurm_async_runner/src/py_export/bridge.rs`: 新規。bridge 型 + `FromPyObject` 実装
- `slurm_async_runner/src/py_export/tssrun.rs`: `#[pymodule_export] use gaussian_job_shared::...` を削除し、bridge 引数で受ける
- `slurm_async_runner/src/py_export/mod.rs`: shared2 由来の pyclass を再 export しない
- `gaussian-job-shared2/src/entities/.../resource_spec.rs`: `pub fn ResourceSpec::from_parts(...)` を追加
