# コードマップ

「この機能はどのファイルにある?」を引くための逆引きマップです。
レイヤーの設計思想は [`architecture.md`](./architecture.md)、
ランタイム挙動は [`process-flow.md`](./process-flow.md) を参照。

## 1. リポジトリ全体の構造

```
slurm-async-runner2/
├── Cargo.toml             # 依存関係 + feature gate (pyo3 / stub_gen)
├── Cargo.lock
├── pyproject.toml         # maturin / ruff / uv 設定
├── rust-toolchain.toml    # nightly 固定 (pyo3 nightly feature 用)
├── README.md              # 公開 API のクイックリファレンス
├── CHANGELOG.md           # Keep a Changelog 形式
│
├── src/                   # Rust ソース
│   ├── lib.rs             # crate root / re-export ハブ
│   ├── manager.rs         # SlurmCmd / SlurmManager (Spec 層)
│   ├── dispatcher.rs      # JobDispatcher / Background系 (Runtime 層)
│   ├── runner.rs          # squeue/sacct/qgroup パース + バッチ問合せ (Query 層)
│   ├── store.rs           # 汎用 JobStateStore<S> trait + InMemoryStateStore<S> /
│   │                      # FileSystemStateStore<S> (kind discriminator で
│   │                      # tssrun/sbatch を同居)
│   ├── handle.rs          # (PR #7) JobHandleCommon trait + DynJobHandleCommon
│   │                      # + DynHandleAdapter<H> + into_dyn()
│   ├── util/              # path 絶対化ヘルパ ほか
│   │   ├── mod.rs
│   │   └── path.rs        # absolutize (旧 sbatch/cmd.rs + tssrun/cmd.rs から DRY 化)
│   ├── entities/          # in-tree SLURM 語彙 (PR #5 で gaussian_job_shared から移管)
│   │   └── slurm/         # JobStatus / JobState / JobReason / ResourceSpec /
│   │                      # JobTimeLimit / JobPartition / Memory / ArraySpec /
│   │                      # SlurmDependency / MailTypeInput / SlurmSignalSpec ...
│   ├── tssrun/            # tssrun サブシステム
│   │   ├── mod.rs         # 公開 re-export と概要
│   │   ├── cmd.rs         # TssrunCmd (rsc: ResourceSpec / time_limit: JobTimeLimit)
│   │   ├── parse.rs       # salloc: 行パーサ (純関数)
│   │   ├── log.rs         # JobLogSink trait + 4 実装
│   │   ├── handle.rs      # TssrunJobHandle / TssrunJobSnapshot / live_env
│   │   │                  # (PR #7 で JobHandle/JobHandleSnapshot から rename。
│   │   │                  #  #[deprecated] alias は PR #11 で削除済み)
│   │   ├── store.rs       # tssrun 固有の薄いラッパ (汎用 store は src/store.rs)
│   │   └── manager.rs     # TssrunManager: spawn / attach / query_state
│   ├── sbatch/            # sbatch サブシステム (PR #6)
│   │   ├── mod.rs         # 公開 re-export と概要
│   │   ├── cmd.rs         # SbatchCmd (typed --flag フィールド + build_argv)
│   │   ├── parse.rs       # qgroup -l / sacct / submitted-jobid パーサ
│   │   ├── handle.rs      # SbatchJobHandle / SbatchJobSnapshot / FinishedInfo /
│   │   │                  # SbatchLifecycle / log_lines / refresh_with_sacct /
│   │   │                  # wait_terminal
│   │   ├── store.rs       # sbatch 固有 store ラッパ
│   │   ├── manager.rs     # SbatchManager: spawn / spawn_array / run / cancel /
│   │   │                  # attach_uuid / attach_jobid / attach_array_jobid /
│   │   │                  # attach_file
│   │   └── error.rs       # SbatchSpawnError / SbatchRunError / SbatchCancelError /
│   │                      # SbatchAttachError (#[non_exhaustive] typed enum)
│   ├── py_export/         # pyo3 公開層
│   │   ├── mod.rs         # _slurm_async_runner_core モジュール定義
│   │   ├── manager.rs     # PySlurmCmd / PySlurmManager
│   │   ├── runner.rs      # query_job_states_batch (async)
│   │   ├── tssrun.rs      # PyTssrunCmd / PyLogSink / PyTssrunJobHandle /
│   │   │                  # PyTssrunManager / PyJobStateStore (PyResourceSpec /
│   │   │                  # PyJobTimeLimit を再エクスポート)。PR #7 で
│   │   │                  # uuid/jobid/is_running/is_finished/exit_code が sync 化
│   │   ├── sbatch.rs      # PySbatchCmd / PySbatchManager / PySbatchJobHandle /
│   │   │                  # PyFinishedInfo (PR #6)
│   │   └── entities/      # SLURM 語彙の pyclass (status / sbatch_options)
│   │                      # signal.rs は sbatch_options 内の Rust 分割で、
│   │                      # SlurmSignalSpec は sbatch_options 名前空間に統合露出
│   └── bin/
│       └── stub_gen.rs    # pyo3-stub-gen を起動して .pyi を再生成
│
├── tests/                 # Rust 統合テスト (cargo test 経由)
│   ├── tssrun_integration.rs    # tssrun spawn/wait/attach 一連
│   └── job_handle_common.rs     # (PR #7) SbatchJobHandle / TssrunJobHandle
│                                # 両方が JobHandleCommon の同一 contract を満たす
│                                # ことを generic test fn で検証
│
├── python/                # Python 側パッケージ
│   ├── slurm_async_runner/
│   │   ├── __init__.py    # _slurm_async_runner_core を _core で取り込み、
│   │   │                  # 加えて PR #7 の JobHandleCommon Protocol を定義
│   │   └── _slurm_async_runner_core/   # *.pyi 型スタブ置き場
│   │       ├── __init__.pyi            # 自動生成 (pyo3-stub-gen)
│   │       ├── manager.pyi             # 手書き (async pyfunctions)
│   │       ├── runner.pyi              # 手書き
│   │       ├── tssrun.pyi              # 手書き
│   │       ├── sbatch.pyi              # 手書き (PR #6)
│   │       └── entities/slurm/         # 自動生成 (status / sbatch_options)
│   │           ├── status/__init__.pyi
│   │           └── sbatch_options/__init__.pyi  # ResourceSpec / Memory /
│   │                                            # JobTimeLimit / JobPartition /
│   │                                            # ArraySpec / SlurmDependency /
│   │                                            # MailTypeInput / SlurmSignalSpec ...
│   └── tests/             # pytest スイート
│       ├── test_all.py          # 既存挙動の regression
│       ├── test_tssrun.py       # tssrun サブシステムの async テスト
│       ├── test_tssrun_live.py  # 実機ライブテスト (RUN_LIVE_TSSRUN ゲート)
│       ├── test_sbatch.py       # sbatch サブシステム (PR #6)
│       └── test_protocol.py     # (PR #7) JobHandleCommon Protocol が
│                                # 両 backend に対して isinstance / call shape
│                                # の両方で成立することを検証
│
├── scripts/
│   ├── test_tssrun_live.py     # スタンドアロン版ライブスモークテスト (tssrun)
│   └── test_sbatch_live.py     # 同 (sbatch、PR #6)
│
├── docs/                  # 本ドキュメント群
│   ├── README.md          # 索引
│   ├── architecture.md    # 設計の全体像
│   ├── code-map.md        # ← このファイル
│   ├── process-flow.md    # 主要フロー
│   ├── development.md     # 開発手順
│   ├── setup_test.md      # 実機セットアップ
│   └── superpowers/       # 機能追加時の設計ドラフト履歴
│       ├── plans/
│       └── specs/
│
└── .github/workflows/
    ├── test.yml           # PR/push 時の cargo + pytest CI
    ├── CI.yml             # wheel ビルド (push to main/develop)
    └── release.yml        # v* タグで wheel + sdist を GitHub Releases へ
```

## 2. ファイル別の責務

### 2.1 ルート設定

| ファイル | 中身 |
|---|---|
| `Cargo.toml` | 依存（anyhow / thiserror / tokio / serde / **uuid (v4+v7)** / **async-trait** / pyo3 / pyo3-async-runtimes / pyo3-stub-gen）と `[features] default = ["pyo3", "stub_gen"]`。PR #5 で `gaussian_job_shared` への直接依存は撤廃（SLURM 語彙は in-tree に移管）。`pyo3` feature は **このクレートが pyclass の唯一の owner** ということを宣言している（Pyclass Single Owner ルール、`Cargo.toml:99-112` のコメント参照） |
| `pyproject.toml` | maturin の `module-name = "slurm_async_runner._slurm_async_runner_core"` 設定、`features = ["pyo3/extension-module"]`、ruff の `target-version = "py312"` |
| `rust-toolchain.toml` | `channel = "nightly"`（pyo3 の `"nightly"` feature が要求） |

### 2.2 Rust コア (`src/`)

| ファイル | 公開している主な型/関数 |
|---|---|
| `lib.rs` | `pub use` の集中管理。`JobReason` / `JobState` / `JobStatus`（in-tree 移管後）/ `SlurmCmd` / `SlurmManager` / `JobDispatcher` / `TokioDispatcher` / `DryRunDispatcher` / `BackgroundDispatcher` / `SpawnedChild` / `TokioBackgroundDispatcher` / tssrun モジュールの主要型 (`TssrunJobHandle` / `TssrunJobSnapshot`。旧 `JobHandle` / `JobHandleSnapshot` alias は PR #11 で削除) / sbatch モジュールの主要型 (`SbatchCmd` / `SbatchManager` / `SbatchJobHandle` / `SbatchJobSnapshot` / `FinishedInfo` / `SbatchSpawnError` ほか) / `JobStateStore` / `InMemoryStateStore` / `FileSystemStateStore` / **跨 backend handle 抽象 (PR #7)**: `handle::{JobHandleCommon, DynJobHandleCommon, DynHandleAdapter}` / SLURM 語彙再エクスポート（`JobPartition` / `JobTimeLimit` / `Memory` / `MemoryUnit` / `ResourceSpec` / `ResourceSpecCPU` / `ResourceSpecGPU` / `SlurmArraySpec` / `SlurmDependency` / `MailTypeInput` / `SlurmSignalSpec`） |
| `manager.rs` | `SlurmCmd::new / build_argv`, `SlurmManager::{run_job, run_job_with, query_job_state, query_job_states_batch}` |
| `dispatcher.rs` | `JobDispatcher`, `TokioDispatcher`, `DryRunDispatcher`, `BackgroundDispatcher`, `SpawnedChild`, `TokioBackgroundDispatcher`, `DynJobDispatcher` (dyn-safe facade) + `DynDispatcherAdapter` + `into_dyn(d)` |
| `runner.rs` | `query_job_states_batch`, `query_job_states_batch_with`, `query_job_states_with_exit_code_with` (PR #6), `query_array_task_state_with` / `query_array_task_outcome_with` (PR #12: `<master>_<idx>` 形式で squeue/sacct を直接叩く per-task クエリ), 内部 `parse_squeue` / `parse_sacct` / `parse_qgroup_l` / `merge_results` / `parse_squeue_array_task` / `parse_sacct_array_task_with_exit_code` |
| `store.rs` | 汎用 `JobSnapshot` trait (`kind() -> &'static str` 付き) + `JobStateStore<S>` trait + `InMemoryStateStore<S>` + `FileSystemStateStore<S>` (atomic-rename + kind discriminator による silent-skip) |
| `handle.rs` (PR #7) | `JobHandleCommon` trait (associated `Snapshot: JobSnapshot` 型) + `DynJobHandleCommon` trait (`serde_json::Value` で flatten) + `DynHandleAdapter<H>` + `into_dyn<H>(h) -> Arc<dyn DynJobHandleCommon>` |
| `util/path.rs` | `absolutize(path)` の DRY 化 (旧 `sbatch::cmd` / `tssrun::cmd` / `manager` の重複を統合) |

### 2.3 tssrun サブシステム (`src/tssrun/`)

| ファイル | 公開している主な型/関数 |
|---|---|
| `mod.rs` | サブモジュール宣言と概要 doc-comment |
| `cmd.rs` | `TssrunCmd { tssrun_bin, partition: Option<JobPartition>, time_limit: Option<JobTimeLimit>, rsc: Option<ResourceSpec>, x11, program, args, env, cwd }`, `TssrunCmd::build_argv`。リソース仕様型は in-tree の `crate::entities::slurm::ResourceSpec`（PR #5 で旧 `Resource` をリプレース） |
| `parse.rs` | `parse_salloc_jobid(line) -> Option<u64>`, `parse_salloc_node(line) -> Option<String>` |
| `log.rs` | `LogStream`, `JobLogSink` trait, `NullLogSink`, `StdLogSink`, `InMemoryLogSink`, `FileLogSink::create` |
| `handle.rs` | `LogLocations`, `FinishedInfo`, `TssrunJobSnapshot { uuid, pid, argv, sent_env, cwd, started_at_unix, log_locations, jobid, node, finished }` (PR #7 で `JobHandleSnapshot` から rename。旧 alias は PR #11 で削除)、`TssrunJobHandle::{from_spawn, attach_snapshot, watch, snapshot, uuid, pid, jobid, node, sent_env, is_running, is_finished, exit_code, wait, refresh, wait_terminal, live_env}` (PR #7 で `refresh` の戻り値が `Result<TssrunJobSnapshot>` に変更、`wait_terminal` 追加、`is_finished` 追加)、free fn `read_live_env_for_pid` |
| `store.rs` | tssrun 固有の薄いラッパ。汎用 store trait は `crate::store` 側で集約 |
| `manager.rs` | `AttachKey { Uuid, Pid, JobId, File }`, `TssrunManager::{new, with_state_dir, with_state_store, with_log_sink, store, spawn, spawn_with, attach, query_state}` |

### 2.3.5 sbatch サブシステム (`src/sbatch/`、PR #6)

| ファイル | 公開している主な型/関数 |
|---|---|
| `mod.rs` | サブモジュール宣言と概要 doc-comment |
| `cmd.rs` | `SbatchCmd { sbatch_bin, job_name, partition, time_limit, output, error, env, cwd, no_requeue, comment, signal, dependency, mail_user, mail_types, array_spec, ... }`, `SbatchCmd::build_argv`。値型はすべて `entities::slurm::sbatch_options::*` |
| `parse.rs` | `parse_submitted_jobid` (sbatch 出力), `parse_qgroup_l_line`, `parse_sacct_exit_code` ほか |
| `handle.rs` | `SbatchJobSnapshot { uuid, jobid, kind="sbatch", last_observed_state, array_task_id, finished, left_active_listing, ... }`, `SbatchJobHandle::{watch, snapshot, uuid, jobid, is_running, is_finished, exit_code, array_task_id, refresh, refresh_with_sacct, wait_terminal, log_lines, read_log_to_end}`, `SbatchLifecycle`, `FinishedInfo`, `LogStream { Stdout, Stderr }`。PR #10 で `array_jobid` フィールドと getter を削除（`array_task_id.is_some()` が単発/array task の唯一の discriminator）。PR #12 で `refresh()` / `refresh_with_sacct()` に array-task branch を追加: `array_task_id.is_some()` の handle は `qgroup -l` を skip して `query_array_task_state_with` / `query_array_task_outcome_with` で `<master>_<idx>` 直叩きする。PR #9 で `SbatchJobHandleInner` に `Drop` を実装し、終端到達前に最終 clone が drop された場合は `tracing::warn!` を発火 |
| `store.rs` | sbatch 固有 store ラッパ |
| `manager.rs` | `SbatchAttachKey`, `SbatchManager::{new, with_dispatcher, with_state_dir, with_poll_interval, spawn, spawn_array, run, cancel, attach_uuid, attach_jobid, attach_array_jobid, attach_file, find_all_by_jobid}` |
| `error.rs` | `SbatchSpawnError`, `SbatchRunError`, `SbatchCancelError`, `SbatchAttachError` (`#[non_exhaustive]` typed enum 群) |

### 2.4 pyo3 公開層 (`src/py_export/`)

| ファイル | Python 名 | 中身 |
|---|---|---|
| `mod.rs` | `slurm_async_runner._slurm_async_runner_core` | トップ pymodule（pymodule 名は PR #5 で `_core` から rename）。`runner` / `manager` / `tssrun` / `sbatch` / `entities.slurm.*` の inner_module を export。`sum_as_string` というデモ関数も入っている |
| `manager.rs` | `slurm_async_runner._slurm_async_runner_core.manager` | `SlurmCmd` / `SlurmManager`。`run_job` / `query_job_state` / `query_job_states_batch` は `pyo3_async_runtimes::tokio::future_into_py` で coroutine 化 |
| `runner.rs` | `slurm_async_runner._slurm_async_runner_core.runner` | `query_job_states_batch`。`PyOnceLock<Py<PyAny>>` で **このクレート自身の** `JobStatus` クラスをプロセス内 1 回だけ import（PR #5 で `gaussian_job_shared` から in-tree 移管） |
| `tssrun.rs` | `slurm_async_runner._slurm_async_runner_core.tssrun` | `TssrunCmd` / `LogSink` / `TssrunJobHandle` / `TssrunManager` / `JobStateStore` + sink ファクトリ（`null_log_sink` / `std_log_sink` / `file_log_sink`）と store ファクトリ（`in_memory_state_store` / `file_system_state_store`）、加えて再エクスポート pyclass の `ResourceSpec` / `JobTimeLimit`。**スナップショット getter は `watch::Receiver` から読む**ので `wait()` の Mutex を持たない（lock-free 設計）。PR #7 で `uuid` / `jobid` / `is_running` / `is_finished` / `exit_code` を sync 化 (`*_async` エスケープハッチは PR #6 review fix で削除済み) |
| `sbatch.rs` | `slurm_async_runner._slurm_async_runner_core.sbatch` | PR #6 で追加。`SbatchCmd` / `SbatchManager` / `SbatchJobHandle` / `FinishedInfo`。`run` / `cancel` などは `future_into_py` で coroutine 化。 5 つの共通 sync getter (`uuid` / `jobid` / `is_running` / `is_finished` / `exit_code`) を tssrun と同じシグネチャで提供 |
| `entities/slurm/status.rs` | `slurm_async_runner._slurm_async_runner_core.entities.slurm.status` | `JobStatus` / `JobState` / `JobReason` の pyclass。PR #5 で `gaussian_job_shared` から本クレートに移管された SLURM 語彙の正本 |
| `entities/slurm/sbatch_options.rs` | `slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options` | `ResourceSpec` / `ResourceSpecCPU` / `ResourceSpecGPU` / `JobTimeLimit` / `JobPartition` / `Memory` / `MemoryUnit` / `ArraySpec` / `SlurmDependency` / `MailTypeInput` ほか sbatch オプション系の pyclass |
| `entities/slurm/sbatch_options/signal.rs` | (Python 上は独立 submodule を持たず、`SlurmSignalSpec` は親 `sbatch_options` 名前空間に直接 add される) | `SlurmSignalSpec` の pyclass impl (PR #6)。Rust 上だけのファイル分割 |

### 2.5 stub 生成 (`src/bin/stub_gen.rs`)

`pyo3-stub-gen` を呼び、自動生成可能な範囲（= top-level の sync な
pyfunction、および `#[gen_stub_pyclass]` を付けた pyclass）の型スタブを
`python/slurm_async_runner/_slurm_async_runner_core/__init__.pyi` ほか
配下のサブモジュール stub に書き出します。`#[pymodule_export]` でぶら
下げた async pyfunction や lock-free getter はジェネレータの対象外
なので、`manager.pyi` / `runner.pyi` / `tssrun.pyi` は **手書き** です
（`entities/slurm/*` 配下は stub_gen 側で自動生成）。

### 2.6 Python パッケージ (`python/slurm_async_runner/`)

| ファイル | 中身 |
|---|---|
| `__init__.py` | `_slurm_async_runner_core` を取り込み、加えて PR #7 で追加された **`JobHandleCommon` Protocol** (`runtime_checkable`) を定義・`__all__` に追加。`JobHandleCommon` の `uuid` / `jobid` は `@property`、`is_running` / `is_finished` / `exit_code` は sync method、`refresh` / `wait_terminal` は async (PR #7 の sync 化を反映) |
| `_slurm_async_runner_core/__init__.pyi` | `pyo3-stub-gen` 自動生成（`sum_as_string` のみ） |
| `_slurm_async_runner_core/manager.pyi` | 手書き。`SlurmCmd`, `SlurmManager` の型 |
| `_slurm_async_runner_core/runner.pyi` | 手書き。`query_job_states_batch` の型 |
| `_slurm_async_runner_core/tssrun.pyi` | 手書き。`TssrunCmd`/`LogSink`/`TssrunJobHandle`/`TssrunManager`/`JobStateStore`、再エクスポート `ResourceSpec`/`JobTimeLimit`、各 sink/store ファクトリ。PR #7 で sync `uuid` / `jobid` (`@property`) と sync `is_running` / `is_finished` / `exit_code` メソッドに変更（`*_async` エスケープハッチは PR #6 review fix で削除済み） |
| `_slurm_async_runner_core/sbatch.pyi` | 手書き (PR #6)。`SbatchCmd` / `SbatchManager` / `SbatchJobHandle` / `FinishedInfo`。5 つの共通 sync getter を tssrun と同じ shape で公開 |
| `_slurm_async_runner_core/entities/slurm/status/__init__.pyi` | stub_gen 自動生成。`JobStatus` / `JobState` / `JobReason` |
| `_slurm_async_runner_core/entities/slurm/sbatch_options/__init__.pyi` | stub_gen 自動生成。`ResourceSpec` / `ResourceSpecCPU` / `ResourceSpecGPU` / `JobTimeLimit` / `JobPartition` / `Memory` / `MemoryUnit` / `ArraySpec` / `SlurmDependency` / `MailTypeInput` / `SlurmSignalSpec` ほか (PR #6 で項目追加) |

### 2.7 テスト

| ファイル | 種別 | 動かすコマンド |
|---|---|---|
| `src/**/*.rs` の `#[cfg(test)] mod tests` | 単体テスト（Rust） | `cargo test --lib` |
| `tests/tssrun_integration.rs` | 統合テスト（Rust） | `cargo test --test tssrun_integration` |
| `tests/job_handle_common.rs` | 跨 backend contract test (PR #7)。sbatch / tssrun handle が `JobHandleCommon` の同一 contract を満たすことを generic test fn で検証 | `cargo test --test job_handle_common` |
| `python/tests/test_all.py` | 既存挙動 regression（pytest） | `uv run pytest python/tests/test_all.py` |
| `python/tests/test_tssrun.py` | tssrun サブシステムの async テスト | `uv run pytest python/tests/test_tssrun.py` |
| `python/tests/test_tssrun_live.py` | 実機ライブ tssrun（要 `RUN_LIVE_TSSRUN=1`） | `RUN_LIVE_TSSRUN=1 uv run pytest python/tests/test_tssrun_live.py` |
| `python/tests/test_sbatch.py` | sbatch サブシステム (PR #6) | `uv run pytest python/tests/test_sbatch.py` |
| `python/tests/test_protocol.py` | (PR #7) `JobHandleCommon` Protocol の structural type check + 実際の call shape | `uv run pytest python/tests/test_protocol.py` |
| `scripts/test_tssrun_live.py` | スタンドアロン実機 tssrun | `uv run python scripts/test_tssrun_live.py` |
| `scripts/test_sbatch_live.py` | スタンドアロン実機 sbatch | `uv run python scripts/test_sbatch_live.py` |

### 2.8 CI (`.github/workflows/`)

| ファイル | トリガー | やること |
|---|---|---|
| `test.yml` | push to main/master, PR, manual | nightly toolchain + Python 3.12 で `cargo fmt --check` / `cargo clippy -- -D warnings` / `cargo test --lib` / `maturin develop` / `pytest` / `ruff check` / `ruff format --check` |
| `CI.yml` | push to main/develop, PR, manual | linux (manylinux x86_64) と windows (x64) で wheel をビルドして smoke 用にアーティファクト化 (publish は行わない) |
| `release.yml` | `v*` タグ push, manual | manylinux (x86_64, aarch64) + musllinux x86_64 + windows (x64, arm64) + macos arm64 (macos-latest) + sdist をビルドし、タグ名のリリースに **GitHub Releases** アセットとしてアップロード (PyPI には publish しない) |

## 3. 「○○ をいじりたい」逆引き表

| やりたいこと | 触る主なファイル |
|---|---|
| `srun` に渡す引数を増やす | `src/manager.rs::SlurmCmd::build_argv` + 対応する `src/py_export/manager.rs::PySlurmCmd` |
| 別のジョブランチャ（例: `mpirun`）に対応させる | `SlurmCmd::srun_cmd` を変えるだけで OK。`JobDispatcher` には触らない |
| dry-run の出力フォーマットを変える | `src/dispatcher.rs::DryRunDispatcher` |
| `squeue` の出力フォーマットを変える | `src/runner.rs::query_job_states_batch_with` の argv と `parse_squeue` |
| 新しい SLURM 状態トークンに対応する | `src/entities/slurm/status.rs` の `JobState` / `JobReason` に variant を追加。`is_running` / `is_terminal` 両方を同時に更新（PR #5 で in-tree 移管済み） |
| `tssrun` の `--rsc` キーを増やす | `src/entities/slurm/sbatch_options/resource_spec.rs::ResourceSpecCPU` / `ResourceSpecGPU` のフィールドと `Display` |
| `sbatch` の `--*` フラグを増やす | typed entity を `src/entities/slurm/sbatch_options/<flag>.rs` に追加して `FromStr` / `Display` / `Serialize` 実装し、`src/sbatch/cmd.rs::SbatchCmd` にフィールドと argv 出力を足す（sbatch サブシステム導入時 (PR #6) の vocab 重複禁止ルールに従う） |
| `salloc:` バナーが site-specific に書き換わった | `src/tssrun/parse.rs` に新しい prefix を追加（既存をいじらず分岐推奨） |
| `qgroup -l` / `sacct` の出力フォーマットが site-specific に違う | `src/runner.rs::parse_qgroup_l` (KUDPC は `\|` 区切り + 先頭 2 行のサマリ行をスキップ、PR #14) / `src/sbatch/parse.rs::parse_sacct_exit_code` を更新 |
| KUDPC `qgroup -l` の新トークン (例: `EXIT`, `CANC`, `TOUT` 等) を追加する | `src/entities/slurm/status.rs::JobState::parse` の `match` に `"NEW" => Self::Variant` を追加 (FINI → Completed / FAIL → Failed と同じ input-only alias パターン、`as_token()` は SLURM 正規語彙を返し続ける、PR #14)。`is_terminal()` でも認識させたい場合は `JobState::is_terminal` の `matches!` リストにも追加 |
| ジョブ終了後に `handle.is_finished()` / `exit_code()` が反映されない | (1) qgroup の STAT トークンが未マッピングで `Unknown` 扱いされていないか — KUDPC は `FINI` / `FAIL` を追加済 (PR #14) (2) `refresh()` / `refresh_with_sacct()` の watch 更新が `send_replace` を使っているか — `send` だと receiver 0 で silent fail (`src/sbatch/handle.rs` / `src/tssrun/handle.rs`、PR #14)。詳細は `docs/architecture.md` §3.5 設計判断 #6 |
| sbatch のデフォルト polling 間隔を変える | `src/sbatch/manager.rs::SbatchManager::new` の `poll_interval` 既定値 (PR #14 で 30s → 60s、SLURM の task-sampling 30s 既定を必ず跨ぐ設計)、または呼び出し側で `with_poll_interval(Duration)` |
| `capture` の失敗時にエラー詳細が空で困る | `src/dispatcher.rs::TokioDispatcher::capture` が stdout 末尾に `[stderr]` マーカー区切りで stderr を結合する (PR #14)。`SbatchSpawnError::SubmitFailed::output` に sbatch の `sbatch: error: ...` がそのまま入る |
| 配列ジョブの per-task ステート/exit_code を取りたい | `array_task_id.is_some()` の `SbatchJobHandle` から `refresh()` / `refresh_with_sacct()` を呼ぶ。内部では `<master>_<idx>` をキーに `query_array_task_state_with` (`squeue -j ...`) と `query_array_task_outcome_with` (`sacct -j ...`) が叩かれる (PR #12)。master 集計を返す `qgroup -l` は array task 経路では skip される |
| 配列ジョブの squeue / sacct 出力パーサを増やす | `src/runner.rs::parse_squeue_array_task` / `parse_sacct_array_task_with_exit_code`（`<master>_<idx>` 形式の `JobID` を扱う）に分岐を追加 |
| ログ出力先を増やす（DB / 外部 API） | `src/tssrun/log.rs` で `JobLogSink` を実装した型を追加 |
| Snapshot 永続化バックエンドを増やす（Redis / SQLite 等） | `src/store.rs` で `#[async_trait] impl JobStateStore<S> for X` を追加。tssrun と sbatch の両方で再利用可能 |
| クロスプロセス attach を有効にしたい | `TssrunManager::new(cmd).with_state_dir(path)` / `SbatchManager::new(...).with_state_dir(path)`（FS バックエンド）に切り替え。デフォルトの `InMemoryStateStore` ではプロセス間で共有できない |
| 跨 backend handle ABC（Rust）で受けたい | `H: JobHandleCommon` で generic に取る。dyn が必要なら `crate::handle::into_dyn(h)` で `Arc<dyn DynJobHandleCommon>` を作る（PR #7、architecture.md §3.6） |
| 跨 backend handle ABC（Python）で受けたい | `from slurm_async_runner import JobHandleCommon` の Protocol を `isinstance(h, JobHandleCommon)` でチェック (PR #7) |
| 新しい backend (e.g. `srun` 同期 handle) を追加したい | `src/<backend>/handle.rs` で `impl JobHandleCommon for X` を書き、`tests/job_handle_common.rs` の汎用 contract test に fixture を 1 つ足す |
| Python 側の async API を増やす | `src/py_export/<module>.rs` に `#[pyfunction]` または `#[pyclass]` を追加し、対応する `_core/*.pyi` を手書きで更新 |
| Python に新しい sync ヘルパーを追加 | `src/py_export/mod.rs` に `#[pyo3_stub_gen::derive::gen_stub_pyfunction]` を付けて足す。`cargo run --bin stub_gen` で `.pyi` 再生成 |
| 新しい dispatcher を実装 | `src/dispatcher.rs` に `impl JobDispatcher for X` を足し、必要なら `BackgroundDispatcher` も実装。dyn 経由で `SbatchManager::with_dispatcher` に注入したい場合は `into_dyn(d)` で wrap |

## 4. 依存ライブラリの読み解き

| クレート | 主な用途 | 読むときの参照 |
|---|---|---|
| `tokio` | async ランタイム / `process::Command` / `sync::watch` / `sync::Mutex` | `dispatcher.rs`, `tssrun/handle.rs` |
| `pyo3` 0.28 | Python バインディング (abi3-py312, nightly feature) | `py_export/*` |
| `pyo3-async-runtimes` 0.28 | Tokio Future ↔ Python coroutine 変換 | `py_export/*` の `future_into_py` |
| `pyo3-stub-gen` | top-level pyfunction の `.pyi` 生成 | `bin/stub_gen.rs` |
| `pyo3-log` | Rust `log` crate を Python `logging` にブリッジ | `py_export/mod.rs` |
| `pythonize` | Rust ↔ Python 値変換 | （現状の使用頻度は低い） |
| `serde` / `serde_json` | snapshot のシリアライズ (両 backend) + `DynJobHandleCommon::snapshot_json` の JSON flatten | `tssrun/handle.rs`, `sbatch/handle.rs`, `store.rs`, `handle.rs` |
| `tempfile` | atomic-rename による snapshot 書き込み | `store.rs::write_atomic_json` |
| `uuid` (`v4` + `v7` features) | snapshot の primary key（時刻順 UUID v7） | `tssrun/handle.rs`, `sbatch/handle.rs`, `store.rs`, `tssrun/manager.rs`, `sbatch/manager.rs` |
| `async-trait` | `JobStateStore` / `JobHandleCommon` / `DynJobHandleCommon` の `async fn` を `dyn Trait` で持つための desugaring | `store.rs`, `handle.rs`, `tssrun/store.rs` |
| `anyhow` / `thiserror` | エラーハンドリング (sbatch 側は `#[non_exhaustive]` typed enum を厚めに使う) | crate 全体 |
| `tracing` / `log` | 構造化ログ | `tssrun/handle.rs` / `sbatch/handle.rs` 中心 |

> 注: PR #5 (`docs/superpowers/specs/2026-05-10-slurm-vocab-migration-and-pyclass-ownership-design.md`)
> で `gaussian_job_shared` への依存は撤廃された。SLURM 語彙
> (`JobStatus` / `JobState` / `JobReason` / `ResourceSpec` / `JobTimeLimit` /
> `Memory` ほか) はこのクレートが正本。
