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
│   ├── handle.rs          # Phase 3: JobHandleCommon trait + DynJobHandleCommon
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
│   │   │                  # (Phase 3 P1 で JobHandle/JobHandleSnapshot から rename、
│   │   │                  #  #[deprecated] alias を保持)
│   │   ├── store.rs       # tssrun 固有の薄いラッパ (汎用 store は src/store.rs)
│   │   └── manager.rs     # TssrunManager: spawn / attach / query_state
│   ├── sbatch/            # sbatch サブシステム (Phase 2)
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
│   │   │                  # PyJobTimeLimit を再エクスポート)。Phase 3 P5 で
│   │   │                  # uuid/jobid/is_running/is_finished/exit_code が sync 化
│   │   ├── sbatch.rs      # PySbatchCmd / PySbatchManager / PySbatchJobHandle /
│   │   │                  # PyFinishedInfo (Phase 2)
│   │   └── entities/      # SLURM 語彙の pyclass (status / sbatch_options / signal)
│   └── bin/
│       └── stub_gen.rs    # pyo3-stub-gen を起動して .pyi を再生成
│
├── tests/                 # Rust 統合テスト (cargo test 経由)
│   ├── tssrun_integration.rs    # tssrun spawn/wait/attach 一連
│   └── job_handle_common.rs     # Phase 3: SbatchJobHandle / TssrunJobHandle
│                                # 両方が JobHandleCommon の同一 contract を満たす
│                                # ことを generic test fn で検証
│
├── python/                # Python 側パッケージ
│   ├── slurm_async_runner/
│   │   ├── __init__.py    # _slurm_async_runner_core を _core で取り込み、
│   │   │                  # 加えて Phase 3 P4 の JobHandleCommon Protocol を定義
│   │   └── _slurm_async_runner_core/   # *.pyi 型スタブ置き場
│   │       ├── __init__.pyi            # 自動生成 (pyo3-stub-gen)
│   │       ├── manager.pyi             # 手書き (async pyfunctions)
│   │       ├── runner.pyi              # 手書き
│   │       ├── tssrun.pyi              # 手書き
│   │       ├── sbatch.pyi              # 手書き (Phase 2)
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
│       ├── test_sbatch.py       # sbatch サブシステム (Phase 2)
│       └── test_protocol.py     # Phase 3 P4/P5: JobHandleCommon Protocol が
│                                # 両 backend に対して isinstance / call shape
│                                # の両方で成立することを検証
│
├── scripts/
│   ├── test_tssrun_live.py     # スタンドアロン版ライブスモークテスト (tssrun)
│   └── test_sbatch_live.py     # 同 (sbatch、Phase 2)
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
    └── CI.yml             # wheel ビルド + PyPI 配信
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
| `lib.rs` | `pub use` の集中管理。`JobReason` / `JobState` / `JobStatus`（in-tree 移管後）/ `SlurmCmd` / `SlurmManager` / `JobDispatcher` / `TokioDispatcher` / `DryRunDispatcher` / `BackgroundDispatcher` / `SpawnedChild` / `TokioBackgroundDispatcher` / tssrun モジュールの主要型 (`TssrunJobHandle` / `TssrunJobSnapshot` + deprecated `JobHandle` / `JobHandleSnapshot` alias) / sbatch モジュールの主要型 (`SbatchCmd` / `SbatchManager` / `SbatchJobHandle` / `SbatchJobSnapshot` / `FinishedInfo` / `SbatchSpawnError` ほか) / `JobStateStore` / `InMemoryStateStore` / `FileSystemStateStore` / **Phase 3**: `handle::{JobHandleCommon, DynJobHandleCommon, DynHandleAdapter}` / SLURM 語彙再エクスポート（`JobPartition` / `JobTimeLimit` / `Memory` / `MemoryUnit` / `ResourceSpec` / `ResourceSpecCPU` / `ResourceSpecGPU` / `SlurmArraySpec` / `SlurmDependency` / `MailTypeInput` / `SlurmSignalSpec`） |
| `manager.rs` | `SlurmCmd::new / build_argv`, `SlurmManager::{run_job, run_job_with, query_job_state, query_job_states_batch}` |
| `dispatcher.rs` | `JobDispatcher`, `TokioDispatcher`, `DryRunDispatcher`, `BackgroundDispatcher`, `SpawnedChild`, `TokioBackgroundDispatcher`, `DynJobDispatcher` (dyn-safe facade) + `DynDispatcherAdapter` + `into_dyn(d)` |
| `runner.rs` | `query_job_states_batch`, `query_job_states_batch_with`, `query_job_states_with_exit_code_with` (Phase 2 P1), 内部 `parse_squeue` / `parse_sacct` / `parse_qgroup_l` / `merge_results` |
| `store.rs` | 汎用 `JobSnapshot` trait (`kind() -> &'static str` 付き) + `JobStateStore<S>` trait + `InMemoryStateStore<S>` + `FileSystemStateStore<S>` (atomic-rename + kind discriminator による silent-skip) |
| `handle.rs` (Phase 3) | `JobHandleCommon` trait (associated `Snapshot: JobSnapshot` 型) + `DynJobHandleCommon` trait (`serde_json::Value` で flatten) + `DynHandleAdapter<H>` + `into_dyn<H>(h) -> Arc<dyn DynJobHandleCommon>` |
| `util/path.rs` | `absolutize(path)` の DRY 化 (旧 `sbatch::cmd` / `tssrun::cmd` / `manager` の重複を統合) |

### 2.3 tssrun サブシステム (`src/tssrun/`)

| ファイル | 公開している主な型/関数 |
|---|---|
| `mod.rs` | サブモジュール宣言と概要 doc-comment |
| `cmd.rs` | `TssrunCmd { tssrun_bin, partition: Option<JobPartition>, time_limit: Option<JobTimeLimit>, rsc: Option<ResourceSpec>, x11, program, args, env, cwd }`, `TssrunCmd::build_argv`。リソース仕様型は in-tree の `crate::entities::slurm::ResourceSpec`（PR #5 で旧 `Resource` をリプレース） |
| `parse.rs` | `parse_salloc_jobid(line) -> Option<u64>`, `parse_salloc_node(line) -> Option<String>` |
| `log.rs` | `LogStream`, `JobLogSink` trait, `NullLogSink`, `StdLogSink`, `InMemoryLogSink`, `FileLogSink::create` |
| `handle.rs` | `LogLocations`, `FinishedInfo`, `TssrunJobSnapshot { uuid, pid, argv, sent_env, cwd, started_at_unix, log_locations, jobid, node, finished }` (Phase 3 P1 で `JobHandleSnapshot` から rename、`#[deprecated]` alias 保持)、`TssrunJobHandle::{from_spawn, attach_snapshot, watch, snapshot, uuid, pid, jobid, node, sent_env, is_running, is_finished, exit_code, wait, refresh, wait_terminal, live_env}` (Phase 3 P2 で `refresh` の戻り値が `Result<TssrunJobSnapshot>` に変更、`wait_terminal` 追加、`is_finished` 追加)、free fn `read_live_env_for_pid` |
| `store.rs` | tssrun 固有の薄いラッパ。汎用 store trait は `crate::store` 側で集約 |
| `manager.rs` | `AttachKey { Uuid, Pid, JobId, File }`, `TssrunManager::{new, with_state_dir, with_state_store, with_log_sink, store, spawn, spawn_with, attach, query_state}` |

### 2.3.5 sbatch サブシステム (`src/sbatch/`、Phase 2)

| ファイル | 公開している主な型/関数 |
|---|---|
| `mod.rs` | サブモジュール宣言と概要 doc-comment |
| `cmd.rs` | `SbatchCmd { sbatch_bin, job_name, partition, time_limit, output, error, env, cwd, no_requeue, comment, signal, dependency, mail_user, mail_types, array_spec, ... }`, `SbatchCmd::build_argv`。値型はすべて `entities::slurm::sbatch_options::*` |
| `parse.rs` | `parse_submitted_jobid` (sbatch 出力), `parse_qgroup_l_line`, `parse_sacct_exit_code` ほか |
| `handle.rs` | `SbatchJobSnapshot { uuid, jobid, kind="sbatch", last_observed_state, array_jobid, array_task_id, finished, left_active_listing, ... }`, `SbatchJobHandle::{watch, snapshot, uuid, jobid, is_running, is_finished, exit_code, array_jobid, array_task_id, refresh, refresh_with_sacct, wait_terminal, log_lines, read_log_to_end}`, `SbatchLifecycle`, `FinishedInfo`, `LogStream { Stdout, Stderr }` |
| `store.rs` | sbatch 固有 store ラッパ |
| `manager.rs` | `SbatchAttachKey`, `SbatchManager::{new, with_dispatcher, with_state_dir, with_poll_interval, spawn, spawn_array, run, cancel, attach_uuid, attach_jobid, attach_array_jobid, attach_file, find_all_by_jobid}` |
| `error.rs` | `SbatchSpawnError`, `SbatchRunError`, `SbatchCancelError`, `SbatchAttachError` (`#[non_exhaustive]` typed enum 群) |

### 2.4 pyo3 公開層 (`src/py_export/`)

| ファイル | Python 名 | 中身 |
|---|---|---|
| `mod.rs` | `slurm_async_runner._slurm_async_runner_core` | トップ pymodule（pymodule 名は PR #5 で `_core` から rename）。`runner` / `manager` / `tssrun` / `sbatch` / `entities.slurm.*` の inner_module を export。`sum_as_string` というデモ関数も入っている |
| `manager.rs` | `slurm_async_runner._slurm_async_runner_core.manager` | `SlurmCmd` / `SlurmManager`。`run_job` / `query_job_state` / `query_job_states_batch` は `pyo3_async_runtimes::tokio::future_into_py` で coroutine 化 |
| `runner.rs` | `slurm_async_runner._slurm_async_runner_core.runner` | `query_job_states_batch`。`PyOnceLock<Py<PyAny>>` で **このクレート自身の** `JobStatus` クラスをプロセス内 1 回だけ import（PR #5 で `gaussian_job_shared` から in-tree 移管） |
| `tssrun.rs` | `slurm_async_runner._slurm_async_runner_core.tssrun` | `TssrunCmd` / `LogSink` / `TssrunJobHandle` / `TssrunManager` / `JobStateStore` + sink ファクトリ（`null_log_sink` / `std_log_sink` / `file_log_sink`）と store ファクトリ（`in_memory_state_store` / `file_system_state_store`）、加えて再エクスポート pyclass の `ResourceSpec` / `JobTimeLimit`。**スナップショット getter は `watch::Receiver` から読む**ので `wait()` の Mutex を持たない（lock-free 設計）。Phase 3 P5 で `uuid` / `jobid` / `is_running` / `is_finished` / `exit_code` を sync 化、`*_async` を backward-compat として併設 |
| `sbatch.rs` | `slurm_async_runner._slurm_async_runner_core.sbatch` | Phase 2 で追加。`SbatchCmd` / `SbatchManager` / `SbatchJobHandle` / `FinishedInfo`。`run` / `cancel` などは `future_into_py` で coroutine 化。 5 つの共通 sync getter (`uuid` / `jobid` / `is_running` / `is_finished` / `exit_code`) を tssrun と同じシグネチャで提供 |
| `entities/slurm/status.rs` | `slurm_async_runner._slurm_async_runner_core.entities.slurm.status` | `JobStatus` / `JobState` / `JobReason` の pyclass。PR #5 で `gaussian_job_shared` から本クレートに移管された SLURM 語彙の正本 |
| `entities/slurm/sbatch_options.rs` | `slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options` | `ResourceSpec` / `ResourceSpecCPU` / `ResourceSpecGPU` / `JobTimeLimit` / `JobPartition` / `Memory` / `MemoryUnit` / `ArraySpec` / `SlurmDependency` / `MailTypeInput` ほか sbatch オプション系の pyclass |
| `entities/slurm/sbatch_options/signal.rs` | `slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options.signal` | `SlurmSignalSpec` (Phase 2 P4) |

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
| `__init__.py` | `_slurm_async_runner_core` を取り込み、加えて Phase 3 P4 で追加された **`JobHandleCommon` Protocol** (`runtime_checkable`) を定義・`__all__` に追加。`JobHandleCommon` の `uuid` / `jobid` は `@property`、`is_running` / `is_finished` / `exit_code` は sync method、`refresh` / `wait_terminal` は async (Phase 3 P5 の sync 化を反映) |
| `_slurm_async_runner_core/__init__.pyi` | `pyo3-stub-gen` 自動生成（`sum_as_string` のみ） |
| `_slurm_async_runner_core/manager.pyi` | 手書き。`SlurmCmd`, `SlurmManager` の型 |
| `_slurm_async_runner_core/runner.pyi` | 手書き。`query_job_states_batch` の型 |
| `_slurm_async_runner_core/tssrun.pyi` | 手書き。`TssrunCmd`/`LogSink`/`TssrunJobHandle`/`TssrunManager`/`JobStateStore`、再エクスポート `ResourceSpec`/`JobTimeLimit`、各 sink/store ファクトリ。Phase 3 P5 で sync `uuid` / `jobid` (`@property`) と sync `is_running` / `is_finished` / `exit_code` メソッドに変更、`*_async` を併設 |
| `_slurm_async_runner_core/sbatch.pyi` | 手書き (Phase 2)。`SbatchCmd` / `SbatchManager` / `SbatchJobHandle` / `FinishedInfo`。5 つの共通 sync getter を tssrun と同じ shape で公開 |
| `_slurm_async_runner_core/entities/slurm/status/__init__.pyi` | stub_gen 自動生成。`JobStatus` / `JobState` / `JobReason` |
| `_slurm_async_runner_core/entities/slurm/sbatch_options/__init__.pyi` | stub_gen 自動生成。`ResourceSpec` / `ResourceSpecCPU` / `ResourceSpecGPU` / `JobTimeLimit` / `JobPartition` / `Memory` / `MemoryUnit` / `ArraySpec` / `SlurmDependency` / `MailTypeInput` / `SlurmSignalSpec` ほか (Phase 2 で項目追加) |

### 2.7 テスト

| ファイル | 種別 | 動かすコマンド |
|---|---|---|
| `src/**/*.rs` の `#[cfg(test)] mod tests` | 単体テスト（Rust） | `cargo test --lib` |
| `tests/tssrun_integration.rs` | 統合テスト（Rust） | `cargo test --test tssrun_integration` |
| `tests/job_handle_common.rs` | Phase 3 跨 backend contract test。sbatch / tssrun handle が `JobHandleCommon` の同一 contract を満たすことを generic test fn で検証 | `cargo test --test job_handle_common` |
| `python/tests/test_all.py` | 既存挙動 regression（pytest） | `uv run pytest python/tests/test_all.py` |
| `python/tests/test_tssrun.py` | tssrun サブシステムの async テスト | `uv run pytest python/tests/test_tssrun.py` |
| `python/tests/test_tssrun_live.py` | 実機ライブ tssrun（要 `RUN_LIVE_TSSRUN=1`） | `RUN_LIVE_TSSRUN=1 uv run pytest python/tests/test_tssrun_live.py` |
| `python/tests/test_sbatch.py` | sbatch サブシステム (Phase 2) | `uv run pytest python/tests/test_sbatch.py` |
| `python/tests/test_protocol.py` | Phase 3 P4/P5: `JobHandleCommon` Protocol の structural type check + 実際の call shape | `uv run pytest python/tests/test_protocol.py` |
| `scripts/test_tssrun_live.py` | スタンドアロン実機 tssrun | `uv run python scripts/test_tssrun_live.py` |
| `scripts/test_sbatch_live.py` | スタンドアロン実機 sbatch | `uv run python scripts/test_sbatch_live.py` |

### 2.8 CI (`.github/workflows/`)

| ファイル | トリガー | やること |
|---|---|---|
| `test.yml` | push to main/master, PR, manual | nightly toolchain + Python 3.12 で `cargo fmt --check` / `cargo clippy -- -D warnings` / `cargo test --lib` / `maturin develop` / `pytest` / `ruff check` / `ruff format --check` |
| `CI.yml` | （wheel ビルド + PyPI 公開のフロー） | manylinux wheel と sdist を作って PyPI へ |

## 3. 「○○ をいじりたい」逆引き表

| やりたいこと | 触る主なファイル |
|---|---|
| `srun` に渡す引数を増やす | `src/manager.rs::SlurmCmd::build_argv` + 対応する `src/py_export/manager.rs::PySlurmCmd` |
| 別のジョブランチャ（例: `mpirun`）に対応させる | `SlurmCmd::srun_cmd` を変えるだけで OK。`JobDispatcher` には触らない |
| dry-run の出力フォーマットを変える | `src/dispatcher.rs::DryRunDispatcher` |
| `squeue` の出力フォーマットを変える | `src/runner.rs::query_job_states_batch_with` の argv と `parse_squeue` |
| 新しい SLURM 状態トークンに対応する | `src/entities/slurm/status.rs` の `JobState` / `JobReason` に variant を追加。`is_running` / `is_terminal` 両方を同時に更新（PR #5 で in-tree 移管済み） |
| `tssrun` の `--rsc` キーを増やす | `src/entities/slurm/sbatch_options/resource_spec.rs::ResourceSpecCPU` / `ResourceSpecGPU` のフィールドと `Display` |
| `sbatch` の `--*` フラグを増やす | typed entity を `src/entities/slurm/sbatch_options/<flag>.rs` に追加して `FromStr` / `Display` / `Serialize` 実装し、`src/sbatch/cmd.rs::SbatchCmd` にフィールドと argv 出力を足す（Phase 2 spec §2.1 の vocab 重複禁止に従う） |
| `salloc:` バナーが site-specific に書き換わった | `src/tssrun/parse.rs` に新しい prefix を追加（既存をいじらず分岐推奨） |
| `qgroup -l` / `sacct` の出力フォーマットが site-specific に違う | `src/sbatch/parse.rs::parse_qgroup_l_line` / `parse_sacct_exit_code` を更新 |
| ログ出力先を増やす（DB / 外部 API） | `src/tssrun/log.rs` で `JobLogSink` を実装した型を追加 |
| Snapshot 永続化バックエンドを増やす（Redis / SQLite 等） | `src/store.rs` で `#[async_trait] impl JobStateStore<S> for X` を追加。tssrun と sbatch の両方で再利用可能 |
| クロスプロセス attach を有効にしたい | `TssrunManager::new(cmd).with_state_dir(path)` / `SbatchManager::new(...).with_state_dir(path)`（FS バックエンド）に切り替え。デフォルトの `InMemoryStateStore` ではプロセス間で共有できない |
| 跨 backend handle ABC（Rust）で受けたい | `H: JobHandleCommon` で generic に取る。dyn が必要なら `crate::handle::into_dyn(h)` で `Arc<dyn DynJobHandleCommon>` を作る（Phase 3 §3.6） |
| 跨 backend handle ABC（Python）で受けたい | `from slurm_async_runner import JobHandleCommon` の Protocol を `isinstance(h, JobHandleCommon)` でチェック (Phase 3 P4) |
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
