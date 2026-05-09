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
│   ├── runner.rs          # squeue/sacct パース + バッチ問合せ (Query 層)
│   ├── tssrun/            # tssrun サブシステム
│   │   ├── mod.rs         # 公開 re-export と概要
│   │   ├── cmd.rs         # TssrunCmd / Resource (Spec)
│   │   ├── parse.rs       # salloc: 行パーサ (純関数)
│   │   ├── log.rs         # JobLogSink trait + 4 実装
│   │   ├── handle.rs      # JobHandle / Snapshot / live_env
│   │   ├── store.rs       # JobStateStore trait + InMemory / FS 実装
│   │   └── manager.rs     # TssrunManager: spawn / attach / query_state
│   ├── py_export/         # pyo3 公開層
│   │   ├── mod.rs         # _core モジュール定義
│   │   ├── manager.rs     # PySlurmCmd / PySlurmManager
│   │   ├── runner.rs      # query_job_states_batch (async)
│   │   └── tssrun.rs      # PyResource / PyTssrunCmd / PyLogSink /
│   │                      # PyTssrunJobHandle / PyTssrunManager
│   └── bin/
│       └── stub_gen.rs    # pyo3-stub-gen を起動して .pyi を再生成
│
├── tests/                 # Rust 統合テスト (cargo test 経由)
│   └── tssrun_integration.rs
│
├── python/                # Python 側パッケージ
│   ├── slurm_async_runner/
│   │   ├── __init__.py    # _core を import するだけの薄いラッパ
│   │   └── _core/         # *.pyi 型スタブ置き場
│   │       ├── __init__.pyi   # 自動生成 (pyo3-stub-gen)
│   │       ├── manager.pyi    # 手書き (async pyfunctions)
│   │       ├── runner.pyi     # 手書き
│   │       └── tssrun.pyi     # 手書き
│   └── tests/             # pytest スイート
│       ├── test_all.py        # 既存挙動の regression
│       ├── test_tssrun.py     # tssrun サブシステムの async テスト
│       └── test_tssrun_live.py# 実機ライブテスト (RUN_LIVE_TSSRUN ゲート)
│
├── scripts/
│   └── test_tssrun_live.py    # スタンドアロン版ライブスモークテスト
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
| `Cargo.toml` | 依存（anyhow / thiserror / tokio / serde / **uuid (v4+v7)** / **async-trait** / pyo3 / pyo3-async-runtimes / pyo3-stub-gen / `gaussian_job_shared`）と `[features] default = ["pyo3", "stub_gen"]`。`gaussian_job_shared` は `default-features = false` 必須（理由は同ファイルのコメント参照） |
| `pyproject.toml` | maturin の `module-name = "slurm_async_runner._core"` 設定、`features = ["pyo3/extension-module"]`、ruff の `target-version = "py312"` |
| `rust-toolchain.toml` | `channel = "nightly"`（pyo3 の `"nightly"` feature が要求） |

### 2.2 Rust コア (`src/`)

| ファイル | 公開している主な型/関数 | 行数の目安 |
|---|---|---|
| `lib.rs` | `pub use` の集中管理。`JobReason` / `JobState` / `JobStatus` / `SlurmCmd` / `SlurmManager` / `JobDispatcher` / `TokioDispatcher` / `DryRunDispatcher` / `BackgroundDispatcher` / `SpawnedChild` / `TokioBackgroundDispatcher` / tssrun モジュールの主要型（`JobStateStore` / `InMemoryStateStore` / `FileSystemStateStore` 含む） | 67 |
| `manager.rs` | `SlurmCmd::new / build_argv`, `SlurmManager::{run_job, run_job_with, query_job_state, query_job_states_batch}` | 243 |
| `dispatcher.rs` | `JobDispatcher`, `TokioDispatcher`, `DryRunDispatcher`, `BackgroundDispatcher`, `SpawnedChild`, `TokioBackgroundDispatcher` | 269 |
| `runner.rs` | `query_job_states_batch`, `query_job_states_batch_with`, 内部 `parse_squeue` / `parse_sacct` / `merge_results` | 370 |

### 2.3 tssrun サブシステム (`src/tssrun/`)

| ファイル | 公開している主な型/関数 |
|---|---|
| `mod.rs` | サブモジュール宣言と概要 doc-comment |
| `cmd.rs` | `Resource { processes, threads, cores, memory, gpus }`, `TssrunCmd { tssrun_bin, queue, time_limit, rsc, x11, program, args, env, cwd }`, `TssrunCmd::build_argv` |
| `parse.rs` | `parse_salloc_jobid(line) -> Option<u64>`, `parse_salloc_node(line) -> Option<String>` |
| `log.rs` | `LogStream`, `JobLogSink` trait, `NullLogSink`, `StdLogSink`, `InMemoryLogSink`, `FileLogSink::create` |
| `handle.rs` | `LogLocations`, `FinishedInfo`, `JobHandleSnapshot { uuid, pid, argv, sent_env, cwd, started_at_unix, log_locations, jobid, node, finished }`, `JobHandle::{from_spawn, attach_snapshot, watch, snapshot, uuid, pid, jobid, node, sent_env, is_running, exit_code, wait, refresh_from_disk, live_env}`, free fn `read_live_env_for_pid` |
| `store.rs` | `JobStateStore` trait（`save` / `load` / `find_by_pid` / `find_by_jobid`）と組み込み実装 `InMemoryStateStore`（`HashMap<Uuid, _>`）/ `FileSystemStateStore`（`{dir}/{uuid}.json`、atomic-rename、ディレクトリ遅延作成、欠損ディレクトリは `Ok(None)`） |
| `manager.rs` | `AttachKey { Uuid, Pid, JobId, File }`, `TssrunManager::{new, with_state_dir, with_state_store, with_log_sink, store, spawn, spawn_with, attach, query_state}` |

### 2.4 pyo3 公開層 (`src/py_export/`)

| ファイル | Python 名 | 中身 |
|---|---|---|
| `mod.rs` | `slurm_async_runner._core` | トップ pymodule。`runner` / `manager` / `tssrun` の inner_module を export。`sum_as_string` というデモ関数も入っている |
| `manager.rs` | `slurm_async_runner._core.manager` | `SlurmCmd` / `SlurmManager`。`run_job` / `query_job_state` / `query_job_states_batch` は `pyo3_async_runtimes::tokio::future_into_py` で coroutine 化 |
| `runner.rs` | `slurm_async_runner._core.runner` | `query_job_states_batch`。`PyOnceLock<Py<PyAny>>` で `gaussian_job_shared` 側の `JobStatus` クラスをプロセス内 1 回だけ import |
| `tssrun.rs` | `slurm_async_runner._core.tssrun` | `Resource` / `TssrunCmd` / `LogSink` / `TssrunJobHandle` / `TssrunManager` + 3 つの sink ファクトリ関数。**スナップショット getter は `watch::Receiver` から読む**ので `wait()` の Mutex を持たない（lock-free 設計） |

### 2.5 stub 生成 (`src/bin/stub_gen.rs`)

`pyo3-stub-gen` を呼び、自動生成可能な範囲（= top-level の sync な
pyfunction）の型スタブを `python/slurm_async_runner/_core/__init__.pyi`
に書き出します。`#[pymodule_export]` でぶら下げた pyclass や async
pyfunction はこのジェネレータの対象外なので、`manager.pyi` /
`runner.pyi` / `tssrun.pyi` は **手書き** です。

### 2.6 Python パッケージ (`python/slurm_async_runner/`)

| ファイル | 中身 |
|---|---|
| `__init__.py` | `from slurm_async_runner import _core` だけの 7 行。`__doc__` / `__all__` を継承 |
| `_core/__init__.pyi` | `pyo3-stub-gen` 自動生成（`sum_as_string` のみ） |
| `_core/manager.pyi` | 手書き。`SlurmCmd`, `SlurmManager` の型 |
| `_core/runner.pyi` | 手書き。`query_job_states_batch` の型 |
| `_core/tssrun.pyi` | 手書き。`Resource`/`TssrunCmd`/`LogSink`/`TssrunJobHandle`/`TssrunManager` |

### 2.7 テスト

| ファイル | 種別 | 動かすコマンド |
|---|---|---|
| `src/**/*.rs` の `#[cfg(test)] mod tests` | 単体テスト（Rust） | `cargo test --lib` |
| `tests/tssrun_integration.rs` | 統合テスト（Rust） | `cargo test --test tssrun_integration` |
| `python/tests/test_all.py` | 既存挙動 regression（pytest） | `uv run pytest python/tests/test_all.py` |
| `python/tests/test_tssrun.py` | tssrun サブシステムの async テスト | `uv run pytest python/tests/test_tssrun.py` |
| `python/tests/test_tssrun_live.py` | 実機ライブ（要 `RUN_LIVE_TSSRUN=1`） | `RUN_LIVE_TSSRUN=1 uv run pytest python/tests/test_tssrun_live.py` |
| `scripts/test_tssrun_live.py` | スタンドアロン実行版 | `uv run python scripts/test_tssrun_live.py` |

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
| 新しい SLURM 状態トークンに対応する | このリポジトリではなく [`gaussian_job_shared`](https://github.com/kkiyama117/gaussian_job_shared) 側で追加 |
| `tssrun` の `--rsc` キーを増やす | `src/tssrun/cmd.rs::Resource` のフィールドと `render` |
| `salloc:` バナーが site-specific に書き換わった | `src/tssrun/parse.rs` に新しい prefix を追加（既存をいじらず分岐推奨） |
| ログ出力先を増やす（DB / 外部 API） | `src/tssrun/log.rs` で `JobLogSink` を実装した型を追加 |
| Snapshot 永続化バックエンドを増やす（Redis / SQLite 等） | `src/tssrun/store.rs` で `#[async_trait] impl JobStateStore for X` を追加し、`TssrunManager::with_state_store(Arc::new(X))` で注入 |
| クロスプロセス attach を有効にしたい | `TssrunManager::new(cmd).with_state_dir(path)`（FS バックエンド）に切り替え。デフォルトの `InMemoryStateStore` ではプロセス間で共有できない |
| Python 側の async API を増やす | `src/py_export/<module>.rs` に `#[pyfunction]` または `#[pyclass]` を追加し、対応する `_core/*.pyi` を手書きで更新 |
| Python に新しい sync ヘルパーを追加 | `src/py_export/mod.rs` に `#[pyo3_stub_gen::derive::gen_stub_pyfunction]` を付けて足す。`cargo run --bin stub_gen` で `.pyi` 再生成 |
| 新しい dispatcher を実装 | `src/dispatcher.rs` に `impl JobDispatcher for X` を足し、必要なら `BackgroundDispatcher` も実装 |

## 4. 依存ライブラリの読み解き

| クレート | 主な用途 | 読むときの参照 |
|---|---|---|
| `tokio` | async ランタイム / `process::Command` / `sync::watch` / `sync::Mutex` | `dispatcher.rs`, `tssrun/handle.rs` |
| `pyo3` 0.28 | Python バインディング (abi3-py312, nightly feature) | `py_export/*` |
| `pyo3-async-runtimes` 0.28 | Tokio Future ↔ Python coroutine 変換 | `py_export/*` の `future_into_py` |
| `pyo3-stub-gen` | top-level pyfunction の `.pyi` 生成 | `bin/stub_gen.rs` |
| `pyo3-log` | Rust `log` crate を Python `logging` にブリッジ | `py_export/mod.rs` |
| `pythonize` | Rust ↔ Python 値変換 | （現状の使用頻度は低い） |
| `serde` / `serde_json` | `JobHandleSnapshot` のシリアライズ | `tssrun/handle.rs`, `tssrun/store.rs` |
| `tempfile` | atomic-rename による snapshot 書き込み | `tssrun/store.rs::write_atomic_json` |
| `uuid` (`v4` + `v7` features) | `JobHandleSnapshot` の primary key（時刻順 UUID v7） | `tssrun/handle.rs`, `tssrun/store.rs`, `tssrun/manager.rs` |
| `async-trait` | `JobStateStore` の `async fn` を `dyn Trait` で持つための desugaring | `tssrun/store.rs` |
| `anyhow` / `thiserror` | エラーハンドリング | crate 全体 |
| `tracing` / `log` | 構造化ログ | `tssrun/handle.rs` 中心 |
| `gaussian_job_shared` | `JobStatus` / `JobState` / `JobReason` の正本 | `lib.rs::pub use ...`, `runner.rs`, `py_export/runner.rs` |
