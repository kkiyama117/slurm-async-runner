# アーキテクチャ

`slurm-async-runner` は、SLURM ジョブの非同期投入とライフサイクル状態
取得を Rust で実装し、それを `pyo3` + `pyo3-async-runtimes` 経由で
Python の `await` 可能 API として公開する **Rust + Python ハイブリッド
パッケージ**です。

このドキュメントは、コミッターが「なぜこのレイヤー分割になっているか」
を 5 分で把握できるようにすることを目的にしています。
ファイル単位の対応は [`code-map.md`](./code-map.md)、
実行時のデータの流れは [`process-flow.md`](./process-flow.md) を参照。

## 1. 全体像

```
+------------------------------------------------------------------+
|                Python 利用者                                       |
|   slurm_async_runner._core.{manager, runner, tssrun}              |
+----------------------------+-------------------------------------+
                             | pyo3 (async = pyo3-async-runtimes)
+----------------------------v-------------------------------------+
|                pyo3 公開層 (src/py_export/*)                      |
|   PySlurmManager / PyTssrunManager / PyTssrunJobHandle ...        |
|   - Rust 型を Py* でラップ                                          |
|   - Tokio Future を Python coroutine に変換                        |
|   - JobStatus は gaussian_job_shared の Python 型へ橋渡し             |
+----------------------------+-------------------------------------+
                             | pure Rust API
+----------------------------v-------------------------------------+
|             コア Rust ライブラリ (src/*.rs, src/tssrun/*.rs)         |
|                                                                   |
|  +-----------+  +--------------+  +--------------------+          |
|  | Spec 層    |  | Runtime 層   |  | Query 層           |          |
|  | SlurmCmd  |->| JobDispatcher|->| runner::query_*    |          |
|  | TssrunCmd |  |(trait + impl)|  | squeue/sacct パース |          |
|  +-----------+  +--------------+  +--------------------+          |
|                                                                   |
|  +------------------ tssrun サブシステム ------------------+        |
|  | TssrunManager -> JobHandle (watch スナップショット)      |        |
|  |   - tee_stdout/stderr -> JobLogSink + salloc: パース     |        |
|  |   - wait -> finished の確定                              |        |
|  |   - JobStateStore (InMemory / FileSystem / 任意 backend)  |        |
|  |     primary key = UUID v7、{dir}/{uuid}.json を atomic 保存|       |
|  +---------------------------------------------------------+        |
+----------------------------+-------------------------------------+
                             | OS / SLURM
+----------------------------v-------------------------------------+
|         srun / tssrun(salloc+srun) / squeue / sacct                |
+------------------------------------------------------------------+
```

## 2. 設計の 2 軸分割

このリポジトリ全体を貫く設計判断は次の 2 つの直交分離です。
コードを読むときはこの軸を意識してください。

### 軸 1: Spec vs. Runtime — 「argv の組み立て」と「実行」を分離

| 層 | 型 | 役割 | I/O |
|---|---|---|---|
| Spec | `SlurmCmd`, `TssrunCmd`, `Resource` | 引数を typed に持ち、`build_argv()` だけを提供 | なし（純データ） |
| Runtime | `JobDispatcher` trait, `TokioDispatcher`, `DryRunDispatcher`, `BackgroundDispatcher`, `TokioBackgroundDispatcher` | `argv` を受け取りプロセスを起動 | tokio |

このため、

- **テスト時はモックディスパッチャを差し込める**（`run_job_with(&MockDispatcher, ...)`）。
- **Spec 型は言語非依存**で、同じ argv 規約を Python 側でも再利用しやすい。
- **dry-run モードがそのまま型ベースで表現できる**（`DryRunDispatcher`）。

### 軸 2: Sync (1ショット) vs. Background (常駐 + watch)

| カテゴリ | 代表 trait | 出力 | 用途 |
|---|---|---|---|
| 同期実行 | `JobDispatcher::run / capture` | `i32` または `(i32, String)` | `srun job.sh` の単発投入、`squeue`/`sacct` の単発呼び出し |
| 背景実行 | `BackgroundDispatcher::spawn` | `SpawnedChild { pid, child }` | `tssrun` を非ブロッキングで起動して `JobHandle` を返す |

`tssrun` を「子プロセスを起動した瞬間に Python に制御を返し、後から
スナップショットを覗ける」ようにするためにこの分離が必要でした。
詳細は §4。

## 3. クレートのレイヤーごとの詳細

### 3.1 Spec 層（純データ）

- `src/manager.rs::SlurmCmd` … `srun_cmd` だけを保持し
  `build_argv(&Path) -> Result<Vec<String>>` を提供。パスは
  `std::path::absolute` で絶対化（Python の `Path.absolute()` 互換）。
- `src/manager.rs::SlurmManager` … `SlurmCmd` を保持するだけ。
  `run_job` / `query_job_state` / `query_job_states_batch` の
  メソッドはあるが、内部では Runtime 層と Query 層を呼ぶだけ。
- `src/tssrun/cmd.rs::TssrunCmd` / `Resource` … `tssrun --rsc p=...:t=...` の
  引数を typed フィールドで持ち、`build_argv()` で argv 配列を生成。

> **重要**: Spec 型は I/O を一切やりません。プロセス起動・ファイル
> アクセス・SLURM 通信は必ず Runtime / Query / tssrun ハンドル側で
> 起こります。これを破ると軸 1 の旨味が消えます。

### 3.2 Runtime 層（プロセス実行抽象）

- `src/dispatcher.rs::JobDispatcher` … `Send + Sync` な trait。
  `run(argv) -> i32` と `capture(argv) -> (i32, String)` の 2 メソッド。
- `TokioDispatcher` … `tokio::process::Command` で実プロセスを起動。
  `run` 側は stdout/stderr を pipe + 親へ echo、`capture` 側は stdout
  だけを文字列に集約。
- `DryRunDispatcher` … プロセスを起動せず argv を `println!` するだけ。
  常に `Ok(0)` を返す。
- `BackgroundDispatcher` extends `JobDispatcher` … `spawn` を追加。
  非ブロッキングで `SpawnedChild { pid, child }` を返す。
- `TokioBackgroundDispatcher` … 上記の唯一のプロダクション実装。
  stdin=null, stdout/stderr=pipe, env / cwd を `Command` に伝播。

### 3.3 Query 層（squeue / sacct パース）

- `src/runner.rs::query_job_states_batch_with<D: JobDispatcher>` …
  `JobDispatcher` を受け取り、

  1. `squeue -h -j <ids> -o "%i %T %r"` を `capture`
  2. ヒットしなかった id だけを `sacct -P -n -j <ids> -o JobID,State,Reason` で再問合せ
  3. それでも見つからない id は `JobStatus::default()` で埋める
  4. 結果を `HashMap<u64, JobStatus>` で返す

- `query_job_states_batch(jobids)` は上記の `TokioDispatcher` 既定版。
- パースは `JobState::parse` / `JobReason::parse`（`gaussian_job_shared`
  クレート提供）に委譲し、未知トークンは `Unknown` / `Other(String)` に
  落とす forward-compat 設計。

### 3.4 tssrun サブシステム（`src/tssrun/`）

ECCS の `tssrun`（= `salloc` + `srun` 対話バッチフロントエンド）を
**非ブロッキング**に扱うためのサブシステム。重要なのは次の 5 概念。

| 型 | 役割 |
|---|---|
| `TssrunCmd` (cmd.rs) | argv の Spec ビルダー |
| `JobHandle` (handle.rs) | `BackgroundDispatcher::spawn` で得た子プロセスを所有し、tee タスク・wait タスク・スナップショット送信器を保持 |
| `JobHandleSnapshot` (handle.rs) | Serde 対応の状態。**primary key は `uuid: Uuid`（v7、時刻順）**。`pid` / `argv` / `sent_env` / `jobid` / `node` / `finished` などを含み、watch チャンネルで配信されストアに永続化される |
| `JobStateStore` trait (store.rs) | スナップショット永続化の抽象。組み込み実装は `InMemoryStateStore`（`HashMap<Uuid, _>`、デフォルト）と `FileSystemStateStore`（`{dir}/{uuid}.json`、atomic-rename、ディレクトリ遅延作成）。Redis / SQLite 等は外部 crate で `#[async_trait]` 実装するだけで差し込める |
| `TssrunManager` (manager.rs) | `TssrunCmd` + `Arc<dyn JobStateStore>` + `Arc<dyn JobLogSink>` を保持。`spawn` / `attach` / `query_state` を提供 |

サブシステム独自の設計判断:

1. **スナップショットは `tokio::sync::watch` で公開**。`JobHandle::watch()`
   が `Receiver` をクローンして返すので、Python 側が `pid` / `jobid` /
   `is_running` をポーリングしても **`wait()` がブロックしない**
   （ロックフリーな読み取り）。
2. **`wait()` は `&mut self` の唯一のメソッド**。`Option<JoinHandle>` を
   `.take()` するので 2 回目の `wait()` はエラー。「子の所有権 = wait
   する権利」を型で表現している。
3. **シグナル kill を `Ok(None)` で表現**。`status.code()` が `None` の
   場合（SLURM の time-limit kill、OOM など）は曖昧に 0 を返さず、
   `Result<Option<i32>>` の `Ok(None)` を返す。Python 側でも `int | None`。
4. **永続化は trait 越し**。`TssrunManager::new(cmd)` だけだと
   `InMemoryStateStore`（プロセス内 `HashMap`）が選ばれ、`save` は無謬。
   クロスプロセス attach が要るなら `with_state_dir(path)` で
   `FileSystemStateStore` に切り替えると `{path}/{uuid}.json` が
   `tempfile::NamedTempFile::persist` で atomic に書き換わる。
   別プロセスからは `AttachKey::{Uuid, Pid, JobId, File}` のいずれかで
   **read-only ハンドル**として再構成可能。
5. **primary key は UUID v7**。spawn のたびに `Uuid::now_v7()` を 1 回
   生成し、in-memory snapshot・on-disk filename・store エントリで同じ
   key を共有する（second source of truth を作らない）。`AttachKey::Uuid`
   は store.load() で O(1)、`Pid` / `JobId` は scan ベースのフォールバック。
   Pid はカーネルで再利用されうるため、長期参照には `Uuid` を使うこと。

### 3.5 ログシンク（`src/tssrun/log.rs`）

`JobLogSink` trait は **dyn 互換** にするため、戻り値を
`Pin<Box<dyn Future<...> + Send + 'a>>` で揃えています（RPIT は dyn
不可）。これにより `Arc<dyn JobLogSink>` を tee タスクで複数共有できる。
組み込み実装:

| 実装 | 用途 |
|---|---|
| `NullLogSink` | 行を捨てる。`salloc:` パースだけは tee タスクが行う |
| `StdLogSink` | 親プロセスの stdout/stderr に転送 |
| `InMemoryLogSink` | テストや診断向けに `Vec` に蓄積 |
| `FileLogSink` | 行ごとに 2 ファイル（stdout / stderr）に追記 |

## 4. なぜ Python ではなく Rust で書き直したか

歴史的経緯は `docs/superpowers/specs/2026-05-08-slurm-gaussian-migration-design.md`
に詳しく書かれていますが、要点は次のとおりです。

- **API 互換維持**。Python ユーザーは引き続き `await manager.run_job(...)`
  と書ける。`pyo3-async-runtimes::tokio::future_into_py` が Tokio の
  Future を Python の coroutine に変換する。
- **Rust 側からも単独で使えるライブラリにする**。`crate-type = ["cdylib", "rlib"]`
  になっているのはこのため。`pyo3` 依存は `feature = "pyo3"` でゲートされ、
  `--no-default-features` でビルドすれば pure Rust ライブラリになる。
- **`JobStatus` 型を `gaussian_job_shared` に集約**。SLURM の状態/理由
  enum は別クレートが正本で、ここでは re-export と Python 側ブリッジ
  だけを行う。`Cargo.toml` で `default-features = false` にしているのは、
  upstream の `pyo3` feature が `_core` シンボルを生成して衝突するのを
  防ぐため（`Cargo.toml:50-59` のコメント参照）。

## 5. pyo3 公開層の構造（`src/py_export/`）

Python の名前空間 `slurm_async_runner._core` 配下に 3 つのサブモジュールが
ぶら下がります。トップレベルは `src/py_export/mod.rs::slurm_async_runner`
の `#[pymodule]` で組み立てています。

| Python のパス | 実体（Rust） | 主なエクスポート |
|---|---|---|
| `slurm_async_runner._core` | `py_export/mod.rs` | `sum_as_string` (デモ) |
| `slurm_async_runner._core.manager` | `py_export/manager.rs::inner_module` | `SlurmCmd`, `SlurmManager` |
| `slurm_async_runner._core.runner` | `py_export/runner.rs::inner_module` | `query_job_states_batch` |
| `slurm_async_runner._core.tssrun` | `py_export/tssrun.rs::inner_module` | `Resource`, `TssrunCmd`, `LogSink`, `TssrunJobHandle`, `TssrunManager`, `null_log_sink`, `std_log_sink`, `file_log_sink` |

それぞれの `#[pymodule_init]` で `sys.modules` に明示登録しているのは、
サブモジュールの import が pyo3 規約上で `module-name` フルパスから
解決されるようにするため。

`JobStatus` を Python に渡すときは、毎回 `gaussian_job_shared._core.entities.slurm.status`
を `py.import` するとコストが重いので、`PyOnceLock<Py<PyAny>>` で
**プロセス内 1 回だけインポートしてキャッシュ**しています
（`py_export/runner.rs:19` の `JOB_STATUS_CLS` と同じパターンが
`py_export/manager.rs:161` にも存在）。

## 6. 不変条件と落とし穴

コミッターが踏みやすい地雷を列挙します。

- **Spec 型に I/O を入れない**。`SlurmCmd::build_argv` で stat / glob
  したくなるかもしれませんが、テスト独立性とランタイム選択の柔軟性が
  消えるので NG。
- **`JobHandle::wait()` 後にスナップショット getter を呼ぶのは OK**。
  ただし 2 回目の `wait()` はエラー。これを誤って書き直して曖昧な
  `Ok(0)` を返してしまわないように。
- **`TssrunManager` のフィールドは `pub(crate)`**。`with_state_dir` /
  `with_state_store` / `with_log_sink` ビルダー以外で書き換えると、
  既に走っている `JobHandle` には反映されません（後から store を
  切り替えても既存ハンドルは spawn 時にクローンした旧 `Arc<dyn
  JobStateStore>` に書き続ける）。
- **デフォルトはプロセス内 in-memory ストア**。`TssrunManager::new` だけ
  だと `InMemoryStateStore` が選ばれ、別プロセスから `attach` できません。
  クロスプロセス attach が必要なら `with_state_dir(path)` を必ず付ける
  こと。`FileSystemStateStore` のディレクトリは遅延作成 (`mkdir -p` on
  first save) なので、構築時点では存在しなくて構いません。
- **`find_by_jobid` はジョブ ID パース後でないと当たらない**。`salloc:`
  バナーを tee タスクが読んで snapshot に書き込み、store にも反映され
  たあと初めて検索ヒットします。spawn 直後に呼ぶと `Ok(None)` が正常。
- **`live_env()` が `None` を返すのは正常系**。非 Linux、子が既に終了、
  setuid バイナリで `PR_SET_DUMPABLE` がクリアされている等のケースを
  全部 `None` に丸めています（`src/tssrun/handle.rs::read_live_env_for_pid`）。
  `Err` で返すと ECCS の `tssrun`（setuid）で必ず失敗するため、この丸めが
  仕様です。
- **`gaussian_job_shared` の `pyo3` feature を絶対に有効化しない**。
  両クレートがそれぞれ `_core` という Python モジュール名を持っているため、
  シンボル衝突して `PyInit__core` が duplicate symbol になります。
  `Cargo.toml:50-59` のコメントが warning として残っています。
