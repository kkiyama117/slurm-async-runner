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
|   slurm_async_runner._slurm_async_runner_core.                    |
|     {manager, runner, tssrun, sbatch, entities.slurm.*}           |
|   slurm_async_runner.JobHandleCommon (Protocol, PR #7)             |
+----------------------------+-------------------------------------+
                             | pyo3 (async = pyo3-async-runtimes)
+----------------------------v-------------------------------------+
|                pyo3 公開層 (src/py_export/*)                      |
|   PySlurmManager / PyTssrunManager / PyTssrunJobHandle             |
|   PySbatchManager / PySbatchJobHandle / PyFinishedInfo            |
|   - Rust 型を Py* でラップ                                          |
|   - Tokio Future を Python coroutine に変換                        |
|   - 5 つの共通 sync getter (uuid / jobid / is_running /            |
|     is_finished / exit_code) は両 backend で同一シグネチャ          |
+----------------------------+-------------------------------------+
                             | pure Rust API
+----------------------------v-------------------------------------+
|             コア Rust ライブラリ (src/*.rs, src/tssrun/*.rs, src/sbatch/*.rs)|
|                                                                   |
|  +-----------+  +--------------+  +--------------------+          |
|  | Spec 層    |  | Runtime 層   |  | Query 層           |          |
|  | SlurmCmd  |->| JobDispatcher|->| runner::query_*    |          |
|  | TssrunCmd |  |(trait + impl)|  | squeue/sacct パース |          |
|  | SbatchCmd |  | Background系  |  | qgroup -l パース    |          |
|  +-----------+  +--------------+  +--------------------+          |
|                                                                   |
|  +-- 跨 backend 抽象 (PR #7, src/handle.rs) -----+                  |
|  | JobHandleCommon trait (associated Snapshot 型)                  |
|  | DynJobHandleCommon + into_dyn() (object-safe facade)            |
|  +----------------------------------------------+                  |
|                                                                   |
|  +------------------ tssrun サブシステム ------------------+        |
|  | TssrunManager -> TssrunJobHandle (watch スナップショット)|        |
|  |   - tee_stdout/stderr -> JobLogSink + salloc: パース     |        |
|  |   - wait -> finished の確定                              |        |
|  |   - JobStateStore (InMemory / FileSystem / 任意 backend)  |        |
|  |     primary key = UUID v7、{dir}/{uuid}.json を atomic 保存|       |
|  +---------------------------------------------------------+        |
|                                                                   |
|  +------------------ sbatch サブシステム (PR #6) ----------+        |
|  | SbatchManager -> SbatchJobHandle (watch スナップショット)|        |
|  |   - sbatch でキュー投入、子プロセスは持たない              |        |
|  |   - 単発 refresh()  = qgroup -l → squeue (sacct なし)    |        |
|  |   - array task refresh() = squeue -j <master>_<idx>     |        |
|  |     (PR #12、qgroup は per-task では集計しか返さないため) |        |
|  |   - refresh_with_sacct() / run() のみ sacct 経由         |        |
|  |   - SlurmDependency / SlurmSignalSpec / MailTypeInput /  |        |
|  |     SlurmArraySpec などの typed --flag entities         |        |
|  |   - 同じ JobStateStore + UUID v7 を共有 (kind="sbatch")  |        |
|  +---------------------------------------------------------+        |
+----------------------------+-------------------------------------+
                             | OS / SLURM
+----------------------------v-------------------------------------+
|  srun / tssrun(salloc+srun) / sbatch / scancel / squeue /          |
|  qgroup / sacct                                                    |
+------------------------------------------------------------------+
```

## 2. 設計の 2 軸分割

このリポジトリ全体を貫く設計判断は次の 2 つの直交分離です。
コードを読むときはこの軸を意識してください。

### 軸 1: Spec vs. Runtime — 「argv の組み立て」と「実行」を分離

| 層 | 型 | 役割 | I/O |
|---|---|---|---|
| Spec | `SlurmCmd`, `TssrunCmd`, `ResourceSpec`, `JobTimeLimit`, `JobPartition`, `Memory` | 引数を typed に持ち、`build_argv()` だけを提供 | なし（純データ） |
| Runtime | `JobDispatcher` trait, `TokioDispatcher`, `DryRunDispatcher`, `BackgroundDispatcher`, `TokioBackgroundDispatcher` | `argv` を受け取りプロセスを起動 | tokio |

このため、

- **テスト時はモックディスパッチャを差し込める**（`run_job_with(&MockDispatcher, ...)`）。
- **Spec 型は言語非依存**で、同じ argv 規約を Python 側でも再利用しやすい。
- **dry-run モードがそのまま型ベースで表現できる**（`DryRunDispatcher`）。

### 軸 2: Sync (1ショット) vs. Background (常駐 + watch)

| カテゴリ | 代表 trait | 出力 | 用途 |
|---|---|---|---|
| 同期実行 | `JobDispatcher::run / capture` | `i32` または `(i32, String)` | `srun job.sh` の単発投入、`sbatch` の投入、`squeue`/`sacct`/`qgroup`/`scancel` の単発呼び出し |
| 背景実行 | `BackgroundDispatcher::spawn` | `SpawnedChild { pid, child }` | `tssrun` を非ブロッキングで起動して `TssrunJobHandle` を返す |

sbatch サブシステム（§3.5）は **子プロセスを持たない**ため
`BackgroundDispatcher` を使わず、`Arc<dyn DynJobDispatcher>` で
`capture()` を回して `qgroup` / `squeue` の出力をポーリングする。

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
- `src/tssrun/cmd.rs::TssrunCmd` … `tssrun -p ... -t ... --rsc p=...:t=...` の
  引数を typed フィールドで持ち、`build_argv()` で argv 配列を生成。
  リソース仕様は `crate::entities::slurm::ResourceSpec`（CPU / GPU enum、
  KUDPC マニュアル準拠の partial CPU 許容）、時間制限は `JobTimeLimit`、
  パーティション名は `JobPartition`、メモリは `Memory` を使う。
  PR #5 までは `tssrun::cmd::Resource` というローカル型を使っていたが、
  `gaussian_job_shared` の vocab を in-tree に取り込んだタイミングで
  `ResourceSpec` 1 つに統合された。
- `src/sbatch/cmd.rs::SbatchCmd` … `sbatch` 用 argv ビルダー（PR #6）。
  `--dependency` / `--mail-user` / `--mail-type` / `--no-requeue` /
  `--signal` / `--comment` / `--array` / `--export` などを typed
  フィールドで保持。値型はすべて `crate::entities::slurm::sbatch_options::*`
  （`SlurmDependency`, `MailTypeInput`, `SlurmSignalSpec`, `SlurmArraySpec`
  ほか）に寄せてあり、untyped 経路を作らない。

> **重要**: Spec 型は I/O を一切やりません。プロセス起動・ファイル
> アクセス・SLURM 通信は必ず Runtime / Query / tssrun ハンドル側で
> 起こります。これを破ると軸 1 の旨味が消えます。

### 3.2 Runtime 層（プロセス実行抽象）

- `src/dispatcher.rs::JobDispatcher` … `Send + Sync` な trait。
  `run(argv) -> i32` と `capture(argv) -> (i32, String)` の 2 メソッド。
- `TokioDispatcher` … `tokio::process::Command` で実プロセスを起動。
  `run` 側は stdout/stderr を pipe + 親へ echo、`capture` 側は stdout
  に加えて **stderr も `[stderr]` マーカー区切りで末尾結合**する
  （sbatch 失敗時の `sbatch: error: ...` 等の診断メッセージを
  `SbatchSpawnError::SubmitFailed::output` まで届けるため。`stderr` が
  空のときは何も付けないので line-based パーサ群 (`parse_qgroup_l` /
  `parse_squeue` / `parse_sacct_*` / `parse_submitted_jobid`) は
  非破壊）。
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
- パースは `JobState::parse` / `JobReason::parse`
  （`crate::entities::slurm::status` 提供 — PR #5 で `gaussian_job_shared`
  から in-tree 移管）に委譲し、未知トークンは `Unknown` / `Other(String)`
  に落とす forward-compat 設計。

### 3.4 tssrun サブシステム（`src/tssrun/`）

ECCS の `tssrun`（= `salloc` + `srun` 対話バッチフロントエンド）を
**非ブロッキング**に扱うためのサブシステム。重要なのは次の 5 概念。

| 型 | 役割 |
|---|---|
| `TssrunCmd` (cmd.rs) | argv の Spec ビルダー |
| `TssrunJobHandle` (handle.rs) | `BackgroundDispatcher::spawn` で得た子プロセスを所有し、tee タスク・wait タスク・スナップショット送信器を保持。PR #7 で `JobHandle` から rename (旧 alias は PR #11 で削除) |
| `TssrunJobSnapshot` (handle.rs) | Serde 対応の状態。**primary key は `uuid: Uuid`（v7、時刻順）**。`pid` / `argv` / `sent_env` / `jobid` / `node` / `finished` などを含み、watch チャンネルで配信されストアに永続化される。PR #7 で `JobHandleSnapshot` から rename (旧 alias は PR #11 で削除) |
| `JobStateStore<S>` trait (`crate::store`) | スナップショット永続化の抽象（汎用化されており tssrun / sbatch 両方が利用）。組み込み実装は `InMemoryStateStore<S>`（`HashMap<Uuid, _>`、デフォルト）と `FileSystemStateStore<S>`（`{dir}/{uuid}.json`、atomic-rename、ディレクトリ遅延作成、`kind` discriminator により他 backend snapshot は silent-skip）。Redis / SQLite 等は外部 crate で `#[async_trait]` 実装するだけで差し込める |
| `TssrunManager` (manager.rs) | `TssrunCmd` + `Arc<dyn JobStateStore<TssrunJobSnapshot>>` + `Arc<dyn JobLogSink>` を保持。`spawn` / `attach` / `query_state` を提供 |

サブシステム独自の設計判断:

1. **スナップショットは `tokio::sync::watch` で公開**。`TssrunJobHandle::watch()`
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
6. **`refresh()` / `wait_terminal()` の追加 (PR #7)**。`refresh()` は
   永続化スナップショットを読み直して broadcast し、`Result<TssrunJobSnapshot>`
   を返す（旧 `Result<()>` シグネチャと non-breaking 互換）。
   `wait_terminal(poll_interval)` は `refresh()` を繰り返して
   `is_finished()` が立つまで待つ。`sbatch` 側の同名メソッドと parity を
   揃えてあり、`JobHandleCommon` trait (PR #7、§3.6) が両 backend を
   束ねる前提として導入された。

### 3.5 sbatch サブシステム（`src/sbatch/`、PR #6）

KUDPC の `sbatch`（=キュー投入型バッチ）に対応するサブシステム。tssrun と
違い **子プロセスを持たない** — ジョブは SLURM 側で動き、handle は
`qgroup -l` / `squeue` の出力でステートを観測する。

| 型 | 役割 |
|---|---|
| `SbatchCmd` (`src/sbatch/cmd.rs`) | `sbatch` argv の Spec ビルダー。`--dependency` / `--mail-user` / `--mail-type` / `--no-requeue` / `--signal` / `--comment` / `--array` / `--export` などを typed フィールドで保持し、`build_argv()` を提供 |
| `SbatchJobSnapshot` (`src/sbatch/handle.rs`) | Serde 対応の状態。`uuid`（UUID v7）/ `jobid` / `last_observed_state` / `array_task_id` / `finished` (`FinishedInfo`) を保持。`kind = "sbatch"` で tssrun と区別される。PR #10 (#8 B1) で `array_jobid` フィールドは削除（旧 JSON は `deny_unknown_fields` 未指定により silently 無視されて読み込める）|
| `SbatchJobHandle` (`src/sbatch/handle.rs`) | 子プロセスを持たない handle。`refresh()` で `qgroup -l → squeue` チェーン、`refresh_with_sacct()` で sacct を呼んで `exit_code` を確定、`wait_terminal()` で polling、`log_lines` / `read_log_to_end` で stdout/stderr のテール読み。`SbatchJobHandleInner` には PR #9 (#8 A7) で `Drop` 実装が追加され、終端到達前に最後の clone が drop された場合は `tracing::warn!` を発火する |
| `SbatchManager` (`src/sbatch/manager.rs`) | `Arc<dyn DynJobDispatcher>` + `JobStateStore<SbatchJobSnapshot>` を保持。`spawn` / `spawn_array` / `run` / `cancel` / `attach_uuid` / `attach_jobid` / `attach_array_jobid` / `attach_file` を提供 |
| `SbatchSpawnError` / `SbatchRunError` / `SbatchCancelError` / `SbatchAttachError` (`src/sbatch/error.rs`) | `#[non_exhaustive]` typed enum。`anyhow::Error` への型崩しを避ける（PR #6 final review HIGH issue から） |

サブシステム独自の設計判断:

1. **sacct は限定的に呼ぶ**。`refresh()` の hot path では sacct を呼ばず、
   `qgroup -l` がキューから消えたジョブに対しては `left_active_listing`
   flag を立てるだけ。終端 exit_code が必要なときだけ `refresh_with_sacct()` を
   明示的に呼ぶ。`run()` だけが内部で `spawn → wait_terminal → refresh_with_sacct`
   を合成する。これは sbatch サブシステム導入時からの不変条件として固定。
   PR #12 (#8 A5) で array-task の per-task `refresh` を追加: `array_task_id.is_some()`
   の handle は `qgroup -l`（master 集計しか返さない）を skip して
   `squeue -j <master>_<idx>` (`query_array_task_state_with`) を直叩きする。
   `refresh_with_sacct()` の array-task branch も同様で `sacct -j <master>_<idx>`
   (`query_array_task_outcome_with`) を使う。
   **KUDPC `qgroup -l` 互換 (PR #14)**: 詳細行は `QUEUE USER JOBID | STAT
   SUBMIT_AT | RSC:core | PROC CORE MEM ELAPSE` のパイプ区切りレイアウト。
   `parse_qgroup_l` は `|` トークンを skip し、`jobid_str.parse::<u64>() == 0`
   は per-queue/per-user サマリ行として除外する。STAT トークンの `FINI` /
   `FAIL` は KUDPC 独自エイリアスで、それぞれ `JobState::Completed` /
   `JobState::Failed` (どちらも `is_terminal() == true`) に **input-only
   alias** としてマップ — `as_token()` は SLURM 正規の `COMPLETED` /
   `FAILED` を返すので永続化される文字列は SLURM 語彙のまま。
   **`refresh_with_sacct` の起動条件 (PR #14)**: `lifecycle.finished` が
   None の状態で「`left_active_listing` が立った」**または**
   「`last_observed_state.is_terminal()`」のどちらかが真なら sacct を
   呼ぶ。KUDPC では FINI/FAIL を観測しても qgroup から消えるまでに数十
   秒のラグがあるため、`left_active_listing` だけを起動条件にすると
   `wait_terminal` 直後の `refresh_with_sacct()` が sacct を呼ばずに
   早期 return してしまっていた。
   **default polling cadence (PR #14)**: `SbatchManager::new` の
   `poll_interval` 既定値は **60 秒**。SLURM の task-sampling 既定が 30 秒
   なので、2 連続 poll が同一サンプリング窓に閉じ込められないよう 60 秒
   以上にしている。`with_poll_interval(Duration)` で override 可能、
   テストは 1〜10 ms。
2. **typed `--flag` entities を強制**。すべての SLURM `--*` 値型は
   `crate::entities::slurm::*` で定義し、Spec 層 (`SbatchCmd`) は型 leaf を
   持つ。`MailTypeInput::parse("BEGIN,END")` のように `FromStr` で
   ground-truth 検証する。raw string の untyped 経路は禁止。
3. **配列ジョブは 1 spawn = N snapshot**。`spawn_array` は `sbatch --array=<spec>`
   を 1 回叩き、master jobid から `expand_array_indices(&SlurmArraySpec)` で
   各 task に 1 つずつ `SbatchJobHandle` (= 別 UUID) を発行する。`attach_array_jobid`
   は master jobid から N 個の handle を `array_task_id` 昇順で再構成する。
   各 task の状態は §3.5 設計判断 #1 で述べた per-task `refresh()` 経路
   (`<master>_<idx>` 直叩き、PR #12) で個別に追跡できる。`attach_array_jobid`
   は `find_all_by_jobid -> list -> decode_with_kind_check` を経由するため、
   同じ state dir に tssrun の snapshot が同居していても kind 不一致で
   silent-skip される (PR #12 でこの contract を test で固定)。
4. **`run()` は `sbatch --wait` を使わない**。KUDPC で disconnect すると
   ジョブが orphan 化するため、poll ベースで `spawn → wait_terminal → refresh_with_sacct`
   を合成する。`ArrayNotSupported` を typed error で早期 reject。
5. **on-disk フォーマットを共有**。`{root}/<uuid>.json` の場所と naming は
   tssrun と同じ。先頭の `"kind": "sbatch"` で peek し、`FileSystemStateStore<S>::list`
   は不一致 kind を silent-skip する設計のため、tssrun と sbatch は安全に
   同居できる。
6. **watch チャンネル更新は `send_replace` を使う (PR #14)**。
   `SbatchJobHandle::new` / `TssrunJobHandle::new` は
   `let (tx, _rx) = watch::channel(...)` で初期 receiver を drop するため、
   呼び出し側が `.watch()` を叩かない限り receiver count は 0。
   `tokio::sync::watch::Sender::send` は receiver 0 のとき **値を更新せず
   `Err(SendError)` を返す**仕様で、`let _ = ` で握り潰すと snapshot が
   spawn 時の初期値で凍結する。`is_finished()` / `exit_code()` が常に
   spawn 時 default を返す live バグの原因だった。`send_replace` は
   receiver の有無に関わらず値を unconditional に置換するので、`refresh`
   / `refresh_with_sacct` 内部の 6 つの送信点はすべてこちらを使う。
   Python 側のラッパは `handle.is_finished()` 等を `snapshot_tx.borrow()`
   経由で読むため、この置換なしには Rust 側の更新が一切伝わらない。

### 3.6 跨 backend handle 抽象（`src/handle.rs`、PR #7）

PR #6 で sbatch / tssrun の handle が「コア 5 sync getter (`uuid` /
`jobid` / `is_running` / `is_finished` / `exit_code`)」を持ち、`watch()` /
`snapshot()` / `refresh()` / `wait_terminal()` のシグネチャも揃った時点で、
これを **trait として機械的に保証する** のが PR #7 の主眼。

| 型 / trait | 役割 |
|---|---|
| `JobHandleCommon` trait | 5 sync getter + `snapshot()` / `watch()` + `async fn refresh() / wait_terminal()` を associated `type Snapshot: JobSnapshot` 経由で表現。`SbatchJobHandle` / `TssrunJobHandle` の両方が impl |
| `DynJobHandleCommon` trait | associated type を `serde_json::Value` に flatten した object-safe 版。公開メソッドは 5 sync getter + `kind() -> &'static str` + `snapshot_json()` + `async refresh_json() -> Result<serde_json::Value>` のみ。`watch::Receiver<S>` と `wait_terminal()` は dyn 経路に乗らないため **本体 trait からのみアクセス可能**（dashboard 用途では `refresh_json()` を手動 polling）。`Vec<Arc<dyn DynJobHandleCommon>>` で sbatch + tssrun handle を混ぜて持てる |
| `DynHandleAdapter<H>` + `into_dyn(handle) -> Arc<dyn DynJobHandleCommon>` | 明示的な type-erasure コンストラクタ。**blanket impl を提供しない** ことで過去に発生した E0034 ambiguity を回避（`DynJobDispatcher` の `into_dyn` パターンと同じ） |

設計判断:

1. **本体 trait は dyn-safe ではない**。associated `Snapshot` 型を保持する
   ことで sbatch / tssrun それぞれ自分の concrete snapshot 型を返せる
   （boxing / JSON 化なし）。Box が必要なときだけ `DynJobHandleCommon` を使う。
2. **`refresh()` は sacct を呼ばない**。`SbatchJobHandle::refresh` の不変条件を
   trait レベルでも継承。sacct は `refresh_with_sacct` / `run()` 専用。
3. **Python では `runtime_checkable Protocol` だけ提供**。
   `slurm_async_runner.JobHandleCommon` を Python に新規 pyclass として
   公開する案 (PR #7 で議論) は YAGNI として見送り。両 backend は既に
   構造的に Protocol を満たすので `isinstance(h, JobHandleCommon)` で
   structural type check できる。
4. **Python side の sync 化** (`JobHandleCommon` trait 導入時)。
   `TssrunJobHandle.uuid` / `jobid` / `is_running()` / `is_finished()` /
   `exit_code()` は元々 tokio runtime work を持たない読み取りだったが、
   `future_into_py` でラップして awaitable として露出していた。これを
   **sync `@property` / 普通の sync method に直し**、`SbatchJobHandle` と
   call shape を揃えた。旧 await-style の `*_async` エスケープハッチは
   PR #6 review fix で削除済み (sync 版で十分なため drop)。

### 3.7 ログシンク（`src/tssrun/log.rs`）

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
- **SLURM 語彙を in-tree で集約**（PR #5、Pyclass Single Owner ルール）。
  `JobStatus` / `JobState` / `JobReason` / `ResourceSpec` / `JobTimeLimit`
  / `JobPartition` / `Memory` などの SLURM enum/値型は **このクレートが
  正本**。Python の pyclass はこの cdylib にちょうど 1 つだけぶら下がり、
  下流クレートは `default-features = false`（必要なら `features =
  ["pyo3-types"]`）で **Rust 型のみ** を取り込む。同じ pyclass 実装が
  複数 cdylib に重複コンパイルされると、`__module__` が同一でも
  `id(cls)` が異なる別 Python 型になり `isinstance` が壊れる — それを
  避けるためのアーキテクチャルールが Cargo.toml の `pyo3` feature
  コメント（`Cargo.toml:99-112`）に明記されています。

## 5. pyo3 公開層の構造（`src/py_export/`）

Python の名前空間 `slurm_async_runner._slurm_async_runner_core` 配下に
複数のサブモジュールがぶら下がります。トップレベルは
`src/py_export/mod.rs::slurm_async_runner` の `#[pymodule]` で組み立てて
います。pymodule 名は `_core` ではなく
`_slurm_async_runner_core` です（PR #5 で `gaussian_job_shared` 側との
`PyInit__core` シンボル衝突を避けるために rename されました）。

| Python のパス | 実体（Rust） | 主なエクスポート |
|---|---|---|
| `slurm_async_runner._slurm_async_runner_core` | `py_export/mod.rs` | `sum_as_string` (デモ) |
| `slurm_async_runner._slurm_async_runner_core.manager` | `py_export/manager.rs::inner_module` | `SlurmCmd`, `SlurmManager` |
| `slurm_async_runner._slurm_async_runner_core.runner` | `py_export/runner.rs::inner_module` | `query_job_states_batch` |
| `slurm_async_runner._slurm_async_runner_core.tssrun` | `py_export/tssrun.rs::inner_module` | `TssrunCmd`, `LogSink`, `TssrunJobHandle`, `TssrunManager`, `JobStateStore`, `ResourceSpec`, `JobTimeLimit`（再エクスポート）, `null_log_sink`, `std_log_sink`, `file_log_sink`, `in_memory_state_store`, `file_system_state_store` |
| `slurm_async_runner._slurm_async_runner_core.sbatch` | `py_export/sbatch.rs::inner_module` | `SbatchCmd`, `SbatchManager`, `SbatchJobHandle`, `FinishedInfo`（PR #6） |
| `slurm_async_runner._slurm_async_runner_core.entities.slurm.status` | `py_export/entities/slurm/status.rs` | `JobStatus`, `JobState`, `JobReason` |
| `slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options` | `py_export/entities/slurm/sbatch_options.rs` | `ResourceSpec`, `ResourceSpecCPU`, `ResourceSpecGPU`, `JobTimeLimit`, `JobPartition`, `Memory`, `MemoryUnit`, `ArraySpec`, `SlurmDependency`, `MailTypeInput`, `SlurmSignalSpec` ほか |
| `slurm_async_runner` (Python facade) | `python/slurm_async_runner/__init__.py` | `JobHandleCommon`（PR #7 の `runtime_checkable Protocol`） |

それぞれの `#[pymodule_init]` で `sys.modules` に明示登録しているのは、
サブモジュールの import が pyo3 規約上で `module-name` フルパスから
解決されるようにするため。

`JobStatus` を Python に渡すときは、毎回
`slurm_async_runner._slurm_async_runner_core.entities.slurm.status`
を `py.import` するとコストが重いので、`PyOnceLock<Py<PyAny>>` で
**プロセス内 1 回だけインポートしてキャッシュ**しています
（`py_export/runner.rs:17` の `UPSTREAM_STATUS_MODULE` /
`JOB_STATUS_CLS` と同じパターンが `py_export/manager.rs:159` にも存在）。
PR #5 までは `gaussian_job_shared._core.entities.slurm.status` から
取り込んでいましたが、Pyclass Single Owner ルールで in-tree に移管
されたため、SAR は自前のモジュールパスを参照します。

## 6. 不変条件と落とし穴

コミッターが踏みやすい地雷を列挙します。

- **Spec 型に I/O を入れない**。`SlurmCmd::build_argv` で stat / glob
  したくなるかもしれませんが、テスト独立性とランタイム選択の柔軟性が
  消えるので NG。
- **`TssrunJobHandle::wait()` 後にスナップショット getter を呼ぶのは OK**。
  ただし 2 回目の `wait()` はエラー。これを誤って書き直して曖昧な
  `Ok(0)` を返してしまわないように。
- **`JobHandleCommon::refresh()` から `sacct` を呼んではいけない**
  （§3.5 + §3.6）。`sacct` は重いので
  `SbatchJobHandle::refresh_with_sacct` と `SbatchManager::run` の
  内部からしか呼ばれない。trait に新しい backend を加えるときも
  この不変条件を継承すること。
- **`JobSnapshot::kind()` の戻り値を rename しない**。`"sbatch"` /
  `"tssrun"` は `{root}/<uuid>.json` の `kind` フィールドに書かれて
  永続化されているため、変更すると既存ユーザの state ディレクトリを
  silent break する。PR #7 で Rust struct を rename したときも
  `TssrunJobSnapshot::kind()` は `"tssrun"` を返し続けている。
- **`JobHandleCommon` 本体 trait を dyn 化しようとしない**。
  associated `Snapshot` 型がある時点で dyn-safe ではない。
  `Arc<dyn DynJobHandleCommon>` が必要なときは `crate::handle::into_dyn`
  を経由すること（blanket impl は意図的に提供していない、§3.6 参照）。
- **watch 更新は `send_replace` 一択**。`SbatchJobHandle` /
  `TssrunJobHandle` の `snapshot_tx` は初期 receiver が drop されるため、
  `send` は receiver 0 のとき値を更新せず Err を返す。新規 refresh
  経路を増やすときは必ず `send_replace(snap)` を呼ぶこと
  （§3.5 設計判断 #6）。`let _ = snapshot_tx.send(...)` を書くと
  Python 側の `is_finished()` / `exit_code()` がサイレントに spawn 時の
  default を返し続ける live バグになる。
- **`TssrunManager` のフィールドは `pub(crate)`**。`with_state_dir` /
  `with_state_store` / `with_log_sink` ビルダー以外で書き換えると、
  既に走っている `TssrunJobHandle` には反映されません（後から store を
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
- **このクレートの `pyo3` feature を下流クレートで有効化しない**
  （Pyclass Single Owner ルール）。SAR の cdylib が
  `PyInit__slurm_async_runner_core` を発行する側の唯一の owner です。
  下流クレートが SAR の pyclass を使うなら `default-features = false`
  もしくは `features = ["pyo3-types"]`（pyclass 実装は持つが pymodule
  entry は出さない）に留めること。両側で feature を有効化すると
  `PyInit__slurm_async_runner_core` が duplicate symbol になりますし、
  そもそも実装が複製されると `isinstance` が壊れます。詳細は
  `Cargo.toml:99-112` の `[features]` コメント、および
  `docs/superpowers/specs/2026-05-10-slurm-vocab-migration-and-pyclass-ownership-design.md`
  §2 を参照。`gaussian_job_shared` 側にも同じ ownership ルールが適用
  されています。
