# プロセスフロー

公開 API を Python から呼び出した瞬間に、Rust 側でどんな順序で
何が起きるかを追跡するためのドキュメントです。
レイヤー構造を先に把握してから読んだ方が分かりやすいので、
未読なら [`architecture.md`](./architecture.md) を先に。

## 1. `SlurmManager.run_job` — バッチスクリプトの非同期投入

### Python 側のコード例

```python
manager = SlurmManager()
exit_code = await manager.run_job("./job.sh", dry_run=False)
```

### 処理フロー

```
[Python]                       [pyo3 公開層]                  [Rust コア]
   |                                |                              |
   | await m.run_job(path, dry_run) |                              |
   |------------------------------->|                              |
   |                                | PySlurmManager::run_job      |
   |                                |   inner = self.0.clone()     |
   |                                |   future_into_py(py, async{  |
   |                                |     inner.run_job(...).await |
   |                                |   })                         |
   |                                |----------------------------->|
   |                                |                              | SlurmManager::run_job
   |                                |                              |   match dry_run {
   |                                |                              |     true  => DryRunDispatcher
   |                                |                              |     false => TokioDispatcher
   |                                |                              |   }
   |                                |                              |   self.run_job_with(disp, &path)
   |                                |                              |     v
   |                                |                              |   build_argv(path)
   |                                |                              |     = [srun_cmd, abs_path]
   |                                |                              |     v
   |                                |                              |   dispatcher.run(&argv).await
   |                                |                              |     = tokio::process::Command
   |                                |                              |       .args(...).output().await
   |                                |                              |     stdout/stderr を pipe
   |                                |                              |     親に echo
   |                                |                              |     i32 を返す
   |                                |<-----------------------------|
   |                                | Result<i32> を Python へ返却  |
   |<-------------------------------|                              |
   | exit_code (int)                |                              |
```

### キーになる関数

- `src/py_export/manager.rs::PySlurmManager::run_job` … `Arc::clone` で
  内側の `SlurmManager` を共有してから `future_into_py` に async ブロックを
  渡す。これで Python coroutine を返す。
- `src/manager.rs::SlurmManager::run_job` … `dry_run` で 2 つの内蔵
  ディスパッチャを使い分け。テストで自前 dispatcher を差し込みたい場合は
  `run_job_with(&MyDispatcher, path)` を直接呼ぶ。
- `src/dispatcher.rs::TokioDispatcher::run` … `output().await` で完了を
  待ち、stdout/stderr を親に転送、`status.code().unwrap_or(0)` を返す。

## 2. `query_job_states_batch` — squeue → sacct フォールバックの非同期問合せ

### Python 側のコード例

```python
from slurm_async_runner._slurm_async_runner_core.runner import query_job_states_batch
states: dict[int, JobStatus] = await query_job_states_batch([12345, 12346, 12347])
```

### 処理フロー

```
入力: jobids = [12345, 12346, 12347]
   |
   v
runner::query_job_states_batch_with(dispatcher, jobids)
   |
   |-- jobids が空 -> return {} (短絡)
   |
   |-- dedupe_preserving_order
   |     [12345, 12346, 12347]
   |
   |-- csv_join -> "12345,12346,12347"
   |
   |-- dispatcher.capture(["squeue", "-h", "-j", csv, "-o", "%i %T %r"])
   |     v stdout 例
   |     "12345 PENDING Priority\n12346 RUNNING None\n"
   |
   |-- parse_squeue -> { 12345: PENDING/Priority, 12346: RUNNING/None }
   |
   |-- missing = [12347]   <- squeue に出てこなかった id だけ
   |
   |-- if missing.is_empty() -> history = {}
   |   else -> dispatcher.capture(["sacct", "-P", "-n", "-j", "12347",
   |                               "-o", "JobID,State,Reason"])
   |     v stdout 例
   |     "12347|COMPLETED|None\n12347.batch|COMPLETED|None\n"
   |
   |-- parse_sacct
   |     - "12347.batch" のような step 行は jid に '.' を含むので skip
   |     - { 12347: COMPLETED/None }
   |
   |-- merge_results(jobids, active, history)
   |     - active を優先、なければ history、それでもなければ
   |       JobStatus::default() (state=Unknown, reason=None)
   |
   v
出力: { 12345: PENDING/Priority, 12346: RUNNING/None, 12347: COMPLETED/None }
```

### Python 型変換の流れ

`HashMap<u64, JobStatus>` (Rust) → `dict[int, JobStatus]` (Python) の橋渡し
は `src/py_export/runner.rs::query_job_states_batch` で行われる:

1. `pyo3_async_runtimes::tokio::future_into_py` で Python coroutine を作る
2. await 後の Rust `HashMap` を `Python::attach` 内で
   `PyDict::new(py)` に展開
3. 各エントリで
   `slurm_async_runner._slurm_async_runner_core.entities.slurm.status.JobStatus`
   クラスを `PyOnceLock` キャッシュから取り出し、
   `JobStatus.parse(state_token, reason_str)` を呼ぶ
   （PR #5 で `gaussian_job_shared` から in-tree に移管された）
4. 結果の Python `JobStatus` を dict にセット

> **重要**: Rust 側の `JobStatus` 構造体と Python 側の `JobStatus` クラスは
> 別物です。境界では必ず `state.as_token()` / `reason.as_str()` の文字列に
> 落として `JobStatus.parse` で再構築します。これによりローカル enum 定義の
> 二重メンテを避けています。

## 3. tssrun spawn — 非ブロッキング起動 + 並行ポーリング

### Python 側のコード例

```python
from slurm_async_runner._slurm_async_runner_core.tssrun import (
    TssrunManager, file_system_state_store, file_log_sink,
)

manager = TssrunManager(
    cmd,
    store=file_system_state_store("/var/lib/slurm-runner"),
    log_sink=await file_log_sink("/tmp/o.log", "/tmp/e.log"),
)
handle = await manager.spawn()
print("uuid", await handle.uuid)           # <- primary key（UUID v7 文字列）
print("pid", await handle.pid)             # <- wait と並行して読める
print("jobid", await handle.jobid)         # <- wait と並行して読める
code = await handle.wait()                 # <- 子の終了を待つ
```

> `store=` を省略するとプロセス内 in-memory ストアが使われます。
> 別プロセスから attach したい場合は `file_system_state_store(path)`
> を必ず渡してください。

### 処理フロー

```
[1] PyTssrunManager.spawn()
       |
       v
   TssrunManager::spawn -> spawn_with(&TokioBackgroundDispatcher)
       |
       |-- argv = self.cmd.build_argv()        <- Spec 層
       |
       |-- dispatcher.spawn(&argv, &env, cwd)  <- Background Runtime
       |     SpawnedChild { pid, child }
       |     stdin=null, stdout/stderr=pipe
       |
       |-- uuid = Uuid::now_v7()               <- spawn ごとに 1 回
       |     in-memory snapshot / on-disk filename / store entry が
       |     全部この uuid を共有（second source of truth を作らない）
       |
       |-- init = TssrunJobSnapshot { uuid, pid, argv, sent_env,    // PR #7 rename
       |          cwd, started_at_unix, log_locations: None,
       |          jobid: None, node: None, finished: None }
       |
       `-- TssrunJobHandle::from_spawn(spawned, init, log_sink, Some(store))
              |
              |-- watch::channel(init) -> (tx, rx)
              |
              |-- store.save(&init).await    <- 初期 snapshot を永続化
              |   (InMemory なら HashMap に insert、FS なら
              |    {dir}/{uuid}.json を atomic-rename で書き出し)
              |
              |-- tokio::spawn(tee_stdout(stdout, sink, tx, store))
              |     stdout から行単位に読み、
              |       1) sink.append(Stdout, line)
              |       2) parse_salloc_jobid(line) で jobid を tx.send_modify
              |       3) parse_salloc_node(line) で node を tx.send_modify
              |       4) snapshot 変更時は store.save(&snap) で再永続化
              |
              |-- tokio::spawn(tee_stderr(...))     同上 (Stderr)
              |
              `-- tokio::spawn(wait task)
                    let status = child.wait().await
                    code = status.code()                <- Option<i32>
                    tx.send_modify(|s| s.finished = Some(FinishedInfo {
                        exit_code: code,
                        finished_at_unix: ...,
                    }))
                    store.save(&snap).await
                    log_sink.flush()

[2] Python が `await handle.pid` する
       |
       v
   PyTssrunJobHandle::pid (getter)
       |
       `-- self.rx.borrow().pid   <- snapshot からロックフリーで読む
                                     Mutex<TssrunJobHandle> には触らない

[3] Python が `await handle.wait()` する
       |
       v
   PyTssrunJobHandle::wait
       |
       `-- self.inner.lock().await.wait().await
              |
              `-- TssrunJobHandle::wait
                    - tee_stdout_handle.take().await  (drain)
                    - tee_stderr_handle.take().await
                    - wait_handle.take().await        <- Option<i32>
              => Result<Option<i32>>
                  Ok(Some(0))  = 正常終了
                  Ok(None)     = シグナル kill
                  Err(...)     = wait 自体の失敗 / 二重 wait
```

### 並行性のキーポイント

- **スナップショット getter は `watch::Receiver` から読む**ので
  `wait()` の `Mutex<TssrunJobHandle>` を取らない。Python 側で
  `is_running` をループしながら `wait()` を進めても両者は競合しない。
- **`wait()` は唯一の `&mut self` メソッド**で、内部の
  `Option<JoinHandle>` を `.take()` する。1 回しか呼べない設計を
  Rust の所有権で表現している。
- **シグナル kill を `Ok(None)` で表現**。0 を返して隠蔽する
  バグを避ける目的。Python の `wait()` も `int | None` を返す。

## 4. tssrun attach — 別プロセスから JSON スナップショットを読む

### Python 側のコード例

```python
# プロセス A で投入したジョブを、プロセス B から再 attach する
from slurm_async_runner._slurm_async_runner_core.tssrun import (
    TssrunManager, file_system_state_store,
)

manager_b = TssrunManager(
    cmd,
    store=file_system_state_store("/var/lib/slurm-runner"),
)
# 推奨: UUID v7 primary key で O(1) attach
attached = await manager_b.attach_uuid("01900000-0000-7000-8000-000000000000")
# best-effort fallback: pid / jobid scan
# attached = await manager_b.attach_pid(12345)
# attached = await manager_b.attach_jobid(102362)
print(await attached.jobid, await attached.node)
# attached.wait() は RuntimeError —— wait できるのは spawn 元だけ
```

### 処理フロー

```
[A プロセス: spawn]                 [ファイルシステム / store]      [B プロセス: attach]
TssrunManager.with_state_dir(d)         |
        .spawn()                        |
   v TssrunJobHandle::from_spawn         |
   v store.save(&snap).await             |
                                         |
                            <state_dir>/<uuid>.json
                            (例: 01900000-0000-7000-8000-000000000000.json)
                            { uuid: "01900000-...", pid: 12345,
                              argv: [...], sent_env: {...},
                              jobid: 102362, node: "cnode3",
                              finished: {...} }
                                         |
                                         |  <- B プロセスが
                                         |     manager_b.attach_uuid("01900000-...")
                                         |     （または attach_pid / attach_jobid /
                                         |       attach_file のいずれか）
                                         |
                                         |  TssrunManager::attach(key)
                                         |   match key {
                                         |     Uuid(u)  => store.load(u).await,
                                         |     Pid(p)   => store.find_by_pid(p).await,
                                         |     JobId(j) => store.find_by_jobid(j).await,
                                         |     File(p)  => tokio::fs::read(p) +
                                         |                 serde_json::from_slice,
                                         |   }
                                         |   -> TssrunJobHandle::attach_snapshot(
                                         |        snap, Some(store))
                                         |       wait_handle = None
                                         |       tee_handles = None
                                         |
                                         v
                            attached.uuid / .pid / .jobid / .node は
                            すべて snapshot から読める
                            attached.wait()  ->  RuntimeError
                              ("not owner of the child / already waited")
```

### `AttachKey` のバリエーション

- `AttachKey::Uuid(uuid)` — primary key で O(1) lookup。長期参照ならこれ。
- `AttachKey::Pid(pid)` — best-effort。`InMemoryStateStore` は `HashMap`
  値を線形走査、`FileSystemStateStore` は `state_dir` 内を `read_dir` で
  走査して `snapshot.pid == pid` の最初のファイルを採用。Pid はカーネルで
  再利用されうるので一過性の用途のみ。
- `AttachKey::JobId(jobid)` — 同じく best-effort scan。`salloc:` バナーが
  パースされて `snapshot.jobid` がセットされた後でないとヒットしない。
- `AttachKey::File(path)` — JSON パスを直接指定。store を経由しないので
  デバッグ・リカバリ用。

### `TssrunJobSnapshot` の永続フォーマット

JSON ファイルのスキーマは `src/tssrun/handle.rs::TssrunJobSnapshot` の
（PR #7 で `JobHandleSnapshot` から rename。`kind = "tssrun"` 文字列は不変）
`Serialize` 派生に従います。ファイル名は `{uuid}.json` で、`uuid` は
スナップショット内のフィールドと完全一致します（合成例）:

```json
{
  "uuid": "01900000-0000-7000-8000-000000000000",
  "pid": 12345,
  "argv": ["tssrun", "-p", "gr19999b", "-t", "0:01:00", "/work/job.sh"],
  "sent_env": {"OMP_NUM_THREADS": "4"},
  "cwd": "/work",
  "started_at_unix": 1762700000,
  "log_locations": { "Files": { "stdout": "/tmp/o.log", "stderr": "/tmp/e.log" } },
  "jobid": 102362,
  "node": "cnode3",
  "finished": { "exit_code": 0, "finished_at_unix": 1762700085 }
}
```

- `uuid` は **UUID v7**（時刻順に並ぶので `ls` で投入順に整列する）。
  `JobStateStore` の primary key であり、ファイル名と内容で同一値を保つ。
- `started_at_unix` / `finished_at_unix` は `i64`（UNIX エポック秒、UTC）。
- `finished` は子が終了するまで `null`。シグナル kill 時は
  `finished.exit_code = null`。
- `log_locations` の variant は `"None"` / `{ "Files": {...} }` の 2 つ
  （将来 SQLite 等に拡張する想定）。

## 5. live_env — `/proc/<pid>/environ` の best-effort 読み出し

### 流れ

```
PyTssrunJobHandle::live_env
   |
   v
read_live_env_for_pid(pid)
   |
   |-- cfg!(target_os = "linux") == false  -> Ok(None)        # mac/windows
   |
   `-- tokio::fs::read("/proc/{pid}/environ")
         |
         |-- Ok(bytes)
         |     parse_environ(bytes)
         |       - NUL 区切り
         |       - "K=V" にスプリット
         |       - 非UTF8 / '=' 無し は dropped カウンタ + tracing::warn
         |     -> Ok(Some(HashMap))
         |
         |-- Err(NotFound)         -> Ok(None)                # 子は既に終了
         |
         |-- Err(PermissionDenied) -> Ok(None) + tracing::debug
         |     ECCS の tssrun は setuid + PR_SET_DUMPABLE クリア
         |     /proc/<pid>/environ が root:root 0400 になる
         |
         `-- Err(other)            -> Err (本当の I/O エラー)
```

> Python 側からは `await handle.live_env()` が **`dict[str, str] | None`**
> を返します。`None` は「読めなかった」を**全部丸めた**ものなので、
> 失敗扱いしないでください（仕様）。詳しくは `docs/setup_test.md` §6.1。

## 6. 全体タイムライン例: tssrun ジョブの生涯

| t | アクター | イベント |
|---|---|---|
| 0ms | Python | `await manager.spawn()` |
| 1ms | Rust | `TssrunCmd.build_argv` → `["tssrun", "-p", ..., "/work/job.sh"]` |
| 2ms | Rust | `TokioBackgroundDispatcher.spawn` → 子 pid=12345 取得 |
| 2ms | Rust | `Uuid::now_v7()` → `01900000-0000-7000-8000-000000000000` 生成 |
| 3ms | Rust | `TssrunJobHandle::from_spawn` で watch チャンネルと 3 タスク起動 |
| 3ms | Store | `store.save(&init)` で初期スナップショット永続化（FS なら `<state_dir>/01900000-….json`、jobid/node/finished は None） |
| 4ms | Python | `spawn()` が `PyTssrunJobHandle` を返す |
| 5ms | Python | `await handle.pid` → 12345（rx.borrow から即返） |
| ~50ms | tssrun | `salloc: Granted job allocation 102362` を stdout |
| ~50ms | Rust | tee_stdout が parse_salloc_jobid で 102362 を抽出、`snapshot.jobid = Some(102362)`、`store.save` で再永続化 |
| ~80ms | tssrun | `salloc: Nodes cnode3 are ready for job` |
| ~80ms | Rust | tee_stdout が parse_salloc_node で `cnode3` を抽出、`store.save` で再永続化 |
| ... | child | アプリケーション本体実行 |
| Tend | child | exit(0) |
| Tend | Rust | wait task が `status.code() = Some(0)` を `snapshot.finished` に書き、`store.save`、`log_sink.flush()` |
| Tend | Python | `code = await handle.wait()` → `Some(0)` |
| Tend+ε | 別 Python | `await another_manager.attach_uuid("01900000-…")` で O(1) 再構成。`attach_jobid(102362)` / `attach_pid(12345)` も可（scan ベース、best-effort） |
