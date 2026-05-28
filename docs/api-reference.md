# API Reference — `tssrun` / `sbatch` backends

ルート [`README.md`](../README.md) は **Manager / Handle の契約面**
(spawn / attach / wait / refresh のシグネチャと sync vs async の区別) を
中心にまとめています。本ファイルは README から外した **データクラスの
全フィールド・factory helper・cross-process attach の内部メカニクス**
を補完します。

> ソース・オブ・トゥルースは `.pyi` stub と Rust 側 `src/py_export/`
> です。本リファレンスと食い違った場合は stub と pyo3 export を正と
> してください。

## 想定読者

- ルート README を読んだあと、Cmd / FinishedInfo の全フラグや
  factory helper を確認したい開発者
- attach 経由で別プロセスから handle を再構築するときの不変条件を
  確認したい運用者

## 目次

- [`tssrun` backend](#tssrun-backend)
  - [`TssrunCmd` fields](#tssruncmd-fields)
  - [`LogSink` factory helpers](#logsink-factory-helpers)
  - [`JobStateStore` factory helpers](#jobstatestore-factory-helpers)
  - [Cross-process attach internals](#tssrun-cross-process-attach-internals)
- [`sbatch` backend](#sbatch-backend)
  - [`SbatchCmd` fields](#sbatchcmd-fields)
  - [`FinishedInfo` fields](#finishedinfo-fields)
  - [Typed flag entities (`sbatch_options::*`)](#typed-flag-entities-sbatch_options)
  - [Cross-process attach internals](#sbatch-cross-process-attach-internals)

---

## `tssrun` backend

`slurm_async_runner._slurm_async_runner_core.tssrun` 配下。Stub:
[`python/.../tssrun.pyi`](../python/slurm_async_runner/_slurm_async_runner_core/tssrun.pyi).

### `TssrunCmd` fields

1 回の `tssrun` 実行仕様を保持する純データクラス。Rust 側で
`build_argv()` が argv を組み立てます。

| Field | Shape | Default | Notes |
|---|---|---|---|
| `program` | `str \| PathLike` | required | 実行するプログラム / スクリプトの path |
| `args` | `list[str]` | `[]` | プログラムへの位置引数 |
| `partition` | `str \| None` | `None` | SLURM `-p` (queue / partition) |
| `time_limit` | `JobTimeLimit \| None` | `None` | `-t` (`"HH:MM:SS"` を `JobTimeLimit` で typed-wrap) |
| `rsc` | `ResourceSpec \| None` | `None` | `--rsc` (`p=` / `c=` / `m=` を `ResourceSpec` で typed-wrap) |
| `x11` | `bool` | `False` | `--x11` フォワーディング |
| `env` | `dict[str, str]` | `{}` | 子プロセスに export する環境変数 (snapshot 対象) |
| `cwd` | `str \| PathLike \| None` | `None` | 子プロセスの作業ディレクトリ |
| `tssrun_bin` | `str` | `"tssrun"` | バイナリ path / 名前のオーバーライド |

> `JobTimeLimit` と `ResourceSpec` は
> `slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options`
> から再エクスポートされています (tssrun と sbatch で共有)。

### `LogSink` factory helpers

子プロセスの stdout / stderr の出口を決める opaque ハンドル。
Python から直接サブクラス化はできず、以下の factory のみ:

| Factory | Returns | Notes |
|---|---|---|
| `null_log_sink()` | `LogSink` | 全破棄 (`/dev/null` 等価) |
| `std_log_sink()` | `LogSink` | Python プロセスの stdout / stderr へそのまま流す |
| `await file_log_sink(stdout, stderr)` | `LogSink` | 指定 2 path へ追記。親ディレクトリは事前に存在している必要 |

### `JobStateStore` factory helpers

snapshot 永続化のバックエンド。Python からサブクラス化はできず、
以下の factory のみ。新規バックエンドを足したい場合は Rust 側で
`JobStateStore` trait を実装してから Python へ再エクスポートします。

| Factory | Returns | Notes |
|---|---|---|
| `in_memory_state_store()` | `JobStateStore` | プロセスローカルの in-memory store。`TssrunManager` のデフォルト |
| `file_system_state_store(dir)` | `JobStateStore` | `{dir}/{uuid}.json` に atomic rename で書き出す。ディレクトリは初回 save 時に lazy 作成。親が writable であれば未作成パスでも OK |

> `SbatchManager` 側は `state_dir: str | PathLike | None` を直接受けます
> (opaque type を経由しません)。これは backend ごとの設計判断であり、
> `JobStateStore` の opaque ラップは tssrun 由来の `LogSink` と
> 対称性を保つ意図で残しています。

### tssrun Cross-process attach internals

`file_system_state_store(dir)` を渡した `TssrunManager` は、`salloc:`
パース / wait 完了などの状態遷移ごとに `{dir}/{uuid}.json` を
atomic rename で書き出します。別プロセスは `TssrunManager.attach_*`
の 4 種で read-only ハンドルを再構築できます:

| Attach key | Method | Resolution cost | Notes |
|---|---|---|---|
| UUID v7 (canonical hyphenated string) | `attach_uuid` | O(1) primary-key lookup | **推奨**。canonical reference として永続的に safe |
| OS pid | `attach_pid` | linear scan over state dir | pid は kernel に再利用されうるので長期保管 NG |
| SLURM jobid | `attach_jobid` | linear scan | `salloc:` パース完了後のみ解決可能 |
| JSON path | `attach_file` | direct read | `{uuid}.json` を直接渡す |

Attached handle は snapshot getter (`uuid` / `jobid` / `pid` / `node` /
`sent_env` / `is_running` / `is_finished` / `exit_code`) すべて使えます
が、`wait()` は **owner-only** で attached 側で呼ぶと `RuntimeError`
を投げます。代わりに `wait_terminal(poll_interval_secs)` を使ってください
(これは polling ベースなので attached 側でも安全)。

`live_env()` は `/proc/<pid>/environ` を読むため、子プロセスが
**生存中の Linux ホスト** からしか有効ではありません (オフ Linux /
プロセス終了後は `None`)。

シーケンス図は [`process-flow.md`](process-flow.md) の `AttachKey` 章、
設計判断は [`architecture.md`](architecture.md) §3 を参照。

---

## `sbatch` backend

`slurm_async_runner._slurm_async_runner_core.sbatch` 配下。Stub:
[`python/.../sbatch.pyi`](../python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi).

### `SbatchCmd` fields

1 回の `sbatch` 投入仕様を保持する純データクラス。Rust 側で
`build_argv()` が argv を組み立てます。

| Field | Shape | Default | Notes |
|---|---|---|---|
| `script` | `str \| PathLike` | required | 投入する shell script の path |
| `sbatch_bin` | `str` | `"sbatch"` | バイナリ path / 名前のオーバーライド |
| `job_name` | `str \| None` | `None` | `--job-name` |
| `partition` | `str \| None` | `None` | `-p` |
| `time_limit` | `str \| None` | `None` | `-t` (`"HH:MM:SS"` 文字列。tssrun と異なり typed wrap なし) |
| `rsc` | `str \| None` | `None` | `--rsc` (raw 文字列。tssrun と異なり `ResourceSpec` 非対応) |
| `output` | `str \| None` | `None` | `-o` (`%j` / `%a` 等の SLURM プレースホルダ可) |
| `error` | `str \| None` | `None` | `-e` (同上) |
| `chdir` | `str \| PathLike \| None` | `None` | `-D` |
| `env` | `dict[str, str] \| None` | `None` | export する環境変数 (`--export=ALL` ベース) |
| `args` | `list[str] \| None` | `None` | script への末尾位置引数 |
| `no_requeue` | `bool` | `False` | `--no-requeue` |
| `comment` | `str \| None` | `None` | `--comment` |
| `nice` | `int \| None` | `None` | `--nice` (issue #13) |
| `dependency` | `SlurmDependency \| None` | `None` | typed `--dependency` (下記 entity 参照) |
| `mail_user` | `str \| None` | `None` | `--mail-user` |
| `mail_types` | `MailTypeInput \| None` | `None` | `--mail-type` |
| `signal` | `SlurmSignalSpec \| None` | `None` | `--signal=[B:]<sig>[@<seconds>]` |
| `array_spec` | `SlurmArraySpec \| None` | `None` | `--array=<spec>`。**`spawn_array` 経由で投入する場合のみ** 設定 |

### `FinishedInfo` fields

`SbatchManager.run()` / `run_with_jobid_callback()` の戻り値。
`sacct` で terminal state を解決したあとに構築されます。

| Field | Shape | Notes |
|---|---|---|
| `final_state` | `str` | SLURM state string (`"COMPLETED"` / `"FAILED"` / `"TIMEOUT"` / etc.) |
| `final_reason` | `str` | `sacct` の `%r` カラム。clean exit は `"None"`、それ以外は `"NonZeroExitCode"` / `"TimeLimit"` / `"OutOfMemory"` 等。Unknown reason は raw のまま |
| `exit_code` | `int \| None` | 通常 Unix exit code。signal kill 時は `128 + signum`。解決不能で `None` |
| `finished_at` | `str` | RFC3339 timestamp 文字列 |

### Typed flag entities (`sbatch_options::*`)

`SbatchCmd` の typed フラグ引数は
`slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options`
配下から import します:

| Entity | 用途 | 構築例 |
|---|---|---|
| `SlurmDependency` | `--dependency=` | `SlurmDependency.afterok([105501])` / `afterany([...])` / `aftercorr([...])` |
| `MailTypeInput` | `--mail-type=` | `MailTypeInput.from_str("END,FAIL")` |
| `SlurmSignalSpec` | `--signal=[B:]<sig>[@<seconds>]` | `SlurmSignalSpec(sig="USR1", seconds_before_end=60)` |
| `SlurmArraySpec` | `--array=<spec>` | `SlurmArraySpec.from_str("0-9%2")` |
| `Memory` | `--mem=` などのメモリ表現 | `Memory("2G")` (tssrun の `ResourceSpec` 内でも使用) |

詳細フィールド・コンストラクタの全シグネチャは
[`python/.../entities/slurm/sbatch_options/__init__.pyi`](../python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/sbatch_options/__init__.pyi)
を参照。

### sbatch Cross-process attach internals

`state_dir=<path>` を渡した `SbatchManager` は tssrun と **同じディレクトリ**
を共有でき、`{state_dir}/{uuid}.json` 内の `kind` discriminator フィールドで
backend を区別します (`"tssrun"` / `"sbatch"`)。

| Attach key | Method | Resolution cost | Notes |
|---|---|---|---|
| UUID v7 | `attach_uuid` | O(1) primary-key lookup | **推奨**。canonical reference |
| SLURM jobid | `attach_jobid` | linear scan | `kind == "sbatch"` のエントリのみ拾う |
| JSON path | `attach_file` | direct read | `{uuid}.json` を直接渡す |
| Array master jobid | `attach_array_jobid` | linear scan | `--array` 投入の全タスク handle を `list[SbatchJobHandle]` で返す |

sbatch backend は **owner-only な `wait` 概念が薄い** ため (sbatch は
キュー経由で動き、子プロセスを親 Python が直接持つわけではない)、
attached 側でも `refresh()` / `refresh_with_sacct()` /
`wait_terminal(poll_interval_secs)` / `log_lines()` /
`read_log_to_end()` がすべて使えます。

`wait_terminal` の polling chain
(`refresh` → `qgroup -l` → `squeue` → 必要なら `sacct`) と
FINI / FAIL terminal 認識、sacct gating heuristic は
[`process-flow.md`](process-flow.md) §5 を参照。設計判断
(poll cadence default / `send_replace` 不変条件 / sacct を heavyweight
opt-in にしている理由) は [`architecture.md`](architecture.md) §3.5。
