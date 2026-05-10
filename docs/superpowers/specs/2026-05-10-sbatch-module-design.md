# sbatch モジュール設計

- **Date**: 2026-05-10
- **Status**: Draft (brainstorming 完了、レビュー待ち)
- **Targets**: `crate::sbatch::*` (Rust) + `slurm_async_runner._core.sbatch` (Python)
- **References**:
  - KUDPC: <https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/batch>
  - KUDPC: <https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/tips>
  - SLURM: <https://slurm.schedmd.com/sbatch.html>
  - Prior art (本リポ内): `docs/superpowers/specs/2026-05-09-tssrun-wrapper-env-design.md`
  - Prior art (本リポ内): `docs/superpowers/specs/2026-05-10-slurm-vocab-migration-and-pyclass-ownership-design.md`

---

## 1. 背景と目的

`slurm-async-runner` には現状 2 つの実行経路がある:

1. `srun` 経由の同期投入 (`SlurmManager::run_job`)
2. `tssrun` (kudpc 拡張、`salloc + srun`) の非ブロッキング投入 (`TssrunManager::spawn` → `JobHandle`)

`sbatch` は **バッチジョブの最も一般的な投入手段** であり、長期計算ワークフロー (Gaussian、MD、ML 学習) の標準。tssrun との大きな違いは:

- `sbatch script.sh` は `Submitted batch job <jobid>` を 1 行返して即終了する。**長期駐在する子プロセスがない**。
- `#SBATCH` ディレクティブはスクリプト内 or CLI 引数の二系統。CLI 引数が優先 (sbatch 規約)。
- 出力ログは SLURM が直接 `-o`/`-e` パスへ書く (tee 不要)。
- 「wait」は `squeue`/`sacct` ポーリングか `sbatch --wait` のどちらか。
- 配列ジョブ (`--array`)、依存関係 (`-d`)、メール通知が固有機能として加わる。

本設計は **Phase 1 で fire-and-forget + 後追い監視 (attach)** をカバーし、**Phase 2 で `run()` (sbatch --wait 相当) と配列ジョブ** を非破壊で追加できる構造を整える。

### 1.1 Phase 1 のスコープ

- 単発バッチジョブ (`sbatch script.sh`) の投入と jobid 取得
- 投入後の状態 polling (`qgroup -l` → `squeue` フォールバック)、終端確定 (sacct を opt-in で 1 回のみ)
- スナップショットの永続化 (`{root}/sbatch/<uuid>.json` atomic-rename)
- 別プロセスからの attach (Uuid / JobId / File)
- Rust + pyo3 同時公開
- KUDPC マニュアル準拠の typed CLI フィールド (account/-A は不採用、リソース統括は `--rsc`)

### 1.2 明示的に Phase 1 外

- `--array` (配列ジョブ)
- `-d` 依存関係
- `--mail-user` / `--mail-type`
- `--no-requeue`、`--signal`
- `sbatch --wait` を使った同期 `run()` メソッド
- ログファイルの `tail` / `read` ユーティリティ (パス記録のみ)
- handle 共通 trait 化 (Phase 2 で抽象化が要るタイミングまで保留)

---

## 2. 採用アプローチ: **Approach A — Passive handle + watch スナップショット**

| 比較項目 | Approach A (採用) | Approach B (auto-poll task) | Approach C (jobid のみ) |
|---|---|---|---|
| バックグラウンドタスク | なし | spawn 時に auto-poll を起動 | なし |
| watch 駆動 | refresh() のみが send | auto-poll が定期 send | watch なし |
| クラスタ負荷の制御 | caller が頻度を完全制御 | poll_interval を bake-in | caller 依存 |
| Phase 2 `run()` 拡張 | `spawn → wait_terminal` で素直 | 同上 | handle 型を後付けする必要 |
| tssrun との API 連続性 | 高 (handle/attach/store 一致) | 高 | 低 |
| 実装コスト | 最小 | abort handle 管理が必要 | 最小 |

**A の決め手** は、`sbatch` で auto-poll をデフォルト ON にすると `sacct`/`squeue` のレートリミットを踏みやすく、KUDPC マニュアルが明示する負荷ガイドラインに反する点。能動 polling を caller が呼ぶ形に統一する。

---

## 3. モジュールレイアウト

```text
src/
├── store.rs                         ← 新設: 汎用 store (tssrun と sbatch で共有)
├── sbatch/                          ← 新設
│   ├── mod.rs                       // module-level rustdoc + pub mod 宣言
│   ├── cmd.rs                       // SbatchCmd (Spec 層) + build_argv
│   ├── parse.rs                     // parse_submitted_jobid + resolve_log_path
│   ├── handle.rs                    // SbatchJobHandle, SbatchJobSnapshot, SbatchLifecycle
│   ├── store.rs                     // impl JobSnapshot for SbatchJobSnapshot
│   ├── manager.rs                   // SbatchManager (spawn / attach)
│   └── error.rs                     // SbatchSpawnError (thiserror)
├── tssrun/store.rs                  ← 縮小: impl JobSnapshot for JobHandleSnapshot のみ
├── runner.rs                        ← 増設: parse_qgroup_l / query_*_via_qgroup
├── py_export/sbatch.rs              ← 新設
└── lib.rs                            ← pub mod sbatch + re-export
```

**既存からの再利用:**

- `crate::dispatcher::JobDispatcher` の `capture()` を sbatch 投入と SLURM クエリの両方に使う (新トレイト不要)
- `crate::runner::query_job_states_batch_with` の squeue/sacct パーサ
- `crate::entities::slurm::{JobPartition, JobTimeLimit, ResourceSpec, ...}` の typed vocab
- `crate::JobStatus` (= gaussian_job_shared)

**lib.rs re-export:**

```rust
pub mod sbatch;
pub use sbatch::cmd::SbatchCmd;
pub use sbatch::handle::{
    SbatchJobHandle, SbatchJobSnapshot, SbatchLifecycle, SbatchAttachKey,
    ResolvedLogPaths, FinishedInfo,
};
pub use sbatch::manager::SbatchManager;
pub use sbatch::error::SbatchSpawnError;

// 汎用 store (tssrun も sbatch も使う)
pub use store::{JobSnapshot, JobStateStore, InMemoryStateStore, FileSystemStateStore};
```

---

## 4. SbatchCmd (Spec 層)

`TssrunCmd` と同じ「pure data + `build_argv()`」パターン。I/O なし。

```rust
#[derive(Debug, Clone)]
pub struct SbatchCmd {
    pub sbatch_bin: String,                      // default "sbatch"

    // ジョブ識別/配置 (KUDPC ドキュメント済み)
    pub job_name: Option<String>,                // -J
    pub partition: Option<JobPartition>,         // -p (KUDPC では実質必須)

    // リソース・時間
    pub time_limit: Option<JobTimeLimit>,        // -t
    pub rsc: Option<ResourceSpec>,               // --rsc (kudpc 拡張)

    // I/O
    pub output: Option<String>,                  // -o (raw template、%j/%x 含む)
    pub error: Option<String>,                   // -e
    pub chdir: Option<PathBuf>,                  // --chdir

    // 環境
    pub env: HashMap<String, String>,            // --export=ALL,K1=V1,...

    // スクリプト
    pub script: PathBuf,                         // 必須、絶対化される
    pub args: Vec<String>,                       // 位置引数 (script 後)
}
```

**`build_argv()` 出力順:**

```text
[sbatch_bin,
 -J <job_name>?, -p <partition>?,
 -t <time_limit>?, --rsc <rsc>?,
 -o <output>?, -e <error>?,
 --chdir <chdir>?,
 --export=ALL,K=V,...?,
 <script_abs>,
 args...]
```

### 4.1 仕様判断

1. **`env` は `--export` にレンダリング**、tokio `Command::env` には設定しない。空 HashMap なら `--export` フラグ自体を出さない (= SLURM デフォルト = ALL)。
2. **`chdir` は SLURM の `--chdir`**、tokio `Command::current_dir` ではない。ジョブ自体の CWD を制御する。
3. **`script` は `std::path::absolute` で絶対化**。`SlurmCmd`/`TssrunCmd` と同じ規約。
4. **既存の `#SBATCH` ディレクティブは尊重**。CLI 引数は sbatch の規約により script 内ディレクティブを上書きする。
5. **`-A`/`--account` は採用しない**。KUDPC マニュアル未記載 + `--rsc` がリソース統括をしているので不要。
6. **KUDPC マニュアルが禁止する 50+ オプション** (`--nodes`, `--ntasks`, `--cpus-per-task`, `--mem`, `--gpus`, `--exclusive`, ...) は **typed フィールドに入れない**。誤指定の経路を最初から塞ぐ。

### 4.2 Phase 2 で追加予定 (additive、非破壊)

- `array: Option<ArraySpec>` — `-a`
- `dependency: Option<DependencySpec>` — `-d after[ok|notok|any]:N,...`
- `comment: Option<String>` — `--comment`
- `no_requeue: bool` — `--no-requeue`
- `mail_user: Option<String>` / `mail_type: Option<MailType>` — `--mail-*`

---

## 5. Store 層: tssrun と sbatch で共有 (同一ディレクトリ + kind discriminator)

### 5.1 設計方針

UUID v7 が事実上衝突しないことを利用し、tssrun と sbatch のスナップショットを **同じ `{root}/<uuid>.json` 名前空間** に共存させる。物理的なサブディレクトリ分割は行わず、JSON ファイル内に `kind` 識別子を埋めて論理分離する。

利点:

- UUID 1 つを渡せば (種別を知らなくても) 該当ファイルを直接読める
- root を 1 つだけ管理すれば良い (ユーザの mental model がシンプル)
- 移動先/移動元のサブディレクトリを誤る運用ミスがなくなる
- 既存 tssrun ファイルは path 移動なし (= migration が小さくなる)

トレードオフ:

- `list()` / `find_by_jobid()` の scan 時に「自分の kind ではない JSON」をスキップするオーバーヘッド (= ファイル読み + kind フィールド peek、わずか)

### 5.2 汎用 trait

```rust
// src/store.rs
pub trait JobSnapshot:
    Serialize + DeserializeOwned + Clone + Send + Sync + 'static
{
    fn uuid(&self) -> Uuid;
    fn jobid(&self) -> Option<u64>;
    /// On-disk JSON の `kind` フィールド値。
    /// scan 時に他の kind の snapshot を silent skip するために使う。
    fn kind() -> &'static str;
}

#[async_trait]
pub trait JobStateStore<S: JobSnapshot>: Send + Sync {
    async fn save(&self, snapshot: &S) -> Result<()>;
    async fn load(&self, uuid: Uuid) -> Result<Option<S>>;
    async fn find_by_jobid(&self, jobid: u64) -> Result<Option<S>>;
    async fn list(&self) -> Result<Vec<S>>;
}

pub struct InMemoryStateStore<S: JobSnapshot> {
    inner: Arc<Mutex<HashMap<Uuid, S>>>,
}

pub struct FileSystemStateStore<S: JobSnapshot> {
    root: PathBuf,                                     // 単一ディレクトリ、subdir なし
    _phantom: PhantomData<S>,
}
```

### 5.3 各 snapshot の実装

```rust
// src/tssrun/store.rs
impl JobSnapshot for JobHandleSnapshot {
    fn uuid(&self) -> Uuid { self.uuid }
    fn jobid(&self) -> Option<u64> { self.jobid }
    fn kind() -> &'static str { "tssrun" }
}

// src/sbatch/store.rs
impl JobSnapshot for SbatchJobSnapshot {
    fn uuid(&self) -> Uuid { self.uuid }
    fn jobid(&self) -> Option<u64> { Some(self.jobid) }
    fn kind() -> &'static str { "sbatch" }
}
```

### 5.4 On-disk JSON スキーマ

各ファイルは `{root}/<uuid>.json` で、トップレベルに `kind` フィールドを含む:

```jsonc
// {root}/01927a4d-7c8b-7000-8000-abcdef012345.json (tssrun snapshot)
{
  "kind": "tssrun",
  "uuid": "01927a4d-7c8b-7000-8000-abcdef012345",
  "pid": 12345,
  "argv": ["tssrun", "..."],
  "jobid": 102362
  // ...
}

// {root}/01927a4e-9d2c-7000-9100-fedcba987654.json (sbatch snapshot)
{
  "kind": "sbatch",
  "uuid": "01927a4e-9d2c-7000-9100-fedcba987654",
  "jobid": 102363,
  "argv": ["sbatch", "..."]
  // ...
}
```

`kind` は serde の flatten/wrap で snapshot 構造体外側に付与する (snapshot struct 自体には kind フィールドを持たない、store 層で envelope する)。

### 5.5 FileSystemStateStore の load / list 実装メモ

```rust
async fn load(&self, uuid: Uuid) -> Result<Option<S>> {
    let path = self.root.join(format!("{uuid}.json"));
    let bytes = match tokio::fs::read(&path).await {
        Ok(b) => b,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    if value.get("kind").and_then(|v| v.as_str()) != Some(S::kind()) {
        // ファイルは存在するが別 kind (例: tssrun の load() が sbatch ファイルを引いた)
        return Ok(None);
    }
    Ok(Some(serde_json::from_value(value)?))
}
```

`list()` と `find_by_jobid()` は同じパターンで dir を scan し、kind 不一致の JSON は silent skip。`save()` は serialize 後に envelope で `{"kind": S::kind(), ...flattened snapshot}` を作って atomic-rename。

### 5.6 移行の影響 (breaking change、ただし path 不変)

- API: 既存 `tssrun::store::JobStateStore` (concrete trait) → `JobStateStore<JobHandleSnapshot>` (parametrized) への置換。`Arc<dyn JobStateStore<JobHandleSnapshot>>` に書き換え。
- ファイルレイアウト: **path は変わらず `{root}/<uuid>.json` のまま** (= ユーザの手動 mv 不要)。
- JSON 形式: 既存 tssrun ファイルには `kind` フィールドが無い。**load 時の互換性ハンドリング**を 2 通りから選択:
  1. **Lenient**: kind フィールドが無い tssrun ファイルは "tssrun" とみなして読む (legacy fallback)。次の save で kind が自動付与される
  2. **Strict**: kind 必須、無いファイルは load() が `None` を返す
- 0.1.x なので Lenient を採用、CHANGELOG に「次の save で kind が補完される」旨を記載。新 sbatch ファイルは最初から kind 付き。

---

## 6. SbatchJobSnapshot

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SbatchJobSnapshot {
    // 識別子
    pub uuid: Uuid,
    pub jobid: u64,

    // 投入時情報 (immutable)
    pub argv: Vec<String>,
    pub sent_env: HashMap<String, String>,
    pub script_path: PathBuf,
    pub chdir: Option<PathBuf>,
    pub partition: Option<JobPartition>,
    pub submitted_at: DateTime<Utc>,

    // ログ
    pub output_template: Option<String>,
    pub error_template: Option<String>,
    pub resolved: ResolvedLogPaths,

    // ライフサイクル (refresh で書き換わる部分)
    pub lifecycle: SbatchLifecycle,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SbatchLifecycle {
    /// 直近 polling で観測した state。`refresh()` を 1 度も呼んでないなら None。
    pub last_observed_state: Option<JobStatus>,
    pub last_observed_at: Option<DateTime<Utc>>,
    /// `qgroup -l` および `squeue` の active listing から消えたら true。
    /// 一度立ったら以降立ち下がらない (monotonic)。
    /// この立ち上がりが「終端確定のため sacct を 1 回叩くべき」シグナル。
    pub left_active_listing: bool,
    /// 終端確定 (sacct 経由 or active listing が直接終端 state を返した) で Some。
    pub finished: Option<FinishedInfo>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResolvedLogPaths {
    /// 例: "/work/slurm-12345.out"。
    /// client 側で展開できなかった %x/%A/%a 等は raw のまま残る。
    pub output: Option<PathBuf>,
    pub error: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinishedInfo {
    pub final_state: JobState,
    pub final_reason: JobReason,
    pub exit_code: Option<i32>,
    pub finished_at: DateTime<Utc>,                // 観測時刻 (sacct の End ではない)
}
```

### 6.1 設計判断

- **lifecycle struct で refresh の書き換え範囲を分離**。投入時 immutable な情報との混同を防ぐ。
- **`left_active_listing` はツール名に依存しない命名**。今は qgroup -l + squeue がソースだが将来別ツールに切り替えても意味が壊れない。
- **`finished_at` は観測時刻**であって SLURM の `End` フィールドではない。クロックドリフト混入回避。
- **`finished.is_some()` なら以降の refresh は no-op で良い** (state は遷移しない前提)。

---

## 7. SbatchAttachKey

```rust
pub enum SbatchAttachKey {
    Uuid(Uuid),                                    // O(1)、長期参照向け推奨
    JobId(u64),                                    // O(n) scan、便利
    File(PathBuf),                                 // 既知 {uuid}.json 直接指定
}
```

`Pid` バリアントは持たない。sbatch プロセスは即終了するため pid はジョブと無関係。

---

## 8. SbatchManager (spawn / attach)

### 8.1 構造体とビルダー

```rust
#[derive(Clone)]
pub struct SbatchManager {
    cmd: SbatchCmd,
    store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
    dispatcher: Arc<dyn JobDispatcher>,
}

impl SbatchManager {
    pub fn new(cmd: SbatchCmd) -> Self { /* InMemoryStateStore + TokioDispatcher */ }
    pub fn with_state_dir(self, root: impl Into<PathBuf>) -> Self;
    pub fn with_state_store(
        self,
        store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
    ) -> Self;
    pub fn with_dispatcher(self, dispatcher: Arc<dyn JobDispatcher>) -> Self;
}
```

`with_log_sink` はない (sbatch のログは SLURM が直接ファイルに書くため tee 不要)。

### 8.2 spawn フロー

```rust
pub async fn spawn(&self) -> Result<SbatchJobHandle, SbatchSpawnError> {
    let argv = self.cmd.build_argv()?;
    let (exit_code, stdout) = self.dispatcher.capture(&argv).await?;
    if exit_code != 0 {
        return Err(SbatchSpawnError::SubmitFailed { exit_code, stdout });
    }
    let jobid = parse_submitted_jobid(&stdout)
        .ok_or_else(|| SbatchSpawnError::JobidParseError { stdout: stdout.clone() })?;

    let uuid = Uuid::now_v7();
    let resolved = ResolvedLogPaths {
        output: self.cmd.output.as_deref()
            .map(|t| resolve_log_path(t, jobid, self.cmd.job_name.as_deref())),
        error: self.cmd.error.as_deref()
            .map(|t| resolve_log_path(t, jobid, self.cmd.job_name.as_deref())),
    };
    let snapshot = SbatchJobSnapshot { /* ... */ lifecycle: Default::default() };

    self.store.save(&snapshot).await
        .map_err(|source| SbatchSpawnError::SubmittedButUnpersisted { jobid, source })?;
    Ok(SbatchJobHandle::new(snapshot, self.store.clone(), self.dispatcher.clone()))
}
```

### 8.3 attach フロー

```rust
pub async fn attach(&self, key: SbatchAttachKey) -> Result<SbatchJobHandle> { /* ... */ }
pub async fn attach_uuid(&self, u: Uuid) -> Result<SbatchJobHandle>;
pub async fn attach_jobid(&self, j: u64) -> Result<SbatchJobHandle>;
pub async fn attach_file(&self, p: impl Into<PathBuf>) -> Result<SbatchJobHandle>;
```

### 8.4 owned vs attached の区別なし

tssrun と違い、子プロセスを所有しないため **`SbatchJobHandle` は単一の型で、spawn と attach のどちらから来ても同じ機能**。`refresh()` / `wait_terminal()` はすべて `&self`、複数 handle が並行に呼んでも安全。

---

## 9. SbatchJobHandle (concurrency model)

### 9.1 内部構造

```rust
#[derive(Clone)]
pub struct SbatchJobHandle(Arc<SbatchJobHandleInner>);

struct SbatchJobHandleInner {
    snapshot_tx: watch::Sender<SbatchJobSnapshot>,
    store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
    dispatcher: Arc<dyn JobDispatcher>,
    refresh_lock: tokio::sync::Mutex<()>,
}
```

`Arc` ラッパで `Clone` 安価、タスク間共有が容易。

### 9.2 公開 API

```rust
impl SbatchJobHandle {
    // Lock-free read
    pub fn snapshot(&self) -> SbatchJobSnapshot;
    pub fn watch(&self) -> watch::Receiver<SbatchJobSnapshot>;

    // 不変フィールド getter
    pub fn uuid(&self) -> Uuid;
    pub fn jobid(&self) -> Option<u64>;             // 常に Some (uniformity のため Option)
    pub fn partition(&self) -> Option<JobPartition>;
    pub fn resolved_log_paths(&self) -> ResolvedLogPaths;
    pub fn sent_env(&self) -> HashMap<String, String>;

    // 共通ステータス helper (tssrun と uniform)
    pub fn is_running(&self) -> bool;
    pub fn is_finished(&self) -> bool;
    pub fn exit_code(&self) -> Option<i32>;

    // 状態更新 (refresh_lock で直列化)
    pub async fn refresh(&self) -> Result<SbatchJobSnapshot>;
    pub async fn refresh_with_sacct(&self) -> Result<SbatchJobSnapshot>;
    pub async fn wait_terminal(&self, poll_interval: Duration)
        -> Result<SbatchJobSnapshot>;
}
```

### 9.3 refresh のフォールバック順序: **qgroup -l → squeue → 諦める** (sacct なし)

```rust
pub async fn refresh(&self) -> Result<SbatchJobSnapshot> {
    let _guard = self.0.refresh_lock.lock().await;
    let mut snap = self.0.snapshot_tx.borrow().clone();
    let now = Utc::now();

    if let Some(state) = qgroup_l_lookup(&*self.0.dispatcher, snap.jobid).await? {
        snap.lifecycle.last_observed_state = Some(state);
        snap.lifecycle.last_observed_at = Some(now);
    } else if let Some(state) = squeue_lookup(&*self.0.dispatcher, snap.jobid).await? {
        snap.lifecycle.last_observed_state = Some(state);
        snap.lifecycle.last_observed_at = Some(now);
    } else {
        snap.lifecycle.left_active_listing = true;
        snap.lifecycle.last_observed_at = Some(now);
    }

    self.0.store.save(&snap).await?;
    let _ = self.0.snapshot_tx.send(snap.clone());
    Ok(snap)
}
```

### 9.4 refresh_with_sacct: 終端確定が必要な時だけ呼ぶ

```rust
pub async fn refresh_with_sacct(&self) -> Result<SbatchJobSnapshot> {
    let mut snap = self.refresh().await?;
    if snap.lifecycle.finished.is_some() { return Ok(snap); }
    if !snap.lifecycle.left_active_listing { return Ok(snap); }

    // qgroup -l と squeue の両方から消えた + finished 未確定 → sacct を 1 回だけ
    let _guard = self.0.refresh_lock.lock().await;
    let final_status = sacct_lookup(&*self.0.dispatcher, snap.jobid).await?;
    snap.lifecycle.finished = Some(FinishedInfo {
        final_state: final_status.state,
        final_reason: final_status.reason,
        exit_code: final_status.exit_code,
        finished_at: Utc::now(),
    });
    self.0.store.save(&snap).await?;
    let _ = self.0.snapshot_tx.send(snap.clone());
    Ok(snap)
}
```

### 9.5 wait_terminal: sacct を呼ばず軽量ループ

```rust
pub async fn wait_terminal(
    &self,
    poll_interval: Duration,
) -> Result<SbatchJobSnapshot> {
    loop {
        let snap = self.refresh().await?;
        if let Some(state) = snap.lifecycle.last_observed_state {
            if state.state.is_terminal() { return Ok(snap); }
        }
        if snap.lifecycle.left_active_listing { return Ok(snap); }
        tokio::time::sleep(poll_interval).await;
    }
}
```

呼び出し側パターン:

```rust
let snap = handle.wait_terminal(Duration::from_secs(30)).await?;   // 軽量
let snap = if snap.lifecycle.finished.is_none() {
    handle.refresh_with_sacct().await?                              // 1 回だけ sacct
} else { snap };
```

### 9.6 concurrency 不変条件

| 操作 | 並行性 | ロック |
|---|---|---|
| `snapshot()` / `watch()` / `uuid()` / `jobid()` / `partition()` / `resolved_log_paths()` / `is_running()` / `is_finished()` / `exit_code()` | 完全 lock-free | なし (watch::borrow) |
| `refresh()` / `refresh_with_sacct()` | 直列化 | `tokio::sync::Mutex<()>` |
| `wait_terminal()` のループ | 各反復で短時間ロック取得・解放 | 同上 |

**重要:**
- `watch::Sender::send()` は `refresh_lock` 取得中にだけ実行 → Receiver は linearizable な状態遷移のみ観測。
- `wait_terminal` は sleep 中ロック解放、外部 `refresh()` 呼び出しをブロックしない。

### 9.7 tssrun との API 統一

| メソッド | tssrun::JobHandle | SbatchJobHandle |
|---|---|---|
| `snapshot()` | ✓ | ✓ |
| `watch()` | ✓ | ✓ |
| `uuid()` | ✓ | ✓ |
| `jobid() -> Option<u64>` | ✓ | ✓ (常に Some だが型統一) |
| `sent_env()` | ✓ | ✓ |
| `is_running()` | ✓ | ✓ |
| `is_finished()` | ✓ | ✓ |
| `exit_code() -> Option<i32>` | ✓ | ✓ |

意味の定義は **lifecycle struct に集約** し、handle メソッドは委譲のみ:

```rust
impl SbatchLifecycle {
    pub fn is_running(&self) -> bool {
        if self.left_active_listing { return false; }
        self.last_observed_state.as_ref()
            .map(|s| s.state.is_running())
            .unwrap_or(false)
    }
    pub fn is_finished(&self) -> bool { self.finished.is_some() }
    pub fn exit_code(&self) -> Option<i32> {
        self.finished.as_ref().and_then(|f| f.exit_code)
    }
}
```

tssrun 側も同じパターンに揃える小さなリファクタが Phase 1 のスコープに含まれる (handle 上の `is_running`/`exit_code` 計算ロジックを snapshot 側 helper に降ろす)。

### 9.8 type-specific extras (uniform にしない)

| 機能 | tssrun のみ | sbatch のみ |
|---|---|---|
| `pid` | ✓ | ✗ (sbatch process 即死) |
| `node` | ✓ | ✗ (Phase 2 で qgroup -l 出力に依存) |
| `live_env()` | ✓ | ✗ |
| `wait()` (owner-only、子プロセス join) | ✓ | ✗ |
| `wait_terminal(poll)` | ✗ | ✓ |
| `refresh()` / `refresh_with_sacct()` | ✗ (tee で自動更新) | ✓ |
| `resolved_log_paths()` | ✗ | ✓ |

意味のないメソッドは生やさず、ドキュメントで「これは型限定」と明記。

---

## 10. ログパス解決

```rust
// src/sbatch/parse.rs
pub fn resolve_log_path(
    template: &str,
    jobid: u64,
    job_name: Option<&str>,
) -> PathBuf {
    let mut s = template.to_string();
    s = s.replace("%j", &jobid.to_string());
    if let Some(name) = job_name {
        s = s.replace("%x", name);
    }
    PathBuf::from(s)
}
```

- `%j` のみ確実に展開 (spawn 時 jobid 確定済み)。
- `%x` は `cmd.job_name` が `Some` の時のみ。
- `%A`/`%a`/`%u`/`%N` は Phase 2/3 で。
- `resolved.output` は **「展開後の最良推定」** であり、SLURM が実際に書く path とは食い違う可能性 (raw 残しトークンを含む場合) を doc 明記。

---

## 11. KUDPC ツール選定の根拠

| Command | 対象 | コスト | 採否 |
|---|---|---|---|
| `squeue` | キュー上 + 実行中 (active のみ) | 軽 | **採用** (qgroup の補助) |
| `qs` | 実行中のみ + KUDPC 拡張カラム | 軽 | 不採用 (state 種別は squeue と同等情報量) |
| `qgroup` (no flag) | グループ別 aggregate のみ | 軽 | 不採用 (per-job 状態は得られない) |
| `qgroup -l` | per-job 詳細、終了直後の余韻情報を含む | 軽 | **採用** (primary polling source) |
| `sacct` | 過去ジョブ含む全部、accounting DB | **重** (KUDPC マニュアル明記) | **opt-in 限定** (refresh_with_sacct でのみ) |

`refresh()` は qgroup -l → squeue の順、sacct 不使用。終端確定が要る時だけ caller が `refresh_with_sacct()` を 1 回呼ぶ。

### 11.1 parse_qgroup_l の実装メモ

`runner.rs` に追加。`qgroup -l` の正確な出力フォーマット (列順、ヘッダ有無、状態文字列の SLURM との一致度) は **実装フェーズで KUDPC 環境上で実測** して確定する。`scripts/test_sbatch_live.py` (live smoke) で format drift を守る。

---

## 12. pyo3 公開層

`src/py_export/sbatch.rs` を新設、`slurm_async_runner._core.sbatch` で公開。

| Python のパス | 実体 | 公開 |
|---|---|---|
| `slurm_async_runner._core.sbatch` | `py_export/sbatch.rs::inner_module` | `SbatchCmd`, `SbatchManager`, `SbatchJobHandle`, `ResolvedLogPaths`, `SbatchAttachKey`, `SbatchSpawnError` |

```python
import asyncio
from slurm_async_runner._core.sbatch import SbatchCmd, SbatchManager

async def main():
    cmd = SbatchCmd(
        script="/work/job.sh",
        partition="gr19999b",
        time_limit="1:00:00",
        rsc="p=4:c=8:m=2G",
        output="slurm-%j.out",
        error="slurm-%j.err",
        env={"FOO": "bar"},
    )
    manager = SbatchManager(cmd, state_dir="/var/lib/slurm-runner/state")
    handle = await manager.spawn()
    snap = await handle.wait_terminal(poll_interval_secs=30)
    if snap.lifecycle.finished is None:
        snap = await handle.refresh_with_sacct()
    print("exit_code", await handle.exit_code())

asyncio.run(main())
```

**設計判断:**

- async pyfunctions は `pyo3-async-runtimes::tokio::future_into_py` 経由 (tssrun と同流儀)。
- `JobStatus` は `gaussian_job_shared._core.entities.slurm.status.JobStatus` を `PyOnceLock` でキャッシュ参照。
- `SbatchSpawnError` は pyo3 経由で Python 例外型として公開、`SubmittedButUnpersisted` の `e.jobid` 属性アクセス可能。
- **Pyclass Single Owner ルール** (`Cargo.toml:96-119` の Note) に従い、本 crate のみが pyclass 実装を持つ。
- `SbatchJobHandle.watch()` は **Phase 1 では公開せず**、`snapshot()` ベースの polling で十分。Phase 2 で需要が出たら追加。

---

## 13. エラーハンドリング戦略

| レイヤ | エラー型 |
|---|---|
| `SbatchCmd::build_argv` | `anyhow::Error` |
| `parse_submitted_jobid` / `parse_qgroup_l` | `Option<T>` または `anyhow::Error` |
| `resolve_log_path` | 戻り値 `PathBuf` (失敗ケースなし) |
| `JobStateStore<S>` | `anyhow::Error` |
| `SbatchManager::spawn` | **`Result<_, SbatchSpawnError>`** |
| `SbatchManager::attach` / `SbatchJobHandle::*` | `anyhow::Error` |

```rust
// src/sbatch/error.rs
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchSpawnError {
    #[error("sbatch invocation failed (exit={exit_code}): {stdout}")]
    SubmitFailed { exit_code: i32, stdout: String },

    #[error("sbatch stdout did not contain a parseable jobid: {stdout}")]
    JobidParseError { stdout: String },

    /// 副作用 (SLURM submit) 完了後に local store への persist が失敗。
    /// caller は jobid を使って手動 scancel または別経路 persist するべき。
    #[error("sbatch submitted jobid={jobid} but snapshot save failed: {source}")]
    SubmittedButUnpersisted {
        jobid: u64,
        #[source]
        source: anyhow::Error,
    },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
```

`#[non_exhaustive]` で variant 追加を非破壊にする。

---

## 14. テスト戦略

| カテゴリ | 場所 | 何をテストするか |
|---|---|---|
| Unit (`#[cfg(test)] mod tests`) | `cmd.rs` | `build_argv` の各組み合わせ、絶対化、`--export` レンダリング、KUDPC 禁止オプションが含まれない |
| Unit | `parse.rs` | `parse_submitted_jobid` (1行/複数行/警告付き/エラー)、`parse_qgroup_l` (実出力サンプル必要)、`resolve_log_path` (%j/%x/raw 残し) |
| Unit | `store.rs` | `InMemoryStateStore<SbatchJobSnapshot>` save/load/find_by_jobid。tempdir + `FileSystemStateStore<SbatchJobSnapshot>` で `{root}/sbatch/<uuid>.json` の atomic-rename |
| Unit | `handle.rs` | mock dispatcher で refresh の状態遷移、wait_terminal の終端検出、refresh_with_sacct の sacct スキップ条件 |
| Unit | `manager.rs` | mock dispatcher で spawn の各失敗モード (sbatch exit != 0 / jobid parse 失敗 / store.save 失敗 → `SubmittedButUnpersisted`) |
| Integration | `tests/sbatch_integration.rs` | `bash` を sbatch 代替に使った end-to-end (kudpc/SLURM 不要) |
| Live smoke | `scripts/test_sbatch_live.py` | `RUN_LIVE_SBATCH=1` で実 KUDPC 上で投入 → qgroup -l ポーリング → 終了確認 |
| pyo3 | `python/tests/test_sbatch_*.py` | Python 側 API の async 動作、attach、例外属性 |

カバレッジ目標 **80% 以上** (cargo-llvm-cov)、tssrun と同基準。

---

## 15. 実装フェーズ案 (writing-plans に渡す前の概観)

Phase 1 を以下の順序で実装することを想定:

1. **Store 層の generify** (tssrun への影響を先に吸収)
   - `src/store.rs` 新設、`JobSnapshot` / `JobStateStore<S>` / `InMemoryStateStore<S>` / `FileSystemStateStore<S>`
   - `tssrun::store` を `impl JobSnapshot for JobHandleSnapshot` だけに縮小、`kind = "tssrun"`
   - JSON envelope に `kind` フィールドを追加 (lenient legacy fallback で既存ファイルも読める)
   - tssrun 既存テストを generic 版に置換、CHANGELOG に「JSON 形式拡張、path 不変」を記載
2. **`runner.rs` に qgroup -l パーサ追加**
   - `parse_qgroup_l` (実出力サンプルが必要 — 実装時 KUDPC 上で取得)
   - `query_*_via_qgroup` (squeue/sacct と並列の API)
3. **sbatch Spec / Parse 層**
   - `SbatchCmd` + `build_argv`
   - `parse_submitted_jobid` + `resolve_log_path`
4. **sbatch Snapshot / Lifecycle / Store**
   - `SbatchJobSnapshot` + `SbatchLifecycle` + `FinishedInfo` + `ResolvedLogPaths`
   - `impl JobSnapshot for SbatchJobSnapshot` (`kind = "sbatch"`)
5. **sbatch Handle**
   - `SbatchJobHandle` + Arc-wrapped Inner、watch::Sender、refresh_lock
   - `refresh` / `refresh_with_sacct` / `wait_terminal` 実装
   - tssrun lifecycle helper への refactor (`is_running`/`is_finished`/`exit_code` を snapshot 側に降ろす)
6. **sbatch Manager + Error**
   - `SbatchManager` + spawn / attach
   - `SbatchSpawnError`
7. **pyo3 公開層**
   - `src/py_export/sbatch.rs` + `inner_module`
   - Python 例外型、async wrapping、Pyclass Single Owner 遵守
8. **Live smoke + 統合テスト**
   - `scripts/test_sbatch_live.py`
   - `tests/sbatch_integration.rs`
   - `python/tests/test_sbatch_*.py`

---

## 16. 不変条件と落とし穴 (実装者向け)

- **Spec 型に I/O を入れない**。`SbatchCmd::build_argv` 内で stat/glob しない。テスト独立性とランタイム選択の柔軟性が消える。
- **`refresh()` は sacct を絶対に呼ばない**。`refresh_with_sacct()` のみが sacct 経路を持つ。混同するとクラスタ負荷が爆発する。
- **`wait_terminal()` は軽量ループ**。caller が exit_code を欲しければ戻り値後に `refresh_with_sacct()` を **1 回だけ** 呼ぶ。
- **store の `{root}/<uuid>.json` レイアウト**。tssrun と sbatch は同一ディレクトリで物理的に共存し、JSON 内の `kind` フィールド (`"tssrun"` / `"sbatch"`) で論理分離する。subdir は無い。`load(uuid)` で別 kind を引いた場合は silent に `None` を返す。
- **`SubmittedButUnpersisted` は致命的**。SLURM 側はジョブが生きている。`scancel` を打つか、jobid を別経路で記録する責任が caller にある。
- **`resolved_log_paths` は最良推定**。`%x` が job_name なしで指定されている場合 raw が残る → SLURM が実際に書く path と食い違う。
- **`gaussian_job_shared` の `pyo3` feature を絶対に有効化しない**。シンボル衝突で `PyInit__core` が duplicate symbol になる (既存の Pyclass Single Owner ルール参照)。

---

## 17. 参考資料

- [KUDPC: バッチ処理](https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/batch)
- [KUDPC: ジョブ実行のヒント](https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/tips)
- [SLURM: sbatch documentation](https://slurm.schedmd.com/sbatch.html)
- 本リポ `docs/superpowers/specs/2026-05-09-tssrun-wrapper-env-design.md` (tssrun 設計の motivation)
- 本リポ `docs/superpowers/specs/2026-05-10-slurm-vocab-migration-and-pyclass-ownership-design.md` (typed vocab + Pyclass Single Owner ルール)
- 本リポ `docs/architecture.md` §2 (2 軸分割: Spec/Runtime, Sync/Background)
