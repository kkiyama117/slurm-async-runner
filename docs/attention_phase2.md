# Phase 2 への引き継ぎ — 注意事項とガイダンス

> **Audience:** Phase 2 を別ブランチで担当する次の Agent / 人間コミッター。
> **Phase 1 の成果物:** `crate::sbatch` (Rust) + `slurm_async_runner._slurm_async_runner_core.sbatch` (Python) の **fire-and-forget + attach 後追い監視**。
> **Phase 1 の merge 先:** `develop` ブランチ (origin に未 push)。

---

## 0. まず読むべきもの

Phase 2 を始める前に、必ず以下を上から順に読む。読み飛ばすと過去判断と矛盾する設計を提案して手戻りする。

1. `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` — Phase 1 の設計仕様
   - `1.2 明示的に Phase 1 外` (line 41 付近) と `4.2 Phase 2 で追加予定` (line 165 付近) に Phase 2 の追加候補が列挙されている
2. `docs/superpowers/plans/2026-05-10-sbatch-module.md` — Phase 1 の実装計画
   - `Out of Scope (Phase 2 — separate plan)` (line 2921) — Phase 2 で扱う 8 項目
3. `CHANGELOG.md` の `[Unreleased]` — Phase 1 で導入された API・破壊的変更の正確なリスト
4. `docs/architecture.md` / `docs/code-map.md` — リポジトリ全体の architecture・モジュール対応表

**ブランチ運用:** `develop` から切り出して作業する。`main` から切るのは禁止 (Phase 1 の差分が main にまだないため)。

---

## 1. Phase 2 で取り組む内容 (Out of Scope from Phase 1)

優先度は概ね上から順 (依存関係・実装コスト・ユーザニーズ複合)。

| # | 項目 | 概要 | 想定難度 |
|---|---|---|---|
| 1 | sacct `ExitCode` parsing | `parse_sacct` を拡張し `FinishedInfo.exit_code` を実値で埋める | 中 |
| 2 | `--array` (`-a`) + array task per-snapshot | 配列ジョブ、`%A`/`%a`/`%u`/`%N` の log path 解決、handle/store 拡張 | 高 |
| 3 | `--dependency` (`-d`) typed enum | `after[ok\|notok\|any]:N,...` を typed に表現 | 低-中 |
| 4 | `--mail-user` / `--mail-type` | typed enum で `MailType` を導入 | 低 |
| 5 | `--no-requeue`, `--signal`, `--comment` | フラグ/単純値の追加 | 低 |
| 6 | `sbatch --wait` based `run()` | 同期実行 API。`spawn → wait_terminal` の素直な合成 | 中 |
| 7 | Log-file `tail` / `read` ergonomics | handle 上で出力ログを読む API | 低-中 |
| 8 | Common `JobHandleCommon` trait | tssrun + sbatch の共通抽象 (上が出揃ってから) | 中 |

---

## 2. Phase 1 で固まった "触ると壊れる" 不変条件

これらは **on-disk フォーマットや公開 API に焼き付いている**。変更したい場合はマイグレーション計画必須。

### 2.1 `JobSnapshot::kind()` の値は永続化されている

- `"tssrun"` / `"sbatch"` の文字列は `{root}/<uuid>.json` の `kind` フィールドに書き出されている
- 既存ユーザの state ディレクトリを silent break するので **絶対に rename しない**
- 新しい snapshot 種別を追加するときだけ新文字列 (`"sbatch_array"` 等) を採用、既存は維持

### 2.2 `FileSystemStateStore<S>` の scan は **kind 不一致を silent skip**

- `list()` / `find_by_jobid()` は他種 snapshot を読み飛ばす設計
- 既存 tssrun と sbatch は同一 root に共存 (`docs/superpowers/specs/2026-05-10-sbatch-module-design.md` 5 章)
- Phase 2 で新 kind を追加しても、この skip ロジックは正しく動く (kind 文字列さえ一意なら)

### 2.3 SbatchJobSnapshot の serde 後方互換

- 新フィールドを足すときは必ず `#[serde(default)]` を付ける
- `array_jobid: Option<u64>`, `array_task_id: Option<u32>` のような `Option` + `default` で過去ファイルがロード可能に保つ
- フィールド削除や型変更は **migration なしには不可**

### 2.4 `SbatchAttachKey` / `attach_*` の入口仕様

- `attach_uuid` / `attach_jobid` / `attach_file` は public API
- `attach_file` は kind 判別子を peek して不一致なら拒否する (Phase 1 final-review HIGH-2 で追加)
- 新 attach 経路を増やすときも同じガード必須

### 2.5 `JobState::is_running()` / `is_terminal()` は同期する

- `src/entities/slurm/status.rs` で 11 種類の terminal variant を `is_terminal` に列挙
- 新 variant (例: `Reservation`) を追加する場合は **両方の関数を同時に更新**
- KUDPC トークン (`RUN`/`QUE`/`CMP` 短縮形) も `parse` に登録すること (Phase 1 で `RUN`/`QUE`/`CMP` 対応済み)

### 2.6 KUDPC マニュアルが禁止するオプションは **typed フィールドにしない**

`--nodes`, `--ntasks`, `--cpus-per-task`, `--mem`, `--gpus`, `--exclusive` 等は **Phase 2 でも追加禁止**。`--rsc` がリソース統括するので意味も不要 (spec §4.1)。
誤指定の経路を typing で塞ぐのが Phase 1 の方針。Phase 2 もこれを継承する。

---

## 3. Phase 1 で確立されたパターン (Phase 2 で踏襲する)

### 3.1 Spec / Runtime 二軸

- `SbatchCmd` (Spec) — public フィールドの builder。検証は `build_argv` で
- `SbatchJobHandle` / `SbatchJobSnapshot` (Runtime) — Arc + watch::Sender + lock-free 読み取り
- `SbatchManager` (Coordinator) — Spec を受け取り Runtime を返す
- 新オプションは **まず Spec に**、必要なら snapshot にも追加

### 3.2 Pyclass Single Owner ルール

- pyo3 binding は `src/py_export/sbatch.rs` を参照
- `Py<...>` で wrap、`from_py_object` で Rust に渡す
- 1 つの Rust struct を **2 個以上の pyclass が共有しない** (clone semantics 不明確を避ける)

### 3.3 DynJobDispatcher facade

- `JobDispatcher` は RPITIT を使うため `dyn`-incompatible
- `Arc<dyn DynJobDispatcher>` で型消去するときは **必ず `into_dyn(...)` を経由**
- 新メソッドを `JobDispatcher` に足す → `DynJobDispatcher` / `DynDispatcherAdapter` / `DynView` の 3 箇所も同時更新
- blanket impl は **追加しない** (Phase 1 で E0034 ambiguity 多発、`into_dyn` は明示構築のまま維持)

### 3.4 Mock dispatcher pattern (Phase 2 のテストでも使う)

`src/runner.rs` の `#[cfg(test)] mod tests` 内に揃っている。再利用すること:

- `MoveDispatcher` — 単一の `MockCapture` を 1 回返す。期待入力検証
- `PanicDispatcher` — 呼ばれたら panic。「呼ばれないこと」の検証
- `CannedDispatcher` — 固定文字列を返す
- `MockCapture` — `Capture` trait を満たす test fake

新しい subprocess 経路 (例: `sbatch --wait` の long-running pipe) を足すときは、これらに合わせた fake を **新規導入せずまず流用** を検討。

### 3.5 lock-free snapshot

- `tokio::sync::watch::Sender` で snapshot を broadcast、getter は all lock-free
- `refresh_lock: Mutex<()>` で並行 refresh の単一化のみ。読み取り側は触らない
- Phase 2 で新しい getter を足すときは **同じ pattern を維持** (`async` にしない、`Mutex` を持たせない)

---

## 4. Phase 1 で見つかった "計画見落とし" と対処パターン

Phase 2 でも同種の見落としが起きやすい。特に **「計画には書いてあるが実コード上の前提が成立していない」** パターン。

| Phase 1 で起きた事例 | 教訓 |
|---|---|
| 計画では `JobState::is_running` 存在前提だったが未定義 | trait/method の存在は **grep で実存確認** してから計画に書く |
| `JobState::parse` が KUDPC `RUN`/`QUE`/`CMP` を受けない | 入力データのバリエーションを **実物で確認**、テストで再現 |
| `pub type JobStateStore = dyn ...` が `py_export/tssrun.rs` で E0404 を起こす | 公開 alias 変更は **`grep -r` で全 usage 走査** が必須 |
| RPITIT trait の `dyn` 化で blanket impl が E0034 ambiguity | dyn-safe にしたいなら **専用 wrapper trait** + 明示 constructor |
| `attach_file` が kind チェックを忘れていた | 公開 attach 経路は **必ず kind peek + 拒否テスト** |
| `InMemoryStateStore` が `std::sync::Mutex` を `async fn` 内で持っていた | async 文脈で持つ lock は **`tokio::sync::Mutex`** |

**運用ルール:** Phase 2 計画書のレビュー時には、上記 6 つを **チェックリストとして必ず照合**する。

---

## 5. 既知の Phase 2 着手対象別 詳細メモ

### 5.1 sacct `ExitCode` parser (最優先)

- 該当箇所: `src/sbatch/handle.rs:281` 付近 — 「surface as None for now. Phase 2 may extend the parser.」
- sacct 出力例: `ExitCode` カラムは `0:0` (exit_code:signal)、`139:11`、`0:9` などの形
- KUDPC sacct がライセンス的に重い → 既に `refresh_with_sacct` は **opt-in** に分離済み。`refresh` / `wait_terminal` には **絶対に sacct を入れない**
- 実装後は `SbatchLifecycle::exit_code` / `SbatchJobSnapshot::exit_code` / `SbatchJobHandle::exit_code` の **3 箇所の Phase 1 limitation doc-comment を削除** すること
- `python/.../sbatch.pyi` 内の docstring も同期更新

### 5.2 配列ジョブ (`--array` `-a`)

仕様: `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` §1.2, §4.2, §6 (line 373-410 付近) を参照。

- snapshot に **`array_jobid: Option<u64>`** と **`array_task_id: Option<u32>`** を追加
- log path resolver (`src/sbatch/parse.rs::resolve_log_path`) を `%A`/`%a`/`%u`/`%N` に拡張。**未知の `%`-token は raw のまま残す** 既存戦略を維持
- handle 種別を分けるか単一化するかは **設計判断が要る**。spec §6 で言及されているが Phase 1 段階では結論未定。Phase 2 計画前にブレストすべし
- 新しい kind 文字列 (例 `"sbatch_array"`) を導入するか、既存 `"sbatch"` のままにするかも要決定

### 5.3 `sbatch --wait` based `run()`

- Phase 1 の `spawn → wait_terminal` パイプを **置換しない**。additive で `run()` を足す
- `--wait` は sbatch プロセスがジョブ終了まで block する KUDPC の仕様。コネクション切断 (timeout) で残骸ジョブが残るリスクあり → **timeout / cancel-on-drop の挙動を明記**
- `Drop` で auto-cancel するか、明示的 `cancel()` API か、Phase 2 計画段階で要決定

### 5.4 `JobHandleCommon` trait

- Phase 1 では **意図的に抽象化を保留** (spec §1.2)。先に concrete commonality を観察してから
- やるなら tssrun と sbatch の **getter 名の収斂を確認** してから (`is_running`, `is_finished`, `exit_code`, `jobid`, `uuid`)
- ただし sbatch の log path / sacct opt-in / array task は tssrun に対応物がない → trait に含めない

### 5.5 `--export` 値のバリデーション

- Phase 1 は `SbatchCmd.export` を `Option<Vec<(String, String)>>` で受けるが、値の **`,` `=` をエスケープ/拒否しない**
- sbatch CLI は positional に `--export=A=1,B=2` でパースするので、`A=1,2` のようなカンマ含み値は破壊される
- Phase 2 で **`build_argv` 内で値検証 (拒否 or escape)** を追加

### 5.6 DRY: `absolutize` の重複

- `src/tssrun/cmd.rs:97` と `src/sbatch/cmd.rs:96` に同名関数が重複
- Phase 2 で **`src/util/path.rs` (新規)** などに移して `pub(crate) fn absolutize` として共有
- `src/manager.rs:62` も類似ロジック (in-line `std::path::absolute`) なので同時に集約

---

## 6. テスト規律 (Phase 2 でも厳守)

### 6.1 TDD

- 新 API は **必ず test 先行** (`subagent-driven-development` skill 前提)
- `#[cfg(test)] mod tests` を同ファイル内に置く (Phase 1 全モジュールがこの構造)
- 統合テストは `tests/` 配下、live test は `scripts/test_sbatch_live.py` パターンを踏襲

### 6.2 必須コマンド

実装の各ステップで、以下を全て pass させてから commit:

```bash
cargo test --lib --features pyo3
cargo clippy --all-targets --features pyo3 -- -D warnings
cargo fmt --all --check
uv run pytest python/tests
```

### 6.3 Live smoke

KUDPC 環境で:

```bash
SBATCH_LIVE_BIN=/path/to/sbatch \
SBATCH_LIVE_QUEUE=<queue> \
SBATCH_LIVE_RSC=<resource> \
uv run python scripts/test_sbatch_live.py
```

`scripts/test_sbatch_live.py` を **Phase 2 の新機能ごとに smoke test path を 1 つ追加** する慣習を維持。

---

## 7. コミュニケーション・運用

### 7.1 Phase 2 専用ブランチ

- ブランチ名は `sbatch-phase2` 系を推奨。複数機能を別ブランチに分ける場合は `sbatch-phase2/<feature>`
- `develop` から切り出し、PR は `develop` に向ける (本リポジトリは `develop` を統合ブランチとして運用開始)

### 7.2 計画書/設計書

- Phase 2 全体は **1 つの spec → 複数の plan に分割** が望ましい (上 8 項目を 1 plan にまとめると過大)
- spec/plan の命名: `docs/superpowers/specs/YYYY-MM-DD-sbatch-phase2-<topic>-design.md` / 同 plans
- Phase 1 の spec/plan を **footnote で参照** すれば前提共有が短く済む

### 7.3 Phase 1 で commit した sbatch-module-backup ブランチ

- `sbatch-module-backup` (`cedbedb`) — rebase 前の本来の sbatch-module。**Phase 2 では基本不要**、削除可
- ただし Phase 1 の merge 結果に問題が見つかったときの roll-back 用に **develop が origin に push されるまでは残す**

---

## 8. クイック・チェックリスト (Phase 2 PR 提出前)

- [ ] `develop` から切り出している
- [ ] kind 文字列の追加/変更は migration 計画とセット
- [ ] 新 snapshot フィールドに `#[serde(default)]`
- [ ] 新 `JobDispatcher` メソッドは `DynJobDispatcher` / `DynDispatcherAdapter` / `DynView` も更新
- [ ] 新 `JobState` variant は `is_running` / `is_terminal` / `parse` 三方更新
- [ ] sacct 呼び出しは opt-in (`refresh` には絶対入れない)
- [ ] 公開 attach 経路には kind peek 入っている
- [ ] async 内 lock は `tokio::sync::Mutex`
- [ ] CHANGELOG `[Unreleased]` 更新
- [ ] `python/.../*.pyi` 同期 + Phase 1 limitation doc 削除 (該当機能のみ)
- [ ] `cargo test` / clippy / fmt / pytest 全 pass
- [ ] Live smoke (KUDPC で実行可能なら)

---

## 9. 連絡先 / リンク

- Phase 1 設計: `docs/superpowers/specs/2026-05-10-sbatch-module-design.md`
- Phase 1 計画: `docs/superpowers/plans/2026-05-10-sbatch-module.md`
- Phase 1 backup: `sbatch-module-backup` ブランチ (`cedbedb`)
- 統合 baseline: `develop` ブランチ (`8cf6622`)
