# 開発ガイド

ローカル開発でのビルド・テスト・ぶつかりがちなハマりどころと、
PR を出すまでの手順をまとめています。
ライブラリの公開 API の使い方は [`README.md`](../README.md)、
コードのレイヤー構造は [`architecture.md`](./architecture.md) を参照。

## 1. ツールチェイン

| ツール | バージョン | 入手元 / 補足 |
|---|---|---|
| Rust | nightly（`rust-toolchain.toml` で固定） | `rustup` でも `mise` でも可。`pyo3` の `nightly` feature が要求するため stable では通りません |
| Python | 3.12 以上 | `pyo3` の `abi3-py312` で固定。3.12 未満で maturin develop すると拒否されます |
| uv | 任意の最新 | <https://docs.astral.sh/uv/> |
| maturin | 1.13.x（`pyproject.toml` で `>=1.13,<2.0`） | `uv sync --all-extras` で自動投入 |
| ruff | 任意 | 同上 |

clippy / rustfmt は nightly の標準コンポーネントです。`rustup` で
`rustup component add rustfmt clippy` を一度叩いておけば足ります。

## 2. 初回セットアップ

```bash
# 1. 依存を解決して仮想環境を作る
uv sync --all-extras

# 2. Rust 拡張をビルドして site-packages に install
uv run maturin develop

# 3. (推奨) pre-commit フックを登録
#    .pre-commit-config.yaml で ruff / cargo fmt / cargo clippy が
#    コミット前に自動で走るようになります（CI と同じ条件）。
uv tool install pre-commit       # or: pipx install pre-commit
pre-commit install

# 4. 何かしら触ったらまず一通り回す
cargo test --lib
uv run pytest python/tests -v
```

`maturin develop` は **Rust 側を編集するたびに必要** です。
`.pyi` 編集だけなら不要。

pre-commit が autofix で書き換えると commit はいったん中断します。
`git add -u && git commit` で再ステージしてください。

## 3. テストレイヤー

### 3.1 Rust 単体（クラスタ不要）

```bash
cargo test --lib                     # 全モジュールの #[cfg(test)] mod tests
cargo test --lib parse_              # 名前で絞り込み
cargo test --lib -- --nocapture      # println! を見たい時
```

`TokioDispatcher` のテストは `srun` の代わりに coreutils の
`true` / `false` / `echo` を使うので SLURM 不要です。

### 3.2 Rust 統合テスト

```bash
cargo test --test tssrun_integration
```

`tests/tssrun_integration.rs` は `bash` を `tssrun` のスタブとして使い、
`spawn → wait → snapshot → attach_file` の一連を実行します。

### 3.3 Python 単体（クラスタ不要）

```bash
uv run pytest python/tests -v
uv run pytest python/tests/test_tssrun.py -v
```

`maturin develop` 後でないと `_core` が見えないので注意。

### 3.4 Python ライブテスト（要 ECCS / kudpc 環境）

opt-in。`RUN_LIVE_TSSRUN=1` を立てた場合のみ実行されます。
詳細は [`setup_test.md`](./setup_test.md)。

```bash
TMPDIR="$HOME/.cache/tssrun-live" \
TSSRUN_LIVE_QUEUE="<group-queue>" \
RUN_LIVE_TSSRUN=1 \
  uv run pytest python/tests/test_tssrun_live.py -v -s
```

スタンドアロン版（pytest を介さない）も同じ環境変数で実行できます:

```bash
TMPDIR="$HOME/.cache/tssrun-live" \
TSSRUN_LIVE_QUEUE="<group-queue>" \
  uv run python scripts/test_tssrun_live.py
```

### 3.5 Lint / Format

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings

uv run ruff check python/
uv run ruff format --check python/
```

CI が `-D warnings` で clippy を回しているので、警告 1 つでも残ると
落ちます。

## 4. `.pyi` 型スタブの再生成

スタブは 2 系統あります。

| ファイル | 種別 | 再生成タイミング |
|---|---|---|
| `python/slurm_async_runner/_core/__init__.pyi` | **自動生成** | top-level の sync な `#[pyfunction]` を増減した時 |
| `python/slurm_async_runner/_core/{manager,runner,tssrun}.pyi` | **手書き** | `#[pyclass]` や async pyfunction を増減した時。サブモジュール内の export はジェネレータの対象外なので必ず手で更新 |

自動生成側のコマンド:

```bash
cargo run --bin stub_gen
uv run ruff format python/        # pyo3-stub-gen の出力は ruff 整形済みではない
```

`stub_gen` バイナリは `cargo run` 時のみビルドされる
`required-features = ["stub_gen"]` 指定です。デフォルトの
`cargo build` には含まれません。

## 5. ローカルでよく踏む地雷

### 5.1 `cargo build` だけ通って `maturin develop` で落ちる

`maturin develop` は `pyo3/extension-module` を有効化してビルドします。
この feature を直接 `[features]` に入れていないのは、
`stub_gen` バイナリが libpython を二重リンクして失敗するからです。
維持してください（`Cargo.toml:71-83` 参照）。

### 5.2 `PyInit__core` の duplicate symbol エラー

`gaussian_job_shared` の `pyo3` feature を有効にしてしまうと、
upstream も `_core` という Python モジュールを生成して衝突します。
`Cargo.toml` の `default-features = false` は外さないこと。

### 5.3 `cargo test --lib` は通るのに `pytest` が ImportError

`maturin develop` を再実行していない可能性が高いです。Rust 側で
`#[pymodule]` の構造を変えた／pyclass を追加した場合は必ず再ビルド。

### 5.4 `cargo test --doc` が落ちる

このリポジトリは doctest をほぼ書いておらず、`tssrun/manager.rs` の
ビルダー説明など意図的に `ignore` した例しかありません。
新しく doctest を書く場合は `feature = "pyo3"` 配下にあるシンボルが
Rust-only ビルド経路でも見えるかを意識してください。

### 5.5 `JobHandle::wait()` の二重呼び出し

`Option<JoinHandle>` の `.take()` 設計上、2 回目は
`Err("not owner of the child / already waited")` になります。
attach 済みハンドルでも同様。テストで再現する場合は
`tssrun_integration.rs` の `spawn_then_wait_then_snapshot_then_attach`
を参考に。

### 5.6 `live_env()` が ECCS 上で `Err` を返す

`uv run maturin develop` で `_core` を最新ビルドに上げ直してください。
`PermissionDenied → None` の丸めは
`src/tssrun/handle.rs::read_live_env_for_pid` で行われており、
古い拡張モジュールを残したままだと旧挙動を引きずります。

### 5.7 別プロセスから `attach_*` が「persisted handle が見つからない」で落ちる

`TssrunManager::new(cmd)` だけだと `InMemoryStateStore`（プロセス内
`HashMap`）が選ばれます。これは spawn したプロセスでしか参照できないため、
別プロセスから attach するには明示的にファイルシステムバックエンドへ
切り替える必要があります:

```rust
// Rust
let mgr = TssrunManager::new(cmd).with_state_dir("/var/lib/slurm-runner");
```

```python
# Python
from slurm_async_runner._core.tssrun import (
    TssrunManager, file_system_state_store,
)
mgr = TssrunManager(cmd, store=file_system_state_store("/var/lib/slurm-runner"))
```

ディレクトリは未作成でも構いません — `FileSystemStateStore` は最初の
`save` で `mkdir -p` を行います。`find_by_jobid` 等もディレクトリ欠損を
`Ok(None)` として扱います（旧実装は `ENOENT` を misleading なエラーとして
surfaced していました）。

### 5.8 `attach_jobid` が成功しない

`salloc:` バナーがパースされて `snapshot.jobid` がセットされてからでないと
ヒットしません。spawn 直後だと `snapshot.jobid` はまだ `None` です。
長期参照には `attach_uuid`（UUID v7 primary key）を使うのが安全です。

## 6. CI と同じ条件をローカルで回す

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --lib

uv sync --all-extras
uv run maturin develop
uv run pytest python/tests -v
uv run ruff check python/
uv run ruff format --check python/
```

これが `.github/workflows/test.yml` と同等です。
CI が落ちたら、まずこの手順をローカルで再現してください。

`pre-commit install` 済みなら、`cargo fmt --check` / `clippy -D warnings`
/ `ruff check` / `ruff format --check` はコミット時に自動で走ります
（`.pre-commit-config.yaml` 参照）。明示的に全ファイルへ走らせる場合は:

```bash
pre-commit run --all-files
pre-commit run cargo-clippy-fix          # 単一フックだけ
```

## 7. ブランチ運用と PR

### 7.1 ブランチ命名

機能別の短いケバブケースで切ります（`tssrun-wrapper-env`、
`docs-structure` など）。

### 7.2 コミットメッセージ

`feat:` / `fix:` / `refactor:` / `docs:` / `test:` / `chore:` /
`perf:` / `ci:` のいずれかをプレフィックスに。

```
feat(tssrun): expose live_env via /proc/<pid>/environ

Reads the child's environment best-effort on Linux. Non-Linux,
already-exited children, and setuid binaries with PR_SET_DUMPABLE
cleared all map to None so the wrapper never breaks on the ECCS
tssrun (which is setuid).
```

### 7.3 PR を出す前のチェックリスト

- [ ] `cargo fmt --check` を通す
- [ ] `cargo clippy --all-targets -- -D warnings` を通す
- [ ] `cargo test --lib` を通す
- [ ] `uv run maturin develop && uv run pytest python/tests -v` を通す
- [ ] `uv run ruff check python/` と `ruff format --check python/` を通す
- [ ] 公開 API を変えたら `README.md` と `CHANGELOG.md` を更新
- [ ] 必要に応じて `docs/` 配下も更新（特にこのファイルか `architecture.md`）
- [ ] `tssrun` 周りに触ったら、可能であれば実機ライブテスト（§3.4）も走らせる

## 8. リリース

`CHANGELOG.md` の `[Unreleased]` セクションに変更を追記し、
適切なタイミングで `[X.Y.Z]` セクションへ昇格させます。
Wheel ビルドと PyPI 公開は `.github/workflows/CI.yml` が tag push 時に
manylinux ホイールと sdist を作成して配布する想定。
（リリース手順の詳細は CI.yml を参照してください。）
