# slurm-async-runner ドキュメント索引

このディレクトリは、`slurm-async-runner` を初めて触るコミッター向けに、
**コードを読み始める前にリポジトリ全体の地図を渡す**ことを目的とした
ドキュメント群です。クラスター運用ガイドや個別の設計仕様は、
それぞれ別ファイルに分かれています。

## 想定読者

- このリポジトリへ初めてコミットしようとしている開発者
- 既存実装に手を入れる前に、責務分割と依存関係を把握したい人
- pyo3 + Tokio の async 連携が「どこからどう走っているのか」を理解したい人

> ライブラリの**使い方**だけが知りたい場合は、リポジトリルートの
> [`README.md`](../README.md) の方が手短です。

## 目次

| ファイル | 内容 |
|---|---|
| [architecture.md](./architecture.md) | レイヤー設計（spec / runtime / query / tssrun + sbatch サブシステム + Phase 3 の `JobHandleCommon` 跨 backend 抽象 + `JobStateStore` 永続化抽象 + UUID v7 primary key）と、なぜこの分割なのか |
| [code-map.md](./code-map.md) | ディレクトリ・ファイル単位での役割マップ。「この機能はどこにある?」を逆引きする用 |
| [process-flow.md](./process-flow.md) | 主要ワークフロー（`run_job` / `query_job_states_batch` / tssrun spawn / attach の 4 種 `AttachKey`）のシーケンス |
| [development.md](./development.md) | ビルド・テスト・pre-commit / CI・stub 再生成・PR 手順、ありがちなハマりどころ |
| [setup_test.md](./setup_test.md) | ライブ tssrun スモークテストの運用者向けセットアップ（クラスター側手順） |
| [attention_phase2.md](./attention_phase2.md) | Phase 1 → Phase 2 ハンドオーバ。`crate::sbatch` の不変条件と Phase 2 で追加された 8 項目の roadmap |
| [error.md](./error.md) | 過去 cross-cdylib pyo3 type identity 問題（PR #5 で解消済みの歴史メモ） |

## 設計仕様（履歴）

`docs/superpowers/` 以下に、機能追加時の設計ドラフトと実行プランが
時系列で残されています。新機能を足す際の**過去の意思決定の根拠**を
追いたいときに参照してください。

| パス | 内容 |
|---|---|
| [`superpowers/specs/2026-05-08-slurm-gaussian-migration-design.md`](./superpowers/specs/2026-05-08-slurm-gaussian-migration-design.md) | Python 版 `slurm-async-runner` を Rust + pyo3 へ移植したときの設計ドラフト |
| [`superpowers/specs/2026-05-09-tssrun-wrapper-env-design.md`](./superpowers/specs/2026-05-09-tssrun-wrapper-env-design.md) | `tssrun` サブシステム（背景実行 + 環境変数スナップショット + クロスプロセス attach）の設計仕様 |
| [`superpowers/plans/2026-05-09-tssrun-wrapper-env.md`](./superpowers/plans/2026-05-09-tssrun-wrapper-env.md) | 上記設計の実装プラン |
| [`superpowers/specs/2026-05-09-merge-tssrun-resourcespec-design.md`](./superpowers/specs/2026-05-09-merge-tssrun-resourcespec-design.md) | `tssrun::Resource` を `crate::entities::slurm::ResourceSpec` に統合する設計 |
| [`superpowers/specs/2026-05-10-slurm-vocab-migration-and-pyclass-ownership-design.md`](./superpowers/specs/2026-05-10-slurm-vocab-migration-and-pyclass-ownership-design.md) | PR #5: SLURM 語彙の in-tree 移管と Pyclass Single Owner ルール |
| [`superpowers/specs/2026-05-10-sbatch-module-design.md`](./superpowers/specs/2026-05-10-sbatch-module-design.md) | **Phase 1**: `crate::sbatch` モジュールの最初の設計（spawn / attach / refresh） |
| [`superpowers/plans/2026-05-10-sbatch-module.md`](./superpowers/plans/2026-05-10-sbatch-module.md) | Phase 1 実装プラン |
| [`superpowers/specs/2026-05-10-sbatch-phase2-design.md`](./superpowers/specs/2026-05-10-sbatch-phase2-design.md) | **Phase 2**: sbatch の機能拡張設計（sacct exit code / `--array` / `--dependency` / `--mail-*` / `--signal` / `--no-requeue` / `--comment` / `run()` / `cancel()`） |
| `superpowers/plans/2026-05-10-sbatch-phase2-p1.md` … `2026-05-11-sbatch-phase2-p6.md` | Phase 2 の P1〜P6 実装プラン |
| [`superpowers/specs/2026-05-11-sbatch-phase3-design.md`](./superpowers/specs/2026-05-11-sbatch-phase3-design.md) | **Phase 3**: `JobHandleCommon` trait 抽出の設計 (tssrun rename + 跨 backend trait + dyn facade + Python Protocol) |
| `superpowers/plans/2026-05-11-sbatch-phase3-p1.md` … `2026-05-11-sbatch-phase3-p4.md` | Phase 3 の P1〜P4 実装プラン (P5 = Protocol parity は PR #7 review feedback 由来でプラン外追加) |

> 仕様書は**書かれた時点での意思決定**を記録したスナップショットです。
> 現状コードと食い違うことがあるので、最新の挙動は `src/` を正、
> 仕様書は「なぜそう決めたか」の参照として読んでください。

## 推奨される読み順

1. リポジトリルートの [`README.md`](../README.md) で公開 API のシグネチャを眺める
2. [`architecture.md`](./architecture.md) でレイヤー分割と責務を把握
3. [`code-map.md`](./code-map.md) で「どのファイルに何があるか」を頭に入れる
4. [`process-flow.md`](./process-flow.md) で実行時のデータの流れを追う
5. [`development.md`](./development.md) でローカル開発フローに合わせる
6. ライブ実機テストを動かす予定があれば [`setup_test.md`](./setup_test.md)
