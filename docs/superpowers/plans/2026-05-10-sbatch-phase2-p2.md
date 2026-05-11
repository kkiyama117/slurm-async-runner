# sbatch Phase 2 P2 — `--dependency` + `--mail-*` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire SLURM `--dependency` (`-d`), `--mail-user`, `--mail-type` flags into `SbatchCmd::build_argv` while reusing existing `entities::slurm::sbatch_options` types and adding only the missing `Display` / `as_slurm_str` impls.

**Architecture:** Three new public fields on `SbatchCmd` (`dependency`, `mail_user`, `mail_types`), each defaulting to `None`. The argv emitter prints them in a stable order between the existing `--export` block and `--no-requeue`. Vocab types come exclusively from `crate::entities::slurm::sbatch_options::*` (spec §2.1). The only entities-side additions are: `MailType::as_slurm_str() -> &'static str`, `impl Display for MailType`, and `impl Display for MailTypeInput` (comma-separated). Python wrappers (`PySlurmDependency`, `PyMailTypeInput`) already exist; we only thread them through `PySbatchCmd::new` as new optional kwargs.

**Tech Stack:** Rust 2021, `pyo3` (feature gate `pyo3`), `pyo3-async-runtimes` for async, `thiserror`, `serde`, existing `entities::slurm::sbatch_options::{SlurmDependency, MailType, MailTypeInput, MailAddress}`.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/entities/slurm/sbatch_options.rs` | Add `MailType::as_slurm_str` + `impl Display` for both `MailType` and `MailTypeInput`. No struct changes. |
| `src/sbatch/cmd.rs` | Add three public fields (`dependency`, `mail_user`, `mail_types`) + default in `new()` + argv emission in `build_argv()` + unit tests. |
| `src/py_export/sbatch.rs` | Add three new kwargs to `PySbatchCmd::new`: `dependency: Option<PySlurmDependency>`, `mail_user: Option<String>`, `mail_types: Option<PyMailTypeInput>`. |
| `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi` | Add the three kwargs to `SbatchCmd.__init__`. |
| `python/tests/test_sbatch.py` | Add three smoke tests covering each kwarg (dependency parsing, mail_user only, mail_types via MailTypeInput). |
| `CHANGELOG.md` | Append `### Added (Phase 2 P2)` block under `[Unreleased]`. |

No new source files are created. The plan adds **≈170 LOC of source + ≈70 LOC of tests**.

---

## Task 1: `MailType::as_slurm_str` + `impl Display for MailType`

**Files:**
- Modify: `src/entities/slurm/sbatch_options.rs:77-102`

**Why:** `entities` currently has `MailType` as a plain enum with only `TryFrom<&str>`. We need a forward conversion (`MailType -> &'static str`) for the upcoming `MailTypeInput::Display` and for any future caller that wants to render a single mail type. `as_slurm_str` returns the exact uppercase Slurm token (`BEGIN` / `END` / `FAIL` / `REQUEUE` / `ALL`).

**Constraints:**
- New method must live in `entities`. The spec §2.1 forbids redefining vocab in `crate::sbatch::*`.
- The `Display` output MUST match `as_slurm_str` byte-for-byte so `format!("{}", mt)` and `mt.as_slurm_str()` are interchangeable.

- [ ] **Step 1: Write the failing test**

Append to the bottom of `src/entities/slurm/sbatch_options.rs` (the file currently has no test module — create one):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mail_type_as_slurm_str_matches_kudpc_tokens() {
        assert_eq!(MailType::BEGIN.as_slurm_str(), "BEGIN");
        assert_eq!(MailType::END.as_slurm_str(), "END");
        assert_eq!(MailType::FAIL.as_slurm_str(), "FAIL");
        assert_eq!(MailType::REQUEUE.as_slurm_str(), "REQUEUE");
        assert_eq!(MailType::ALL.as_slurm_str(), "ALL");
    }

    #[test]
    fn mail_type_display_matches_as_slurm_str() {
        for mt in [
            MailType::BEGIN,
            MailType::END,
            MailType::FAIL,
            MailType::REQUEUE,
            MailType::ALL,
        ] {
            assert_eq!(mt.to_string(), mt.as_slurm_str());
        }
    }

    #[test]
    fn mail_type_display_roundtrips_through_try_from() {
        for mt in [
            MailType::BEGIN,
            MailType::END,
            MailType::FAIL,
            MailType::REQUEUE,
            MailType::ALL,
        ] {
            let rendered = mt.to_string();
            let parsed = MailType::try_from(rendered.as_str()).unwrap();
            assert_eq!(parsed, mt);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --features pyo3 mail_type_as_slurm_str_matches_kudpc_tokens`

Expected: FAIL with `error[E0599]: no method named 'as_slurm_str' found for enum 'MailType'`.

- [ ] **Step 3: Add `as_slurm_str` method and `Display` impl**

Insert immediately after the existing `impl TryFrom<&str> for MailType { ... }` block (at `src/entities/slurm/sbatch_options.rs:102`):

```rust
impl MailType {
    /// Render this mail type as the canonical uppercase Slurm token
    /// (`BEGIN`, `END`, `FAIL`, `REQUEUE`, `ALL`).
    ///
    /// This is the exact string accepted by sbatch's `--mail-type` flag and
    /// produced by sacct, so `Display` is implemented in terms of this method.
    pub const fn as_slurm_str(self) -> &'static str {
        match self {
            MailType::BEGIN => "BEGIN",
            MailType::END => "END",
            MailType::FAIL => "FAIL",
            MailType::REQUEUE => "REQUEUE",
            MailType::ALL => "ALL",
        }
    }
}

impl std::fmt::Display for MailType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_slurm_str())
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib --features pyo3 -- mail_type`

Expected: 3 passed (the three tests above).

- [ ] **Step 5: Run full lints**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`

Expected: 0 warnings, no formatting diff.

- [ ] **Step 6: Commit**

```bash
git add src/entities/slurm/sbatch_options.rs
git commit -m "feat(entities): add MailType::as_slurm_str + Display impl"
```

---

## Task 2: `impl Display for MailTypeInput`

**Files:**
- Modify: `src/entities/slurm/sbatch_options.rs:104-115`

**Why:** Slurm's `--mail-type` accepts a comma-separated list. `MailTypeInput` already round-trips from `String` via `TryFrom`, but lacks the reverse direction. `Display` produces the canonical comma-joined form so `SbatchCmd::build_argv` can render it directly without poking at the inner `Vec<MailType>`.

**Constraints:**
- Must use `MailType::as_slurm_str` from Task 1 (avoid duplicating the match) — `Display` for `MailType` already calls `as_slurm_str`, so this impl reuses it via `Display::fmt`.
- Empty `MailTypeInput` is unreachable through `TryFrom<String>` (an empty string parse-fails). Render the empty case as the empty string just to keep the impl total.

- [ ] **Step 1: Write the failing test**

Append to the existing `#[cfg(test)] mod tests` block in `src/entities/slurm/sbatch_options.rs` (the one created in Task 1):

```rust
    #[test]
    fn mail_type_input_display_joins_with_commas() {
        let mti = MailTypeInput::try_from("BEGIN,END".to_string()).unwrap();
        assert_eq!(mti.to_string(), "BEGIN,END");
    }

    #[test]
    fn mail_type_input_display_single_value() {
        let mti = MailTypeInput::try_from("FAIL".to_string()).unwrap();
        assert_eq!(mti.to_string(), "FAIL");
    }

    #[test]
    fn mail_type_input_display_preserves_order() {
        let mti = MailTypeInput::try_from("END,BEGIN,FAIL".to_string()).unwrap();
        assert_eq!(mti.to_string(), "END,BEGIN,FAIL");
    }

    #[test]
    fn mail_type_input_display_roundtrips() {
        let original = MailTypeInput::try_from("ALL".to_string()).unwrap();
        let rendered = original.to_string();
        let parsed = MailTypeInput::try_from(rendered).unwrap();
        assert_eq!(parsed, original);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --features pyo3 mail_type_input_display_joins_with_commas`

Expected: FAIL — `MailTypeInput` does not yet implement `Display`. The exact compiler diagnostic is `error[E0277]: 'MailTypeInput' doesn't implement 'std::fmt::Display'`.

- [ ] **Step 3: Add `Display` impl for `MailTypeInput`**

Insert immediately after the existing `impl TryFrom<String> for MailTypeInput { ... }` block (around `src/entities/slurm/sbatch_options.rs:115`):

```rust
impl std::fmt::Display for MailTypeInput {
    /// Comma-separated rendering matching Slurm's `--mail-type` syntax.
    /// Round-trips with [`TryFrom<String>`] for any non-empty value.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for mt in &self.0 {
            if !first {
                f.write_str(",")?;
            }
            first = false;
            std::fmt::Display::fmt(mt, f)?;
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib --features pyo3 -- mail_type_input_display`

Expected: 4 passed.

- [ ] **Step 5: Run full lints**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`

Expected: 0 warnings, no formatting diff.

- [ ] **Step 6: Commit**

```bash
git add src/entities/slurm/sbatch_options.rs
git commit -m "feat(entities): add Display impl for MailTypeInput"
```

---

## Task 3: `SbatchCmd::dependency` field + `-d` argv emission

**Files:**
- Modify: `src/sbatch/cmd.rs:13` (add import)
- Modify: `src/sbatch/cmd.rs:17-40` (struct definition)
- Modify: `src/sbatch/cmd.rs:43-59` (`new()`)
- Modify: `src/sbatch/cmd.rs:61-109` (`build_argv()`)
- Modify: `src/sbatch/cmd.rs:127-253` (test module)

**Why:** P2 wires `--dependency`. `SlurmDependency` already exists with `FromStr` / `Display` / serde, so the `SbatchCmd` field is `Option<SlurmDependency>` and `build_argv` emits `["-d", dep.to_string()]` when present. We follow the existing `if let Some(...)` pattern used for `partition`, `time_limit`, etc.

**Constraints:**
- Place the argv emission **between the `--export` block (currently at lines 96-98) and the `--no-requeue` block (currently at lines 99-101)** so it sits with the other CLI-only Phase 2 flags.
- Field order in the struct: insert `dependency` **immediately after `chdir`** so spec-shaped fields (job_name / partition / time / rsc / output / error / chdir / dependency) cluster, and runtime-only fields (env / no_requeue / comment) follow.

- [ ] **Step 1: Write the failing tests**

Append to the existing `#[cfg(test)] mod tests` block in `src/sbatch/cmd.rs` (after the `comment_omitted_when_none` test at line ~248):

```rust
    #[test]
    fn dependency_emits_dash_d_with_display_form() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.dependency = Some("afterok:200".parse().unwrap());
        let argv = cmd.build_argv().unwrap();
        let i = argv.iter().position(|a| a == "-d").expect("-d present");
        assert_eq!(argv[i + 1], "afterok:200");
    }

    #[test]
    fn dependency_with_and_join_emits_comma_form() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.dependency = Some("afterok:200,afterany:201".parse().unwrap());
        let argv = cmd.build_argv().unwrap();
        let i = argv.iter().position(|a| a == "-d").expect("-d present");
        assert_eq!(argv[i + 1], "afterok:200,afterany:201");
    }

    #[test]
    fn dependency_with_or_join_emits_question_form() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.dependency = Some("afterok:200?afterany:201".parse().unwrap());
        let argv = cmd.build_argv().unwrap();
        let i = argv.iter().position(|a| a == "-d").expect("-d present");
        assert_eq!(argv[i + 1], "afterok:200?afterany:201");
    }

    #[test]
    fn dependency_omitted_when_none() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a == "-d"));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib --features pyo3 dependency_emits_dash_d_with_display_form`

Expected: FAIL with `error[E0609]: no field 'dependency' on type 'SbatchCmd'`.

- [ ] **Step 3: Add the `dependency` field, default, and argv emission**

Edit the existing `use` line at `src/sbatch/cmd.rs:13`:

```rust
use crate::entities::slurm::{JobPartition, JobTimeLimit, ResourceSpec, SlurmDependency};
```

Add the field to `SbatchCmd` immediately after `pub chdir: Option<PathBuf>,` (around line 28-29):

```rust
    pub chdir: Option<PathBuf>,

    /// `--dependency` (`-d`) spec. When `Some`, emitted as `["-d", dep.to_string()]`
    /// (e.g. `["-d", "afterok:200,afterany:201"]`).
    pub dependency: Option<SlurmDependency>,

    pub env: HashMap<String, String>,
```

Add the default in `new()` (around line 52-53), keeping order aligned with the struct:

```rust
            chdir: None,
            dependency: None,
            env: HashMap::new(),
```

Add the argv emission inside `build_argv()` between the `--export` block and `--no-requeue`. The existing block at lines ~96-101 is:

```rust
        if !self.env.is_empty() {
            argv.push(format!("--export={}", render_export(&self.env)));
        }
        if self.no_requeue {
            argv.push("--no-requeue".to_string());
        }
```

Replace with:

```rust
        if !self.env.is_empty() {
            argv.push(format!("--export={}", render_export(&self.env)));
        }
        if let Some(dep) = &self.dependency {
            argv.push("-d".to_string());
            argv.push(dep.to_string());
        }
        if self.no_requeue {
            argv.push("--no-requeue".to_string());
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib --features pyo3 -- dependency`

Expected: 4 passed.

- [ ] **Step 5: Run full lints + full sbatch::cmd test suite**

Run: `cargo test --lib --features pyo3 sbatch::cmd && cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`

Expected: all sbatch::cmd tests pass; 0 clippy warnings; no formatting diff. The pre-existing `full_flags_cpu_variant_argv_layout` test does NOT exercise dependency, so it should still pass byte-for-byte.

- [ ] **Step 6: Commit**

```bash
git add src/sbatch/cmd.rs
git commit -m "feat(sbatch): wire --dependency via SlurmDependency entity"
```

---

## Task 4: `SbatchCmd::mail_user` + `mail_types` fields + `--mail-*` argv emission

**Files:**
- Modify: `src/sbatch/cmd.rs:13` (extend import)
- Modify: `src/sbatch/cmd.rs:17-40` (struct definition)
- Modify: `src/sbatch/cmd.rs:43-59` (`new()`)
- Modify: `src/sbatch/cmd.rs:61-109` (`build_argv()`)
- Modify: `src/sbatch/cmd.rs:127-253` (test module)

**Why:** P2 wires `--mail-user` and `--mail-type`. We use the entities-side `MailAddress = String` alias and `MailTypeInput` (now with `Display` from Task 2). When only `mail_types` is set without `mail_user`, sbatch falls back to the `$MAILUSER`/`$USER` env (KUDPC default), so this is allowed but logged later (out of scope here — log emission is a Phase 3 concern; here we just emit the argv).

**Constraints:**
- Argv emission order: **after** `dependency` (Task 3) and **before** `--no-requeue` (Phase 1). Concretely: insert between `dependency` and `no_requeue`. This keeps Phase 2 P2 flags grouped.
- `mail_user` accepts any non-empty string (no email validation in entities — `MailAddress` is just a type alias). Validation belongs in P3.

- [ ] **Step 1: Write the failing tests**

Append to the existing `#[cfg(test)] mod tests` block in `src/sbatch/cmd.rs` (after the dependency tests added in Task 3):

```rust
    #[test]
    fn mail_user_emits_flag_and_value() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.mail_user = Some("alice@example.com".to_string());
        let argv = cmd.build_argv().unwrap();
        let i = argv
            .iter()
            .position(|a| a == "--mail-user")
            .expect("--mail-user present");
        assert_eq!(argv[i + 1], "alice@example.com");
    }

    #[test]
    fn mail_user_omitted_when_none() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a == "--mail-user"));
    }

    #[test]
    fn mail_types_emits_comma_separated_list() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.mail_types = Some("BEGIN,END".to_string().try_into().unwrap());
        let argv = cmd.build_argv().unwrap();
        let i = argv
            .iter()
            .position(|a| a == "--mail-type")
            .expect("--mail-type present");
        assert_eq!(argv[i + 1], "BEGIN,END");
    }

    #[test]
    fn mail_types_single_value_emits_one_token() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.mail_types = Some("FAIL".to_string().try_into().unwrap());
        let argv = cmd.build_argv().unwrap();
        let i = argv
            .iter()
            .position(|a| a == "--mail-type")
            .expect("--mail-type present");
        assert_eq!(argv[i + 1], "FAIL");
    }

    #[test]
    fn mail_types_omitted_when_none() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a == "--mail-type"));
    }

    #[test]
    fn mail_user_and_mail_types_emit_in_stable_order() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.mail_user = Some("bob@example.com".to_string());
        cmd.mail_types = Some("ALL".to_string().try_into().unwrap());
        let argv = cmd.build_argv().unwrap();
        let user_idx = argv.iter().position(|a| a == "--mail-user").unwrap();
        let type_idx = argv.iter().position(|a| a == "--mail-type").unwrap();
        assert!(
            user_idx < type_idx,
            "expected --mail-user before --mail-type, got argv={argv:?}"
        );
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib --features pyo3 mail_user_emits_flag_and_value`

Expected: FAIL with `error[E0609]: no field 'mail_user' on type 'SbatchCmd'`.

- [ ] **Step 3: Add the two fields and argv emission**

Extend the `use` line at `src/sbatch/cmd.rs:13`:

```rust
use crate::entities::slurm::{
    JobPartition, JobTimeLimit, MailAddress, MailTypeInput, ResourceSpec, SlurmDependency,
};
```

Add the fields immediately after the `dependency` field added in Task 3:

```rust
    pub dependency: Option<SlurmDependency>,

    /// `--mail-user` value. When `Some`, emitted as `["--mail-user", addr.clone()]`.
    /// Stored as [`MailAddress`] (a `String` alias from
    /// `crate::entities::slurm::sbatch_options`).
    pub mail_user: Option<MailAddress>,

    /// `--mail-type` list. When `Some`, emitted as `["--mail-type", types.to_string()]`
    /// in the canonical comma-separated Slurm form (e.g. `"BEGIN,END"`).
    pub mail_types: Option<MailTypeInput>,

    pub env: HashMap<String, String>,
```

Add the defaults in `new()` (immediately after `dependency: None,`):

```rust
            dependency: None,
            mail_user: None,
            mail_types: None,
            env: HashMap::new(),
```

Extend `build_argv()`. The block from Task 3 currently ends with:

```rust
        if let Some(dep) = &self.dependency {
            argv.push("-d".to_string());
            argv.push(dep.to_string());
        }
        if self.no_requeue {
            argv.push("--no-requeue".to_string());
        }
```

Replace with:

```rust
        if let Some(dep) = &self.dependency {
            argv.push("-d".to_string());
            argv.push(dep.to_string());
        }
        if let Some(addr) = &self.mail_user {
            argv.push("--mail-user".to_string());
            argv.push(addr.clone());
        }
        if let Some(mts) = &self.mail_types {
            argv.push("--mail-type".to_string());
            argv.push(mts.to_string());
        }
        if self.no_requeue {
            argv.push("--no-requeue".to_string());
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib --features pyo3 -- mail_user mail_types`

Expected: 6 passed (the six tests added in Step 1).

- [ ] **Step 5: Run full lints + full sbatch::cmd test suite**

Run: `cargo test --lib --features pyo3 sbatch::cmd && cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`

Expected: all sbatch::cmd tests pass (the existing `full_flags_cpu_variant_argv_layout` does NOT set the new fields, so it must still match byte-for-byte); 0 clippy warnings; no formatting diff.

- [ ] **Step 6: Commit**

```bash
git add src/sbatch/cmd.rs
git commit -m "feat(sbatch): wire --mail-user and --mail-type via MailAddress + MailTypeInput"
```

---

## Task 5: pyo3 binding — three new kwargs on `PySbatchCmd::new`

**Files:**
- Modify: `src/py_export/sbatch.rs:13` (extend imports)
- Modify: `src/py_export/sbatch.rs:30-89` (`PySbatchCmd::new` signature + body)
- Modify: `python/tests/test_sbatch.py` (append three smoke tests)

**Why:** Python users construct `SbatchCmd` via `PySbatchCmd(script, **kwargs)`. We add three optional kwargs that thread through to the new Rust fields. `PySlurmDependency` and `PyMailTypeInput` already exist in `src/py_export/entities/slurm/sbatch_options/` and convert to/from the `entities` types via `From` / `Into` impls.

**Constraints:**
- Follow the existing kwargs-only signature pattern: all new args after `*,`.
- Place new kwargs **after `comment` and before the closing paren** to keep the visual order aligned with the Rust struct (no_requeue / comment / dependency / mail_user / mail_types).
- Pyo3 `from_py_object` is already on the wrapper structs; pass them by value into `Option<...>` directly.

- [ ] **Step 1: Confirm wrapper paths**

Run: `grep -n 'pub struct PySlurmDependency\|pub struct PyMailTypeInput' src/py_export/entities/slurm/sbatch_options/*.rs src/py_export/entities/slurm/sbatch_options.rs`

Expected output (line numbers may vary):
```
src/py_export/entities/slurm/sbatch_options/config.rs:89:pub struct PyMailTypeInput(pub inner::MailTypeInput);
src/py_export/entities/slurm/sbatch_options/dependency.rs:<some-line>:pub struct PySlurmDependency(...);
```

If `PySlurmDependency` lives at a different path, **adjust the import in Step 4 accordingly** but do not modify any source yet.

- [ ] **Step 2: Write the failing test (Python)**

Append to `python/tests/test_sbatch.py`:

```python
def test_sbatch_cmd_dependency_kwarg(tmp_path):
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options.dependency import (
        SlurmDependency,
    )

    job = tmp_path / "job.sh"
    job.write_text("#!/usr/bin/env bash\necho hi\n")
    cmd = SbatchCmd(str(job), dependency=SlurmDependency.parse("afterok:200"))
    assert cmd is not None


def test_sbatch_cmd_mail_user_kwarg(tmp_path):
    job = tmp_path / "job.sh"
    job.write_text("#!/usr/bin/env bash\necho hi\n")
    cmd = SbatchCmd(str(job), mail_user="alice@example.com")
    assert cmd is not None


def test_sbatch_cmd_mail_types_kwarg(tmp_path):
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        MailTypeInput,
    )

    job = tmp_path / "job.sh"
    job.write_text("#!/usr/bin/env bash\necho hi\n")
    cmd = SbatchCmd(str(job), mail_types=MailTypeInput.parse("BEGIN,END"))
    assert cmd is not None
```

- [ ] **Step 3: Run the failing test (after current `maturin develop`)**

Run: `uv run pytest python/tests/test_sbatch.py::test_sbatch_cmd_dependency_kwarg -v`

Expected: FAIL with `TypeError: SbatchCmd.__init__() got an unexpected keyword argument 'dependency'`.

- [ ] **Step 4: Extend the pyo3 binding**

Edit the `use` block at the top of `src/py_export/sbatch.rs`. The current line 13 reads:

```rust
use crate::entities::slurm::{JobTimeLimit, ResourceSpec};
```

Replace with:

```rust
use crate::entities::slurm::{JobTimeLimit, MailTypeInput, ResourceSpec, SlurmDependency};
```

Add two more `use` lines beneath the existing `use crate::sbatch::manager::SbatchManager;` (around line 17):

```rust
use crate::py_export::entities::slurm::sbatch_options::config::PyMailTypeInput;
use crate::py_export::entities::slurm::sbatch_options::dependency::PySlurmDependency;
```

Edit `PySbatchCmd::new`. Replace the existing block:

```rust
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        script,
        *,
        sbatch_bin = "sbatch".to_string(),
        job_name = None,
        partition = None,
        time_limit = None,
        rsc = None,
        output = None,
        error = None,
        chdir = None,
        env = None,
        args = None,
        no_requeue = false,
        comment = None,
    ))]
    fn new(
        script: PathBuf,
        sbatch_bin: String,
        job_name: Option<String>,
        partition: Option<String>,
        time_limit: Option<String>,
        rsc: Option<String>,
        output: Option<String>,
        error: Option<String>,
        chdir: Option<PathBuf>,
        env: Option<HashMap<String, String>>,
        args: Option<Vec<String>>,
        no_requeue: bool,
        comment: Option<String>,
    ) -> PyResult<Self> {
        let mut cmd = SbatchCmd::new(script);
        cmd.sbatch_bin = sbatch_bin;
        cmd.job_name = job_name;
        cmd.partition = partition;
        if let Some(s) = time_limit {
            cmd.time_limit = Some(s.parse::<JobTimeLimit>().map_err(py_err)?);
        }
        if let Some(s) = rsc {
            cmd.rsc = Some(s.parse::<ResourceSpec>().map_err(py_err)?);
        }
        cmd.output = output;
        cmd.error = error;
        cmd.chdir = chdir;
        cmd.env = env.unwrap_or_default();
        cmd.args = args.unwrap_or_default();
        cmd.no_requeue = no_requeue;
        cmd.comment = comment;
        Ok(Self(cmd))
    }
```

with:

```rust
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        script,
        *,
        sbatch_bin = "sbatch".to_string(),
        job_name = None,
        partition = None,
        time_limit = None,
        rsc = None,
        output = None,
        error = None,
        chdir = None,
        env = None,
        args = None,
        no_requeue = false,
        comment = None,
        dependency = None,
        mail_user = None,
        mail_types = None,
    ))]
    fn new(
        script: PathBuf,
        sbatch_bin: String,
        job_name: Option<String>,
        partition: Option<String>,
        time_limit: Option<String>,
        rsc: Option<String>,
        output: Option<String>,
        error: Option<String>,
        chdir: Option<PathBuf>,
        env: Option<HashMap<String, String>>,
        args: Option<Vec<String>>,
        no_requeue: bool,
        comment: Option<String>,
        dependency: Option<PySlurmDependency>,
        mail_user: Option<String>,
        mail_types: Option<PyMailTypeInput>,
    ) -> PyResult<Self> {
        let mut cmd = SbatchCmd::new(script);
        cmd.sbatch_bin = sbatch_bin;
        cmd.job_name = job_name;
        cmd.partition = partition;
        if let Some(s) = time_limit {
            cmd.time_limit = Some(s.parse::<JobTimeLimit>().map_err(py_err)?);
        }
        if let Some(s) = rsc {
            cmd.rsc = Some(s.parse::<ResourceSpec>().map_err(py_err)?);
        }
        cmd.output = output;
        cmd.error = error;
        cmd.chdir = chdir;
        cmd.env = env.unwrap_or_default();
        cmd.args = args.unwrap_or_default();
        cmd.no_requeue = no_requeue;
        cmd.comment = comment;
        cmd.dependency = dependency.map(<PySlurmDependency as Into<SlurmDependency>>::into);
        cmd.mail_user = mail_user;
        cmd.mail_types = mail_types.map(<PyMailTypeInput as Into<MailTypeInput>>::into);
        Ok(Self(cmd))
    }
```

The fully-qualified `Into::into` form avoids ambiguity even when multiple `From` impls coexist on the wrapper types.

- [ ] **Step 5: Rebuild the Python extension**

Run: `uv run maturin develop --features pyo3`

Expected: build succeeds, `pip install ...` reports the wheel built and installed into the venv.

- [ ] **Step 6: Run the Python tests**

Run: `uv run pytest python/tests/test_sbatch.py -v`

Expected: all P1 tests pass + 3 new tests pass (`test_sbatch_cmd_dependency_kwarg`, `test_sbatch_cmd_mail_user_kwarg`, `test_sbatch_cmd_mail_types_kwarg`).

- [ ] **Step 7: Run full lints**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`

Expected: 0 warnings, no formatting diff.

- [ ] **Step 8: Commit**

```bash
git add src/py_export/sbatch.rs python/tests/test_sbatch.py
git commit -m "feat(py): expose dependency/mail_user/mail_types kwargs on SbatchCmd"
```

---

## Task 6: `.pyi` sync

**Files:**
- Modify: `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi:1-38`

**Why:** The `.pyi` is hand-written (the file's leading comment says so explicitly). Adding kwargs in pyo3 without updating the stub silently degrades editor type checking. We mirror the new kwargs as `<entity-py-type> | None = None`.

**Constraints:**
- Use the `entity` python class names (`SlurmDependency`, `MailTypeInput`) imported via `TYPE_CHECKING` to avoid runtime circular import.
- Match the order in the pyo3 signature: `dependency`, `mail_user`, `mail_types`.

- [ ] **Step 1: Inspect the current top of the .pyi**

Run: `head -20 python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi`

Expected (verbatim): the existing `# Hand-written stubs ...` comment, then `import builtins`, `import os`, `from collections.abc import Awaitable`, `from typing import final`, then `__all__`.

- [ ] **Step 2: Add a Python smoke test that exercises all three kwargs at once**

Append to `python/tests/test_sbatch.py`:

```python
def test_sbatch_cmd_kwargs_listed_in_runtime_signature():
    """Smoke: pyo3 must accept all three P2 kwargs as recognized names."""
    cmd = SbatchCmd(
        "/tmp/job.sh",
        dependency=None,
        mail_user=None,
        mail_types=None,
    )
    assert cmd is not None
```

- [ ] **Step 3: Run the test (should already PASS thanks to Task 5)**

Run: `uv run pytest python/tests/test_sbatch.py::test_sbatch_cmd_kwargs_listed_in_runtime_signature -v`

Expected: PASS. This step exists to cement runtime parity before we hand-edit the stub.

- [ ] **Step 4: Update the `.pyi`**

Edit `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi`. Replace the block from `import builtins` through the closing `) -> None: ...` of `SbatchCmd.__init__`. The current block (lines 7-38) reads:

```python
import builtins
import os
from collections.abc import Awaitable
from typing import final

__all__ = [
    "SbatchCmd",
    "SbatchManager",
    "SbatchJobHandle",
]

@final
class SbatchCmd:
    """Spec for one ``sbatch`` invocation. Pure data + ``build_argv`` (Rust-side)."""

    def __init__(
        self,
        script: builtins.str | os.PathLike[builtins.str],
        *,
        sbatch_bin: builtins.str = "sbatch",
        job_name: builtins.str | None = None,
        partition: builtins.str | None = None,
        time_limit: builtins.str | None = None,
        rsc: builtins.str | None = None,
        output: builtins.str | None = None,
        error: builtins.str | None = None,
        chdir: builtins.str | os.PathLike[builtins.str] | None = None,
        env: builtins.dict[builtins.str, builtins.str] | None = None,
        args: builtins.list[builtins.str] | None = None,
        no_requeue: builtins.bool = False,
        comment: builtins.str | None = None,
    ) -> None: ...
```

Replace with:

```python
import builtins
import os
from collections.abc import Awaitable
from typing import TYPE_CHECKING, final

if TYPE_CHECKING:
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        MailTypeInput,
    )
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options.dependency import (
        SlurmDependency,
    )

__all__ = [
    "SbatchCmd",
    "SbatchManager",
    "SbatchJobHandle",
]

@final
class SbatchCmd:
    """Spec for one ``sbatch`` invocation. Pure data + ``build_argv`` (Rust-side)."""

    def __init__(
        self,
        script: builtins.str | os.PathLike[builtins.str],
        *,
        sbatch_bin: builtins.str = "sbatch",
        job_name: builtins.str | None = None,
        partition: builtins.str | None = None,
        time_limit: builtins.str | None = None,
        rsc: builtins.str | None = None,
        output: builtins.str | None = None,
        error: builtins.str | None = None,
        chdir: builtins.str | os.PathLike[builtins.str] | None = None,
        env: builtins.dict[builtins.str, builtins.str] | None = None,
        args: builtins.list[builtins.str] | None = None,
        no_requeue: builtins.bool = False,
        comment: builtins.str | None = None,
        dependency: "SlurmDependency | None" = None,
        mail_user: builtins.str | None = None,
        mail_types: "MailTypeInput | None" = None,
    ) -> None: ...
```

(The `# Hand-written stubs ...` header comment block at the very top of the file stays unchanged. The existing `# ruff: noqa: ...` directive also stays.)

- [ ] **Step 5: Smoke-import to verify the .pyi parses (optional, best-effort)**

Run: `uv run python -c "import slurm_async_runner._slurm_async_runner_core.sbatch as m; print(m.SbatchCmd)"`

Expected: prints something like `<class 'builtins.SbatchCmd'>`. The `.pyi` is consumed by type checkers, not at runtime, so this only confirms the package still imports.

- [ ] **Step 6: Run the full Python test suite**

Run: `uv run pytest python/tests/ -v`

Expected: all tests pass (30 from P1 + 4 new from P2: three `_kwarg` tests + the runtime signature test).

- [ ] **Step 7: Run lints**

Run: `uv run ruff check python/`

Expected: 0 lint errors.

- [ ] **Step 8: Commit**

```bash
git add python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi python/tests/test_sbatch.py
git commit -m "docs(py): sync .pyi for dependency/mail_user/mail_types kwargs"
```

---

## Task 7: CHANGELOG + final validation

**Files:**
- Modify: `CHANGELOG.md:8-40`

**Why:** Update the `[Unreleased]` block to record P2 additions in the same shape as the existing P1 entries (`### Added (Phase 2 P1)` is already present). Run the full validation gate end-to-end once more before merging.

- [ ] **Step 1: Append the P2 section to CHANGELOG**

Open `CHANGELOG.md`. The current `[Unreleased]` block starts at line 8 with:

```markdown
## [Unreleased]

### Added (Phase 2 P1)
```

Insert a new `### Added (Phase 2 P2)` block IMMEDIATELY after the line `## [Unreleased]` and **before** the existing `### Added (Phase 2 P1)` block. Use this exact content:

```markdown
### Added (Phase 2 P2)

- **`SbatchCmd::dependency: Option<SlurmDependency>`** — emits `["-d", dep.to_string()]`.
  Reuses `crate::entities::slurm::SlurmDependency` (already implements `FromStr` /
  `Display` for `afterok:200`, `afterok:200,afterany:201`, `afterok:200?afterany:201`,
  `singleton`, etc.). Python:
  `PySbatchCmd(..., dependency=SlurmDependency.parse("afterok:200"))`.
- **`SbatchCmd::mail_user: Option<MailAddress>`** — emits `["--mail-user", addr]`.
  `MailAddress` is the `String` alias from
  `crate::entities::slurm::sbatch_options`. Python:
  `PySbatchCmd(..., mail_user="alice@example.com")`.
- **`SbatchCmd::mail_types: Option<MailTypeInput>`** — emits
  `["--mail-type", types.to_string()]` in canonical comma-separated form
  (`BEGIN,END,FAIL,REQUEUE,ALL`). Python:
  `PySbatchCmd(..., mail_types=MailTypeInput.parse("BEGIN,END"))`.
- **`MailType::as_slurm_str(self) -> &'static str`** plus
  **`impl Display for MailType`** and **`impl Display for MailTypeInput`** in
  `entities::slurm::sbatch_options` — required for round-tripping the
  comma-separated `--mail-type` value.

```

- [ ] **Step 2: Run the full validation gate**

Run these in order:

```bash
cargo fmt --all --check
cargo clippy --all-targets --features pyo3 -- -D warnings
cargo test --lib --features pyo3
uv run maturin develop --features pyo3
uv run pytest python/tests/
uv run ruff check python/
```

Expected:
- `cargo fmt`: clean (no diff)
- `cargo clippy`: 0 warnings
- `cargo test --lib --features pyo3`: all passing (P1 baseline 276 + ~13 new from P2 Tasks 1-4 = ~289)
- `maturin develop`: build succeeds
- `pytest`: all passing (P1 baseline 30 + 4 new from P2 Tasks 5-6 = 34, plus 2 skipped live)
- `ruff`: 0 errors

- [ ] **Step 3: Verify no regression on the existing argv layout test**

Run: `cargo test --lib --features pyo3 full_flags_cpu_variant_argv_layout -- --exact`

Expected: PASS. This test does NOT set the new fields, so its expected argv must be byte-identical to before P2.

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): record Phase 2 P2 dependency/mail wiring"
```

- [ ] **Step 5: Sanity-check the commit graph**

Run: `git log --oneline $(git merge-base HEAD origin/develop)..HEAD`

Expected: 7 new commits on top of the P1 head — one per task, in order:
```
<sha> docs(changelog): record Phase 2 P2 dependency/mail wiring
<sha> docs(py): sync .pyi for dependency/mail_user/mail_types kwargs
<sha> feat(py): expose dependency/mail_user/mail_types kwargs on SbatchCmd
<sha> feat(sbatch): wire --mail-user and --mail-type via MailAddress + MailTypeInput
<sha> feat(sbatch): wire --dependency via SlurmDependency entity
<sha> feat(entities): add Display impl for MailTypeInput
<sha> feat(entities): add MailType::as_slurm_str + Display impl
```

---

## Self-Review Coverage

Spec §4.2 (`--dependency`) → Tasks 3 + 5 + 6.
Spec §4.3 (`--mail-user` / `--mail-type`) → Tasks 1 + 2 + 4 + 5 + 6.
Spec §2.1 (vocab single-source) → enforced: only `entities::slurm::sbatch_options` adds `Display`/`as_slurm_str`; `crate::sbatch::*` only imports.
Spec §2.2 invariants: no new `JobDispatcher` method, no new `JobState` variant, no new kind string, no sacct calls outside `refresh_with_sacct`/`run()`. The argv emission is pure-data; no I/O is added.
Spec §11 PR checklist: CHANGELOG updated (Task 7), `.pyi` synced (Task 6), full test/lint pass (Task 7).

## Dependencies

This plan is independent of P1 sources beyond reusing the same worktree branch. No P5 / P6 prerequisite.
