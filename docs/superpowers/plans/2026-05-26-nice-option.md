# `--nice` Scheduling-Priority Option Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `nice` option to `SbatchCmd` (emitted as `--nice=<v>` in argv) and to the `SlurmJobConfig` config envelope, so callers can adjust SLURM scheduling priority (issue #13).

**Architecture:** `nice: Option<i32>`, pass-through (no validation). `SbatchCmd.build_argv()` emits the single token `--nice=<v>` after `--comment`. `SlurmJobConfig` gets a parallel `#[serde(default)]` field for config parity only — there is no `SlurmJobConfig → SbatchCmd` conversion in the codebase and this plan does not add one. PyO3 wrappers expose `nice` as a keyword arg; the handwritten `SbatchCmd` stub is edited by hand and the `gen_stub`-driven `SlurmJobConfig` stub is regenerated.

**Tech Stack:** Rust, PyO3 (`pyo3` + `pyo3-stub-gen`), maturin, pytest.

**Reference spec:** `docs/superpowers/specs/2026-05-26-nice-option-design.md` (includes verified KUDPC compatibility).

---

## File Structure

- `src/sbatch/cmd.rs` — `SbatchCmd` struct + `build_argv()` (the only place `--nice` reaches argv). Core change + Rust unit tests.
- `src/entities/slurm/sbatch_options.rs` — `SlurmJobConfig` envelope. Add parallel field + serde tests.
- `src/py_export/sbatch.rs` — `PySbatchCmd` constructor. Add kwarg.
- `src/py_export/entities/slurm/sbatch_options/config.rs` — `PySlurmJobConfig` constructor + getter/setter.
- `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi` — handwritten stub; hand-edit.
- `python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/sbatch_options/__init__.pyi` — `gen_stub`-generated stub; regenerate.
- `python/tests/test_sbatch.py` — Python kwarg tests.
- `CHANGELOG.md` — one `feat` line.

---

## Task 1: Core — `SbatchCmd.nice` field + `build_argv`

**Files:**
- Modify: `src/sbatch/cmd.rs` (struct ~line 64, `new()` ~line 88, `build_argv()` ~line 158, tests in `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing tests**

Add these four tests inside the existing `#[cfg(test)] mod tests { ... }` block in `src/sbatch/cmd.rs` (e.g. after `comment_omitted_when_none`):

```rust
    #[test]
    fn nice_emits_single_token() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.nice = Some(100);
        let argv = cmd.build_argv().unwrap();
        assert!(
            argv.iter().any(|a| a == "--nice=100"),
            "expected --nice=100 token, got argv={argv:?}"
        );
    }

    #[test]
    fn nice_zero_is_emitted() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.nice = Some(0);
        let argv = cmd.build_argv().unwrap();
        assert!(
            argv.iter().any(|a| a == "--nice=0"),
            "expected --nice=0 (explicit no-op), got argv={argv:?}"
        );
    }

    #[test]
    fn nice_negative_value_is_single_token() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.nice = Some(-5);
        let argv = cmd.build_argv().unwrap();
        assert!(
            argv.iter().any(|a| a == "--nice=-5"),
            "expected single token --nice=-5, got argv={argv:?}"
        );
    }

    #[test]
    fn nice_omitted_when_none() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a.starts_with("--nice")));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib sbatch::cmd::tests::nice 2>&1 | tail -20`
Expected: compile error `no field 'nice' on type '&SbatchCmd'` (field not yet added).

- [ ] **Step 3: Add the field, initializer, and argv emission**

In `src/sbatch/cmd.rs`, add the field to the `SbatchCmd` struct immediately after the `comment` field (`pub comment: Option<String>,`):

```rust
    /// `--nice` scheduling-priority adjustment. When `Some`, emitted as the
    /// single token `--nice=<v>` so negative values are not parsed as a
    /// separate flag. Positive lowers priority, negative raises it (negative
    /// requires privilege). Pass-through: out-of-range values are left for
    /// SLURM to reject.
    pub nice: Option<i32>,
```

In `SbatchCmd::new()`, add the initializer immediately after `comment: None,`:

```rust
            nice: None,
```

In `build_argv()`, add this block immediately after the `--comment` block (the `if let Some(c) = &self.comment { ... }`) and before `argv.push(absolutize(&self.script)?);`:

```rust
        if let Some(n) = self.nice {
            argv.push(format!("--nice={n}"));
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib sbatch::cmd::tests::nice 2>&1 | tail -20`
Expected: `test result: ok. 4 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/sbatch/cmd.rs
git commit -m "feat(sbatch): add SbatchCmd.nice -> --nice=<v> in build_argv (issue #13)"
```

---

## Task 2: Entity — `SlurmJobConfig.nice` field

**Files:**
- Modify: `src/entities/slurm/sbatch_options.rs` (`SlurmJobConfig` struct ~line 217, tests in `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing tests**

Add these two tests inside the existing `#[cfg(test)] mod tests { ... }` block in `src/entities/slurm/sbatch_options.rs`:

```rust
    #[test]
    fn slurm_job_config_nice_roundtrips_through_json() {
        let cfg = SlurmJobConfig {
            partition: "gr10641a".to_string(),
            time_limit: None,
            log_stdout: None,
            log_stderr: None,
            comment: None,
            job_name: None,
            array_spec: None,
            dependency: None,
            mail_user: None,
            mail_types: None,
            resource_spec: None,
            nice: Some(100),
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: SlurmJobConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.nice, Some(100));
    }

    #[test]
    fn slurm_job_config_nice_defaults_to_none_when_absent() {
        // `nice` carries #[serde(default)]; absent key deserializes to None.
        // `comment` and `dependency` lack a serde default, so the minimal
        // document must still carry them (as null).
        let json = r#"{"partition":"gr10641a","comment":null,"dependency":null}"#;
        let cfg: SlurmJobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.nice, None);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib sbatch_options::tests::slurm_job_config_nice 2>&1 | tail -20`
Expected: compile error `missing field 'nice'` in the struct literal (field not yet added).

- [ ] **Step 3: Add the field**

In `src/entities/slurm/sbatch_options.rs`, add the field to the `SlurmJobConfig` struct immediately after the `resource_spec` field (the last field, `pub resource_spec: Option<ResourceSpec>,`):

```rust
    /// `--nice` scheduling-priority adjustment (signed). Config-envelope field
    /// for parity with [`crate::sbatch::cmd::SbatchCmd`]; it is NOT auto-wired
    /// to argv (mirrors the other `SlurmJobConfig` fields — there is no
    /// `SlurmJobConfig -> SbatchCmd` conversion).
    #[serde(default)]
    pub nice: Option<i32>,
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib sbatch_options::tests::slurm_job_config_nice 2>&1 | tail -20`
Expected: `test result: ok. 2 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/entities/slurm/sbatch_options.rs
git commit -m "feat(entities): add SlurmJobConfig.nice config field (issue #13)"
```

---

## Task 3: PyO3 — `PySbatchCmd` `nice` kwarg

**Files:**
- Modify: `src/py_export/sbatch.rs` (`#[pyo3(signature = ...)]` ~line 39-59, `new()` params ~line 60-79, body ~line 96)

- [ ] **Step 1: Add `nice` to the signature, parameter list, and body**

In `src/py_export/sbatch.rs`, inside `#[pyo3(signature = ( ... ))]`, add `nice = None,` immediately after `comment = None,`:

```rust
        comment = None,
        nice = None,
```

In the `fn new(...)` parameter list, add the parameter immediately after `comment: Option<String>,`:

```rust
        comment: Option<String>,
        nice: Option<i32>,
```

In the body, add the assignment immediately after `cmd.comment = comment;`:

```rust
        cmd.comment = comment;
        cmd.nice = nice;
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build --lib 2>&1 | tail -20`
Expected: builds with no errors. (Python-level behaviour is exercised in Task 6.)

- [ ] **Step 3: Commit**

```bash
git add src/py_export/sbatch.rs
git commit -m "feat(py): expose SbatchCmd nice kwarg (issue #13)"
```

---

## Task 4: PyO3 — `PySlurmJobConfig` `nice` kwarg + getter/setter

**Files:**
- Modify: `src/py_export/entities/slurm/sbatch_options/config.rs` (`#[pyo3(signature = ...)]` ~line 157-169, `new()` ~line 171-197, getters/setters block ~line 299-307)

- [ ] **Step 1: Add `nice` to the constructor signature, params, and struct literal**

In `src/py_export/entities/slurm/sbatch_options/config.rs`, inside the `#[pyo3(signature = ( ... ))]` for `new`, add `nice=None,` immediately after `resource_spec=None,`:

```rust
        resource_spec=None,
        nice=None,
```

In the `fn new(...)` parameter list, add the parameter immediately after `resource_spec: Option<PyResourceSpec>,`:

```rust
        resource_spec: Option<PyResourceSpec>,
        nice: Option<i32>,
```

In the `inner::SlurmJobConfig { ... }` struct literal, add `nice,` immediately after `resource_spec: resource_spec.map(|v| v.0),`:

```rust
            resource_spec: resource_spec.map(|v| v.0),
            nice,
```

- [ ] **Step 2: Add the getter and setter**

In the same `#[pymethods] impl PySlurmJobConfig`, add immediately after the existing `set_resource_spec` setter (before `fn __repr__`):

```rust
    #[getter]
    fn nice(&self) -> Option<i32> {
        self.0.nice
    }

    #[setter]
    fn set_nice(&mut self, v: Option<i32>) {
        self.0.nice = v;
    }
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build --lib 2>&1 | tail -20`
Expected: builds with no errors.

- [ ] **Step 4: Commit**

```bash
git add src/py_export/entities/slurm/sbatch_options/config.rs
git commit -m "feat(py): expose SlurmJobConfig nice kwarg + getter/setter (issue #13)"
```

---

## Task 5: Stubs

**Files:**
- Modify (by hand): `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi` (`SbatchCmd.__init__` ~line 46)
- Regenerate: `python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/sbatch_options/__init__.pyi`

- [ ] **Step 1: Hand-edit the `SbatchCmd` stub**

In `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi`, in `SbatchCmd.__init__`, add the parameter immediately after `comment: builtins.str | None = None,`:

```python
        comment: builtins.str | None = None,
        nice: builtins.int | None = None,
```

- [ ] **Step 2: Regenerate the `SlurmJobConfig` stub**

Run: `cargo run --bin stub_gen && uv run ruff format python/`
Expected: exit 0; the `SlurmJobConfig` entry in
`.../entities/slurm/sbatch_options/__init__.pyi` now contains a `nice` getter,
`nice` setter, and `nice: typing.Optional[builtins.int] = None` in `__init__`.

Verify: `git diff --stat python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/sbatch_options/__init__.pyi`
Expected: that file shows added `nice` lines.

> **If `stub_gen` fails** with an undefined-Python-symbol linker error (the
> known `pyo3-stub-gen` × `extension-module` conflict), hand-edit
> `.../sbatch_options/__init__.pyi` instead: mirror the `comment` getter/setter
> and the `__init__` signature, adding `nice` with type
> `typing.Optional[builtins.int]` (default `None`).

- [ ] **Step 3: Sanity-check the stubs are valid Python**

Run: `uv run python -c "import ast; ast.parse(open('python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi').read()); ast.parse(open('python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/sbatch_options/__init__.pyi').read()); print('stubs parse OK')"`
Expected: `stubs parse OK`.

- [ ] **Step 4: Commit**

```bash
git add python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi \
        python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/sbatch_options/__init__.pyi
git commit -m "docs(stubs): add nice to SbatchCmd + SlurmJobConfig type stubs (issue #13)"
```

---

## Task 6: Python tests (built extension)

**Files:**
- Modify: `python/tests/test_sbatch.py` (after `test_sbatch_cmd_comment_kwarg`, ~line 95)

- [ ] **Step 1: Build the extension so the new kwarg is importable**

Run: `uv run maturin develop 2>&1 | tail -5`
Expected: `Installed slurm_async_runner` (build succeeds).

- [ ] **Step 2: Write the failing tests**

In `python/tests/test_sbatch.py`, add these tests after `test_sbatch_cmd_comment_kwarg`:

```python
def test_sbatch_cmd_nice_kwarg(tmp_path):
    """nice kwarg should produce the single token --nice=<v> in argv."""
    job = tmp_path / "job.sh"
    job.write_text("#!/bin/sh\necho hi\n")

    cmd = SbatchCmd(str(job), nice=100)
    argv = cmd.build_argv()
    assert "--nice=100" in argv


def test_sbatch_cmd_nice_omitted_when_absent(tmp_path):
    """No nice kwarg should produce no --nice flag."""
    job = tmp_path / "job.sh"
    job.write_text("#!/bin/sh\necho hi\n")

    cmd = SbatchCmd(str(job))
    argv = cmd.build_argv()
    assert not any(a.startswith("--nice") for a in argv)
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `uv run pytest python/tests/test_sbatch.py -k nice -v 2>&1 | tail -20`
Expected: `test_sbatch_cmd_nice_kwarg PASSED` and `test_sbatch_cmd_nice_omitted_when_absent PASSED`.

- [ ] **Step 4: Commit**

```bash
git add python/tests/test_sbatch.py
git commit -m "test(py): cover SbatchCmd nice kwarg in build_argv (issue #13)"
```

---

## Task 7: CHANGELOG + full verification

**Files:**
- Modify: `CHANGELOG.md` (`## [Unreleased]` section ~line 8)

- [ ] **Step 1: Add the changelog entry**

In `CHANGELOG.md`, under the `## [Unreleased]` heading, add:

```markdown
## [Unreleased]

### Added

- **`nice` option on `SbatchCmd` / `SlurmJobConfig`** (issue #13). Emits
  `--nice=<v>` (single token, so negative values pass through) to adjust SLURM
  scheduling priority — positive lowers priority, negative raises it. Verified
  accepted by the KUDPC sbatch wrapper. `SlurmJobConfig.nice` is a config field
  only (not auto-wired to argv).
```

- [ ] **Step 2: Run the full verification suite**

Run each and confirm:

```bash
cargo test --lib 2>&1 | tail -5
```
Expected: all unit tests pass (previous count + 6 new).

```bash
cargo clippy --all-targets -- -D warnings 2>&1 | tail -5
```
Expected: no warnings.

```bash
cargo fmt --all -- --check
```
Expected: no diff (exit 0).

```bash
uv run pytest python/tests -v 2>&1 | tail -10
```
Expected: all Python tests pass.

```bash
uv run ruff check python/ 2>&1 | tail -5
```
Expected: `All checks passed!`.

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): record nice option (issue #13)"
```

---

## Acceptance Criteria (from spec)

- `SbatchCmd(script, nice=100).build_argv()` contains `--nice=100`. (Task 1, Task 6)
- Omitting `nice` produces no `--nice` flag. (Task 1, Task 6)
- `nice=0` produces `--nice=0`. (Task 1)
- `nice=-5` produces the single token `--nice=-5`. (Task 1)
- `SlurmJobConfig` carries a `nice` field through serde and PyO3. (Task 2, Task 4)
- Type stubs expose `nice` on both classes. (Task 5)
