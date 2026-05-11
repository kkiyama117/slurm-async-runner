# sbatch Phase 2 P3 — `--export` Value Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reject `--export` keys/values that contain the reserved characters `,` or `=` before `sbatch` is ever invoked, surfacing structured `SbatchSpawnError::InvalidExportKey` / `InvalidExportValue` so callers can repair input without parsing stderr.

**Architecture:** SLURM's `--export=ALL,K1=V1,K2=V2,...` uses `,` and `=` as in-band separators. Today `render_export` blindly concatenates with no escaping, so a value like `"a,b"` silently produces `KEY=a,b` which sbatch interprets as a key `b` with no value. We move the check upstream: `render_export` becomes fallible (returns `Result<String, SbatchSpawnError>`), `build_argv` returns `Result<Vec<String>, SbatchSpawnError>` instead of `anyhow::Result<...>`, and absolutize failures roll into the existing `SbatchSpawnError::Other(#[from] anyhow::Error)` variant. The two new error variants describe exactly which key/value broke the contract.

**Tech Stack:** Rust 2021, `pyo3` (feature gate `pyo3`), `thiserror` 2.0 (already in use), `anyhow` (already in use). No new dependencies.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/sbatch/error.rs` | Add two new error variants (`InvalidExportKey`, `InvalidExportValue`). |
| `src/sbatch/cmd.rs` | Change `render_export` to return `Result<String, SbatchSpawnError>` with key/value validation; change `build_argv` to return `Result<Vec<String>, SbatchSpawnError>` (absolutize errors flow through `SbatchSpawnError::Other`); add new tests. |
| `src/sbatch/manager.rs` | Trivial update: `cmd.build_argv()?` already works because the new return type matches `spawn`'s `Result<_, SbatchSpawnError>`. The existing `.map_err(SbatchSpawnError::Other)` wrapper around `build_argv()` is removed (no longer needed). |
| `src/py_export/sbatch.rs` | Update `PySbatchCmd::build_argv` to map `SbatchSpawnError` → `PyRuntimeError` (string already works via `Display`; no signature change). |
| `python/tests/test_sbatch.py` | One smoke test: building argv with a comma-laden value raises `RuntimeError`. |
| `CHANGELOG.md` | Append `### Added (Phase 2 P3)` block. |

No new files. The plan adds **≈90 LOC of source + ≈55 LOC of tests**.

---

## Task 1: Add `SbatchSpawnError::InvalidExportKey` and `InvalidExportValue` variants

**Files:**
- Modify: `src/sbatch/error.rs:3-21`

**Why:** Establish the typed surface for the validation failures before any code that produces them exists. This is a no-op API extension — the variants are constructible but not yet returned by any function.

**Constraints:**
- The enum is `#[non_exhaustive]`, so external matches are not broken by adding variants.
- Use `thiserror`'s `#[error("...")]` attribute format, mirroring the existing `SubmitFailed` / `JobidParseError` patterns. Quote any user-supplied values with `{:?}` so unprintable bytes survive the round-trip in logs.
- Variant names must be exactly `InvalidExportKey` and `InvalidExportValue` (the spec §4.8 names).

- [ ] **Step 1: Write the failing test**

Append a new `#[cfg(test)] mod tests` block to the bottom of `src/sbatch/error.rs` (the file currently has no test module):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_export_key_carries_offending_string() {
        let e = SbatchSpawnError::InvalidExportKey {
            key: "BAD,KEY".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("BAD,KEY"), "expected key in message, got: {msg}");
    }

    #[test]
    fn invalid_export_value_carries_offending_strings() {
        let e = SbatchSpawnError::InvalidExportValue {
            key: "FOO".to_string(),
            value: "a=b".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("FOO"), "expected key in message, got: {msg}");
        assert!(msg.contains("a=b"), "expected value in message, got: {msg}");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --features pyo3 invalid_export_key_carries_offending_string`

Expected: FAIL with `error[E0599]: no variant or associated item named 'InvalidExportKey' found for enum 'SbatchSpawnError'`.

- [ ] **Step 3: Add the two error variants**

Edit `src/sbatch/error.rs`. The current `SbatchSpawnError` enum block (lines 3-21) is:

```rust
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchSpawnError {
    #[error("sbatch invocation failed (exit={exit_code}): {stdout}")]
    SubmitFailed { exit_code: i32, stdout: String },

    #[error("sbatch stdout did not contain a parseable jobid: {stdout}")]
    JobidParseError { stdout: String },

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

Insert two new variants between `JobidParseError` and `SubmittedButUnpersisted`:

```rust
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchSpawnError {
    #[error("sbatch invocation failed (exit={exit_code}): {stdout}")]
    SubmitFailed { exit_code: i32, stdout: String },

    #[error("sbatch stdout did not contain a parseable jobid: {stdout}")]
    JobidParseError { stdout: String },

    #[error("--export key contains forbidden char (`,` or `=`): {key:?}")]
    InvalidExportKey { key: String },

    #[error("--export value for key {key:?} contains forbidden char (`,` or `=`): {value:?}")]
    InvalidExportValue { key: String, value: String },

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

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib --features pyo3 -- invalid_export`

Expected: 2 passed.

- [ ] **Step 5: Run full lints**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`

Expected: 0 warnings, no formatting diff.

- [ ] **Step 6: Commit**

```bash
git add src/sbatch/error.rs
git commit -m "feat(sbatch): add InvalidExportKey/InvalidExportValue error variants"
```

---

## Task 2: Validate in `render_export`, propagate through `build_argv`

**Files:**
- Modify: `src/sbatch/cmd.rs:1-15` (imports)
- Modify: `src/sbatch/cmd.rs:79-139` (`build_argv` return type)
- Modify: `src/sbatch/cmd.rs:142-155` (`render_export` signature + validation)
- Modify: `src/sbatch/cmd.rs:157-end` (test module: new tests)
- Modify: `src/sbatch/manager.rs:55-56` (drop `.map_err(SbatchSpawnError::Other)`)

**Why:** The whole point of P3 is to surface validation errors as typed variants, not blob them through `anyhow::Error`. We change `render_export` to return `Result<String, SbatchSpawnError>` with the actual validation logic; we change `build_argv` to return `Result<Vec<String>, SbatchSpawnError>` so the new variants surface unwrapped; absolutize failures (which return `anyhow::Result<String>`) flow through `SbatchSpawnError::Other(#[from] anyhow::Error)` via `?`.

**Constraints:**
- Keys and values are both checked; reject if either contains `,` or `=`.
- Validation order: iterate sorted keys; check the key first, then the value. The error on a key wins over an error on a value if both apply (deterministic).
- Empty key (`""`) is rejected by neither rule, so an empty key currently produces `--export=ALL,=VALUE` which is malformed. The spec §4.8 does not require rejecting empty keys, so we DO NOT add an empty-key check here (out of scope; can be added in a follow-up).
- `build_argv` already returns `Result<Vec<String>>` (with `anyhow`). The change to `Result<Vec<String>, SbatchSpawnError>` is a breaking API change for any external caller binding the error type — but in this crate, the only Rust callers are `manager.rs` (which uses the result via `?` and `Result<_, SbatchSpawnError>`) and tests (which use `.unwrap()`). The Python binding's `.map_err(|e| PyRuntimeError::new_err(e.to_string()))` is unaffected because `e.to_string()` works on any `Display`.

- [ ] **Step 1: Write the failing tests**

Append to the existing `#[cfg(test)] mod tests` block in `src/sbatch/cmd.rs`. Use `Read` first to locate the precise end of the module. The new tests go after the existing `mail_*` tests:

```rust
    #[test]
    fn export_key_with_comma_is_rejected() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.env.insert("BAD,KEY".to_string(), "ok".to_string());
        let err = cmd.build_argv().unwrap_err();
        match err {
            crate::sbatch::error::SbatchSpawnError::InvalidExportKey { key } => {
                assert_eq!(key, "BAD,KEY");
            }
            other => panic!("expected InvalidExportKey, got {other:?}"),
        }
    }

    #[test]
    fn export_key_with_equals_is_rejected() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.env.insert("BAD=KEY".to_string(), "ok".to_string());
        let err = cmd.build_argv().unwrap_err();
        assert!(
            matches!(
                err,
                crate::sbatch::error::SbatchSpawnError::InvalidExportKey { .. }
            ),
            "expected InvalidExportKey, got {err:?}"
        );
    }

    #[test]
    fn export_value_with_comma_is_rejected() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.env.insert("FOO".to_string(), "1,2".to_string());
        let err = cmd.build_argv().unwrap_err();
        match err {
            crate::sbatch::error::SbatchSpawnError::InvalidExportValue { key, value } => {
                assert_eq!(key, "FOO");
                assert_eq!(value, "1,2");
            }
            other => panic!("expected InvalidExportValue, got {other:?}"),
        }
    }

    #[test]
    fn export_value_with_equals_is_rejected() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.env.insert("FOO".to_string(), "a=b".to_string());
        let err = cmd.build_argv().unwrap_err();
        assert!(
            matches!(
                err,
                crate::sbatch::error::SbatchSpawnError::InvalidExportValue { .. }
            ),
            "expected InvalidExportValue, got {err:?}"
        );
    }

    #[test]
    fn export_valid_pairs_pass_through_unchanged() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.env.insert("FOO".to_string(), "bar".to_string());
        cmd.env.insert("OMP_NUM_THREADS".to_string(), "8".to_string());
        let argv = cmd.build_argv().expect("valid pairs accepted");
        assert!(
            argv.iter()
                .any(|a| a == "--export=ALL,FOO=bar,OMP_NUM_THREADS=8"),
            "expected canonical --export form, got argv={argv:?}"
        );
    }
```

- [ ] **Step 2: Run the failing test**

Run: `cargo test --lib --features pyo3 export_key_with_comma_is_rejected`

Expected: FAIL — `build_argv` currently returns `anyhow::Result<Vec<String>>`, so `.unwrap_err()` yields `anyhow::Error`, not `SbatchSpawnError`. The match arm references `SbatchSpawnError::InvalidExportKey` which compiles (the variant exists from Task 1) but the type mismatch on `err` is the failure — the `match` pattern fails because `err` is `anyhow::Error`.

(The compiler reports `error[E0308]: mismatched types` or `error[E0599]: no variant ... for type 'anyhow::Error'`. Either confirms TDD red.)

- [ ] **Step 3: Change `render_export` to return `Result<String, SbatchSpawnError>` and validate**

Edit `src/sbatch/cmd.rs`. The current `use` block (lines 1-15) does NOT import `SbatchSpawnError`. Add at the bottom of the `use` block:

```rust
use crate::sbatch::error::SbatchSpawnError;
```

Then locate `render_export` at lines ~142-155:

```rust
/// Render `--export=ALL,K1=V1,K2=V2,...` with deterministic key order
/// so argv is reproducible.
fn render_export(env: &HashMap<String, String>) -> String {
    let mut keys: Vec<&String> = env.keys().collect();
    keys.sort();
    let mut out = String::from("ALL");
    for k in keys {
        out.push(',');
        out.push_str(k);
        out.push('=');
        out.push_str(&env[k]);
    }
    out
}
```

Replace with:

```rust
/// Render `--export=ALL,K1=V1,K2=V2,...` with deterministic key order
/// so argv is reproducible.
///
/// Both keys and values are rejected if they contain `,` or `=`, since
/// those characters are SLURM's in-band separators on the `--export`
/// payload and any inline occurrence would silently corrupt the argv.
fn render_export(env: &HashMap<String, String>) -> Result<String, SbatchSpawnError> {
    let mut keys: Vec<&String> = env.keys().collect();
    keys.sort();
    let mut out = String::from("ALL");
    for k in keys {
        let v = &env[k];
        if k.contains(',') || k.contains('=') {
            return Err(SbatchSpawnError::InvalidExportKey { key: k.clone() });
        }
        if v.contains(',') || v.contains('=') {
            return Err(SbatchSpawnError::InvalidExportValue {
                key: k.clone(),
                value: v.clone(),
            });
        }
        out.push(',');
        out.push_str(k);
        out.push('=');
        out.push_str(v);
    }
    Ok(out)
}
```

- [ ] **Step 4: Change `build_argv` signature and update the `render_export` call**

In `src/sbatch/cmd.rs:79`, the current signature is:

```rust
pub fn build_argv(&self) -> Result<Vec<String>> {
```

The `Result` here refers to `anyhow::Result` because of the `use anyhow::Result;` at the top of the file. Change to:

```rust
pub fn build_argv(&self) -> Result<Vec<String>, SbatchSpawnError> {
```

Now the `absolutize(c)?` and `absolutize(&self.script)?` calls (lines 112, 136) need to map `anyhow::Error` into `SbatchSpawnError::Other`. Since `SbatchSpawnError::Other` has `#[from] anyhow::Error`, the `?` operator handles the conversion automatically — **no code change needed inside `build_argv` other than the return type**.

Update the `render_export` call site at line 115. The current line is:

```rust
argv.push(format!("--export={}", render_export(&self.env)));
```

Replace with:

```rust
argv.push(format!("--export={}", render_export(&self.env)?));
```

If the top of the file has `use anyhow::Result;`, check whether it is still referenced. If `anyhow::Result` is no longer used anywhere in the file (search for `Result<` and `anyhow::Result`), remove the `use anyhow::Result;` line. If it is still referenced (e.g. by a sibling function in the same file), leave it alone.

- [ ] **Step 5: Update `manager.rs` caller**

The current call site in `src/sbatch/manager.rs:55-56` is:

```rust
let argv = self
    .cmd
    .build_argv()
    .map_err(SbatchSpawnError::Other)?;
```

After our signature change, `build_argv()` already returns `Result<Vec<String>, SbatchSpawnError>`, so the `.map_err(SbatchSpawnError::Other)` adapter is redundant **and incorrect** (it would wrap a `SbatchSpawnError` in `Other(anyhow::Error)`, defeating the point of P3). Replace with:

```rust
let argv = self.cmd.build_argv()?;
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --lib --features pyo3 -- export_`

Expected: 5 passed (the 5 new tests in Step 1).

Then run: `cargo test --lib --features pyo3 sbatch::cmd`

Expected: ALL existing sbatch::cmd tests pass (the previous `cmd.build_argv().unwrap()` calls still work because `SbatchSpawnError: Debug`).

Then run: `cargo test --lib --features pyo3 sbatch::manager`

Expected: ALL existing sbatch::manager tests pass.

- [ ] **Step 7: Run full lints**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`

Expected: 0 warnings, no formatting diff.

- [ ] **Step 8: Verify the Python binding still compiles**

The existing `src/py_export/sbatch.rs` `build_argv` mapping at line ~95-99 is:

```rust
fn build_argv(&self) -> PyResult<Vec<String>> {
    self.0
        .build_argv()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}
```

This already works unchanged because `SbatchSpawnError` implements `Display` via `thiserror`. **No edit needed.** Verify by running:

```bash
cargo build --features pyo3 2>&1 | tail -20
```

Expected: build succeeds with no errors.

- [ ] **Step 9: Add a Python smoke test**

Append to `python/tests/test_sbatch.py`:

```python
def test_sbatch_cmd_build_argv_rejects_comma_in_value(tmp_path):
    job = tmp_path / "job.sh"
    job.write_text("#!/usr/bin/env bash\necho hi\n")
    cmd = SbatchCmd(str(job), env={"FOO": "a,b"})
    with pytest.raises(RuntimeError, match="FOO"):
        cmd.build_argv()
```

Then run:

```bash
uv run maturin develop --features pyo3
uv run pytest python/tests/test_sbatch.py::test_sbatch_cmd_build_argv_rejects_comma_in_value -v
```

Expected: 1 passed.

- [ ] **Step 10: Commit**

```bash
git add src/sbatch/cmd.rs src/sbatch/manager.rs python/tests/test_sbatch.py
git commit -m "feat(sbatch): validate --export keys and values for forbidden chars"
```

---

## Task 3: CHANGELOG + final validation

**Files:**
- Modify: `CHANGELOG.md` (append `### Added (Phase 2 P3)` block to `[Unreleased]`)

**Why:** Record the new error variants and the behavior change.

- [ ] **Step 1: Append the P3 section to CHANGELOG**

Open `CHANGELOG.md`. The current `[Unreleased]` block starts with `### Added (Phase 2 P2)`. Insert a new `### Added (Phase 2 P3)` block **immediately after** the line `## [Unreleased]` and **before** the existing `### Added (Phase 2 P2)` block. Use this exact content:

```markdown
### Added (Phase 2 P3)

- **`SbatchSpawnError::InvalidExportKey { key }`** and
  **`SbatchSpawnError::InvalidExportValue { key, value }`** —
  `SbatchCmd::build_argv()` now rejects any `env` entry whose key or value
  contains `,` or `=` (SLURM's in-band separators on the `--export` payload).
  Valid pairs round-trip unchanged. Python: `cmd.build_argv()` raises
  `RuntimeError` whose message contains the offending key (and value, for
  value errors).
- **`SbatchCmd::build_argv` return type** is now
  `Result<Vec<String>, SbatchSpawnError>` (was `anyhow::Result<Vec<String>>`).
  Absolutize/I-O errors flow through the existing
  `SbatchSpawnError::Other(#[from] anyhow::Error)` variant, so external
  callers using `?` against `SbatchSpawnError` are unaffected.

```

(Note the trailing blank line.)

- [ ] **Step 2: Run the full validation gate**

Run in order:

```bash
cargo fmt --all --check
cargo clippy --all-targets --features pyo3 -- -D warnings
cargo test --lib --features pyo3 2>&1 | tail -10
uv run maturin develop --features pyo3 2>&1 | tail -5
uv run pytest python/tests/ 2>&1 | tail -10
uv run ruff check python/
```

Expected:
- `cargo fmt`: clean
- `cargo clippy`: 0 warnings
- `cargo test --lib --features pyo3`: ≈ 300 passing (P2 baseline 293 + 2 from Task 1 error tests + 5 from Task 2 export tests = ≈ 300)
- `maturin develop`: build succeeds
- `pytest`: ≈ 35 passing (P2 baseline 34 + 1 new from Task 2 Step 9)
- `ruff`: 0 errors

- [ ] **Step 3: Verify no regression on the existing argv layout test**

Run: `cargo test --lib --features pyo3 full_flags_cpu_variant_argv_layout -- --exact`

Expected: PASS. This test uses `FOO=bar` and `OMP_NUM_THREADS=8` — both pass the new validation, so argv must be byte-identical to before P3.

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): record Phase 2 P3 --export validation"
```

- [ ] **Step 5: Sanity-check the commit graph**

Run: `git log --oneline 5063679..HEAD`

Expected: 3 new commits on top of the P2 head — one per task, in order:
```
<sha> docs(changelog): record Phase 2 P3 --export validation
<sha> feat(sbatch): validate --export keys and values for forbidden chars
<sha> feat(sbatch): add InvalidExportKey/InvalidExportValue error variants
```

---

## Self-Review Coverage

Spec §4.8 (`--export` 値バリデーション) → Tasks 1 + 2 + 3.
Spec §2.1 (vocab single-source) → no new vocab outside entities.
Spec §2.2 invariants: no new `JobDispatcher` method, no new `JobState` variant, no new kind string. The new error variants live on the existing `SbatchSpawnError`; no I/O is added.
Spec §11 PR checklist: CHANGELOG updated (Task 3), `.pyi` unchanged (return type of `build_argv` is still `list[str]` from Python's POV — `RuntimeError` is the convention for any pyo3 binding), full test/lint pass (Task 3).

## Dependencies

P3 is independent of P5 / P6. Can be merged in parallel with P4 (`SlurmSignalSpec`).
