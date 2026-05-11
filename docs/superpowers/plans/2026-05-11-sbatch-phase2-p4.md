# sbatch Phase 2 P4 — `SlurmSignalSpec` entity + `--signal` wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `SlurmSignalSpec` entity in `crate::entities::slurm::sbatch_options::signal` modeling SLURM's `--signal=[R:]<sig_num|sig_name>[@<sig_time>]` BNF, then wire it through `SbatchCmd::signal: Option<SlurmSignalSpec>` and `build_argv` so callers can request a pre-termination signal in a typed way.

**Architecture:** A new sibling module under `entities/slurm/sbatch_options/` named `signal.rs` defines `pub struct SlurmSignalSpec { allow_resignal: bool, signal: SignalIdent, seconds_before_end: Option<u16> }` and `pub enum SignalIdent { Number(u8), Name(String) }`. Both implement `FromStr`/`Display` exactly like the existing `SlurmDependency`. `SlurmSignalSpec` also implements `serde::Serialize`/`Deserialize` via `collect_str` + a `Visitor` (the dependency pattern). The pyo3 wrapper `PySlurmSignalSpec` exposes a `parse` static method and string `__str__`. `SbatchCmd` gains a single field `signal: Option<SlurmSignalSpec>` and emits `["--signal", spec.to_string()]` between `--mail-type` and `--no-requeue` in `build_argv`.

**Tech Stack:** Rust 2021, `pyo3` (feature gate `pyo3`), `pyo3-stub-gen` (for the entity-side stub), `thiserror`, `serde`. Reuses the existing `crate::error::SchemaParseError`. No new dependencies.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/entities/slurm/sbatch_options/signal.rs` (new) | `SignalIdent` enum + `SlurmSignalSpec` struct + `FromStr` + `Display` + `serde` + unit tests. |
| `src/entities/slurm/sbatch_options.rs` | Add `pub mod signal;` declaration and re-export `pub use signal::{SignalIdent, SlurmSignalSpec};`. |
| `src/entities/slurm.rs` | Extend the top-level `pub use sbatch_options::{...}` line to include `SignalIdent, SlurmSignalSpec`. |
| `src/sbatch/cmd.rs` | Add `pub signal: Option<SlurmSignalSpec>` field + default + argv emission + unit tests. |
| `src/py_export/entities/slurm/sbatch_options/signal.rs` (new) | `PySlurmSignalSpec` pyclass wrapper. |
| `src/py_export/entities/slurm/sbatch_options.rs` | Add `pub mod signal;` and `#[pymodule_export] use super::signal::PySlurmSignalSpec;`. |
| `src/py_export/sbatch.rs` | Thread a new `signal: Option<PySlurmSignalSpec>` kwarg through `PySbatchCmd::new`. |
| `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi` | Add `signal: "SlurmSignalSpec | None" = None,` kwarg to `SbatchCmd.__init__`. |
| `python/tests/test_sbatch.py` | One smoke test: build argv with a `SlurmSignalSpec.parse("USR1@60")` value. |
| `CHANGELOG.md` | Append `### Added (Phase 2 P4)` block. |

Two new source files, ≈340 LOC source + ≈90 LOC tests total.

---

## Task 1: Define `SignalIdent` + `SlurmSignalSpec` + `FromStr` + `Display` + `serde`

**Files:**
- Create: `src/entities/slurm/sbatch_options/signal.rs`
- Modify: `src/entities/slurm/sbatch_options.rs` (add `pub mod signal;` declaration ONLY in this task — the re-export is deferred to Task 2)

**Why:** The entity has to exist before re-exports and `SbatchCmd` wiring. We follow the `dependency.rs` shape exactly: pub types, then `impl FromStr`, then `impl Display`, then `impl TryFrom<&str>`/`TryFrom<String>` for ergonomics, then `serde::Serialize` + `Deserialize`.

**Constraints:**
- `SlurmSignalSpec` and `SignalIdent` are the EXACT names required by spec §4.6.
- `sig_num` MUST validate to `1..=64`. POSIX reserves `0` (kill probe) and SLURM doesn't accept it on `--signal`. Numbers above 64 are not standard POSIX signals.
- `seconds_before_end` is a `u16` (Slurm BNF allows `1..=65535`). `0` is rejected (no point sending the signal at end of time).
- `SignalIdent::Name` accepts non-empty strings matching `^[A-Z][A-Z0-9_]*$` so `,`, `@`, `:`, and whitespace cannot survive `FromStr`. This prevents argv corruption when the spec is round-tripped through `Display`.
- The `R:` prefix is case-sensitive (SLURM convention). `r:` is rejected.

- [ ] **Step 1: Add `pub mod signal;` to `sbatch_options.rs`**

Edit `src/entities/slurm/sbatch_options.rs`. Find the existing `pub mod ...;` block (around lines 10-16: `pub mod array_spec;`, `pub mod dependency;`, etc.) and add `pub mod signal;` in alphabetical order:

```rust
pub mod array_spec;

pub mod dependency;

pub mod resource_spec;

pub mod signal;

pub mod time_limit;
```

(Only the `pub mod` declaration; the `pub use signal::{...}` re-export is deferred to Task 2.)

- [ ] **Step 2: Create `src/entities/slurm/sbatch_options/signal.rs` with types + failing tests**

Write this file:

```rust
//! `--signal` spec for a Slurm batch submission.
//!
//! References:
//! - <https://slurm.schedmd.com/sbatch.html> (`--signal`)
//!
//! Slurm BNF: `[R:]<sig_num|sig_name>[@<sig_time>]`
//! - `R:` prefix — also signal a job that already had the signal queued
//!   (allow re-signal during an overlapping reservation)
//! - `sig_num` — POSIX signal number (1..=64)
//! - `sig_name` — `SIGINT`, `SIGTERM`, `SIGKILL`, `USR1`, etc.
//! - `@<sig_time>` — seconds before time limit to send the signal (1..=65535)

use crate::error::SchemaParseError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignalIdent {
    Number(u8),
    Name(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlurmSignalSpec {
    pub allow_resignal: bool,
    pub signal: SignalIdent,
    pub seconds_before_end: Option<u16>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- FromStr / Display roundtrip ----

    #[test]
    fn parses_signal_name_only() {
        let s: SlurmSignalSpec = "USR1".parse().unwrap();
        assert!(!s.allow_resignal);
        assert_eq!(s.signal, SignalIdent::Name("USR1".to_string()));
        assert_eq!(s.seconds_before_end, None);
        assert_eq!(s.to_string(), "USR1");
    }

    #[test]
    fn parses_signal_number_only() {
        let s: SlurmSignalSpec = "15".parse().unwrap();
        assert_eq!(s.signal, SignalIdent::Number(15));
        assert_eq!(s.to_string(), "15");
    }

    #[test]
    fn parses_signal_with_at_seconds() {
        let s: SlurmSignalSpec = "USR1@60".parse().unwrap();
        assert_eq!(s.signal, SignalIdent::Name("USR1".to_string()));
        assert_eq!(s.seconds_before_end, Some(60));
        assert_eq!(s.to_string(), "USR1@60");
    }

    #[test]
    fn parses_signal_with_r_prefix() {
        let s: SlurmSignalSpec = "R:USR1".parse().unwrap();
        assert!(s.allow_resignal);
        assert_eq!(s.signal, SignalIdent::Name("USR1".to_string()));
        assert_eq!(s.to_string(), "R:USR1");
    }

    #[test]
    fn parses_full_form() {
        let s: SlurmSignalSpec = "R:SIGTERM@30".parse().unwrap();
        assert!(s.allow_resignal);
        assert_eq!(s.signal, SignalIdent::Name("SIGTERM".to_string()));
        assert_eq!(s.seconds_before_end, Some(30));
        assert_eq!(s.to_string(), "R:SIGTERM@30");
    }

    #[test]
    fn parses_full_form_with_number() {
        let s: SlurmSignalSpec = "R:9@5".parse().unwrap();
        assert!(s.allow_resignal);
        assert_eq!(s.signal, SignalIdent::Number(9));
        assert_eq!(s.seconds_before_end, Some(5));
        assert_eq!(s.to_string(), "R:9@5");
    }

    // ---- error cases ----

    #[test]
    fn rejects_empty_string() {
        assert!("".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_lowercase_r_prefix() {
        assert!("r:USR1".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_signal_number_zero() {
        assert!("0".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_signal_number_above_64() {
        assert!("65".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_seconds_zero() {
        assert!("USR1@0".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_seconds_overflow() {
        assert!("USR1@70000".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_empty_signal_with_r_prefix() {
        assert!("R:".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_empty_signal_with_seconds() {
        assert!("@60".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_signal_name_with_comma() {
        assert!("USR1,FOO".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_signal_name_with_lowercase() {
        // Spec: SignalIdent::Name MUST match ^[A-Z][A-Z0-9_]*$
        assert!("usr1".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_signal_name_starting_with_digit() {
        // "9SIG" — first char is digit but doesn't parse as full number
        assert!("9SIG".parse::<SlurmSignalSpec>().is_err());
    }
}
```

- [ ] **Step 3: Run tests to verify all fail**

Run: `cargo test --lib --features pyo3 parses_signal_name_only`

Expected: FAIL with `error[E0277]: the trait 'FromStr' is not implemented for 'SlurmSignalSpec'`.

- [ ] **Step 4: Implement `FromStr` and `Display`**

Insert these impls in `signal.rs` AFTER the `pub struct SlurmSignalSpec { ... }` block and BEFORE the `#[cfg(test)] mod tests` block:

```rust
impl std::fmt::Display for SignalIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SignalIdent::Number(n) => write!(f, "{n}"),
            SignalIdent::Name(name) => f.write_str(name),
        }
    }
}

impl std::fmt::Display for SlurmSignalSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.allow_resignal {
            f.write_str("R:")?;
        }
        std::fmt::Display::fmt(&self.signal, f)?;
        if let Some(sec) = self.seconds_before_end {
            write!(f, "@{sec}")?;
        }
        Ok(())
    }
}

fn parse_signal_ident(s: &str) -> Result<SignalIdent, SchemaParseError> {
    let err = || SchemaParseError::ParseError {
        key: "signal/identifier".to_string(),
        value: s.to_string(),
    };
    if s.is_empty() {
        return Err(err());
    }
    // Numeric form: must parse as u8 in 1..=64
    if s.chars().all(|c| c.is_ascii_digit()) {
        let n: u8 = s.parse().map_err(|_| err())?;
        if !(1..=64).contains(&n) {
            return Err(err());
        }
        return Ok(SignalIdent::Number(n));
    }
    // Name form: ^[A-Z][A-Z0-9_]*$
    let mut chars = s.chars();
    let first = chars.next().ok_or_else(err)?;
    if !first.is_ascii_uppercase() {
        return Err(err());
    }
    for c in chars {
        if !(c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_') {
            return Err(err());
        }
    }
    Ok(SignalIdent::Name(s.to_string()))
}

impl std::str::FromStr for SlurmSignalSpec {
    type Err = SchemaParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || SchemaParseError::ParseError {
            key: "signal".to_string(),
            value: s.to_string(),
        };

        if s.is_empty() {
            return Err(err());
        }

        let (allow_resignal, rest) = if let Some(stripped) = s.strip_prefix("R:") {
            (true, stripped)
        } else {
            (false, s)
        };

        let (sig_part, sec_part) = match rest.split_once('@') {
            Some((l, r)) => (l, Some(r)),
            None => (rest, None),
        };

        if sig_part.is_empty() {
            return Err(err());
        }

        let signal = parse_signal_ident(sig_part)?;

        let seconds_before_end = match sec_part {
            Some(r) => {
                let n: u16 = r.parse().map_err(|_| err())?;
                if n == 0 {
                    return Err(err());
                }
                Some(n)
            }
            None => None,
        };

        Ok(Self {
            allow_resignal,
            signal,
            seconds_before_end,
        })
    }
}

impl TryFrom<&str> for SlurmSignalSpec {
    type Error = SchemaParseError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        <Self as std::str::FromStr>::from_str(s)
    }
}

impl TryFrom<String> for SlurmSignalSpec {
    type Error = SchemaParseError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        <Self as std::str::FromStr>::from_str(&s)
    }
}
```

- [ ] **Step 5: Run tests to verify FromStr/Display tests pass**

Run: `cargo test --lib --features pyo3 -- signal::tests 2>&1 | tail -25`

Expected: 17 passed (all parse/error tests from Step 2).

- [ ] **Step 6: Add `serde::Serialize` and `serde::Deserialize` impls**

Append at the end of `signal.rs`, immediately BEFORE the `#[cfg(test)] mod tests` block:

```rust
impl serde::Serialize for SlurmSignalSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for SlurmSignalSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SignalVisitor;

        impl<'de> serde::de::Visitor<'de> for SignalVisitor {
            type Value = SlurmSignalSpec;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(
                    "a Slurm `--signal` spec string, e.g. \"USR1\", \
                     \"SIGTERM@60\", or \"R:9@5\"",
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse::<SlurmSignalSpec>().map_err(E::custom)
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&v)
            }
        }

        deserializer.deserialize_str(SignalVisitor)
    }
}
```

Then append two serde tests inside the existing `#[cfg(test)] mod tests` block:

```rust
    // ---- serde JSON roundtrip ----

    #[test]
    fn serde_string_roundtrip() {
        let original = SlurmSignalSpec {
            allow_resignal: true,
            signal: SignalIdent::Name("USR1".to_string()),
            seconds_before_end: Some(60),
        };
        let json = serde_json::to_string(&original).unwrap();
        assert_eq!(json, "\"R:USR1@60\"");
        let parsed: SlurmSignalSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn serde_rejects_invalid_string() {
        let json = "\"r:bad\"";
        let parsed: Result<SlurmSignalSpec, _> = serde_json::from_str(json);
        assert!(parsed.is_err());
    }
```

Before writing the serde tests, verify `serde_json` is a dev-dependency: `grep -A1 '\[dev-dependencies\]' Cargo.toml | grep serde_json`. If it is NOT listed, report DONE_WITH_CONCERNS — we do not add new dependencies in this plan; you may swap `serde_json` for `toml` (which is more likely to be present) and adjust the asserted form accordingly:

```rust
let toml_str = toml::to_string(&original).unwrap();  // emits `"R:USR1@60"\n` form
```

- [ ] **Step 7: Run all signal tests**

Run: `cargo test --lib --features pyo3 -- signal:: 2>&1 | tail -25`

Expected: 19 passed (17 from Step 2 + 2 serde tests).

- [ ] **Step 8: Run full lints**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -10 && cargo fmt --all --check`

Expected: 0 warnings, no diff.

- [ ] **Step 9: Commit**

```bash
git add src/entities/slurm/sbatch_options/signal.rs src/entities/slurm/sbatch_options.rs
git commit -m "feat(entities): add SlurmSignalSpec entity with FromStr/Display/serde"
```

---

## Task 2: Re-export `SlurmSignalSpec` / `SignalIdent` from `entities/slurm`

**Files:**
- Modify: `src/entities/slurm/sbatch_options.rs` (re-export line near top)
- Modify: `src/entities/slurm.rs` (top-level re-export)

**Why:** Callers in `crate::sbatch::*` (Task 3) import from `crate::entities::slurm::{SignalIdent, SlurmSignalSpec}`. The re-export chain makes that path work.

**Constraints:**
- Add to the existing `pub use sbatch_options::{...}` line in `entities/slurm.rs` in alphabetical order.
- Add a `pub use signal::{SignalIdent, SlurmSignalSpec};` line in `sbatch_options.rs` near the other entity re-exports.

- [ ] **Step 1: Write the failing test**

Append to the existing `#[cfg(test)] mod tests` block in `src/entities/slurm/sbatch_options.rs` (the one created in P2 Task 1):

```rust
    #[test]
    fn signal_types_reachable_from_entities_slurm() {
        use crate::entities::slurm::{SignalIdent, SlurmSignalSpec};
        let s = SlurmSignalSpec {
            allow_resignal: false,
            signal: SignalIdent::Number(15),
            seconds_before_end: None,
        };
        assert_eq!(s.to_string(), "15");
    }
```

- [ ] **Step 2: Run the failing test**

Run: `cargo test --lib --features pyo3 signal_types_reachable_from_entities_slurm 2>&1 | tail -10`

Expected: FAIL with `error[E0432]: unresolved imports 'crate::entities::slurm::SignalIdent', 'crate::entities::slurm::SlurmSignalSpec'`.

- [ ] **Step 3: Add the re-export in `sbatch_options.rs`**

Edit `src/entities/slurm/sbatch_options.rs`. Find the existing `pub use` block near the top (around lines 28-67). Add a new re-export AFTER the `resource_spec` block (around line 55) and BEFORE the `time_limit` block:

```rust
// `SlurmSignalSpec` and `SignalIdent` live in their own file
// (see [`crate::entities::slurm::sbatch_options::signal`]) so the
// `--signal` BNF parsing and serde plumbing can be reasoned about in
// isolation. Re-exported here so existing references such as
// `crate::entities::slurm::SlurmSignalSpec` keep working.
//
//   #SBATCH --signal=USR1@60
//   #SBATCH --signal=R:SIGTERM@30
//
// https://slurm.schedmd.com/sbatch.html (`--signal`)
pub use signal::{SignalIdent, SlurmSignalSpec};
```

- [ ] **Step 4: Add the top-level re-export in `entities/slurm.rs`**

Edit `src/entities/slurm.rs`. The current `pub use sbatch_options::{...}` block is:

```rust
pub use sbatch_options::{
    ArrayIndex, DependencyClause, DependencyJobRef, DependencyJoin, DependencyType, JobPartition,
    JobRSC, JobTimeLimit, MailAddress, MailType, MailTypeInput, Memory, MemoryUnit, ResourceSpec,
    ResourceSpecCPU, ResourceSpecGPU, SlurmArraySpec, SlurmDependency, SlurmJobConfig,
};
```

Add `SignalIdent` and `SlurmSignalSpec` in alphabetical order:

```rust
pub use sbatch_options::{
    ArrayIndex, DependencyClause, DependencyJobRef, DependencyJoin, DependencyType, JobPartition,
    JobRSC, JobTimeLimit, MailAddress, MailType, MailTypeInput, Memory, MemoryUnit, ResourceSpec,
    ResourceSpecCPU, ResourceSpecGPU, SignalIdent, SlurmArraySpec, SlurmDependency, SlurmJobConfig,
    SlurmSignalSpec,
};
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test --lib --features pyo3 signal_types_reachable_from_entities_slurm`

Expected: 1 passed.

- [ ] **Step 6: Run full lints**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -10 && cargo fmt --all --check`

Expected: 0 warnings, no diff.

- [ ] **Step 7: Commit**

```bash
git add src/entities/slurm/sbatch_options.rs src/entities/slurm.rs
git commit -m "feat(entities): re-export SlurmSignalSpec at crate::entities::slurm"
```

---

## Task 3: Wire `SbatchCmd::signal` field + `--signal` argv emission

**Files:**
- Modify: `src/sbatch/cmd.rs` (imports, struct, `new()`, `build_argv()`, tests)

**Why:** Now the entity is reachable, wire it into `SbatchCmd`. Argv emission order: `--mail-type` (P2) → `--signal` (P4) → `--no-requeue` (P1).

**Constraints:**
- Field order in struct: `mail_types` → **`signal`** → `env`.
- Argv emission MUST go between `--mail-type` and `--no-requeue`.
- Pre-existing tests must not regress (`full_flags_cpu_variant_argv_layout` byte-identical).

- [ ] **Step 1: Write the failing tests**

Append to the existing `#[cfg(test)] mod tests` block in `src/sbatch/cmd.rs` (after the `export_*` tests added in P3 Task 2):

```rust
    #[test]
    fn signal_name_only_emits_double_dash_signal() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.signal = Some("USR1".parse().unwrap());
        let argv = cmd.build_argv().unwrap();
        let i = argv
            .iter()
            .position(|a| a == "--signal")
            .expect("--signal present");
        assert_eq!(argv[i + 1], "USR1");
    }

    #[test]
    fn signal_with_seconds_renders_at_form() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.signal = Some("USR1@60".parse().unwrap());
        let argv = cmd.build_argv().unwrap();
        let i = argv
            .iter()
            .position(|a| a == "--signal")
            .expect("--signal present");
        assert_eq!(argv[i + 1], "USR1@60");
    }

    #[test]
    fn signal_r_prefix_round_trips_through_argv() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.signal = Some("R:SIGTERM@30".parse().unwrap());
        let argv = cmd.build_argv().unwrap();
        let i = argv
            .iter()
            .position(|a| a == "--signal")
            .expect("--signal present");
        assert_eq!(argv[i + 1], "R:SIGTERM@30");
    }

    #[test]
    fn signal_omitted_when_none() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a == "--signal"));
    }

    #[test]
    fn signal_emits_after_mail_type_and_before_no_requeue() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.mail_types = Some("ALL".to_string().try_into().unwrap());
        cmd.signal = Some("USR1@10".parse().unwrap());
        cmd.no_requeue = true;
        let argv = cmd.build_argv().unwrap();
        let mail_idx = argv.iter().position(|a| a == "--mail-type").unwrap();
        let signal_idx = argv.iter().position(|a| a == "--signal").unwrap();
        let nr_idx = argv.iter().position(|a| a == "--no-requeue").unwrap();
        assert!(
            mail_idx < signal_idx && signal_idx < nr_idx,
            "expected mail < signal < no-requeue, got argv={argv:?}"
        );
    }
```

- [ ] **Step 2: Run the failing test**

Run: `cargo test --lib --features pyo3 signal_name_only_emits_double_dash_signal 2>&1 | tail -10`

Expected: FAIL with `error[E0609]: no field 'signal' on type 'SbatchCmd'`.

- [ ] **Step 3: Add the `signal` field**

Edit `src/sbatch/cmd.rs`. Extend the `use crate::entities::slurm::{...}` import block (line 13). The current line is:

```rust
use crate::entities::slurm::{
    JobPartition, JobTimeLimit, MailAddress, MailTypeInput, ResourceSpec, SlurmDependency,
};
```

Add `SlurmSignalSpec` in alphabetical order:

```rust
use crate::entities::slurm::{
    JobPartition, JobTimeLimit, MailAddress, MailTypeInput, ResourceSpec, SlurmDependency,
    SlurmSignalSpec,
};
```

In the `SbatchCmd` struct, locate the `mail_types` field added in P2 Task 4. Insert `signal` between `mail_types` and `env`:

```rust
    pub mail_types: Option<MailTypeInput>,

    /// `--signal` spec. When `Some`, emitted as `["--signal", spec.to_string()]`
    /// (e.g. `["--signal", "USR1@60"]` or `["--signal", "R:SIGTERM@30"]`).
    /// See [`SlurmSignalSpec`] for the BNF and parsing rules.
    pub signal: Option<SlurmSignalSpec>,

    pub env: HashMap<String, String>,
```

Add the default in `SbatchCmd::new()` between `mail_types: None,` and `env: HashMap::new(),`:

```rust
            mail_types: None,
            signal: None,
            env: HashMap::new(),
```

- [ ] **Step 4: Add the argv emission**

In `build_argv()`, find the `--mail-type` emission (P2 Task 4):

```rust
        if let Some(mts) = &self.mail_types {
            argv.push("--mail-type".to_string());
            argv.push(mts.to_string());
        }
        if self.no_requeue {
            argv.push("--no-requeue".to_string());
        }
```

Insert the signal emission BETWEEN `--mail-type` and `--no-requeue`:

```rust
        if let Some(mts) = &self.mail_types {
            argv.push("--mail-type".to_string());
            argv.push(mts.to_string());
        }
        if let Some(sig) = &self.signal {
            argv.push("--signal".to_string());
            argv.push(sig.to_string());
        }
        if self.no_requeue {
            argv.push("--no-requeue".to_string());
        }
```

- [ ] **Step 5: Run tests**

Run: `cargo test --lib --features pyo3 -- signal_ 2>&1 | tail -15`

Expected: 5 passed (the 5 new tests in Step 1).

Run: `cargo test --lib --features pyo3 sbatch::cmd 2>&1 | tail -10`

Expected: all sbatch::cmd tests pass (`full_flags_cpu_variant_argv_layout` doesn't set `signal`, byte-identical).

- [ ] **Step 6: Run full lints**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -10 && cargo fmt --all --check`

Expected: 0 warnings, no diff.

- [ ] **Step 7: Commit**

```bash
git add src/sbatch/cmd.rs
git commit -m "feat(sbatch): wire --signal via SlurmSignalSpec entity"
```

---

## Task 4: pyo3 binding for `SlurmSignalSpec`

**Files:**
- Create: `src/py_export/entities/slurm/sbatch_options/signal.rs`
- Modify: `src/py_export/entities/slurm/sbatch_options.rs` (add `pub mod signal;` + `#[pymodule_export]`)
- Modify: `src/py_export/sbatch.rs` (extend `use` + add `signal` kwarg)
- Modify: `python/tests/test_sbatch.py` (append smoke test)

**Why:** Python users construct `SlurmSignalSpec` via the parse path. The pyo3 wrapper exposes `.parse(s)` static method and string `__str__`, mirroring `PySlurmDependency`.

**Constraints:**
- `PySlurmSignalSpec` is a thin wrapper around `inner::SlurmSignalSpec`, following the `PySlurmDependency` shape at `src/py_export/entities/slurm/sbatch_options/dependency.rs:259-331`.
- The new submodule lives at `slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options.SlurmSignalSpec` (flat, alongside `MailTypeInput`).

- [ ] **Step 1: Create `src/py_export/entities/slurm/sbatch_options/signal.rs`**

Write this content:

```rust
//! PyO3 wrappers for `entities::slurm::sbatch_options::signal::*`.

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::entities::slurm::sbatch_options::signal as inner;

#[gen_stub_pyclass]
#[pyclass(
    name = "SlurmSignalSpec",
    module = "slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options",
    from_py_object,
    eq
)]
#[derive(Clone, PartialEq, Eq)]
pub struct PySlurmSignalSpec(pub inner::SlurmSignalSpec);

#[gen_stub_pymethods]
#[pymethods]
impl PySlurmSignalSpec {
    /// Parse a Slurm `--signal` spec string, e.g. `"USR1@60"` or `"R:SIGTERM@30"`.
    #[new]
    fn new(s: &str) -> PyResult<Self> {
        s.parse::<inner::SlurmSignalSpec>()
            .map(Self)
            .map_err(Into::into)
    }

    #[staticmethod]
    fn parse(s: &str) -> PyResult<Self> {
        Self::new(s)
    }

    #[getter]
    fn allow_resignal(&self) -> bool {
        self.0.allow_resignal
    }

    #[getter]
    fn signal(&self) -> String {
        // Render the inner SignalIdent as its Display form, so Python sees
        // a uniform string rather than an opaque enum. Round-trips via
        // SlurmSignalSpec.parse.
        self.0.signal.to_string()
    }

    #[getter]
    fn seconds_before_end(&self) -> Option<u16> {
        self.0.seconds_before_end
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!("SlurmSignalSpec({:?})", self.0.to_string())
    }
}

impl From<inner::SlurmSignalSpec> for PySlurmSignalSpec {
    fn from(v: inner::SlurmSignalSpec) -> Self {
        Self(v)
    }
}

impl From<PySlurmSignalSpec> for inner::SlurmSignalSpec {
    fn from(v: PySlurmSignalSpec) -> Self {
        v.0
    }
}
```

Verify `SchemaParseError: Into<PyErr>` exists via: `grep -n 'impl From<.*SchemaParseError' src/error.rs src/py_export/`. If absent, report DONE_WITH_CONCERNS.

- [ ] **Step 2: Register the submodule in `src/py_export/entities/slurm/sbatch_options.rs`**

The current file has `pub mod array_spec; pub mod config; pub mod dependency; pub mod resource_spec; pub mod time_limit;` at lines 6-10. Add `pub mod signal;` in alphabetical order:

```rust
pub mod array_spec;
pub mod config;
pub mod dependency;
pub mod resource_spec;
pub mod signal;
pub mod time_limit;
```

Then inside the `#[pymodule(name = "sbatch_options")] pub(crate) mod inner_module { ... }` block (lines 14-50), add a new `#[pymodule_export]` block (placed after `dependency`, before `array_spec`):

```rust
    #[pymodule_export]
    use super::signal::PySlurmSignalSpec;
```

- [ ] **Step 3: Verify the binding builds**

Run: `cargo build --features pyo3 2>&1 | tail -20`

Expected: succeeds.

- [ ] **Step 4: Write the failing Python smoke test**

Append to `python/tests/test_sbatch.py`:

```python
def test_sbatch_cmd_signal_kwarg(tmp_path):
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        SlurmSignalSpec,
    )

    job = tmp_path / "job.sh"
    job.write_text("#!/usr/bin/env bash\necho hi\n")
    cmd = SbatchCmd(str(job), signal=SlurmSignalSpec.parse("USR1@60"))
    argv = cmd.build_argv()
    i = argv.index("--signal")
    assert argv[i + 1] == "USR1@60"
```

- [ ] **Step 5: Run the failing test**

Run: `uv run maturin develop --features pyo3 2>&1 | tail -3`

Expected: build succeeds.

Run: `uv run pytest python/tests/test_sbatch.py::test_sbatch_cmd_signal_kwarg -v 2>&1 | tail -10`

Expected: FAIL with `TypeError: SbatchCmd.__init__() got an unexpected keyword argument 'signal'`.

- [ ] **Step 6: Add `signal` kwarg to `PySbatchCmd::new`**

Edit `src/py_export/sbatch.rs`. The current `use crate::entities::slurm::{...}` line is:

```rust
use crate::entities::slurm::{JobTimeLimit, MailTypeInput, ResourceSpec, SlurmDependency};
```

Add `SlurmSignalSpec` alphabetically:

```rust
use crate::entities::slurm::{JobTimeLimit, MailTypeInput, ResourceSpec, SlurmDependency, SlurmSignalSpec};
```

Add a `use` line beneath the existing PySlurmDependency import:

```rust
use crate::py_export::entities::slurm::sbatch_options::signal::PySlurmSignalSpec;
```

Edit `PySbatchCmd::new`. After the `mail_types` parameter, add `signal`:

In `#[pyo3(signature = (...))]`:

```rust
        mail_types = None,
        signal = None,
```

In the parameter list:

```rust
        mail_types: Option<PyMailTypeInput>,
        signal: Option<PySlurmSignalSpec>,
```

In the body, after the `cmd.mail_types = ...` assignment:

```rust
        cmd.signal = signal.map(<PySlurmSignalSpec as Into<SlurmSignalSpec>>::into);
```

- [ ] **Step 7: Rebuild and run the smoke test**

```bash
uv run maturin develop --features pyo3 2>&1 | tail -3
uv run pytest python/tests/test_sbatch.py::test_sbatch_cmd_signal_kwarg -v 2>&1 | tail -10
```

Expected: 1 passed.

- [ ] **Step 8: Run full lints**

```bash
cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -10
cargo fmt --all --check
uv run ruff check python/
```

Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add src/py_export/entities/slurm/sbatch_options/signal.rs src/py_export/entities/slurm/sbatch_options.rs src/py_export/sbatch.rs python/tests/test_sbatch.py
git commit -m "feat(py): expose SlurmSignalSpec + signal kwarg on SbatchCmd"
```

---

## Task 5: `.pyi` sync

**Files:**
- Modify: `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi` (add `signal` kwarg)

**Why:** Mirror the new pyo3 kwarg in the hand-written `.pyi` stub so type checkers see it.

- [ ] **Step 1: Read the current `.pyi`**

Use the Read tool on `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi`. Confirm the current `TYPE_CHECKING` block imports `MailTypeInput` and `SlurmDependency` and the `SbatchCmd.__init__` signature ends with `mail_types: "MailTypeInput | None" = None,`.

- [ ] **Step 2: Extend the `TYPE_CHECKING` block**

Add `SlurmSignalSpec`:

```python
if TYPE_CHECKING:
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        MailTypeInput,
        SlurmDependency,
        SlurmSignalSpec,
    )
```

- [ ] **Step 3: Add the `signal` kwarg to `SbatchCmd.__init__`**

The current `SbatchCmd.__init__` ends with:

```python
        mail_types: "MailTypeInput | None" = None,
    ) -> None: ...
```

Insert `signal` between `mail_types` and the closing `)`:

```python
        mail_types: "MailTypeInput | None" = None,
        signal: "SlurmSignalSpec | None" = None,
    ) -> None: ...
```

- [ ] **Step 4: Smoke-import to verify the `.pyi` parses and the runtime kwarg is accepted**

Run:

```bash
uv run python -c "
import slurm_async_runner._slurm_async_runner_core.sbatch as m
from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import SlurmSignalSpec
cmd = m.SbatchCmd('/tmp/job.sh', signal=SlurmSignalSpec.parse('USR1@60'))
print('OK', cmd)
"
```

Expected: prints `OK <...>`.

- [ ] **Step 5: Run the full Python test suite**

```bash
uv run pytest python/tests/ -v 2>&1 | tail -15
uv run ruff check python/
```

Expected: all pass; 0 ruff errors.

- [ ] **Step 6: Commit**

```bash
git add python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi
git commit -m "docs(py): sync .pyi for signal kwarg on SbatchCmd"
```

---

## Task 6: CHANGELOG + final validation

**Files:**
- Modify: `CHANGELOG.md` (append `### Added (Phase 2 P4)` block)

- [ ] **Step 1: Insert the P4 section**

Open `CHANGELOG.md`. The current `[Unreleased]` block starts with `### Added (Phase 2 P3)`. Insert a new `### Added (Phase 2 P4)` block IMMEDIATELY after the line `## [Unreleased]` and BEFORE the existing `### Added (Phase 2 P3)` block. Use this content:

```markdown
### Added (Phase 2 P4)

- **`crate::entities::slurm::SlurmSignalSpec`** + **`SignalIdent`** — new
  entity modeling SLURM's `--signal=[R:]<sig_num|sig_name>[@<sig_time>]` BNF.
  `FromStr` accepts: `"USR1"`, `"15"`, `"USR1@60"`, `"R:USR1"`,
  `"R:SIGTERM@30"`, `"R:9@5"`. Rejects: empty, lowercase `r:`,
  signal number outside `1..=64`, seconds zero, seconds above `u16::MAX`,
  empty signal, signal names with non-uppercase/non-digit/non-underscore
  characters. `Display` round-trips with `FromStr`. `serde::Serialize` /
  `Deserialize` via the string form.
- **`SbatchCmd::signal: Option<SlurmSignalSpec>`** — emits
  `["--signal", spec.to_string()]` between `--mail-type` and `--no-requeue`.
  Python:
  `PySbatchCmd(..., signal=SlurmSignalSpec.parse("USR1@60"))`.
- **`PySlurmSignalSpec`** pyo3 wrapper at
  `slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options.SlurmSignalSpec`
  exposes `parse(s)` static method, `__str__`, and getters
  `allow_resignal`, `signal` (rendered Display form), `seconds_before_end`.

```

(Note the trailing blank line.)

- [ ] **Step 2: Run the full validation gate**

```bash
cargo fmt --all --check
cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -5
cargo test --lib --features pyo3 2>&1 | tail -10
uv run maturin develop --features pyo3 2>&1 | tail -3
uv run pytest python/tests/ 2>&1 | tail -10
uv run ruff check python/
```

Expected:
- `cargo fmt`: clean
- `cargo clippy`: 0 warnings
- `cargo test --lib --features pyo3`: ≈ 325 passing (P3 baseline 300 + 19 from signal entity + 5 from cmd + 1 reachability = 325; small fluctuation OK)
- `maturin develop`: build succeeds
- `pytest`: ≈ 36 passing (P3 baseline 35 + 1 new from Task 4 Step 4)
- `ruff`: 0 errors

- [ ] **Step 3: Verify no regression on the existing argv layout test**

Run: `cargo test --lib --features pyo3 full_flags_cpu_variant_argv_layout -- --exact`

Expected: PASS. `signal` defaults to `None`, byte-identical argv.

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): record Phase 2 P4 SlurmSignalSpec + --signal wiring"
```

- [ ] **Step 5: Sanity-check the commit graph**

Run: `git log --oneline 156f365..HEAD`

Expected: 6 new commits on top of the P3 head:
```
<sha> docs(changelog): record Phase 2 P4 SlurmSignalSpec + --signal wiring
<sha> docs(py): sync .pyi for signal kwarg on SbatchCmd
<sha> feat(py): expose SlurmSignalSpec + signal kwarg on SbatchCmd
<sha> feat(sbatch): wire --signal via SlurmSignalSpec entity
<sha> feat(entities): re-export SlurmSignalSpec at crate::entities::slurm
<sha> feat(entities): add SlurmSignalSpec entity with FromStr/Display/serde
```

---

## Self-Review Coverage

Spec §4.6 (`--signal` typed 化) → Tasks 1 + 2 + 3 + 4 + 5 + 6.
Spec §2.1 (vocab single-source) → new `SlurmSignalSpec` lives in `entities::slurm::sbatch_options::signal`; `crate::sbatch::cmd` only imports.
Spec §2.2 invariants: no new `JobDispatcher` method, no new `JobState` variant, no new kind string, no sacct calls.
Spec §11 PR checklist: CHANGELOG updated (Task 6), `.pyi` synced (Task 5), full test/lint pass (Task 6).

## Dependencies

P4 is independent of P5 / P6. Stacked on P3 (`156f365`).
