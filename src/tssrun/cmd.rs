//! Spec types: [`Resource`] and `TssrunCmd` with argv builder. No I/O.

/// Resource spec passed to `tssrun --rsc p=:t=:c=:m=:g=`.
///
/// All fields are optional; `render` only emits keys whose value is `Some`.
/// Order is fixed: `p`, `t`, `c`, `m`, `g`. The `memory` field is a free
/// string (`"2G"`, `"512M"`, etc.) since SLURM accepts unit suffixes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resource {
    pub processes: Option<u32>,
    pub threads: Option<u32>,
    pub cores: Option<u32>,
    pub memory: Option<String>,
    pub gpus: Option<u32>,
}

impl Resource {
    /// Renders the colon-joined `p=…:t=…:m=…:g=…` string.
    /// Returns `None` if every field is `None`.
    pub fn render(&self) -> Option<String> {
        let mut parts: Vec<String> = Vec::with_capacity(5);
        if let Some(p) = self.processes {
            parts.push(format!("p={p}"));
        }
        if let Some(t) = self.threads {
            parts.push(format!("t={t}"));
        }
        if let Some(c) = self.cores {
            parts.push(format!("c={c}"));
        }
        if let Some(m) = &self.memory {
            parts.push(format!("m={m}"));
        }
        if let Some(g) = self.gpus {
            parts.push(format!("g={g}"));
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join(":"))
        }
    }
}

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Spec for a single `tssrun` invocation. Pure data + an argv builder —
/// no subprocess work. Mirrors [`crate::manager::SlurmCmd`] in spirit but
/// represents the kudpc-manual options as typed fields.
#[derive(Debug, Clone)]
pub struct TssrunCmd {
    pub tssrun_bin: String,
    pub queue: Option<String>,
    pub time_limit: Option<String>,
    pub rsc: Option<Resource>,
    pub x11: bool,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub cwd: Option<PathBuf>,
}

impl TssrunCmd {
    /// Construct with defaults: `tssrun_bin = "tssrun"`, no flags, given program.
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            tssrun_bin: "tssrun".to_string(),
            queue: None,
            time_limit: None,
            rsc: None,
            x11: false,
            program: program.into(),
            args: Vec::new(),
            env: HashMap::new(),
            cwd: None,
        }
    }

    /// Build the argv: `[bin, -p QUEUE?, -t TIME?, --rsc SPEC?, --x11?, program_abs, args…]`.
    pub fn build_argv(&self) -> Result<Vec<String>> {
        // Maximum prelude slots: bin + (-p QUEUE) + (-t TIME) + (--rsc SPEC)
        // + --x11 + program = 8. All flags except the bin and program are
        // optional, so the actual length will usually be smaller.
        const MAX_PRELUDE_SLOTS: usize = 8;
        let mut argv: Vec<String> = Vec::with_capacity(MAX_PRELUDE_SLOTS + self.args.len());
        argv.push(self.tssrun_bin.clone());

        if let Some(q) = &self.queue {
            argv.push("-p".to_string());
            argv.push(q.clone());
        }
        if let Some(t) = &self.time_limit {
            argv.push("-t".to_string());
            argv.push(t.clone());
        }
        if let Some(r) = &self.rsc
            && let Some(spec) = r.render()
        {
            argv.push("--rsc".to_string());
            argv.push(spec);
        }
        if self.x11 {
            argv.push("--x11".to_string());
        }

        argv.push(absolutize(&self.program)?);

        for a in &self.args {
            argv.push(a.clone());
        }
        Ok(argv)
    }
}

fn absolutize(p: &Path) -> Result<String> {
    let abs =
        std::path::absolute(p).with_context(|| format!("failed to absolutize {}", p.display()))?;
    abs.into_os_string()
        .into_string()
        .map_err(|os| anyhow::anyhow!("non-UTF8 program path: {os:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_default_renders_none() {
        assert_eq!(Resource::default().render(), None);
    }

    #[test]
    fn resource_full_renders_in_order() {
        let r = Resource {
            processes: Some(4),
            threads: Some(8),
            cores: Some(8),
            memory: Some("2G".into()),
            gpus: Some(1),
        };
        assert_eq!(r.render().as_deref(), Some("p=4:t=8:c=8:m=2G:g=1"));
    }

    #[test]
    fn resource_partial_skips_none_keys() {
        let r = Resource {
            processes: Some(4),
            memory: Some("2G".into()),
            ..Default::default()
        };
        assert_eq!(r.render().as_deref(), Some("p=4:m=2G"));
    }

    #[test]
    fn cmd_minimal_argv_is_bin_then_program() {
        let c = TssrunCmd::new("/work/job.sh");
        let argv = c.build_argv().unwrap();
        assert_eq!(argv, vec!["tssrun".to_string(), "/work/job.sh".to_string()]);
    }

    #[test]
    fn cmd_relative_program_is_absolutized() {
        let c = TssrunCmd::new("job.sh");
        let argv = c.build_argv().unwrap();
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(argv[0], "tssrun");
        assert_eq!(argv[1], format!("{}/job.sh", cwd.display()));
    }

    #[test]
    fn cmd_full_flags_in_documented_order() {
        let mut c = TssrunCmd::new("/work/job.sh");
        c.queue = Some("gr19999b".into());
        c.time_limit = Some("1:0:0".into());
        c.rsc = Some(Resource {
            processes: Some(4),
            threads: Some(8),
            cores: Some(8),
            memory: Some("2G".into()),
            gpus: Some(1),
        });
        c.x11 = true;
        c.args = vec!["--flag".into(), "value".into()];
        let argv = c.build_argv().unwrap();
        assert_eq!(
            argv,
            vec![
                "tssrun".to_string(),
                "-p".to_string(),
                "gr19999b".to_string(),
                "-t".to_string(),
                "1:0:0".to_string(),
                "--rsc".to_string(),
                "p=4:t=8:c=8:m=2G:g=1".to_string(),
                "--x11".to_string(),
                "/work/job.sh".to_string(),
                "--flag".to_string(),
                "value".to_string(),
            ]
        );
    }

    #[test]
    fn cmd_rsc_with_only_some_keys() {
        let mut c = TssrunCmd::new("/work/job.sh");
        c.rsc = Some(Resource {
            processes: Some(4),
            memory: Some("2G".into()),
            ..Default::default()
        });
        let argv = c.build_argv().unwrap();
        assert!(argv.contains(&"--rsc".to_string()));
        assert!(argv.contains(&"p=4:m=2G".to_string()));
    }

    #[test]
    fn cmd_rsc_all_none_omits_flag_entirely() {
        let mut c = TssrunCmd::new("/work/job.sh");
        c.rsc = Some(Resource::default());
        let argv = c.build_argv().unwrap();
        assert!(!argv.contains(&"--rsc".to_string()));
    }
}
