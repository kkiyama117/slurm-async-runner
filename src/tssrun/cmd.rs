//! Pure-data spec types for one `tssrun` invocation:
//!
//! - [`TssrunCmd`] — full argv spec including binary path, optional
//!   partition / time-limit / x11 / explicit env / cwd, plus
//!   [`TssrunCmd::build_argv`] which produces the argv that
//!   [`crate::dispatcher::TokioBackgroundDispatcher`] will hand to
//!   `tokio::process::Command`.
//!
//! The `--rsc` spec is the crate-local
//! [`crate::entities::slurm::ResourceSpec`] (CPU / GPU
//! enum); the wall-clock limit is the crate-local
//! [`crate::entities::slurm::JobTimeLimit`].
//!
//! No I/O happens in this module — all subprocess work is in the
//! dispatcher / handle / manager.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;

use crate::entities::slurm::{JobPartition, JobTimeLimit, ResourceSpec};
use crate::util::path::absolutize;

/// Spec for a single `tssrun` invocation. Pure data + an argv builder —
/// no subprocess work. Mirrors [`crate::manager::SlurmCmd`] in spirit but
/// represents the kudpc-manual options as typed fields.
#[derive(Debug, Clone)]
pub struct TssrunCmd {
    pub tssrun_bin: String,
    /// Renamed from `queue` to match Slurm's `--partition` vocabulary.
    /// `JobPartition` is a `String` alias from `crate::entities::slurm`.
    pub partition: Option<JobPartition>,
    /// Validated wall-clock limit. Was `Option<String>` previously.
    pub time_limit: Option<JobTimeLimit>,
    /// Validated `--rsc` spec. Was the local `Resource` previously.
    pub rsc: Option<ResourceSpec>,
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
            partition: None,
            time_limit: None,
            rsc: None,
            x11: false,
            program: program.into(),
            args: Vec::new(),
            env: HashMap::new(),
            cwd: None,
        }
    }

    /// Build the argv: `[bin, -p PARTITION?, -t TIME?, --rsc SPEC?, --x11?, program_abs, args…]`.
    pub fn build_argv(&self) -> Result<Vec<String>> {
        // Maximum prelude slots: bin + (-p PARTITION) + (-t TIME) + (--rsc SPEC)
        // + --x11 + program = 8.
        const MAX_PRELUDE_SLOTS: usize = 8;
        let mut argv: Vec<String> = Vec::with_capacity(MAX_PRELUDE_SLOTS + self.args.len());
        argv.push(self.tssrun_bin.clone());

        if let Some(p) = &self.partition {
            argv.push("-p".to_string());
            argv.push(p.clone());
        }
        if let Some(t) = &self.time_limit {
            argv.push("-t".to_string());
            argv.push(t.to_string());
        }
        if let Some(r) = &self.rsc {
            // Display emits "" for the all-None CPU case; treat that
            // identically to `rsc: None` (omit `--rsc` entirely).
            let spec = r.to_string();
            if !spec.is_empty() {
                argv.push("--rsc".to_string());
                argv.push(spec);
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::slurm::{ResourceSpecCPU, ResourceSpecGPU};

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
    fn cmd_full_flags_cpu_variant() {
        use std::num::NonZeroU32;
        let mut c = TssrunCmd::new("/work/job.sh");
        c.partition = Some("gr19999b".into());
        c.time_limit = Some("1:0:0".parse().unwrap());
        c.rsc = Some(ResourceSpec::CPU(ResourceSpecCPU {
            p: NonZeroU32::new(4),
            t: NonZeroU32::new(8),
            c: NonZeroU32::new(8),
            m: Some("2G".parse().unwrap()),
        }));
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
                "01:00:00".to_string(),
                "--rsc".to_string(),
                "p=4:t=8:c=8:m=2G".to_string(),
                "--x11".to_string(),
                "/work/job.sh".to_string(),
                "--flag".to_string(),
                "value".to_string(),
            ]
        );
    }

    #[test]
    fn cmd_full_flags_gpu_variant() {
        use std::num::NonZeroU32;
        let mut c = TssrunCmd::new("/work/job.sh");
        c.rsc = Some(ResourceSpec::GPU(ResourceSpecGPU {
            g: NonZeroU32::new(1).unwrap(),
        }));
        let argv = c.build_argv().unwrap();
        assert!(argv.contains(&"--rsc".to_string()));
        assert!(argv.contains(&"g=1".to_string()));
    }

    #[test]
    fn cmd_rsc_partial_cpu_emits_only_some_keys() {
        use std::num::NonZeroU32;
        let mut c = TssrunCmd::new("/work/job.sh");
        c.rsc = Some(ResourceSpec::CPU(ResourceSpecCPU {
            p: NonZeroU32::new(4),
            m: Some("2G".parse().unwrap()),
            ..Default::default()
        }));
        let argv = c.build_argv().unwrap();
        assert!(argv.contains(&"--rsc".to_string()));
        assert!(argv.contains(&"p=4:m=2G".to_string()));
    }

    #[test]
    fn cmd_rsc_empty_cpu_omits_flag() {
        // Some(ResourceSpec::CPU(default)) renders to "" → omit --rsc.
        let mut c = TssrunCmd::new("/work/job.sh");
        c.rsc = Some(ResourceSpec::CPU(ResourceSpecCPU::default()));
        let argv = c.build_argv().unwrap();
        assert!(!argv.contains(&"--rsc".to_string()));
    }

    #[test]
    fn cmd_rsc_none_omits_flag() {
        let c = TssrunCmd::new("/work/job.sh");
        let argv = c.build_argv().unwrap();
        assert!(!argv.contains(&"--rsc".to_string()));
    }
}
