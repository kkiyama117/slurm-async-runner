//! Pure-data spec for one `sbatch` invocation.
//!
//! No I/O — all subprocess work is in [`crate::sbatch::manager::SbatchManager`]
//! / [`crate::dispatcher::JobDispatcher`]. The argv is laid out so that
//! `#SBATCH` directives in the script (which sbatch parses on its own)
//! are still respected; CLI flags only override per the sbatch convention.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;

use crate::entities::slurm::{JobPartition, JobTimeLimit, ResourceSpec, SlurmDependency};
use crate::util::path::absolutize;

#[derive(Debug, Clone)]
pub struct SbatchCmd {
    pub sbatch_bin: String,

    pub job_name: Option<String>,
    pub partition: Option<JobPartition>,

    pub time_limit: Option<JobTimeLimit>,
    pub rsc: Option<ResourceSpec>,

    pub output: Option<String>,
    pub error: Option<String>,
    pub chdir: Option<PathBuf>,

    /// `--dependency` (`-d`) spec. When `Some`, emitted as `["-d", dep.to_string()]`
    /// (e.g. `["-d", "afterok:200,afterany:201"]`).
    pub dependency: Option<SlurmDependency>,

    pub env: HashMap<String, String>,

    /// `--no-requeue` flag. When `true`, the job is not requeued on node failure.
    pub no_requeue: bool,

    /// `--comment` flag value. When `Some`, emitted as `--comment <value>`.
    pub comment: Option<String>,

    pub script: PathBuf,
    pub args: Vec<String>,
}

impl SbatchCmd {
    pub fn new(script: impl Into<PathBuf>) -> Self {
        Self {
            sbatch_bin: "sbatch".to_string(),
            job_name: None,
            partition: None,
            time_limit: None,
            rsc: None,
            output: None,
            error: None,
            chdir: None,
            dependency: None,
            env: HashMap::new(),
            no_requeue: false,
            comment: None,
            script: script.into(),
            args: Vec::new(),
        }
    }

    pub fn build_argv(&self) -> Result<Vec<String>> {
        let mut argv = Vec::with_capacity(16 + self.args.len());
        argv.push(self.sbatch_bin.clone());

        if let Some(name) = &self.job_name {
            argv.push("-J".to_string());
            argv.push(name.clone());
        }
        if let Some(p) = &self.partition {
            argv.push("-p".to_string());
            argv.push(p.clone());
        }
        if let Some(t) = &self.time_limit {
            argv.push("-t".to_string());
            argv.push(t.to_string());
        }
        if let Some(r) = &self.rsc {
            let spec = r.to_string();
            if !spec.is_empty() {
                argv.push("--rsc".to_string());
                argv.push(spec);
            }
        }
        if let Some(o) = &self.output {
            argv.push("-o".to_string());
            argv.push(o.clone());
        }
        if let Some(e) = &self.error {
            argv.push("-e".to_string());
            argv.push(e.clone());
        }
        if let Some(c) = &self.chdir {
            argv.push("--chdir".to_string());
            argv.push(absolutize(c)?);
        }
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
        if let Some(c) = &self.comment {
            argv.push("--comment".to_string());
            argv.push(c.clone());
        }
        argv.push(absolutize(&self.script)?);
        argv.extend(self.args.iter().cloned());
        Ok(argv)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::slurm::{ResourceSpecCPU, ResourceSpecGPU};
    use std::num::NonZeroU32;

    #[test]
    fn minimal_argv_is_bin_then_script() {
        let cmd = SbatchCmd::new("/work/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert_eq!(argv, vec!["sbatch".to_string(), "/work/job.sh".to_string()]);
    }

    #[test]
    fn relative_script_is_absolutized() {
        let cmd = SbatchCmd::new("job.sh");
        let argv = cmd.build_argv().unwrap();
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(argv[0], "sbatch");
        assert_eq!(argv[1], format!("{}/job.sh", cwd.display()));
    }

    #[test]
    fn full_flags_cpu_variant_argv_layout() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.job_name = Some("g09run".into());
        cmd.partition = Some("gr19999b".into());
        cmd.time_limit = Some("1:0:0".parse().unwrap());
        cmd.rsc = Some(ResourceSpec::CPU(ResourceSpecCPU {
            p: NonZeroU32::new(4),
            t: NonZeroU32::new(8),
            c: NonZeroU32::new(8),
            m: Some("2G".parse().unwrap()),
        }));
        cmd.output = Some("slurm-%j.out".into());
        cmd.error = Some("slurm-%j.err".into());
        cmd.chdir = Some(PathBuf::from("/w"));
        cmd.env.insert("OMP_NUM_THREADS".into(), "8".into());
        cmd.env.insert("FOO".into(), "bar".into());
        cmd.args = vec!["--flag".into(), "v".into()];
        let argv = cmd.build_argv().unwrap();
        assert_eq!(
            argv,
            vec![
                "sbatch".to_string(),
                "-J".into(),
                "g09run".into(),
                "-p".into(),
                "gr19999b".into(),
                "-t".into(),
                "01:00:00".into(),
                "--rsc".into(),
                "p=4:t=8:c=8:m=2G".into(),
                "-o".into(),
                "slurm-%j.out".into(),
                "-e".into(),
                "slurm-%j.err".into(),
                "--chdir".into(),
                "/w".into(),
                "--export=ALL,FOO=bar,OMP_NUM_THREADS=8".into(),
                "/w/job.sh".into(),
                "--flag".into(),
                "v".into(),
            ]
        );
    }

    #[test]
    fn empty_env_omits_export_flag() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a.starts_with("--export")));
    }

    #[test]
    fn rsc_empty_cpu_omits_rsc_flag() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.rsc = Some(ResourceSpec::CPU(ResourceSpecCPU::default()));
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a == "--rsc"));
    }

    #[test]
    fn gpu_variant_renders_g_flag() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.rsc = Some(ResourceSpec::GPU(ResourceSpecGPU {
            g: NonZeroU32::new(1).unwrap(),
        }));
        let argv = cmd.build_argv().unwrap();
        assert!(argv.contains(&"--rsc".to_string()));
        assert!(argv.contains(&"g=1".to_string()));
    }

    #[test]
    fn no_requeue_flag_is_emitted_when_true() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.no_requeue = true;
        let argv = cmd.build_argv().unwrap();
        assert!(argv.iter().any(|a| a == "--no-requeue"));
    }

    #[test]
    fn no_requeue_flag_is_omitted_when_false() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a == "--no-requeue"));
    }

    #[test]
    fn comment_flag_emits_value() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.comment = Some("post-deadline rerun".to_string());
        let argv = cmd.build_argv().unwrap();
        let i = argv
            .iter()
            .position(|a| a == "--comment")
            .expect("--comment present");
        assert_eq!(argv[i + 1], "post-deadline rerun");
    }

    #[test]
    fn comment_omitted_when_none() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a == "--comment"));
    }

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
}
