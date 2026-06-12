use super::*;
use crate::dispatcher::CaptureOutput;

/// stdout-only success [`CaptureOutput`] — the common canned shape.
fn cap_ok(stdout: &str) -> CaptureOutput {
    CaptureOutput {
        exit_code: 0,
        stdout: stdout.to_string(),
        stderr: String::new(),
    }
}

// ---- parse_squeue ----

#[test]
fn parse_squeue_three_field_rows() {
    let text = "100 PENDING Priority\n200 RUNNING None\n";
    let m = parse_squeue(text);
    assert_eq!(m.len(), 2);
    assert_eq!(
        m[&100],
        JobStatus::with_reason(JobState::Pending, JobReason::Priority)
    );
    assert_eq!(m[&200], JobStatus::new(JobState::Running));
}

#[test]
fn parse_squeue_compact_state_codes() {
    let text = "1 PD Priority\n2 R None\n3 OOM OutOfMemory\n";
    let m = parse_squeue(text);
    assert_eq!(m[&1].state, JobState::Pending);
    assert_eq!(m[&2].state, JobState::Running);
    assert_eq!(m[&3].state, JobState::OutOfMemory);
    assert_eq!(m[&3].reason, JobReason::OutOfMemory);
}

#[test]
fn parse_squeue_missing_reason_defaults_to_none() {
    let text = "42 RUNNING\n";
    let m = parse_squeue(text);
    assert_eq!(m[&42], JobStatus::new(JobState::Running));
}

#[test]
fn parse_squeue_skips_malformed_and_empty() {
    let text = "\n\
                    notanid PENDING Priority\n\
                    just-one-token\n\
                    \n\
                    777 RUNNING None\n";
    let m = parse_squeue(text);
    assert_eq!(m.len(), 1);
    assert!(m.contains_key(&777));
}

#[test]
fn parse_squeue_unknown_state_falls_back() {
    // Forward-compat: a future SLURM version emits a state we
    // don't know yet — must round-trip to JobState::Unknown.
    let text = "999 FUTURE_STATE FutureReason\n";
    let m = parse_squeue(text);
    assert_eq!(m[&999].state, JobState::Unknown);
    assert!(matches!(
        m[&999].reason,
        JobReason::Other(ref s) if s == "FutureReason"
    ));
}

// ---- parse_sacct ----

#[test]
fn parse_sacct_three_field_rows() {
    let text = "100|COMPLETED|None\n200|FAILED|NonZeroExitCode\n";
    let m = parse_sacct(text);
    assert_eq!(m.len(), 2);
    assert_eq!(m[&100], JobStatus::new(JobState::Completed));
    assert_eq!(
        m[&200],
        JobStatus::with_reason(JobState::Failed, JobReason::NonZeroExitCode)
    );
}

#[test]
fn parse_sacct_filters_step_rows() {
    let text = "100|COMPLETED|None\n\
                    100.batch|COMPLETED|None\n\
                    100.extern|COMPLETED|None\n\
                    100.0|COMPLETED|None\n";
    let m = parse_sacct(text);
    assert_eq!(m.len(), 1);
    assert!(m.contains_key(&100));
}

#[test]
fn parse_sacct_handles_trailing_context_in_state() {
    // sacct prints `CANCELLED by 1234` for the State column. The
    // upstream JobState::parse trims to the first whitespace token.
    let text = "100|CANCELLED by 1234|None\n";
    let m = parse_sacct(text);
    assert_eq!(m[&100].state, JobState::Cancelled);
}

#[test]
fn parse_sacct_two_field_rows_default_reason_to_none() {
    let text = "42|COMPLETED\n";
    let m = parse_sacct(text);
    assert_eq!(m[&42], JobStatus::new(JobState::Completed));
}

#[test]
fn parse_sacct_skips_lines_without_pipe() {
    let text = "no-pipe-here\n100|COMPLETED|None\n";
    let m = parse_sacct(text);
    assert_eq!(m.len(), 1);
    assert!(m.contains_key(&100));
}

// ---- parse_sacct_with_exit_code ----

#[test]
fn parse_sacct_with_exit_code_three_fields_completed() {
    let text = "12345|COMPLETED|None|0:0\n";
    let m = parse_sacct_with_exit_code(text);
    let oc = m.get(&12345).expect("jobid present");
    assert_eq!(oc.status.state, JobState::Completed);
    assert_eq!(oc.exit_code, Some(0));
}

#[test]
fn parse_sacct_with_exit_code_signaled() {
    let text = "12345|CANCELLED by 1001|None|0:9\n";
    let m = parse_sacct_with_exit_code(text);
    let oc = m.get(&12345).expect("jobid present");
    assert_eq!(oc.status.state, JobState::Cancelled);
    assert_eq!(oc.exit_code, Some(137));
}

#[test]
fn parse_sacct_with_exit_code_filters_step_rows() {
    let text = "12345|COMPLETED|None|0:0\n12345.batch|COMPLETED|None|0:0\n";
    let m = parse_sacct_with_exit_code(text);
    assert_eq!(m.len(), 1);
    assert!(m.contains_key(&12345));
}

#[test]
fn parse_sacct_with_exit_code_handles_missing_exit_field() {
    let text = "12345|COMPLETED|None\n";
    let m = parse_sacct_with_exit_code(text);
    let oc = m.get(&12345).expect("jobid present");
    assert_eq!(oc.exit_code, None);
    assert_eq!(oc.status.state, JobState::Completed);
}

// ---- query_job_states_with_exit_code_with ----

#[tokio::test]
async fn query_with_exit_code_squeue_only_reports_no_exit_code() {
    struct D;
    impl crate::dispatcher::JobDispatcher for D {
        async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
            unimplemented!()
        }
        async fn capture(&self, argv: &[String]) -> anyhow::Result<CaptureOutput> {
            let bin = argv[0].as_str();
            let out = if bin == "squeue" {
                "12345 RUNNING None\n"
            } else {
                ""
            };
            Ok(cap_ok(out))
        }
    }
    let m = query_job_states_with_exit_code_with(&D, &[12345])
        .await
        .unwrap();
    let oc = m.get(&12345).unwrap();
    assert_eq!(oc.status.state, JobState::Running);
    assert_eq!(oc.exit_code, None);
}

#[tokio::test]
async fn query_with_exit_code_sacct_supplies_exit_code() {
    struct D;
    impl crate::dispatcher::JobDispatcher for D {
        async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
            unimplemented!()
        }
        async fn capture(&self, argv: &[String]) -> anyhow::Result<CaptureOutput> {
            let bin = argv[0].as_str();
            let out = if bin == "squeue" {
                ""
            } else if bin == "sacct" {
                let format_idx = argv.iter().position(|a| a == "-o").unwrap();
                assert!(
                    argv[format_idx + 1].contains("ExitCode"),
                    "sacct argv must include ExitCode column, got: {:?}",
                    argv
                );
                "12345|COMPLETED|None|0:0\n"
            } else {
                ""
            };
            Ok(cap_ok(out))
        }
    }
    let m = query_job_states_with_exit_code_with(&D, &[12345])
        .await
        .unwrap();
    let oc = m.get(&12345).unwrap();
    assert_eq!(oc.status.state, JobState::Completed);
    assert_eq!(oc.exit_code, Some(0));
}

#[tokio::test]
async fn query_with_exit_code_missing_id_defaults_to_unknown() {
    struct D;
    impl crate::dispatcher::JobDispatcher for D {
        async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
            unimplemented!()
        }
        async fn capture(&self, _argv: &[String]) -> anyhow::Result<CaptureOutput> {
            // Both squeue and sacct return empty — id 99999 is unknown to both
            Ok(CaptureOutput::default())
        }
    }
    let m = query_job_states_with_exit_code_with(&D, &[99999])
        .await
        .unwrap();
    let oc = m
        .get(&99999)
        .expect("missing id must still appear in returned map");
    assert_eq!(oc.status, JobStatus::default());
    assert_eq!(oc.status.state, JobState::Unknown);
    assert_eq!(oc.status.reason, JobReason::None);
    assert_eq!(oc.exit_code, None);
}

// ---- merge_results ----

#[test]
fn merge_active_wins_over_history() {
    let active = HashMap::from([(
        1,
        JobStatus::with_reason(JobState::Running, JobReason::None),
    )]);
    let history = HashMap::from([(
        1,
        JobStatus::with_reason(JobState::Completed, JobReason::None),
    )]);
    let m = merge_results(&[1], &active, &history);
    assert_eq!(m[&1].state, JobState::Running);
}

#[test]
fn merge_falls_back_to_history() {
    let active = HashMap::new();
    let history = HashMap::from([(7, JobStatus::new(JobState::Completed))]);
    let m = merge_results(&[7], &active, &history);
    assert_eq!(m[&7].state, JobState::Completed);
}

#[test]
fn merge_unknown_for_missing() {
    let m = merge_results(&[42], &HashMap::new(), &HashMap::new());
    assert_eq!(m[&42], JobStatus::default());
    assert_eq!(m[&42].state, JobState::Unknown);
    assert_eq!(m[&42].reason, JobReason::None);
}

#[test]
fn merge_returns_every_input_id() {
    // Contract: every input id appears as a key in the result.
    let active = HashMap::from([(1, JobStatus::new(JobState::Running))]);
    let history = HashMap::from([(2, JobStatus::new(JobState::Completed))]);
    let m = merge_results(&[1, 2, 3], &active, &history);
    assert_eq!(m.len(), 3);
    assert_eq!(m[&1].state, JobState::Running);
    assert_eq!(m[&2].state, JobState::Completed);
    assert_eq!(m[&3].state, JobState::Unknown);
}

// ---- dedupe / csv_join ----

#[test]
fn dedupe_preserves_first_occurrence_order() {
    assert_eq!(
        dedupe_preserving_order(&[3, 1, 2, 1, 3, 4]),
        vec![3, 1, 2, 4]
    );
}

#[test]
fn csv_join_basic() {
    assert_eq!(csv_join(&[]), "");
    assert_eq!(csv_join(&[1]), "1");
    assert_eq!(csv_join(&[1, 2, 3]), "1,2,3");
}

// ---- end-to-end (no subprocess) ----

#[tokio::test]
async fn empty_input_short_circuits() {
    let m = query_job_states_batch(&[]).await.unwrap();
    assert!(m.is_empty());
}

// ---- test fixtures for async query functions ----

struct PanicDispatcher;
impl JobDispatcher for PanicDispatcher {
    async fn run(&self, _argv: &[String]) -> Result<i32> {
        panic!("PanicDispatcher.run called")
    }
    async fn capture(&self, _argv: &[String]) -> Result<CaptureOutput> {
        panic!("PanicDispatcher.capture called")
    }
}

struct MockCapture {
    expected_argv: Vec<String>,
    output: CaptureOutput,
}
impl MockCapture {
    /// stdout-only success mock — the common case.
    fn ok(expected_argv: Vec<String>, stdout: &str) -> Self {
        Self {
            expected_argv,
            output: cap_ok(stdout),
        }
    }
}
impl JobDispatcher for MockCapture {
    async fn run(&self, _argv: &[String]) -> Result<i32> {
        panic!("not used")
    }
    async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
        assert_eq!(argv, self.expected_argv.as_slice());
        Ok(self.output.clone())
    }
}

// ---- query_job_states_via_qgroup_with ----

#[tokio::test]
async fn query_via_qgroup_short_circuits_on_empty_input() {
    // No dispatcher call should happen for empty input.
    let m = super::query_job_states_via_qgroup_with(&PanicDispatcher, &[])
        .await
        .unwrap();
    assert!(m.is_empty());
}

#[tokio::test]
async fn query_squeue_only_short_circuits_on_empty_input() {
    let m = super::query_job_states_squeue_only_with(&PanicDispatcher, &[])
        .await
        .unwrap();
    assert!(m.is_empty());
}

#[tokio::test]
async fn query_via_qgroup_filters_to_requested_jobids() {
    // qgroup returns jobs 1, 2, 3; we only ask for 2 — only 2 should come back.
    let stdout = "\
queue user 1 RUN 1 1 1M 0:0:1(0:1:0)
queue user 2 RUN 1 1 1M 0:0:1(0:1:0)
queue user 3 RUN 1 1 1M 0:0:1(0:1:0)
";
    let mock = MockCapture::ok(vec!["qgroup".into(), "-l".into()], stdout);
    let m = super::query_job_states_via_qgroup_with(&mock, &[2])
        .await
        .unwrap();
    assert_eq!(m.len(), 1);
    assert!(m.contains_key(&2));
}

// ---- parse_qgroup_l ----

#[test]
fn parse_qgroup_l_extracts_jobid_and_state() {
    let out = "\
QUEUE     USER     JOBID          STATUS  PROC  CORE    MEM    ELAPSE(    limit)
gr19999b  b59999   12345          RUN        4     1  4570M  00:00:07( 01:00:00)
gr19999b  b59999   12346          QUE        1     1   100M  00:00:00( 00:30:00)
";
    let map = super::parse_qgroup_l(out);
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&12345).unwrap().state, JobState::Running);
    assert_eq!(map.get(&12346).unwrap().state, JobState::Pending);
}

#[test]
fn parse_qgroup_l_skips_blank_and_short_lines() {
    let out = "\
QUEUE USER JOBID STATUS

short
gr19999b u 9999 RUN 1 1 1M 0:0:1(0:1:0)
";
    let map = super::parse_qgroup_l(out);
    assert_eq!(map.len(), 1);
    assert!(map.contains_key(&9999));
}

#[test]
fn parse_qgroup_l_handles_completed_state() {
    let out = "\
QUEUE USER JOBID STATUS PROC
gr19999b u 5555 CMP 1
";
    let map = super::parse_qgroup_l(out);
    assert_eq!(map.get(&5555).map(|s| s.state), Some(JobState::Completed));
}

/// Current KUDPC `qgroup -l` output uses `|` as column dividers and
/// emits `FINI` as the "completed" status token. Regression guard for
/// the bug where `wait_terminal` timed out at 180 s even though the
/// job had finished, because the previous parser took field index 3
/// (`"|"`) as STATUS and `FINI` had no mapping in `JobState::parse`.
#[test]
fn parse_qgroup_l_handles_kudpc_pipe_separated_fini_row() {
    let out = "\
 QUEUE    SYS |   RUN  PEND OTHER | ALLOC ( MIN/ STD/ MAX)
----------------------------------------------------------------
 gr10641a  A  |     0     0     1 |     0 (   0/ 112/ 112)

 QUEUE    USER     |   RUN(ALLOC)  PEND(REQUEST) OTHER(REQUEST)
----------------------------------------------------------------
 gr10641a b39027   |     0(    0)     0(     0)     1(     1)

 QUEUE    USER     JOBID    | STAT  SUBMIT_AT        | RSC:core | PROC CORE    MEM       ELAPSE
------------------------------------------------------------------------------------------------
 gr10641a b39027   7519503  | FINI  2026-05-12 01:29 |        1 |    1    1  1070M     00:01:00
";
    let map = super::parse_qgroup_l(out);
    // Only the detail row carries a real jobid; the two summary
    // sections must be skipped.
    assert_eq!(map.len(), 1, "expected 1 detail row, got: {map:?}");
    assert_eq!(
        map.get(&7519503).map(|s| s.state),
        Some(JobState::Completed),
        "FINI must map to Completed so wait_terminal can exit"
    );
}

/// The pending-side equivalent of the KUDPC pipe layout — `QUE`
/// should still resolve to `Pending`, confirming the new token
/// stream filter works for non-terminal states too.
#[test]
fn parse_qgroup_l_handles_kudpc_pipe_separated_que_row() {
    let out = "\
 gr10641a b39027   7519600  | QUE   2026-05-12 02:00 |        1 |    1    1  1070M     00:00:00
";
    let map = super::parse_qgroup_l(out);
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&7519600).map(|s| s.state), Some(JobState::Pending));
}

/// KUDPC qgroup -l emits two side-by-side detail rows when both a
/// finished and a failed job are still in the listing — confirms the
/// FINI/FAIL token pair resolves to terminal states so
/// `wait_terminal` exits for both outcomes.
#[test]
fn parse_qgroup_l_handles_kudpc_pipe_separated_fini_and_fail_rows() {
    let out = "\
 QUEUE    USER     JOBID    | STAT  SUBMIT_AT        | RSC:core | PROC CORE    MEM       ELAPSE
------------------------------------------------------------------------------------------------
 gr10641a b39027   7519510  | FINI  2026-05-12 01:48 |        1 |    1    1  1070M     00:01:00
 gr10641a b39027   7519511  | FAIL  2026-05-12 01:49 |        1 |    1    1  1070M     00:01:00
";
    let map = super::parse_qgroup_l(out);
    assert_eq!(map.len(), 2, "expected 2 detail rows, got: {map:?}");
    assert_eq!(
        map.get(&7519510).map(|s| s.state),
        Some(JobState::Completed),
    );
    assert_eq!(map.get(&7519511).map(|s| s.state), Some(JobState::Failed));
}

// ---- parse_squeue_array_task ----

#[test]
fn parse_squeue_array_task_extracts_state_and_reason() {
    // squeue with -r -o "%i %T %r" gives `KEY STATE REASON` per row.
    let out = "12345_3 RUNNING None\n";
    let s = super::parse_squeue_array_task(out, "12345_3").expect("Some");
    assert_eq!(s.state, JobState::Running);
    assert_eq!(s.reason, JobReason::None);
}

#[test]
fn parse_squeue_array_task_returns_none_for_empty_output() {
    assert_eq!(super::parse_squeue_array_task("", "12345_3"), None);
    assert_eq!(super::parse_squeue_array_task("\n", "12345_3"), None);
}

#[test]
fn parse_squeue_array_task_carries_pending_reason() {
    let out = "12345_3 PENDING Priority\n";
    let s = super::parse_squeue_array_task(out, "12345_3").expect("Some");
    assert_eq!(s.state, JobState::Pending);
    assert_eq!(s.reason, JobReason::Priority);
}

#[test]
fn parse_squeue_array_task_ignores_rows_for_other_keys() {
    // Exact-key match: a batched listing shared with plain jobs and
    // other tasks must never contribute another row's status, and a
    // missing key must read as "left the queue" (None).
    let out = "\
100 RUNNING None
12345_2 PENDING Priority
12345_3 RUNNING None
";
    let s = super::parse_squeue_array_task(out, "12345_2").expect("Some");
    assert_eq!(s.state, JobState::Pending);
    assert_eq!(
        super::parse_squeue_array_task(out, "12345_9"),
        None,
        "a key absent from the listing must resolve to None, never another row"
    );
}

// ---- parse_sacct_array_task_with_exit_code ----

#[test]
fn parse_sacct_array_task_returns_parent_row_skipping_steps() {
    // Real sacct emits the parent row plus step rows (`.batch`, `.0`).
    // Only the parent row should contribute.
    let out = "\
12345_3|COMPLETED|None|0:0
12345_3.batch|COMPLETED|None|0:0
12345_3.extern|COMPLETED|None|0:0
";
    let oc = super::parse_sacct_array_task_with_exit_code(out, "12345_3").expect("Some");
    assert_eq!(oc.status.state, JobState::Completed);
    assert_eq!(oc.exit_code, Some(0));
}

#[test]
fn parse_sacct_array_task_recovers_nonzero_exit_code() {
    let out = "12345_7|FAILED|NonZeroExitCode|2:0\n";
    let oc = super::parse_sacct_array_task_with_exit_code(out, "12345_7").expect("Some");
    assert_eq!(oc.status.state, JobState::Failed);
    assert_eq!(oc.status.reason, JobReason::NonZeroExitCode);
    assert_eq!(oc.exit_code, Some(2));
}

#[test]
fn parse_sacct_array_task_returns_none_for_only_step_rows() {
    // Defensive: if sacct somehow returns only step rows (purged
    // parent, …), the parser must not synthesize a parent.
    let out = "12345_3.batch|COMPLETED|None|0:0\n";
    assert_eq!(
        super::parse_sacct_array_task_with_exit_code(out, "12345_3"),
        None
    );
}

#[test]
fn parse_sacct_array_task_returns_none_for_empty_output() {
    assert_eq!(
        super::parse_sacct_array_task_with_exit_code("", "12345_3"),
        None
    );
}

#[test]
fn parse_sacct_array_task_ignores_rows_for_other_tasks() {
    // Exact-key match: a listing that contains rows for other tasks
    // (e.g. a future batched query, or sacct returning the whole array)
    // must never contribute another task's outcome.
    let out = "\
12345_2|FAILED|NonZeroExitCode|2:0
12345_2.batch|FAILED||2:0
12345_3|COMPLETED|None|0:0
12345_3.batch|COMPLETED|None|0:0
";
    let oc = super::parse_sacct_array_task_with_exit_code(out, "12345_3").expect("Some");
    assert_eq!(oc.status.state, JobState::Completed);
    assert_eq!(oc.exit_code, Some(0));
    assert_eq!(
        super::parse_sacct_array_task_with_exit_code(out, "12345_9"),
        None,
        "a task absent from the listing must resolve to None, never another task's row"
    );
}

// ---- query_array_task_state_with / query_array_task_outcome_with ----

#[tokio::test]
async fn query_array_task_state_uses_master_underscore_idx_squeue_key() {
    // The argv must be the batchable summary shape (`-r`, `%i %T %r`) —
    // any drift silently bypasses the squeue cache (see the shape-sync
    // invariant in docs/architecture.md §6).
    let mock = MockCapture::ok(
        vec![
            "squeue".into(),
            "-h".into(),
            "-r".into(),
            "-j".into(),
            "12345_3".into(),
            "-o".into(),
            "%i %T %r".into(),
        ],
        "12345_3 RUNNING None\n",
    );
    let s = super::query_array_task_state_with(&mock, 12345, 3)
        .await
        .unwrap()
        .expect("Some");
    assert_eq!(s.state, JobState::Running);
}

#[tokio::test]
async fn query_array_task_state_returns_none_when_squeue_reports_no_row() {
    let mock = MockCapture::ok(
        vec![
            "squeue".into(),
            "-h".into(),
            "-r".into(),
            "-j".into(),
            "99999_0".into(),
            "-o".into(),
            "%i %T %r".into(),
        ],
        "",
    );
    let r = super::query_array_task_state_with(&mock, 99999, 0)
        .await
        .unwrap();
    assert!(r.is_none());
}

// ---- summary-argv shape pinning ----
//
// All three bulk query functions must build the exact batchable summary
// shape the squeue cache recognizes (`-r` included) — any drift silently
// bypasses the cache (shape-sync invariant, docs/architecture.md §6).
// MockCapture asserts the full argv byte-for-byte.

fn pinned_summary_argv(csv: &str) -> Vec<String> {
    vec![
        "squeue".into(),
        "-h".into(),
        "-r".into(),
        "-j".into(),
        csv.into(),
        "-o".into(),
        "%i %T %r".into(),
    ]
}

#[tokio::test]
async fn query_job_states_batch_builds_the_batchable_summary_argv() {
    let mock = MockCapture::ok(pinned_summary_argv("100"), "100 RUNNING None\n");
    let map = super::query_job_states_batch_with(&mock, &[100])
        .await
        .unwrap();
    assert_eq!(map.get(&100).map(|s| s.state), Some(JobState::Running));
}

#[tokio::test]
async fn query_job_states_with_exit_code_builds_the_batchable_summary_argv() {
    let mock = MockCapture::ok(pinned_summary_argv("100"), "100 RUNNING None\n");
    let map = super::query_job_states_with_exit_code_with(&mock, &[100])
        .await
        .unwrap();
    assert_eq!(
        map.get(&100).map(|o| o.status.state),
        Some(JobState::Running)
    );
}

#[tokio::test]
async fn query_job_states_squeue_only_builds_the_batchable_summary_argv() {
    let mock = MockCapture::ok(pinned_summary_argv("100"), "100 RUNNING None\n");
    let map = super::query_job_states_squeue_only_with(&mock, &[100])
        .await
        .unwrap();
    assert_eq!(map.get(&100).map(|s| s.state), Some(JobState::Running));
}

// ---- failure classification (vanish vs. transient) ----

/// Mock that always returns the same canned [`CaptureOutput`]
/// regardless of argv — for classification tests where only the
/// exit_code/stderr combination matters.
struct FixedCapture(CaptureOutput);
impl JobDispatcher for FixedCapture {
    async fn run(&self, _argv: &[String]) -> Result<i32> {
        panic!("not used")
    }
    async fn capture(&self, _argv: &[String]) -> Result<CaptureOutput> {
        Ok(self.0.clone())
    }
}

fn vanished_squeue() -> CaptureOutput {
    CaptureOutput {
        exit_code: 1,
        stdout: String::new(),
        stderr: "slurm_load_jobs error: Invalid job id specified\n".into(),
    }
}

fn transient_squeue() -> CaptureOutput {
    CaptureOutput {
        exit_code: 1,
        stdout: String::new(),
        stderr: "slurm_load_jobs error: Socket timed out on send/recv operation\n".into(),
    }
}

/// KUDPC purges terminated jobs from the queue immediately, so squeue
/// exits non-zero with `Invalid job id specified` once every queried
/// jobid has left the queue. That is the *vanish* signal, not a
/// failure — per-task query reports `None`.
#[tokio::test]
async fn query_array_task_state_treats_invalid_jobid_failure_as_vanished() {
    let mock = FixedCapture(vanished_squeue());
    let r = super::query_array_task_state_with(&mock, 12345, 3)
        .await
        .unwrap();
    assert!(r.is_none(), "vanished jobid must map to None, got {r:?}");
}

/// Same vanish signal on the batch squeue-only helper: empty map, no
/// error.
#[tokio::test]
async fn query_squeue_only_treats_invalid_jobid_failure_as_empty() {
    let mock = FixedCapture(vanished_squeue());
    let m = super::query_job_states_squeue_only_with(&mock, &[12345])
        .await
        .unwrap();
    assert!(m.is_empty(), "vanished jobids must map to empty, got {m:?}");
}

/// A transient controller failure (`Socket timed out`) is NOT a
/// vanish — it must surface as an error naming the tool and carrying
/// the stderr text, never as a silent empty listing.
#[tokio::test]
async fn query_array_task_state_propagates_transient_squeue_failure() {
    let mock = FixedCapture(transient_squeue());
    let err = super::query_array_task_state_with(&mock, 12345, 3)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("squeue"), "tool name expected, got: {msg}");
    assert!(
        msg.contains("Socket timed out"),
        "stderr text expected, got: {msg}"
    );
}

#[tokio::test]
async fn query_squeue_only_propagates_transient_squeue_failure() {
    let mock = FixedCapture(transient_squeue());
    let err = super::query_job_states_squeue_only_with(&mock, &[12345])
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("squeue"), "tool name expected, got: {msg}");
    assert!(
        msg.contains("Socket timed out"),
        "stderr text expected, got: {msg}"
    );
}

/// sacct returns exit 0 + empty stdout for unknown jobids; a nonzero
/// exit is a genuine failure and must propagate.
#[tokio::test]
async fn query_array_task_outcome_propagates_sacct_failure() {
    let mock = FixedCapture(CaptureOutput {
        exit_code: 1,
        stdout: String::new(),
        stderr: "sacct: error: slurmdbd is unresponsive\n".into(),
    });
    let err = super::query_array_task_outcome_with(&mock, 12345, 3)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("sacct"), "tool name expected, got: {msg}");
    assert!(
        msg.contains("slurmdbd is unresponsive"),
        "stderr text expected, got: {msg}"
    );
}

#[tokio::test]
async fn query_array_task_outcome_uses_master_underscore_idx_sacct_key() {
    let mock = MockCapture::ok(
        vec![
            "sacct".into(),
            "-P".into(),
            "-n".into(),
            "-j".into(),
            "12345_3".into(),
            "-o".into(),
            "JobID,State,Reason,ExitCode".into(),
        ],
        "12345_3|COMPLETED|None|0:0\n",
    );
    let oc = super::query_array_task_outcome_with(&mock, 12345, 3)
        .await
        .unwrap()
        .expect("Some");
    assert_eq!(oc.status.state, JobState::Completed);
    assert_eq!(oc.exit_code, Some(0));
}
