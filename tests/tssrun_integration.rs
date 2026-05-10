//! End-to-end test: TssrunManager.spawn → handle.wait → snapshot reflects
//! parsed jobid/node and FileLogSink captures the child's stdout.

use std::sync::Arc;

use slurm_async_runner::{AttachKey, FileLogSink, JobLogSink, TssrunCmd, TssrunManager};

const BANNER: &str = r#"echo "salloc: Granted job allocation 12345"
echo "salloc: Nodes node-int are ready for job"
echo "hello from child"
"#;

#[tokio::test]
async fn spawn_then_wait_then_snapshot_then_attach() {
    let tmp = tempfile::tempdir().unwrap();
    let stdout_log = tmp.path().join("o.log");
    let stderr_log = tmp.path().join("e.log");
    let state_dir = tmp.path().join("state");
    tokio::fs::create_dir_all(&state_dir).await.unwrap();

    // Write the bash mock script to a tempfile so build_argv produces
    // `[bash, <script_path>]` rather than `[bash, /bin/true, -c, ...]`.
    let script_path = tmp.path().join("mock.sh");
    tokio::fs::write(&script_path, BANNER).await.unwrap();

    let mut cmd = TssrunCmd::new(&script_path);
    cmd.tssrun_bin = "bash".to_string();
    // rsc is intentionally omitted: build_argv would emit --rsc p=1:m=128M
    // *before* the script path, which bash interprets as an unknown option
    // (exit 2). The ResourceSpec type is still exercised by unit tests in cmd.rs.

    let sink: Arc<dyn JobLogSink> = Arc::new(
        FileLogSink::create(stdout_log.clone(), stderr_log.clone())
            .await
            .unwrap(),
    );
    let manager = TssrunManager::new(cmd)
        .with_state_dir(state_dir.clone())
        .with_log_sink(sink);

    let mut handle = manager.spawn().await.unwrap();
    let pid = handle.pid();
    let uuid = handle.uuid();
    let code = handle.wait().await.unwrap();
    assert_eq!(code, Some(0));

    let snap = handle.snapshot();
    assert_eq!(snap.uuid, uuid);
    assert_eq!(snap.jobid, Some(12345));
    assert_eq!(snap.node.as_deref(), Some("node-int"));
    assert!(snap.finished.is_some());

    let stdout_content = tokio::fs::read_to_string(&stdout_log).await.unwrap();
    assert!(stdout_content.contains("hello from child"));

    // Persisted snapshot file is named after the UUID v7 primary key, not pid.
    let path = state_dir.join(format!("{uuid}.json"));
    assert!(
        path.exists(),
        "missing persisted handle at {}",
        path.display()
    );

    let attached = manager.attach(AttachKey::File(path)).await.unwrap();
    assert_eq!(attached.uuid(), uuid);
    assert_eq!(attached.pid(), pid);
    assert_eq!(attached.jobid(), Some(12345));
    assert_eq!(attached.node().as_deref(), Some("node-int"));

    // Round-trip via the new AttachKey::Uuid resolution path.
    let by_uuid = manager.attach(AttachKey::Uuid(uuid)).await.unwrap();
    assert_eq!(by_uuid.uuid(), uuid);
    assert_eq!(by_uuid.pid(), pid);
}
