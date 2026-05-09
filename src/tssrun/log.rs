//! Pluggable log sinks for tee-ed stdout/stderr lines from a tssrun child.
//!
//! Sinks are invoked once per `\n`-delimited line (no trailing newline).
//! Implementations must be `Send + Sync` so the tee task can be spawned
//! on the multi-threaded tokio runtime.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::Result;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

pub trait JobLogSink: Send + Sync {
    fn append(&self, stream: LogStream, line: &str) -> impl Future<Output = Result<()>> + Send;
    fn flush(&self) -> impl Future<Output = Result<()>> + Send;
}

/// Drops every line. Use when stdout is irrelevant (still parses for jobid).
#[derive(Debug, Default, Clone, Copy)]
pub struct NullLogSink;

impl JobLogSink for NullLogSink {
    async fn append(&self, _stream: LogStream, _line: &str) -> Result<()> {
        Ok(())
    }
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Captures every appended line in memory. Test/diagnostics oriented.
#[derive(Debug, Default)]
pub struct InMemoryLogSink {
    buf: Mutex<Vec<(LogStream, String)>>,
}

impl InMemoryLogSink {
    pub fn snapshot(&self) -> Vec<(LogStream, String)> {
        self.buf.lock().unwrap().clone()
    }
}

impl JobLogSink for InMemoryLogSink {
    async fn append(&self, stream: LogStream, line: &str) -> Result<()> {
        self.buf.lock().unwrap().push((stream, line.to_string()));
        Ok(())
    }
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Forwards stdout lines to the parent's stdout, stderr lines to stderr.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdLogSink;

impl JobLogSink for StdLogSink {
    async fn append(&self, stream: LogStream, line: &str) -> Result<()> {
        match stream {
            LogStream::Stdout => println!("{line}"),
            LogStream::Stderr => eprintln!("{line}"),
        }
        Ok(())
    }
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Append-only file sink. Each `append` writes one line followed by `\n`.
pub struct FileLogSink {
    stdout: tokio::sync::Mutex<tokio::fs::File>,
    stderr: tokio::sync::Mutex<tokio::fs::File>,
    paths: (PathBuf, PathBuf),
}

impl FileLogSink {
    pub async fn create(stdout: PathBuf, stderr: PathBuf) -> Result<Self> {
        let stdout_f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stdout)
            .await?;
        let stderr_f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stderr)
            .await?;
        Ok(Self {
            stdout: tokio::sync::Mutex::new(stdout_f),
            stderr: tokio::sync::Mutex::new(stderr_f),
            paths: (stdout, stderr),
        })
    }

    pub fn paths(&self) -> &(PathBuf, PathBuf) {
        &self.paths
    }
}

impl JobLogSink for FileLogSink {
    async fn append(&self, stream: LogStream, line: &str) -> Result<()> {
        let mut f = match stream {
            LogStream::Stdout => self.stdout.lock().await,
            LogStream::Stderr => self.stderr.lock().await,
        };
        f.write_all(line.as_bytes()).await?;
        f.write_all(b"\n").await?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        self.stdout.lock().await.flush().await?;
        self.stderr.lock().await.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn null_sink_append_is_noop() {
        let s = NullLogSink;
        s.append(LogStream::Stdout, "hi").await.unwrap();
        s.append(LogStream::Stderr, "bye").await.unwrap();
        s.flush().await.unwrap();
    }

    #[tokio::test]
    async fn in_memory_sink_preserves_order() {
        let s = InMemoryLogSink::default();
        s.append(LogStream::Stdout, "a").await.unwrap();
        s.append(LogStream::Stderr, "b").await.unwrap();
        s.append(LogStream::Stdout, "c").await.unwrap();
        let v = s.snapshot();
        assert_eq!(
            v,
            vec![
                (LogStream::Stdout, "a".to_string()),
                (LogStream::Stderr, "b".to_string()),
                (LogStream::Stdout, "c".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn std_sink_does_not_panic() {
        // Validates the type and trait wiring; the actual stdout/stderr
        // bytes go to the test harness output and are not assertable here.
        let s = StdLogSink;
        s.append(LogStream::Stdout, "alpha").await.unwrap();
        s.append(LogStream::Stderr, "beta").await.unwrap();
        s.flush().await.unwrap();
    }

    #[tokio::test]
    async fn file_sink_writes_lines_to_correct_streams() {
        use std::path::PathBuf;
        let tmp = tempfile::tempdir().unwrap();
        let stdout_path: PathBuf = tmp.path().join("o.log");
        let stderr_path: PathBuf = tmp.path().join("e.log");
        let s = FileLogSink::create(stdout_path.clone(), stderr_path.clone())
            .await
            .unwrap();
        s.append(LogStream::Stdout, "hello").await.unwrap();
        s.append(LogStream::Stderr, "world").await.unwrap();
        s.flush().await.unwrap();

        let o = std::fs::read_to_string(&stdout_path).unwrap();
        let e = std::fs::read_to_string(&stderr_path).unwrap();
        assert_eq!(o, "hello\n");
        assert_eq!(e, "world\n");
    }
}
