//! Pluggable log sinks for tee-ed stdout/stderr lines from a tssrun child.
//!
//! Sinks are invoked once per `\n`-delimited line (no trailing newline).
//! Implementations must be `Send + Sync` so the tee task can be spawned
//! on the multi-threaded tokio runtime, and [`JobLogSink`] is
//! dyn-compatible (returns boxed futures rather than RPIT) so callers can
//! hold `Arc<dyn JobLogSink>`.
//!
//! Built-in implementations:
//!
//! | Sink | Use case |
//! |---|---|
//! | [`NullLogSink`] | Discard every line. Snapshot parsing still runs. |
//! | [`StdLogSink`] | Forward to the parent's stdout / stderr. |
//! | [`InMemoryLogSink`] | Capture to a `Vec` for tests / diagnostics. |
//! | [`FileLogSink`] | Append to two on-disk files (one per stream). |

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Mutex;

use anyhow::Result;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

/// Pluggable log sink. Dyn-compatible so callers can hold `Arc<dyn JobLogSink>`.
pub trait JobLogSink: Send + Sync {
    fn append<'a>(
        &'a self,
        stream: LogStream,
        line: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>;
    fn flush(&self) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>>;
}

/// Drops every line. Use when stdout is irrelevant (still parses for jobid).
#[derive(Debug, Default, Clone, Copy)]
pub struct NullLogSink;

impl JobLogSink for NullLogSink {
    fn append<'a>(
        &'a self,
        _stream: LogStream,
        _line: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    fn flush(&self) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

/// Captures every appended line in memory. Test/diagnostics oriented.
///
/// Uses `std::sync::Mutex` (not `tokio::sync::Mutex`) on purpose: the
/// `append` impl never holds the lock across an `.await`, so the sync
/// mutex is both faster and works under any runtime — including code paths
/// that the tokio runtime hasn't entered yet.
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
    fn append<'a>(
        &'a self,
        stream: LogStream,
        line: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        let entry = (stream, line.to_string());
        Box::pin(async move {
            self.buf.lock().unwrap().push(entry);
            Ok(())
        })
    }
    fn flush(&self) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

/// Forwards stdout lines to the parent's stdout, stderr lines to stderr.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdLogSink;

impl JobLogSink for StdLogSink {
    fn append<'a>(
        &'a self,
        stream: LogStream,
        line: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        let owned = line.to_string();
        Box::pin(async move {
            match stream {
                LogStream::Stdout => println!("{owned}"),
                LogStream::Stderr => eprintln!("{owned}"),
            }
            Ok(())
        })
    }
    fn flush(&self) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
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
    fn append<'a>(
        &'a self,
        stream: LogStream,
        line: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        let owned = line.to_string();
        Box::pin(async move {
            let mut f = match stream {
                LogStream::Stdout => self.stdout.lock().await,
                LogStream::Stderr => self.stderr.lock().await,
            };
            f.write_all(owned.as_bytes()).await?;
            f.write_all(b"\n").await?;
            Ok(())
        })
    }

    fn flush(&self) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async {
            self.stdout.lock().await.flush().await?;
            self.stderr.lock().await.flush().await?;
            Ok(())
        })
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
