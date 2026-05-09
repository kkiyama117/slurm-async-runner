//! Pluggable log sinks for tee-ed stdout/stderr lines from a tssrun child.
//!
//! Sinks are invoked once per `\n`-delimited line (no trailing newline).
//! Implementations must be `Send + Sync` so the tee task can be spawned
//! on the multi-threaded tokio runtime.

use std::future::Future;
use std::sync::Mutex;

use anyhow::Result;

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
}
