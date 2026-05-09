//! Pluggable log sinks for tee-ed stdout/stderr lines from a tssrun child.
//!
//! Sinks are invoked once per `\n`-delimited line (no trailing newline).
//! Implementations must be `Send + Sync` so the tee task can be spawned
//! on the multi-threaded tokio runtime.

use std::future::Future;

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
}
