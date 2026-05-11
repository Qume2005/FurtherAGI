//! WorkPlatform trait and supporting types.

use std::path::Path;

use async_trait::async_trait;
use thiserror::Error;

/// Errors from work platform operations.
#[derive(Error, Debug)]
pub enum PlatformError {
    /// A command exited with a non-zero status code.
    #[error("command failed with exit code {code}: {stderr}")]
    CommandFailed {
        code: i64,
        stderr: String,
    },

    /// An I/O error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// A Docker API error.
    #[error("Docker error: {0}")]
    Docker(String),

    /// The platform does not support the requested operation.
    #[error("unsupported operation: {0}")]
    Unsupported(String),
}

/// Output from a command execution.
#[derive(Debug)]
pub struct CommandOutput {
    /// Standard output bytes.
    pub stdout: Vec<u8>,
    /// Standard error bytes.
    pub stderr: Vec<u8>,
    /// Process exit code.
    pub exit_code: i64,
}

impl CommandOutput {
    /// Parse stdout as a UTF-8 string.
    pub fn stdout_string(&self) -> Result<String, std::string::FromUtf8Error> {
        String::from_utf8(self.stdout.clone())
    }

    /// Parse stderr as a UTF-8 string.
    pub fn stderr_string(&self) -> Result<String, std::string::FromUtf8Error> {
        String::from_utf8(self.stderr.clone())
    }
}

/// Unified interface for workflow execution environments.
///
/// Most builtin workflows do not need a platform — they operate on in-memory
/// data. This trait is for workflows that require external execution
/// (e.g., running a Python script in a Docker container).
#[async_trait]
pub trait WorkPlatform: Send + Sync {
    /// Execute a command in the platform's environment.
    ///
    /// Returns the captured stdout, stderr, and exit code.
    async fn run_command(
        &self,
        command: &str,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> Result<CommandOutput, PlatformError>;

    /// Write a file to the platform's workspace directory.
    async fn write_file(
        &self,
        path: &Path,
        content: &[u8],
    ) -> Result<(), PlatformError>;

    /// Read a file from the platform's workspace directory.
    async fn read_file(
        &self,
        path: &Path,
    ) -> Result<Vec<u8>, PlatformError>;

    /// Get the host-side workspace root path.
    fn workspace_root(&self) -> &Path;

    /// Release platform resources (stop containers, clean tempdirs, etc.).
    async fn cleanup(&self) -> Result<(), PlatformError>;
}
