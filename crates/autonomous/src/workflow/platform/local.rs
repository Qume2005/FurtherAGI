//! LocalPlatform — 本机执行。
//!
//! 直接在本机执行命令，操作本机文件系统。无沙箱隔离。

use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use tokio::process::Command;

use super::api::{CommandOutput, PlatformError, WorkPlatform};

/// A local work platform that executes commands on the host machine.
///
/// No sandboxing — commands run with the same privileges as the host process.
pub struct LocalPlatform {
    root: PathBuf,
}

impl LocalPlatform {
    /// Create a local platform with the given workspace root directory.
    ///
    /// The directory is created if it doesn't exist.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        std::fs::create_dir_all(&root).ok();
        Self { root }
    }
}

#[async_trait]
impl WorkPlatform for LocalPlatform {
    async fn run_command(
        &self,
        command: &str,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> Result<CommandOutput, PlatformError> {
        let mut cmd = Command::new(command);
        cmd.args(args)
            .envs(env.iter().map(|&(k, v)| (k, v)))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let output = cmd.output().await.map_err(|e| {
            PlatformError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("failed to execute '{}': {e}", command),
            ))
        })?;

        let exit_code = output.status.code().unwrap_or(-1) as i64;

        if !output.status.success() {
            return Err(PlatformError::CommandFailed {
                code: exit_code,
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            });
        }

        Ok(CommandOutput {
            stdout: output.stdout,
            stderr: output.stderr,
            exit_code,
        })
    }

    async fn write_file(
        &self,
        path: &Path,
        content: &[u8],
    ) -> Result<(), PlatformError> {
        let full_path = self.root.join(path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(full_path, content)?;
        Ok(())
    }

    async fn read_file(
        &self,
        path: &Path,
    ) -> Result<Vec<u8>, PlatformError> {
        let full_path = self.root.join(path);
        Ok(std::fs::read(full_path)?)
    }

    fn workspace_root(&self) -> &Path {
        &self.root
    }

    async fn cleanup(&self) -> Result<(), PlatformError> {
        // Local platform doesn't clean up its workspace.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_echo() {
        let dir = tempfile::tempdir().unwrap();
        let platform = LocalPlatform::new(dir.path());
        let output = platform.run_command("echo", &["hello"], &[]).await.unwrap();
        assert_eq!(output.exit_code, 0);
        assert!(String::from_utf8_lossy(&output.stdout).contains("hello"));
    }

    #[tokio::test]
    async fn failed_command() {
        let dir = tempfile::tempdir().unwrap();
        let platform = LocalPlatform::new(dir.path());
        let result = platform.run_command("false", &[], &[]).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PlatformError::CommandFailed { code, .. } => assert_ne!(code, 0),
            other => panic!("expected CommandFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn write_and_read_file() {
        let dir = tempfile::tempdir().unwrap();
        let platform = LocalPlatform::new(dir.path());
        platform
            .write_file(Path::new("test.txt"), b"hello local")
            .await
            .unwrap();
        let data = platform.read_file(Path::new("test.txt")).await.unwrap();
        assert_eq!(data, b"hello local");
    }
}
