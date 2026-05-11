//! NullPlatform — 默认空实现。
//!
//! 适用于不需要外部执行的纯内存工作流。
//! `run_command` 返回空结果，文件操作在临时目录中进行。

use std::path::Path;

use async_trait::async_trait;
use tempfile::TempDir;

use super::api::{CommandOutput, PlatformError, WorkPlatform};

/// A no-op work platform for workflows that don't need external execution.
///
/// File operations (`write_file`, `read_file`) work on a temporary directory.
/// `run_command` returns empty output with exit code 0.
/// `cleanup` deletes the temporary directory.
pub struct NullPlatform {
    tempdir: TempDir,
}

impl NullPlatform {
    pub fn new() -> Self {
        Self {
            tempdir: TempDir::new().expect("failed to create tempdir for NullPlatform"),
        }
    }
}

impl Default for NullPlatform {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WorkPlatform for NullPlatform {
    async fn run_command(
        &self,
        _command: &str,
        _args: &[&str],
        _env: &[(&str, &str)],
    ) -> Result<CommandOutput, PlatformError> {
        Ok(CommandOutput {
            stdout: Vec::new(),
            stderr: Vec::new(),
            exit_code: 0,
        })
    }

    async fn write_file(
        &self,
        path: &Path,
        content: &[u8],
    ) -> Result<(), PlatformError> {
        let full_path = self.tempdir.path().join(path);
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
        let full_path = self.tempdir.path().join(path);
        Ok(std::fs::read(full_path)?)
    }

    fn workspace_root(&self) -> &Path {
        self.tempdir.path()
    }

    async fn cleanup(&self) -> Result<(), PlatformError> {
        // TempDir auto-deletes on drop, nothing to do here.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_command_returns_empty() {
        let platform = NullPlatform::new();
        let output = platform.run_command("echo", &[], &[]).await.unwrap();
        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.is_empty());
    }

    #[tokio::test]
    async fn write_and_read_file() {
        let platform = NullPlatform::new();
        platform
            .write_file(Path::new("test.txt"), b"hello")
            .await
            .unwrap();
        let data = platform.read_file(Path::new("test.txt")).await.unwrap();
        assert_eq!(data, b"hello");
    }

    #[tokio::test]
    async fn write_creates_parent_dirs() {
        let platform = NullPlatform::new();
        platform
            .write_file(Path::new("a/b/c.txt"), b"nested")
            .await
            .unwrap();
        let data = platform.read_file(Path::new("a/b/c.txt")).await.unwrap();
        assert_eq!(data, b"nested");
    }

    #[test]
    fn workspace_root_is_tempdir() {
        let platform = NullPlatform::new();
        let root = platform.workspace_root();
        assert!(root.exists());
    }
}
