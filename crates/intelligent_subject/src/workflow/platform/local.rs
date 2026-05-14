//! # LocalPlatform — 本机执行
//!
//! 直接在本机执行命令，操作本机文件系统。无沙箱隔离。
//!
//! ## 功能实现
//!
//! `LocalPlatform` 是 [`WorkPlatform`](WorkPlatform) 的本机实现，
//! 通过 `tokio::process::Command` 执行系统命令，直接操作本机文件系统。
//! 命令以宿主进程的相同权限运行，适用于开发环境和可信执行场景。
//!
//! ## 实现特色
//!
//! - 通过 `tokio::process::Command` 实现异步命令执行，支持 stdout/stderr 管道捕获
//! - `run_command` 支持通过 `env` 参数传递环境变量
//! - 非零退出码触发 [`PlatformError::CommandFailed`](PlatformError::CommandFailed)，
//!   包含退出码和 stderr 内容
//! - 工作区根目录在构造时自动创建（`create_dir_all`）
//! - `cleanup` 为空操作 — 本地平台不会删除其工作区
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | `async-trait`（异步 trait）、`tokio`（process::Command） |
//! | 内部模块 | [`super::api::{CommandOutput, PlatformError, WorkPlatform}`] |
//!
//! ## 示例
//!
//! **运行命令并检查输出：**
//!
//! ```rust
//! use intelligent_subject::workflow::platform::{LocalPlatform, WorkPlatform};
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! let platform = LocalPlatform::new(dir.path());
//!
//! let output = platform.run_command("echo", &["hello"], &[]).await?;
//! assert_eq!(output.exit_code, 0);
//! assert!(output.stdout_string()?.contains("hello"));
//! # Ok(())
//! # }
//! ```
//!
//! **写入文件并运行命令读取：**
//!
//! ```rust
//! use intelligent_subject::workflow::platform::{LocalPlatform, WorkPlatform};
//! use std::path::Path;
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! let platform = LocalPlatform::new(dir.path());
//!
//! platform.write_file(Path::new("test.txt"), b"hello local").await?;
//! let data = platform.read_file(Path::new("test.txt")).await?;
//! assert_eq!(data, b"hello local");
//! # Ok(())
//! # }
//! ```
//!
//! **处理命令失败：**
//!
//! ```rust
//! use intelligent_subject::workflow::platform::{LocalPlatform, PlatformError, WorkPlatform};
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! let platform = LocalPlatform::new(dir.path());
//!
//! let result = platform.run_command("false", &[], &[]).await;
//! match result {
//!     Err(PlatformError::CommandFailed { code, stderr }) => {
//!         assert_ne!(code, 0);
//!     }
//!     _ => panic!("expected CommandFailed"),
//! }
//! # Ok(())
//! # }
//! ```

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
