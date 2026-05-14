//! # NullPlatform — 默认空实现
//!
//! 适用于不需要外部执行的纯内存工作流。
//! `run_command` 返回空结果，文件操作在临时目录中进行。
//!
//! ## 功能实现
//!
//! `NullPlatform` 是 [`WorkPlatform`](WorkPlatform) 的默认空实现。
//! - `run_command` 始终返回空 stdout/stderr 和退出码 0（不执行任何实际命令）
//! - `write_file` 和 `read_file` 在 `tempfile::TempDir` 中操作，生命周期结束时自动清理
//! - `cleanup` 为空操作（TempDir 在析构时自动删除）
//!
//! 适用于所有工作在内存中完成的纯计算工作流（如 `Identity`、`Map`、`Predicate` 等）。
//!
//! ## 实现特色
//!
//! - `write_file` 自动创建父目录（`create_dir_all`），无需预先确保目录存在
//! - `workspace_root()` 返回临时目录路径，可用于测试中的文件路径拼接
//! - 零开销：命令执行不产生任何系统调用
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | `async-trait`（异步 trait）、`tempfile`（临时目录） |
//! | 内部模块 | [`super::api::{CommandOutput, PlatformError, WorkPlatform}`] |
//!
//! ## 示例
//!
//! **文件读写：**
//!
//! ```rust
//! use intelligent_subject::workflow::platform::NullPlatform;
//! use intelligent_subject::workflow::platform::WorkPlatform;
//! use std::path::Path;
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let platform = NullPlatform::new();
//!
//! platform.write_file(Path::new("data.txt"), b"hello").await?;
//! let data = platform.read_file(Path::new("data.txt")).await?;
//! assert_eq!(data, b"hello");
//! # Ok(())
//! # }
//! ```
//!
//! **run_command 返回空输出：**
//!
//! ```rust
//! use intelligent_subject::workflow::platform::NullPlatform;
//! use intelligent_subject::workflow::platform::WorkPlatform;
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let platform = NullPlatform::new();
//! let output = platform.run_command("any_command", &["any", "args"], &[]).await?;
//! assert_eq!(output.exit_code, 0);
//! assert!(output.stdout.is_empty());
//! assert!(output.stderr.is_empty());
//! # Ok(())
//! # }
//! ```
//!
//! **在 ExecutionContext 中使用：**
//!
//! ```rust
//! use intelligent_subject::workflow::model::ExecutionContext;
//! use intelligent_subject::workflow::platform::NullPlatform;
//! use std::sync::Arc;
//!
//! let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };
//! // ctx.platform.run_command() → 空操作
//! // ctx.platform.workspace_root() → 临时目录
//! ```

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
