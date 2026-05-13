//! # 工作平台接口（WorkPlatform）
//!
//! 定义统一的执行环境抽象接口和配套类型。
//!
//! ## 功能实现
//!
//! 本模块提供三个核心类型：
//!
//! - **[`WorkPlatform`]** trait — 定义了命令执行、文件读写、工作区路径和资源清理的异步接口，
//!   是所有平台实现（Null、Local、Docker）必须遵循的契约。
//! - **[`PlatformError`]** — 平台操作错误枚举，包含命令失败（含退出码和 stderr）、
//!   I/O 错误、Docker 错误和不支持的操作四种变体。
//! - **[`CommandOutput`]** — 命令执行结果，捕获 stdout、stderr 字节流和进程退出码，
//!   提供 `stdout_string()` / `stderr_string()` 便捷 UTF-8 解析方法。
//!
//! ## 实现特色
//!
//! - `WorkPlatform` 通过 `#[async_trait]` 实现对象安全，可在 `Arc<dyn WorkPlatform>` 中使用
//! - `CommandOutput` 同时保存原始字节和提供 UTF-8 解析，兼顾二进制和文本输出
//! - `PlatformError::CommandFailed` 捕获退出码和 stderr，便于诊断命令失败原因
//! - 插件式设计：工作流代码只依赖 trait，运行时选择具体平台后端
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | `async-trait`（异步 trait）、`thiserror`（错误派生） |
//! | 内部模块 | 无（叶子模块） |
//!
//! ## 示例
//!
//! **通过 trait 调用 `run_command`：**
//!
//! ```rust
//! use intelligent_subject::workflow::platform::{NullPlatform, WorkPlatform};
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let platform = NullPlatform::new();
//! let output = platform.run_command("echo", &["hello"], &[]).await?;
//! assert_eq!(output.exit_code, 0);
//! # Ok(())
//! # }
//! ```
//!
//! **解析 CommandOutput：**
//!
//! ```rust
//! use intelligent_subject::workflow::platform::{NullPlatform, WorkPlatform};
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let platform = NullPlatform::new();
//! let output = platform.run_command("cat", &["nonexistent"], &[]).await?;
//! let stdout = output.stdout_string()?;
//! // NullPlatform 的 run_command 总是返回空输出
//! assert!(stdout.is_empty());
//! # Ok(())
//! # }
//! ```

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
