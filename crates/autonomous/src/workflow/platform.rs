//! # 工作平台（WorkPlatform）
//!
//! 统一的执行环境抽象，合并了之前的 `ComputePlatform` 和 `Workspace`。
//!
//! ## 核心概念
//!
//! - [`WorkPlatform`] trait — 定义了命令执行、文件读写、资源清理的异步接口
//! - [`NullPlatform`] — 默认空实现，纯内存工作流无需平台
//! - [`LocalPlatform`] — 本机执行，直接调用系统命令和文件系统
//! - [`DockerPlatform`] — Docker 容器内执行，适用于 Python 脚本等需要沙箱隔离的场景
//!
//! ## 使用场景
//!
//! 绝大多数内建工作流（`Identity`、`Map`、`Predicate` 等）不需要工作平台，
//! 使用默认的 `NullPlatform` 即可。只有需要执行外部脚本（如 Python）的
//! 工作流才需要 `LocalPlatform` 或 `DockerPlatform`。
//!
//! ## 示例
//!
//! ```rust
//! use autonomous::workflow::platform::{NullPlatform, WorkPlatform};
//! use autonomous::workflow::model::ExecutionContext;
//! use std::path::Path;
//! use std::sync::Arc;
//!
//! let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };
//! // platform.workspace_root() → 临时目录
//! // platform.run_command() → 空操作
//! ```

mod api;
mod null;
mod local;
mod docker;

pub use docker::DockerPlatform;
pub use local::LocalPlatform;
pub use null::NullPlatform;
pub use api::{CommandOutput, PlatformError, WorkPlatform};
