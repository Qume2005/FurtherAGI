//! # 错误类型
//!
//! 定义工作流系统中所有可能出现的错误。
//!
//! 错误在以下阶段产生：
//! - **构建时** — [`TypeMismatch`](WorkflowError::TypeMismatch)、[`CycleDetected`](WorkflowError::CycleDetected)
//! - **注册时** — [`ValidationError`](WorkflowError::ValidationError)、[`WorkflowNotFound`](WorkflowError::WorkflowNotFound)
//! - **执行时** — [`ExecutionError`](WorkflowError::ExecutionError)、[`DowncastError`](WorkflowError::DowncastError)
//!
//! ## 功能实现
//!
//! 本模块定义统一的 [`WorkflowError`] 枚举，覆盖工作流生命周期中构建、注册、执行三个阶段
//! 可能产生的 7 种错误变体。所有变体通过 `thiserror` 派生 `std::error::Error` 和 `Display`。
//!
//! ## 实现特色
//!
//! - 通过 `thiserror` 自动实现 `Error` trait 和 `Display`，错误信息格式统一
//! - `ExecutionError` 携带 `anyhow::Error` 作为 `#[source]`，支持错误链追踪
//! - `Platform(#[from] PlatformError)` 自动从平台错误转换，无需手动 `.map_err()`
//! - 提供 [`type_mismatch()`](WorkflowError::type_mismatch) 和
//!   [`execution()`](WorkflowError::execution) 便捷构造器，减少样板代码
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | `thiserror`（错误派生宏）、`anyhow`（执行错误链） |
//! | 内部模块 | `crate::workflow::platform::PlatformError`、[`NodeId`]、[`WorkflowId`] |
//!
//! ## 示例
//!
//! **匹配 `TypeMismatch` 错误：**
//!
//! ```rust
//! use intelligent_subject::workflow::error::WorkflowError;
//! use intelligent_subject::workflow::model::NodeId;
//! use std::any::TypeId;
//!
//! let err = WorkflowError::type_mismatch(
//!     NodeId(1),
//!     NodeId(2),
//!     TypeId::of::<i32>(),
//!     TypeId::of::<String>(),
//! );
//!
//! match err {
//!     WorkflowError::TypeMismatch { from_node, to_node, expected, actual } => {
//!         assert_eq!(from_node, NodeId(1));
//!         assert_eq!(to_node, NodeId(2));
//!     }
//!     _ => panic!("unexpected error type"),
//! }
//! ```
//!
//! **匹配 `ExecutionError` 并访问 source 链：**
//!
//! ```rust
//! use intelligent_subject::workflow::error::WorkflowError;
//! use intelligent_subject::workflow::model::NodeId;
//!
//! let err = WorkflowError::execution(NodeId(42), "something went wrong".to_string());
//!
//! match err {
//!     WorkflowError::ExecutionError { node, source } => {
//!         assert_eq!(node, NodeId(42));
//!         // source 是 anyhow::Error，可以通过 std::error::Error::source() 追踪链
//!     }
//!     _ => panic!("unexpected error type"),
//! }
//! ```

use std::any::TypeId;

use anyhow::Error as AnyhowError;
use thiserror::Error;

use super::platform::PlatformError;
use super::model::{NodeId, WorkflowId};

/// 工作流系统中所有错误的统一枚举。
///
/// 使用 [`thiserror`](https://docs.rs/thiserror) 派生 `std::error::Error` 和 `Display`。
#[derive(Error, Debug)]
pub enum WorkflowError {
    /// 两个相连节点之间的类型不匹配。
    ///
    /// 上游节点的输出 `TypeId` 与下游节点的输入 `TypeId` 不一致。
    /// 在 [`PlanBuilder`](super::dag::PlanBuilder) 构建时检测。
    #[error("type mismatch from node {from_node:?} to node {to_node:?}: expected {expected}, got {actual}")]
    TypeMismatch {
        from_node: NodeId,
        to_node: NodeId,
        expected: String,
        actual: String,
    },

    /// DAG 中检测到环。
    ///
    /// 由 [`PlanBuilder::build`](super::dag::PlanBuilder::build) 中的 Kahn 算法检测。
    /// `nodes` 字段列出参与环的所有节点。
    #[error("cycle detected among nodes: {nodes:?}")]
    CycleDetected { nodes: Vec<NodeId> },

    /// 引用了不存在的节点。
    #[error("node not found: {0:?}")]
    NodeNotFound(NodeId),

    /// 引用了未注册的工作流。
    #[error("workflow not found: {0}")]
    WorkflowNotFound(WorkflowId),

    /// 工作流执行期间发生的错误。
    ///
    /// `source` 携带原始的 [`anyhow::Error`](https://docs.rs/anyhow)，
    /// 可通过 `Error::source()` 链式获取。
    #[error("execution error at node {node:?}: {source}")]
    ExecutionError {
        node: NodeId,
        source: AnyhowError,
    },

    /// 类型擦除边界上的 downcast 失败。
    ///
    /// 通常意味着注册时的类型校验逻辑存在 bug。
    #[error("downcast error at node {node:?}: expected {expected}")]
    DowncastError {
        node: NodeId,
        expected: String,
    },

    /// 注册校验失败。
    ///
    /// 包括：缺少子工作流引用、自引用、跨工作流环、谓词非 bool 等情况。
    #[error("validation error: {0}")]
    ValidationError(String),

    /// 工作平台执行错误。
    ///
    /// 由需要外部执行环境的工作流（如 Docker 内运行 Python）产生。
    #[error("platform error: {0}")]
    Platform(#[from] PlatformError),

    /// LLM 工具未找到。
    #[error("tool not found: {0}")]
    ToolNotFound(String),

    /// 工具参数反序列化失败。
    #[error("tool argument error for '{tool}': {message}")]
    ToolArgumentError { tool: String, message: String },

    /// 工具输出序列化失败。
    #[error("tool output error for '{tool}': {message}")]
    ToolOutputError { tool: String, message: String },
}

impl WorkflowError {
    /// 便捷构造器：使用 `TypeId` 创建类型不匹配错误。
    pub fn type_mismatch(from: NodeId, to: NodeId, expected: TypeId, actual: TypeId) -> Self {
        Self::TypeMismatch {
            from_node: from,
            to_node: to,
            expected: format!("{expected:?}"),
            actual: format!("{actual:?}"),
        }
    }

    /// 便捷构造器：创建执行错误。
    pub fn execution(node: NodeId, msg: String) -> Self {
        Self::ExecutionError {
            node,
            source: AnyhowError::msg(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        let err = WorkflowError::NodeNotFound(NodeId(42));
        assert!(err.to_string().contains("42"));

        let err = WorkflowError::WorkflowNotFound(WorkflowId::from("missing"));
        assert!(err.to_string().contains("missing"));
    }

    #[test]
    fn type_mismatch_convenience() {
        let err = WorkflowError::type_mismatch(
            NodeId(1),
            NodeId(2),
            TypeId::of::<i32>(),
            TypeId::of::<String>(),
        );
        let msg = err.to_string();
        assert!(msg.contains("type mismatch"));
    }
}
