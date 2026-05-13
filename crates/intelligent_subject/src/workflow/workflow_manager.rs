//! # Workflow Manager — 中央注册器
//!
//! 管理所有工作流的生命周期：注册、依赖校验和执行。
//!
//! ## 功能实现
//!
//! [`WorkflowManager`] 是工作流系统的中央注册器，管理所有工作流的完整生命周期：
//!
//! - **注册** — 通过 [`add()`](WorkflowManager::add)、[`add_with_ctx()`](WorkflowManager::add_with_ctx)、
//!   [`add_erased()`](WorkflowManager::add_erased) 注册叶工作流，
//!   通过 [`register_composite()`](WorkflowManager::register_composite) 注册复合工作流（DAG）
//! - **校验** — [`validate_all()`](WorkflowManager::validate_all) 解析子工作流引用、
//!   检查类型兼容性、检测跨工作流环
//! - **执行** — [`execute()`](WorkflowManager::execute) 和
//!   [`execute_typed()`](WorkflowManager::execute_typed) 提供类型擦除和强类型执行
//!
//! 子模块按职责划分：`registry`（注册）、`validation`（依赖校验）、`execution`（执行）。
//!
//! ## 实现特色
//!
//! - 线程安全的 `DashMap` 后端，支持并发注册和执行
//! - 双注册路径：叶工作流（即时验证）vs 复合工作流（延迟验证，通过 `validate_all()`）
//! - `RegisteredWorkflow` 存储类型擦除工作流或 DAG、输入/输出 `TypeId` 和验证状态
//! - `execute_typed()` 在边界执行 `TypeId` 检查，然后委托类型擦除执行
//! - `validate_all()` 解析 SubWorkflow 引用、检查类型兼容性、通过 Kahn 算法检测跨工作流环
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | `dashmap`（并发 HashMap）、`tracing`（instrument） |
//! | 内部模块 | [`crate::workflow::dag::WorkflowDag`]、[`crate::workflow::definition::ErasedWorkflow`]、[`crate::workflow::error::WorkflowError`]、[`crate::workflow::model::{ExecutionContext, NodeId, WorkflowId}`] |
//!
//! ## 示例
//!
//! ```rust
//! use intelligent_subject::workflow::workflow_manager::WorkflowManager;
//! use intelligent_subject::workflow::model::ExecutionContext;
//! use intelligent_subject::workflow::platform::NullPlatform;
//! use intelligent_subject::workflow::error::WorkflowError;
//! use std::sync::Arc;
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), WorkflowError> {
//! let mgr = WorkflowManager::new();
//!
//! // 注册叶工作流
//! mgr.add("builtin@Double", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input * 2)
//! })?;
//!
//! // 校验（叶工作流在注册时即已校验）
//! mgr.validate_all()?;
//!
//! // 强类型执行
//! let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };
//! let result: i32 = mgr.execute_typed("builtin@Double", 21, &ctx).await?;
//! assert_eq!(result, 42);
//! # Ok(())
//! # }
//! ```

mod execution;
mod registry;
mod validation;

#[cfg(test)]
mod tests;

use std::any::TypeId;

use crate::workflow::dag::WorkflowDag;
use crate::workflow::definition::ErasedWorkflow;
#[cfg(test)]
use crate::workflow::error::WorkflowError;
#[cfg(test)]
use crate::workflow::model::ExecutionContext;
use crate::workflow::model::WorkflowId;

pub use registry::WorkflowManager;

/// A registered workflow entry.
struct RegisteredWorkflow {
    id: WorkflowId,
    /// The type-erased workflow instance.
    workflow: Option<Box<dyn ErasedWorkflow>>,
    /// If this is a composite workflow, its DAG.
    dag: Option<WorkflowDag>,
    /// Whether this workflow has been validated.
    validated: bool,
    /// Input type ID (from the entry node of the DAG or the workflow itself).
    input_type: TypeId,
    /// Output type ID (from the exit node of the DAG or the workflow itself).
    output_type: TypeId,
}
