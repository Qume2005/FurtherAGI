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
//!   通过 [`register_ns_composite()`](WorkflowManager::register_ns_composite) 注册命名空间复合工作流
//! - **校验** — [`validate_all()`](WorkflowManager::validate_all) 标记所有工作流为已验证，允许执行
//! - **执行** — [`execute_typed()`](WorkflowManager::execute_typed) 提供强类型执行叶工作流，
//!   [`execute_ns()`](WorkflowManager::execute_ns) 执行命名空间复合工作流
//!
//! 子模块按职责划分：`registry`（注册）、`validation`（依赖校验）、`execution`（执行）。
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

use crate::workflow::dag::ExecutionPlan;
use crate::workflow::definition::ErasedWorkflow;
use crate::workflow::model::WorkflowId;

pub use registry::WorkflowManager;

/// 已注册的工作流条目。
#[allow(dead_code)]
struct RegisteredWorkflow {
    id: WorkflowId,
    /// 叶工作流实例（类型擦除）。
    workflow: Option<Box<dyn ErasedWorkflow>>,
    /// 命名空间复合工作流的执行计划。
    ns_plan: Option<ExecutionPlan>,
    /// 是否已校验。
    validated: bool,
    /// 输入类型 ID。
    input_type: TypeId,
    /// 输出类型 ID。
    output_type: TypeId,
}
