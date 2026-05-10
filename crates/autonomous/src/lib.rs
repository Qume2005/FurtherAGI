//! # Autonomous — FurtherAGI Workflow Harness
//!
//! 一个强类型、async-first 的工作流执行引擎。工作流由有向无环图（DAG）描述，
//! 支持子工作流、广播、条件分支、循环和错误处理。
//!
//! ## 核心概念
//!
//! - **Workflow** — 强类型的「输入 → 处理 → 输出」管道，由 [`workflow::traits::Workflow`] trait 定义
//! - **DAG** — 由 [`workflow::dag::WorkflowDag`] 表示的有向无环图，描述组合工作流的拓扑
//! - **WorkflowManager** — 中央注册器，管理所有工作流的生命周期和依赖校验
//! - **State / WorkPlatform** — 运行时上下文，提供状态管理和工作平台
//!
//! ## 快速开始
//!
//! ```rust
//! use autonomous::workflow::workflow_manager::WorkflowManager;
//! use autonomous::workflow::types::{State, ExecutionContext};
//! use autonomous::workflow::platform::NullPlatform;
//! use autonomous::workflow::error::WorkflowError;
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), WorkflowError> {
//! let mgr = WorkflowManager::new();
//!
//! mgr.add("builtin@Double", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input * 2)
//! })?;
//!
//! let state = State::new();
//! let platform = NullPlatform::new();
//! let ctx = ExecutionContext { state: &state, platform: &platform };
//!
//! let result: i32 = mgr.execute_typed("builtin@Double", 21, &ctx).await?;
//! assert_eq!(result, 42);
//! # Ok(())
//! # }
//! ```

pub mod workflow;
