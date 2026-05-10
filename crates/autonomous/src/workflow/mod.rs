//! # Workflow — 工作流引擎
//!
//! 本模块实现了 FurtherAGI 的核心工作流系统。
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
//! mgr.add("builtin@Double", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input * 2)
//! })?;
//!
//! let state = State::new();
//! let platform = NullPlatform::new();
//! let ctx = ExecutionContext { state: &state, platform: &platform };
//! let result: i32 = mgr.execute_typed("builtin@Double", 21, &ctx).await?;
//! assert_eq!(result, 42);
//! # Ok(())
//! # }
//! ```
//!
//! ## 模块结构
//!
//! | 模块 | 说明 |
//! |------|------|
//! | [`types`] | 基础类型：`NodeId`、`WorkflowId`、`State`、`ExecutionContext` |
//! | [`platform`] | 工作平台：`WorkPlatform` trait、`NullPlatform`、`LocalPlatform`、`DockerPlatform` |
//! | [`error`] | 统一错误类型 `WorkflowError` |
//! | [`traits`] | 核心 trait：`Workflow<I, O>`（用户实现）和 `ErasedWorkflow`（内部类型擦除） |
//! | [`dag`] | DAG 数据结构和构建器：`WorkflowDag`、`DagBuilder`、`NodeKind` |
//! | [`executor`] | Async 执行引擎：拓扑层级并行执行、条件路由、循环、错误恢复 |
//! | [`builtin_workflows`] | 内建工作流标准库：`Identity`、`Map`、`Predicate`、`Constant`、`Log`、`Delay` |
//! | [`workflow_manager`] | 中央注册器：`WorkflowManager`，支持注册、校验、类型擦除/强类型执行 |
//! | [`config`] | 配置驱动构建：从 TOML 文件声明式构建 DAG |

pub mod builtin_workflows;
pub mod config;
pub mod dag;
pub mod error;
pub mod executor;
pub mod platform;
pub mod traits;
pub mod types;
pub mod workflow_manager;
