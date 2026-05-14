//! # Workflow — 工作流引擎
//!
//! 本模块实现了 FurtherAGI 的核心工作流系统。
//!
//! ## 三层架构
//!
//! | 层级 | 模块 | 说明 |
//! |------|------|------|
//! | Layer 1 — 服务 | [`services`] | 不可再分的原子能力（Map、Predicate 等），不实现 Workflow trait |
//! | Layer 2 — Builtin | [`builtin`] | 由服务组合的预构建工作流，实现 Workflow trait |
//! | Layer 3 — Config | [`config`] | 通过 XML 声明式组合 builtin workflow |
//!
//! ## 实现特色
//!
//! - 双 trait 架构：强类型 [`Workflow<I, O>`](definition::Workflow)（用户层）+ 类型擦除
//!   [`ErasedWorkflow`](definition::ErasedWorkflow)（存储层），通过 `TypeId` 保证安全
//! - DAG 构建器模式：[`DagBuilder`](dag::DagBuilder) 提供流式 API，即时类型校验 + Kahn 算法环检测
//! - async-first 执行引擎：[`Executor`](executor::Executor) 按拓扑层级并发执行
//! - 插件式工作平台：[`WorkPlatform`](platform::WorkPlatform) trait 支持 Null/Local/Docker 后端
//! - XML 声明式构建：[`ConfigBuilder`](config::ConfigBuilder) 从配置文件构建 DAG
//! - 和/或类型组合：[`ProductJoin`](dag::NodeKind::ProductJoin) 合并为 `(A, B, ...)`，[`SumMatch`](dag::NodeKind::SumMatch) 拆解 `T | E`
//!   [`Reshape`](dag::NodeKind::Reshape) 重组元组嵌套，[`Dispatch`](dag::NodeKind::Dispatch) 拆分和类型到多条路径
//!
//! ## 快速开始
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
//! mgr.add("builtin@Double", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input * 2)
//! })?;
//!
//! let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };
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
//! | [`model`] | 基础类型：`NodeId`、`WorkflowId`、`ExecutionContext`、`StateStore` |
//! | [`platform`] | 工作平台：`WorkPlatform` trait、`NullPlatform`、`LocalPlatform`、`DockerPlatform` |
//! | [`error`] | 统一错误类型 `WorkflowError` |
//! | [`definition`] | 核心 trait：`Workflow<I, O>`（用户实现）和 `ErasedWorkflow`（内部类型擦除） |
//! | [`dag`] | DAG 数据结构和构建器：`WorkflowDag`、`DagBuilder`、`NodeKind`（8 种） |
//! | [`executor`] | Async 执行引擎：拓扑层级并行执行、条件路由、循环、或类型拆解、和类型合并 |
//! | [`services`] | Layer 1 — 原子服务：`MapFn`、`PredicateFn`、`Identity`、`Constant`、`LogService` 等 |
//! | [`builtin`] | Layer 2 — 预构建工作流：`Gt`、`Lt`、`Not`、`And`、`Or` 等 |
//! | [`workflow_manager`] | 中央注册器：`WorkflowManager`，支持注册、校验、类型擦除/强类型执行 |
//! | [`config`] | Layer 3 — XML 配置驱动构建：`ConfigBuilder`、`TypeRegistry`、`WorkflowFactoryRegistry` |
//! | [`tool_registry`] | 工具注册表：`ToolRegistry`，将工作流暴露为 LLM 可调用工具 |

pub mod builtin;
pub mod config;
pub mod dag;
pub mod error;
pub mod executor;
pub mod platform;
pub mod definition;
pub mod model;
pub mod services;
pub mod tool_registry;
pub mod workflow_manager;
