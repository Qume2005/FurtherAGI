//! # Workflow — 工作流引擎
//!
//! 声明式、类型安全、并发执行的工作流引擎。
//!
//! ## 核心概念
//!
//! 工作流是一个 **DAG（有向无环图）**，由 4 种节点组成：
//!
//! | 节点 | 说明 |
//! |------|------|
//! | `<workflow>` | 执行一个工作流闭包，结果写入命名空间 |
//! | `<if>` | 条件分支，predicate 为 true 时在子命名空间执行子节点，`then` 传播值到父 |
//! | `<loop>` | 固定次数循环，通过 `latest_state` / `next_state` 传递状态 |
//! | `<end>` | 终止并返回结果 |
//!
//! 每个节点将输出存入 **命名空间**（`result_name`），后续节点通过 `{name.field}` 引用。
//! 无依赖的节点 **自动并发执行**。
//!
//! ## 两种创建方式
//!
//! ### 方式一：闭包注册（简单场景）
//!
//! 适合单一输入/输出的叶工作流：
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
//! // 不需要 ctx 的闭包
//! mgr.add("math@Double", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input * 2)
//! })?;
//!
//! // 需要 ctx 的闭包
//! mgr.add_with_ctx("math@Echo", |input: String, _ctx: &ExecutionContext| async move {
//!     Ok::<String, WorkflowError>(input)
//! })?;
//!
//! let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };
//! let result: i32 = mgr.execute_typed("math@Double", 21, &ctx).await?;
//! assert_eq!(result, 42);
//! # Ok(())
//! # }
//! ```
//!
//! ### 方式二：XML 配置（复杂场景）
//!
//! 适合多节点 DAG 工作流，声明式定义节点关系：
//!
//! ```rust
//! use intelligent_subject::workflow::config::ConfigBuilder;
//! use intelligent_subject::workflow::config::WorkflowFactoryRegistry;
//! use intelligent_subject::workflow::definition::from_fn;
//! use intelligent_subject::workflow::model::ExecutionContext;
//! use intelligent_subject::workflow::error::WorkflowError;
//! use std::sync::Arc;
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut reg = WorkflowFactoryRegistry::new();
//! reg.register("append_x", || {
//!     from_fn("append_x", |input: String, _| async move {
//!         Ok::<String, WorkflowError>(format!("{input}X"))
//!     })
//! });
//!
//! let builder = ConfigBuilder::new(reg);
//! let plan = builder.build_from_str(r#"
//!     <workflow>
//!       <workflow result_name="a" impl="append_x" input="hello"/>
//!       <workflow result_name="b" impl="append_x" input="{a.value}"/>
//!       <end result="{b.value}"/>
//!     </workflow>
//! "#)?;
//! assert_eq!(plan.topo_order.len(), 3); // 2 个 workflow 节点 + 1 个 end
//! # Ok(())
//! # }
//! ```
//!
//! ## 三层架构
//!
//! | 层级 | 模块 | 说明 |
//! |------|------|------|
//! | Layer 1 — 服务 | [`services`] | 原子能力：Map、Predicate、LLM 调用等 |
//! | Layer 2 — Builtin | [`builtin`] | 预构建工作流：比较器、类型转换、HTTP、LLM |
//! | Layer 3 — Config | [`config`] | XML 声明式组合工作流 |
//!
//! ## 模块结构
//!
//! | 模块 | 说明 |
//! |------|------|
//! | [`definition`] | 核心 trait：[`Workflow<I,O>`]、[`ErasedWorkflow`]、[`from_fn()`]、[`into_erased()`] |
//! | [`dag`] | DAG 数据结构：[`ExecutionPlan`]、[`PlanBuilder`]、[`NodeKind`]（4 种） |
//! | [`executor`] | 执行引擎：[`Executor`]，自动依赖分析 + 并发 |
//! | [`config`] | XML 配置：[`ConfigBuilder`]（4 种元素） |
//! | [`model`] | 基础类型：`NodeId`、`WorkflowId`、`ExecutionContext`、`Namespace`、`StateStore` |
//! | [`platform`] | 工作平台：`WorkPlatform` trait、`NullPlatform`、`LocalPlatform`、`DockerPlatform` |
//! | [`error`] | 统一错误类型 [`WorkflowError`] |
//! | [`workflow_manager`] | 中央注册器：[`WorkflowManager`] |
//! | [`tool_registry`] | 工具注册表：[`ToolRegistry`]（桥接 JSON ↔ 工作流） |
//! | [`builtin`] | Layer 2 — 预构建工作流 |
//! | [`services`] | Layer 1 — 原子服务 |
//!
//! [`Workflow<I,O>`]: definition::Workflow
//! [`ErasedWorkflow`]: definition::ErasedWorkflow
//! [`from_fn()`]: definition::from_fn
//! [`into_erased()`]: definition::into_erased
//! [`ExecutionPlan`]: dag::ExecutionPlan
//! [`PlanBuilder`]: dag::PlanBuilder
//! [`NodeKind`]: dag::NodeKind
//! [`Executor`]: executor::Executor
//! [`ConfigBuilder`]: config::ConfigBuilder
//! [`WorkflowManager`]: workflow_manager::WorkflowManager
//! [`ToolRegistry`]: tool_registry::ToolRegistry
//! [`WorkflowError`]: error::WorkflowError

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