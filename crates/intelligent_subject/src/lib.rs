//! # Intelligent Subject — FurtherAGI Workflow Harness
//!
//! 一个强类型、async-first 的工作流执行引擎。工作流由有向无环图（DAG）描述，
//! 支持子工作流、克隆扇出、条件分支、循环、和/积类型组合。
//!
//! ## 核心概念
//!
//! - **Workflow** — 强类型的「输入 → 处理 → 输出」管道，由 [`workflow::definition::Workflow`] trait 定义
//! - **DAG** — 由 [`workflow::dag::ExecutionPlan`] 表示的有向无环图，描述组合工作流的拓扑
//! - **WorkflowManager** — 中央注册器，管理所有工作流的生命周期和依赖校验
//! - **WorkPlatform** — 运行时上下文，提供工作平台能力（文件系统、容器等）
//!
//! ## 三层架构
//!
//! | 层级 | 模块 | 说明 |
//! |------|------|------|
//! | Layer 1 — 服务 | [`workflow::services`] | 不可再分的原子能力，不实现 Workflow trait |
//! | Layer 2 — Builtin | [`workflow::builtin`] | 由服务组合的预构建工作流，实现 Workflow trait |
//! | Layer 3 — Config | [`workflow::config`] | 通过 XML 声明式组合 builtin workflow |
//!
//! ## 节点类型
//!
//! | NodeKind | 说明 |
//! |----------|------|
//! | `Workflow` | 工作流实现节点 |
//! | `Clone` | Scatter-gather 节点：并行分支 + gather 元组输出 |
//! | `Conditional` | 条件分支节点（true/false） |
//! | `Loop` | 固定次数循环节点 |
//! | `Connection` | 命名透传节点 |
//! | `SumMatch` | 或类型拆解节点：`T | E` → ok(T) / err(E) |
//! | `ProductJoin` | 和类型合并节点：多输入 → `(A, B, ...)` |
//! | `Reshape` | 元组重组节点：`(A, B, C)` → `(A, (B, C))` |
//! | `Dispatch` | 积类型拆分节点：`(A, B, C)` → N 条路径 |
//!
//! ## 实现特色
//!
//! - 类型擦除执行通过 `ErasedWorkflow` + `TypeId` 实现，在注册时校验类型安全，执行时 downcast 恢复
//! - 通过 Kahn 算法实现层级并行执行，同一拓扑层的节点并发运行
//! - 支持条件路由、循环、scatter-gather、和/积类型组合
//! - XML 声明式构建与运行时代码驱动构建双路径
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
//!
//! mgr.add("builtin@Double", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input * 2)
//! })?;
//!
//! let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };
//!
//! let result: i32 = mgr.execute_typed("builtin@Double", 21, &ctx).await?;
//! assert_eq!(result, 42);
//! # Ok(())
//! # }
//! ```

pub mod workflow;
