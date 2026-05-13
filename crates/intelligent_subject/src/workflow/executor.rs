//! # 执行引擎
//!
//! 按 DAG 拓扑层级异步执行工作流。
//!
//! ## 执行策略
//!
//! - **层级并行**：同一拓扑层级的节点通过 `join_all` 并发执行
//! - **Conditional 路由**：谓词求值后仅激活匹配的标签分支（`"true"` / `"false"`）
//! - **Loop**：循环体子图固定次数迭代串联执行
//! - **Clone**：通过类型擦除的 clone function 扇出到所有下游
//!
//! ## 实现特色
//!
//! - 通过 `futures_util::future::join_all` 实现层级并行，同一层节点并发执行
//! - Conditional 节点评估谓词后仅激活匹配标签的下游边，未激活的分支不执行
//! - Loop 节点通过子图逐步迭代，每次迭代独立管理结果映射
//! - Clone 通过缓存的 `CloneFn` 克隆值到多个下游，避免所有权转移
//! - `#[instrument]` tracing spans 提供可观测性
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | `futures-util`（`join_all` 并发）、`tracing`（instrument）、`anyhow`（错误上下文） |
//! | 内部模块 | [`dag::{NodeKind, WorkflowDag}`]、[`model::{ExecutionContext, NodeId}`] |
//!
//! ## 示例
//!
//! ```rust
//! use intelligent_subject::workflow::executor::Executor;
//! use intelligent_subject::workflow::dag::DagBuilder;
//! use intelligent_subject::workflow::definition::{Workflow, into_erased};
//! use intelligent_subject::workflow::model::ExecutionContext;
//! use intelligent_subject::workflow::platform::NullPlatform;
//! use intelligent_subject::workflow::error::WorkflowError;
//! use async_trait::async_trait;
//! use std::sync::Arc;
//!
//! struct Double;
//! #[async_trait]
//! impl Workflow<i32, i32> for Double {
//!     fn name(&self) -> &str { "double" }
//!     async fn execute(&self, input: i32, _ctx: &ExecutionContext)
//!         -> Result<i32, WorkflowError> { Ok(input * 2) }
//! }
//!
//! # #[tokio::main]
//! # async fn example() -> anyhow::Result<()> {
//! let mut builder = DagBuilder::new();
//! let a = builder.add_workflow("double", into_erased(Double));
//! let b = builder.add_workflow("double2", into_erased(Double));
//! builder.connect(a, b).unwrap();
//! builder.set_entry(a).unwrap();
//! builder.set_exit(b).unwrap();
//! let dag = builder.build().unwrap();
//!
//! let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };
//!
//! let result = Executor::execute(&dag, Box::new(3i32), &ctx).await?;
//! let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
//! assert_eq!(*output, 12); // 3 → 6 → 12
//! # Ok(())
//! # }
//! ```

mod engine;
mod handler;
#[cfg(test)]
mod tests;

use std::any::Any;
use std::collections::{HashMap, HashSet};

use tracing::instrument;

use super::dag::NodeKind;
use super::dag::SumMatchResult;
use super::model::{ExecutionContext, NodeId};
use super::dag::WorkflowDag;

pub use engine::ExecutionResult;

/// `Box<dyn Any + Send + Sync>` 的类型别名，用于内部值传递。
pub(super) type BoxedValue = Box<dyn Any + Send + Sync>;

/// The async execution engine.
pub struct Executor;
