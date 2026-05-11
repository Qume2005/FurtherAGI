//! # 执行引擎
//!
//! 按 DAG 拓扑层级异步执行工作流。
//!
//! ## 执行策略
//!
//! - **层级并行**：同一拓扑层级的节点通过 `join_all` 并发执行
//! - **Conditional 路由**：谓词求值后仅激活匹配的标签分支（`"true"` / `"false"`）
//! - **Loop**：循环体子图固定次数迭代串联执行
//! - **Broadcast**：通过类型擦除的 clone function 扇出到所有下游
//! - **Error handler**：节点失败时查找配对的 Error handler 尝试恢复
//!
//! ## 示例
//!
//! ```rust
//! use autonomous::workflow::executor::Executor;
//! use autonomous::workflow::dag::DagBuilder;
//! use autonomous::workflow::definition::{Workflow, into_erased};
//! use autonomous::workflow::model::{State, ExecutionContext};
//! use autonomous::workflow::platform::NullPlatform;
//! use autonomous::workflow::error::WorkflowError;
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
//! let ctx = ExecutionContext { state: Arc::new(State::new()), platform: Arc::new(NullPlatform::new()) };
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
use super::model::{ExecutionContext, NodeId};
use super::dag::WorkflowDag;

pub use engine::ExecutionResult;

/// `Box<dyn Any + Send + Sync>` 的类型别名，用于内部值传递。
pub(super) type BoxedValue = Box<dyn Any + Send + Sync>;

/// The async execution engine.
pub struct Executor;
