//! # DAG — 执行计划
//!
//! 工作流 DAG 的数据结构和流式构建器。
//!
//! 每个节点执行后将结果存入命名空间（`result_name`），后续节点通过 `{name.field}` 引用。
//! 4 种节点类型：`Workflow`、`If`、`Loop`、`End`，自动依赖分析 + 并发执行。
//!
//! ## 核心 API
//!
//! | 类型 | 说明 |
//! |------|------|
//! | [`PlanBuilder`] | 流式构建器，自动从 `{ref}` 推导依赖边 |
//! | [`ExecutionPlan`] | 构建完成的不可变执行计划 |
//! | [`Node`] | 节点：参数、依赖、子节点 |
//! | [`NodeKind`] | 4 种节点类型枚举 |
//!
//! ## 使用示例
//!
//! ```rust
//! use intelligent_subject::workflow::dag::PlanBuilder;
//! use intelligent_subject::workflow::definition::from_fn;
//! use intelligent_subject::workflow::config::ParamValue;
//! use intelligent_subject::workflow::error::WorkflowError;
//! use intelligent_subject::workflow::model::ExecutionContext;
//!
//! let mut builder = PlanBuilder::new();
//!
//! // 添加工作流节点
//! let a = builder.add_workflow(
//!     "a", "append",
//!     vec![("input".to_string(), ParamValue::Literal("hi".to_string()))],
//!     from_fn("append", |input: String, _| async move {
//!         Ok::<String, WorkflowError>(format!("{input}!"))
//!     }),
//! );
//!
//! // b 引用 {a.value}，自动等待 a 完成
//! let b = builder.add_workflow(
//!     "b", "append",
//!     vec![("input".to_string(), ParamValue::Reference {
//!         namespace: "a".to_string(),
//!         field: "value".to_string(),
//!     })],
//!     from_fn("append", |input: String, _| async move {
//!         Ok::<String, WorkflowError>(format!("{input}!"))
//!     }),
//! );
//!
//! // 终止节点
//! builder.add_end("{b.value}");
//!
//! let plan = builder.build().unwrap();
//! assert_eq!(plan.topo_order.len(), 3); // a, b, end
//! ```

mod builder;
mod graph;

pub use graph::ExecutionPlan;
pub use graph::NodeKind;
pub use graph::Node;
pub use builder::PlanBuilder;
