//! # DAG — 有向无环图
//!
//! 定义工作流的拓扑结构和构建器。
//!
//! ## 功能实现
//!
//! 本模块实现工作流 DAG 的数据模型和流式构建器：
//!
//! - **[`NodeKind`]** — 节点类型枚举，涵盖所有组合模式：
//!   `Workflow`、`Clone`、`Loop`、`Conditional`、`SubWorkflow`、`Connection`、`SumMatch`、`ProductJoin`、`Reshape`、`Dispatch`
//! - **[`Node`]** — DAG 节点，包含 `NodeKind`、类型擦除工作流和输入/输出 `TypeId`
//! - **[`Edge`]** — 有向边，可选标签（用于条件分支 `"true"` / `"false"` 或或类型 `"ok"` / `"err"`）
//! - **[`WorkflowDag`]** — 构建完成的不可变 DAG，包含缓存的拓扑排序
//! - **[`DagBuilder`]** — 流式构建器，提供即时类型校验和环检测
//!
//! ## Clone（Scatter-Gather）
//!
//! - **[`Clone`]**(`NodeKind::Clone`) — Scatter-Gather 模式：接收输入 T，
//!   并行运行 N 个分支 workflow，收集结果输出和类型 `(R1, R2, ..., RN)`
//!
//! ## 和类型与或类型
//!
//! - **ProductJoin**(`NodeKind::ProductJoin`) — 和类型合并：收集多个上游值，
//!   通过 [`ProductJoinFn`] 合并为 `(A, B, ...)` 元组输出
//! - **SumMatch**(`NodeKind::SumMatch`) — 或类型拆解：接收 `T | E`（`Result<T, E>`），
//!   路由到 `"ok"` 边（传递 T）或 `"err"` 边（传递 E）
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | `strum_macros`（Display 派生） |
//! | 内部模块 | [`ErasedWorkflow`]、[`NodeId`]、[`WorkflowId`] |
//!
//! ## Core types
//!
//! - [`NodeKind`] -- node type enum (Workflow, Clone, Loop, Conditional, SubWorkflow, Connection, SumMatch, ProductJoin)
//! - [`Node`] -- a node in the DAG, containing a type-erased workflow and input/output types
//! - [`Edge`] -- directed edge, optional label (for conditional/sum_match branching)
//! - [`WorkflowDag`] -- immutable, built DAG
//! - [`DagBuilder`] -- fluent builder with immediate type validation and cycle detection

mod builder;
mod connect;
mod graph;

#[cfg(test)]
mod tests;

use std::any::Any;
use std::collections::HashMap;

use strum_macros::Display;

use super::definition::ErasedWorkflow;
use super::error::WorkflowError;
use super::model::{NodeId, WorkflowId};

// ----- Re-exports -----
pub use graph::{CloneFn, WorkflowDag, make_clone_fn};

/// Result of destructuring a sum type (`T | E`).
pub enum SumMatchResult {
    /// Ok variant: carries the unwrapped T value.
    Ok(Box<dyn Any + Send + Sync>),
    /// Err variant: carries the unwrapped E value.
    Err(Box<dyn Any + Send + Sync>),
}

/// Type-erased function that destructures a `Result<T, E>` into a [`SumMatchResult`].
///
/// Created via [`make_sum_match_destruct_fn`] when T and E are statically known.
/// Returns `Err` if the input cannot be downcast to `Result<T, E>`.
pub type SumMatchDestructFn = Box<
    dyn Fn(Box<dyn Any + Send + Sync>) -> Result<SumMatchResult, WorkflowError> + Send + Sync,
>;

/// Creates a sum-match destructuring function for `Result<T, E>`.
///
/// The returned closure downcasts the input to `Result<T, E>` and extracts
/// the inner `T` (Ok) or `E` (Err) value. Returns an error if downcast fails.
pub fn make_sum_match_destruct_fn<T: Send + Sync + 'static, E: Send + Sync + 'static>() -> SumMatchDestructFn {
    Box::new(|input: Box<dyn Any + Send + Sync>| {
        match input.downcast::<Result<T, E>>() {
            Ok(result) => match *result {
                Ok(t) => Ok(SumMatchResult::Ok(Box::new(t))),
                Err(e) => Ok(SumMatchResult::Err(Box::new(e))),
            },
            Err(_) => Err(WorkflowError::ValidationError(format!(
                "SumMatch: downcast to Result<{}, {}> failed",
                std::any::type_name::<T>(),
                std::any::type_name::<E>()
            ))),
        }
    })
}

/// Type-erased function that combines multiple values into one.
///
/// Takes a `Vec` of boxed values (one per incoming edge) and produces
/// a single boxed output (e.g., a tuple).
pub type ProductJoinFn = Box<
    dyn Fn(Vec<Box<dyn Any + Send + Sync>>) -> Box<dyn Any + Send + Sync> + Send + Sync,
>;

/// Type-erased function that restructures a single value.
///
/// Used by Reshape nodes to change tuple nesting without altering values
/// (e.g., `(A, B, C)` → `(A, (B, C))`).
pub type ReshapeFn = Box<
    dyn Fn(Box<dyn Any + Send + Sync>) -> Box<dyn Any + Send + Sync> + Send + Sync,
>;

/// Type-erased function that splits a single value into multiple values.
///
/// Used by Dispatch nodes to unpack a product type into N separate outputs,
/// one per outgoing edge (in order). The i-th element goes to the i-th edge.
pub type DispatchFn = Box<
    dyn Fn(Box<dyn Any + Send + Sync>) -> Vec<Box<dyn Any + Send + Sync>> + Send + Sync,
>;

/// The kind of a node in the workflow DAG.
#[derive(Debug, Clone, Display)]
pub enum NodeKind {
    /// A concrete workflow implementation.
    #[strum(serialize = "workflow")]
    Workflow(WorkflowId),
    /// Scatter-gather: clones its input to N branch workflows,
    /// runs them in parallel, gathers results into a tuple output.
    #[strum(serialize = "clone")]
    Clone { branch_count: usize },
    /// Fixed-iteration loop: re-runs its body subgraph N times.
    #[strum(serialize = "loop")]
    Loop {
        count: usize,
        body_entry: NodeId,
        body_exit: NodeId,
    },
    /// Conditional branch: routes input to one of two downstream paths
    /// based on a predicate workflow's boolean output.
    #[strum(serialize = "conditional")]
    Conditional { predicate: WorkflowId },
    /// Reference to another registered workflow (expanded at execution time).
    #[strum(serialize = "sub_workflow")]
    SubWorkflow(WorkflowId),
    /// Connection: a named pass-through node for visual organization.
    /// Carries data unchanged. Used to break long edges into segments
    /// for cleaner diagrams.
    #[strum(serialize = "connection")]
    Connection { label: String },
    /// Or-type destructure: takes `T | E` (`Result<T, E>`) and routes to "ok" (T) or "err" (E) edge.
    #[strum(serialize = "sum_match")]
    SumMatch {
        ok_type: std::any::TypeId,
        err_type: std::any::TypeId,
    },
    /// Product-type join: collects outputs from multiple upstream nodes into `(A, B, ...)`.
    #[strum(serialize = "product_join")]
    ProductJoin { input_count: usize },
    /// Tuple restructuring: reshapes tuple nesting (e.g., `(A, B, C)` → `(A, (B, C))`).
    /// Single input, single output.
    #[strum(serialize = "reshape")]
    Reshape,
    /// Product-type dispatch: splits a tuple into N separate values,
    /// one per outgoing edge (in order). The inverse of ProductJoin.
    #[strum(serialize = "dispatch")]
    Dispatch { output_count: usize },
}

/// A node in the workflow DAG.
pub struct Node {
    pub id: NodeId,
    pub kind: NodeKind,
    /// The erased workflow to execute.
    /// `None` for structural nodes (Clone, SumMatch, ProductJoin, Reshape, Dispatch, Connection)
    /// or unresolved SubWorkflow nodes.
    pub workflow: Option<Box<dyn ErasedWorkflow>>,
    /// The TypeId this node expects as input.
    pub input_type: Option<std::any::TypeId>,
    /// The TypeId this node produces as output.
    pub output_type: Option<std::any::TypeId>,
}

/// A directed edge from one node to another.
#[derive(Debug, Clone)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
    /// Optional label for conditional branching (e.g. "true", "false")
    /// or or-type branching (e.g. "ok", "err" for `T | E` destructure).
    pub label: Option<String>,
}

/// Builder for constructing a `WorkflowDag` with validation.
pub struct DagBuilder {
    nodes: HashMap<NodeId, Node>,
    edges: Vec<Edge>,
    next_id: u64,
    entry_node: Option<NodeId>,
    exit_node: Option<NodeId>,
    /// Branch workflows for Clone (scatter-gather) nodes.
    clone_branches: HashMap<NodeId, Vec<Box<dyn ErasedWorkflow>>>,
    /// Gather functions for Clone (scatter-gather) nodes.
    clone_gather_fns: HashMap<NodeId, ProductJoinFn>,
    /// Clone functions for Clone (scatter-gather) input values.
    clone_input_clone_fns: HashMap<NodeId, CloneFn>,
    /// Destruct functions for SumMatch nodes.
    sum_match_fns: HashMap<NodeId, SumMatchDestructFn>,
    /// Join functions for ProductJoin nodes.
    product_join_fns: HashMap<NodeId, ProductJoinFn>,
    /// Clone functions for ProductJoin input values (per node, ordered by edge).
    product_join_input_clone_fns: HashMap<NodeId, Vec<CloneFn>>,
    /// Reshape functions for Reshape nodes.
    reshape_fns: HashMap<NodeId, ReshapeFn>,
    /// Dispatch functions for Dispatch nodes.
    dispatch_fns: HashMap<NodeId, DispatchFn>,
}

impl Default for DagBuilder {
    fn default() -> Self {
        Self::new()
    }
}
