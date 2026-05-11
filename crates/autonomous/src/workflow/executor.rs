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
//! use autonomous::workflow::traits::{Workflow, into_erased};
//! use autonomous::workflow::types::{State, ExecutionContext};
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
//! # async fn example() -> Result<(), WorkflowError> {
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

use std::any::Any;
use std::collections::{HashMap, HashSet};

use tracing::instrument;

use super::dag::NodeKind;
use super::error::WorkflowError;
use super::types::{ExecutionContext, NodeId};
use super::dag::WorkflowDag;

/// `Box<dyn Any + Send + Sync>` 的类型别名，用于内部值传递。
type BoxedValue = Box<dyn Any + Send + Sync>;

/// DAG 执行的结果。
///
/// `output` 是类型擦除的最终输出值，`output_type` 是其 `TypeId`，
/// 用于安全的 downcast。
///
/// ```rust,ignore
/// let result = Executor::execute(&dag, Box::new(3i32), &ctx).await?;
/// let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
/// ```
impl std::fmt::Debug for ExecutionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionResult")
            .field("output_type", &self.output_type)
            .finish()
    }
}

pub struct ExecutionResult {
    /// The final output, type-erased.
    pub output: BoxedValue,
    /// The TypeId of the output for downcasting.
    pub output_type: std::any::TypeId,
}

/// The async execution engine.
pub struct Executor;

impl Executor {
    /// Execute a DAG with type-erased input.
    ///
    /// The caller must ensure `input` matches the entry node's expected type.
    /// Nodes at the same topological level run concurrently via tokio.
    #[instrument(skip(dag, input, ctx), fields(nodes = dag.topo_order().len()))]
    pub async fn execute(
        dag: &WorkflowDag,
        input: BoxedValue,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, WorkflowError> {
        let topo = dag.topo_order();
        let entry = dag.entry_node().ok_or_else(|| {
            WorkflowError::ValidationError("DAG has no entry node".into())
        })?;
        let exit = dag.exit_node().ok_or_else(|| {
            WorkflowError::ValidationError("DAG has no exit node".into())
        })?;

        // Collect nodes that belong to loop bodies — they are executed inside execute_node
        // for Loop nodes, not in the main topological walk.
        let body_nodes = Self::collect_body_nodes(dag, topo);

        let mut results: HashMap<NodeId, BoxedValue> = HashMap::new();
        results.insert(entry, input);

        // Build in-degree map.
        let mut in_degree: HashMap<NodeId, usize> = HashMap::new();
        for &node_id in topo {
            in_degree.insert(node_id, dag.incoming(node_id).len());
        }
        in_degree.insert(entry, 0);

        let mut processed: HashSet<NodeId> = HashSet::new();
        loop {
            // Collect ALL nodes with in-degree 0 that haven't been processed yet.
            let mut level: Vec<NodeId> = topo
                .iter()
                .copied()
                .filter(|id| !processed.contains(id) && in_degree.get(id).copied() == Some(0))
                .collect();

            if level.is_empty() {
                break;
            }

            for &id in &level {
                processed.insert(id);
            }

            // Skip: body nodes (handled by Loop), Error handler nodes (handled by try_error_handler).
            level.retain(|&node_id| {
                if body_nodes.contains(&node_id) {
                    // Body nodes are managed by Loop execute_node — skip entirely.
                    // Don't decrement downstream since they're not connected in the main DAG.
                    return false;
                }
                if let Some(node) = dag.get_node(node_id) {
                    if matches!(node.kind, NodeKind::Error { .. }) {
                        for edge in dag.outgoing(node_id) {
                            if let Some(deg) = in_degree.get_mut(&edge.to) {
                                *deg = deg.saturating_sub(1);
                            }
                        }
                        return false;
                    }
                }
                true
            });

            // Gather inputs for all nodes in this level before executing concurrently.
            let mut level_inputs: HashMap<NodeId, Result<BoxedValue, WorkflowError>> = HashMap::new();
            for &node_id in &level {
                let incoming = dag.incoming(node_id);
                let input = if incoming.is_empty() {
                    results.remove(&node_id).ok_or(WorkflowError::NodeNotFound(node_id))
                } else {
                    let from = incoming[0].from;
                    if let Some(clone_fn) = dag.clone_fn(from) {
                        let val = results.get(&from).ok_or(WorkflowError::NodeNotFound(from))?;
                        Ok(clone_fn(val.as_ref()))
                    } else {
                        results.remove(&from).ok_or(WorkflowError::NodeNotFound(from))
                    }
                };
                level_inputs.insert(node_id, input);
            }

            // Execute all nodes in this level concurrently.
            let futures = level.into_iter().map(|node_id| {
                let input = level_inputs.remove(&node_id).unwrap();
                async move {
                    let result = match input {
                        Ok(inp) => Self::execute_node(dag, node_id, inp, ctx).await,
                        Err(e) => Err(e),
                    };
                    (node_id, result)
                }
            });
            let completed = futures_util::future::join_all(futures).await;

            for (node_id, outcome) in completed {
                match outcome {
                    Ok(val) => {
                        let node = dag.get_node(node_id);
                        let is_conditional = node.map_or(false, |n| matches!(n.kind, NodeKind::Conditional { .. }));

                        if is_conditional {
                            // Route only the matching branch.
                            let predicate_result = val.downcast_ref::<bool>().copied();
                            results.insert(node_id, val);
                            for edge in dag.outgoing(node_id) {
                                let activate = match (&edge.label, predicate_result) {
                                    (Some(label), Some(true)) if label == "true" => true,
                                    (Some(label), Some(false)) if label == "false" => true,
                                    (None, _) => true, // unlabeled edges always activate
                                    _ => false,
                                };
                                if activate {
                                    if let Some(deg) = in_degree.get_mut(&edge.to) {
                                        *deg = deg.saturating_sub(1);
                                    }
                                }
                            }
                        } else {
                            // Normal node: decrement all downstream.
                            results.insert(node_id, val);
                            for edge in dag.outgoing(node_id) {
                                if let Some(deg) = in_degree.get_mut(&edge.to) {
                                    *deg = deg.saturating_sub(1);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if let Some(recovered) =
                            Self::try_error_handler(dag, node_id, &e, ctx).await?
                        {
                            for edge in dag.outgoing(node_id) {
                                if let Some(deg) = in_degree.get_mut(&edge.to) {
                                    *deg = deg.saturating_sub(1);
                                }
                            }
                            results.insert(node_id, recovered);
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        let output = results.remove(&exit).ok_or(WorkflowError::NodeNotFound(exit))?;

        let output_type = dag
            .get_node(exit)
            .and_then(|n| n.output_type)
            .ok_or_else(|| {
                WorkflowError::ValidationError("exit node has no output type".into())
            })?;

        Ok(ExecutionResult { output, output_type })
    }

    /// Collect all NodeIds that are part of a Loop node's body subgraph.
    /// These nodes are executed inside execute_node for Loop, not in the main walk.
    fn collect_body_nodes(dag: &WorkflowDag, topo: &[NodeId]) -> HashSet<NodeId> {
        let mut body_nodes = HashSet::new();
        for node in dag.nodes().values() {
            if let NodeKind::Loop { body_entry, body_exit, .. } = &node.kind {
                let start = topo.iter().position(|&id| id == *body_entry);
                let end = topo.iter().position(|&id| id == *body_exit);
                if let (Some(s), Some(e)) = (start, end) {
                    for i in s..=e {
                        body_nodes.insert(topo[i]);
                    }
                }
            }
        }
        body_nodes
    }

    /// Execute a single node.
    async fn execute_node(
        dag: &WorkflowDag,
        node_id: NodeId,
        input: BoxedValue,
        ctx: &ExecutionContext,
    ) -> Result<BoxedValue, WorkflowError> {
        let node = dag.get_node(node_id).ok_or(WorkflowError::NodeNotFound(node_id))?;

        match &node.kind {
            NodeKind::Broadcast => {
                tracing::debug!(node = ?node_id, "broadcast pass-through");
                Ok(input)
            }
            NodeKind::Connection { label } => {
                tracing::debug!(node = ?node_id, label = %label, "connection pass-through");
                Ok(input)
            }
            NodeKind::Loop {
                count,
                body_entry,
                body_exit,
            } => {
                let mut current = input;
                for i in 0..*count {
                    tracing::debug!(node = ?node_id, iteration = i, "loop iteration");
                    let mut body_results: HashMap<NodeId, BoxedValue> = HashMap::new();
                    body_results.insert(*body_entry, current);
                    current =
                        Self::execute_body(dag, *body_entry, *body_exit, &mut body_results, ctx)
                            .await?;
                }
                Ok(current)
            }
            NodeKind::Conditional { .. } => {
                if let Some(ref wf) = node.workflow {
                    let result = wf.execute_erased(input, ctx).await?;
                    Ok(result)
                } else {
                    Err(WorkflowError::execution(node_id, "conditional has no predicate".to_string()))
                }
            }
            NodeKind::Workflow(_) | NodeKind::SubWorkflow(_) => {
                if let Some(ref wf) = node.workflow {
                    let result = wf.execute_erased(input, ctx).await?;
                    Ok(result)
                } else {
                    Err(WorkflowError::execution(node_id, "node has no workflow implementation".to_string()))
                }
            }
            NodeKind::Error { .. } => {
                // Error handlers are invoked by try_error_handler, pass through.
                Ok(input)
            }
        }
    }

    /// Execute a body subgraph from body_entry to body_exit (for loop nodes).
    fn execute_body<'a>(
        dag: &'a WorkflowDag,
        body_entry: NodeId,
        body_exit: NodeId,
        results: &'a mut HashMap<NodeId, BoxedValue>,
        ctx: &'a ExecutionContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<BoxedValue, WorkflowError>> + 'a>> {
        Box::pin(async move {
        let topo = dag.topo_order();
        let start = topo.iter().position(|&id| id == body_entry).unwrap_or(0);
        let end = topo.iter().position(|&id| id == body_exit).unwrap_or(topo.len() - 1);

        for i in start..=end {
            let node_id = topo[i];

            // Get input: either seeded (for body_entry) or from predecessor.
            let incoming = dag.incoming(node_id);
            let input = if incoming.is_empty() {
                // Body entry: take from seeded results.
                results
                    .remove(&node_id)
                    .ok_or(WorkflowError::NodeNotFound(node_id))?
            } else {
                let from = incoming[0].from;
                results.remove(&from).ok_or(WorkflowError::NodeNotFound(from))?
            };

            // Execute the node's workflow.
            let node = dag.get_node(node_id).ok_or(WorkflowError::NodeNotFound(node_id))?;
            if let Some(ref wf) = node.workflow {
                let result = wf.execute_erased(input, ctx).await?;
                results.insert(node_id, result);
            } else {
                // No workflow (e.g., structural node) — pass through.
                results.insert(node_id, input);
            }
        }

        results.remove(&body_exit).ok_or(WorkflowError::NodeNotFound(body_exit))
        })
    }

    /// If a node has a paired error handler, invoke it.
    async fn try_error_handler(
        dag: &WorkflowDag,
        failed_node: NodeId,
        error: &WorkflowError,
        ctx: &ExecutionContext,
    ) -> Result<Option<BoxedValue>, WorkflowError> {
        for node in dag.nodes().values() {
            if let NodeKind::Error { paired_with } = &node.kind {
                if *paired_with == failed_node {
                    if let Some(ref handler) = node.workflow {
                        let error_input: BoxedValue = Box::new(error.to_string());
                        let result = handler.execute_erased(error_input, ctx).await?;
                        return Ok(Some(result));
                    }
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::dag::DagBuilder;
    use crate::workflow::traits::{into_erased, Workflow};
    use crate::workflow::types::State;
    use crate::workflow::platform::NullPlatform;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct AddOne;
    #[async_trait]
    impl Workflow<i32, i32> for AddOne {
        fn name(&self) -> &str { "add_one" }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
            Ok(input + 1)
        }
    }

    struct MulTwo;
    #[async_trait]
    impl Workflow<i32, i32> for MulTwo {
        fn name(&self) -> &str { "mul_two" }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
            Ok(input * 2)
        }
    }

    fn make_ctx() -> ExecutionContext {
        ExecutionContext {
            state: Arc::new(State::new()),
            platform: Arc::new(NullPlatform::new()),
        }
    }

    #[tokio::test]
    async fn linear_chain() {
        // AddOne(3) -> MulTwo(4) -> AddOne(9) = 10
        let mut builder = DagBuilder::new();
        let a = builder.add_workflow("add1", into_erased(AddOne));
        let b = builder.add_workflow("mul2", into_erased(MulTwo));
        let c = builder.add_workflow("add2", into_erased(AddOne));
        builder.connect(a, b).unwrap();
        builder.connect(b, c).unwrap();
        builder.set_entry(a).unwrap();
        builder.set_exit(c).unwrap();
        let dag = builder.build().unwrap();

        let ctx = make_ctx();
        let result = Executor::execute(&dag, Box::new(3i32), &ctx).await.unwrap();
        let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
        assert_eq!(*output, 9);
    }

    #[tokio::test]
    async fn broadcast_fanout() {
        // AddOne(0) -> Broadcast -> [MulTwo, AddOne]
        let mut builder = DagBuilder::new();
        let src = builder.add_workflow("add1", into_erased(AddOne));
        let bc = builder.add_broadcast::<i32>();
        let branch_a = builder.add_workflow("mul2", into_erased(MulTwo));
        let branch_b = builder.add_workflow("add2", into_erased(AddOne));

        builder.connect(src, bc).unwrap();
        builder.connect(bc, branch_a).unwrap();
        builder.connect(bc, branch_b).unwrap();
        builder.set_entry(src).unwrap();
        builder.set_exit(branch_a).unwrap();

        let dag = builder.build().unwrap();
        let ctx = make_ctx();
        let result = Executor::execute(&dag, Box::new(0i32), &ctx).await.unwrap();
        let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
        // branch_a: AddOne(0)=1, MulTwo(1)=2
        assert_eq!(*output, 2);
    }

    #[tokio::test]
    async fn error_handler_recovery() {
        struct Fail;
        #[async_trait]
        impl Workflow<i32, i32> for Fail {
            fn name(&self) -> &str { "fail" }
            async fn execute(&self, _input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
                Err(WorkflowError::ValidationError("intentional failure".into()))
            }
        }

        struct ErrorHandler;
        #[async_trait]
        impl Workflow<String, i32> for ErrorHandler {
            fn name(&self) -> &str { "error_handler" }
            async fn execute(&self, _input: String, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
                Ok(-1)
            }
        }

        let mut builder = DagBuilder::new();
        let fail_node = builder.add_workflow("fail", into_erased(Fail));
        let _err_handler = builder.add_error_handler(fail_node, into_erased(ErrorHandler)).unwrap();
        builder.set_entry(fail_node).unwrap();
        builder.set_exit(fail_node).unwrap();

        let dag = builder.build().unwrap();
        let ctx = make_ctx();
        let result = Executor::execute(&dag, Box::new(42i32), &ctx).await.unwrap();
        let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
        assert_eq!(*output, -1);
    }
}