//! # 执行引擎 — 节点分派与辅助方法
//!
//! 实现 [`Executor`](Executor) 的单节点执行分派和辅助方法。
//!
//! ## 功能实现
//!
//! - **[`execute_node()`**](Executor::execute_node) — 根据 [`NodeKind`](NodeKind)
//!   分派到对应处理逻辑
//! - **[`execute_body()`**](Executor::execute_body) — 为 Loop 节点执行 body_entry 到 body_exit 的子图
//! - **[`collect_body_nodes()`**](Executor::collect_body_nodes) — 识别属于循环体的节点，在主遍历中跳过

use anyhow::Context;

use super::*;

use super::engine::{SumMatchCarrier, DispatchCarrier};

impl Executor {
    /// Collect all NodeIds that are part of a Loop node's body subgraph.
    /// These nodes are executed inside execute_node for Loop, not in the main walk.
    pub(super) fn collect_body_nodes(dag: &WorkflowDag, topo: &[NodeId]) -> HashSet<NodeId> {
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
    pub(super) async fn execute_node(
        dag: &WorkflowDag,
        node_id: NodeId,
        input: BoxedValue,
        ctx: &ExecutionContext,
    ) -> anyhow::Result<BoxedValue> {
        let node = dag.get_node(node_id).with_context(|| format!("node not found: {node_id:?}"))?;

        match &node.kind {
            NodeKind::Clone { branch_count } => {
                tracing::debug!(node = ?node_id, branches = branch_count, "scatter-gather execution");
                let branches = dag.clone_branches(node_id)
                    .context("clone node has no branches")?;
                let input_clone_fn = dag.clone_input_clone_fn(node_id)
                    .context("clone node has no input clone fn")?;
                let gather_fn = dag.clone_gather_fn(node_id)
                    .context("clone node has no gather fn")?;

                // Clone input for each branch.
                let inputs: Vec<BoxedValue> = (0..*branch_count)
                    .map(|_| input_clone_fn(input.as_ref()))
                    .collect();

                // Execute all branches concurrently.
                let futures: Vec<_> = branches.iter().zip(inputs.into_iter())
                    .map(|(branch, inp)| branch.execute_erased(inp, ctx))
                    .collect();
                let branch_results = futures_util::future::join_all(futures).await;

                // Collect results, propagating errors.
                let mut gathered: Vec<BoxedValue> = Vec::with_capacity(*branch_count);
                for result in branch_results {
                    gathered.push(result?);
                }

                // Apply the gather function to produce the output tuple.
                Ok(gather_fn(gathered))
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
                    let result = wf
                        .execute_erased(input, ctx)
                        .await
                        .context(format!("conditional predicate failed at node {node_id:?}"))?;
                    Ok(result)
                } else {
                    anyhow::bail!("conditional node {node_id:?} has no predicate")
                }
            }
            NodeKind::Workflow(_) | NodeKind::SubWorkflow(_) => {
                if let Some(ref wf) = node.workflow {
                    let result = wf
                        .execute_erased(input, ctx)
                        .await
                        .context(format!("workflow execution failed at node {node_id:?}"))?;
                    Ok(result)
                } else {
                    anyhow::bail!("node {node_id:?} has no workflow implementation")
                }
            }
            NodeKind::SumMatch { .. } => {
                let destruct_fn = dag.sum_match_fn(node_id)
                    .context("sum_match node has no destruct function")?;
                let result = destruct_fn(input)?;
                match result {
                    SumMatchResult::Ok(val) => Ok(Box::new(SumMatchCarrier { value: val, is_ok: true })),
                    SumMatchResult::Err(val) => Ok(Box::new(SumMatchCarrier { value: val, is_ok: false })),
                }
            }
            NodeKind::ProductJoin { .. } => {
                // Input has already been combined by the join_fn in the main loop.
                Ok(input)
            }
            NodeKind::Reshape => {
                tracing::debug!(node = ?node_id, "reshape execution");
                let reshape_fn = dag.reshape_fn(node_id)
                    .context("reshape node has no reshape function")?;
                Ok(reshape_fn(input))
            }
            NodeKind::Dispatch { output_count } => {
                tracing::debug!(node = ?node_id, outputs = output_count, "dispatch execution");
                let dispatch_fn = dag.dispatch_fn(node_id)
                    .context("dispatch node has no dispatch function")?;
                let values = dispatch_fn(input);
                if values.len() != *output_count {
                    anyhow::bail!(
                        "dispatch node {:?}: expected {} outputs, got {}",
                        node_id, output_count, values.len()
                    );
                }
                Ok(Box::new(DispatchCarrier { values }))
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
    ) -> std::pin::Pin<Box<dyn Future<Output = anyhow::Result<BoxedValue>> + Send + 'a>> {
        Box::pin(async move {
            let topo = dag.topo_order();
            let start = topo.iter().position(|&id| id == body_entry).unwrap_or(0);
            let end = topo.iter().position(|&id| id == body_exit).unwrap_or(topo.len() - 1);

            for i in start..=end {
                let node_id = topo[i];

                // Get input: either seeded (for body_entry) or from predecessor.
                let incoming = dag.incoming(node_id);
                let input = if incoming.is_empty() {
                    results
                        .remove(&node_id)
                        .with_context(|| format!("node not found: {node_id:?}"))?
                } else {
                    let from = incoming[0].from;
                    results.remove(&from).with_context(|| format!("node not found: {from:?}"))?
                };

                // Execute the node's workflow.
                let node = dag.get_node(node_id).with_context(|| format!("node not found: {node_id:?}"))?;
                if matches!(node.kind, NodeKind::Clone { .. }) {
                    // Clone (scatter-gather): execute via execute_node for full branch handling.
                    let result = Self::execute_node(dag, node_id, input, ctx).await?;
                    results.insert(node_id, result);
                } else if let Some(ref wf) = node.workflow {
                    let result = wf
                        .execute_erased(input, ctx)
                        .await
                        .context(format!("workflow execution failed at node {node_id:?}"))?;
                    results.insert(node_id, result);
                } else {
                    // No workflow (e.g., structural node) — pass through.
                    results.insert(node_id, input);
                }
            }

            results.remove(&body_exit).with_context(|| format!("node not found: {body_exit:?}"))
        })
    }
}
