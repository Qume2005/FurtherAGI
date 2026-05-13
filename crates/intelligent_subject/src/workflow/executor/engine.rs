//! # 执行引擎 — 核心执行循环
//!
//! 按 DAG 拓扑层级异步执行工作流。
//!
//! ## 功能实现
//!
//! [`Executor::execute()`] 按 DAG 的拓扑层级遍历节点，在每个层级通过 `join_all` 并发
//! 执行所有节点。处理条件分支路由、或类型 `T | E` 拆解路由、和类型 `(A, B)` 合并、
//! 循环体子图迭代和 scatter-gather。

use anyhow::Context;

use super::*;

/// DAG 执行的结果。
pub struct ExecutionResult {
    /// The final output, type-erased.
    pub output: BoxedValue,
    /// The TypeId of the output for downcasting.
    pub output_type: std::any::TypeId,
}

impl std::fmt::Debug for ExecutionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionResult")
            .field("output_type", &self.output_type)
            .finish()
    }
}

impl Executor {
    /// Execute a DAG with type-erased input.
    #[instrument(skip(dag, input, ctx), fields(nodes = dag.topo_order().len()))]
    pub async fn execute(
        dag: &WorkflowDag,
        input: BoxedValue,
        ctx: &ExecutionContext,
    ) -> anyhow::Result<ExecutionResult> {
        let topo = dag.topo_order();
        let entry = dag.entry_node().context("DAG has no entry node")?;
        let exit = dag.exit_node().context("DAG has no exit node")?;

        let body_nodes = Self::collect_body_nodes(dag, topo);

        let mut results: HashMap<NodeId, BoxedValue> = HashMap::new();
        results.insert(entry, input);

        let mut in_degree: HashMap<NodeId, usize> = HashMap::new();
        for &node_id in topo {
            in_degree.insert(node_id, dag.incoming(node_id).len());
        }
        in_degree.insert(entry, 0);

        let mut processed: HashSet<NodeId> = HashSet::new();
        loop {
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

            // Skip body nodes (handled by Loop).
            level.retain(|&node_id| !body_nodes.contains(&node_id));

            // Gather inputs for all nodes in this level.
            let mut level_inputs: HashMap<NodeId, anyhow::Result<BoxedValue>> = HashMap::new();
            for &node_id in &level {
                let node = dag.get_node(node_id);
                let is_product_join = node.map_or(false, |n| matches!(n.kind, NodeKind::ProductJoin { .. }));

                let input = if is_product_join {
                    // ProductJoin: collect from ALL incoming edges via clone_fns.
                    let join_fn = dag.product_join_fn(node_id)
                        .context("product_join node has no join function")?;
                    let input_clone_fns = dag.product_join_input_clone_fns(node_id)
                        .context("product_join node has no input clone fns")?;
                    let incoming = dag.incoming(node_id);
                    let mut collected: Vec<BoxedValue> = Vec::with_capacity(incoming.len());
                    for (i, edge) in incoming.iter().enumerate() {
                        let val = results.get(&edge.from)
                            .with_context(|| format!("product_join: upstream node {:?} not found", edge.from))?;
                        let clone_fn = input_clone_fns.get(i)
                            .with_context(|| format!("product_join: no clone fn for input {}", i))?;
                        collected.push(clone_fn(val.as_ref()));
                    }
                    Ok(join_fn(collected))
                } else {
                    // Standard single-input gathering.
                    let incoming = dag.incoming(node_id);
                    if incoming.is_empty() {
                        results.remove(&node_id).with_context(|| format!("node not found: {node_id:?}"))
                    } else {
                        let from = incoming[0].from;
                        results.remove(&from).with_context(|| format!("node not found: {from:?}"))
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
                        let is_sum_match = node.map_or(false, |n| matches!(n.kind, NodeKind::SumMatch { .. }));

                        if is_sum_match {
                            // SumMatch: handler wraps result in SumMatchCarrier for routing.
                            match val.downcast::<SumMatchCarrier>() {
                                Ok(carrier_box) => {
                                    let carrier = *carrier_box;
                                    results.insert(node_id, carrier.value);
                                    for edge in dag.outgoing(node_id) {
                                        let activate = match &edge.label {
                                            Some(label) if label == "ok" && carrier.is_ok => true,
                                            Some(label) if label == "err" && !carrier.is_ok => true,
                                            None => true,
                                            _ => false,
                                        };
                                        if activate {
                                            if let Some(deg) = in_degree.get_mut(&edge.to) {
                                                *deg = deg.saturating_sub(1);
                                            }
                                        }
                                    }
                                }
                                Err(val) => {
                                    // Fallback: no carrier, just pass through
                                    results.insert(node_id, val);
                                    for edge in dag.outgoing(node_id) {
                                        if let Some(deg) = in_degree.get_mut(&edge.to) {
                                            *deg = deg.saturating_sub(1);
                                        }
                                    }
                                }
                            }
                        } else if is_conditional {
                            let predicate_result = val.downcast_ref::<bool>().copied();
                            results.insert(node_id, val);
                            for edge in dag.outgoing(node_id) {
                                let activate = match (&edge.label, predicate_result) {
                                    (Some(label), Some(true)) if label == "true" => true,
                                    (Some(label), Some(false)) if label == "false" => true,
                                    (None, _) => true,
                                    _ => false,
                                };
                                if activate {
                                    if let Some(deg) = in_degree.get_mut(&edge.to) {
                                        *deg = deg.saturating_sub(1);
                                    }
                                }
                            }
                        } else {
                            results.insert(node_id, val);
                            for edge in dag.outgoing(node_id) {
                                if let Some(deg) = in_degree.get_mut(&edge.to) {
                                    *deg = deg.saturating_sub(1);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        }

        let output = results.remove(&exit).with_context(|| format!("node not found: {exit:?}"))?;
        let output_type = dag
            .get_node(exit)
            .and_then(|n| n.output_type)
            .context("exit node has no output type")?;

        Ok(ExecutionResult { output, output_type })
    }
}

/// Internal carrier used by SumMatch to communicate branch direction back to the executor.
pub(super) struct SumMatchCarrier {
    pub value: BoxedValue,
    pub is_ok: bool,
}
