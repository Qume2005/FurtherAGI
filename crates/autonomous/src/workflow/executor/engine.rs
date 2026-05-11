use super::*;

/// DAG 执行的结果。
///
/// `output` 是类型擦除的最终输出值，`output_type` 是其 `TypeId`，
/// 用于安全的 downcast。
///
/// ```rust,ignore
/// let result = Executor::execute(&dag, Box::new(3i32), &ctx).await?;
/// let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
/// ```
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
}
