use super::*;

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
    pub(super) async fn try_error_handler(
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
