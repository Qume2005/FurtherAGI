use std::collections::{HashMap, HashSet};

use super::super::error::WorkflowError;
use super::super::model::NodeId;
use super::{DagBuilder, Edge, WorkflowDag};

impl DagBuilder {
    /// Connect two nodes. Validates type compatibility immediately.
    pub fn connect(&mut self, from: NodeId, to: NodeId) -> Result<(), WorkflowError> {
        let from_output = self
            .nodes
            .get(&from)
            .ok_or(WorkflowError::NodeNotFound(from))?
            .output_type;

        let to_input = self
            .nodes
            .get(&to)
            .ok_or(WorkflowError::NodeNotFound(to))?
            .input_type;

        if let (Some(out_ty), Some(in_ty)) = (from_output, to_input) {
            if out_ty != in_ty {
                return Err(WorkflowError::type_mismatch(from, to, in_ty, out_ty));
            }
        }

        self.edges.push(Edge {
            from,
            to,
            label: None,
        });
        Ok(())
    }

    /// Connect two nodes with a label (for conditional branching).
    pub fn connect_labeled(
        &mut self,
        from: NodeId,
        to: NodeId,
        label: impl Into<String>,
    ) -> Result<(), WorkflowError> {
        self.connect(from, to)?;
        self.edges.last_mut().unwrap().label = Some(label.into());
        Ok(())
    }

    /// Set the entry point of the DAG.
    pub fn set_entry(&mut self, node: NodeId) -> Result<(), WorkflowError> {
        if !self.nodes.contains_key(&node) {
            return Err(WorkflowError::NodeNotFound(node));
        }
        self.entry_node = Some(node);
        Ok(())
    }

    /// Set the exit point of the DAG.
    pub fn set_exit(&mut self, node: NodeId) -> Result<(), WorkflowError> {
        if !self.nodes.contains_key(&node) {
            return Err(WorkflowError::NodeNotFound(node));
        }
        self.exit_node = Some(node);
        Ok(())
    }

    /// Build the DAG. Performs cycle detection via Kahn's algorithm
    /// and caches the topological order.
    pub fn build(self) -> Result<WorkflowDag, WorkflowError> {
        let node_count = self.nodes.len();

        // Build adjacency list and in-degree map.
        let mut adj: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        let mut in_degree: HashMap<NodeId, usize> = HashMap::new();

        for node_id in self.nodes.keys() {
            in_degree.insert(*node_id, 0);
            adj.insert(*node_id, Vec::new());
        }

        for edge in &self.edges {
            adj.get_mut(&edge.from).unwrap().push(edge.to);
            *in_degree.get_mut(&edge.to).unwrap() += 1;
        }

        // Kahn's algorithm.
        let mut queue: Vec<NodeId> = in_degree
            .iter()
            .filter(|&(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut topo_order = Vec::with_capacity(node_count);

        while let Some(node_id) = queue.pop() {
            topo_order.push(node_id);
            if let Some(neighbors) = adj.get(&node_id) {
                for &neighbor in neighbors {
                    let deg = in_degree.get_mut(&neighbor).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push(neighbor);
                    }
                }
            }
        }

        if topo_order.len() != node_count {
            // Cycle detected: find nodes not in topo_order.
            let sorted_set: HashSet<NodeId> = topo_order.iter().copied().collect();
            let cycle_nodes: Vec<NodeId> = self
                .nodes
                .keys()
                .filter(|id| !sorted_set.contains(id))
                .copied()
                .collect();
            return Err(WorkflowError::CycleDetected { nodes: cycle_nodes });
        }

        Ok(WorkflowDag::new(
            self.nodes,
            self.edges,
            self.entry_node,
            self.exit_node,
            topo_order,
            self.clone_fns,
        ))
    }
}
