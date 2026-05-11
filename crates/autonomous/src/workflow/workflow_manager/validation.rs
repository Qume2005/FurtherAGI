use std::collections::{HashMap, HashSet};

use tracing::instrument;

use crate::workflow::dag::NodeKind;
use crate::workflow::error::WorkflowError;
use crate::workflow::model::WorkflowId;

use super::WorkflowManager;

impl WorkflowManager {
    /// Validate all registered workflows. Checks:
    /// 1. All SubWorkflow references resolve to registered WorkflowIds.
    /// 2. No cycles in the sub-workflow reference graph.
    /// 3. Type compatibility at sub-workflow boundaries.
    /// 4. No self-references.
    #[instrument(skip(self))]
    pub fn validate_all(&self) -> Result<(), WorkflowError> {
        let ids: Vec<WorkflowId> = self.workflows.iter().map(|r| r.id.clone()).collect();

        // 1. Resolve SubWorkflow references and check type compatibility.
        for id in &ids {
            let entry = self.workflows.get(id).ok_or_else(|| {
                WorkflowError::WorkflowNotFound(id.clone())
            })?;

            if let Some(ref dag) = entry.dag {
                for node in dag.nodes().values() {
                    if let NodeKind::SubWorkflow(ref sub_id) = node.kind {
                        // No self-reference.
                        if sub_id == id {
                            return Err(WorkflowError::ValidationError(format!(
                                "workflow '{id}' references itself as sub-workflow"
                            )));
                        }

                        // Sub-workflow must exist.
                        let sub = self.workflows.get(sub_id).ok_or_else(|| {
                            WorkflowError::ValidationError(format!(
                                "workflow '{id}' references non-existent sub-workflow '{sub_id}'"
                            ))
                        })?;

                        // Type compatibility.
                        if let Some(node_input) = node.input_type {
                            if sub.input_type != node_input {
                                return Err(WorkflowError::ValidationError(format!(
                                    "sub-workflow '{sub_id}' input type mismatch in '{id}'"
                                )));
                            }
                        }
                        if let Some(node_output) = node.output_type {
                            if sub.output_type != node_output {
                                return Err(WorkflowError::ValidationError(format!(
                                    "sub-workflow '{sub_id}' output type mismatch in '{id}'"
                                )));
                            }
                        }
                    }
                }
            }
        }

        // 2. Cycle detection in the sub-workflow reference graph.
        // Build a directed graph: edge from A to B means A contains a SubWorkflow ref to B.
        let mut dep_graph: HashMap<WorkflowId, Vec<WorkflowId>> = HashMap::new();
        for id in &ids {
            let mut deps = Vec::new();
            let entry = self.workflows.get(id).unwrap();
            if let Some(ref dag) = entry.dag {
                for node in dag.nodes().values() {
                    if let NodeKind::SubWorkflow(ref sub_id) = node.kind {
                        if !deps.contains(sub_id) {
                            deps.push(sub_id.clone());
                        }
                    }
                }
            }
            dep_graph.insert(id.clone(), deps);
        }

        // Kahn's algorithm on the dependency graph.
        let mut in_degree: HashMap<&WorkflowId, usize> = HashMap::new();
        for id in &ids {
            in_degree.insert(id, 0);
        }
        for (_, deps) in &dep_graph {
            for dep in deps {
                if let Some(deg) = in_degree.get_mut(dep) {
                    *deg += 1;
                }
            }
        }

        let mut queue: Vec<&WorkflowId> = in_degree
            .iter()
            .filter(|&(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut sorted = Vec::new();
        while let Some(id) = queue.pop() {
            sorted.push(id);
            if let Some(deps) = dep_graph.get(id) {
                for dep in deps {
                    if let Some(deg) = in_degree.get_mut(dep) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push(dep);
                        }
                    }
                }
            }
        }

        if sorted.len() != ids.len() {
            let sorted_set: HashSet<&WorkflowId> = sorted.into_iter().collect();
            let cycle_nodes: Vec<WorkflowId> = ids
                .iter()
                .filter(|id| !sorted_set.contains(id))
                .cloned()
                .collect();
            return Err(WorkflowError::ValidationError(format!(
                "circular sub-workflow reference among: {:?}", cycle_nodes
            )));
        }

        // Mark all as validated.
        for mut entry in self.workflows.iter_mut() {
            entry.validated = true;
        }

        Ok(())
    }
}
