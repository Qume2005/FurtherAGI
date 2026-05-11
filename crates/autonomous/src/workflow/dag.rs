//! # DAG -- directed acyclic graph
//!
//! Defines the topology and builder for workflows.
//!
//! ## Core types
//!
//! - [`NodeKind`] -- node type enum (Workflow, Broadcast, Error, Loop, Conditional, SubWorkflow)
//! - [`Node`] -- a node in the DAG, containing a type-erased workflow and input/output types
//! - [`Edge`] -- directed edge, optional label (for conditional branching "true" / "false")
//! - [`WorkflowDag`] -- immutable, built DAG
//! - [`DagBuilder`] -- fluent builder with immediate type validation and cycle detection

mod builder;
mod connect;
mod graph;

#[cfg(test)]
mod tests;

use std::any::TypeId;
use std::collections::HashMap;

use strum_macros::Display;

use super::definition::ErasedWorkflow;
#[cfg(test)]
use super::error::WorkflowError;
use super::model::{NodeId, WorkflowId};

// ----- Re-exports -----
pub use graph::{CloneFn, WorkflowDag, make_clone_fn};

/// The kind of a node in the workflow DAG.
#[derive(Debug, Clone, Display)]
pub enum NodeKind {
    /// A concrete workflow implementation.
    #[strum(serialize = "workflow")]
    Workflow(WorkflowId),
    /// Fan-out: sends its input to all downstream nodes.
    #[strum(serialize = "broadcast")]
    Broadcast,
    /// Error handler: catches errors from a paired node.
    #[strum(serialize = "error")]
    Error { paired_with: NodeId },
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
}

/// A node in the workflow DAG.
pub struct Node {
    pub id: NodeId,
    pub kind: NodeKind,
    /// The erased workflow to execute.
    /// `None` for structural nodes (Broadcast) or unresolved SubWorkflow nodes.
    pub workflow: Option<Box<dyn ErasedWorkflow>>,
    /// The TypeId this node expects as input.
    pub input_type: Option<TypeId>,
    /// The TypeId this node produces as output.
    pub output_type: Option<TypeId>,
}

/// A directed edge from one node to another.
#[derive(Debug, Clone)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
    /// Optional label for conditional branching (e.g. "true", "false").
    pub label: Option<String>,
}

/// Builder for constructing a `WorkflowDag` with validation.
pub struct DagBuilder {
    nodes: HashMap<NodeId, Node>,
    edges: Vec<Edge>,
    next_id: u64,
    entry_node: Option<NodeId>,
    exit_node: Option<NodeId>,
    clone_fns: HashMap<NodeId, CloneFn>,
}

impl Default for DagBuilder {
    fn default() -> Self {
        Self::new()
    }
}
