//! TOML configuration types for declarative workflow DAG construction.
//!
//! See the [module-level documentation](super) for the full TOML schema.

use serde::Deserialize;
use std::collections::HashMap;

/// Top-level TOML workflow configuration.
///
/// ```toml
/// [workflow]
/// name = "my_pipeline"
/// entry = "node_a"
/// exit = "node_c"
///
/// [nodes.node_a]
/// kind = "workflow"
/// implementation = "add_one"
///
/// [[edges]]
/// from = "node_a"
/// to = "node_b"
/// ```
#[derive(Debug, Deserialize)]
pub struct WorkflowConfig {
    /// Workflow metadata (name, entry, exit).
    pub workflow: WorkflowMeta,
    /// Node definitions, keyed by user-chosen node name.
    pub nodes: HashMap<String, NodeConfig>,
    /// Directed edges between nodes.
    #[serde(default)]
    pub edges: Vec<EdgeConfig>,
}

/// Workflow metadata section.
#[derive(Debug, Deserialize)]
pub struct WorkflowMeta {
    /// The workflow name, used as `WorkflowId`.
    pub name: String,
    /// Name of the entry node (must exist in `nodes`).
    pub entry: String,
    /// Name of the exit node (must exist in `nodes`).
    pub exit: String,
}

/// A single node definition.
///
/// Discriminated by the `kind` field via `#[serde(tag = "kind")]`.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeConfig {
    /// A concrete workflow implementation node.
    Workflow {
        /// Name of a registered workflow factory.
        implementation: String,
    },
    /// A fan-out broadcast node.
    Broadcast {
        /// Registered type name (e.g. `"i32"`).
        #[serde(rename = "type")]
        type_name: String,
    },
    /// A conditional branch node with a predicate.
    Conditional {
        /// Name of a registered predicate workflow (must output `bool`).
        implementation: String,
    },
    /// A fixed-iteration loop node.
    Loop {
        /// Number of iterations.
        count: usize,
        /// Name of the loop body's entry node.
        body_entry: String,
        /// Name of the loop body's exit node.
        body_exit: String,
    },
    /// An error handler paired with another node.
    ErrorHandler {
        /// Name of the node this handler catches errors for.
        paired_with: String,
        /// Name of a registered handler workflow (input: `String`, output matches paired node).
        implementation: String,
    },
    /// A reference to another registered workflow (resolved at validation time).
    SubWorkflow {
        /// `WorkflowId` of the referenced workflow.
        workflow: String,
        /// Registered type name for the sub-workflow's input.
        input_type: String,
        /// Registered type name for the sub-workflow's output.
        output_type: String,
    },
    /// A named connection (pass-through) node for visual organization.
    Connection {
        /// Label identifying this connection point.
        label: String,
        /// Registered type name for the value passing through.
        #[serde(rename = "type")]
        type_name: String,
    },
}

/// A directed edge between two nodes.
#[derive(Debug, Deserialize)]
pub struct EdgeConfig {
    /// Name of the source node.
    pub from: String,
    /// Name of the target node.
    pub to: String,
    /// Optional label for conditional branching (`"true"` / `"false"`).
    pub label: Option<String>,
}
