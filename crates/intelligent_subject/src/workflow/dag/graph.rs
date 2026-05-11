//! Immutable workflow DAG and accessor methods.

use std::any::Any;
use std::collections::HashMap;

use super::{Edge, Node, NodeId};

/// Type alias for the broadcast clone function.
pub type CloneFn = fn(&(dyn Any + Send + Sync)) -> Box<dyn Any + Send + Sync>;

/// Helper to create a clone function for a specific type.
pub fn make_clone_fn<T: Clone + Send + Sync + 'static>() -> CloneFn {
    |val: &(dyn Any + Send + Sync)| -> Box<dyn Any + Send + Sync> {
        Box::new(val.downcast_ref::<T>().unwrap().clone())
    }
}

/// A directed acyclic graph representing a composite workflow's topology.
pub struct WorkflowDag {
    nodes: HashMap<NodeId, Node>,
    edges: Vec<Edge>,
    entry_node: Option<NodeId>,
    exit_node: Option<NodeId>,
    /// Cached topological order, computed at build time.
    topo_order: Vec<NodeId>,
    /// Clone functions for broadcast nodes.
    /// Maps a broadcast NodeId to a function that can clone its boxed output.
    clone_fns: HashMap<NodeId, CloneFn>,
}

impl std::fmt::Debug for WorkflowDag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowDag")
            .field("node_count", &self.nodes.len())
            .field("edge_count", &self.edges.len())
            .field("entry_node", &self.entry_node)
            .field("exit_node", &self.exit_node)
            .field("topo_order", &self.topo_order)
            .finish()
    }
}

impl WorkflowDag {
    /// Constructor used by the builder in sibling modules.
    pub(super) fn new(
        nodes: HashMap<NodeId, Node>,
        edges: Vec<Edge>,
        entry_node: Option<NodeId>,
        exit_node: Option<NodeId>,
        topo_order: Vec<NodeId>,
        clone_fns: HashMap<NodeId, CloneFn>,
    ) -> Self {
        Self {
            nodes,
            edges,
            entry_node,
            exit_node,
            topo_order,
            clone_fns,
        }
    }

    /// Return a reference to the node map.
    pub fn nodes(&self) -> &HashMap<NodeId, Node> {
        &self.nodes
    }

    /// Return a slice of all edges in the DAG.
    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    /// Return the entry node ID, if set.
    pub fn entry_node(&self) -> Option<NodeId> {
        self.entry_node
    }

    /// Return the exit node ID, if set.
    pub fn exit_node(&self) -> Option<NodeId> {
        self.exit_node
    }

    /// Return the cached topological order of nodes.
    pub fn topo_order(&self) -> &[NodeId] {
        &self.topo_order
    }

    /// Get outgoing edges from a node.
    pub fn outgoing(&self, node_id: NodeId) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.from == node_id).collect()
    }

    /// Get incoming edges to a node.
    pub fn incoming(&self, node_id: NodeId) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.to == node_id).collect()
    }

    /// Get a node by ID.
    pub fn get_node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(&id)
    }

    /// Get the clone function for a broadcast node, if any.
    pub fn clone_fn(&self, id: NodeId) -> Option<CloneFn> {
        self.clone_fns.get(&id).copied()
    }
}
