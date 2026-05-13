//! # 不可变工作流 DAG
//!
//! [`WorkflowDag`] 是 [`DagBuilder`](super::DagBuilder) 构建完成的不可变结果。
//! 存储 DAG 节点、边、入口/出口节点、缓存拓扑排序、
//! Clone scatter-gather 分支、和类型拆解函数和积类型合并函数，仅提供只读访问器方法。

use std::any::Any;
use std::collections::HashMap;

use crate::workflow::definition::ErasedWorkflow;
use super::{Edge, Node, NodeId, ProductJoinFn, SumMatchDestructFn};

/// Type alias for the clone fan-out function.
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
    /// Branch workflows for Clone (scatter-gather) nodes.
    clone_branches: HashMap<NodeId, Vec<Box<dyn ErasedWorkflow>>>,
    /// Gather functions for Clone (scatter-gather) nodes.
    clone_gather_fns: HashMap<NodeId, ProductJoinFn>,
    /// Clone functions for Clone (scatter-gather) input values.
    clone_input_clone_fns: HashMap<NodeId, CloneFn>,
    /// Destruct functions for SumMatch nodes.
    sum_match_fns: HashMap<NodeId, SumMatchDestructFn>,
    /// Join functions for ProductJoin nodes.
    product_join_fns: HashMap<NodeId, ProductJoinFn>,
    /// Clone functions for ProductJoin input values (per node, ordered by edge).
    product_join_input_clone_fns: HashMap<NodeId, Vec<CloneFn>>,
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

/// Builder arguments for constructing a WorkflowDag.
pub(super) struct DagParts {
    pub nodes: HashMap<NodeId, Node>,
    pub edges: Vec<Edge>,
    pub entry_node: Option<NodeId>,
    pub exit_node: Option<NodeId>,
    pub topo_order: Vec<NodeId>,
    pub clone_branches: HashMap<NodeId, Vec<Box<dyn ErasedWorkflow>>>,
    pub clone_gather_fns: HashMap<NodeId, ProductJoinFn>,
    pub clone_input_clone_fns: HashMap<NodeId, CloneFn>,
    pub sum_match_fns: HashMap<NodeId, SumMatchDestructFn>,
    pub product_join_fns: HashMap<NodeId, ProductJoinFn>,
    pub product_join_input_clone_fns: HashMap<NodeId, Vec<CloneFn>>,
}

impl WorkflowDag {
    /// Constructor used by the builder in sibling modules.
    pub(super) fn from_parts(parts: DagParts) -> Self {
        Self {
            nodes: parts.nodes,
            edges: parts.edges,
            entry_node: parts.entry_node,
            exit_node: parts.exit_node,
            topo_order: parts.topo_order,
            clone_branches: parts.clone_branches,
            clone_gather_fns: parts.clone_gather_fns,
            clone_input_clone_fns: parts.clone_input_clone_fns,
            sum_match_fns: parts.sum_match_fns,
            product_join_fns: parts.product_join_fns,
            product_join_input_clone_fns: parts.product_join_input_clone_fns,
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

    /// Get the branch workflows for a Clone (scatter-gather) node, if any.
    pub fn clone_branches(&self, id: NodeId) -> Option<&Vec<Box<dyn ErasedWorkflow>>> {
        self.clone_branches.get(&id)
    }

    /// Get the gather function for a Clone (scatter-gather) node, if any.
    pub fn clone_gather_fn(&self, id: NodeId) -> Option<&ProductJoinFn> {
        self.clone_gather_fns.get(&id)
    }

    /// Get the input clone function for a Clone (scatter-gather) node, if any.
    pub fn clone_input_clone_fn(&self, id: NodeId) -> Option<CloneFn> {
        self.clone_input_clone_fns.get(&id).copied()
    }

    /// Get the destruct function for a SumMatch node, if any.
    pub fn sum_match_fn(&self, id: NodeId) -> Option<&SumMatchDestructFn> {
        self.sum_match_fns.get(&id)
    }

    /// Get the join function for a ProductJoin node, if any.
    pub fn product_join_fn(&self, id: NodeId) -> Option<&ProductJoinFn> {
        self.product_join_fns.get(&id)
    }

    /// Get the input clone functions for a ProductJoin node, if any.
    pub fn product_join_input_clone_fns(&self, id: NodeId) -> Option<&Vec<CloneFn>> {
        self.product_join_input_clone_fns.get(&id)
    }
}
