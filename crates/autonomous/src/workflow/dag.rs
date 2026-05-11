//! # DAG — 有向无环图
//!
//! 定义工作流的拓扑结构和构建器。
//!
//! ## 核心类型
//!
//! - [`NodeKind`] — 节点类型枚举（Workflow、Broadcast、Error、Loop、Conditional、SubWorkflow）
//! - [`Node`] — DAG 中的节点，包含类型擦除的 workflow 和输入/输出类型
//! - [`Edge`] — 有向边，可选标签（用于条件分支的 `"true"` / `"false"`）
//! - [`WorkflowDag`] — 构建完成的不可变 DAG
//! - [`DagBuilder`] — 流式构建器，支持即时类型校验和环检测
//!
//! ## 节点类型
//!
//! | 类型 | 说明 | 有 workflow |
//! |------|------|:-----------:|
//! | [`Workflow`](NodeKind::Workflow) | 具体的工作流实现 | 是 |
//! | [`Broadcast`](NodeKind::Broadcast) | 扇出到多个下游 | 否 |
//! | [`Error`](NodeKind::Error) | 错误处理器，配对某个节点 | 是 |
//! | [`Loop`](NodeKind::Loop) | 固定次数循环体 | 否 |
//! | [`Conditional`](NodeKind::Conditional) | 条件分支谓词 | 是 |
//! | [`SubWorkflow`](NodeKind::SubWorkflow) | 子工作流引用 | 否 |
//!
//! ## 示例
//!
//! ```rust
//! use autonomous::workflow::dag::DagBuilder;
//! use autonomous::workflow::error::WorkflowError;
//!
//! let mut builder = DagBuilder::new();
//!
//! // 纯闭包，最常用的方式：
//! let a = builder.add("builtin@AddOne", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input + 1)
//! });
//! let b = builder.add("builtin@AddOne", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input + 1)
//! });
//! builder.connect(a, b).unwrap();
//! builder.set_entry(a).unwrap();
//! builder.set_exit(b).unwrap();
//!
//! let dag = builder.build().unwrap(); // 自动进行环检测
//! ```
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};

use strum_macros::Display;

use super::error::WorkflowError;
use super::traits::{from_fn, ErasedWorkflow};
use super::types::{ExecutionContext, NodeId, WorkflowId};

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

/// A directed acyclic graph representing a composite workflow's topology.
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
pub struct WorkflowDag {
    nodes: HashMap<NodeId, Node>,
    edges: Vec<Edge>,
    entry_node: Option<NodeId>,
    exit_node: Option<NodeId>,
    /// Cached topological order, computed at build time.
    topo_order: Vec<NodeId>,
    /// Clone functions for broadcast nodes.
    /// Maps a broadcast NodeId to a function that can clone its boxed output.
    clone_fns: HashMap<NodeId, fn(&(dyn Any + Send + Sync)) -> Box<dyn Any + Send + Sync>>,
}

/// Type alias for the broadcast clone function.
pub type CloneFn = fn(&(dyn Any + Send + Sync)) -> Box<dyn Any + Send + Sync>;

/// Helper to create a clone function for a specific type.
pub fn make_clone_fn<T: Clone + Send + Sync + 'static>() -> CloneFn {
    |val: &(dyn Any + Send + Sync)| -> Box<dyn Any + Send + Sync> {
        Box::new(val.downcast_ref::<T>().unwrap().clone())
    }
}

impl WorkflowDag {
    pub fn nodes(&self) -> &HashMap<NodeId, Node> {
        &self.nodes
    }

    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    pub fn entry_node(&self) -> Option<NodeId> {
        self.entry_node
    }

    pub fn exit_node(&self) -> Option<NodeId> {
        self.exit_node
    }

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

/// Builder for constructing a `WorkflowDag` with validation.
pub struct DagBuilder {
    nodes: HashMap<NodeId, Node>,
    edges: Vec<Edge>,
    next_id: u64,
    entry_node: Option<NodeId>,
    exit_node: Option<NodeId>,
    clone_fns: HashMap<NodeId, CloneFn>,
}

impl DagBuilder {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: Vec::new(),
            next_id: 0,
            entry_node: None,
            exit_node: None,
            clone_fns: HashMap::new(),
        }
    }

    fn alloc_id(&mut self) -> NodeId {
        let id = NodeId(self.next_id);
        self.next_id += 1;
        id
    }

    /// Add a workflow node. Returns its `NodeId`.
    pub fn add_workflow(
        &mut self,
        id: impl Into<WorkflowId>,
        workflow: Box<dyn ErasedWorkflow>,
    ) -> NodeId {
        let id = id.into();
        let node_id = self.alloc_id();
        let input_type = Some(workflow.input_type_id());
        let output_type = Some(workflow.output_type_id());
        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                kind: NodeKind::Workflow(id),
                workflow: Some(workflow),
                input_type,
                output_type,
            },
        );
        node_id
    }

    /// Add a workflow node from a pure async closure (no context). Returns its `NodeId`.
    ///
    /// The closure takes only the input and returns a future. Use [`add_with_ctx`](Self::add_with_ctx)
    /// if you need access to [`ExecutionContext`](super::types::ExecutionContext).
    ///
    /// ID uses `"namespace@Name"` format; the name part becomes the workflow name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autonomous::workflow::dag::DagBuilder;
    /// use autonomous::workflow::error::WorkflowError;
    ///
    /// let mut builder = DagBuilder::new();
    ///
    /// builder.add("builtin@Double", |input: i32| async move {
    ///     Ok::<i32, WorkflowError>(input * 2)
    /// });
    /// ```
    pub fn add<I, O, F, Fut>(&mut self, id: &str, f: F) -> NodeId
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, WorkflowError>> + Send,
    {
        let wid = WorkflowId::from(id);
        let name = wid.name().to_string();
        self.add_workflow(wid, from_fn(name, move |input: I, _ctx: &ExecutionContext| f(input)))
    }

    /// Add a workflow node from an async closure that needs [`ExecutionContext`](super::types::ExecutionContext).
    /// Returns its `NodeId`.
    ///
    /// Use this when the workflow needs to read/write shared state via `ctx.state`.
    /// For simple workflows that don't need context, prefer [`add`](Self::add).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autonomous::workflow::dag::DagBuilder;
    /// use autonomous::workflow::types::ExecutionContext;
    /// use autonomous::workflow::error::WorkflowError;
    ///
    /// let mut builder = DagBuilder::new();
    ///
    /// builder.add_with_ctx("builtin@Log",
    ///     |input: i32, _ctx: &ExecutionContext| async move {
    ///         Ok::<i32, WorkflowError>(input)
    ///     });
    /// ```
    pub fn add_with_ctx<I, O, F, Fut>(&mut self, id: &str, f: F) -> NodeId
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(I, &ExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, WorkflowError>> + Send,
    {
        let wid = WorkflowId::from(id);
        let name = wid.name().to_string();
        self.add_workflow(wid, from_fn(name, f))
    }

    /// Add a pre-built type-erased workflow node. Returns its `NodeId`.
    ///
    /// Use this when you have a `Box<dyn ErasedWorkflow>` from [`into_erased`](super::traits::into_erased)
    /// or [`from_fn`](super::traits::from_fn).
    pub fn add_erased(&mut self, id: &str, workflow: Box<dyn ErasedWorkflow>) -> NodeId {
        self.add_workflow(WorkflowId::from(id), workflow)
    }

    /// Add a broadcast (fan-out) node.
    /// `T` is the type that passes through unchanged. Must implement `Clone`.
    pub fn add_broadcast<T: Clone + Send + Sync + 'static>(&mut self) -> NodeId {
        self.add_broadcast_erased(TypeId::of::<T>(), make_clone_fn::<T>())
    }

    /// Add a broadcast node with pre-computed type info (type-erased variant).
    pub fn add_broadcast_erased(&mut self, type_id: TypeId, clone_fn: CloneFn) -> NodeId {
        let node_id = self.alloc_id();
        self.clone_fns.insert(node_id, clone_fn);
        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                kind: NodeKind::Broadcast,
                workflow: None,
                input_type: Some(type_id),
                output_type: Some(type_id),
            },
        );
        node_id
    }

    /// Add a named connection (pass-through) node.
    /// The data passes through unchanged. Use for visual organization
    /// to break long edges into labeled segments.
    pub fn add_connection(
        &mut self,
        label: impl Into<String>,
        type_id: TypeId,
    ) -> NodeId {
        let node_id = self.alloc_id();
        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                kind: NodeKind::Connection { label: label.into() },
                workflow: None,
                input_type: Some(type_id),
                output_type: Some(type_id),
            },
        );
        node_id
    }

    /// Add a conditional node with a predicate workflow.
    /// The predicate must output `bool` — this is enforced here.
    pub fn add_conditional(
        &mut self,
        predicate_id: impl Into<WorkflowId>,
        predicate: Box<dyn ErasedWorkflow>,
    ) -> Result<NodeId, WorkflowError> {
        let predicate_id = predicate_id.into();
        if predicate.output_type_id() != TypeId::of::<bool>() {
            return Err(WorkflowError::ValidationError(format!(
                "conditional predicate must output bool, got {:?}",
                predicate.output_type_name()
            )));
        }
        let node_id = self.alloc_id();
        let input_type = Some(predicate.input_type_id());
        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                kind: NodeKind::Conditional { predicate: predicate_id },
                workflow: Some(predicate),
                input_type,
                output_type: Some(TypeId::of::<bool>()),
            },
        );
        Ok(node_id)
    }

    /// Add an error handler node paired with another node.
    pub fn add_error_handler(
        &mut self,
        paired_with: NodeId,
        handler: Box<dyn ErasedWorkflow>,
    ) -> Result<NodeId, WorkflowError> {
        if !self.nodes.contains_key(&paired_with) {
            return Err(WorkflowError::NodeNotFound(paired_with));
        }
        let node_id = self.alloc_id();
        let input_type = Some(handler.input_type_id());
        let output_type = Some(handler.output_type_id());
        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                kind: NodeKind::Error { paired_with },
                workflow: Some(handler),
                input_type,
                output_type,
            },
        );
        Ok(node_id)
    }

    /// Add a loop node that re-runs a body subgraph `count` times.
    pub fn add_loop(
        &mut self,
        count: usize,
        body_entry: NodeId,
        body_exit: NodeId,
    ) -> Result<NodeId, WorkflowError> {
        if !self.nodes.contains_key(&body_entry) {
            return Err(WorkflowError::NodeNotFound(body_entry));
        }
        if !self.nodes.contains_key(&body_exit) {
            return Err(WorkflowError::NodeNotFound(body_exit));
        }
        let node_id = self.alloc_id();
        // Loop input/output types are inferred from the body entry/exit.
        let body_entry_node = self.nodes.get(&body_entry).unwrap();
        let body_exit_node = self.nodes.get(&body_exit).unwrap();
        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                kind: NodeKind::Loop {
                    count,
                    body_entry,
                    body_exit,
                },
                workflow: None,
                input_type: body_entry_node.input_type,
                output_type: body_exit_node.output_type,
            },
        );
        Ok(node_id)
    }

    /// Add a sub-workflow reference node (resolved at registration time).
    pub fn add_sub_workflow(
        &mut self,
        workflow_id: impl Into<WorkflowId>,
        input_type: TypeId,
        output_type: TypeId,
    ) -> NodeId {
        let workflow_id = workflow_id.into();
        let node_id = self.alloc_id();
        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                kind: NodeKind::SubWorkflow(workflow_id),
                workflow: None,
                input_type: Some(input_type),
                output_type: Some(output_type),
            },
        );
        node_id
    }

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

        Ok(WorkflowDag {
            nodes: self.nodes,
            edges: self.edges,
            entry_node: self.entry_node,
            exit_node: self.exit_node,
            topo_order,
            clone_fns: self.clone_fns,
        })
    }
}

impl Default for DagBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::traits::{into_erased, Workflow};
    use crate::workflow::types::ExecutionContext;
    use async_trait::async_trait;

    struct AddOne;
    #[async_trait]
    impl Workflow<i32, i32> for AddOne {
        fn name(&self) -> &str { "add_one" }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
            Ok(input + 1)
        }
    }

    struct MulTwo;
    #[async_trait]
    impl Workflow<i32, i32> for MulTwo {
        fn name(&self) -> &str { "mul_two" }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
            Ok(input * 2)
        }
    }

    struct IsPositive;
    #[async_trait]
    impl Workflow<i32, bool> for IsPositive {
        fn name(&self) -> &str { "is_positive" }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
            Ok(input > 0)
        }
    }

    #[test]
    fn linear_dag() {
        let mut builder = DagBuilder::new();
        let a = builder.add_workflow("add_one", into_erased(AddOne));
        let b = builder.add_workflow("mul_two", into_erased(MulTwo));
        let c = builder.add_workflow("add_one_2", into_erased(AddOne));

        builder.connect(a, b).unwrap();
        builder.connect(b, c).unwrap();
        builder.set_entry(a).unwrap();
        builder.set_exit(c).unwrap();

        let dag = builder.build().unwrap();
        assert_eq!(dag.topo_order().len(), 3);
        assert_eq!(dag.entry_node(), Some(a));
        assert_eq!(dag.exit_node(), Some(c));
        assert_eq!(dag.edges().len(), 2);
    }

    #[test]
    fn cycle_detected() {
        let mut builder = DagBuilder::new();
        let a = builder.add_workflow("a", into_erased(AddOne));
        let b = builder.add_workflow("b", into_erased(AddOne));

        builder.connect(a, b).unwrap();
        builder.connect(b, a).unwrap();

        let result = builder.build();
        assert!(result.is_err());
        match result.unwrap_err() {
            WorkflowError::CycleDetected { nodes } => {
                assert_eq!(nodes.len(), 2);
            }
            other => panic!("expected CycleDetected, got {other:?}"),
        }
    }

    #[test]
    fn type_mismatch_rejected() {
        let mut builder = DagBuilder::new();
        let a = builder.add_workflow("add_one", into_erased(AddOne));
        let b = builder.add_workflow("is_positive", into_erased(IsPositive));

        // a outputs i32, is_positive expects i32 — this should succeed.
        builder.connect(a, b).unwrap();

        // Now try connecting is_positive (outputs bool) to something expecting i32.
        let c = builder.add_workflow("mul_two", into_erased(MulTwo));
        let result = builder.connect(b, c);
        assert!(result.is_err());
    }

    #[test]
    fn broadcast_node() {
        let mut builder = DagBuilder::new();
        let a = builder.add_workflow("add_one", into_erased(AddOne));
        let bc = builder.add_broadcast::<i32>();
        let b = builder.add_workflow("mul_two", into_erased(MulTwo));
        let c = builder.add_workflow("add_one_2", into_erased(AddOne));

        builder.connect(a, bc).unwrap();
        builder.connect(bc, b).unwrap();
        builder.connect(bc, c).unwrap();
        builder.set_entry(a).unwrap();

        let dag = builder.build().unwrap();
        assert_eq!(dag.outgoing(bc).len(), 2);
        assert_eq!(dag.topo_order().len(), 4);
    }

    #[test]
    fn conditional_predicate_must_output_bool() {
        let mut builder = DagBuilder::new();
        let result = builder.add_conditional("bad_pred", into_erased(AddOne));
        assert!(result.is_err());
        match result.unwrap_err() {
            WorkflowError::ValidationError(msg) => {
                assert!(msg.contains("bool"));
            }
            other => panic!("expected ValidationError, got {other:?}"),
        }
    }

    #[test]
    fn labeled_edge() {
        let mut builder = DagBuilder::new();
        let cond = builder.add_conditional("is_pos", into_erased(IsPositive)).unwrap();

        // Branch workflows that accept bool input
        struct BoolPass;
        #[async_trait]
        impl Workflow<bool, bool> for BoolPass {
            fn name(&self) -> &str { "bool_pass" }
            async fn execute(&self, input: bool, _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
                Ok(input)
            }
        }

        let t = builder.add_workflow("true_branch", into_erased(BoolPass));
        let f = builder.add_workflow("false_branch", into_erased(BoolPass));

        builder.connect_labeled(cond, t, "true").unwrap();
        builder.connect_labeled(cond, f, "false").unwrap();

        let dag = builder.build().unwrap();
        let outgoing = dag.outgoing(cond);
        assert_eq!(outgoing.len(), 2);
        let labels: Vec<&str> = outgoing.iter().map(|e| e.label.as_deref().unwrap()).collect();
        assert!(labels.contains(&"true"));
        assert!(labels.contains(&"false"));
    }

    #[test]
    fn connection_node() {
        let mut builder = DagBuilder::new();
        let a = builder.add_workflow("add_one", into_erased(AddOne));
        let conn = builder.add_connection("after_add", TypeId::of::<i32>());
        let b = builder.add_workflow("mul_two", into_erased(MulTwo));

        builder.connect(a, conn).unwrap();
        builder.connect(conn, b).unwrap();
        builder.set_entry(a).unwrap();
        builder.set_exit(b).unwrap();

        let dag = builder.build().unwrap();
        assert_eq!(dag.topo_order().len(), 3);

        // Verify the connection node's kind.
        let conn_node = dag.get_node(conn).unwrap();
        assert!(matches!(conn_node.kind, NodeKind::Connection { .. }));
        if let NodeKind::Connection { label } = &conn_node.kind {
            assert_eq!(label, "after_add");
        }
    }
}