use std::any::TypeId;
use std::future::Future;

use super::super::definition::from_fn;
use super::super::error::WorkflowError;
use super::super::model::{ExecutionContext, NodeId, WorkflowId};
use super::{
    CloneFn, DagBuilder, ErasedWorkflow, Node, NodeKind, make_clone_fn,
};

impl DagBuilder {
    pub fn new() -> Self {
        Self {
            nodes: std::collections::HashMap::new(),
            edges: Vec::new(),
            next_id: 0,
            entry_node: None,
            exit_node: None,
            clone_fns: std::collections::HashMap::new(),
        }
    }

    pub(super) fn alloc_id(&mut self) -> NodeId {
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
    /// if you need access to [`ExecutionContext`](ExecutionContext).
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

    /// Add a workflow node from an async closure that needs [`ExecutionContext`](ExecutionContext).
    /// Returns its `NodeId`.
    ///
    /// Use this when the workflow needs to read/write shared state via `ctx.state`.
    /// For simple workflows that don't need context, prefer [`add`](Self::add).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autonomous::workflow::dag::DagBuilder;
    /// use autonomous::workflow::model::ExecutionContext;
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
    /// Use this when you have a `Box<dyn ErasedWorkflow>` from [`into_erased`](super::super::definition::into_erased)
    /// or [`from_fn`](from_fn).
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
    /// The predicate must output `bool` -- this is enforced here.
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
}
