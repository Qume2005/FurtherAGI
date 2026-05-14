//! # DAG 构建器 — 节点添加
//!
//! 在 [`DagBuilder`](DagBuilder) 上实现节点添加方法。
//!
//! ## 功能实现
//!
//! 本模块提供节点添加方法，覆盖所有 [`NodeKind`](NodeKind) 变体：
//!
//! | 方法 | 节点类型 | 说明 |
//! |------|----------|------|
//! | [`add()`](DagBuilder::add) | `Workflow` | 从纯异步闭包创建 |
//! | [`add_with_ctx()`](DagBuilder::add_with_ctx) | `Workflow` | 从需要 `ExecutionContext` 的闭包创建 |
//! | [`add_workflow()`](DagBuilder::add_workflow) | `Workflow` | 添加预构建的 `ErasedWorkflow` |
//! | [`add_erased()`](DagBuilder::add_erased) | `Workflow` | 同上，接受字符串 ID |
//! | [`add_scatter_gather()`](DagBuilder::add_scatter_gather) | `Clone` | Scatter-gather 节点 |
//! | [`add_connection()`](DagBuilder::add_connection) | `Connection` | 命名透传节点 |
//! | [`add_conditional()`](DagBuilder::add_conditional) | `Conditional` | 条件分支节点（谓词必须输出 `bool`） |
//! | [`add_loop()`](DagBuilder::add_loop) | `Loop` | 固定次数循环节点 |
//!
//! 每个方法分配新的 [`NodeId`](NodeId) 并捕获类型信息。
//!
//! ## 实现特色
//!
//! - 闭包到工作流自动转换：`add()` / `add_with_ctx()` 通过 [`from_fn`](from_fn)
//!   将闭包包装为 `ErasedWorkflow`
//! - `add_conditional()` 在添加时强制校验谓词输出类型为 `bool`，否则返回 `ValidationError`
//! - `add_loop()` 从循环体入口/出口节点推断输入/输出类型
//! - `add_scatter_gather()` 接收 N 个分支 workflow + gather 函数，校验每个分支的输入类型
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | `std::future::Future` |
//! | 内部模块 | [`from_fn`]、[`WorkflowError`]、[`crate::workflow::model::{ExecutionContext, NodeId, WorkflowId}`] |
//!
//! ## 示例
//!
//! **Scatter-gather 节点：**
//!
//! ```rust
//! use intelligent_subject::workflow::dag::DagBuilder;
//! use intelligent_subject::workflow::definition::{into_erased, Workflow};
//! use intelligent_subject::workflow::error::WorkflowError;
//! use intelligent_subject::workflow::model::ExecutionContext;
//! use async_trait::async_trait;
//!
//! struct Double;
//! #[async_trait]
//! impl Workflow<i32, i32> for Double {
//!     fn name(&self) -> &str { "double" }
//!     async fn execute(&self, input: i32, _ctx: &ExecutionContext)
//!         -> Result<i32, WorkflowError> { Ok(input * 2) }
//! }
//!
//! let mut builder = DagBuilder::new();
//! let gather_fn = Box::new(|vals: Vec<Box<dyn std::any::Any + Send + Sync>>| {
//!     let a = *vals[0].downcast_ref::<i32>().unwrap();
//!     let b = *vals[1].downcast_ref::<i32>().unwrap();
//!     Box::new((a, b)) as Box<dyn std::any::Any + Send + Sync>
//! });
//! let sg = builder.add_scatter_gather_typed::<i32>(
//!     vec![into_erased(Double), into_erased(Double)],
//!     gather_fn,
//!     std::any::TypeId::of::<(i32, i32)>(),
//! );
//! ```

use std::any::TypeId;
use std::future::Future;

use super::super::definition::from_fn;
use super::super::error::WorkflowError;
use super::super::model::{ExecutionContext, NodeId, WorkflowId};
use super::{
    CloneFn, DagBuilder, ErasedWorkflow, Node, NodeKind,
    ProductJoinFn, SumMatchDestructFn, ReshapeFn, DispatchFn, make_clone_fn,
};

impl DagBuilder {
    /// Create a new, empty `DagBuilder`.
    pub fn new() -> Self {
        Self {
            nodes: std::collections::HashMap::new(),
            edges: Vec::new(),
            next_id: 0,
            entry_node: None,
            exit_node: None,
            clone_branches: std::collections::HashMap::new(),
            clone_gather_fns: std::collections::HashMap::new(),
            clone_input_clone_fns: std::collections::HashMap::new(),
            sum_match_fns: std::collections::HashMap::new(),
            product_join_fns: std::collections::HashMap::new(),
            product_join_input_clone_fns: std::collections::HashMap::new(),
            reshape_fns: std::collections::HashMap::new(),
            dispatch_fns: std::collections::HashMap::new(),
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
    /// if you need access to [`ExecutionContext`].
    ///
    /// ID uses `"namespace@Name"` format; the name part becomes the workflow name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use intelligent_subject::workflow::dag::DagBuilder;
    /// use intelligent_subject::workflow::error::WorkflowError;
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

    /// Add a workflow node from an async closure that needs [`ExecutionContext`].
    /// Returns its `NodeId`.
    ///
    /// Use this when the workflow needs access to the execution platform.
    /// For simple workflows that don't need context, prefer [`add`](Self::add).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use intelligent_subject::workflow::dag::DagBuilder;
    /// use intelligent_subject::workflow::model::ExecutionContext;
    /// use intelligent_subject::workflow::error::WorkflowError;
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
    /// or [`from_fn`].
    pub fn add_erased(&mut self, id: &str, workflow: Box<dyn ErasedWorkflow>) -> NodeId {
        self.add_workflow(WorkflowId::from(id), workflow)
    }

    /// Add a scatter-gather (clone) node.
    ///
    /// Takes input `T`, fans out to N branch workflows (each receives a cloned T),
    /// runs all branches in parallel, gathers results into a tuple output `(R1, R2, ..., RN)`.
    ///
    /// # Arguments
    ///
    /// * `input_type` — The `TypeId` of the input type T.
    /// * `input_clone_fn` — Function to clone the input T for each branch.
    /// * `branches` — N branch workflows, each accepting T and producing Ri.
    /// * `gather_fn` — Combines N branch results into the output tuple.
    /// * `output_type` — The `TypeId` of the output tuple type.
    pub fn add_scatter_gather(
        &mut self,
        input_type: TypeId,
        input_clone_fn: CloneFn,
        branches: Vec<Box<dyn ErasedWorkflow>>,
        gather_fn: ProductJoinFn,
        output_type: TypeId,
    ) -> NodeId {
        let branch_count = branches.len();
        // Validate: each branch must accept the input type.
        for (i, branch) in branches.iter().enumerate() {
            assert_eq!(
                branch.input_type_id(),
                input_type,
                "scatter-gather branch {i}: input type mismatch"
            );
        }
        let node_id = self.alloc_id();
        self.clone_branches.insert(node_id, branches);
        self.clone_gather_fns.insert(node_id, gather_fn);
        self.clone_input_clone_fns.insert(node_id, input_clone_fn);
        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                kind: NodeKind::Clone { branch_count },
                workflow: None,
                input_type: Some(input_type),
                output_type: Some(output_type),
            },
        );
        node_id
    }

    /// Add a scatter-gather node with compile-time type information.
    ///
    /// Convenience wrapper around [`add_scatter_gather`](Self::add_scatter_gather)
    /// that automatically captures `TypeId` and `CloneFn` from the generic parameter.
    pub fn add_scatter_gather_typed<T: Clone + Send + Sync + 'static>(
        &mut self,
        branches: Vec<Box<dyn ErasedWorkflow>>,
        gather_fn: ProductJoinFn,
        output_type: TypeId,
    ) -> NodeId {
        self.add_scatter_gather(
            TypeId::of::<T>(),
            make_clone_fn::<T>(),
            branches,
            gather_fn,
            output_type,
        )
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

    /// Add a sum-match node that destructures `Result<T, E>`.
    ///
    /// Routes to the `"ok"` labeled edge with `T`, or the `"err"` labeled edge with `E`.
    /// The input type is `Result<T, E>`; the output type is set to `T` (the ok variant).
    pub fn add_sum_match<T: Send + Sync + 'static, E: Send + Sync + 'static>(&mut self) -> NodeId {
        let ok_type = TypeId::of::<T>();
        let err_type = TypeId::of::<E>();
        let destruct_fn = super::make_sum_match_destruct_fn::<T, E>();
        self.add_sum_match_erased(ok_type, err_type, destruct_fn)
    }

    /// Add a sum-match node with type-erased destructuring function.
    pub fn add_sum_match_erased(
        &mut self,
        ok_type: TypeId,
        err_type: TypeId,
        destruct_fn: SumMatchDestructFn,
    ) -> NodeId {
        let node_id = self.alloc_id();
        self.sum_match_fns.insert(node_id, destruct_fn);
        // Input type is Result<T, E>, output_type set to ok_type for type checking convenience.
        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                kind: NodeKind::SumMatch { ok_type, err_type },
                workflow: None,
                input_type: None,  // SumMatch input type depends on context; skip validation
                output_type: None, // Outputs have different types on ok/err branches
            },
        );
        node_id
    }

    /// Add a product-join node that combines multiple upstream values.
    ///
    /// `input_clone_fns` provides a `CloneFn` for each expected input, in edge order.
    /// `join_fn` combines the collected values into a single output.
    pub fn add_product_join(
        &mut self,
        output_type: TypeId,
        input_clone_fns: Vec<CloneFn>,
        join_fn: ProductJoinFn,
    ) -> NodeId {
        let input_count = input_clone_fns.len();
        let node_id = self.alloc_id();
        self.product_join_fns.insert(node_id, join_fn);
        self.product_join_input_clone_fns.insert(node_id, input_clone_fns);
        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                kind: NodeKind::ProductJoin { input_count },
                workflow: None,
                input_type: None,  // ProductJoin accepts heterogeneous inputs
                output_type: Some(output_type),
            },
        );
        node_id
    }

    /// Add a reshape node that restructures tuple nesting.
    ///
    /// Takes a single input and produces a single output via the provided function.
    /// Input and output types are both `None` because the reshape function
    /// may change the type structure (e.g., `(A, B, C)` → `(A, (B, C))`).
    pub fn add_reshape(
        &mut self,
        reshape_fn: ReshapeFn,
    ) -> NodeId {
        let node_id = self.alloc_id();
        self.reshape_fns.insert(node_id, reshape_fn);
        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                kind: NodeKind::Reshape,
                workflow: None,
                input_type: None,
                output_type: None,
            },
        );
        node_id
    }

    /// Add a dispatch node that splits a product type into multiple outputs.
    ///
    /// The dispatch function receives the input tuple and returns a `Vec` of boxed values,
    /// one per outgoing edge. The i-th element is routed to the i-th outgoing edge.
    pub fn add_dispatch(
        &mut self,
        output_count: usize,
        dispatch_fn: DispatchFn,
    ) -> NodeId {
        let node_id = self.alloc_id();
        self.dispatch_fns.insert(node_id, dispatch_fn);
        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                kind: NodeKind::Dispatch { output_count },
                workflow: None,
                input_type: None,
                output_type: None,
            },
        );
        node_id
    }
}
