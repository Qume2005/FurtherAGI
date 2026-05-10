//! # Workflow Manager — 中央注册器
//!
//! 管理所有工作流的生命周期：注册、依赖校验和执行。
//!
//! ## 功能
//!
//! - **注册**：[`add`](WorkflowManager::add) 直接从闭包注册，
//!   [`register_composite`](WorkflowManager::register_composite) 注册组合工作流
//! - **校验**：[`validate_all`](WorkflowManager::validate_all) 检查子工作流引用、跨工作流环、类型兼容
//! - **执行**：类型擦除执行 [`execute`](WorkflowManager::execute)
//!   和强类型执行 [`execute_typed`](WorkflowManager::execute_typed)
//!
//! ## 线程安全
//!
//! 内部使用 [`DashMap`](https://docs.rs/dashmap) 存储，支持并发注册和执行。
//!
//! ## 示例
//!
//! ```rust
//! use autonomous::workflow::workflow_manager::WorkflowManager;
//! use autonomous::workflow::types::{State, ExecutionContext};
//! use autonomous::workflow::platform::NullPlatform;
//! use autonomous::workflow::error::WorkflowError;
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), WorkflowError> {
//! let mgr = WorkflowManager::new();
//!
//! // 纯闭包注册
//! mgr.add("builtin@Double", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input * 2)
//! })?;
//!
//! // 强类型执行
//! let state = State::new();
//! let platform = NullPlatform::new();
//! let ctx = ExecutionContext { state: &state, platform: &platform };
//!
//! let result: i32 = mgr.execute_typed("builtin@Double", 21, &ctx).await?;
//! assert_eq!(result, 42);
//! # Ok(())
//! # }
//! ```

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};

use dashmap::DashMap;
use tracing::instrument;

use crate::workflow::dag::{NodeKind, WorkflowDag};
use crate::workflow::error::WorkflowError;
use crate::workflow::executor::{ExecutionResult, Executor};
use crate::workflow::traits::{from_fn, ErasedWorkflow};
use crate::workflow::types::{ExecutionContext, NodeId, WorkflowId};

/// A registered workflow entry.
struct RegisteredWorkflow {
    id: WorkflowId,
    /// The type-erased workflow instance.
    workflow: Option<Box<dyn ErasedWorkflow>>,
    /// If this is a composite workflow, its DAG.
    dag: Option<WorkflowDag>,
    /// Whether this workflow has been validated.
    validated: bool,
    /// Input type ID (from the entry node of the DAG or the workflow itself).
    input_type: TypeId,
    /// Output type ID (from the exit node of the DAG or the workflow itself).
    output_type: TypeId,
}

/// The central workflow registry.
///
/// Thread-safe registry where all workflows register after dependency validation.
/// Supports both node (builtin) and composite (DAG-based) workflows.
pub struct WorkflowManager {
    workflows: DashMap<WorkflowId, RegisteredWorkflow>,
}

impl WorkflowManager {
    pub fn new() -> Self {
        Self {
            workflows: DashMap::new(),
        }
    }

    /// Register a node (builtin) workflow (internal).
    /// Validated immediately — no sub-workflow deps to resolve.
    pub(crate) fn register_node(
        &self,
        id: impl Into<WorkflowId>,
        workflow: Box<dyn ErasedWorkflow>,
    ) -> Result<(), WorkflowError> {
        let id = id.into();
        self.register_node_inner(&id, workflow)
    }

    #[instrument(skip(self, workflow))]
    fn register_node_inner(
        &self,
        id: &WorkflowId,
        workflow: Box<dyn ErasedWorkflow>,
    ) -> Result<(), WorkflowError> {
        let input_type = workflow.input_type_id();
        let output_type = workflow.output_type_id();
        let entry = RegisteredWorkflow {
            id: id.clone(),
            workflow: Some(workflow),
            dag: None,
            validated: true,
            input_type,
            output_type,
        };
        self.workflows.insert(id.clone(), entry);
        Ok(())
    }

    /// 添加工作流（纯闭包，不需要 `ExecutionContext`）。
    ///
    /// 最常用的注册方式。闭包只接收输入，返回输出。
    /// 如果需要访问 `ctx.state`，使用 [`add_with_ctx`](Self::add_with_ctx)。
    ///
    /// ID 使用 `"namespace@Name"` 格式，工作流名称自动取 name 部分。
    ///
    /// # 示例
    ///
    /// ```rust
    /// use autonomous::workflow::workflow_manager::WorkflowManager;
    /// use autonomous::workflow::types::{State, ExecutionContext};
    /// use autonomous::workflow::platform::NullPlatform;
    /// use autonomous::workflow::error::WorkflowError;
    ///
    /// # #[tokio::main]
    /// # async fn example() -> Result<(), WorkflowError> {
    /// let mgr = WorkflowManager::new();
    ///
    /// mgr.add("builtin@Double", |input: i32| async move {
    ///     Ok::<i32, WorkflowError>(input * 2)
    /// })?;
    ///
    /// let state = State::new();
    /// let platform = NullPlatform::new();
    /// let ctx = ExecutionContext { state: &state, platform: &platform };
    ///
    /// let result: i32 = mgr.execute_typed("builtin@Double", 21, &ctx).await?;
    /// assert_eq!(result, 42);
    /// # Ok(())
    /// # }
    /// ```
    pub fn add<I, O, F, Fut>(&self, id: &str, f: F) -> Result<(), WorkflowError>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<O, WorkflowError>> + Send,
    {
        let wid = WorkflowId::from(id);
        let name = wid.name().to_string();
        self.register_node(wid, from_fn(name, move |input: I, _ctx: &ExecutionContext<'_>| f(input)))
    }

    /// 添加工作流（需要 `ExecutionContext` 的闭包）。
    ///
    /// 用于需要读写共享状态 `ctx.state` 的工作流。
    /// 简单场景优先使用 [`add`](Self::add)。
    ///
    /// # 示例
    ///
    /// ```rust
    /// use autonomous::workflow::workflow_manager::WorkflowManager;
    /// use autonomous::workflow::types::{State, ExecutionContext};
    /// use autonomous::workflow::platform::NullPlatform;
    /// use autonomous::workflow::error::WorkflowError;
    ///
    /// # #[tokio::main]
    /// # async fn example() -> Result<(), WorkflowError> {
    /// let mgr = WorkflowManager::new();
    ///
    /// mgr.add_with_ctx("builtin@Accumulate",
    ///     |input: i32, ctx: &ExecutionContext<'_>| {
    ///         let prev = ctx.state.get::<i32>("acc").unwrap_or(0);
    ///         ctx.state.set("acc", prev + input);
    ///         async move { Ok::<i32, WorkflowError>(prev + input) }
    ///     })?;
    ///
    /// let state = State::new();
    /// let platform = NullPlatform::new();
    /// let ctx = ExecutionContext { state: &state, platform: &platform };
    ///
    /// let r: i32 = mgr.execute_typed("builtin@Accumulate", 10, &ctx).await?;
    /// assert_eq!(r, 10);
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_with_ctx<I, O, F, Fut>(&self, id: &str, f: F) -> Result<(), WorkflowError>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(I, &ExecutionContext<'_>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<O, WorkflowError>> + Send,
    {
        let wid = WorkflowId::from(id);
        let name = wid.name().to_string();
        self.register_node(wid, from_fn(name, f))
    }

    /// 添加预构建的类型擦除工作流。
    ///
    /// 用于 `into_erased()` 或 `from_fn()` 的结果。
    pub fn add_erased(&self, id: &str, workflow: Box<dyn ErasedWorkflow>) -> Result<(), WorkflowError> {
        self.register_node(WorkflowId::from(id), workflow)
    }

    /// Register a composite workflow defined by a DAG.
    /// The DAG may contain SubWorkflow nodes referencing other WorkflowIds.
    /// Validation is deferred until `validate_all()` is called.
    pub fn register_composite(
        &self,
        id: impl Into<WorkflowId>,
        dag: WorkflowDag,
    ) -> Result<(), WorkflowError> {
        let id = id.into();
        self.register_composite_inner(&id, dag)
    }

    #[instrument(skip(self, dag))]
    fn register_composite_inner(
        &self,
        id: &WorkflowId,
        dag: WorkflowDag,
    ) -> Result<(), WorkflowError> {
        let entry_node = dag.entry_node().ok_or_else(|| {
            WorkflowError::ValidationError(format!("composite workflow '{id}' has no entry node"))
        })?;
        let exit_node = dag.exit_node().ok_or_else(|| {
            WorkflowError::ValidationError(format!("composite workflow '{id}' has no exit node"))
        })?;

        let input_type = dag
            .get_node(entry_node)
            .and_then(|n| n.input_type)
            .ok_or_else(|| {
                WorkflowError::ValidationError(format!(
                    "entry node of '{id}' has no input type"
                ))
            })?;
        let output_type = dag
            .get_node(exit_node)
            .and_then(|n| n.output_type)
            .ok_or_else(|| {
                WorkflowError::ValidationError(format!(
                    "exit node of '{id}' has no output type"
                ))
            })?;

        let entry = RegisteredWorkflow {
            id: id.clone(),
            workflow: None,
            dag: Some(dag),
            validated: false,
            input_type,
            output_type,
        };
        self.workflows.insert(id.clone(), entry);
        Ok(())
    }

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

    /// Execute a registered workflow by ID with type-erased input.
    pub async fn execute(
        &self,
        id: impl Into<WorkflowId>,
        input: Box<dyn Any + Send + Sync>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<ExecutionResult, WorkflowError> {
        let id = id.into();
        self.execute_inner(&id, input, ctx).await
    }

    #[instrument(skip(self, input, ctx))]
    async fn execute_inner(
        &self,
        id: &WorkflowId,
        input: Box<dyn Any + Send + Sync>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<ExecutionResult, WorkflowError> {
        let entry = self.workflows.get(id).ok_or_else(|| {
            WorkflowError::WorkflowNotFound(id.clone())
        })?;

        if !entry.validated {
            return Err(WorkflowError::ValidationError(format!(
                "workflow '{id}' has not been validated — call validate_all() first"
            )));
        }

        if let Some(ref dag) = entry.dag {
            // Composite workflow: execute its DAG.
            Executor::execute(dag, input, ctx).await
        } else if let Some(ref wf) = entry.workflow {
            // Leaf workflow: execute directly.
            let output_type = wf.output_type_id();
            let output = wf.execute_erased(input, ctx).await?;
            Ok(ExecutionResult { output, output_type })
        } else {
            Err(WorkflowError::execution(
                NodeId(0),
                format!("workflow '{id}' has no implementation"),
            ))
        }
    }

    /// Execute a workflow with typed input and get typed output.
    /// This is the primary user-facing API.
    pub async fn execute_typed<I: Send + Sync + 'static, O: Send + Sync + 'static>(
        &self,
        id: impl Into<WorkflowId>,
        input: I,
        ctx: &ExecutionContext<'_>,
    ) -> Result<O, WorkflowError> {
        let id = id.into();
        let entry = self.workflows.get(&id).ok_or_else(|| {
            WorkflowError::WorkflowNotFound(id.clone())
        })?;

        // Type check at the boundary.
        if entry.input_type != TypeId::of::<I>() {
            return Err(WorkflowError::ValidationError(format!(
                "input type mismatch for '{}': expected {:?}, got {:?}",
                id,
                entry.input_type,
                TypeId::of::<I>()
            )));
        }
        if entry.output_type != TypeId::of::<O>() {
            return Err(WorkflowError::ValidationError(format!(
                "output type mismatch for '{}': expected {:?}, got {:?}",
                id,
                entry.output_type,
                TypeId::of::<O>()
            )));
        }

        drop(entry); // Release DashMap reference.

        let result = self.execute(id, Box::new(input), ctx).await?;
        result
            .output
            .downcast::<O>()
            .map(|b| *b)
            .map_err(|_| WorkflowError::ValidationError("output downcast failed".into()))
    }

    /// Check if a workflow is registered.
    pub fn contains(&self, id: impl Into<WorkflowId>) -> bool {
        self.workflows.contains_key(&id.into())
    }

    /// Get the number of registered workflows.
    pub fn len(&self) -> usize {
        self.workflows.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.workflows.is_empty()
    }
}

impl Default for WorkflowManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::dag::DagBuilder;
    use crate::workflow::traits::{into_erased, Workflow};
    use crate::workflow::types::State;
    use crate::workflow::platform::NullPlatform;
    use async_trait::async_trait;
    use std::sync::LazyLock;

    struct AddOne;
    #[async_trait]
    impl Workflow<i32, i32> for AddOne {
        fn name(&self) -> &str { "add_one" }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext<'_>) -> Result<i32, WorkflowError> {
            Ok(input + 1)
        }
    }

    struct MulTwo;
    #[async_trait]
    impl Workflow<i32, i32> for MulTwo {
        fn name(&self) -> &str { "mul_two" }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext<'_>) -> Result<i32, WorkflowError> {
            Ok(input * 2)
        }
    }

    static STATE: LazyLock<State> = LazyLock::new(State::new);
    static PLATFORM: LazyLock<NullPlatform> = LazyLock::new(NullPlatform::new);

    fn make_ctx() -> ExecutionContext<'static> {
        ExecutionContext {
            state: &STATE,
            platform: &*PLATFORM,
        }
    }

    #[tokio::test]
    async fn register_and_execute_node() {
        let mgr = WorkflowManager::new();
        mgr.register_node("add_one", into_erased(AddOne))
            .unwrap();

        let ctx = make_ctx();
        let result: i32 = mgr
            .execute_typed("add_one", 5, &ctx)
            .await
            .unwrap();
        assert_eq!(result, 6);
    }

    #[tokio::test]
    async fn register_composite_and_execute() {
        let mgr = WorkflowManager::new();

        // Register nodes first.
        mgr.register_node("add_one", into_erased(AddOne))
            .unwrap();
        mgr.register_node("mul_two", into_erased(MulTwo))
            .unwrap();

        // Build a composite: add_one -> mul_two.
        let mut builder = DagBuilder::new();
        let a = builder.add_workflow("add_one", into_erased(AddOne));
        let b = builder.add_workflow("mul_two", into_erased(MulTwo));
        builder.connect(a, b).unwrap();
        builder.set_entry(a).unwrap();
        builder.set_exit(b).unwrap();
        let dag = builder.build().unwrap();

        mgr.register_composite("double_plus_one", dag)
            .unwrap();
        mgr.validate_all().unwrap();

        let ctx = make_ctx();
        let result: i32 = mgr
            .execute_typed("double_plus_one", 3, &ctx)
            .await
            .unwrap();
        // AddOne(3)=4, MulTwo(4)=8
        assert_eq!(result, 8);
    }

    #[test]
    fn cycle_detected_across_workflows() {
        let mgr = WorkflowManager::new();

        // Build workflow A that references B (via SubWorkflow node).
        let mut builder_a = DagBuilder::new();
        let sub_b = builder_a.add_sub_workflow(
            "B",
            std::any::TypeId::of::<i32>(),
            std::any::TypeId::of::<i32>(),
        );
        builder_a.set_entry(sub_b).unwrap();
        builder_a.set_exit(sub_b).unwrap();
        let dag_a = builder_a.build().unwrap();

        // Build workflow B that references A.
        let mut builder_b = DagBuilder::new();
        let sub_a = builder_b.add_sub_workflow(
            "A",
            std::any::TypeId::of::<i32>(),
            std::any::TypeId::of::<i32>(),
        );
        builder_b.set_entry(sub_a).unwrap();
        builder_b.set_exit(sub_a).unwrap();
        let dag_b = builder_b.build().unwrap();

        mgr.register_composite("A", dag_a).unwrap();
        mgr.register_composite("B", dag_b).unwrap();

        let result = mgr.validate_all();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("circular"));
    }

    #[test]
    fn self_reference_rejected() {
        let mgr = WorkflowManager::new();

        let mut builder = DagBuilder::new();
        let sub = builder.add_sub_workflow(
            "self_ref",
            std::any::TypeId::of::<i32>(),
            std::any::TypeId::of::<i32>(),
        );
        builder.set_entry(sub).unwrap();
        builder.set_exit(sub).unwrap();
        let dag = builder.build().unwrap();

        mgr.register_composite("self_ref", dag)
            .unwrap();

        let result = mgr.validate_all();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("itself"));
    }

    #[test]
    fn missing_sub_workflow_rejected() {
        let mgr = WorkflowManager::new();

        let mut builder = DagBuilder::new();
        let sub = builder.add_sub_workflow(
            "nonexistent",
            std::any::TypeId::of::<i32>(),
            std::any::TypeId::of::<i32>(),
        );
        builder.set_entry(sub).unwrap();
        builder.set_exit(sub).unwrap();
        let dag = builder.build().unwrap();

        mgr.register_composite("my_wf", dag)
            .unwrap();

        let result = mgr.validate_all();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("non-existent"));
    }

    #[tokio::test]
    async fn unvalidated_workflow_rejected() {
        let mgr = WorkflowManager::new();

        // Register a composite without validation.
        let mut builder = DagBuilder::new();
        let sub = builder.add_sub_workflow(
            "missing",
            std::any::TypeId::of::<i32>(),
            std::any::TypeId::of::<i32>(),
        );
        builder.set_entry(sub).unwrap();
        builder.set_exit(sub).unwrap();
        let dag = builder.build().unwrap();

        mgr.register_composite("unvalidated", dag)
            .unwrap();
        // Don't call validate_all().

        let ctx = make_ctx();
        let result = mgr
            .execute("unvalidated", Box::new(1i32), &ctx)
            .await;
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("not been validated"));
    }
}
