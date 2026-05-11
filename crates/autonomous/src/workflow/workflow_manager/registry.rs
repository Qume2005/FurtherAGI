use tracing::instrument;

use crate::workflow::dag::WorkflowDag;
use crate::workflow::definition::{from_fn, ErasedWorkflow};
use crate::workflow::error::WorkflowError;
use crate::workflow::model::{ExecutionContext, WorkflowId};

use super::RegisteredWorkflow;

/// The central workflow registry.
///
/// Thread-safe registry where all workflows register after dependency validation.
/// Supports both node (builtin) and composite (DAG-based) workflows.
pub struct WorkflowManager {
    pub(super) workflows: dashmap::DashMap<WorkflowId, RegisteredWorkflow>,
}

impl WorkflowManager {
    pub fn new() -> Self {
        Self {
            workflows: dashmap::DashMap::new(),
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
    /// use autonomous::workflow::model::{State, ExecutionContext};
    /// use autonomous::workflow::platform::NullPlatform;
    /// use autonomous::workflow::error::WorkflowError;
    /// use std::sync::Arc;
    ///
    /// # #[tokio::main]
    /// # async fn example() -> Result<(), WorkflowError> {
    /// let mgr = WorkflowManager::new();
    ///
    /// mgr.add("builtin@Double", |input: i32| async move {
    ///     Ok::<i32, WorkflowError>(input * 2)
    /// })?;
    ///
    /// let ctx = ExecutionContext { state: Arc::new(State::new()), platform: Arc::new(NullPlatform::new()) };
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
        self.register_node(wid, from_fn(name, move |input: I, _ctx: &ExecutionContext| f(input)))
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
    /// use autonomous::workflow::model::{State, ExecutionContext};
    /// use autonomous::workflow::platform::NullPlatform;
    /// use autonomous::workflow::error::WorkflowError;
    /// use std::sync::Arc;
    ///
    /// # #[tokio::main]
    /// # async fn example() -> Result<(), WorkflowError> {
    /// let mgr = WorkflowManager::new();
    ///
    /// mgr.add_with_ctx("builtin@Accumulate",
    ///     |input: i32, ctx: &ExecutionContext| {
    ///         let prev = ctx.state.get::<i32>("acc").unwrap_or(0);
    ///         ctx.state.set("acc", prev + input);
    ///         async move { Ok::<i32, WorkflowError>(prev + input) }
    ///     })?;
    ///
    /// let ctx = ExecutionContext { state: Arc::new(State::new()), platform: Arc::new(NullPlatform::new()) };
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
        F: Fn(I, &ExecutionContext) -> Fut + Send + Sync + 'static,
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
}

impl Default for WorkflowManager {
    fn default() -> Self {
        Self::new()
    }
}
