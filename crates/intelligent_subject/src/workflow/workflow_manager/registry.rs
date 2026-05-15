use std::any::TypeId;
use std::future::Future;
use tracing::instrument;

use crate::workflow::dag::ExecutionPlan;
use crate::workflow::definition::{from_fn, ErasedWorkflow};
use crate::workflow::error::WorkflowError;
use crate::workflow::model::{ExecutionContext, Namespace, WorkflowId};

use super::RegisteredWorkflow;

/// 中央工作流注册表。
///
/// 线程安全的注册器，所有工作流在依赖校验后注册。
/// 支持叶工作流（builtin）和命名空间复合工作流。
pub struct WorkflowManager {
    pub(super) workflows: dashmap::DashMap<WorkflowId, RegisteredWorkflow>,
}

impl WorkflowManager {
    pub fn new() -> Self {
        Self {
            workflows: dashmap::DashMap::new(),
        }
    }

    /// 注册叶工作流（内部使用）。
    /// 立即标记为已校验 — 叶工作流无复合依赖。
    pub(crate) fn register_node(
        &self,
        id: impl Into<WorkflowId>,
        workflow: Box<dyn ErasedWorkflow>,
        input_type: TypeId,
        output_type: TypeId,
    ) -> Result<(), WorkflowError> {
        let id = id.into();
        let entry = RegisteredWorkflow {
            id: id.clone(),
            workflow: Some(workflow),
            ns_plan: None,
            validated: true,
            input_type,
            output_type,
        };
        self.workflows.insert(id, entry);
        Ok(())
    }

    /// 添加工作流（纯闭包，不需要 `ExecutionContext`）。
    ///
    /// 最常用的注册方式。闭包只接收输入，返回输出。
    /// 如果需要访问 `ctx`，使用 [`add_with_ctx`](Self::add_with_ctx)。
    ///
    /// ID 使用 `"namespace@Name"` 格式，工作流名称自动取 name 部分。
    ///
    /// # 示例
    ///
    /// ```rust
    /// use intelligent_subject::workflow::workflow_manager::WorkflowManager;
    /// use intelligent_subject::workflow::model::ExecutionContext;
    /// use intelligent_subject::workflow::platform::NullPlatform;
    /// use intelligent_subject::workflow::error::WorkflowError;
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
    /// let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };
    ///
    /// let result: i32 = mgr.execute_typed("builtin@Double", 21, &ctx).await?;
    /// assert_eq!(result, 42);
    /// # Ok(())
    /// # }
    /// ```
    pub fn add<I, O, F, Fut>(&self, id: &str, f: F) -> Result<(), WorkflowError>
    where
        I: Clone + Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, WorkflowError>> + Send,
    {
        let wid = WorkflowId::from(id);
        let name = wid.name().to_string();
        self.register_node(
            wid,
            from_fn(name, move |input: I, _ctx: &ExecutionContext| f(input)),
            TypeId::of::<I>(),
            TypeId::of::<O>(),
        )
    }

    /// 添加工作流（需要 `ExecutionContext` 的闭包）。
    ///
    /// 用于需要访问 `ctx.platform` 的工作流。
    /// 简单场景优先使用 [`add`](Self::add)。
    ///
    /// # 示例
    ///
    /// ```rust
    /// use intelligent_subject::workflow::workflow_manager::WorkflowManager;
    /// use intelligent_subject::workflow::model::ExecutionContext;
    /// use intelligent_subject::workflow::platform::NullPlatform;
    /// use intelligent_subject::workflow::error::WorkflowError;
    /// use std::sync::Arc;
    ///
    /// # #[tokio::main]
    /// # async fn example() -> Result<(), WorkflowError> {
    /// let mgr = WorkflowManager::new();
    ///
    /// mgr.add_with_ctx("builtin@CtxDouble",
    ///     |input: i32, _ctx: &ExecutionContext| {
    ///         async move { Ok::<i32, WorkflowError>(input * 2) }
    ///     })?;
    ///
    /// let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };
    ///
    /// let r: i32 = mgr.execute_typed("builtin@CtxDouble", 10, &ctx).await?;
    /// assert_eq!(r, 20);
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_with_ctx<I, O, F, Fut>(&self, id: &str, f: F) -> Result<(), WorkflowError>
    where
        I: Clone + Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(I, &ExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, WorkflowError>> + Send,
    {
        let wid = WorkflowId::from(id);
        let name = wid.name().to_string();
        self.register_node(
            wid,
            from_fn(name, f),
            TypeId::of::<I>(),
            TypeId::of::<O>(),
        )
    }

    /// 添加预构建的类型擦除工作流。
    ///
    /// 用于 `into_erased()` 或 `from_fn()` 的结果。
    pub fn add_erased(
        &self,
        id: &str,
        workflow: Box<dyn ErasedWorkflow>,
        input_type: TypeId,
        output_type: TypeId,
    ) -> Result<(), WorkflowError> {
        self.register_node(WorkflowId::from(id), workflow, input_type, output_type)
    }

    /// 注册一个命名空间复合工作流（基于 `ExecutionPlan`）。
    #[instrument(skip(self, plan))]
    pub fn register_ns_composite(
        &self,
        id: impl Into<WorkflowId> + std::fmt::Debug,
        plan: ExecutionPlan,
    ) -> Result<(), WorkflowError> {
        let id = id.into();
        // 命名空间工作流没有单一的输入/输出 TypeId。
        // 使用 () 占位，因为 execute_ns 绕过类型检查。
        let entry = RegisteredWorkflow {
            id: id.clone(),
            workflow: None,
            ns_plan: Some(plan),
            validated: true,
            input_type: std::any::TypeId::of::<()>(),
            output_type: std::any::TypeId::of::<()>(),
        };
        self.workflows.insert(id.clone(), entry);
        Ok(())
    }

    /// 执行命名空间复合工作流。
    ///
    /// 返回 `Arc<dyn Any + Send + Sync>`，由调用方 downcast 为具体类型。
    pub async fn execute_ns(
        &self,
        id: &str,
        ctx: &ExecutionContext,
    ) -> Result<std::sync::Arc<dyn std::any::Any + Send + Sync>, WorkflowError> {
        let wid = WorkflowId::from(id);
        let entry = self.workflows.get(&wid).ok_or_else(|| {
            WorkflowError::ValidationError(format!("workflow '{id}' not found"))
        })?;

        let plan = entry.ns_plan.as_ref().ok_or_else(|| {
            WorkflowError::ValidationError(format!("workflow '{id}' is not a namespace composite"))
        })?;

        let namespace = Namespace::new();
        crate::workflow::executor::Executor::execute(plan, &namespace, ctx).await
    }
}

impl Default for WorkflowManager {
    fn default() -> Self {
        Self::new()
    }
}
