use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use tracing::instrument;

use crate::workflow::error::WorkflowError;
use crate::workflow::executor::ExecutionResult;
use crate::workflow::model::{ExecutionContext, WorkflowId};

use super::WorkflowManager;

impl WorkflowManager {
    /// 以类型擦除输入执行已注册的工作流。
    pub async fn execute(
        &self,
        id: impl Into<WorkflowId>,
        input: Box<dyn Any + Send + Sync>,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, WorkflowError> {
        let id = id.into();
        let entry = self.workflows.get(&id).ok_or_else(|| {
            WorkflowError::WorkflowNotFound(id.clone())
        })?;
        let output_type = entry.output_type;
        drop(entry);
        self.execute_inner(&id, input, ctx, output_type).await
    }

    #[instrument(skip(self, input, ctx))]
    async fn execute_inner(
        &self,
        id: &WorkflowId,
        input: Box<dyn Any + Send + Sync>,
        ctx: &ExecutionContext,
        output_type: TypeId,
    ) -> Result<ExecutionResult, WorkflowError> {
        let entry = self.workflows.get(id).ok_or_else(|| {
            WorkflowError::WorkflowNotFound(id.clone())
        })?;

        if !entry.validated {
            return Err(WorkflowError::ValidationError(format!(
                "workflow '{id}' has not been validated — call validate_all() first"
            )));
        }

        if let Some(ref wf) = entry.workflow {
            // 叶工作流：将输入包装到 HashMap 参数并执行。
            let mut params: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
            params.insert("input".to_string(), Arc::from(input));
            let ns_output = wf.execute_erased(params, ctx).await?;
            let value = ns_output.fields.into_iter()
                .find(|(k, _)| k == "value")
                .map(|(_, v)| v)
                .ok_or_else(|| WorkflowError::ValidationError("no 'value' field in workflow output".into()))?;
            Ok(ExecutionResult { output: value, output_type })
        } else {
            Err(WorkflowError::execution(
                crate::workflow::model::NodeId(0),
                format!("workflow '{id}' has no implementation"),
            ))
        }
    }

    /// 以强类型输入/输出执行工作流。
    /// 这是主要面向用户的 API。
    pub async fn execute_typed<I: Send + Sync + 'static, O: Send + Sync + 'static>(
        &self,
        id: impl Into<WorkflowId>,
        input: I,
        ctx: &ExecutionContext,
    ) -> Result<O, WorkflowError> {
        let id = id.into();
        let entry = self.workflows.get(&id).ok_or_else(|| {
            WorkflowError::WorkflowNotFound(id.clone())
        })?;

        // 在边界进行类型检查。
        if entry.input_type != TypeId::of::<I>() {
            return Err(WorkflowError::ValidationError(format!(
                "input type mismatch for '{}': expected {:?}, got {:?}",
                id,
                entry.input_type,
                TypeId::of::<I>()
            )));
        }
        let output_type = entry.output_type;
        if output_type != TypeId::of::<O>() {
            return Err(WorkflowError::ValidationError(format!(
                "output type mismatch for '{}': expected {:?}, got {:?}",
                id,
                output_type,
                TypeId::of::<O>()
            )));
        }

        drop(entry); // 释放 DashMap 引用。

        let result = self.execute_inner(&id, Box::new(input), ctx, output_type).await?;
        result
            .output
            .downcast::<O>()
            .map(|b| *b)
            .map_err(|_| WorkflowError::ValidationError("output downcast failed".into()))
    }

    /// 检查工作流是否已注册。
    pub fn contains(&self, id: impl Into<WorkflowId>) -> bool {
        self.workflows.contains_key(&id.into())
    }

    /// 获取已注册的工作流数量。
    pub fn len(&self) -> usize {
        self.workflows.len()
    }

    /// 检查注册表是否为空。
    pub fn is_empty(&self) -> bool {
        self.workflows.is_empty()
    }
}
