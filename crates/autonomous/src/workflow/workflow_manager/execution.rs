use std::any::{Any, TypeId};

use tracing::instrument;

use crate::workflow::error::WorkflowError;
use crate::workflow::executor::{ExecutionResult, Executor};
use crate::workflow::model::{ExecutionContext, NodeId, WorkflowId};

use super::WorkflowManager;

impl WorkflowManager {
    /// Execute a registered workflow by ID with type-erased input.
    pub async fn execute(
        &self,
        id: impl Into<WorkflowId>,
        input: Box<dyn Any + Send + Sync>,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, WorkflowError> {
        let id = id.into();
        self.execute_inner(&id, input, ctx).await
    }

    #[instrument(skip(self, input, ctx))]
    async fn execute_inner(
        &self,
        id: &WorkflowId,
        input: Box<dyn Any + Send + Sync>,
        ctx: &ExecutionContext,
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
        ctx: &ExecutionContext,
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
