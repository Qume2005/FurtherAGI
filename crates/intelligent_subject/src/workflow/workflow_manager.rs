//! # Workflow Manager — 中央注册器
//!
//! 管理所有工作流的生命周期：注册、依赖校验和执行。

mod execution;
mod registry;
mod validation;

#[cfg(test)]
mod tests;

use std::any::TypeId;

use crate::workflow::dag::WorkflowDag;
use crate::workflow::definition::ErasedWorkflow;
#[cfg(test)]
use crate::workflow::error::WorkflowError;
#[cfg(test)]
use crate::workflow::model::ExecutionContext;
use crate::workflow::model::WorkflowId;

pub use registry::WorkflowManager;

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
