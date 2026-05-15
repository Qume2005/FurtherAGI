use super::*;
use crate::workflow::definition::{into_erased, Workflow};
use crate::workflow::error::WorkflowError;
use crate::workflow::model::ExecutionContext;
use crate::workflow::platform::NullPlatform;
use async_trait::async_trait;
use std::any::TypeId;
use std::sync::Arc;

struct AddOne;
#[async_trait]
impl Workflow<i32, i32> for AddOne {
    fn name(&self) -> &str { "add_one" }
    async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
        Ok(input + 1)
    }
}

fn make_ctx() -> ExecutionContext {
    ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    }
}

#[tokio::test]
async fn register_and_execute_node() {
    let mgr = WorkflowManager::new();
    mgr.register_node("add_one", into_erased(AddOne), TypeId::of::<i32>(), TypeId::of::<i32>())
        .unwrap();

    let ctx = make_ctx();
    let result: i32 = mgr
        .execute_typed("add_one", 5, &ctx)
        .await
        .unwrap();
    assert_eq!(result, 6);
}

#[tokio::test]
async fn unvalidated_workflow_rejected() {
    let mgr = WorkflowManager::new();

    // Register a node (auto-validated), then manually un-validate it.
    mgr.register_node("test", into_erased(AddOne), TypeId::of::<i32>(), TypeId::of::<i32>()).unwrap();

    // Manually set unvalidated.
    {
        let mut entry = mgr.workflows.get_mut(&WorkflowId::from("test")).unwrap();
        entry.validated = false;
    }

    let ctx = make_ctx();
    let result = mgr
        .execute("test", Box::new(1i32), &ctx)
        .await;
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("not been validated"));
}
