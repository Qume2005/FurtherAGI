use super::*;
use crate::workflow::dag::DagBuilder;
use crate::workflow::definition::{into_erased, Workflow};
use crate::workflow::model::State;
use crate::workflow::platform::NullPlatform;
use async_trait::async_trait;
use std::sync::Arc;

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

fn make_ctx() -> ExecutionContext {
    ExecutionContext {
        state: Arc::new(State::new()),
        platform: Arc::new(NullPlatform::new()),
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
