//! Integration tests for config-driven workflow building.

use async_trait::async_trait;
use intelligent_subject::workflow::config::{ConfigBuilder, TypeRegistry, WorkflowFactoryRegistry};
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::executor::Executor;
use intelligent_subject::workflow::definition::{into_erased, Workflow};
use intelligent_subject::workflow::model::ExecutionContext;
use intelligent_subject::workflow::workflow_manager::WorkflowManager;
use intelligent_subject::workflow::platform::NullPlatform;
use std::sync::Arc;

// ── Test workflows ──────────────────────────────────────────

struct AddOne;
#[async_trait]
impl Workflow<i32, i32> for AddOne {
    fn name(&self) -> &str {
        "add_one"
    }
    async fn execute(
        &self,
        input: i32,
        _ctx: &ExecutionContext,
    ) -> Result<i32, WorkflowError> {
        Ok(input + 1)
    }
}

struct MulTwo;
#[async_trait]
impl Workflow<i32, i32> for MulTwo {
    fn name(&self) -> &str {
        "mul_two"
    }
    async fn execute(
        &self,
        input: i32,
        _ctx: &ExecutionContext,
    ) -> Result<i32, WorkflowError> {
        Ok(input * 2)
    }
}

struct IsPositive;
#[async_trait]
impl Workflow<i32, bool> for IsPositive {
    fn name(&self) -> &str {
        "is_positive"
    }
    async fn execute(
        &self,
        input: i32,
        _ctx: &ExecutionContext,
    ) -> Result<bool, WorkflowError> {
        Ok(input > 0)
    }
}

struct BoolToInt;
#[async_trait]
impl Workflow<bool, i32> for BoolToInt {
    fn name(&self) -> &str {
        "bool_to_int"
    }
    async fn execute(
        &self,
        input: bool,
        _ctx: &ExecutionContext,
    ) -> Result<i32, WorkflowError> {
        Ok(if input { 100 } else { -100 })
    }
}

struct FailIfNegative;
#[async_trait]
impl Workflow<i32, i32> for FailIfNegative {
    fn name(&self) -> &str {
        "fail_if_negative"
    }
    async fn execute(
        &self,
        input: i32,
        _ctx: &ExecutionContext,
    ) -> Result<i32, WorkflowError> {
        if input < 0 {
            Err(WorkflowError::ValidationError("negative".into()))
        } else {
            Ok(input)
        }
    }
}

struct RecoverDefault;
#[async_trait]
impl Workflow<String, i32> for RecoverDefault {
    fn name(&self) -> &str {
        "recover"
    }
    async fn execute(
        &self,
        _input: String,
        _ctx: &ExecutionContext,
    ) -> Result<i32, WorkflowError> {
        Ok(0)
    }
}

// ── Shared context ──────────────────────────────────────────

fn make_ctx() -> ExecutionContext {
    ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    }
}

// ── Helpers ─────────────────────────────────────────────────

fn make_builder() -> ConfigBuilder {
    let types = TypeRegistry::with_primitives();
    let mut workflows = WorkflowFactoryRegistry::new();
    workflows.register("add_one", || into_erased(AddOne));
    workflows.register("mul_two", || into_erased(MulTwo));
    workflows.register("is_positive", || into_erased(IsPositive));
    workflows.register("bool_to_int", || into_erased(BoolToInt));
    workflows.register("fail_if_negative", || into_erased(FailIfNegative));
    workflows.register("recover", || into_erased(RecoverDefault));
    ConfigBuilder::new(types, workflows)
}

// ── Tests ───────────────────────────────────────────────────

#[test]
fn config_linear_pipeline() {
    let builder = make_builder();
    let (id, dag) = builder
        .build_from_str(
            r#"
            [workflow]
            name = "pipeline"
            entry = "a"
            exit = "c"

            [nodes.a]
            kind = "workflow"
            implementation = "add_one"

            [nodes.b]
            kind = "workflow"
            implementation = "mul_two"

            [nodes.c]
            kind = "workflow"
            implementation = "add_one"

            [[edges]]
            from = "a"
            to = "b"

            [[edges]]
            from = "b"
            to = "c"
        "#,
        )
        .unwrap();

    assert_eq!(id.as_str(), "pipeline");
    assert_eq!(dag.topo_order().len(), 3);
}

#[tokio::test]
async fn config_linear_pipeline_execute() {
    let builder = make_builder();
    let (workflow_id, dag) = builder
        .build_from_str(
            r#"
            [workflow]
            name = "pipeline"
            entry = "a"
            exit = "c"

            [nodes.a]
            kind = "workflow"
            implementation = "add_one"

            [nodes.b]
            kind = "workflow"
            implementation = "mul_two"

            [nodes.c]
            kind = "workflow"
            implementation = "add_one"

            [[edges]]
            from = "a"
            to = "b"

            [[edges]]
            from = "b"
            to = "c"
        "#,
        )
        .unwrap();

    let mgr = WorkflowManager::new();
    mgr.register_composite(workflow_id, dag).unwrap();
    mgr.validate_all().unwrap();

    let ctx = make_ctx();
    let result: i32 = mgr
        .execute_typed("pipeline", 3, &ctx)
        .await
        .unwrap();
    // AddOne(3)=4, MulTwo(4)=8, AddOne(8)=9
    assert_eq!(result, 9);
}

#[tokio::test]
async fn config_broadcast_pipeline() {
    let builder = make_builder();
    let (_workflow_id, dag) = builder
        .build_from_str(
            r#"
            [workflow]
            name = "bc_test"
            entry = "src"
            exit = "left"

            [nodes.src]
            kind = "workflow"
            implementation = "add_one"

            [nodes.bc]
            kind = "broadcast"
            type = "i32"

            [nodes.left]
            kind = "workflow"
            implementation = "mul_two"

            [nodes.right]
            kind = "workflow"
            implementation = "add_one"

            [[edges]]
            from = "src"
            to = "bc"

            [[edges]]
            from = "bc"
            to = "left"

            [[edges]]
            from = "bc"
            to = "right"
        "#,
        )
        .unwrap();

    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(0i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    // AddOne(0)=1, Broadcast, MulTwo(1)=2
    assert_eq!(*output, 2);
}

#[tokio::test]
async fn config_loop_pipeline() {
    let builder = make_builder();
    let (_workflow_id, dag) = builder
        .build_from_str(
            r#"
            [workflow]
            name = "loop_test"
            entry = "loop1"
            exit = "loop1"

            [nodes.body]
            kind = "workflow"
            implementation = "add_one"

            [nodes.loop1]
            kind = "loop"
            count = 5
            body_entry = "body"
            body_exit = "body"
        "#,
        )
        .unwrap();

    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(0i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    // AddOne × 5 starting from 0 = 5
    assert_eq!(*output, 5);
}

#[tokio::test]
async fn config_conditional_pipeline() {
    let builder = make_builder();
    let (_, dag) = builder
        .build_from_str(
            r#"
            [workflow]
            name = "cond_test"
            entry = "cond"
            exit = "pos"

            [nodes.cond]
            kind = "conditional"
            implementation = "is_positive"

            [nodes.pos]
            kind = "workflow"
            implementation = "bool_to_int"

            [nodes.neg]
            kind = "workflow"
            implementation = "bool_to_int"

            [[edges]]
            from = "cond"
            to = "pos"
            label = "true"

            [[edges]]
            from = "cond"
            to = "neg"
            label = "false"
        "#,
        )
        .unwrap();

    let ctx = make_ctx();

    // Positive input
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*output, 100);
}

#[tokio::test]
async fn config_error_handler_pipeline() {
    let builder = make_builder();
    let (_, dag) = builder
        .build_from_str(
            r#"
            [workflow]
            name = "err_test"
            entry = "fail"
            exit = "fail"

            [nodes.fail]
            kind = "workflow"
            implementation = "fail_if_negative"

            [nodes.handler]
            kind = "error_handler"
            paired_with = "fail"
            implementation = "recover"
        "#,
        )
        .unwrap();

    let ctx = make_ctx();

    // Negative → fail → recover to 0
    let result = Executor::execute(&dag, Box::new(-5i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*output, 0);

    // Positive → passes through
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*output, 5);
}

#[test]
fn config_unknown_type_error() {
    let builder = make_builder();
    let result = builder.build_from_str(
        r#"
            [workflow]
            name = "bad"
            entry = "bc"
            exit = "bc"

            [nodes.bc]
            kind = "broadcast"
            type = "NonExistentType"
        "#,
    );
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("unknown type"));
    assert!(msg.contains("NonExistentType"));
}

#[test]
fn config_unknown_workflow_error() {
    let builder = make_builder();
    let result = builder.build_from_str(
        r#"
            [workflow]
            name = "bad"
            entry = "a"
            exit = "a"

            [nodes.a]
            kind = "workflow"
            implementation = "nonexistent"
        "#,
    );
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("unknown workflow"));
    assert!(msg.contains("nonexistent"));
}

#[test]
fn config_unknown_node_reference_error() {
    let builder = make_builder();
    let result = builder.build_from_str(
        r#"
            [workflow]
            name = "bad"
            entry = "a"
            exit = "a"

            [nodes.a]
            kind = "workflow"
            implementation = "add_one"

            [[edges]]
            from = "a"
            to = "nonexistent"
        "#,
    );
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("unknown node"));
    assert!(msg.contains("nonexistent"));
}

#[test]
fn config_sub_workflow_node() {
    let builder = make_builder();
    let (id, dag) = builder
        .build_from_str(
            r#"
            [workflow]
            name = "with_sub"
            entry = "sub"
            exit = "sub"

            [nodes.sub]
            kind = "sub_workflow"
            workflow = "helper"
            input_type = "i32"
            output_type = "i32"
        "#,
        )
        .unwrap();

    assert_eq!(id.as_str(), "with_sub");
    assert_eq!(dag.topo_order().len(), 1);
}

#[tokio::test]
async fn config_connection_passthrough() {
    let builder = make_builder();
    let (workflow_id, dag) = builder
        .build_from_str(
            r#"
            [workflow]
            name = "conn_test"
            entry = "a"
            exit = "c"

            [nodes.a]
            kind = "workflow"
            implementation = "add_one"

            [nodes.jump1]
            kind = "connection"
            label = "after_add"
            type = "i32"

            [nodes.b]
            kind = "workflow"
            implementation = "mul_two"

            [nodes.jump2]
            kind = "connection"
            label = "after_mul"
            type = "i32"

            [nodes.c]
            kind = "workflow"
            implementation = "add_one"

            [[edges]]
            from = "a"
            to = "jump1"

            [[edges]]
            from = "jump1"
            to = "b"

            [[edges]]
            from = "b"
            to = "jump2"

            [[edges]]
            from = "jump2"
            to = "c"
        "#,
        )
        .unwrap();

    assert_eq!(dag.topo_order().len(), 5);

    let mgr = WorkflowManager::new();
    mgr.register_composite(workflow_id, dag).unwrap();
    mgr.validate_all().unwrap();

    let ctx = make_ctx();
    let result: i32 = mgr
        .execute_typed("conn_test", 3, &ctx)
        .await
        .unwrap();
    // AddOne(3)=4 → Connection → MulTwo(4)=8 → Connection → AddOne(8)=9
    assert_eq!(result, 9);
}
