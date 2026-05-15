use intelligent_subject::workflow::dag::PlanBuilder;
use intelligent_subject::workflow::config::ParamValue;
use intelligent_subject::workflow::definition::{into_erased, Workflow};
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::executor::Executor;
use intelligent_subject::workflow::model::ExecutionContext;
use intelligent_subject::workflow::platform::NullPlatform;
use async_trait::async_trait;
use std::sync::Arc;

fn make_ctx() -> ExecutionContext {
    ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    }
}

/// AppendX: String → String
struct AppendX;
#[async_trait]
impl Workflow<String, String> for AppendX {
    fn name(&self) -> &str { "append_x" }
    async fn execute(&self, input: String, _ctx: &ExecutionContext) -> Result<String, WorkflowError> {
        Ok(format!("{input}X"))
    }
}

/// Format: wraps input in "Report: {input}"
struct Format;
#[async_trait]
impl Workflow<String, String> for Format {
    fn name(&self) -> &str { "format" }
    async fn execute(&self, input: String, _ctx: &ExecutionContext) -> Result<String, WorkflowError> {
        Ok(format!("Report: {input}"))
    }
}

/// E2E: linear pipeline via PlanBuilder + Executor
#[tokio::test]
async fn e2e_linear_pipeline() {
    let mut builder = PlanBuilder::new();
    builder.add_workflow(
        "a",
        "append_x",
        vec![("input".to_string(), ParamValue::Literal("hello".to_string()))],
        into_erased(AppendX),
    );
    builder.add_workflow(
        "b",
        "append_x",
        vec![("input".to_string(), ParamValue::Reference {
            namespace: "a".to_string(),
            field: "value".to_string(),
        })],
        into_erased(AppendX),
    );
    builder.add_end("{b.value}");

    let plan = builder.build().unwrap();
    let ns = intelligent_subject::workflow::model::Namespace::new();
    let ctx = make_ctx();

    let result = Executor::execute(&plan, &ns, &ctx).await.unwrap();
    let val = result.downcast_ref::<String>().unwrap();
    assert_eq!(*val, "helloXX");
}

/// E2E: two independent nodes executed concurrently
#[tokio::test]
async fn e2e_independent_nodes() {
    let mut builder = PlanBuilder::new();
    builder.add_workflow(
        "x",
        "append_x",
        vec![("input".to_string(), ParamValue::Literal("foo".to_string()))],
        into_erased(AppendX),
    );
    builder.add_workflow(
        "y",
        "append_x",
        vec![("input".to_string(), ParamValue::Literal("bar".to_string()))],
        into_erased(AppendX),
    );
    builder.add_end("{x.value}");

    let plan = builder.build().unwrap();
    let ns = intelligent_subject::workflow::model::Namespace::new();
    let ctx = make_ctx();

    let result = Executor::execute(&plan, &ns, &ctx).await.unwrap();
    let val = result.downcast_ref::<String>().unwrap();
    assert_eq!(*val, "fooX");
}

/// E2E: workflow via WorkflowManager
#[tokio::test]
async fn e2e_manager_leaf_workflow() {
    let mgr = intelligent_subject::workflow::workflow_manager::WorkflowManager::new();

    mgr.add("append_x", |input: String| async move {
        Ok::<String, WorkflowError>(format!("{input}X"))
    }).unwrap();

    let ctx = make_ctx();
    let result: String = mgr.execute_typed("append_x", "hello".to_string(), &ctx).await.unwrap();
    assert_eq!(result, "helloX");
}

/// E2E: ns composite via WorkflowManager
#[tokio::test]
async fn e2e_manager_ns_composite() {
    let mut builder = PlanBuilder::new();
    builder.add_workflow(
        "step",
        "format",
        vec![("input".to_string(), ParamValue::Literal("world".to_string()))],
        into_erased(Format),
    );
    builder.add_end("{step.value}");

    let plan = builder.build().unwrap();

    let mgr = intelligent_subject::workflow::workflow_manager::WorkflowManager::new();
    mgr.register_ns_composite("report", plan).unwrap();

    let ctx = make_ctx();
    let result = mgr.execute_ns("report", &ctx).await.unwrap();
    let val = result.downcast_ref::<String>().unwrap();
    assert_eq!(*val, "Report: world");
}
