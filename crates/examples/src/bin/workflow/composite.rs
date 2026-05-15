//! 命名空间复合工作流：用 PlanBuilder 构建多步管道并执行。演示如何用闭包创建工作流节点。
//!
//! 运行：`cargo run -p examples --bin workflow_composite`

use std::sync::Arc;

use intelligent_subject::workflow::dag::PlanBuilder;
use intelligent_subject::workflow::config::ParamValue;
use intelligent_subject::workflow::definition::from_fn;
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::executor::Executor;
use intelligent_subject::workflow::model::{ExecutionContext, Namespace};
use intelligent_subject::workflow::platform::NullPlatform;

#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    // 构建管道: append_x("hello") → append_x("helloX") → format("Report: helloXX")
    let mut builder = PlanBuilder::new();
    builder.add_workflow(
        "a",
        "append_x",
        vec![("input".to_string(), ParamValue::Literal("hello".to_string()))],
        from_fn("append_x", |input: String, _ctx: &ExecutionContext| async move {
            Ok::<String, WorkflowError>(format!("{input}X"))
        }),
    );
    builder.add_workflow(
        "b",
        "append_x",
        vec![("input".to_string(), ParamValue::Reference {
            namespace: "a".to_string(),
            field: "value".to_string(),
        })],
        from_fn("append_x", |input: String, _ctx: &ExecutionContext| async move {
            Ok::<String, WorkflowError>(format!("{input}X"))
        }),
    );
    builder.add_workflow(
        "report",
        "format",
        vec![("input".to_string(), ParamValue::Reference {
            namespace: "b".to_string(),
            field: "value".to_string(),
        })],
        from_fn("format", |input: String, _ctx: &ExecutionContext| async move {
            Ok::<String, WorkflowError>(format!("Report: {input}"))
        }),
    );
    builder.add_end("{report.value}");

    let plan = builder.build()?;
    let ns = Namespace::new();

    let result = Executor::execute(&plan, &ns, &ctx).await?;
    let val = result.downcast_ref::<String>().unwrap();

    println!("Pipeline: hello -> {val}");
    assert_eq!(val, "Report: helloXX");
    println!("OK");
    Ok(())
}
