//! 组合工作流：用 DAG 串联多个闭包工作流。
//!
//! 运行：`cargo run -p examples --bin workflow_composite`

use std::sync::Arc;

use autonomous::workflow::dag::DagBuilder;
use autonomous::workflow::error::WorkflowError;
use autonomous::workflow::model::ExecutionContext;
use autonomous::workflow::workflow_manager::WorkflowManager;
use autonomous::workflow::platform::NullPlatform;

#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    let mgr = WorkflowManager::new();
    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    let mut builder = DagBuilder::new();
    // 构建管道: AddOne(3) → MulTwo(4) → AddOne(9)
    let a = builder.add("builtin@AddOne", |input: i32| async move {
        Ok::<i32, WorkflowError>(input + 1)
    });
    let b = builder.add("builtin@MulTwo", |input: i32| async move {
        Ok::<i32, WorkflowError>(input * 2)
    });
    let c = builder.add("builtin@AddOne", |input: i32| async move {
        Ok::<i32, WorkflowError>(input + 1)
    });
    builder.connect(a, b)?;
    builder.connect(b, c)?;
    builder.set_entry(a)?;
    builder.set_exit(c)?;
    let dag = builder.build()?;

    mgr.register_composite("pipeline@Main", dag)?;
    mgr.validate_all()?;

    let result: i32 = mgr
        .execute_typed("pipeline@Main", 3, &ctx)
        .await?;

    println!("Pipeline(3) = {result}");
    assert_eq!(result, 9);
    println!("OK");
    Ok(())
}
