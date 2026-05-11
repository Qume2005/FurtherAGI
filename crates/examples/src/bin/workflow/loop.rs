//! 循环（Loop）：将子图重复执行固定次数。
//!
//! 运行：`cargo run -p examples --bin workflow_loop`

use std::sync::Arc;

use intelligent_subject::workflow::dag::DagBuilder;
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::executor::Executor;
use intelligent_subject::workflow::model::ExecutionContext;
use intelligent_subject::workflow::platform::NullPlatform;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    let mut builder = DagBuilder::new();
    let body = builder.add("builtin@AddOne", |input: i32| async move {
        println!("  iteration: {input} → {}", input + 1);
        Ok::<i32, WorkflowError>(input + 1)
    });
    // body 同时作为循环体子图的入口和出口，每次迭代将输出传回自身
    let loop_node = builder.add_loop(5, body, body)?;
    builder.set_entry(loop_node)?;
    builder.set_exit(loop_node)?;
    let dag = builder.build()?;

    println!("Loop: AddOne × 5, starting from 0");
    let result = Executor::execute(&dag, Box::new(0i32), &ctx).await?;
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();

    println!("Result = {output}");
    assert_eq!(*output, 5);
    println!("OK");
    Ok(())
}
