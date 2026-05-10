//! 循环（Loop）：将子图重复执行固定次数。
//!
//! 运行：`cargo run -p examples --bin workflow_loop`

use autonomous::workflow::dag::DagBuilder;
use autonomous::workflow::error::WorkflowError;
use autonomous::workflow::executor::Executor;
use autonomous::workflow::types::{ExecutionContext, State};
use autonomous::workflow::platform::NullPlatform;

#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    let state = State::new();
    let platform = NullPlatform::new();
    let ctx = ExecutionContext {
        state: &state,
        platform: &platform,
    };

    let mut builder = DagBuilder::new();
    let body = builder.add("builtin@AddOne", |input: i32| async move {
        println!("  iteration: {input} → {}", input + 1);
        Ok::<i32, WorkflowError>(input + 1)
    });
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
