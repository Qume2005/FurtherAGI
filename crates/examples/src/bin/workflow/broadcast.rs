//! 广播（Broadcast）：一个值扇出到多条独立分支并行执行。
//!
//! 运行：`cargo run -p examples --bin workflow_broadcast`

use std::sync::Arc;

use autonomous::workflow::dag::DagBuilder;
use autonomous::workflow::error::WorkflowError;
use autonomous::workflow::executor::Executor;
use autonomous::workflow::model::ExecutionContext;
use autonomous::workflow::platform::NullPlatform;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    let mut builder = DagBuilder::new();
    let src = builder.add("builtin@AddOne", |input: i32| async move {
        println!("  AddOne({input})");
        Ok::<i32, WorkflowError>(input + 1)
    });
    let bc = builder.add_broadcast::<i32>();
    let left = builder.add("builtin@MulTwo", |input: i32| async move {
        println!("  MulTwo({input})");
        Ok::<i32, WorkflowError>(input * 2)
    });
    let right = builder.add("builtin@AddOne", |input: i32| async move {
        println!("  AddOne({input})");
        Ok::<i32, WorkflowError>(input + 1)
    });

    builder.connect(src, bc)?;
    builder.connect(bc, left)?;
    builder.connect(bc, right)?;
    builder.set_entry(src)?;
    builder.set_exit(left)?;

    let dag = builder.build()?;

    println!("Broadcast: AddOne(0)=1 → [MulTwo(1)=2, AddOne(1)=2]");
    let result = Executor::execute(&dag, Box::new(0i32), &ctx).await?;
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();

    println!("Result (left) = {output}");
    assert_eq!(*output, 2);
    println!("OK");
    Ok(())
}
