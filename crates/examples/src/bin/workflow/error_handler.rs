//! 错误处理（Error Handler）：节点失败时自动触发恢复工作流。
//!
//! 运行：`cargo run -p examples --bin workflow_error_handler`

use autonomous::workflow::dag::DagBuilder;
use autonomous::workflow::error::WorkflowError;
use autonomous::workflow::executor::Executor;
use autonomous::workflow::traits::from_fn;
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
    let fail = builder.add("builtin@FailIfNegative", |input: i32| async move {
        if input < 0 {
            println!("  FailIfNegative({input}): FAIL");
            return Err(WorkflowError::ValidationError("value is negative".into()));
        }
        println!("  FailIfNegative({input}): OK");
        Ok::<i32, WorkflowError>(input)
    });
    let _handler = builder.add_error_handler(
        fail,
        from_fn("error_handler", |error_msg: String, _ctx: &ExecutionContext<'_>| async move {
            println!("  ErrorHandler caught: \"{error_msg}\" → 0");
            Ok::<i32, WorkflowError>(0)
        }),
    )?;
    let downstream = builder.add("builtin@AddOne", |input: i32| async move {
        println!("  AddOne({input})");
        Ok::<i32, WorkflowError>(input + 1)
    });

    builder.connect(fail, downstream)?;
    builder.set_entry(fail)?;
    builder.set_exit(downstream)?;
    let dag = builder.build()?;

    println!("=== negative (-5) ===");
    let result = Executor::execute(&dag, Box::new(-5i32), &ctx).await?;
    let out: &i32 = result.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*out, 1);

    println!("\n=== positive (5) ===");
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await?;
    let out: &i32 = result.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*out, 6);

    println!("\nOK");
    Ok(())
}
