//! 有状态工作流：在 State 中读写数据，跨多次执行共享状态。
//!
//! 运行：`cargo run -p examples --bin workflow_stateful`

use std::sync::Arc;

use autonomous::workflow::error::WorkflowError;
use autonomous::workflow::model::{ExecutionContext, State};
use autonomous::workflow::workflow_manager::WorkflowManager;
use autonomous::workflow::platform::NullPlatform;

#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    let state = Arc::new(State::new());
    let ctx = ExecutionContext {
        state: state.clone(),
        platform: Arc::new(NullPlatform::new()),
    };

    let mgr = WorkflowManager::new();
    mgr.add_with_ctx("builtin@Accumulate",
        |input: i32, ctx: &ExecutionContext| {
            let prev = ctx.state.get::<i32>("accumulator").unwrap_or(0);
            let new_val = prev + input;
            ctx.state.set("accumulator", new_val);
            println!("  accumulator: {prev} + {input} = {new_val}");
            async move { Ok::<i32, WorkflowError>(new_val) }
        }
    )?;

    println!("Run 1: input = 10");
    let r1: i32 = mgr.execute_typed("builtin@Accumulate", 10, &ctx).await?;
    assert_eq!(r1, 10);

    println!("\nRun 2: input = 20");
    let r2: i32 = mgr.execute_typed("builtin@Accumulate", 20, &ctx).await?;
    assert_eq!(r2, 30);

    println!("\nRun 3: input = 5");
    let r3: i32 = mgr.execute_typed("builtin@Accumulate", 5, &ctx).await?;
    assert_eq!(r3, 35);

    assert_eq!(state.get::<i32>("accumulator"), Some(35));
    println!("\nFinal state: accumulator = 35");
    println!("OK");
    Ok(())
}
