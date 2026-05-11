//! 最简单的工作流：从闭包注册 → 执行。
//!
//! 运行：`cargo run -p examples --bin workflow_basic`

use std::sync::Arc;

use autonomous::workflow::error::WorkflowError;
use autonomous::workflow::types::{ExecutionContext, State};
use autonomous::workflow::workflow_manager::WorkflowManager;
use autonomous::workflow::platform::NullPlatform;

#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    let mgr = WorkflowManager::new();
    let ctx = ExecutionContext {
        state: Arc::new(State::new()),
        platform: Arc::new(NullPlatform::new()),
    };

    mgr.add("builtin@Double", |input: i32| async move {
        Ok::<i32, WorkflowError>(input * 2)
    })?;

    let result: i32 = mgr
        .execute_typed("builtin@Double", 21, &ctx)
        .await?;

    println!("Double(21) = {result}");
    assert_eq!(result, 42);
    println!("OK");
    Ok(())
}
