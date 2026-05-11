//! 最简单的工作流：从闭包注册 → 执行。
//!
//! 运行：`cargo run -p examples --bin workflow_basic`

use std::sync::Arc;

use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::model::ExecutionContext;
use intelligent_subject::workflow::workflow_manager::WorkflowManager;
use intelligent_subject::workflow::platform::NullPlatform;

#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    let mgr = WorkflowManager::new();
    // NullPlatform 不执行任何实际操作，适用于纯逻辑工作流（不需要文件系统/容器等平台能力）
    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    // turbofish Ok::<i32, WorkflowError> 帮助编译器推断闭包返回类型
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
