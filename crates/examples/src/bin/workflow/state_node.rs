//! 状态共享（StateStore）：通过 Arc<StateStore> 在多个工作流之间共享状态。
//!
//! 演示如何：
//! - 创建 `Arc<StateStore>` 并在闭包之间捕获共享
//! - 两次执行同一个工作流，展示状态跨运行持久化
//! - 使用 TTL 让条目在指定时间后过期
//!
//! 运行：`cargo run -p examples --bin workflow_state_node`

use std::sync::Arc;
use std::time::Duration;

use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::model::{ExecutionContext, StateStore};
use intelligent_subject::workflow::platform::NullPlatform;
use intelligent_subject::workflow::workflow_manager::WorkflowManager;

#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    let store = Arc::new(StateStore::new());
    let store_clone = store.clone();

    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    let mgr = WorkflowManager::new();
    mgr.add("accumulate", move |input: i32| {
        let s = store_clone.clone();
        async move {
            let prev = s.get::<i32>("sum").unwrap_or(0);
            let new_val = prev + input;
            s.set("sum", new_val, None);
            println!("  accumulate: {prev} + {input} = {new_val}");
            Ok::<i32, WorkflowError>(new_val)
        }
    })?;

    // 第一次执行：初始 sum=0，累加 10 → 10
    println!("=== 第一次执行 (input=10) ===");
    let out1: i32 = mgr.execute_typed("accumulate", 10, &ctx).await?;
    assert_eq!(out1, 10);
    assert_eq!(store.get::<i32>("sum"), Some(10));
    println!("  输出 = {out1}, store[\"sum\"] = {:?}", store.get::<i32>("sum"));

    // 第二次执行：sum 已为 10，累加 20 → 30（状态跨执行持久化）
    println!("\n=== 第二次执行 (input=20) ===");
    let out2: i32 = mgr.execute_typed("accumulate", 20, &ctx).await?;
    assert_eq!(out2, 30);
    assert_eq!(store.get::<i32>("sum"), Some(30));
    println!("  输出 = {out2}, store[\"sum\"] = {:?}", store.get::<i32>("sum"));

    // TTL 演示
    println!("\n=== TTL 过期演示 ===");
    store.set("ephemeral", 42i32, Some(Duration::from_millis(100)));
    println!("  写入 ephemeral=42 (TTL 100ms)");
    println!("  立即读取: {:?}", store.get::<i32>("ephemeral"));

    tokio::time::sleep(Duration::from_millis(150)).await;
    println!("  150ms 后读取: {:?}", store.get::<i32>("ephemeral"));

    println!("\nOK");
    Ok(())
}
