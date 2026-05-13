//! 状态共享（StateStore）：通过 Arc<StateStore> 在多个工作流之间共享状态。
//!
//! 演示如何：
//! - 创建 `Arc<StateStore>` 并在闭包之间捕获共享
//! - 使用 `StateCarrier` 服务 + `from_fn` 构建透传节点（持有 store 引用）
//! - 两次执行同一个 DAG，展示状态跨运行持久化
//! - 使用 TTL 让条目在指定时间后过期
//!
//! 运行：`cargo run -p examples --bin workflow_state_node`

use std::sync::Arc;
use std::time::Duration;

use intelligent_subject::workflow::dag::DagBuilder;
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::executor::Executor;
use intelligent_subject::workflow::model::{ExecutionContext, StateStore};
use intelligent_subject::workflow::platform::NullPlatform;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = Arc::new(StateStore::new());
    let store_for_closure = store.clone();
    let store_clone = store.clone();

    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    let mut builder = DagBuilder::new();
    // from_fn 构建透传节点：输入直接输出，同时通过闭包捕获持有 Arc<StateStore>
    let s = builder.add_with_ctx("state", move |input: i32, _ctx: &ExecutionContext| {
        let _store = store_for_closure.clone();
        async move {
            // store 通过闭包捕获保持存活，其他节点的闭包可以单独捕获 Arc<StateStore>
            Ok::<i32, WorkflowError>(input)
        }
    });
    let step = builder.add("accumulate", move |input: i32| {
        let s = store_clone.clone();
        async move {
            let prev = s.get::<i32>("sum").unwrap_or(0);
            let new_val = prev + input;
            s.set("sum", new_val, None);
            println!("  accumulate: {prev} + {input} = {new_val}");
            Ok::<i32, WorkflowError>(new_val)
        }
    });
    builder.connect(s, step)?;
    builder.set_entry(s)?;
    builder.set_exit(step)?;
    let dag = builder.build()?;

    // 第一次执行：初始 sum=0，累加 10 → 10
    println!("=== 第一次执行 (input=10) ===");
    let r1 = Executor::execute(&dag, Box::new(10i32), &ctx).await?;
    let out1: &i32 = r1.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*out1, 10);
    assert_eq!(store.get::<i32>("sum"), Some(10));
    println!("  输出 = {out1}, store[\"sum\"] = {:?}", store.get::<i32>("sum"));

    // 第二次执行：sum 已为 10，累加 20 → 30（状态跨执行持久化）
    println!("\n=== 第二次执行 (input=20) ===");
    let r2 = Executor::execute(&dag, Box::new(20i32), &ctx).await?;
    let out2: &i32 = r2.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*out2, 30);
    assert_eq!(store.get::<i32>("sum"), Some(30));
    println!("  输出 = {out2}, store[\"sum\"] = {:?}", store.get::<i32>("sum"));

    // TTL 演示：设置一个短命条目，等待过期后读取返回 None
    println!("\n=== TTL 过期演示 ===");
    store.set("ephemeral", 42i32, Some(Duration::from_millis(100)));
    println!("  写入 ephemeral=42 (TTL 100ms)");
    println!("  立即读取: {:?}", store.get::<i32>("ephemeral"));

    tokio::time::sleep(Duration::from_millis(150)).await;
    println!("  150ms 后读取: {:?}", store.get::<i32>("ephemeral"));

    println!("\nOK");
    Ok(())
}
