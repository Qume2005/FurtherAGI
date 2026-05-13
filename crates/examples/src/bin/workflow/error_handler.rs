//! 错误处理（SumMatch）：通过或类型拆解实现错误路由。
//!
//! 演示如何：
//! - 节点输出 `Result<i32, String>`（或类型 `i32 | String`）
//! - SumMatch 节点将 Ok/Err 路由到不同分支
//! - 错误分支可以执行恢复逻辑
//!
//! 运行：`cargo run -p examples --bin workflow_error_handler`

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

    // ── 场景 1: 输入为正数 → Ok 路径 ──────────────────────
    println!("=== 正数输入 (5) ===");
    {
        let mut builder = DagBuilder::new();

        // 节点输出 Result<i32, String>：正数返回 Ok，负数返回 Err
        let check = builder.add("builtin@FailIfNegative", |input: i32| async move {
            if input < 0 {
                println!("  FailIfNegative({input}): ERR");
                Ok::<Result<i32, String>, WorkflowError>(Err("value is negative".into()))
            } else {
                println!("  FailIfNegative({input}): OK");
                Ok::<Result<i32, String>, WorkflowError>(Ok(input))
            }
        });

        let sm = builder.add_sum_match::<i32, String>();

        // Ok 路径：正常处理
        let ok_path = builder.add("builtin@AddOne", |input: i32| async move {
            println!("  ok branch: AddOne({input})");
            Ok::<i32, WorkflowError>(input + 1)
        });

        // Err 路径：错误恢复
        let err_path = builder.add("builtin@Recover", |_error_msg: String| async move {
            println!("  err branch: recovering → 0");
            Ok::<i32, WorkflowError>(0)
        });

        builder.connect(check, sm)?;
        builder.connect_labeled(sm, ok_path, "ok")?;
        builder.connect_labeled(sm, err_path, "err")?;
        builder.set_entry(check)?;
        builder.set_exit(ok_path)?;

        let dag = builder.build()?;
        let result = Executor::execute(&dag, Box::new(5i32), &ctx).await?;
        let out: &i32 = result.output.downcast_ref::<i32>().unwrap();
        println!("  结果: {out}");
        assert_eq!(*out, 6);
    }

    // ── 场景 2: 输入为负数 → Err 路径 ──────────────────────
    println!("\n=== 负数输入 (-5) ===");
    {
        let mut builder = DagBuilder::new();

        let check = builder.add("builtin@FailIfNegative", |input: i32| async move {
            if input < 0 {
                println!("  FailIfNegative({input}): ERR");
                Ok::<Result<i32, String>, WorkflowError>(Err("value is negative".into()))
            } else {
                println!("  FailIfNegative({input}): OK");
                Ok::<Result<i32, String>, WorkflowError>(Ok(input))
            }
        });

        let sm = builder.add_sum_match::<i32, String>();

        let ok_path = builder.add("builtin@AddOne", |input: i32| async move {
            println!("  ok branch: AddOne({input})");
            Ok::<i32, WorkflowError>(input + 1)
        });

        let err_path = builder.add("builtin@Recover", |_error_msg: String| async move {
            println!("  err branch: recovering → 0");
            Ok::<i32, WorkflowError>(0)
        });

        builder.connect(check, sm)?;
        builder.connect_labeled(sm, ok_path, "ok")?;
        builder.connect_labeled(sm, err_path, "err")?;
        builder.set_entry(check)?;
        builder.set_exit(err_path)?;

        let dag = builder.build()?;
        let result = Executor::execute(&dag, Box::new(-5i32), &ctx).await?;
        let out: &i32 = result.output.downcast_ref::<i32>().unwrap();
        println!("  结果: {out}");
        assert_eq!(*out, 0);
    }

    println!("\nOK");
    Ok(())
}