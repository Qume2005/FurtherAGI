//! 循环（Loop）：使用 PlanBuilder 的 add_loop 构建固定次数循环。
//!
//! 演示如何：
//! - 用 `from_fn` 闭包创建工作流节点（无需定义结构体）
//! - 用 `PlanBuilder::add_loop()` 实现固定次数循环
//! - 通过 `latest_state` 和 `next_state` 在迭代间传递状态
//!
//! 运行：`cargo run -p examples --bin workflow_loop`

use std::sync::Arc;

use intelligent_subject::workflow::dag::PlanBuilder;
use intelligent_subject::workflow::definition::from_fn;
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::executor::Executor;
use intelligent_subject::workflow::model::{ExecutionContext, Namespace};
use intelligent_subject::workflow::platform::NullPlatform;

#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    // ── 场景 1：字符串循环追加 ────────────────────────────
    // 初始 "hello"，循环 3 次每次追加 "X" → "helloXXX"
    println!("=== Scenario 1: String loop (append X, 3 times) ===");
    {
        let mut builder = PlanBuilder::new();
        let step_id = builder.add_workflow(
            "step",
            "append_x",
            vec![("input".to_string(),
                intelligent_subject::workflow::config::ParamValue::Reference {
                    namespace: "latest_state".to_string(),
                    field: String::new(),
                },
            )],
            from_fn("append_x", |input: String, _ctx: &ExecutionContext| async move {
                Ok::<String, WorkflowError>(format!("{input}X"))
            }),
        );
        builder.add_loop("result", "hello", "{step.value}", 3, vec![step_id]);
        builder.add_end("{result}");

        let plan = builder.build()?;
        let ns = Namespace::new();
        let result = Executor::execute(&plan, &ns, &ctx).await?;
        let val = result.downcast_ref::<String>().unwrap();
        println!("  hello → {val}");
        assert_eq!(val, "helloXXX");
    }

    // ── 场景 2：数值累加循环 ──────────────────────────────
    // 初始 0，循环 5 次每次 +1 → "5"
    println!("\n=== Scenario 2: Counter loop (add_one, 5 times) ===");
    {
        let mut builder = PlanBuilder::new();
        let step_id = builder.add_workflow(
            "step",
            "add_one",
            vec![("input".to_string(),
                intelligent_subject::workflow::config::ParamValue::Reference {
                    namespace: "latest_state".to_string(),
                    field: String::new(),
                },
            )],
            from_fn("add_one", |input: String, _ctx: &ExecutionContext| async move {
                let n: i32 = input.parse().map_err(|e: std::num::ParseIntError| {
                    WorkflowError::ValidationError(e.to_string())
                })?;
                Ok::<String, WorkflowError>((n + 1).to_string())
            }),
        );
        builder.add_loop("result", "0", "{step.value}", 5, vec![step_id]);
        builder.add_end("{result}");

        let plan = builder.build()?;
        let ns = Namespace::new();
        let result = Executor::execute(&plan, &ns, &ctx).await?;
        let val = result.downcast_ref::<String>().unwrap();
        println!("  0 → {val}");
        assert_eq!(val, "5");
    }

    println!("\nAll 2 scenarios passed.");
    Ok(())
}
