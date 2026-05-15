//! 条件分支（If）：使用 PlanBuilder 的 add_if 实现条件执行。
//!
//! 演示如何：
//! - 用 `from_fn` 闭包创建工作流节点（无需定义结构体）
//! - 用 `PlanBuilder::add_if()` 实现条件分支
//! - 谓词从命名空间读取 bool 值，true 时执行子节点
//!
//! 运行：`cargo run -p examples --bin workflow_conditional`

use std::sync::Arc;

use intelligent_subject::workflow::config::ParamValue;
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

    // ── 场景 1：predicate 为 true，执行子节点 ──────────────
    println!("=== Scenario 1: predicate=true, branch executes ===");
    {
        let mut builder = PlanBuilder::new();

        // 第一步：检查数值是否大于 0（返回 bool）
        builder.add_workflow(
            "check",
            "is_positive",
            vec![("input".to_string(), ParamValue::Literal("42".to_string()))],
            from_fn("is_positive", |input: String, _ctx: &ExecutionContext| async move {
                let n: i32 = input.parse().map_err(|e: std::num::ParseIntError| {
                    WorkflowError::ValidationError(e.to_string())
                })?;
                Ok::<bool, WorkflowError>(n > 0)
            }),
        );

        // 第二步（条件分支内）：追加 "_positive"
        let branch_id = builder.add_workflow(
            "msg",
            "append_positive",
            vec![("input".to_string(), ParamValue::Literal("42".to_string()))],
            from_fn("append_positive", |input: String, _ctx: &ExecutionContext| async move {
                Ok::<String, WorkflowError>(format!("{input}_positive"))
            }),
        );

        // If 节点：predicate 引用 check 的输出
        builder.add_if("__if", "{check.value}", vec![branch_id]);

        // 终止节点：返回 msg 的值
        builder.add_end("{msg.value}");

        let plan = builder.build()?;
        let ns = Namespace::new();
        let result = Executor::execute(&plan, &ns, &ctx).await?;
        let val = result.downcast_ref::<String>().unwrap();
        println!("  42 > 0 → {val}");
        assert_eq!(val, "42_positive");
    }

    // ── 场景 2：predicate 为 false，跳过子节点 ────────────
    println!("\n=== Scenario 2: predicate=false, branch skipped ===");
    {
        let mut builder = PlanBuilder::new();

        builder.add_workflow(
            "check",
            "is_positive",
            vec![("input".to_string(), ParamValue::Literal("-5".to_string()))],
            from_fn("is_positive", |input: String, _ctx: &ExecutionContext| async move {
                let n: i32 = input.parse().map_err(|e: std::num::ParseIntError| {
                    WorkflowError::ValidationError(e.to_string())
                })?;
                Ok::<bool, WorkflowError>(n > 0)
            }),
        );

        let branch_id = builder.add_workflow(
            "msg",
            "append_positive",
            vec![("input".to_string(), ParamValue::Literal("-5".to_string()))],
            from_fn("append_positive", |input: String, _ctx: &ExecutionContext| async move {
                Ok::<String, WorkflowError>(format!("{input}_positive"))
            }),
        );

        builder.add_if("__if", "{check.value}", vec![branch_id]);

        // 当 predicate 为 false 时，msg 未被设置
        // 直接返回 check 的值来验证 predicate 结果
        builder.add_end("{check.value}");

        let plan = builder.build()?;
        let ns = Namespace::new();
        let result = Executor::execute(&plan, &ns, &ctx).await?;
        let val = result.downcast_ref::<bool>().unwrap();
        println!("  -5 > 0 → {val} (msg branch skipped)");
        assert_eq!(*val, false);
    }

    println!("\nAll 2 scenarios passed.");
    Ok(())
}
