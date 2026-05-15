//! 条件分支（If）：使用 PlanBuilder 的 add_if 实现条件执行。
//!
//! 演示如何：
//! - 用 `from_fn` 闭包创建工作流节点（无需定义结构体）
//! - 用 `PlanBuilder::add_if()` 实现条件分支
//! - 谓词从命名空间读取 bool 值，true 时执行子节点
//! - 使用 `then` 将子命名空间的值传播到父命名空间
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

    // ── 场景 1：predicate 为 true，执行子节点并通过 then 传播值 ──────
    println!("=== Scenario 1: predicate=true, then propagates value ===");
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
            "inner",
            "append_positive",
            vec![("input".to_string(), ParamValue::Literal("42".to_string()))],
            from_fn("append_positive", |input: String, _ctx: &ExecutionContext| async move {
                Ok::<String, WorkflowError>(format!("{input}_positive"))
            }),
        );

        // If 节点：predicate 引用 check 的输出
        // then="{inner.value}" 将子命名空间中 inner.value 传播到父命名空间的 msg
        builder.add_if("msg", "{check.value}", vec![branch_id], Some("{inner.value}".to_string()));

        // 终止节点：返回 msg 的值
        builder.add_end("{msg}");

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

        // predicate=false 时，If 内子节点不执行，then 不传播
        // 直接返回 check 的值来验证 predicate 结果
        let branch_id = builder.add_workflow(
            "inner",
            "append_positive",
            vec![("input".to_string(), ParamValue::Literal("-5".to_string()))],
            from_fn("append_positive", |input: String, _ctx: &ExecutionContext| async move {
                Ok::<String, WorkflowError>(format!("{input}_positive"))
            }),
        );
        builder.add_if("__if", "{check.value}", vec![branch_id], None);
        builder.add_end("{check.value}");

        let plan = builder.build()?;
        let ns = Namespace::new();
        let result = Executor::execute(&plan, &ns, &ctx).await?;
        let val = result.downcast_ref::<bool>().unwrap();
        println!("  -5 > 0 → {val} (msg branch skipped)");
        assert_eq!(*val, false);
    }

    // ── 场景 3：then 引用子命名空间的复合值 ──────────────────
    println!("\n=== Scenario 3: then with chained child workflow ===");
    {
        let mut builder = PlanBuilder::new();

        builder.add_workflow(
            "source",
            "append_x",
            vec![("input".to_string(), ParamValue::Literal("data".to_string()))],
            from_fn("append_x", |input: String, _ctx: &ExecutionContext| async move {
                Ok::<String, WorkflowError>(format!("{input}X"))
            }),
        );

        // If 内有两个子节点，第一个的输出是第二个的输入
        let step1 = builder.add_workflow(
            "s1",
            "append_x",
            vec![("input".to_string(), ParamValue::Reference {
                namespace: "source".to_string(),
                field: "value".to_string(),
            })],
            from_fn("append_x", |input: String, _ctx: &ExecutionContext| async move {
                Ok::<String, WorkflowError>(format!("{input}X"))
            }),
        );
        let step2 = builder.add_workflow(
            "s2",
            "append_x",
            vec![("input".to_string(), ParamValue::Reference {
                namespace: "s1".to_string(),
                field: "value".to_string(),
            })],
            from_fn("append_x", |input: String, _ctx: &ExecutionContext| async move {
                Ok::<String, WorkflowError>(format!("{input}X"))
            }),
        );

        // 预设 flag
        let ns = Namespace::new();
        ns.set("flag.value", true);

        builder.add_if("result", "{flag.value}", vec![step1, step2], Some("{s2.value}".to_string()));
        builder.add_end("{result}");

        let plan = builder.build()?;
        let ctx2 = ExecutionContext {
            platform: Arc::new(NullPlatform::new()),
        };
        let result = Executor::execute(&plan, &ns, &ctx2).await?;
        let val = result.downcast_ref::<String>().unwrap();
        // source="dataX" → s1="dataXX" → s2="dataXXX" → result="dataXXX"
        println!("  data → source → s1 → s2 → {val}");
        assert_eq!(val, "dataXXX");
    }

    println!("\nAll 3 scenarios passed.");
    Ok(())
}
