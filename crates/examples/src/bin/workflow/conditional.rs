//! 条件分支（Conditional）：根据谓词结果路由到不同分支。
//!
//! 运行：`cargo run -p examples --bin workflow_conditional`

use std::sync::Arc;

use autonomous::workflow::dag::DagBuilder;
use autonomous::workflow::error::WorkflowError;
use autonomous::workflow::executor::Executor;
use autonomous::workflow::definition::from_fn;
use autonomous::workflow::model::ExecutionContext;
use autonomous::workflow::platform::NullPlatform;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    let mut builder = DagBuilder::new();
    let cond = builder.add_conditional(
        "builtin@IsPositive",
        from_fn("is_positive", |input: i32, _ctx: &ExecutionContext| async move {
            Ok::<bool, WorkflowError>(input > 0)
        }),
    )?;
    let t = builder.add("builtin@PositiveBranch",
        |input: bool| async move {
            assert!(input);
            println!("  → positive branch");
            Ok::<i32, WorkflowError>(100)
        });
    let f = builder.add("builtin@NegativeBranch",
        |input: bool| async move {
            assert!(!input);
            println!("  → negative branch");
            Ok::<i32, WorkflowError>(-100)
        });

    builder.connect_labeled(cond, t, "true")?;
    builder.connect_labeled(cond, f, "false")?;
    builder.set_entry(cond)?;
    // DAG 只有一个出口；要测试另一分支，需要构建不同的 DAG（exit 指向 f）
    builder.set_exit(t)?;

    let dag = builder.build()?;

    println!("Input: 42");
    let result = Executor::execute(&dag, Box::new(42i32), &ctx).await?;
    assert_eq!(*result.output.downcast_ref::<i32>().unwrap(), 100);

    // 负数分支：需要将 exit 切换到 false 分支，因此重新构建整个 DAG
    // 使用 from_fn 是因为 add_conditional 的第二个参数需要 Workflow trait 实现，
    // 而普通闭包无法直接满足；from_fn 将闭包包装为符合 trait 的类型
    println!("\nInput: -7");
    let mut b2 = DagBuilder::new();
    let c2 = b2.add_conditional(
        "builtin@IsPositive",
        from_fn("is_positive", |input: i32, _| async move { Ok::<bool, WorkflowError>(input > 0) }),
    )?;
    let t2 = b2.add("builtin@PositiveBranch",
        |input: bool| async move { Ok::<i32, WorkflowError>(if input { 100 } else { 0 }) });
    let f2 = b2.add("builtin@NegativeBranch",
        |input: bool| async move { Ok::<i32, WorkflowError>(if input { 0 } else { -100 }) });
    b2.connect_labeled(c2, t2, "true")?;
    b2.connect_labeled(c2, f2, "false")?;
    b2.set_entry(c2)?;
    b2.set_exit(f2)?;

    let r2 = Executor::execute(&b2.build()?, Box::new(-7i32), &ctx).await?;
    assert_eq!(*r2.output.downcast_ref::<i32>().unwrap(), -100);
    println!("\nOK");
    Ok(())
}
