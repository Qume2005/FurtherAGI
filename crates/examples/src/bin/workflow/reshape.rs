//! 元组重组（Reshape）：调整元组嵌套结构，不改变值本身。
//!
//! 演示如何：
//! - 使用 `add_reshape` 将 `(i32, i32)` 重组为 `((i32, i32), i32)`
//! - Reshape 节点是纯结构变换，不涉及实际计算
//!
//! 运行：`cargo run -p examples --bin workflow_reshape`

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

    let mut builder = DagBuilder::new();

    // 源节点：i32 → (i32, i32)
    let src = builder.add("builtin@Pair", |input: i32| async move {
        println!("  Pair({input}) → ({}, {})", input + 1, input * 2);
        Ok::<(i32, i32), WorkflowError>((input + 1, input * 2))
    });

    // Reshape: (i32, i32) → ((i32, i32), i32)，将原始元组和它们的和打包
    let reshape = builder.add_reshape(Box::new(|input| {
        let (a, b) = *input.downcast_ref::<(i32, i32)>().unwrap();
        println!("  Reshape: ({a}, {b}) → (({a}, {b}), {})", a + b);
        Box::new(((a, b), a + b))
    }));

    // 下游：((i32, i32), i32) → i32
    let dst = builder.add("builtin@SumAll", |input: ((i32, i32), i32)| async move {
        let result = input.0 .0 + input.0 .1 + input.1;
        println!("  SumAll: (({}, {}), {}) → {result}", input.0 .0, input.0 .1, input.1);
        Ok::<i32, WorkflowError>(result)
    });

    builder.connect(src, reshape)?;
    builder.connect(reshape, dst)?;
    builder.set_entry(src)?;
    builder.set_exit(dst)?;

    let dag = builder.build()?;

    println!("Reshape: Pair(3) → (4, 6) → ((4, 6), 10) → 4+6+10=20");
    let result = Executor::execute(&dag, Box::new(3i32), &ctx).await?;
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();

    println!("Result = {output}");
    assert_eq!(*output, 20);
    println!("OK");
    Ok(())
}
