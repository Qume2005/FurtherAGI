//! Scatter-Gather：一个值并行扇出到多条分支，收集结果为元组。
//!
//! 运行：`cargo run -p examples --bin workflow_broadcast`

use std::sync::Arc;

use intelligent_subject::workflow::dag::{DagBuilder, ProductJoinFn, make_clone_fn};
use intelligent_subject::workflow::definition::from_fn;
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
    let src = builder.add("builtin@AddOne", |input: i32| async move {
println!("  AddOne({input})");
        Ok::<i32, WorkflowError>(input + 1)
    });

    // Scatter-gather: 将输入克隆到 N 个分支并行执行，收集结果为 (i32, i32) 元组
    let gather_fn: ProductJoinFn = Box::new(|vals| {
        let a = *vals[0].downcast_ref::<i32>().unwrap();
        let b = *vals[1].downcast_ref::<i32>().unwrap();
        println!("  gather: ({a}, {b})");
        Box::new((a, b))
    });
    let sg = builder.add_scatter_gather(
        std::any::TypeId::of::<i32>(),
        make_clone_fn::<i32>(),
        vec![
            from_fn("mul_two", |input: i32, _| async move {
                let result = input * 2;
                println!("  MulTwo({input}) = {result}");
                Ok::<i32, WorkflowError>(result)
            }),
            from_fn("add_one", |input: i32, _| async move {
                let result = input + 1;
                println!("  AddOne({input}) = {result}");
                Ok::<i32, WorkflowError>(result)
            }),
        ],
        gather_fn,
        std::any::TypeId::of::<(i32, i32)>(),
    );

    builder.connect(src, sg)?;
    builder.set_entry(src)?;
    builder.set_exit(sg)?;

    let dag = builder.build()?;

    println!("Scatter-Gather: AddOne(0)=1 → [MulTwo(1)=2, AddOne(1)=2] → (2, 2)");
    let result = Executor::execute(&dag, Box::new(0i32), &ctx).await?;
    let output: &(i32, i32) = result.output.downcast_ref::<(i32, i32)>().unwrap();

    println!("Result = {output:?}");
    assert_eq!(*output, (2, 2));
    println!("OK");
    Ok(())
}