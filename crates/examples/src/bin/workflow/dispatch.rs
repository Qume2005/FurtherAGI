//! 积类型拆分（Dispatch）：将元组拆为多条独立路径，ProductJoin 的逆操作。
//!
//! 演示如何：
//! - 使用 `add_dispatch` 将 `(i32, String)` 拆为两条路径（int_path 和 str_path）
//! - 各路径独立处理后通过 ProductJoin 重新合并
//!
//! 运行：`cargo run -p examples --bin workflow_dispatch`

use std::sync::Arc;

use intelligent_subject::workflow::dag::{DagBuilder, ProductJoinFn};
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

    // 源节点：i32 → (i32, String)
    let src = builder.add("builtin@MakePair", |input: i32| async move {
        println!("  MakePair({input}) → ({}, \"n={input}\")", input * 2);
        Ok::<(i32, String), WorkflowError>((input * 2, format!("n={input}")))
    });

    // Dispatch: (i32, String) → [i32, String]，拆到两条路径
    let dispatch = builder.add_dispatch(
        2,
        Box::new(|input| {
            let (a, b) = input.downcast_ref::<(i32, String)>().unwrap();
            println!("  Dispatch: ({a}, \"{b}\") → [{a}, \"{b}\"]");
            vec![
                Box::new(*a) as Box<dyn std::any::Any + Send + Sync>,
                Box::new(b.clone()),
            ]
        }),
    );

    // 路径 A：i32 → String
    let int_path = builder.add("builtin@FormatInt", |input: i32| async move {
        let result = format!("i={}", input);
        println!("  FormatInt({input}) → \"{result}\"");
        Ok::<String, WorkflowError>(result)
    });

    // 路径 B：String → String（转大写）
    let str_path = builder.add("builtin@Upper", |input: String| async move {
        let result = input.to_uppercase();
        println!("  Upper(\"{input}\") → \"{result}\"");
        Ok::<String, WorkflowError>(result)
    });

    // ProductJoin: (String, String) → (String, String)
    let gather_fn: ProductJoinFn = Box::new(|vals| {
        let a = vals[0].downcast_ref::<String>().unwrap().clone();
        let b = vals[1].downcast_ref::<String>().unwrap().clone();
        Box::new((a, b))
    });
    let join = builder.add_product_join(
        std::any::TypeId::of::<(String, String)>(),
        vec![
            |val: &(dyn std::any::Any + Send + Sync)| -> Box<dyn std::any::Any + Send + Sync> {
                Box::new(val.downcast_ref::<String>().unwrap().clone())
            },
            |val: &(dyn std::any::Any + Send + Sync)| -> Box<dyn std::any::Any + Send + Sync> {
                Box::new(val.downcast_ref::<String>().unwrap().clone())
            },
        ],
        gather_fn,
    );

    builder.connect(src, dispatch)?;
    builder.connect(dispatch, int_path)?;
    builder.connect(dispatch, str_path)?;
    builder.connect(int_path, join)?;
    builder.connect(str_path, join)?;
    builder.set_entry(src)?;
    builder.set_exit(join)?;

    let dag = builder.build()?;

    println!("Dispatch: MakePair(5) → (10, \"n=5\")");
    println!("  → Dispatch → [10, \"n=5\"]");
    println!("  → [FormatInt(10)=\"i=10\", Upper(\"n=5\")=\"N=5\"]");
    println!("  → ProductJoin → (\"i=10\", \"N=5\")");

    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await?;
    let output = result.output.downcast_ref::<(String, String)>().unwrap();

    println!("\nResult = {:?}", output);
    assert_eq!(output.0, "i=10");
    assert_eq!(output.1, "N=5");
    println!("OK");
    Ok(())
}
