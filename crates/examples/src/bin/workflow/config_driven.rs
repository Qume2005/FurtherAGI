//! 从 TOML 配置构建工作流 DAG 并执行。
//!
//! 运行：`cargo run -p examples --bin workflow_config_driven`

use std::sync::Arc;

use autonomous::workflow::config::{ConfigBuilder, TypeRegistry, WorkflowFactoryRegistry};
use autonomous::workflow::error::WorkflowError;
use autonomous::workflow::executor::Executor;
use autonomous::workflow::definition::from_fn;
use autonomous::workflow::model::ExecutionContext;
use autonomous::workflow::platform::NullPlatform;

const PIPELINE_TOML: &str = r#"
[workflow]
name = "pipeline@Main"
entry = "add1"
exit = "add2"

[nodes.add1]
kind = "workflow"
implementation = "add_one"

[nodes.mul]
kind = "workflow"
implementation = "mul_two"

[nodes.add2]
kind = "workflow"
implementation = "add_one"

[[edges]]
from = "add1"
to = "mul"

[[edges]]
from = "mul"
to = "add2"
"#;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let types = TypeRegistry::with_primitives();

    let mut workflows = WorkflowFactoryRegistry::new();
    workflows.register("add_one", || from_fn("add_one",
        |input: i32, _ctx: &ExecutionContext| async move {
            println!("  AddOne({input}) → {}", input + 1);
            Ok::<i32, WorkflowError>(input + 1)
        }
    ));
    workflows.register("mul_two", || from_fn("mul_two",
        |input: i32, _ctx: &ExecutionContext| async move {
            println!("  MulTwo({input}) → {}", input * 2);
            Ok::<i32, WorkflowError>(input * 2)
        }
    ));

    let builder = ConfigBuilder::new(types, workflows);
    let (workflow_id, dag) = builder.build_from_str(PIPELINE_TOML)?;
    println!("Built '{}' with {} nodes", workflow_id.as_str(), dag.topo_order().len());

    println!("\n--- Executor 直接执行 ---");
    let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };

    let result = Executor::execute(&dag, Box::new(3i32), &ctx).await?;
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    println!("Result: {output}");
    assert_eq!(*output, 9);

    // WorkflowManager 执行
    println!("\n--- WorkflowManager 执行 ---");
    use autonomous::workflow::workflow_manager::WorkflowManager;

    let types2 = TypeRegistry::with_primitives();
    let mut workflows2 = WorkflowFactoryRegistry::new();
    workflows2.register("add_one", || from_fn("add_one",
        |input: i32, _| async move { Ok::<i32, WorkflowError>(input + 1) }
    ));
    workflows2.register("mul_two", || from_fn("mul_two",
        |input: i32, _| async move { Ok::<i32, WorkflowError>(input * 2) }
    ));
    let builder2 = ConfigBuilder::new(types2, workflows2);
    let (workflow_id2, dag2) = builder2.build_from_str(PIPELINE_TOML)?;

    let mgr = WorkflowManager::new();
    mgr.register_composite(workflow_id2, dag2)?;
    mgr.validate_all()?;

    let result: i32 = mgr.execute_typed("pipeline@Main", 3, &ctx).await?;
    println!("Result: {result}");
    assert_eq!(result, 9);

    println!("\nOK");
    Ok(())
}
