//! 从 XML 配置构建工作流 DAG 并执行。
//!
//! 运行：`cargo run -p examples --bin workflow_config_driven`

use std::sync::Arc;

use intelligent_subject::workflow::config::{ConfigBuilder, TypeRegistry, WorkflowFactoryRegistry};
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::executor::Executor;
use intelligent_subject::workflow::definition::from_fn;
use intelligent_subject::workflow::model::ExecutionContext;
use intelligent_subject::workflow::platform::NullPlatform;
use intelligent_subject::workflow::workflow_manager::WorkflowManager;
use intelligent_subject::workflow::dag::ProductJoinFn;

const PIPELINE_XML: &str = r#"
<workflow name="pipeline" entry="add1" exit="add2">
  <node name="add1" implementation="add_one"/>
  <node name="mul"  implementation="mul_two"/>
  <node name="add2" implementation="add_one"/>
  <connect from="add1" to="mul"/>
  <connect from="mul"  to="add2"/>
</workflow>"#;

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
    let (workflow_id, dag) = builder.build_from_str(PIPELINE_XML)?;
    println!("Built '{}' with {} nodes", workflow_id.as_str(), dag.topo_order().len());

    // ── Executor 直接执行 ──────────────────────────────────
    println!("\n--- Executor 直接执行 ---");
    let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };

    let result = Executor::execute(&dag, Box::new(3i32), &ctx).await?;
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    println!("Result: {output}");
    assert_eq!(*output, 9);

    // ── WorkflowManager 执行 ───────────────────────────────
    println!("\n--- WorkflowManager 执行 ---");
    let types2 = TypeRegistry::with_primitives();
    let mut workflows2 = WorkflowFactoryRegistry::new();
    workflows2.register("add_one", || from_fn("add_one",
        |input: i32, _| async move { Ok::<i32, WorkflowError>(input + 1) }
    ));
    workflows2.register("mul_two", || from_fn("mul_two",
        |input: i32, _| async move { Ok::<i32, WorkflowError>(input * 2) }
    ));
    let builder2 = ConfigBuilder::new(types2, workflows2);
    let (workflow_id2, dag2) = builder2.build_from_str(PIPELINE_XML)?;

    let mgr = WorkflowManager::new();
    mgr.register_composite(workflow_id2, dag2)?;
    mgr.validate_all()?;

    let result: i32 = mgr.execute_typed("pipeline", 3, &ctx).await?;
    println!("Result: {result}");
    assert_eq!(result, 9);

    // ── Scatter-Gather XML 示例 ─────────────────────────────
    println!("\n--- Scatter-Gather XML 示例 ---");
    let types3 = TypeRegistry::with_primitives();
    let mut workflows3 = WorkflowFactoryRegistry::new();
    workflows3.register("add_one", || from_fn("add_one",
        |input: i32, _| async move { Ok::<i32, WorkflowError>(input + 1) }
    ));
    workflows3.register("mul_two", || from_fn("mul_two",
        |input: i32, _| async move { Ok::<i32, WorkflowError>(input * 2) }
    ));

    let mut builder3 = ConfigBuilder::new(types3, workflows3);
    let gather_fn: ProductJoinFn = Box::new(|vals| {
        let a = *vals[0].downcast_ref::<i32>().unwrap();
        let b = *vals[1].downcast_ref::<i32>().unwrap();
        Box::new((a, b))
    });
    builder3.register_clone_gather(
        "tuple2_i32",
        std::any::TypeId::of::<(i32, i32)>(),
        gather_fn,
    );

    let (_, sg_dag) = builder3.build_from_str(r#"
    <workflow name="sg" entry="src" exit="sg">
      <node name="src" implementation="add_one"/>
      <clone name="sg" type="i32" output-type="(i32,i32)" gather="tuple2_i32">
        <branch implementation="mul_two"/>
        <branch implementation="add_one"/>
      </clone>
      <connect from="src" to="sg"/>
    </workflow>"#)?;

    let result = Executor::execute(&sg_dag, Box::new(0i32), &ctx).await?;
    let output = result.output.downcast_ref::<(i32, i32)>().unwrap();
    println!("Scatter-Gather(0): {:?}", output);
    assert_eq!(*output, (2, 2));

    println!("\nOK");
    Ok(())
}