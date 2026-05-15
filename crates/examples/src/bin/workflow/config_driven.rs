//! 从 XML 配置构建命名空间工作流并执行——综合示例。
//!
//! 展示全部 4 种 XML 元素，共 5 个场景：
//!
//! | 场景 | 元素 | 说明 |
//! |------|------|------|
//! | 1 | `<workflow>`, `<end>` | 线性管道 |
//! | 2 | `<workflow>`, `<end>` | 独立并行节点 |
//! | 3 | `<workflow>`, `<end>` | 格式化管道 |
//! | 4 | `<if>`, `<workflow>`, `<end>` | 条件分支 |
//! | 5 | `<loop>`, `<workflow>`, `<end>` | 固定次数循环 |
//!
//! 运行：`cargo run -p examples --bin workflow_config_driven`

use std::sync::Arc;

use intelligent_subject::workflow::config::ConfigBuilder;
use intelligent_subject::workflow::config::WorkflowFactoryRegistry;
use intelligent_subject::workflow::definition::from_fn;
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::executor::Executor;
use intelligent_subject::workflow::model::{ExecutionContext, Namespace};
use intelligent_subject::workflow::platform::NullPlatform;

fn make_registry() -> WorkflowFactoryRegistry {
    let mut reg = WorkflowFactoryRegistry::new();
    reg.register("append_x", || {
        from_fn("append_x", |input: String, _ctx: &ExecutionContext| async move {
            Ok::<String, WorkflowError>(format!("{input}X"))
        })
    });
    reg.register("format", || {
        from_fn("format", |input: String, _ctx: &ExecutionContext| async move {
            Ok::<String, WorkflowError>(format!("Report: {input}"))
        })
    });
    reg.register("is_positive", || {
        from_fn("is_positive", |input: String, _ctx: &ExecutionContext| async move {
            let n: i32 = input.parse().map_err(|e: std::num::ParseIntError| {
                WorkflowError::ValidationError(e.to_string())
            })?;
            Ok::<bool, WorkflowError>(n > 0)
        })
    });
    reg.register("add_one", || {
        from_fn("add_one", |input: String, _ctx: &ExecutionContext| async move {
            let n: i32 = input.parse().map_err(|e: std::num::ParseIntError| {
                WorkflowError::ValidationError(e.to_string())
            })?;
            Ok::<String, WorkflowError>((n + 1).to_string())
        })
    });
    reg
}

// ── XML 场景 ──────────────────────────────────────────────────

/// 场景 1：线性管道
///
/// `hello` -> append_x -> `helloX` -> append_x -> `helloXX`
const LINEAR_XML: &str = r#"
<workflow>
  <workflow result_name="a" impl="append_x" input="hello"/>
  <workflow result_name="b" impl="append_x" input="{a.value}"/>
  <end result="{b.value}"/>
</workflow>"#;

/// 场景 2：独立并行节点
///
/// 两个节点无依赖，并发执行；`<end>` 选取 x 的值。
const PARALLEL_XML: &str = r#"
<workflow>
  <workflow result_name="x" impl="append_x" input="foo"/>
  <workflow result_name="y" impl="append_x" input="bar"/>
  <end result="{x.value}"/>
</workflow>"#;

/// 场景 3：Format 管道
///
/// `world` -> format -> `Report: world`
const FORMAT_XML: &str = r#"
<workflow>
  <workflow result_name="step" impl="format" input="world"/>
  <end result="{step.value}"/>
</workflow>"#;

/// 场景 4：条件分支 (`<if>`)
///
/// `is_positive(42)` -> true -> format("positive!") -> `Report: positive!`
const IF_XML: &str = r#"
<workflow>
  <workflow result_name="check" impl="is_positive" input="42"/>
  <if predicate="{check.value}">
    <workflow result_name="msg" impl="format" input="positive!"/>
  </if>
  <end result="{msg.value}"/>
</workflow>"#;

/// 场景 5：固定次数循环 (`<loop>`)
///
/// 从 0 开始，每次 `add_one` 加 1，循环 3 次 -> 3
const LOOP_XML: &str = r#"
<workflow>
  <loop result_name="result" state_init="0" next_state="{step.value}" count="3">
    <workflow result_name="step" impl="add_one" input="{latest_state}"/>
  </loop>
  <end result="{result}"/>
</workflow>"#;

// ── main ──────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = make_registry();
    let builder = ConfigBuilder::new(registry);
    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    // ── 场景 1：线性管道 ──────────────────────────────────
    println!("=== Scenario 1: Linear Pipeline ===");
    let plan = builder.build_from_str(LINEAR_XML)?;
    let ns = Namespace::new();
    let result = Executor::execute(&plan, &ns, &ctx).await?;
    let val = result.downcast_ref::<String>().unwrap();
    println!("  hello -> {val}");
    assert_eq!(*val, "helloXX");

    // ── 场景 2：独立并行节点 ──────────────────────────────
    println!("\n=== Scenario 2: Independent Parallel ===");
    let plan = builder.build_from_str(PARALLEL_XML)?;
    let ns = Namespace::new();
    let result = Executor::execute(&plan, &ns, &ctx).await?;
    let val = result.downcast_ref::<String>().unwrap();
    println!("  x=fooX, y=barX, end picks x: {val}");
    assert_eq!(*val, "fooX");

    // ── 场景 3：Format 管道 ────────────────────────────────
    println!("\n=== Scenario 3: Format Pipeline ===");
    let plan = builder.build_from_str(FORMAT_XML)?;
    let ns = Namespace::new();
    let result = Executor::execute(&plan, &ns, &ctx).await?;
    let val = result.downcast_ref::<String>().unwrap();
    println!("  world -> {val}");
    assert_eq!(*val, "Report: world");

    // ── 场景 4：条件分支 (`<if>`) ─────────────────────────
    println!("\n=== Scenario 4: Conditional (<if>) ===");
    let plan = builder.build_from_str(IF_XML)?;
    let ns = Namespace::new();
    let result = Executor::execute(&plan, &ns, &ctx).await?;
    let val = result.downcast_ref::<String>().unwrap();
    println!("  is_positive(42) -> true -> {val}");
    assert_eq!(*val, "Report: positive!");

    // ── 场景 5：固定次数循环 (`<loop>`) ───────────────────
    println!("\n=== Scenario 5: Loop (<loop>) ===");
    let plan = builder.build_from_str(LOOP_XML)?;
    let ns = Namespace::new();
    let result = Executor::execute(&plan, &ns, &ctx).await?;
    let val = result.downcast_ref::<String>().unwrap();
    println!("  0 + 1 + 1 + 1 = {val}");
    assert_eq!(*val, "3");

    println!("\nAll 5 scenarios passed.");
    Ok(())
}
