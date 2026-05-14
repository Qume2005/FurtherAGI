//! 从 XML 配置构建工作流 DAG 并执行——综合示例。
//!
//! 展示全部节点类型和属性语法：
//!
//! | 节点类型 | XML 语法 | 示例场景 |
//! |---------|---------|---------|
//! | Workflow | `<node implementation="..."/>` | 场景 1, 3, 4, 5 |
//! | Clone (Scatter-Gather) | `<clone>` + `<branch>` 子元素 | 场景 2 |
//! | Conditional | `<conditional implementation="..."/>` | 场景 6 |
//! | Dispatch (属性) | `<node dispatch="fn" dispatch-count="N"/>` | 场景 3 |
//! | ProductJoin (属性) | `<node join="fn"/>` | 场景 3 |
//! | SumMatch (属性) | `<node sum-match="OkType/ErrType"/>` | 场景 4 |
//! | Reshape (属性) | `<connect reshape="fn"/>` | 场景 5 |
//!
//! 运行：`cargo run -p examples --bin workflow_config_driven`

use std::sync::Arc;

use intelligent_subject::workflow::config::{ConfigBuilder, TypeRegistry, WorkflowFactoryRegistry};
use intelligent_subject::workflow::dag::ProductJoinFn;
use intelligent_subject::workflow::definition::from_fn;
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::executor::Executor;
use intelligent_subject::workflow::model::ExecutionContext;
use intelligent_subject::workflow::platform::NullPlatform;

// ── XML 配置 ──────────────────────────────────────────────────

/// 场景 1：线性管道 i32 → i32
const LINEAR_XML: &str = r#"
<workflow name="linear" entry="a" exit="c">
  <node name="a" implementation="add_one"/>
  <node name="b" implementation="mul_two"/>
  <node name="c" implementation="add_one"/>
  <connect from="a" to="b"/>
  <connect from="b" to="c"/>
</workflow>"#;

/// 场景 2：Scatter-Gather（<clone> 独立元素语法）
const SCATTER_GATHER_XML: &str = r#"
<workflow name="scatter_gather" entry="src" exit="sg">
  <node name="src" implementation="add_one"/>
  <clone name="sg" type="i32" output-type="(i32,i32)" gather="tuple2_i32">
    <branch implementation="mul_two"/>
    <branch implementation="add_one"/>
  </clone>
  <connect from="src" to="sg"/>
</workflow>"#;

/// 场景 3：Dispatch + ProductJoin（属性语法：扇出/扇入）
///
/// `<node dispatch="split_tuple" dispatch-count="2"/>` 拆分输出
/// `<node join="str_tuple2_join"/>` 合并输入
const DISPATCH_JOIN_XML: &str = r#"
<workflow name="dispatch_join" entry="src" exit="result">
  <node name="src" implementation="make_tuple"
        dispatch="split_tuple" dispatch-count="2"/>
  <node name="left" implementation="format_int"/>
  <node name="right" implementation="upper"/>
  <node name="result" implementation="make_report"
        join="str_tuple2_join"/>
  <connect from="src" to="left"/>
  <connect from="src" to="right"/>
  <connect from="left" to="result"/>
  <connect from="right" to="result"/>
</workflow>"#;

/// 场景 4a：SumMatch Ok 路径（属性语法：`sum-match="i32/String"`）
const SUMMATCH_OK_XML: &str = r#"
<workflow name="summatch_ok" entry="src" exit="ok_path">
  <node name="src" implementation="ok_result" sum-match="i32/String"/>
  <node name="ok_path" implementation="format_ok"/>
  <node name="err_path" implementation="format_err"/>
  <connect from="src" to="ok_path" label="ok"/>
  <connect from="src" to="err_path" label="err"/>
</workflow>"#;

/// 场景 4b：SumMatch Err 路径
const SUMMATCH_ERR_XML: &str = r#"
<workflow name="summatch_err" entry="src" exit="err_path">
  <node name="src" implementation="err_result" sum-match="i32/String"/>
  <node name="ok_path" implementation="format_ok"/>
  <node name="err_path" implementation="format_err"/>
  <connect from="src" to="ok_path" label="ok"/>
  <connect from="src" to="err_path" label="err"/>
</workflow>"#;

/// 场景 5：Reshape 元组重组（属性语法：`<connect reshape="..."/>`）
const RESHAPE_XML: &str = r#"
<workflow name="reshape" entry="src" exit="dst">
  <node name="src" implementation="make_pair"/>
  <node name="dst" implementation="sum_all"/>
  <connect from="src" to="dst" reshape="pair_and_sum"/>
</workflow>"#;

/// 场景 6：Conditional 条件分支
///
/// Conditional 节点将 bool 输出传递给下游分支，
/// 所以下游 workflow 需要接受 bool 输入。
const CONDITIONAL_XML: &str = r#"
<workflow name="conditional" entry="check" exit="positive">
  <conditional name="check" implementation="is_positive"/>
  <node name="positive" implementation="on_positive"/>
  <node name="negative" implementation="on_negative"/>
  <connect from="check" to="positive" label="true"/>
  <connect from="check" to="negative" label="false"/>
</workflow>"#;

// ── 注册 ─────────────────────────────────────────────────────

fn make_builder() -> ConfigBuilder {
    let types = TypeRegistry::with_primitives();
    let mut workflows = WorkflowFactoryRegistry::new();

    // --- Workflow 工厂 ---
    workflows.register("add_one", || from_fn("add_one",
        |input: i32, _ctx: &ExecutionContext| async move {
            let out = input + 1;
            println!("    AddOne({input}) -> {out}");
            Ok::<i32, WorkflowError>(out)
        }
    ));
    workflows.register("mul_two", || from_fn("mul_two",
        |input: i32, _ctx: &ExecutionContext| async move {
            let out = input * 2;
            println!("    MulTwo({input}) -> {out}");
            Ok::<i32, WorkflowError>(out)
        }
    ));
    workflows.register("on_positive", || from_fn("on_positive",
        |input: bool, _ctx: &ExecutionContext| async move {
            let out = if input { 42 } else { 0 };
            println!("    OnPositive({input}) -> {out}");
            Ok::<i32, WorkflowError>(out)
        }
    ));
    workflows.register("on_negative", || from_fn("on_negative",
        |input: bool, _ctx: &ExecutionContext| async move {
            let out = if input { 0 } else { -1 };
            println!("    OnNegative({input}) -> {out}");
            Ok::<i32, WorkflowError>(out)
        }
    ));
    workflows.register("is_positive", || from_fn("is_positive",
        |input: i32, _ctx: &ExecutionContext| async move {
            let out = input > 0;
            println!("    IsPositive({input}) -> {out}");
            Ok::<bool, WorkflowError>(out)
        }
    ));
    workflows.register("make_tuple", || from_fn("make_tuple",
        |input: i32, _ctx: &ExecutionContext| async move {
            let out = (input * 2, format!("n={input}"));
            println!("    MakeTuple({input}) -> ({}, {:?})", out.0, out.1);
            Ok::<(i32, String), WorkflowError>(out)
        }
    ));
    workflows.register("format_int", || from_fn("format_int",
        |input: i32, _ctx: &ExecutionContext| async move {
            let out = format!("i={input}");
            println!("    FormatInt({input}) -> \"{out}\"");
            Ok::<String, WorkflowError>(out)
        }
    ));
    workflows.register("upper", || from_fn("upper",
        |input: String, _ctx: &ExecutionContext| async move {
            let out = input.to_uppercase();
            println!("    Upper(\"{input}\") -> \"{out}\"");
            Ok::<String, WorkflowError>(out)
        }
    ));
    workflows.register("make_report", || from_fn("make_report",
        |input: (String, String), _ctx: &ExecutionContext| async move {
            let out = format!("Report: {}, {}", input.0, input.1);
            println!("    MakeReport((\"{}\", \"{}\")) -> \"{out}\"", input.0, input.1);
            Ok::<String, WorkflowError>(out)
        }
    ));
    workflows.register("ok_result", || from_fn("ok_result",
        |input: i32, _ctx: &ExecutionContext| async move {
            let out: Result<i32, String> = Ok(input * 3);
            println!("    OkResult({input}) -> Ok({})", input * 3);
            Ok::<Result<i32, String>, WorkflowError>(out)
        }
    ));
    workflows.register("err_result", || from_fn("err_result",
        |input: i32, _ctx: &ExecutionContext| async move {
            let out: Result<i32, String> = Err(format!("bad: {input}"));
            println!("    ErrResult({input}) -> Err(\"bad: {input}\")");
            Ok::<Result<i32, String>, WorkflowError>(out)
        }
    ));
    workflows.register("format_ok", || from_fn("format_ok",
        |input: i32, _ctx: &ExecutionContext| async move {
            let out = format!("ok: {input}");
            println!("    FormatOk({input}) -> \"{out}\"");
            Ok::<String, WorkflowError>(out)
        }
    ));
    workflows.register("format_err", || from_fn("format_err",
        |input: String, _ctx: &ExecutionContext| async move {
            let out = format!("err: {input}");
            println!("    FormatErr(\"{input}\") -> \"{out}\"");
            Ok::<String, WorkflowError>(out)
        }
    ));
    workflows.register("make_pair", || from_fn("make_pair",
        |input: i32, _ctx: &ExecutionContext| async move {
            let out = (input + 1, input * 2);
            println!("    MakePair({input}) -> ({}, {})", out.0, out.1);
            Ok::<(i32, i32), WorkflowError>(out)
        }
    ));
    workflows.register("sum_all", || from_fn("sum_all",
        |input: ((i32, i32), i32), _ctx: &ExecutionContext| async move {
            let out = input.0 .0 + input.0 .1 + input.1;
            println!("    SumAll((({}, {}), {})) -> {out}", input.0 .0, input.0 .1, input.1);
            Ok::<i32, WorkflowError>(out)
        }
    ));

    let mut builder = ConfigBuilder::new(types, workflows);

    // --- 结构性工厂 ---
    // CloneGather: (i32, i32)
    let gather_fn: ProductJoinFn = Box::new(|vals| {
        let a = *vals[0].downcast_ref::<i32>().unwrap();
        let b = *vals[1].downcast_ref::<i32>().unwrap();
        Box::new((a, b))
    });
    builder.register_clone_gather(
        "tuple2_i32",
        std::any::TypeId::of::<(i32, i32)>(),
        gather_fn,
    );
    // Dispatch: (i32, String) -> [i32, String]
    builder.register_dispatch(
        "split_tuple",
        2,
        Box::new(|input| {
            let (a, b) = input.downcast_ref::<(i32, String)>().unwrap();
            println!("    Dispatch: ({}, {:?}) -> [{}, {:?}]", a, b, a, b);
            vec![
                Box::new(*a) as Box<dyn std::any::Any + Send + Sync>,
                Box::new(b.clone()),
            ]
        }),
    );
    // ProductJoin: (String, String)
    let str_join_fn: ProductJoinFn = Box::new(|vals| {
        let a = vals[0].downcast_ref::<String>().unwrap().clone();
        let b = vals[1].downcast_ref::<String>().unwrap().clone();
        println!("    ProductJoin: [\"{a}\", \"{b}\"] -> (\"{a}\", \"{b}\")");
        Box::new((a, b))
    });
    builder.register_product_join(
        "str_tuple2_join",
        std::any::TypeId::of::<(String, String)>(),
        vec![
            |val: &(dyn std::any::Any + Send + Sync)| -> Box<dyn std::any::Any + Send + Sync> {
                Box::new(val.downcast_ref::<String>().unwrap().clone())
            },
            |val: &(dyn std::any::Any + Send + Sync)| -> Box<dyn std::any::Any + Send + Sync> {
                Box::new(val.downcast_ref::<String>().unwrap().clone())
            },
        ],
        str_join_fn,
    );
    // Reshape: (i32, i32) -> ((i32, i32), i32)
    builder.register_reshape(
        "pair_and_sum",
        Box::new(|input| {
            let (a, b) = *input.downcast_ref::<(i32, i32)>().unwrap();
            println!("    Reshape: ({a}, {b}) -> (({a}, {b}), {})", a + b);
            Box::new(((a, b), a + b))
        }),
    );
    // SumMatch: Result<i32, String>
    builder.register_sum_match::<i32, String>("i32", "String");

    builder
}

// ── main ──────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let builder = make_builder();
    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    // ── 场景 1：线性管道 ──────────────────────────────────
    println!("=== Scenario 1: Linear Pipeline ===");
    println!("XML: AddOne -> MulTwo -> AddOne");
    let output = builder.build_from_str(LINEAR_XML)?;
    println!("  DAG has {} nodes", output.dag.topo_order().len());
    let result = Executor::execute(&output.dag, Box::new(3i32), &ctx).await?;
    let output = result.output.downcast_ref::<i32>().unwrap();
    println!("  Input: 3 -> Output: {output}");
    assert_eq!(*output, 9); // 3+1=4, 4*2=8, 8+1=9

    // ── 场景 2：Scatter-Gather ────────────────────────────
    println!("\n=== Scenario 2: Scatter-Gather (<clone>) ===");
    println!("XML: AddOne -> Clone -> [MulTwo, AddOne] -> (i32, i32)");
    let output = builder.build_from_str(SCATTER_GATHER_XML)?;
    println!("  DAG has {} nodes", output.dag.topo_order().len());
    let result = Executor::execute(&output.dag, Box::new(0i32), &ctx).await?;
    let output = result.output.downcast_ref::<(i32, i32)>().unwrap();
    println!("  Input: 0 -> Output: {:?}", output);
    assert_eq!(*output, (2, 2)); // 0+1=1, [1*2=2, 1+1=2]

    // ── 场景 3：Dispatch + ProductJoin（属性语法）─────────
    println!("\n=== Scenario 3: Dispatch + ProductJoin (attribute syntax) ===");
    println!("XML: <node dispatch=\"split_tuple\" dispatch-count=\"2\"/>");
    println!("     <node join=\"str_tuple2_join\"/>");
    let output = builder.build_from_str(DISPATCH_JOIN_XML)?;
    println!("  DAG has {} nodes (including synthetics)", output.dag.topo_order().len());
    let result = Executor::execute(&output.dag, Box::new(5i32), &ctx).await?;
    let output = result.output.downcast_ref::<String>().unwrap();
    println!("  Input: 5 -> Output: \"{output}\"");
    assert_eq!(*output, "Report: i=10, N=5");

    // ── 场景 4a：SumMatch Ok 路径（属性语法）─────────────
    println!("\n=== Scenario 4a: SumMatch Ok Branch (attribute syntax) ===");
    println!("XML: <node sum-match=\"i32/String\"/>");
    let output = builder.build_from_str(SUMMATCH_OK_XML)?;
    let result = Executor::execute(&output.dag, Box::new(7i32), &ctx).await?;
    let output = result.output.downcast_ref::<String>().unwrap();
    println!("  Input: 7 -> OkResult -> Ok(21) -> \"{output}\"");
    assert_eq!(*output, "ok: 21");

    // ── 场景 4b：SumMatch Err 路径 ────────────────────────
    println!("\n=== Scenario 4b: SumMatch Err Branch ===");
    let output = builder.build_from_str(SUMMATCH_ERR_XML)?;
    let result = Executor::execute(&output.dag, Box::new(5i32), &ctx).await?;
    let output = result.output.downcast_ref::<String>().unwrap();
    println!("  Input: 5 -> ErrResult -> Err(\"bad: 5\") -> \"{output}\"");
    assert_eq!(*output, "err: bad: 5");

    // ── 场景 5：Reshape（属性语法）────────────────────────
    println!("\n=== Scenario 5: Reshape (attribute syntax) ===");
    println!("XML: <connect from=\"src\" to=\"dst\" reshape=\"pair_and_sum\"/>");
    let output = builder.build_from_str(RESHAPE_XML)?;
    println!("  DAG has {} nodes (including synthetic reshape)", output.dag.topo_order().len());
    let result = Executor::execute(&output.dag, Box::new(3i32), &ctx).await?;
    let output = result.output.downcast_ref::<i32>().unwrap();
    println!("  Input: 3 -> MakePair(4,6) -> Reshape((4,6),10) -> SumAll = {output}");
    assert_eq!(*output, 20); // 4+6+10=20

    // ── 场景 6：Conditional 条件分支 ──────────────────────
    println!("\n=== Scenario 6: Conditional Branch ===");
    println!("XML: <conditional implementation=\"is_positive\"/>");
    println!("Note: Conditional passes its bool output to downstream branches.");
    let output = builder.build_from_str(CONDITIONAL_XML)?;
    let result = Executor::execute(&output.dag, Box::new(7i32), &ctx).await?;
    let output = result.output.downcast_ref::<i32>().unwrap();
    println!("  Input: 7 -> IsPositive(true) -> OnPositive -> {output}");
    assert_eq!(*output, 42); // 7>0 → true → OnPositive(42)

    println!("\nAll 6 scenarios passed.");
    Ok(())
}
