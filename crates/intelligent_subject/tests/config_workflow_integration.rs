//! 命名空间 XML 配置驱动工作流构建的集成测试。

use async_trait::async_trait;
use intelligent_subject::workflow::config::ConfigBuilder;
use intelligent_subject::workflow::config::WorkflowFactoryRegistry;
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::definition::{into_erased, Workflow};
use intelligent_subject::workflow::model::ExecutionContext;
use intelligent_subject::workflow::platform::NullPlatform;
use std::sync::Arc;

// ── 测试工作流 ──────────────────────────────────────────

struct AppendX;
#[async_trait]
impl Workflow<String, String> for AppendX {
    fn name(&self) -> &str { "append_x" }
    async fn execute(&self, input: String, _ctx: &ExecutionContext) -> Result<String, WorkflowError> {
        Ok(format!("{input}X"))
    }
}

struct Format;
#[async_trait]
impl Workflow<String, String> for Format {
    fn name(&self) -> &str { "format" }
    async fn execute(&self, input: String, _ctx: &ExecutionContext) -> Result<String, WorkflowError> {
        Ok(format!("Report: {input}"))
    }
}

// ── 共享上下文 ──────────────────────────────────────────

fn make_ctx() -> ExecutionContext {
    ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    }
}

fn make_registry() -> WorkflowFactoryRegistry {
    let mut reg = WorkflowFactoryRegistry::new();
    reg.register("append_x", || into_erased(AppendX));
    reg.register("format", || into_erased(Format));
    reg
}

// ── 测试用例 ───────────────────────────────────────────

#[test]
fn config_simple_plan() {
    let builder = ConfigBuilder::new(make_registry());
    let plan = builder.build_from_str(r#"
        <workflow>
          <workflow result_name="a" impl="append_x" input="hello"/>
          <end result="{a.value}"/>
        </workflow>
    "#).unwrap();
    assert_eq!(plan.topo_order.len(), 2);
}

#[tokio::test]
async fn config_linear_pipeline_execute() {
    let builder = ConfigBuilder::new(make_registry());
    let plan = builder.build_from_str(r#"
        <workflow>
          <workflow result_name="a" impl="append_x" input="hello"/>
          <workflow result_name="b" impl="append_x" input="{a.value}"/>
          <end result="{b.value}"/>
        </workflow>
    "#).unwrap();

    let ns = intelligent_subject::workflow::model::Namespace::new();
    let ctx = make_ctx();
    let result = intelligent_subject::workflow::executor::Executor::execute(&plan, &ns, &ctx).await.unwrap();
    let val = result.downcast_ref::<String>().unwrap();
    // "hello" → "helloX" → "helloXX"
    assert_eq!(*val, "helloXX");
}

#[tokio::test]
async fn config_independent_nodes_execute() {
    let builder = ConfigBuilder::new(make_registry());
    let plan = builder.build_from_str(r#"
        <workflow>
          <workflow result_name="x" impl="append_x" input="foo"/>
          <workflow result_name="y" impl="append_x" input="bar"/>
          <end result="{x.value}"/>
        </workflow>
    "#).unwrap();

    let ns = intelligent_subject::workflow::model::Namespace::new();
    let ctx = make_ctx();
    let result = intelligent_subject::workflow::executor::Executor::execute(&plan, &ns, &ctx).await.unwrap();
    let val = result.downcast_ref::<String>().unwrap();
    assert_eq!(*val, "fooX");
}

#[test]
fn config_unknown_impl_error() {
    let builder = ConfigBuilder::new(make_registry());
    let result = builder.build_from_str(r#"
        <workflow>
          <workflow result_name="a" impl="nonexistent" input="hello"/>
          <end result="{a.value}"/>
        </workflow>
    "#);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("unknown workflow impl"));
    assert!(msg.contains("nonexistent"));
}

#[test]
fn config_cycle_detected() {
    let builder = ConfigBuilder::new(make_registry());
    let result = builder.build_from_str(r#"
        <workflow>
          <workflow result_name="a" impl="append_x" input="{b.value}"/>
          <workflow result_name="b" impl="append_x" input="{a.value}"/>
          <end result="{b.value}"/>
        </workflow>
    "#);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("cycle"));
}

#[tokio::test]
async fn config_format_pipeline() {
    let builder = ConfigBuilder::new(make_registry());
    let plan = builder.build_from_str(r#"
        <workflow>
          <workflow result_name="step" impl="format" input="world"/>
          <end result="{step.value}"/>
        </workflow>
    "#).unwrap();

    let ns = intelligent_subject::workflow::model::Namespace::new();
    let ctx = make_ctx();
    let result = intelligent_subject::workflow::executor::Executor::execute(&plan, &ns, &ctx).await.unwrap();
    let val = result.downcast_ref::<String>().unwrap();
    assert_eq!(*val, "Report: world");
}

// ── If 作用域测试 ─────────────────────────────────────────

#[tokio::test]
async fn config_if_with_then_propagates_value() {
    let builder = ConfigBuilder::new(make_registry());
    let plan = builder.build_from_str(r#"
        <workflow>
          <workflow result_name="a" impl="append_x" input="hello"/>
          <if predicate="{flag.value}" result_name="msg" then="{inner.value}">
            <workflow result_name="inner" impl="append_x" input="{a.value}"/>
          </if>
          <end result="{msg}"/>
        </workflow>
    "#).unwrap();

    let ns = intelligent_subject::workflow::model::Namespace::new();
    // 预设 bool flag
    ns.set("flag.value", true);
    let ctx = make_ctx();
    let result = intelligent_subject::workflow::executor::Executor::execute(&plan, &ns, &ctx).await.unwrap();
    let val = result.downcast_ref::<String>().unwrap();
    // a = "helloX" → if true → inner = "helloXX" → msg = "helloXX"
    assert_eq!(val, "helloXX");
}

#[test]
fn config_if_scope_violation_rejected() {
    let builder = ConfigBuilder::new(make_registry());
    let result = builder.build_from_str(r#"
        <workflow>
          <workflow result_name="a" impl="append_x" input="hello"/>
          <if predicate="{a.value}">
            <workflow result_name="inner" impl="append_x" input="world"/>
          </if>
          <end result="{inner.value}"/>
        </workflow>
    "#);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("scope violation"), "expected scope violation, got: {msg}");
}
