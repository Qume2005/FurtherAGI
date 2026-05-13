//! Integration tests for config-driven workflow building.

use async_trait::async_trait;
use intelligent_subject::workflow::config::{ConfigBuilder, TypeRegistry, WorkflowFactoryRegistry};
use intelligent_subject::workflow::dag::ProductJoinFn;
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::executor::Executor;
use intelligent_subject::workflow::definition::{into_erased, Workflow};
use intelligent_subject::workflow::model::ExecutionContext;
use intelligent_subject::workflow::workflow_manager::WorkflowManager;
use intelligent_subject::workflow::platform::NullPlatform;
use std::sync::Arc;

// ── Test workflows ──────────────────────────────────────────

struct AddOne;
#[async_trait]
impl Workflow<i32, i32> for AddOne {
    fn name(&self) -> &str {
        "add_one"
    }
    async fn execute(
        &self,
        input: i32,
        _ctx: &ExecutionContext,
    ) -> Result<i32, WorkflowError> {
        Ok(input + 1)
    }
}

struct MulTwo;
#[async_trait]
impl Workflow<i32, i32> for MulTwo {
    fn name(&self) -> &str {
        "mul_two"
    }
    async fn execute(
        &self,
        input: i32,
        _ctx: &ExecutionContext,
    ) -> Result<i32, WorkflowError> {
        Ok(input * 2)
    }
}

struct IsPositive;
#[async_trait]
impl Workflow<i32, bool> for IsPositive {
    fn name(&self) -> &str {
        "is_positive"
    }
    async fn execute(
        &self,
        input: i32,
        _ctx: &ExecutionContext,
    ) -> Result<bool, WorkflowError> {
        Ok(input > 0)
    }
}

struct BoolToInt;
#[async_trait]
impl Workflow<bool, i32> for BoolToInt {
    fn name(&self) -> &str {
        "bool_to_int"
    }
    async fn execute(
        &self,
        input: bool,
        _ctx: &ExecutionContext,
    ) -> Result<i32, WorkflowError> {
        Ok(if input { 100 } else { -100 })
    }
}

struct FormatOk;
#[async_trait]
impl Workflow<i32, String> for FormatOk {
    fn name(&self) -> &str {
        "format_ok"
    }
    async fn execute(
        &self,
        input: i32,
        _ctx: &ExecutionContext,
    ) -> Result<String, WorkflowError> {
        Ok(format!("ok: {input}"))
    }
}

struct FormatErr;
#[async_trait]
impl Workflow<String, String> for FormatErr {
    fn name(&self) -> &str {
        "format_err"
    }
    async fn execute(
        &self,
        input: String,
        _ctx: &ExecutionContext,
    ) -> Result<String, WorkflowError> {
        Ok(format!("err: {input}"))
    }
}

struct OkResult;
#[async_trait]
impl Workflow<i32, Result<i32, String>> for OkResult {
    fn name(&self) -> &str { "ok_result" }
    async fn execute(
        &self,
        input: i32,
        _ctx: &ExecutionContext,
    ) -> Result<Result<i32, String>, WorkflowError> {
        Ok(Ok(input * 3))
    }
}

struct ErrResult;
#[async_trait]
impl Workflow<i32, Result<i32, String>> for ErrResult {
    fn name(&self) -> &str { "err_result" }
    async fn execute(
        &self,
        input: i32,
        _ctx: &ExecutionContext,
    ) -> Result<Result<i32, String>, WorkflowError> {
        Ok(Err(format!("bad: {input}")))
    }
}

// ── Shared context ──────────────────────────────────────────

fn make_ctx() -> ExecutionContext {
    ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    }
}

// ── Helpers ─────────────────────────────────────────────────

fn make_builder() -> ConfigBuilder {
    let types = TypeRegistry::with_primitives();
    let mut workflows = WorkflowFactoryRegistry::new();
    workflows.register("add_one", || into_erased(AddOne));
    workflows.register("mul_two", || into_erased(MulTwo));
    workflows.register("is_positive", || into_erased(IsPositive));
    workflows.register("bool_to_int", || into_erased(BoolToInt));
    workflows.register("format_ok", || into_erased(FormatOk));
    workflows.register("format_err", || into_erased(FormatErr));
    workflows.register("ok_result", || into_erased(OkResult));
    workflows.register("err_result", || into_erased(ErrResult));
    let mut builder = ConfigBuilder::new(types, workflows);
    // Register a gather function for (i32, i32) output.
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
    // Register sum-match for i32 | String.
    builder.register_sum_match::<i32, String>("i32", "String");
    builder
}

// ── Tests ───────────────────────────────────────────────────

#[test]
fn config_linear_pipeline() {
    let builder = make_builder();
    let (id, dag) = builder
        .build_from_str(
            r#"
            <workflow name="pipeline" entry="a" exit="c">
              <node name="a" implementation="add_one"/>
              <node name="b" implementation="mul_two"/>
              <node name="c" implementation="add_one"/>
              <connect from="a" to="b"/>
              <connect from="b" to="c"/>
            </workflow>"#,
        )
        .unwrap();

    assert_eq!(id.as_str(), "pipeline");
    assert_eq!(dag.topo_order().len(), 3);
}

#[tokio::test]
async fn config_linear_pipeline_execute() {
    let builder = make_builder();
    let (workflow_id, dag) = builder
        .build_from_str(
            r#"
            <workflow name="pipeline" entry="a" exit="c">
              <node name="a" implementation="add_one"/>
              <node name="b" implementation="mul_two"/>
              <node name="c" implementation="add_one"/>
              <connect from="a" to="b"/>
              <connect from="b" to="c"/>
            </workflow>"#,
        )
        .unwrap();

    let mgr = WorkflowManager::new();
    mgr.register_composite(workflow_id, dag).unwrap();
    mgr.validate_all().unwrap();

    let ctx = make_ctx();
    let result: i32 = mgr
        .execute_typed("pipeline", 3, &ctx)
        .await
        .unwrap();
    // AddOne(3)=4, MulTwo(4)=8, AddOne(8)=9
    assert_eq!(result, 9);
}

#[tokio::test]
async fn config_scatter_gather() {
    let builder = make_builder();
    let (workflow_id, dag) = builder
        .build_from_str(
            r#"
            <workflow name="sg_test" entry="src" exit="sg">
              <node name="src" implementation="add_one"/>
              <clone name="sg" type="i32" output-type="(i32,i32)" gather="tuple2_i32">
                <branch implementation="mul_two"/>
                <branch implementation="add_one"/>
              </clone>
              <connect from="src" to="sg"/>
            </workflow>"#,
        )
        .unwrap();

    assert_eq!(workflow_id.as_str(), "sg_test");

    let mgr = WorkflowManager::new();
    mgr.register_composite(workflow_id, dag).unwrap();
    mgr.validate_all().unwrap();

    let ctx = make_ctx();
    let result: (i32, i32) = mgr
        .execute_typed("sg_test", 0, &ctx)
        .await
        .unwrap();
    // AddOne(0)=1, branches: MulTwo(1)=2, AddOne(1)=2 → (2, 2)
    assert_eq!(result, (2, 2));
}

#[tokio::test]
async fn config_loop_pipeline() {
    let builder = make_builder();
    let (_workflow_id, dag) = builder
        .build_from_str(
            r#"
            <workflow name="loop_test" entry="loop1" exit="loop1">
              <node name="body" implementation="add_one"/>
              <loop name="loop1" count="5" body-entry="body" body-exit="body"/>
            </workflow>"#,
        )
        .unwrap();

    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(0i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    // AddOne × 5 starting from 0 = 5
    assert_eq!(*output, 5);
}

#[tokio::test]
async fn config_conditional_pipeline() {
    let builder = make_builder();
    let (_, dag) = builder
        .build_from_str(
            r#"
            <workflow name="cond_test" entry="cond" exit="pos">
              <conditional name="cond" implementation="is_positive"/>
              <node name="pos" implementation="bool_to_int"/>
              <node name="neg" implementation="bool_to_int"/>
              <connect from="cond" to="pos" label="true"/>
              <connect from="cond" to="neg" label="false"/>
            </workflow>"#,
        )
        .unwrap();

    let ctx = make_ctx();

    // Positive input
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*output, 100);
}

#[test]
fn config_unknown_type_error() {
    let builder = make_builder();
    let result = builder.build_from_str(
        r#"
        <workflow name="bad" entry="a" exit="a">
          <clone name="a" type="NonExistentType" output-type="(i32,i32)" gather="tuple2_i32">
            <branch implementation="add_one"/>
          </clone>
        </workflow>"#,
    );
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("unknown type"));
    assert!(msg.contains("NonExistentType"));
}

#[test]
fn config_unknown_workflow_error() {
    let builder = make_builder();
    let result = builder.build_from_str(
        r#"
        <workflow name="bad" entry="a" exit="a">
          <node name="a" implementation="nonexistent"/>
        </workflow>"#,
    );
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("unknown workflow"));
    assert!(msg.contains("nonexistent"));
}

#[test]
fn config_unknown_node_reference_error() {
    let builder = make_builder();
    let result = builder.build_from_str(
        r#"
        <workflow name="bad" entry="a" exit="a">
          <node name="a" implementation="add_one"/>
          <connect from="a" to="nonexistent"/>
        </workflow>"#,
    );
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("unknown node"));
    assert!(msg.contains("nonexistent"));
}

#[test]
fn config_sub_workflow_node() {
    let builder = make_builder();
    let (id, dag) = builder
        .build_from_str(
            r#"
            <workflow name="with_sub" entry="sub" exit="sub">
              <sub-workflow name="sub" workflow="helper" input-type="i32" output-type="i32"/>
            </workflow>"#,
        )
        .unwrap();

    assert_eq!(id.as_str(), "with_sub");
    assert_eq!(dag.topo_order().len(), 1);
}

#[tokio::test]
async fn config_connection_passthrough() {
    let builder = make_builder();
    let (workflow_id, dag) = builder
        .build_from_str(
            r#"
            <workflow name="conn_test" entry="a" exit="c">
              <node name="a" implementation="add_one"/>
              <connection name="jump1" label="after_add" type="i32"/>
              <node name="b" implementation="mul_two"/>
              <connection name="jump2" label="after_mul" type="i32"/>
              <node name="c" implementation="add_one"/>
              <connect from="a" to="jump1"/>
              <connect from="jump1" to="b"/>
              <connect from="b" to="jump2"/>
              <connect from="jump2" to="c"/>
            </workflow>"#,
        )
        .unwrap();

    assert_eq!(dag.topo_order().len(), 5);

    let mgr = WorkflowManager::new();
    mgr.register_composite(workflow_id, dag).unwrap();
    mgr.validate_all().unwrap();

    let ctx = make_ctx();
    let result: i32 = mgr
        .execute_typed("conn_test", 3, &ctx)
        .await
        .unwrap();
    // AddOne(3)=4 → Connection → MulTwo(4)=8 → Connection → AddOne(8)=9
    assert_eq!(result, 9);
}

#[tokio::test]
async fn config_sum_match_ok_branch() {
    let builder = make_builder();
    let (_, dag) = builder
        .build_from_str(
            r#"
            <workflow name="sm_ok" entry="src" exit="ok_path">
              <node name="src" implementation="ok_result"/>
              <sum-match name="sm" ok-type="i32" err-type="String"/>
              <node name="ok_path" implementation="format_ok"/>
              <node name="err_path" implementation="format_err"/>
              <connect from="src" to="sm"/>
              <connect from="sm" to="ok_path" label="ok"/>
              <connect from="sm" to="err_path" label="err"/>
            </workflow>"#,
        )
        .unwrap();

    let ctx = make_ctx();
    // ok_result(7)=Ok(21), SumMatch routes to ok, format_ok(21)="ok: 21"
    let result = Executor::execute(&dag, Box::new(7i32), &ctx).await.unwrap();
    let output = result.output.downcast_ref::<String>().unwrap();
    assert_eq!(*output, "ok: 21");
}

#[tokio::test]
async fn config_sum_match_err_branch() {
    let builder = make_builder();
    let (_, dag) = builder
        .build_from_str(
            r#"
            <workflow name="sm_err" entry="src" exit="err_path">
              <node name="src" implementation="err_result"/>
              <sum-match name="sm" ok-type="i32" err-type="String"/>
              <node name="ok_path" implementation="format_ok"/>
              <node name="err_path" implementation="format_err"/>
              <connect from="src" to="sm"/>
              <connect from="sm" to="ok_path" label="ok"/>
              <connect from="sm" to="err_path" label="err"/>
            </workflow>"#,
        )
        .unwrap();

    let ctx = make_ctx();
    // err_result(5)=Err("bad: 5"), SumMatch routes to err, format_err("bad: 5")="err: bad: 5"
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await.unwrap();
    let output = result.output.downcast_ref::<String>().unwrap();
    assert_eq!(*output, "err: bad: 5");
}
