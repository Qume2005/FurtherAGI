use super::*;
use crate::workflow::definition::{into_erased, Workflow};
use crate::workflow::model::ExecutionContext;
use async_trait::async_trait;
use std::any::TypeId;

struct AddOne;
#[async_trait]
impl Workflow<i32, i32> for AddOne {
    fn name(&self) -> &str { "add_one" }
    async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
        Ok(input + 1)
    }
}

struct MulTwo;
#[async_trait]
impl Workflow<i32, i32> for MulTwo {
    fn name(&self) -> &str { "mul_two" }
    async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
        Ok(input * 2)
    }
}

struct IsPositive;
#[async_trait]
impl Workflow<i32, bool> for IsPositive {
    fn name(&self) -> &str { "is_positive" }
    async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
        Ok(input > 0)
    }
}

#[test]
fn linear_dag() {
    let mut builder = DagBuilder::new();
    let a = builder.add_workflow("add_one", into_erased(AddOne));
    let b = builder.add_workflow("mul_two", into_erased(MulTwo));
    let c = builder.add_workflow("add_one_2", into_erased(AddOne));

    builder.connect(a, b).unwrap();
    builder.connect(b, c).unwrap();
    builder.set_entry(a).unwrap();
    builder.set_exit(c).unwrap();

    let dag = builder.build().unwrap();
    assert_eq!(dag.topo_order().len(), 3);
    assert_eq!(dag.entry_node(), Some(a));
    assert_eq!(dag.exit_node(), Some(c));
    assert_eq!(dag.edges().len(), 2);
}

#[test]
fn cycle_detected() {
    let mut builder = DagBuilder::new();
    let a = builder.add_workflow("a", into_erased(AddOne));
    let b = builder.add_workflow("b", into_erased(AddOne));

    builder.connect(a, b).unwrap();
    builder.connect(b, a).unwrap();

    let result = builder.build();
    assert!(result.is_err());
    match result.unwrap_err() {
        WorkflowError::CycleDetected { nodes } => {
            assert_eq!(nodes.len(), 2);
        }
        other => panic!("expected CycleDetected, got {other:?}"),
    }
}

#[test]
fn type_mismatch_rejected() {
    let mut builder = DagBuilder::new();
    let a = builder.add_workflow("add_one", into_erased(AddOne));
    let b = builder.add_workflow("is_positive", into_erased(IsPositive));

    // a outputs i32, is_positive expects i32 -- this should succeed.
    builder.connect(a, b).unwrap();

    // Now try connecting is_positive (outputs bool) to something expecting i32.
    let c = builder.add_workflow("mul_two", into_erased(MulTwo));
    let result = builder.connect(b, c);
    assert!(result.is_err());
}

#[test]
fn scatter_gather_node() {
    use std::any::TypeId;
    use crate::workflow::dag::{make_clone_fn, ProductJoinFn};

    let mut builder = DagBuilder::new();
    let a = builder.add_workflow("add_one", into_erased(AddOne));
    let gather_fn: ProductJoinFn = Box::new(|vals| {
        let a = *vals[0].downcast_ref::<i32>().unwrap();
        let b = *vals[1].downcast_ref::<i32>().unwrap();
        Box::new((a, b))
    });
    let sg = builder.add_scatter_gather(
        TypeId::of::<i32>(),
        make_clone_fn::<i32>(),
        vec![into_erased(MulTwo), into_erased(AddOne)],
        gather_fn,
        TypeId::of::<(i32, i32)>(),
    );
    let b = builder.add("process", |input: (i32, i32)| async move {
        Ok::<i32, WorkflowError>(input.0 + input.1)
    });

    builder.connect(a, sg).unwrap();
    builder.connect(sg, b).unwrap();
    builder.set_entry(a).unwrap();
    builder.set_exit(b).unwrap();

    let dag = builder.build().unwrap();
    assert_eq!(dag.topo_order().len(), 3);
}

#[test]
fn sum_match_node() {
    let mut builder = DagBuilder::new();
    let src = builder.add("result_src", |input: i32| async move {
        Ok::<Result<i32, String>, WorkflowError>(if input > 0 { Ok(input) } else { Err("negative".into()) })
    });
    let sm = builder.add_sum_match::<i32, String>();
    let ok_handler = builder.add("ok_path", |input: i32| async move {
        Ok::<String, WorkflowError>(format!("ok: {input}"))
    });
    let err_handler = builder.add("err_path", |input: String| async move {
        Ok::<String, WorkflowError>(format!("err: {input}"))
    });

    builder.connect(src, sm).unwrap();
    builder.connect_labeled(sm, ok_handler, "ok").unwrap();
    builder.connect_labeled(sm, err_handler, "err").unwrap();
    builder.set_entry(src).unwrap();
    builder.set_exit(ok_handler).unwrap();

    let dag = builder.build().unwrap();
    assert_eq!(dag.topo_order().len(), 4);
}

#[test]
fn conditional_predicate_must_output_bool() {
    let mut builder = DagBuilder::new();
    let result = builder.add_conditional("bad_pred", into_erased(AddOne));
    assert!(result.is_err());
    match result.unwrap_err() {
        WorkflowError::ValidationError(msg) => {
            assert!(msg.contains("bool"));
        }
        other => panic!("expected ValidationError, got {other:?}"),
    }
}

#[test]
fn labeled_edge() {
    let mut builder = DagBuilder::new();
    let cond = builder.add_conditional("is_pos", into_erased(IsPositive)).unwrap();

    // Branch workflows that accept bool input
    struct BoolPass;
    #[async_trait]
    impl Workflow<bool, bool> for BoolPass {
        fn name(&self) -> &str { "bool_pass" }
        async fn execute(&self, input: bool, _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
            Ok(input)
        }
    }

    let t = builder.add_workflow("true_branch", into_erased(BoolPass));
    let f = builder.add_workflow("false_branch", into_erased(BoolPass));

    builder.connect_labeled(cond, t, "true").unwrap();
    builder.connect_labeled(cond, f, "false").unwrap();

    let dag = builder.build().unwrap();
    let outgoing = dag.outgoing(cond);
    assert_eq!(outgoing.len(), 2);
    let labels: Vec<&str> = outgoing.iter().map(|e| e.label.as_deref().unwrap()).collect();
    assert!(labels.contains(&"true"));
    assert!(labels.contains(&"false"));
}

#[test]
fn connection_node() {
    let mut builder = DagBuilder::new();
    let a = builder.add_workflow("add_one", into_erased(AddOne));
    let conn = builder.add_connection("after_add", TypeId::of::<i32>());
    let b = builder.add_workflow("mul_two", into_erased(MulTwo));

    builder.connect(a, conn).unwrap();
    builder.connect(conn, b).unwrap();
    builder.set_entry(a).unwrap();
    builder.set_exit(b).unwrap();

    let dag = builder.build().unwrap();
    assert_eq!(dag.topo_order().len(), 3);

    // Verify the connection node's kind.
    let conn_node = dag.get_node(conn).unwrap();
    assert!(matches!(conn_node.kind, NodeKind::Connection { .. }));
    if let NodeKind::Connection { label } = &conn_node.kind {
        assert_eq!(label, "after_add");
    }
}
