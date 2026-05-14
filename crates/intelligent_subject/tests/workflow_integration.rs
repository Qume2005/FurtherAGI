use intelligent_subject::workflow::dag::{DagBuilder, make_clone_fn, ProductJoinFn};
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::executor::Executor;
use intelligent_subject::workflow::definition::{from_fn, into_erased, Workflow};
use intelligent_subject::workflow::model::ExecutionContext;
use intelligent_subject::workflow::workflow_manager::WorkflowManager;
use intelligent_subject::workflow::platform::NullPlatform;
use std::sync::Arc;
use async_trait::async_trait;

fn make_ctx() -> ExecutionContext {
    ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    }
}

/// End-to-end: register node + composite, validate, execute via WorkflowManager.
#[tokio::test]
async fn e2e_manager_composite_workflow() {
    let mgr = WorkflowManager::new();

    mgr.add("add_one", |input: i32| async move { Ok::<i32, WorkflowError>(input + 1) }).unwrap();
    mgr.add("mul_two", |input: i32| async move { Ok::<i32, WorkflowError>(input * 2) }).unwrap();

    // Build composite: add_one -> mul_two -> add_one
    let mut builder = DagBuilder::new();
    let a = builder.add("add1", |input: i32| async move { Ok::<i32, WorkflowError>(input + 1) });
    let b = builder.add("mul2", |input: i32| async move { Ok::<i32, WorkflowError>(input * 2) });
    let c = builder.add("add2", |input: i32| async move { Ok::<i32, WorkflowError>(input + 1) });
    builder.connect(a, b).unwrap();
    builder.connect(b, c).unwrap();
    builder.set_entry(a).unwrap();
    builder.set_exit(c).unwrap();
    let dag = builder.build().unwrap();

    mgr.register_composite("pipeline", dag).unwrap();
    mgr.validate_all().unwrap();

    let ctx = make_ctx();
    let result: i32 = mgr.execute_typed("pipeline", 3, &ctx).await.unwrap();
    // AddOne(3)=4, MulTwo(4)=8, AddOne(8)=9
    assert_eq!(result, 9);
}

/// Scatter-gather: one value fans out to multiple branches, gathered into tuple.
#[tokio::test]
async fn e2e_scatter_gather() {
    struct MulTwo;
    #[async_trait]
    impl Workflow<i32, i32> for MulTwo {
        fn name(&self) -> &str { "mul_two" }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
            Ok(input * 2)
        }
    }

    let mut builder = DagBuilder::new();
    let src = builder.add("src", |input: i32| async move { Ok::<i32, WorkflowError>(input + 1) });
    let gather_fn: ProductJoinFn = Box::new(|vals| {
        let a = *vals[0].downcast_ref::<i32>().unwrap();
        let b = *vals[1].downcast_ref::<i32>().unwrap();
        Box::new((a, b))
    });
    let sg = builder.add_scatter_gather(
        std::any::TypeId::of::<i32>(),
        make_clone_fn::<i32>(),
        vec![into_erased(MulTwo), into_erased(AddOne)],
        gather_fn,
        std::any::TypeId::of::<(i32, i32)>(),
    );

    builder.connect(src, sg).unwrap();
    builder.set_entry(src).unwrap();
    builder.set_exit(sg).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(0i32), &ctx).await.unwrap();
    // src: AddOne(0)=1, branches: MulTwo(1)=2, AddOne(1)=2 → (2, 2)
    let output = result.output.downcast_ref::<(i32, i32)>().unwrap();
    assert_eq!(*output, (2, 2));
}

// Helper for e2e_scatter_gather test
struct AddOne;
#[async_trait]
impl Workflow<i32, i32> for AddOne {
    fn name(&self) -> &str { "add_one" }
    async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
        Ok(input + 1)
    }
}

/// SumMatch: Result<i32, String> → ok branch or err branch based on value.
#[tokio::test]
async fn e2e_sum_match_ok_branch() {
    let mut builder = DagBuilder::new();
    let src = builder.add("src", |input: i32| async move {
        Ok::<Result<i32, String>, WorkflowError>(Ok(input * 3))
    });
    let sm = builder.add_sum_match::<i32, String>();
    let ok_path = builder.add("ok_path", |v: i32| async move {
        Ok::<String, WorkflowError>(format!("got {v}"))
    });
    let err_path = builder.add("err_path", |v: String| async move {
        Ok::<String, WorkflowError>(format!("error: {v}"))
    });

    builder.connect(src, sm).unwrap();
    builder.connect_labeled(sm, ok_path, "ok").unwrap();
    builder.connect_labeled(sm, err_path, "err").unwrap();
    builder.set_entry(src).unwrap();
    builder.set_exit(ok_path).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(7i32), &ctx).await.unwrap();
    let output = result.output.downcast_ref::<String>().unwrap();
    assert_eq!(*output, "got 21");
}

#[tokio::test]
async fn e2e_sum_match_err_branch() {
    let mut builder = DagBuilder::new();
    let src = builder.add("src", |_input: i32| async move {
        Ok::<Result<i32, String>, WorkflowError>(Err("bad".into()))
    });
    let sm = builder.add_sum_match::<i32, String>();
    let ok_path = builder.add("ok_path", |v: i32| async move {
        Ok::<String, WorkflowError>(format!("got {v}"))
    });
    let err_path = builder.add("err_path", |v: String| async move {
        Ok::<String, WorkflowError>(format!("error: {v}"))
    });

    builder.connect(src, sm).unwrap();
    builder.connect_labeled(sm, ok_path, "ok").unwrap();
    builder.connect_labeled(sm, err_path, "err").unwrap();
    builder.set_entry(src).unwrap();
    builder.set_exit(err_path).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(7i32), &ctx).await.unwrap();
    let output = result.output.downcast_ref::<String>().unwrap();
    assert_eq!(*output, "error: bad");
}

/// Loop: execute a body subgraph N times, threading output each iteration.
#[tokio::test]
async fn e2e_loop_iteration() {
    let mut builder = DagBuilder::new();
    let body = builder.add("add_one", |input: i32| async move { Ok::<i32, WorkflowError>(input + 1) });
    let loop_node = builder.add_loop(5, body, body).unwrap();

    builder.set_entry(loop_node).unwrap();
    builder.set_exit(loop_node).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(0i32), &ctx).await.unwrap();
    assert_eq!(*result.output.downcast_ref::<i32>().unwrap(), 5);
}

/// Conditional: route based on a predicate result.
#[tokio::test]
async fn e2e_conditional_predicate() {
    let mut builder = DagBuilder::new();

    let cond = builder.add_conditional(
        "is_pos",
        from_fn("is_positive", |input: i32, _ctx: &ExecutionContext| async move {
            Ok::<bool, WorkflowError>(input > 0)
        }),
    ).unwrap();

    let true_branch = builder.add("true_branch", |input: bool| async move {
        Ok::<i32, WorkflowError>(if input { 100 } else { 0 })
    });
    let false_branch = builder.add("false_branch", |input: bool| async move {
        Ok::<i32, WorkflowError>(if input { 0 } else { -100 })
    });

    builder.connect_labeled(cond, true_branch, "true").unwrap();
    builder.connect_labeled(cond, false_branch, "false").unwrap();
    builder.set_entry(cond).unwrap();
    builder.set_exit(true_branch).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();

    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await.unwrap();
    assert_eq!(*result.output.downcast_ref::<i32>().unwrap(), 100);
}

/// Loop inside a composite workflow registered in the manager.
#[tokio::test]
async fn e2e_loop_in_manager() {
    let mgr = WorkflowManager::new();

    let mut builder = DagBuilder::new();
    let body = builder.add("add_one", |input: i32| async move { Ok::<i32, WorkflowError>(input + 1) });
    let loop_node = builder.add_loop(3, body, body).unwrap();
    builder.set_entry(loop_node).unwrap();
    builder.set_exit(loop_node).unwrap();

    let dag = builder.build().unwrap();
    mgr.register_composite("triple_add", dag).unwrap();
    mgr.validate_all().unwrap();

    let ctx = make_ctx();
    let result: i32 = mgr.execute_typed("triple_add", 0, &ctx).await.unwrap();
    assert_eq!(result, 3);
}

/// Reshape: restructure tuple nesting inside a WorkflowManager pipeline.
#[tokio::test]
async fn e2e_reshape() {
    let mut builder = DagBuilder::new();
    let src = builder.add("src", |input: i32| async move {
        Ok::<(i32, i32), WorkflowError>((input + 1, input * 2))
    });
    let reshape = builder.add_reshape(Box::new(|input| {
        let (a, b) = *input.downcast_ref::<(i32, i32)>().unwrap();
        Box::new(((a, b), a + b))
    }));
    let dst = builder.add("dst", |input: ((i32, i32), i32)| async move {
        Ok::<i32, WorkflowError>(input.0 .0 + input.0 .1 + input.1)
    });

    builder.connect(src, reshape).unwrap();
    builder.connect(reshape, dst).unwrap();
    builder.set_entry(src).unwrap();
    builder.set_exit(dst).unwrap();

    let dag = builder.build().unwrap();
    let mgr = WorkflowManager::new();
    mgr.register_composite("reshape_test", dag).unwrap();
    mgr.validate_all().unwrap();

    let ctx = make_ctx();
    let result: i32 = mgr.execute_typed("reshape_test", 3, &ctx).await.unwrap();
    // src: (4, 6), reshape: ((4, 6), 10), dst: 4+6+10=20
    assert_eq!(result, 20);
}

/// Dispatch: split a tuple into multiple paths, process independently, rejoin.
#[tokio::test]
async fn e2e_dispatch() {
    let mut builder = DagBuilder::new();
    let src = builder.add("src", |input: i32| async move {
        Ok::<(i32, String), WorkflowError>((input * 2, format!("n={}", input)))
    });
    let dispatch = builder.add_dispatch(
        2,
        Box::new(|input| {
            let (a, b) = input.downcast_ref::<(i32, String)>().unwrap();
            vec![
                Box::new(*a) as Box<dyn std::any::Any + Send + Sync>,
                Box::new(b.clone()),
            ]
        }),
    );
    let int_path = builder.add("int_path", |input: i32| async move {
        Ok::<String, WorkflowError>(format!("i={}", input))
    });
    let str_path = builder.add("str_path", |input: String| async move {
        Ok::<String, WorkflowError>(input.to_uppercase())
    });

    // Rejoin with ProductJoin
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

    builder.connect(src, dispatch).unwrap();
    builder.connect(dispatch, int_path).unwrap();
    builder.connect(dispatch, str_path).unwrap();
    builder.connect(int_path, join).unwrap();
    builder.connect(str_path, join).unwrap();
    builder.set_entry(src).unwrap();
    builder.set_exit(join).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await.unwrap();
    let output = result.output.downcast_ref::<(String, String)>().unwrap();
    // src: (10, "n=5"), dispatch: [10, "n=5"], int_path: "i=10", str_path: "N=5"
    assert_eq!(output.0, "i=10");
    assert_eq!(output.1, "N=5");
}
