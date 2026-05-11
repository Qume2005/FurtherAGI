use autonomous::workflow::dag::DagBuilder;
use autonomous::workflow::error::WorkflowError;
use autonomous::workflow::executor::Executor;
use autonomous::workflow::traits::from_fn;
use autonomous::workflow::types::ExecutionContext;
use autonomous::workflow::workflow_manager::WorkflowManager;
use autonomous::workflow::platform::NullPlatform;
use std::sync::Arc;

use autonomous::workflow::types::State;

fn make_ctx() -> ExecutionContext {
    ExecutionContext {
        state: Arc::new(State::new()),
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

/// Broadcast: one value fans out to multiple independent branches.
#[tokio::test]
async fn e2e_broadcast_parallel_branches() {
    let mut builder = DagBuilder::new();
    let src = builder.add("src", |input: i32| async move { Ok::<i32, WorkflowError>(input + 1) });
    let bc = builder.add_broadcast::<i32>();
    let left = builder.add("left", |input: i32| async move { Ok::<i32, WorkflowError>(input * 2) });
    let right = builder.add("right", |input: i32| async move { Ok::<i32, WorkflowError>(input + 1) });

    builder.connect(src, bc).unwrap();
    builder.connect(bc, left).unwrap();
    builder.connect(bc, right).unwrap();
    builder.set_entry(src).unwrap();
    builder.set_exit(left).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(0i32), &ctx).await.unwrap();
    // src: AddOne(0)=1, broadcast 1 -> left: MulTwo(1)=2
    assert_eq!(*result.output.downcast_ref::<i32>().unwrap(), 2);
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

/// Error handler: a node fails, the paired error handler recovers with a default value.
#[tokio::test]
async fn e2e_error_handler_with_downstream() {
    let mut builder = DagBuilder::new();

    let fail = builder.add("fail_if_neg", |input: i32| async move {
        if input < 0 {
            Err(WorkflowError::ValidationError("value is negative".into()))
        } else {
            Ok::<i32, WorkflowError>(input)
        }
    });
    let _handler = builder.add_error_handler(
        fail,
        from_fn("default_recovery", |_input: String, _ctx: &ExecutionContext| async move {
            Ok::<i32, WorkflowError>(0)
        }),
    ).unwrap();
    let downstream = builder.add("add1", |input: i32| async move { Ok::<i32, WorkflowError>(input + 1) });

    builder.connect(fail, downstream).unwrap();
    builder.set_entry(fail).unwrap();
    builder.set_exit(downstream).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();

    // Negative: fail → recovery(0) → AddOne(0)=1
    let result = Executor::execute(&dag, Box::new(-5i32), &ctx).await.unwrap();
    assert_eq!(*result.output.downcast_ref::<i32>().unwrap(), 1);

    // Positive: pass → AddOne(5)=6
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await.unwrap();
    assert_eq!(*result.output.downcast_ref::<i32>().unwrap(), 6);
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

/// WorkflowManager with state: verify State is accessible during execution.
#[tokio::test]
async fn e2e_stateful_workflow() {
    let state = Arc::new(State::new());
    let ctx = ExecutionContext { state: state.clone(), platform: Arc::new(NullPlatform::new()) };

    let mgr = WorkflowManager::new();
    mgr.add_with_ctx("stateful", |input: i32, ctx: &ExecutionContext| {
        let prev = ctx.state.get::<i32>("accumulator").unwrap_or(0);
        let new_val = prev + input;
        ctx.state.set("accumulator", new_val);
        async move { Ok::<i32, WorkflowError>(new_val) }
    }).unwrap();

    let r1: i32 = mgr.execute_typed("stateful", 10, &ctx).await.unwrap();
    assert_eq!(r1, 10);

    let r2: i32 = mgr.execute_typed("stateful", 20, &ctx).await.unwrap();
    assert_eq!(r2, 30);

    assert_eq!(state.get::<i32>("accumulator"), Some(30));
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
