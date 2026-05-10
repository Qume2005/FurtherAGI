use async_trait::async_trait;
use autonomous::workflow::dag::DagBuilder;
use autonomous::workflow::error::WorkflowError;
use autonomous::workflow::executor::Executor;
use autonomous::workflow::traits::{into_erased, Workflow};
use autonomous::workflow::types::{
    ExecutionContext, State,
};
use autonomous::workflow::workflow_manager::WorkflowManager;
use autonomous::workflow::platform::NullPlatform;
use std::sync::LazyLock;

// --- Test workflows ---

struct AddOne;
#[async_trait]
impl Workflow<i32, i32> for AddOne {
    fn name(&self) -> &str { "add_one" }
    async fn execute(&self, input: i32, _ctx: &ExecutionContext<'_>) -> Result<i32, WorkflowError> {
        Ok(input + 1)
    }
}

struct MulTwo;
#[async_trait]
impl Workflow<i32, i32> for MulTwo {
    fn name(&self) -> &str { "mul_two" }
    async fn execute(&self, input: i32, _ctx: &ExecutionContext<'_>) -> Result<i32, WorkflowError> {
        Ok(input * 2)
    }
}

struct IsPositive;
#[async_trait]
impl Workflow<i32, bool> for IsPositive {
    fn name(&self) -> &str { "is_positive" }
    async fn execute(&self, input: i32, _ctx: &ExecutionContext<'_>) -> Result<bool, WorkflowError> {
        Ok(input > 0)
    }
}

struct FailIfNegative;
#[async_trait]
impl Workflow<i32, i32> for FailIfNegative {
    fn name(&self) -> &str { "fail_if_negative" }
    async fn execute(&self, input: i32, _ctx: &ExecutionContext<'_>) -> Result<i32, WorkflowError> {
        if input < 0 {
            Err(WorkflowError::ValidationError("value is negative".into()))
        } else {
            Ok(input)
        }
    }
}

// --- Shared context ---

static STATE: LazyLock<State> = LazyLock::new(State::new);
static PLATFORM: LazyLock<NullPlatform> = LazyLock::new(NullPlatform::new);

fn make_ctx() -> ExecutionContext<'static> {
    ExecutionContext {
        state: &STATE,
        platform: &*PLATFORM,
    }
}

// --- Integration tests ---

/// End-to-end: register node + composite, validate, execute via WorkflowManager.
#[tokio::test]
async fn e2e_manager_composite_workflow() {
    let mgr = WorkflowManager::new();

    // Register nodes.
    mgr.add_erased("add_one", into_erased(AddOne))
        .unwrap();
    mgr.add_erased("mul_two", into_erased(MulTwo))
        .unwrap();

    // Build composite: add_one -> mul_two -> add_one
    let mut builder = DagBuilder::new();
    let a = builder.add_workflow("add1", into_erased(AddOne));
    let b = builder.add_workflow("mul2", into_erased(MulTwo));
    let c = builder.add_workflow("add2", into_erased(AddOne));
    builder.connect(a, b).unwrap();
    builder.connect(b, c).unwrap();
    builder.set_entry(a).unwrap();
    builder.set_exit(c).unwrap();
    let dag = builder.build().unwrap();

    mgr.register_composite("pipeline", dag)
        .unwrap();
    mgr.validate_all().unwrap();

    let ctx = make_ctx();
    let result: i32 = mgr
        .execute_typed("pipeline", 3, &ctx)
        .await
        .unwrap();
    // AddOne(3)=4, MulTwo(4)=8, AddOne(8)=9
    assert_eq!(result, 9);
}

/// Broadcast: one value fans out to multiple independent branches,
/// then merges into a single exit.
#[tokio::test]
async fn e2e_broadcast_parallel_branches() {
    let mgr = WorkflowManager::new();

    mgr.add_erased("add_one", into_erased(AddOne))
        .unwrap();
    mgr.add_erased("mul_two", into_erased(MulTwo))
        .unwrap();

    // Build: add_one -> broadcast -> [mul_two, add_one] -> (exit = mul_two branch)
    let mut builder = DagBuilder::new();
    let src = builder.add_workflow("src", into_erased(AddOne));
    let bc = builder.add_broadcast::<i32>();
    let left = builder.add_workflow("left", into_erased(MulTwo));
    let right = builder.add_workflow("right", into_erased(AddOne));

    builder.connect(src, bc).unwrap();
    builder.connect(bc, left).unwrap();
    builder.connect(bc, right).unwrap();
    builder.set_entry(src).unwrap();
    builder.set_exit(left).unwrap();

    let dag = builder.build().unwrap();

    mgr.register_composite("broadcast_pipeline", dag)
        .unwrap();
    mgr.validate_all().unwrap();

    let ctx = make_ctx();
    let result: i32 = mgr
        .execute_typed("broadcast_pipeline", 0, &ctx)
        .await
        .unwrap();
    // src: AddOne(0)=1, broadcast 1 -> left: MulTwo(1)=2
    assert_eq!(result, 2);
}

/// Loop: execute a body subgraph N times, threading output each iteration.
#[tokio::test]
async fn e2e_loop_iteration() {
    // Loop body: AddOne (add 1 each iteration)
    // Loop count: 5
    // Input: 0, expected output: 5
    let mut builder = DagBuilder::new();
    let body = builder.add_workflow("add_one", into_erased(AddOne));
    let loop_node = builder.add_loop(5, body, body).unwrap();

    builder.set_entry(loop_node).unwrap();
    builder.set_exit(loop_node).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(0i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*output, 5);
}

/// Error handler: a node fails, the paired error handler recovers with a default value.
#[tokio::test]
async fn e2e_error_handler_with_downstream() {
    let mut builder = DagBuilder::new();

    let fail = builder.add_workflow("fail_if_neg", into_erased(FailIfNegative));

    // Error handler: takes error message String, returns a default i32.
    struct DefaultRecovery;
    #[async_trait]
    impl Workflow<String, i32> for DefaultRecovery {
        fn name(&self) -> &str { "default_recovery" }
        async fn execute(&self, _input: String, _ctx: &ExecutionContext<'_>) -> Result<i32, WorkflowError> {
            Ok(0) // recover to 0
        }
    }

    let _handler = builder.add_error_handler(fail, into_erased(DefaultRecovery)).unwrap();
    let downstream = builder.add_workflow("add1", into_erased(AddOne));

    builder.connect(fail, downstream).unwrap();
    builder.set_entry(fail).unwrap();
    builder.set_exit(downstream).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();

    // Negative input triggers failure → recovery → AddOne(0)=1
    let result = Executor::execute(&dag, Box::new(-5i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*output, 1);

    // Positive input succeeds → FailIfNegative(5)=5 → AddOne(5)=6
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*output, 6);
}

/// Conditional: route based on a predicate result.
#[tokio::test]
async fn e2e_conditional_predicate() {
    let mut builder = DagBuilder::new();

    let cond = builder
        .add_conditional("is_pos", into_erased(IsPositive))
        .unwrap();

    struct BoolToInt;
    #[async_trait]
    impl Workflow<bool, i32> for BoolToInt {
        fn name(&self) -> &str { "bool_to_int" }
        async fn execute(&self, input: bool, _ctx: &ExecutionContext<'_>) -> Result<i32, WorkflowError> {
            Ok(if input { 100 } else { 0 })
        }
    }

    let true_branch = builder.add_workflow("true_branch", into_erased(BoolToInt));
    let false_branch = builder.add_workflow("false_branch", into_erased(BoolToInt));

    builder.connect_labeled(cond, true_branch, "true").unwrap();
    builder.connect_labeled(cond, false_branch, "false").unwrap();

    builder.set_entry(cond).unwrap();
    builder.set_exit(true_branch).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();

    // Input 5 → is_positive=true → true branch executes → 100
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*output, 100);
}

/// WorkflowManager with state: verify State is accessible during execution.
#[tokio::test]
async fn e2e_stateful_workflow() {
    struct ReadWriteState;
    #[async_trait]
    impl Workflow<i32, i32> for ReadWriteState {
        fn name(&self) -> &str { "stateful" }
        async fn execute(&self, input: i32, ctx: &ExecutionContext<'_>) -> Result<i32, WorkflowError> {
            // Read previous value, write new one.
            let prev = ctx.state.get::<i32>("accumulator").unwrap_or(0);
            let new_val = prev + input;
            ctx.state.set("accumulator", new_val);
            Ok(new_val)
        }
    }

    let state = State::new();
    let platform = NullPlatform::new();
    let ctx = ExecutionContext {
        state: &state,
        platform: &platform,
    };

    let mgr = WorkflowManager::new();
    mgr.add_erased("stateful", into_erased(ReadWriteState))
        .unwrap();

    // First execution: 0 + 10 = 10
    let r1: i32 = mgr
        .execute_typed("stateful", 10, &ctx)
        .await
        .unwrap();
    assert_eq!(r1, 10);

    // Second execution: 10 + 20 = 30
    let r2: i32 = mgr
        .execute_typed("stateful", 20, &ctx)
        .await
        .unwrap();
    assert_eq!(r2, 30);

    // Verify state persists.
    assert_eq!(state.get::<i32>("accumulator"), Some(30));
}

/// Loop inside a composite workflow registered in the manager.
#[tokio::test]
async fn e2e_loop_in_manager() {
    let mgr = WorkflowManager::new();

    mgr.add_erased("add_one", into_erased(AddOne))
        .unwrap();

    // Build: Loop(3 iterations of AddOne)
    let mut builder = DagBuilder::new();
    let body = builder.add_workflow("add_one", into_erased(AddOne));
    let loop_node = builder.add_loop(3, body, body).unwrap();
    builder.set_entry(loop_node).unwrap();
    builder.set_exit(loop_node).unwrap();

    let dag = builder.build().unwrap();
    mgr.register_composite("triple_add", dag)
        .unwrap();
    mgr.validate_all().unwrap();

    let ctx = make_ctx();
    let result: i32 = mgr
        .execute_typed("triple_add", 0, &ctx)
        .await
        .unwrap();
    assert_eq!(result, 3);
}
