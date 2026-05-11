use super::*;
use crate::workflow::dag::DagBuilder;
use crate::workflow::error::WorkflowError;
use crate::workflow::definition::{into_erased, Workflow};
use crate::workflow::platform::NullPlatform;
use async_trait::async_trait;
use std::sync::Arc;

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

fn make_ctx() -> ExecutionContext {
    ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    }
}

#[tokio::test]
async fn linear_chain() {
    // AddOne(3) -> MulTwo(4) -> AddOne(9) = 10
    let mut builder = DagBuilder::new();
    let a = builder.add_workflow("add1", into_erased(AddOne));
    let b = builder.add_workflow("mul2", into_erased(MulTwo));
    let c = builder.add_workflow("add2", into_erased(AddOne));
    builder.connect(a, b).unwrap();
    builder.connect(b, c).unwrap();
    builder.set_entry(a).unwrap();
    builder.set_exit(c).unwrap();
    let dag = builder.build().unwrap();

    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(3i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*output, 9);
}

#[tokio::test]
async fn broadcast_fanout() {
    // AddOne(0) -> Broadcast -> [MulTwo, AddOne]
    let mut builder = DagBuilder::new();
    let src = builder.add_workflow("add1", into_erased(AddOne));
    let bc = builder.add_broadcast::<i32>();
    let branch_a = builder.add_workflow("mul2", into_erased(MulTwo));
    let branch_b = builder.add_workflow("add2", into_erased(AddOne));

    builder.connect(src, bc).unwrap();
    builder.connect(bc, branch_a).unwrap();
    builder.connect(bc, branch_b).unwrap();
    builder.set_entry(src).unwrap();
    builder.set_exit(branch_a).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(0i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    // branch_a: AddOne(0)=1, MulTwo(1)=2
    assert_eq!(*output, 2);
}

#[tokio::test]
async fn error_handler_recovery() {
    struct Fail;
    #[async_trait]
    impl Workflow<i32, i32> for Fail {
        fn name(&self) -> &str { "fail" }
        async fn execute(&self, _input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
            Err(WorkflowError::ValidationError("intentional failure".into()))
        }
    }

    struct ErrorHandler;
    #[async_trait]
    impl Workflow<String, i32> for ErrorHandler {
        fn name(&self) -> &str { "error_handler" }
        async fn execute(&self, _input: String, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
            Ok(-1)
        }
    }

    let mut builder = DagBuilder::new();
    let fail_node = builder.add_workflow("fail", into_erased(Fail));
    let _err_handler = builder.add_error_handler(fail_node, into_erased(ErrorHandler)).unwrap();
    builder.set_entry(fail_node).unwrap();
    builder.set_exit(fail_node).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(42i32), &ctx).await.unwrap();
    let output: &i32 = result.output.downcast_ref::<i32>().unwrap();
    assert_eq!(*output, -1);
}
