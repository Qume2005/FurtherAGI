use super::*;
use crate::workflow::dag::{DagBuilder, make_clone_fn, ProductJoinFn};
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
async fn scatter_gather() {
    // AddOne(0)=1 → ScatterGather[MulTwo, AddOne] → (2, 2)
    let mut builder = DagBuilder::new();
    let src = builder.add_workflow("add1", into_erased(AddOne));
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
    let output: &(i32, i32) = result.output.downcast_ref::<(i32, i32)>().unwrap();
    // AddOne(0)=1, branches: MulTwo(1)=2, AddOne(1)=2 → (2, 2)
    assert_eq!(*output, (2, 2));
}

#[tokio::test]
async fn sum_match_ok_branch() {
    // Result<i32, String> → SumMatch → ok(i32) handler
    let mut builder = DagBuilder::new();
    let src = builder.add("result_src", |input: i32| async move {
        Ok::<Result<i32, String>, WorkflowError>(Ok(input * 2))
    });
    let sm = builder.add_sum_match::<i32, String>();
    let ok_path = builder.add("ok_path", |input: i32| async move {
        Ok::<String, WorkflowError>(format!("ok: {input}"))
    });
    let err_path = builder.add("err_path", |input: String| async move {
        Ok::<String, WorkflowError>(format!("err: {input}"))
    });

    builder.connect(src, sm).unwrap();
    builder.connect_labeled(sm, ok_path, "ok").unwrap();
    builder.connect_labeled(sm, err_path, "err").unwrap();
    builder.set_entry(src).unwrap();
    builder.set_exit(ok_path).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await.unwrap();
    let output: &String = result.output.downcast_ref::<String>().unwrap();
    assert_eq!(*output, "ok: 10");
}

#[tokio::test]
async fn sum_match_err_branch() {
    // Result<i32, String> → SumMatch → err(String) handler
    let mut builder = DagBuilder::new();
    let src = builder.add("result_src", |_input: i32| async move {
        Ok::<Result<i32, String>, WorkflowError>(Err("failure".into()))
    });
    let sm = builder.add_sum_match::<i32, String>();
    let ok_path = builder.add("ok_path", |input: i32| async move {
        Ok::<String, WorkflowError>(format!("ok: {input}"))
    });
    let err_path = builder.add("err_path", |input: String| async move {
        Ok::<String, WorkflowError>(format!("err: {input}"))
    });

    builder.connect(src, sm).unwrap();
    builder.connect_labeled(sm, ok_path, "ok").unwrap();
    builder.connect_labeled(sm, err_path, "err").unwrap();
    builder.set_entry(src).unwrap();
    builder.set_exit(err_path).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await.unwrap();
    let output: &String = result.output.downcast_ref::<String>().unwrap();
    assert_eq!(*output, "err: failure");
}

#[tokio::test]
async fn reshape_execution() {
    let mut builder = DagBuilder::new();
    let src = builder.add("src", |input: i32| async move {
        Ok::<(i32, i32), WorkflowError>((input, input * 2))
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
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(3i32), &ctx).await.unwrap();
    let output = result.output.downcast_ref::<i32>().unwrap();
    // src: (3, 6), reshape: ((3, 6), 9), dst: 3+6+9=18
    assert_eq!(*output, 18);
}

#[tokio::test]
async fn dispatch_execution() {
    let mut builder = DagBuilder::new();
    let src = builder.add("src", |input: i32| async move {
        Ok::<(i32, i32), WorkflowError>((input, input * 10))
    });
    let dispatch = builder.add_dispatch(
        2,
        Box::new(|input| {
            let (a, b) = *input.downcast_ref::<(i32, i32)>().unwrap();
            vec![Box::new(a) as Box<dyn Any + Send + Sync>, Box::new(b)]
        }),
    );
    let left = builder.add("left", |input: i32| async move {
        Ok::<i32, WorkflowError>(input + 1)
    });
    let right = builder.add("right", |input: i32| async move {
        Ok::<i32, WorkflowError>(input * 2)
    });

    builder.connect(src, dispatch).unwrap();
    builder.connect(dispatch, left).unwrap();
    builder.connect(dispatch, right).unwrap();
    builder.set_entry(src).unwrap();
    builder.set_exit(right).unwrap();

    let dag = builder.build().unwrap();
    let ctx = make_ctx();
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await.unwrap();
    let output = result.output.downcast_ref::<i32>().unwrap();
    // src: (5, 50), dispatch: [5, 50], right: 50*2=100
    assert_eq!(*output, 100);
}