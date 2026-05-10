//! # 核心 Trait
//!
//! 定义工作流系统的 trait 架构：
//!
//! - **用户层** [`Workflow<I, O>`] — 强类型，复杂工作流实现此 trait
//! - **存储层** [`ErasedWorkflow`] — 类型擦除，DAG 和 Manager 内部使用
//!
//! # 快速开始
//!
//! ```rust
//! use autonomous::workflow::dag::DagBuilder;
//! use autonomous::workflow::types::ExecutionContext;
//! use autonomous::workflow::error::WorkflowError;
//!
//! let mut builder = DagBuilder::new();
//!
//! // 不带 ctx（常用）：
//! builder.add("builtin@Double", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input * 2)
//! });
//!
//! // 带 ctx：
//! builder.add_with_ctx("builtin@Log", |input: i32, _ctx: &ExecutionContext<'_>| async move {
//!     Ok::<i32, WorkflowError>(input)
//! });
//! ```
//!
//! # 类型擦除架构
//!
//! ```text
//! |input| async {}       ──→ add()         // 纯闭包，不需要 ctx
//! |input, ctx| async {}  ──→ add_with_ctx() // 需要 ExecutionContext
//! into_erased(Workflow)  ──→ add_erased()
//! ```
//!
//! 注册时通过 `TypeId` 校验类型兼容性，执行时 downcast 保证安全。

use std::any::{Any, TypeId};
use std::future::Future;
use std::marker::PhantomData;

use async_trait::async_trait;

use super::error::WorkflowError;
use super::types::ExecutionContext;

/// 强类型异步工作流。
///
/// 用户实现此 trait 来定义自定义工作流。`I` 是输入类型，`O` 是输出类型，
/// 两者必须满足 `Send + Sync + 'static`。
///
/// # 示例
///
/// ```rust
/// use autonomous::workflow::traits::Workflow;
/// use autonomous::workflow::error::WorkflowError;
/// use autonomous::workflow::types::ExecutionContext;
/// use async_trait::async_trait;
///
/// struct Double;
///
/// #[async_trait]
/// impl Workflow<i32, i32> for Double {
///     fn name(&self) -> &str { "double" }
///
///     async fn execute(&self, input: i32, _ctx: &ExecutionContext<'_>)
///         -> Result<i32, WorkflowError>
///     {
///         Ok(input * 2)
///     }
/// }
/// ```
#[async_trait]
pub trait Workflow<I: Send + Sync + 'static, O: Send + Sync + 'static>: Send + Sync {
    /// 人类可读的名称，用于调试和日志。
    fn name(&self) -> &str;

    /// 执行工作流，接收类型化输入，返回类型化输出。
    async fn execute(&self, input: I, ctx: &ExecutionContext<'_>) -> Result<O, WorkflowError>;
}

/// 类型擦除的工作流 trait，用于异构存储。
///
/// **用户不应直接实现此 trait。** 使用 [`into_erased`] 将
/// `Workflow<I, O>` 转换为 `Box<dyn ErasedWorkflow>`。
///
/// 内部使用 `TypeId` 在注册时校验类型兼容性，在执行时通过 downcast 恢复具体类型。
#[async_trait]
pub trait ErasedWorkflow: Send + Sync {
    /// 工作流名称。
    fn name(&self) -> &str;

    /// 输入类型的 `TypeId`。
    fn input_type_id(&self) -> TypeId;
    /// 输出类型的 `TypeId`。
    fn output_type_id(&self) -> TypeId;
    /// 输入类型名称（用于错误信息）。
    fn input_type_name(&self) -> &'static str;
    /// 输出类型名称（用于错误信息）。
    fn output_type_name(&self) -> &'static str;

    /// 以类型擦除的方式执行工作流。
    ///
    /// 调用方需保证 `input` 的实际类型与 `input_type_id()` 一致
    /// （在注册时通过 `TypeId` 校验）。
    async fn execute_erased(
        &self,
        input: Box<dyn Any + Send + Sync>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Box<dyn Any + Send + Sync>, WorkflowError>;
}

/// 内部包装器，捕获 `I` 和 `O` 类型参数以实现类型擦除。
struct WorkflowWrapper<W, I, O> {
    workflow: W,
    _marker: PhantomData<(I, O)>,
}

/// 将任何 `Workflow<I, O>` 转换为类型擦除的 `Box<dyn ErasedWorkflow>`。
///
/// 这是用户代码和内部存储之间的唯一桥梁。
///
/// # 示例
///
/// ```rust
/// use autonomous::workflow::traits::{Workflow, into_erased, ErasedWorkflow};
/// use autonomous::workflow::error::WorkflowError;
/// use autonomous::workflow::types::ExecutionContext;
/// use async_trait::async_trait;
///
/// struct Double;
/// #[async_trait]
/// impl Workflow<i32, i32> for Double {
///     fn name(&self) -> &str { "double" }
///     async fn execute(&self, input: i32, _ctx: &ExecutionContext<'_>)
///         -> Result<i32, WorkflowError> { Ok(input * 2) }
/// }
///
/// let erased: Box<dyn ErasedWorkflow> = into_erased(Double);
/// assert_eq!(erased.name(), "double");
/// ```
pub fn into_erased<I, O, W>(workflow: W) -> Box<dyn ErasedWorkflow>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    W: Workflow<I, O> + 'static,
{
    Box::new(WorkflowWrapper {
        workflow,
        _marker: PhantomData,
    })
}

/// 闭包包装器，将异步闭包适配为 [`Workflow<I, O>`]。
///
/// 用户不应直接使用此类型，使用 [`from_fn`] 即可。
struct FnWorkflow<I, O, F> {
    name: String,
    f: F,
    _marker: PhantomData<(I, O)>,
}

#[async_trait]
impl<I, O, F, Fut> Workflow<I, O> for FnWorkflow<I, O, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(I, &ExecutionContext<'_>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O, WorkflowError>> + Send,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&self, input: I, ctx: &ExecutionContext<'_>) -> Result<O, WorkflowError> {
        (self.f)(input, ctx).await
    }
}

/// 从异步闭包创建工作流。
///
/// 这是最简单的创建工作流的方式，适合绝大多数纯计算场景。
/// 对于需要多步操作（如写文件再执行命令）的复杂工作流，仍应实现 [`Workflow`] trait。
///
/// # 示例
///
/// ```rust
/// use autonomous::workflow::traits::from_fn;
/// use autonomous::workflow::types::ExecutionContext;
/// use autonomous::workflow::error::WorkflowError;
///
/// let wf = from_fn("add_one", |input: i32, _ctx: &ExecutionContext<'_>| async move {
///     Ok::<i32, WorkflowError>(input + 1)
/// });
/// assert_eq!(wf.name(), "add_one");
/// ```
pub fn from_fn<I, O, F, Fut>(name: impl Into<String>, f: F) -> Box<dyn ErasedWorkflow>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(I, &ExecutionContext<'_>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O, WorkflowError>> + Send,
{
    into_erased(FnWorkflow {
        name: name.into(),
        f,
        _marker: PhantomData,
    })
}

#[async_trait]
impl<I: Send + Sync + 'static, O: Send + Sync + 'static, W: Workflow<I, O>> ErasedWorkflow
    for WorkflowWrapper<W, I, O>
{
    fn name(&self) -> &str {
        self.workflow.name()
    }

    fn input_type_id(&self) -> TypeId {
        TypeId::of::<I>()
    }

    fn output_type_id(&self) -> TypeId {
        TypeId::of::<O>()
    }

    fn input_type_name(&self) -> &'static str {
        std::any::type_name::<I>()
    }

    fn output_type_name(&self) -> &'static str {
        std::any::type_name::<O>()
    }

    async fn execute_erased(
        &self,
        input: Box<dyn Any + Send + Sync>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Box<dyn Any + Send + Sync>, WorkflowError> {
        let typed_input = input.downcast::<I>().map_err(|_| WorkflowError::DowncastError {
            node: super::types::NodeId(0),
            expected: std::any::type_name::<I>().to_string(),
        })?;
        let result = self.workflow.execute(*typed_input, ctx).await?;
        Ok(Box::new(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::platform::NullPlatform;
    use crate::workflow::types::State;
    use std::sync::LazyLock;

    struct AddOne;
    #[async_trait]
    impl Workflow<i32, i32> for AddOne {
        fn name(&self) -> &str {
            "add_one"
        }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext<'_>) -> Result<i32, WorkflowError> {
            Ok(input + 1)
        }
    }

    struct IntToString;
    #[async_trait]
    impl Workflow<i32, String> for IntToString {
        fn name(&self) -> &str {
            "int_to_string"
        }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext<'_>) -> Result<String, WorkflowError> {
            Ok(input.to_string())
        }
    }

    static STATE: LazyLock<State> = LazyLock::new(State::new);
    static PLATFORM: LazyLock<NullPlatform> = LazyLock::new(NullPlatform::new);

    fn make_ctx() -> ExecutionContext<'static> {
        ExecutionContext {
            state: &STATE,
            platform: &*PLATFORM,
        }
    }

    #[tokio::test]
    async fn erased_type_ids() {
        let wf = into_erased(AddOne);
        assert_eq!(wf.input_type_id(), TypeId::of::<i32>());
        assert_eq!(wf.output_type_id(), TypeId::of::<i32>());
        assert_eq!(wf.name(), "add_one");
    }

    #[tokio::test]
    async fn erased_execute() {
        let wf = into_erased(AddOne);
        let ctx = make_ctx();
        let result = wf.execute_erased(Box::new(5i32), &ctx).await.unwrap();
        let output: i32 = *result.downcast::<i32>().unwrap();
        assert_eq!(output, 6);
    }

    #[tokio::test]
    async fn erased_downcast_error() {
        let wf = into_erased(AddOne);
        let ctx = make_ctx();
        let result = wf.execute_erased(Box::new("wrong type"), &ctx).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            WorkflowError::DowncastError { expected, .. } => {
                assert_eq!(expected, "i32");
            }
            other => panic!("expected DowncastError, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn erased_different_types() {
        let wf = into_erased(IntToString);
        assert_eq!(wf.input_type_id(), TypeId::of::<i32>());
        assert_eq!(wf.output_type_id(), TypeId::of::<String>());

        let ctx = make_ctx();
        let result = wf.execute_erased(Box::new(42i32), &ctx).await.unwrap();
        let output: String = *result.downcast::<String>().unwrap();
        assert_eq!(output, "42");
    }

    #[tokio::test]
    async fn from_fn_basic() {
        let wf = from_fn("add_one", |input: i32, _ctx: &ExecutionContext<'_>| async move {
            Ok::<i32, WorkflowError>(input + 1)
        });
        assert_eq!(wf.name(), "add_one");
        assert_eq!(wf.input_type_id(), TypeId::of::<i32>());
        assert_eq!(wf.output_type_id(), TypeId::of::<i32>());

        let ctx = make_ctx();
        let result = wf.execute_erased(Box::new(10i32), &ctx).await.unwrap();
        let output: i32 = *result.downcast::<i32>().unwrap();
        assert_eq!(output, 11);
    }

    #[tokio::test]
    async fn from_fn_different_types() {
        let wf = from_fn("to_string", |input: i32, _ctx: &ExecutionContext<'_>| async move {
            Ok::<String, WorkflowError>(input.to_string())
        });
        let ctx = make_ctx();
        let result = wf.execute_erased(Box::new(42i32), &ctx).await.unwrap();
        let output: String = *result.downcast::<String>().unwrap();
        assert_eq!(output, "42");
    }

}
