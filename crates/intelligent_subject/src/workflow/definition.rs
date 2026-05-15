//! # 核心 Trait
//!
//! 工作流系统的 trait 架构和闭包创建工具。
//!
//! ## 两种 trait
//!
//! | Trait | 用途 | 创建方式 |
//! |-------|------|----------|
//! | [`Workflow<I, O>`] | 强类型 trait，复杂工作流实现此 trait | `struct` + `impl Workflow` |
//! | [`ErasedWorkflow`] | 类型擦除 trait，系统内部存储 | [`from_fn()`] 或 [`into_erased()`] |
//!
//! **大多数情况下，用 [`from_fn()`] 从闭包创建就够了，不需要定义结构体。**
//!
//! ## 创建方式
//!
//! ### 方式一：闭包（推荐）
//!
//! ```rust
//! use intelligent_subject::workflow::definition::from_fn;
//! use intelligent_subject::workflow::model::ExecutionContext;
//! use intelligent_subject::workflow::error::WorkflowError;
//!
//! let wf = from_fn("add_one", |input: i32, _ctx: &ExecutionContext| async move {
//!     Ok::<i32, WorkflowError>(input + 1)
//! });
//! assert_eq!(wf.name(), "add_one");
//! ```
//!
//! ### 方式二：结构体 + into_erased
//!
//! ```rust
//! use intelligent_subject::workflow::definition::{Workflow, into_erased};
//! use intelligent_subject::workflow::model::ExecutionContext;
//! use intelligent_subject::workflow::error::WorkflowError;
//! use async_trait::async_trait;
//!
//! struct Double;
//! #[async_trait]
//! impl Workflow<i32, i32> for Double {
//!     fn name(&self) -> &str { "double" }
//!     async fn execute(&self, input: i32, _ctx: &ExecutionContext)
//!         -> Result<i32, WorkflowError> { Ok(input * 2) }
//! }
//!
//! let wf = into_erased(Double);
//! assert_eq!(wf.name(), "double");
//! ```
//!
//! ## 映射规则
//!
//! 无论用哪种方式创建，类型擦除时的映射规则相同：
//!
//! - 输入：`params["input"]` → downcast 为 `I`
//! - 输出：`O` → [`NamespaceOutput::single(output)`]，字段名 `"value"`
//!
//! ## 在 WorkflowManager 中使用闭包
//!
//! ```rust
//! use intelligent_subject::workflow::workflow_manager::WorkflowManager;
//! use intelligent_subject::workflow::model::ExecutionContext;
//! use intelligent_subject::workflow::platform::NullPlatform;
//! use intelligent_subject::workflow::error::WorkflowError;
//! use std::sync::Arc;
//!
//! let mgr = WorkflowManager::new();
//!
//! // 不带 ctx（最常用）
//! mgr.add("math@Double", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input * 2)
//! }).unwrap();
//!
//! // 带 ctx
//! mgr.add_with_ctx("math@Echo", |input: i32, _ctx: &ExecutionContext| async move {
//!     Ok::<i32, WorkflowError>(input)
//! }).unwrap();
//!
//! assert!(mgr.contains("math@Double"));
//! assert!(mgr.contains("math@Echo"));
//! ```
//!
//! ## 类型擦除架构
//!
//! ```text
//! |input| async {}       ──→ WorkflowManager::add()         // 纯闭包
//! |input, ctx| async {}  ──→ WorkflowManager::add_with_ctx() // 需要 ctx
//! from_fn(closure)       ──→ PlanBuilder::add_workflow()    // 闭包 → DAG 节点
//! into_erased(Workflow)  ──→ PlanBuilder::add_workflow()    // 结构体 → DAG 节点
//! ```

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;

use super::error::WorkflowError;
use super::model::ExecutionContext;

/// 强类型异步工作流。
///
/// 用户实现此 trait 来定义自定义工作流。`I` 是输入类型，`O` 是输出类型，
/// 两者必须满足 `Send + Sync + 'static`。
///
/// # 示例
///
/// ```rust
/// use intelligent_subject::workflow::definition::Workflow;
/// use intelligent_subject::workflow::error::WorkflowError;
/// use intelligent_subject::workflow::model::ExecutionContext;
/// use async_trait::async_trait;
///
/// struct Double;
///
/// #[async_trait]
/// impl Workflow<i32, i32> for Double {
///     fn name(&self) -> &str { "double" }
///
///     async fn execute(&self, input: i32, _ctx: &ExecutionContext)
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
    async fn execute(&self, input: I, ctx: &ExecutionContext) -> Result<O, WorkflowError>;
}

/// 闭包包装器，将异步闭包适配为 [`Workflow<I, O>`]。
///
/// 用户不应直接使用此类型，使用 [`from_fn`] 即可。
struct FnWorkflow<I, O, F> {
    name: String,
    f: F,
    _marker: PhantomData<fn() -> (I, O)>,
}

#[async_trait]
impl<I, O, F, Fut> Workflow<I, O> for FnWorkflow<I, O, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(I, &ExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O, WorkflowError>> + Send,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&self, input: I, ctx: &ExecutionContext) -> Result<O, WorkflowError> {
        (self.f)(input, ctx).await
    }
}

/// 从异步闭包创建命名空间工作流。
///
/// 这是最简单的创建工作流的方式，适合绝大多数场景。
/// 闭包接收类型化输入，返回类型化输出，自动适配为 [`ErasedWorkflow`]。
///
/// 映射规则：
/// - 输入：`params["input"]` → downcast 为 `I`
/// - 输出：`O` → `NamespaceOutput::single(output)`（字段名 `"value"`）
///
/// # 示例
///
/// ```rust
/// use intelligent_subject::workflow::definition::from_fn;
/// use intelligent_subject::workflow::model::ExecutionContext;
/// use intelligent_subject::workflow::error::WorkflowError;
///
/// let wf = from_fn("add_one", |input: i32, _ctx: &ExecutionContext| async move {
///     Ok::<i32, WorkflowError>(input + 1)
/// });
/// assert_eq!(wf.name(), "add_one");
/// assert_eq!(wf.output_fields(), &["value".to_string()]);
/// ```
pub fn from_fn<I, O, F, Fut>(name: impl Into<String>, f: F) -> Box<dyn ErasedWorkflow>
where
    I: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(I, &ExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O, WorkflowError>> + Send,
{
    into_erased(FnWorkflow {
        name: name.into(),
        f,
        _marker: PhantomData,
    })
}

// ── 命名空间工作流 trait ─────────────────────────────────────────

/// 命名空间工作流的输出：多个命名字段。
///
/// 每个字段以 `(String, BoxedValue)` 形式存储，写入命名空间后
/// 可通过 `result_name.field_name` 引用。
pub struct NamespaceOutput {
    pub fields: Vec<(String, Box<dyn Any + Send + Sync>)>,
}

impl NamespaceOutput {
    /// 创建一个空的输出。
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    /// 添加一个命名字段。
    pub fn with_field(mut self, name: impl Into<String>, value: impl Any + Send + Sync + 'static) -> Self {
        self.fields.push((name.into(), Box::new(value)));
        self
    }

    /// 从单个值创建输出（字段名为 `"value"`）。
    pub fn single(value: impl Any + Send + Sync + 'static) -> Self {
        Self {
            fields: vec![("value".to_string(), Box::new(value))],
        }
    }
}

impl Default for NamespaceOutput {
    fn default() -> Self {
        Self::new()
    }
}

/// 多字段输入输出的类型擦除工作流 trait。
///
/// 所有工作流（无论是闭包创建还是结构体实现）最终都通过此 trait 存储。
///
/// - 输入：`HashMap<String, Arc<dyn Any>>` — 多个命名参数
/// - 输出：[`NamespaceOutput`] — 多个命名字段
/// - 声明 `output_fields()` 用于构建时校验引用
///
/// 通过 [`from_fn`] 或 [`into_erased`] 创建。
#[async_trait]
pub trait ErasedWorkflow: Send + Sync {
    /// 工作流名称。
    fn name(&self) -> &str;

    /// 声明输出字段名列表（用于构建时校验 `{ref}` 引用）。
    fn output_fields(&self) -> &[String];

    /// 以类型擦除的方式执行工作流。
    ///
    /// - `params`：从 XML 属性解析的参数（字面量或命名空间引用）
    /// - 返回：[`NamespaceOutput`]，包含零个或多个命名字段
    async fn execute_erased(
        &self,
        params: HashMap<String, Arc<dyn Any + Send + Sync>>,
        ctx: &ExecutionContext,
    ) -> Result<NamespaceOutput, WorkflowError>;
}

/// 内部适配器：将 `Workflow<I, O>` 包装为 `ErasedWorkflow`。
///
/// 映射规则：
/// - 输入：`params["input"]` → downcast 为 `I`
/// - 输出：`O` → `NamespaceOutput::single(output)`
struct WorkflowAdapter<W, I, O> {
    workflow: W,
    output_fields: Vec<String>,
    _marker: PhantomData<fn() -> (I, O)>,
}

// 断言 I: Clone（用于 Arc unwrap）
impl<W, I: Clone, O> WorkflowAdapter<W, I, O> {
    #[allow(dead_code)]
    fn _assert_clone() {}
}

/// 将 `Workflow<I, O>` 适配为 `Box<dyn ErasedWorkflow>`。
///
/// 输入映射为 `params["input"]`，输出映射为 `fields["value"]`。
///
/// # 示例
///
/// ```rust
/// use intelligent_subject::workflow::definition::{Workflow, into_erased, ErasedWorkflow};
/// use intelligent_subject::workflow::error::WorkflowError;
/// use intelligent_subject::workflow::model::ExecutionContext;
/// use async_trait::async_trait;
///
/// struct Double;
/// #[async_trait]
/// impl Workflow<i32, i32> for Double {
///     fn name(&self) -> &str { "double" }
///     async fn execute(&self, input: i32, _ctx: &ExecutionContext)
///         -> Result<i32, WorkflowError> { Ok(input * 2) }
/// }
///
/// let wf: Box<dyn ErasedWorkflow> = into_erased(Double);
/// assert_eq!(wf.name(), "double");
/// assert_eq!(wf.output_fields(), &["value".to_string()]);
/// ```
pub fn into_erased<I, O, W>(workflow: W) -> Box<dyn ErasedWorkflow>
where
    I: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    W: Workflow<I, O> + 'static,
{
    Box::new(WorkflowAdapter {
        workflow,
        output_fields: vec!["value".to_string()],
        _marker: PhantomData,
    })
}

#[async_trait]
impl<I, O, W> ErasedWorkflow for WorkflowAdapter<W, I, O>
where
    I: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    W: Workflow<I, O>,
{
    fn name(&self) -> &str {
        self.workflow.name()
    }

    fn output_fields(&self) -> &[String] {
        &self.output_fields
    }

    async fn execute_erased(
        &self,
        params: HashMap<String, Arc<dyn Any + Send + Sync>>,
        ctx: &ExecutionContext,
    ) -> Result<NamespaceOutput, WorkflowError> {
        let input = params
            .into_iter()
            .find(|(k, _)| k == "input")
            .map(|(_, v)| v)
            .ok_or_else(|| WorkflowError::ValidationError("missing parameter 'input'".into()))?;
        let typed_input = input.downcast::<I>().map_err(|_| WorkflowError::ValidationError(
            format!("parameter 'input' type mismatch, expected {}", std::any::type_name::<I>()),
        ))?;
        let result = self.workflow.execute((*typed_input).clone(), ctx).await?;
        Ok(NamespaceOutput::single(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::platform::NullPlatform;

    struct AddOne;
    #[async_trait]
    impl Workflow<i32, i32> for AddOne {
        fn name(&self) -> &str {
            "add_one"
        }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
            Ok(input + 1)
        }
    }

    struct IntToString;
    #[async_trait]
    impl Workflow<i32, String> for IntToString {
        fn name(&self) -> &str {
            "int_to_string"
        }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<String, WorkflowError> {
            Ok(input.to_string())
        }
    }

    fn make_ctx() -> ExecutionContext {
        ExecutionContext {
            platform: Arc::new(NullPlatform::new()),
        }
    }

    #[tokio::test]
    async fn into_erased_execute() {
        let wf = into_erased(AddOne);
        let ctx = make_ctx();
        let mut params: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
        params.insert("input".to_string(), Arc::new(5i32));
        let output = wf.execute_erased(params, &ctx).await.unwrap();
        let val = output.fields[0].1.downcast_ref::<i32>().unwrap();
        assert_eq!(*val, 6);
    }

    #[tokio::test]
    async fn into_erased_different_types() {
        let wf = into_erased(IntToString);
        let ctx = make_ctx();
        let mut params: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
        params.insert("input".to_string(), Arc::new(42i32));
        let output = wf.execute_erased(params, &ctx).await.unwrap();
        let val = output.fields[0].1.downcast_ref::<String>().unwrap();
        assert_eq!(val, "42");
    }

    #[tokio::test]
    async fn from_fn_basic() {
        let wf = from_fn("add_one", |input: i32, _ctx: &ExecutionContext| async move {
            Ok::<i32, WorkflowError>(input + 1)
        });
        assert_eq!(wf.name(), "add_one");
        assert_eq!(wf.output_fields(), &["value".to_string()]);

        let ctx = make_ctx();
        let mut params: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
        params.insert("input".to_string(), Arc::new(10i32));
        let output = wf.execute_erased(params, &ctx).await.unwrap();
        let val = output.fields[0].1.downcast_ref::<i32>().unwrap();
        assert_eq!(*val, 11);
    }

    #[tokio::test]
    async fn from_fn_different_types() {
        let wf = from_fn("to_string", |input: i32, _ctx: &ExecutionContext| async move {
            Ok::<String, WorkflowError>(input.to_string())
        });
        let ctx = make_ctx();
        let mut params: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
        params.insert("input".to_string(), Arc::new(42i32));
        let output = wf.execute_erased(params, &ctx).await.unwrap();
        let val = output.fields[0].1.downcast_ref::<String>().unwrap();
        assert_eq!(val, "42");
    }

    #[tokio::test]
    async fn missing_input_param() {
        let wf = from_fn("add_one", |input: i32, _ctx: &ExecutionContext| async move {
            Ok::<i32, WorkflowError>(input + 1)
        });
        let ctx = make_ctx();
        let params: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
        let result = wf.execute_erased(params, &ctx).await;
        assert!(result.is_err());
        let msg = result.err().unwrap().to_string();
        assert!(msg.contains("missing parameter"), "actual: {msg}");
    }
}
