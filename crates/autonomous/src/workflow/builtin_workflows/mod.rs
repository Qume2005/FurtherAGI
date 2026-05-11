//! # 内建工作流标准库
//!
//! 提供不可再分的原子工作流实现。这些工作流可以直接使用，
//! 也可以作为 DAG 中的节点。
//!
//! ## 可用工作流
//!
//! | 工作流 | 输入 → 输出 | 说明 |
//! |--------|-------------|------|
//! | [`Identity`] | `T → T` | 透传，原样返回输入 |
//! | [`Map`] | `I → O` | 应用同步闭包 `Fn(I) → O` |
//! | [`Predicate`] | `T → bool` | 应用判断闭包 `Fn(&T) → bool` |
//! | [`Constant`] | `I → O` | 忽略输入，总是返回固定值 |
//! | [`Log`] | `T → T` | 用 `tracing::info!` 记录值并透传 |
//! | [`Delay`] | `T → T` | 等待指定时长后透传 |
//!
//! ## 示例
//!
//! ```rust
//! use autonomous::workflow::builtin_workflows::{Identity, Map, Predicate};
//! use autonomous::workflow::traits::Workflow;
//! use autonomous::workflow::types::ExecutionContext;
//! use autonomous::workflow::error::WorkflowError;
//! use async_trait::async_trait;
//!
//! # #[tokio::main]
//! # async fn example(_ctx: &ExecutionContext) -> Result<(), WorkflowError> {
//! // Map: i32 → String
//! let map = Map::new(|x: i32| x.to_string());
//! // Predicate: i32 → bool
//! let pred = Predicate::new(|x: &i32| *x > 0);
//! # Ok(())
//! # }
//! ```
use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use tracing;

use crate::workflow::error::WorkflowError;
use crate::workflow::traits::Workflow;
use crate::workflow::types::ExecutionContext;

/// Identity workflow: passes input through unchanged.
pub struct Identity<T>(PhantomData<T>);

impl<T> Identity<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for Identity<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> Workflow<T, T> for Identity<T> {
    fn name(&self) -> &str {
        "identity"
    }

    async fn execute(&self, input: T, _ctx: &ExecutionContext) -> Result<T, WorkflowError> {
        Ok(input)
    }
}

/// Map workflow: applies an async function to the input.
pub struct Map<I, O, F> {
    f: F,
    _marker: PhantomData<(I, O)>,
}

impl<I, O, F> Map<I, O, F>
where
    F: Fn(I) -> O + Send + Sync,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<I: Send + Sync + 'static, O: Send + Sync + 'static, F: Fn(I) -> O + Send + Sync> Workflow<I, O>
    for Map<I, O, F>
{
    fn name(&self) -> &str {
        "map"
    }

    async fn execute(&self, input: I, _ctx: &ExecutionContext) -> Result<O, WorkflowError> {
        Ok((self.f)(input))
    }
}

/// Predicate workflow: evaluates a condition on the input, outputs `bool`.
pub struct Predicate<T, P> {
    predicate: P,
    _marker: PhantomData<T>,
}

impl<T, P> Predicate<T, P>
where
    P: Fn(&T) -> bool + Send + Sync,
{
    pub fn new(predicate: P) -> Self {
        Self {
            predicate,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static, P: Fn(&T) -> bool + Send + Sync> Workflow<T, bool> for Predicate<T, P> {
    fn name(&self) -> &str {
        "predicate"
    }

    async fn execute(&self, input: T, _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
        Ok((self.predicate)(&input))
    }
}

/// Constant workflow: always produces the same output, ignoring input.
pub struct Constant<I, O> {
    value: O,
    _marker: PhantomData<I>,
}

impl<I, O: Clone> Constant<I, O> {
    pub fn new(value: O) -> Self {
        Self {
            value,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<I: Send + Sync + 'static, O: Clone + Send + Sync + 'static> Workflow<I, O> for Constant<I, O> {
    fn name(&self) -> &str {
        "constant"
    }

    async fn execute(&self, _input: I, _ctx: &ExecutionContext) -> Result<O, WorkflowError> {
        Ok(self.value.clone())
    }
}

/// Log workflow: logs the input at info level and passes it through unchanged.
pub struct Log<T: Debug>(PhantomData<T>);

impl<T: Debug> Log<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T: Debug> Default for Log<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: Debug + Send + Sync + 'static> Workflow<T, T> for Log<T> {
    fn name(&self) -> &str {
        "log"
    }

    async fn execute(&self, input: T, _ctx: &ExecutionContext) -> Result<T, WorkflowError> {
        tracing::info!(value = ?input, type = std::any::type_name::<T>(), "workflow value");
        Ok(input)
    }
}

/// Delay workflow: sleeps for a specified duration, then passes input through.
pub struct Delay<T> {
    duration: Duration,
    _marker: PhantomData<T>,
}

impl<T> Delay<T> {
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> Workflow<T, T> for Delay<T> {
    fn name(&self) -> &str {
        "delay"
    }

    async fn execute(&self, input: T, _ctx: &ExecutionContext) -> Result<T, WorkflowError> {
        tokio::time::sleep(self.duration).await;
        Ok(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::types::State;
    use crate::workflow::platform::NullPlatform;
    use std::sync::Arc;

    fn make_ctx() -> ExecutionContext {
        ExecutionContext {
            state: Arc::new(State::new()),
            platform: Arc::new(NullPlatform::new()),
        }
    }

    #[tokio::test]
    async fn identity() {
        let wf = Identity::<i32>::new();
        let ctx = make_ctx();
        let result = wf.execute(42, &ctx).await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn map() {
        let wf = Map::new(|x: i32| x * 3);
        let ctx = make_ctx();
        let result = wf.execute(5, &ctx).await.unwrap();
        assert_eq!(result, 15);
    }

    #[tokio::test]
    async fn predicate() {
        let wf = Predicate::new(|x: &i32| *x > 0);
        let ctx = make_ctx();
        assert!(wf.execute(5, &ctx).await.unwrap());
        assert!(!wf.execute(-1, &ctx).await.unwrap());
    }

    #[tokio::test]
    async fn constant() {
        let wf = Constant::<i32, &str>::new("hello");
        let ctx = make_ctx();
        let result = wf.execute(999, &ctx).await.unwrap();
        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn log_passthrough() {
        let wf = Log::<i32>::new();
        let ctx = make_ctx();
        let result = wf.execute(42, &ctx).await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn delay_passthrough() {
        let wf = Delay::new(Duration::from_millis(1));
        let ctx = make_ctx();
        let result = wf.execute("test", &ctx).await.unwrap();
        assert_eq!(result, "test");
    }
}
