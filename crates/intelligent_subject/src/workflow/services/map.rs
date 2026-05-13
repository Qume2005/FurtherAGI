//! # Map 服务
//!
//! 同步映射原语：将 `Fn(I) -> O` 闭包封装为可组合的服务。
//!
//! 这是代码级服务层（Layer 1），**不实现** [`Workflow`](crate::workflow::definition::Workflow) trait。
//! Builtin workflow 层可使用此服务构建有具体语义的 Workflow 实现。

use std::marker::PhantomData;

/// 同步映射服务：包装 `Fn(I) -> O` 闭包。
pub struct MapFn<I, O, F> {
    f: F,
    _marker: PhantomData<fn() -> (I, O)>,
}

impl<I, O, F> MapFn<I, O, F>
where
    F: Fn(I) -> O,
{
    /// 创建新的映射服务。
    pub fn new(f: F) -> Self {
        Self {
            f,
            _marker: PhantomData,
        }
    }

    /// 对输入应用映射函数。
    pub fn apply(&self, input: I) -> O {
        (self.f)(input)
    }
}
