//! # Predicate 服务
//!
//! 条件判断原语：将 `Fn(&T) -> bool` 闭包封装为可组合的服务。
//!
//! 这是代码级服务层（Layer 1），**不实现** [`Workflow`](crate::workflow::definition::Workflow) trait。

use std::marker::PhantomData;

/// 条件判断服务：包装 `Fn(&T) -> bool` 闭包。
pub struct PredicateFn<T, P> {
    predicate: P,
    _marker: PhantomData<fn() -> T>,
}

impl<T, P> PredicateFn<T, P>
where
    P: Fn(&T) -> bool,
{
    /// 创建新的判断服务。
    pub fn new(predicate: P) -> Self {
        Self {
            predicate,
            _marker: PhantomData,
        }
    }

    /// 对输入进行判断。
    pub fn check(&self, input: &T) -> bool {
        (self.predicate)(input)
    }
}
