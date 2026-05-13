//! # Identity 服务
//!
//! 透传原语：原样返回输入值。
//!
//! 这是代码级服务层（Layer 1），**不实现** [`Workflow`](crate::workflow::definition::Workflow) trait。

use std::marker::PhantomData;

/// 透传服务：原样返回输入。
pub struct Identity<T>(PhantomData<fn() -> T>);

impl<T> Identity<T> {
    /// 创建新的透传服务。
    pub fn new() -> Self {
        Self(PhantomData)
    }

    /// 原样返回输入。
    pub fn passthrough(&self, input: T) -> T {
        input
    }
}

impl<T> Default for Identity<T> {
    fn default() -> Self {
        Self::new()
    }
}
