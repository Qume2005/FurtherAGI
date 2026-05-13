//! # Constant 服务
//!
//! 固定值原语：忽略输入，总是返回预设值。
//!
//! 这是代码级服务层（Layer 1），**不实现** [`Workflow`](crate::workflow::definition::Workflow) trait。

use std::marker::PhantomData;

/// 固定值服务：忽略输入，总是返回预设的克隆值。
pub struct Constant<I, O> {
    value: O,
    _marker: PhantomData<fn() -> I>,
}

impl<I, O: Clone> Constant<I, O> {
    /// 创建新的固定值服务。
    pub fn new(value: O) -> Self {
        Self {
            value,
            _marker: PhantomData,
        }
    }

    /// 忽略输入，返回预设值。
    pub fn produce(&self, _input: I) -> O {
        self.value.clone()
    }

    /// 获取预设值的引用。
    pub fn value(&self) -> &O {
        &self.value
    }
}
