//! # Log 服务
//!
//! 日志原语：使用 `tracing::info!` 记录值。
//!
//! 这是代码级服务层（Layer 1），**不实现** [`Workflow`](crate::workflow::definition::Workflow) trait。

use std::fmt::Debug;
use std::marker::PhantomData;

/// 日志服务：使用 tracing 记录值的调试表示。
pub struct LogService<T: Debug>(PhantomData<fn() -> T>);

impl<T: Debug> LogService<T> {
    /// 创建新的日志服务。
    pub fn new() -> Self {
        Self(PhantomData)
    }

    /// 记录值的调试表示并返回原值的引用。
    pub fn log(&self, value: &T) {
        tracing::info!(value = ?value, type = std::any::type_name::<T>(), "workflow value");
    }

    /// 记录值并透传返回。
    pub fn log_and_pass(&self, value: T) -> T {
        self.log(&value);
        value
    }
}

impl<T: Debug> Default for LogService<T> {
    fn default() -> Self {
        Self::new()
    }
}
