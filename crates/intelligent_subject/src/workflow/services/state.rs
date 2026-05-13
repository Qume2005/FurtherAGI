//! # State 服务
//!
//! 状态挂载原语：携带共享 [`StateStore`] 供跨节点状态共享。
//!
//! 这是代码级服务层（Layer 1），**不实现** [`Workflow`](crate::workflow::definition::Workflow) trait。

use std::marker::PhantomData;
use std::sync::Arc;

use crate::workflow::model::StateStore;

/// 状态挂载服务：持有共享的 `Arc<StateStore>`，透传输入值。
///
/// 服务本身不执行状态操作——用户在闭包中捕获 `Arc<StateStore>` 来读写共享状态。
pub struct StateCarrier<T> {
    store: Arc<StateStore>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> StateCarrier<T> {
    /// 创建新的状态挂载服务。
    pub fn new(store: Arc<StateStore>) -> Self {
        Self {
            store,
            _marker: PhantomData,
        }
    }

    /// 获取共享状态存储的引用。
    pub fn store(&self) -> &Arc<StateStore> {
        &self.store
    }

    /// 透传输入值（状态操作由调用方通过 `store()` 完成）。
    pub fn passthrough(&self, input: T) -> T {
        input
    }
}
