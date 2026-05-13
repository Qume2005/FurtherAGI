//! **DEPRECATED**: 此模块已拆分为 `services`（Layer 1）和 `builtin`（Layer 2）。
//!
//! - 原子能力（Map、Predicate 等）→ [`services`](crate::workflow::services)
//! - 预构建工作流（AddOne、IsPositive 等）→ [`builtin`](crate::workflow::builtin)
//!
//! 此模块为向后兼容保留，所有类型均标记为 deprecated。

use std::sync::Arc;

use crate::workflow::model::StateStore;

#[deprecated(
    since = "0.2.0",
    note = "Use `workflow::services::MapFn` instead. MapFn does not implement Workflow trait."
)]
pub use crate::workflow::services::MapFn as Map;

#[deprecated(
    since = "0.2.0",
    note = "Use `workflow::services::PredicateFn` instead. PredicateFn does not implement Workflow trait."
)]
pub use crate::workflow::services::PredicateFn as Predicate;

#[deprecated(
    since = "0.2.0",
    note = "Use `workflow::services::Identity` instead. Identity does not implement Workflow trait."
)]
pub use crate::workflow::services::Identity;

#[deprecated(
    since = "0.2.0",
    note = "Use `workflow::services::Constant` instead. Constant does not implement Workflow trait."
)]
pub use crate::workflow::services::Constant;

#[deprecated(
    since = "0.2.0",
    note = "Use `workflow::services::LogService` instead. LogService does not implement Workflow trait."
)]
pub use crate::workflow::services::LogService as Log;

#[deprecated(
    since = "0.2.0",
    note = "Use `workflow::services::DelayService` instead. DelayService does not implement Workflow trait."
)]
pub use crate::workflow::services::DelayService as Delay;

#[deprecated(
    since = "0.2.0",
    note = "Use `workflow::services::StateCarrier` instead. StateCarrier does not implement Workflow trait."
)]
pub use crate::workflow::services::StateCarrier as StateNode;

/// **DEPRECATED**: Use `workflow::services::StateCarrier` + `workflow::definition::into_erased`.
#[deprecated(
    since = "0.2.0",
    note = "Use `services::StateCarrier` + `definition::into_erased` instead."
)]
#[allow(deprecated)]
pub fn state_node<T: Send + Sync + 'static>(
    store: Arc<StateStore>,
) -> Box<dyn crate::workflow::definition::ErasedWorkflow> {
    // StateCarrier doesn't implement Workflow, so we use from_fn to create a passthrough
    crate::workflow::definition::from_fn(
        "state",
        move |_input: T, _ctx: &crate::workflow::model::ExecutionContext| {
            let _store = store.clone();
            async move {
                // The store is kept alive via the closure capture.
                // Users should capture Arc<StateStore> separately in other closures
                // to actually read/write state.
                Ok::<T, crate::workflow::error::WorkflowError>(_input)
            }
        },
    )
}
