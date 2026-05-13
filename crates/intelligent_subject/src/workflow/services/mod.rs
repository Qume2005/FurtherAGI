//! # 服务层（Layer 1）
//!
//! 提供不可再分的原子能力，**不实现** [`Workflow`](crate::workflow::definition::Workflow) trait。
//! Builtin workflow 层（Layer 2）使用这些服务组合出有具体语义的 Workflow 实现。
//!
//! ## 可用服务
//!
//! | 服务 | 功能 |
//! |------|------|
//! | [`MapFn`] | 同步映射 `Fn(I) → O` |
//! | [`PredicateFn`] | 条件判断 `Fn(&T) → bool` |
//! | [`Identity`] | 透传 |
//! | [`Constant`] | 固定值输出 |
//! | [`LogService`] | tracing 日志 |
//! | [`DelayService`] | 异步延迟 |
//! | [`StateCarrier`] | 共享状态挂载 |

pub mod constant;
pub mod delay;
pub mod identity;
pub mod log;
pub mod map;
pub mod predicate;
pub mod state;

#[cfg(feature = "llm")]
pub mod llm;

pub use constant::Constant;
pub use delay::DelayService;
pub use identity::Identity;
pub use log::LogService;
pub use map::MapFn;
pub use predicate::PredicateFn;
pub use state::StateCarrier;
