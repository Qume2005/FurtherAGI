//! # Delay 服务
//!
//! 延迟原语：使用 `tokio::time::sleep` 实现异步等待。
//!
//! 这是代码级服务层（Layer 1），**不实现** [`Workflow`](crate::workflow::definition::Workflow) trait。

use std::time::Duration;

/// 延迟服务：异步等待指定时长。
pub struct DelayService {
    duration: Duration,
}

impl DelayService {
    /// 创建新的延迟服务。
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }

    /// 异步等待指定时长。
    pub async fn wait(&self) {
        tokio::time::sleep(self.duration).await;
    }

    /// 获取配置的时长。
    pub fn duration(&self) -> Duration {
        self.duration
    }
}
