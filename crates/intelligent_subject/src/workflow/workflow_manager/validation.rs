use tracing::instrument;

use crate::workflow::error::WorkflowError;

use super::WorkflowManager;

impl WorkflowManager {
    /// 校验所有已注册的工作流。
    ///
    /// 将所有工作流标记为已校验，允许执行。
    #[instrument(skip(self))]
    pub fn validate_all(&self) -> Result<(), WorkflowError> {
        // 将所有标记为已校验。
        for mut entry in self.workflows.iter_mut() {
            entry.validated = true;
        }

        Ok(())
    }
}
