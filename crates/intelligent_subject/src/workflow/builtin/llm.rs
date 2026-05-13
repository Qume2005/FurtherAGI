//! # LLM Builtin Workflow
//!
//! 通过 LLM 服务调用大模型的 builtin workflow。
//! 输出 `Result<LlmResponse, LlmError>` — 和类型，可接入 SumMatch 节点处理成功/失败。

use std::sync::Arc;

use async_trait::async_trait;

use crate::workflow::definition::Workflow;
use crate::workflow::error::WorkflowError;
use crate::workflow::model::ExecutionContext;
use crate::workflow::services::llm::{
    ChatMessage, ChatRole, LlmError, LlmRequest, LlmResponse, LlmService, ToolDefinition,
};

/// LLM 聊天完成工作流。
///
/// 持有有状态的 [`LlmService`]（通过 `Arc` 共享）。
/// 输入 [`LlmRequest`]，输出 `Result<LlmResponse, LlmError>`。
///
/// # 示例
///
/// ```rust,ignore
/// let service = Arc::new(OpenAiService::new("sk-...", "gpt-4o"));
/// let wf = LlmComplete::new(service, Some("You are a helpful assistant.".into()));
///
/// // 在 DAG 中使用，输出接入 SumMatch 处理错误：
/// // ... → LlmComplete → SumMatch<Result<LlmResponse, LlmError>>
/// //                        ├─ "ok"  → LlmResponse (成功)
/// //                        └─ "err" → LlmError (失败)
/// ```
pub struct LlmComplete {
    service: Arc<dyn LlmService>,
    system_prompt: Option<String>,
}

impl LlmComplete {
    pub fn new(service: Arc<dyn LlmService>, system_prompt: Option<String>) -> Self {
        Self {
            service,
            system_prompt,
        }
    }
}

#[async_trait]
impl Workflow<LlmRequest, Result<LlmResponse, LlmError>> for LlmComplete {
    fn name(&self) -> &str {
        "llm_complete"
    }

    async fn execute(
        &self,
        mut input: LlmRequest,
        _ctx: &ExecutionContext,
    ) -> Result<Result<LlmResponse, LlmError>, WorkflowError> {
        // Prepend system prompt if set.
        if let Some(ref prompt) = self.system_prompt {
            input.messages.insert(
                0,
                ChatMessage {
                    role: ChatRole::System,
                    content: prompt.clone(),
                },
            );
        }
        Ok(self.service.complete(input).await)
    }
}

/// LLM 带工具调用的工作流。
pub struct LlmCompleteWithTools {
    service: Arc<dyn LlmService>,
    system_prompt: Option<String>,
    tools: Vec<ToolDefinition>,
}

impl LlmCompleteWithTools {
    pub fn new(
        service: Arc<dyn LlmService>,
        system_prompt: Option<String>,
        tools: Vec<ToolDefinition>,
    ) -> Self {
        Self {
            service,
            system_prompt,
            tools,
        }
    }
}

#[async_trait]
impl Workflow<LlmRequest, Result<LlmResponse, LlmError>> for LlmCompleteWithTools {
    fn name(&self) -> &str {
        "llm_complete_with_tools"
    }

    async fn execute(
        &self,
        mut input: LlmRequest,
        _ctx: &ExecutionContext,
    ) -> Result<Result<LlmResponse, LlmError>, WorkflowError> {
        if let Some(ref prompt) = self.system_prompt {
            input.messages.insert(
                0,
                ChatMessage {
                    role: ChatRole::System,
                    content: prompt.clone(),
                },
            );
        }
        Ok(self
            .service
            .complete_with_tools(input, self.tools.clone())
            .await)
    }
}
