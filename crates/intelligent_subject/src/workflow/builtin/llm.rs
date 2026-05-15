//! # LLM Builtin Workflow
//!
//! 通过 LLM 服务调用大模型的 builtin workflow。
//! 输出 `Result<LlmResponse, LlmError>` — 和类型，可在命名空间工作流中处理成功/失败。

use std::sync::Arc;

use async_trait::async_trait;

use crate::workflow::definition::Workflow;
use crate::workflow::error::WorkflowError;
use crate::workflow::model::ExecutionContext;
use crate::workflow::services::llm::{
    ChatMessage, ChatRole, LlmError, LlmRequest, LlmResponse, LlmService, ToolDefinition,
};
use crate::workflow::tool_registry::ToolRegistry;

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
/// // 在 DAG 中使用：
/// // ... → LlmComplete → 处理 Result<LlmResponse, LlmError>
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
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
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
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
            );
        }
        Ok(self
            .service
            .complete_with_tools(input, self.tools.clone())
            .await)
    }
}

/// LLM Agentic Loop — 自动执行工具调用的工作流。
///
/// 核心循环：
/// 1. 将消息和工具定义发送给 LLM
/// 2. 如果 LLM 返回 `tool_calls`，执行每个工具（通过 [`ToolRegistry`]）
/// 3. 将工具结果追加到消息历史
/// 4. 重复直到 LLM 不再调用工具（返回纯文本回复）或达到最大迭代次数
///
/// 所有工具必须是已通过 XML `<tool>` 元素配置并构建到
/// [`ToolRegistry`] 的工具。
///
/// # 示例
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use intelligent_subject::workflow::tool_registry::ToolRegistry;
///
/// // 工具注册表（通常从 ConfigBuilder 构建的 BuildOutput.tools 获取）
/// let tool_registry = Arc::new(ToolRegistry::new());
///
/// // 创建 agentic loop
/// let agent = LlmAgentLoop::new(
///     Arc::new(OpenAiService::new("sk-...", "gpt-4o")),
///     Some("You are a helpful assistant.".into()),
///     tool_registry,
///     10, // 最大迭代次数
/// );
///
/// // 执行
/// let request = LlmRequest {
///     messages: vec![ChatMessage::user("北京今天天气怎么样？")],
///     model: None,
///     temperature: None,
///     max_tokens: None,
/// };
/// let response = agent.execute(request, &ctx).await?;
/// ```
pub struct LlmAgentLoop {
    service: Arc<dyn LlmService>,
    system_prompt: Option<String>,
    tool_registry: Arc<ToolRegistry>,
    max_iterations: usize,
}

impl LlmAgentLoop {
    /// 创建 agentic loop。
    ///
    /// # 参数
    /// - `service` — LLM 服务实现
    /// - `system_prompt` — 可选的系统提示词
    /// - `tool_registry` — 从 XML `<tool>` 元素构建的工具注册表
    /// - `max_iterations` — 最大工具调用轮次（防止无限循环）
    pub fn new(
        service: Arc<dyn LlmService>,
        system_prompt: Option<String>,
        tool_registry: Arc<ToolRegistry>,
        max_iterations: usize,
    ) -> Self {
        Self {
            service,
            system_prompt,
            tool_registry,
            max_iterations,
        }
    }
}

#[async_trait]
impl Workflow<LlmRequest, Result<LlmResponse, LlmError>> for LlmAgentLoop {
    fn name(&self) -> &str {
        "llm_agent_loop"
    }

    async fn execute(
        &self,
        mut input: LlmRequest,
        ctx: &ExecutionContext,
    ) -> Result<Result<LlmResponse, LlmError>, WorkflowError> {
        // Prepend system prompt if set.
        if let Some(ref prompt) = self.system_prompt {
            input.messages.insert(
                0,
                ChatMessage {
                    role: ChatRole::System,
                    content: prompt.clone(),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
            );
        }

        let tool_defs = self.tool_registry.get_tool_definitions();
        let mut messages = input.messages;

        for _ in 0..self.max_iterations {
            let request = LlmRequest {
                messages: messages.clone(),
                model: input.model.clone(),
                temperature: input.temperature,
                max_tokens: input.max_tokens,
            };

            let response = match self
                .service
                .complete_with_tools(request, tool_defs.clone())
                .await
            {
                Ok(r) => r,
                Err(e) => return Ok(Err(e)),
            };

            match response.tool_calls {
                None => return Ok(Ok(response)),
                Some(calls) => {
                    // 追加助手消息（含工具调用）
                    messages.push(ChatMessage::assistant_with_tool_calls(
                        response.content.clone(),
                        calls.clone(),
                    ));

                    // 执行每个工具调用
                    for call in &calls {
                        let result_str = self
                            .tool_registry
                            .execute_tool(call, ctx)
                            .await?;
                        messages.push(ChatMessage::tool_result(
                            &call.id,
                            &call.name,
                            result_str,
                        ));
                    }
                }
            }
        }

        // 超过最大迭代次数
        Ok(Err(LlmError::Api {
            status: 0,
            message: format!(
                "agent loop exceeded max iterations ({})",
                self.max_iterations
            ),
        }))
    }
}
