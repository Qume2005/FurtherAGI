//! # OpenAI LLM 服务
//!
//! 通过 OpenAI Chat Completions API 调用大模型。

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::{ChatMessage, LlmError, LlmRequest, LlmResponse, LlmService, ToolDefinition, Usage};

/// OpenAI API 服务。
pub struct OpenAiService {
    api_key: String,
    base_url: String,
    default_model: String,
    client: Client,
}

impl OpenAiService {
    pub fn new(api_key: impl Into<String>, default_model: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: "https://api.openai.com/v1".to_string(),
            default_model: default_model.into(),
            client: Client::new(),
        }
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }
}

// -- OpenAI request/response types --

#[derive(Serialize)]
struct OpenAiRequest {
    model: String,
    messages: Vec<OpenAiMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<OpenAiTool>>,
}

#[derive(Serialize, Deserialize)]
struct OpenAiMessage {
    role: String,
    content: String,
}

#[derive(Serialize)]
struct OpenAiTool {
    r#type: String,
    function: OpenAiFunction,
}

#[derive(Serialize)]
struct OpenAiFunction {
    name: String,
    description: String,
    parameters: serde_json::Value,
}

#[derive(Deserialize)]
struct OpenAiResponse {
    choices: Vec<OpenAiChoice>,
    usage: Option<OpenAiUsage>,
}

#[derive(Deserialize)]
struct OpenAiChoice {
    message: OpenAiResponseMessage,
}

#[derive(Deserialize)]
struct OpenAiResponseMessage {
    content: Option<String>,
    tool_calls: Option<Vec<OpenAiToolCall>>,
}

#[derive(Deserialize)]
struct OpenAiToolCall {
    id: String,
    function: OpenAiFunctionCall,
}

#[derive(Deserialize)]
struct OpenAiFunctionCall {
    name: String,
    arguments: String,
}

#[derive(Deserialize)]
struct OpenAiUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

#[derive(Deserialize)]
struct OpenAiError {
    error: OpenAiErrorDetail,
}

#[derive(Deserialize)]
struct OpenAiErrorDetail {
    message: String,
}

fn to_openai_messages(messages: &[ChatMessage]) -> Vec<OpenAiMessage> {
    messages
        .iter()
        .map(|m| OpenAiMessage {
            role: match m.role {
                super::ChatRole::System => "system".to_string(),
                super::ChatRole::User => "user".to_string(),
                super::ChatRole::Assistant => "assistant".to_string(),
                super::ChatRole::Tool => "tool".to_string(),
            },
            content: m.content.clone(),
        })
        .collect()
}

#[async_trait]
impl LlmService for OpenAiService {
    async fn complete(&self, request: LlmRequest) -> Result<LlmResponse, LlmError> {
        self.complete_with_tools(request, vec![]).await
    }

    async fn complete_with_tools(
        &self,
        request: LlmRequest,
        tools: Vec<ToolDefinition>,
    ) -> Result<LlmResponse, LlmError> {
        let openai_tools = if tools.is_empty() {
            None
        } else {
            Some(
                tools
                    .into_iter()
                    .map(|t| OpenAiTool {
                        r#type: "function".to_string(),
                        function: OpenAiFunction {
                            name: t.name,
                            description: t.description,
                            parameters: t.parameters,
                        },
                    })
                    .collect(),
            )
        };

        let body = OpenAiRequest {
            model: request.model.unwrap_or_else(|| self.default_model.clone()),
            messages: to_openai_messages(&request.messages),
            temperature: request.temperature,
            max_tokens: request.max_tokens,
            tools: openai_tools,
        };

        let resp = self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&body)
            .send()
            .await
            .map_err(|e| LlmError::Http(e.to_string()))?;

        let status = resp.status().as_u16();
        let text = resp
            .text()
            .await
            .map_err(|e| LlmError::Http(e.to_string()))?;

        if status != 200 {
            let msg = serde_json::from_str::<OpenAiError>(&text)
                .map(|e| e.error.message)
                .unwrap_or_else(|_| text.clone());
            return Err(LlmError::Api { status, message: msg });
        }

        let parsed: OpenAiResponse = serde_json::from_str(&text)
            .map_err(|e| LlmError::Parse(format!("{e}: {}", &text[..text.len().min(200)])))?;

        let choice = parsed.choices.into_iter().next();
        let (content, tool_calls) = match choice {
            Some(c) => (
                c.message.content.unwrap_or_default(),
                c.message.tool_calls.map(|calls| {
                    calls
                        .into_iter()
                        .map(|tc| super::ToolCall {
                            id: tc.id,
                            name: tc.function.name,
                            arguments: tc.function.arguments,
                        })
                        .collect()
                }),
            ),
            None => (String::new(), None),
        };

        Ok(LlmResponse {
            content,
            tool_calls,
            usage: parsed.usage.map(|u| Usage {
                prompt_tokens: u.prompt_tokens,
                completion_tokens: u.completion_tokens,
                total_tokens: u.total_tokens,
            }),
        })
    }
}
