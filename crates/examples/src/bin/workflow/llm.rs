//! LLM 工作流示例：演示三种 LLM builtin workflow。
//!
//! 演示如何：
//! - `LlmComplete` — 基础聊天补全
//! - `LlmCompleteWithTools` — 带工具定义的补全（单轮，LLM 可能返回 tool_calls）
//! - `LlmAgentLoop` — Agentic 循环（自动执行工具调用直到 LLM 返回纯文本）
//!
//! 使用 MockLlmService 演示，不发送真实 API 请求。
//! 生产环境替换为 `OpenAiService` 或 `AnthropicService` 即可。
//!
//! 运行：`cargo run -p examples --bin workflow_llm`

use std::sync::Arc;

use async_trait::async_trait;
use intelligent_subject::workflow::builtin::http::{HttpError, HttpRequest, HttpResponse};
use intelligent_subject::workflow::builtin::llm::{LlmAgentLoop, LlmComplete, LlmCompleteWithTools};
use intelligent_subject::workflow::definition::{from_fn, Workflow};
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::model::ExecutionContext;
use intelligent_subject::workflow::platform::NullPlatform;
use intelligent_subject::workflow::services::llm::{
    ChatMessage, LlmError, LlmRequest, LlmResponse, LlmService, ToolCall, ToolDefinition, Usage,
};
use intelligent_subject::workflow::tool_registry::{ToolEntry, ToolRegistry};

// ── Mock LLM Service ──────────────────────────────────────────

/// Mock LLM 服务：模拟 LLM 的多轮工具调用行为。
///
/// 行为：
/// - 第 1 轮：返回 tool_call（get_weather）
/// - 第 2 轮：返回 tool_call（get_news）
/// - 第 3 轮：返回最终文本回复
struct MockLlmService {
    call_count: std::sync::atomic::AtomicU32,
}

impl MockLlmService {
    fn new() -> Self {
        Self {
            call_count: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

#[async_trait]
impl LlmService for MockLlmService {
    async fn complete(&self, _request: LlmRequest) -> Result<LlmResponse, LlmError> {
        Ok(LlmResponse {
            content: "Hello! I am a mock LLM.".to_string(),
            tool_calls: None,
            usage: Some(Usage {
                prompt_tokens: 10,
                completion_tokens: 8,
                total_tokens: 18,
            }),
        })
    }

    async fn complete_with_tools(
        &self,
        _request: LlmRequest,
        _tools: Vec<ToolDefinition>,
    ) -> Result<LlmResponse, LlmError> {
        let count = self
            .call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        match count {
            0 => {
                // 第 1 轮：调用 get_weather
                println!("    [Mock LLM] → tool_call: get_weather(Beijing)");
                Ok(LlmResponse {
                    content: String::new(),
                    tool_calls: Some(vec![ToolCall {
                        id: "call_001".to_string(),
                        name: "get_weather".to_string(),
                        arguments: serde_json::to_string(&serde_json::json!({
                            "url": "https://api.example.com/weather?city=Beijing"
                        }))
                        .unwrap(),
                    }]),
                    usage: Some(Usage {
                        prompt_tokens: 20,
                        completion_tokens: 15,
                        total_tokens: 35,
                    }),
                })
            }
            1 => {
                // 第 2 轮：调用 get_news
                println!("    [Mock LLM] → tool_call: get_news(Beijing)");
                Ok(LlmResponse {
                    content: String::new(),
                    tool_calls: Some(vec![ToolCall {
                        id: "call_002".to_string(),
                        name: "get_news".to_string(),
                        arguments: serde_json::to_string(&serde_json::json!({
                            "url": "https://api.example.com/news?topic=Beijing"
                        }))
                        .unwrap(),
                    }]),
                    usage: Some(Usage {
                        prompt_tokens: 40,
                        completion_tokens: 12,
                        total_tokens: 52,
                    }),
                })
            }
            _ => {
                // 第 3 轮：最终回复
                println!("    [Mock LLM] → 最终文本回复");
                Ok(LlmResponse {
                    content: "Beijing is sunny, 22°C. Latest news: AI breakthrough reported."
                        .to_string(),
                    tool_calls: None,
                    usage: Some(Usage {
                        prompt_tokens: 60,
                        completion_tokens: 20,
                        total_tokens: 80,
                    }),
                })
            }
        }
    }
}

// ── Mock 工具 workflow ─────────────────────────────────────────

/// 场景 1-3 用：输出 Result<HttpResponse, HttpError>
fn mock_get_weather() -> Box<dyn intelligent_subject::workflow::definition::ErasedWorkflow> {
    from_fn(
        "mock_get_weather",
        |input: HttpRequest, _ctx: &ExecutionContext| async move {
            println!("    [Tool] get_weather: url={}", input.url);
            let response = HttpResponse {
                status: 200,
                body: serde_json::json!({"city": "Beijing", "temp": "22°C", "condition": "sunny"}),
            };
            Ok::<Result<HttpResponse, HttpError>, WorkflowError>(Ok(response))
        },
    )
}

fn mock_get_news() -> Box<dyn intelligent_subject::workflow::definition::ErasedWorkflow> {
    from_fn(
        "mock_get_news",
        |input: HttpRequest, _ctx: &ExecutionContext| async move {
            println!("    [Tool] get_news: url={}", input.url);
            let response = HttpResponse {
                status: 200,
                body: serde_json::json!({"topic": "Beijing", "headline": "AI breakthrough reported"}),
            };
            Ok::<Result<HttpResponse, HttpError>, WorkflowError>(Ok(response))
        },
    )
}

// ── 构建工具注册表 ─────────────────────────────────────────────

fn build_tool_registry() -> ToolRegistry {
    let registry = ToolRegistry::new();

    let make_entry = |name: &str,
                      description: &str,
                      workflow: Box<
        dyn intelligent_subject::workflow::definition::ErasedWorkflow,
    >| {
        ToolEntry {
            tool_def: ToolDefinition {
                name: name.to_string(),
                description: description.to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "url": {"type": "string"}
                    },
                    "required": ["url"]
                }),
            },
            workflow,
            deserialize: Box::new(move |json_str: &str| {
                let req: HttpRequest = serde_json::from_str(json_str)
                    .map_err(|e| WorkflowError::ValidationError(e.to_string()))?;
                Ok(Box::new(req) as Box<dyn std::any::Any + Send + Sync>)
            }),
            serialize_output: Box::new(move |output: &Box<dyn std::any::Any + Send + Sync>| {
                let result = output
                    .downcast_ref::<Result<HttpResponse, HttpError>>()
                    .ok_or_else(|| WorkflowError::ValidationError("downcast failed".into()))?;
                match result {
                    Ok(resp) => serde_json::to_string(resp)
                        .map_err(|e| WorkflowError::ValidationError(e.to_string())),
                    Err(e) => serde_json::to_string(&serde_json::json!({"error": e.to_string()}))
                        .map_err(|e2| WorkflowError::ValidationError(e2.to_string())),
                }
            }),
        }
    };

    registry
        .register(make_entry(
            "get_weather",
            "获取城市天气",
            mock_get_weather(),
        ))
        .unwrap();

    registry
        .register(make_entry(
            "get_news",
            "获取新闻",
            mock_get_news(),
        ))
        .unwrap();

    registry
}

// ── 场景 1：LlmComplete 基础聊天 ──────────────────────────────

async fn scenario_basic_chat(ctx: &ExecutionContext) -> anyhow::Result<()> {
    println!("=== Scenario 1: LlmComplete (basic chat) ===");

    let service = Arc::new(MockLlmService::new());
    let wf = LlmComplete::new(service, Some("You are a helpful assistant.".into()));

    let request = LlmRequest {
        messages: vec![ChatMessage::user("Hello!")],
        model: None,
        temperature: None,
        max_tokens: None,
    };

    println!("  Input: \"Hello!\"");
    let result = wf.execute(request, ctx).await?;
    let response = result.map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("  Output: \"{}\"", response.content);
    if let Some(usage) = &response.usage {
        println!("  Usage: {} prompt + {} completion = {} total",
            usage.prompt_tokens, usage.completion_tokens, usage.total_tokens);
    }

    println!("  OK\n");
    Ok(())
}

// ── 场景 2：LlmCompleteWithTools（单轮工具调用）───────────────

async fn scenario_with_tools(ctx: &ExecutionContext) -> anyhow::Result<()> {
    println!("=== Scenario 2: LlmCompleteWithTools (single turn) ===");

    let service = Arc::new(MockLlmService::new());
    let tools = build_tool_registry().get_tool_definitions();

    let wf = LlmCompleteWithTools::new(
        service,
        Some("You are a helpful assistant with access to weather and news tools.".into()),
        tools,
    );

    let request = LlmRequest {
        messages: vec![ChatMessage::user("What's the weather in Beijing?")],
        model: None,
        temperature: None,
        max_tokens: None,
    };

    println!("  Input: \"What's the weather in Beijing?\"");
    let result = wf.execute(request, ctx).await?;
    let response = result.map_err(|e| anyhow::anyhow!("{e}"))?;

    // Mock LLM 第 1 轮返回 tool_call
    if let Some(calls) = &response.tool_calls {
        println!("  LLM returned {} tool_call(s):", calls.len());
        for call in calls {
            println!("    {}({}) [id={}]", call.name, call.arguments, call.id);
        }
    } else {
        println!("  Output: \"{}\"", response.content);
    }

    println!("  OK\n");
    Ok(())
}

// ── 场景 3：LlmAgentLoop（自动多轮工具调用）───────────────────

async fn scenario_agent_loop(ctx: &ExecutionContext) -> anyhow::Result<()> {
    println!("=== Scenario 3: LlmAgentLoop (agentic loop) ===");
    println!("  Agent will auto-execute tool calls until LLM returns text.\n");

    let service = Arc::new(MockLlmService::new());
    let tool_registry = Arc::new(build_tool_registry());

    let agent = LlmAgentLoop::new(
        service,
        Some("You are a helpful assistant.".into()),
        tool_registry,
        10, // max iterations
    );

    let request = LlmRequest {
        messages: vec![ChatMessage::user(
            "Tell me about Beijing's weather and latest news.",
        )],
        model: None,
        temperature: None,
        max_tokens: None,
    };

    println!("  Input: \"Tell me about Beijing's weather and latest news.\"");
    println!("  --- Agent loop start ---");

    let result = agent.execute(request, ctx).await?;
    let response = result.map_err(|e| anyhow::anyhow!("{e}"))?;

    println!("  --- Agent loop end ---");
    println!("  Final response: \"{}\"", response.content);
    assert!(
        response.tool_calls.is_none(),
        "Agent should have finished with a text response"
    );
    assert!(response.content.contains("22°C"));

    println!("  OK\n");
    Ok(())
}

// ── 场景 4：ToolRegistry + AgentLoop 组合 ─────────────

async fn scenario_tool_agent(ctx: &ExecutionContext) -> anyhow::Result<()> {
    println!("=== Scenario 4: ToolRegistry + AgentLoop ===");

    let tool_registry = build_tool_registry();
    println!("  Tools: {:?}", tool_registry.get_tool_definitions().iter().map(|d| &d.name).collect::<Vec<_>>());
    assert_eq!(tool_registry.get_tool_definitions().len(), 2);

    // 将 ToolRegistry 接入 AgentLoop
    let service = Arc::new(MockLlmService::new());
    let agent = LlmAgentLoop::new(service, None, Arc::new(tool_registry), 10);

    let request = LlmRequest {
        messages: vec![ChatMessage::user("Beijing weather and news?")],
        model: None,
        temperature: None,
        max_tokens: None,
    };

    println!("\n  Running AgentLoop with tools...");
    let result = agent.execute(request, ctx).await?;
    let response = result.map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("  Final: \"{}\"", response.content);

    println!("  OK\n");
    Ok(())
}

// ── main ──────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    scenario_basic_chat(&ctx).await?;
    scenario_with_tools(&ctx).await?;
    scenario_agent_loop(&ctx).await?;
    scenario_tool_agent(&ctx).await?;

    println!("All 4 scenarios passed.");
    Ok(())
}
