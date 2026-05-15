//! HTTP 工作流示例：将 HTTP 方法作为 builtin workflow 在 DAG 中使用。
//!
//! 演示如何：
//! - 使用 `from_fn` 创建 mock HTTP workflow
//! - 通过 WorkflowManager 注册和执行 HTTP 处理工作流
//! - 手动构建 ToolRegistry 模拟 LLM tool_call 完整流程
//!
//! **注意**：本示例使用 mock workflow 演示 DAG 构建和工具注册流程，
//! 不发送真实 HTTP 请求。生产环境中替换为 `http_get()` / `http_post()` 即可。
//!
//! 运行：`cargo run -p examples --bin workflow_http`

use std::sync::Arc;

use intelligent_subject::workflow::builtin::http::{HttpError, HttpRequest, HttpResponse};
use intelligent_subject::workflow::definition::from_fn;
use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::model::ExecutionContext;
use intelligent_subject::workflow::platform::NullPlatform;

/// Mock HTTP GET：模拟 httpbin.org 响应，不发送真实请求。
fn mock_http_get() -> Box<dyn intelligent_subject::workflow::definition::ErasedWorkflow> {
    from_fn("mock_http_get",
        |input: HttpRequest, _ctx: &ExecutionContext| async move {
            println!("    MockHttpGet {{ url: \"{}\" }}", input.url);
            // 模拟响应
            let response = HttpResponse {
                status: 200,
                body: serde_json::json!({
                    "url": input.url,
                    "mock": true,
                    "args": input.body,
                }),
            };
            Ok::<Result<HttpResponse, HttpError>, WorkflowError>(Ok(response))
        }
    )
}

/// Mock HTTP POST：模拟 POST 响应。
fn mock_http_post() -> Box<dyn intelligent_subject::workflow::definition::ErasedWorkflow> {
    from_fn("mock_http_post",
        |input: HttpRequest, _ctx: &ExecutionContext| async move {
            println!("    MockHttpPost {{ url: \"{}\" }}", input.url);
            let response = HttpResponse {
                status: 201,
                body: serde_json::json!({
                    "url": input.url,
                    "mock": true,
                    "received_body": input.body,
                }),
            };
            Ok::<Result<HttpResponse, HttpError>, WorkflowError>(Ok(response))
        }
    )
}

// ── 场景 1：WorkflowManager 注册 HTTP mock workflow ──────────

async fn scenario_workflow_manager(ctx: &ExecutionContext) -> anyhow::Result<()> {
    println!("=== Scenario 1: WorkflowManager with HTTP mock ===");

    let mgr = intelligent_subject::workflow::workflow_manager::WorkflowManager::new();

    // 注册 extract_status workflow
    mgr.add("extract_status", |input: HttpResponse| async move {
        println!("    ExtractStatus: HTTP {}", input.status);
        Ok::<u16, WorkflowError>(input.status)
    })?;

    let test_response = HttpResponse {
        status: 200,
        body: serde_json::json!({"message": "ok"}),
    };
    let status: u16 = mgr.execute_typed("extract_status", test_response, ctx).await?;
    println!("  Result: HTTP {status}");
    assert_eq!(status, 200);

    println!("  OK\n");
    Ok(())
}

// ── 场景 2：模拟 LLM tool_call（通过 ToolRegistry）──────────

async fn scenario_tool_call(ctx: &ExecutionContext) -> anyhow::Result<()> {
    use intelligent_subject::workflow::services::llm::{ToolCall, ToolDefinition};
    use intelligent_subject::workflow::tool_registry::{ToolEntry, ToolRegistry};

    println!("=== Scenario 2: ToolRegistry call (simulated LLM tool_call) ===");

    let registry = ToolRegistry::new();

    // 注册 mock http_get 为工具
    let entry = ToolEntry {
        tool_def: ToolDefinition {
            name: "http_get".to_string(),
            description: "发送 HTTP GET 请求".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "url": {"type": "string"}
                },
                "required": ["url"]
            }),
        },
        workflow: mock_http_get(),
        deserialize: Box::new(|json_str: &str| -> Result<Box<dyn std::any::Any + Send + Sync>, WorkflowError> {
            let req: HttpRequest = serde_json::from_str(json_str)
                .map_err(|e| WorkflowError::ValidationError(e.to_string()))?;
            Ok(Box::new(req))
        }),
        serialize_output: Box::new(|output: &Box<dyn std::any::Any + Send + Sync>| -> Result<String, WorkflowError> {
            // mock_http_get 输出类型是 Result<HttpResponse, HttpError>
            let result = output.downcast_ref::<Result<HttpResponse, HttpError>>()
                .ok_or_else(|| WorkflowError::ValidationError("downcast failed".into()))?;
            match result {
                Ok(resp) => serde_json::to_string(resp)
                    .map_err(|e| WorkflowError::ValidationError(e.to_string())),
                Err(e) => serde_json::to_string(&serde_json::json!({"error": e.to_string()}))
                    .map_err(|e2| WorkflowError::ValidationError(e2.to_string())),
            }
        }),
    };

    registry.register(entry)?;

    // 模拟 LLM 返回的 tool_call
    let tool_call = ToolCall {
        id: "call_123".to_string(),
        name: "http_get".to_string(),
        arguments: serde_json::to_string(&serde_json::json!({
            "url": "https://api.example.com/users/42"
        }))?,
    };

    println!("  Calling tool '{}' (id: {})", tool_call.name, tool_call.id);
    println!("  Arguments: {}", tool_call.arguments);

    // 执行工具调用
    let result = registry.execute_tool(&tool_call, ctx).await?;
    println!("  Response: {}", result);

    // 验证响应内容
    let parsed: serde_json::Value = serde_json::from_str(&result)?;
    assert_eq!(parsed["status"], 200);
    assert_eq!(parsed["body"]["mock"], true);

    println!("  OK\n");
    Ok(())
}

// ── 场景 3：多工具注册（GET + POST）─────────────────────────

async fn scenario_multi_tools(ctx: &ExecutionContext) -> anyhow::Result<()> {
    use intelligent_subject::workflow::services::llm::{ToolCall, ToolDefinition};
    use intelligent_subject::workflow::tool_registry::{ToolEntry, ToolRegistry};

    println!("=== Scenario 3: Multi-tool registry (GET + POST) ===");

    let registry = ToolRegistry::new();

    // 注册 GET 工具
    let get_entry = ToolEntry {
        tool_def: ToolDefinition {
            name: "http_get".to_string(),
            description: "获取资源".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {"url": {"type": "string"}},
                "required": ["url"]
            }),
        },
        workflow: mock_http_get(),
        deserialize: Box::new(|s: &str| -> Result<Box<dyn std::any::Any + Send + Sync>, WorkflowError> {
            Ok(Box::new(serde_json::from_str::<HttpRequest>(s)
                .map_err(|e| WorkflowError::ValidationError(e.to_string()))?))
        }),
        serialize_output: Box::new(|output: &Box<dyn std::any::Any + Send + Sync>| -> Result<String, WorkflowError> {
            let r = output.downcast_ref::<Result<HttpResponse, HttpError>>()
                .ok_or_else(|| WorkflowError::ValidationError("downcast failed".into()))?;
            serde_json::to_string(r.as_ref().map_err(|e| e.to_string()).unwrap())
                .map_err(|e| WorkflowError::ValidationError(e.to_string()))
        }),
    };

    // 注册 POST 工具
    let post_entry = ToolEntry {
        tool_def: ToolDefinition {
            name: "http_post".to_string(),
            description: "创建资源".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {"url": {"type": "string"}, "body": {"type": "object"}},
                "required": ["url"]
            }),
        },
        workflow: mock_http_post(),
        deserialize: Box::new(|s: &str| -> Result<Box<dyn std::any::Any + Send + Sync>, WorkflowError> {
            Ok(Box::new(serde_json::from_str::<HttpRequest>(s)
                .map_err(|e| WorkflowError::ValidationError(e.to_string()))?))
        }),
        serialize_output: Box::new(|output: &Box<dyn std::any::Any + Send + Sync>| -> Result<String, WorkflowError> {
            let r = output.downcast_ref::<Result<HttpResponse, HttpError>>()
                .ok_or_else(|| WorkflowError::ValidationError("downcast failed".into()))?;
            serde_json::to_string(r.as_ref().map_err(|e| e.to_string()).unwrap())
                .map_err(|e| WorkflowError::ValidationError(e.to_string()))
        }),
    };

    registry.register(get_entry)?;
    registry.register(post_entry)?;

    // 验证工具列表
    let defs = registry.get_tool_definitions();
    println!("  Registered tools: {:?}", defs.iter().map(|d| d.name.as_str()).collect::<Vec<_>>());
    assert_eq!(defs.len(), 2);

    // 模拟 LLM 依次调用 GET 和 POST
    let get_call = ToolCall {
        id: "call_1".to_string(),
        name: "http_get".to_string(),
        arguments: serde_json::to_string(&serde_json::json!({"url": "https://api.example.com/users"}))?,
    };
    let post_call = ToolCall {
        id: "call_2".to_string(),
        name: "http_post".to_string(),
        arguments: serde_json::to_string(&serde_json::json!({
            "url": "https://api.example.com/users",
            "body": {"name": "Alice", "age": 30}
        }))?,
    };

    let get_result = registry.execute_tool(&get_call, ctx).await?;
    let post_result = registry.execute_tool(&post_call, ctx).await?;

    println!("  GET  response: {}", get_result);
    println!("  POST response: {}", post_result);

    let get_parsed: serde_json::Value = serde_json::from_str(&get_result)?;
    let post_parsed: serde_json::Value = serde_json::from_str(&post_result)?;
    assert_eq!(get_parsed["status"], 200);
    assert_eq!(post_parsed["status"], 201);

    println!("  OK\n");
    Ok(())
}

// ── main ──────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = ExecutionContext {
        platform: Arc::new(NullPlatform::new()),
    };

    scenario_workflow_manager(&ctx).await?;
    scenario_tool_call(&ctx).await?;
    scenario_multi_tools(&ctx).await?;

    println!("All 3 scenarios passed.");
    Ok(())
}
