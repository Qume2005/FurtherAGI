//! # HTTP Builtin Workflow
//!
//! HTTP 方法工作流，用于在 DAG 中调用外部 API。
//! 输出 `Result<HttpResponse, HttpError>` — 和类型，可接入 SumMatch 节点处理成功/失败。
//!
//! ## 可用 Workflow
//!
//! | 工厂名 | 方法 | 说明 |
//! |--------|------|------|
//! | `http_get`    | GET    | 获取资源 |
//! | `http_post`   | POST   | 创建资源 |
//! | `http_put`    | PUT    | 全量更新资源 |
//! | `http_patch`  | PATCH  | 部分更新资源 |
//! | `http_delete` | DELETE | 删除资源 |

use async_trait::async_trait;
use reqwest::Client;

use crate::workflow::definition::Workflow;
use crate::workflow::error::WorkflowError;
use crate::workflow::model::ExecutionContext;

/// HTTP 方法枚举。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

/// HTTP 请求输入。
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct HttpRequest {
    /// 请求 URL。
    pub url: String,
    /// 请求头，可选。格式：`{"key": "value"}`。
    #[serde(default)]
    pub headers: Option<serde_json::Value>,
    /// 请求体，可选。JSON 序列化。
    #[serde(default)]
    pub body: Option<serde_json::Value>,
}

/// HTTP 响应输出。
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct HttpResponse {
    /// HTTP 状态码。
    pub status: u16,
    /// 响应体（解析后的 JSON 或原始字符串）。
    pub body: serde_json::Value,
}

/// HTTP 错误。
#[derive(Clone, Debug, thiserror::Error, serde::Deserialize, serde::Serialize)]
pub enum HttpError {
    /// 网络/请求错误。
    #[error("request error: {message}")]
    Request { message: String },
    /// 响应体读取错误。
    #[error("body error: {message}")]
    Body { message: String },
}

/// 泛型 HTTP 工作流。
///
/// 实现 `Workflow<HttpRequest, Result<HttpResponse, HttpError>>`。
/// 使用 `reqwest::Client` 发送请求。
pub struct HttpCall {
    method: Method,
    client: Client,
}

impl HttpCall {
    pub fn new(method: Method) -> Self {
        Self {
            method,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl Workflow<HttpRequest, Result<HttpResponse, HttpError>> for HttpCall {
    fn name(&self) -> &str {
        match self.method {
            Method::Get => "http_get",
            Method::Post => "http_post",
            Method::Put => "http_put",
            Method::Patch => "http_patch",
            Method::Delete => "http_delete",
        }
    }

    async fn execute(
        &self,
        input: HttpRequest,
        _ctx: &ExecutionContext,
    ) -> Result<Result<HttpResponse, HttpError>, WorkflowError> {
        let mut builder = match self.method {
            Method::Get => self.client.get(&input.url),
            Method::Post => self.client.post(&input.url),
            Method::Put => self.client.put(&input.url),
            Method::Patch => self.client.patch(&input.url),
            Method::Delete => self.client.delete(&input.url),
        };

        // 添加请求头
        if let Some(ref headers) = input.headers {
            if let Some(map) = headers.as_object() {
                for (key, value) in map {
                    if let Some(str_val) = value.as_str() {
                        builder = builder.header(key.as_str(), str_val);
                    }
                }
            }
        }

        // 添加请求体
        if let Some(ref body) = input.body {
            builder = builder.json(body);
        }

        let response = match builder.send().await {
            Ok(r) => r,
            Err(e) => return Ok(Err(HttpError::Request { message: e.to_string() })),
        };

        let status = response.status().as_u16();

        let body = match response.text().await {
            Ok(text) => {
                // 尝试解析为 JSON，失败则包装为字符串值
                serde_json::from_str::<serde_json::Value>(&text)
                    .unwrap_or(serde_json::Value::String(text))
            }
            Err(e) => return Ok(Err(HttpError::Body { message: e.to_string() })),
        };

        Ok(Ok(HttpResponse { status, body }))
    }
}

/// 便捷构造器。
pub fn http_get() -> HttpCall {
    HttpCall::new(Method::Get)
}

pub fn http_post() -> HttpCall {
    HttpCall::new(Method::Post)
}

pub fn http_put() -> HttpCall {
    HttpCall::new(Method::Put)
}

pub fn http_patch() -> HttpCall {
    HttpCall::new(Method::Patch)
}

pub fn http_delete() -> HttpCall {
    HttpCall::new(Method::Delete)
}
