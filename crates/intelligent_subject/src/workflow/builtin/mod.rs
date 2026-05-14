//! # Builtin Workflow 层（Layer 2）
//!
//! 由服务层（Layer 1）组合而成的预构建工作流。
//! 每个 builtin workflow 实现 [`Workflow`](crate::workflow::definition::Workflow) trait，
//! 可通过 [`register_builtins`] 预注册到 [`WorkflowFactoryRegistry`]，
//! 供 config-driven 层（Layer 3）通过名称引用。
//!
//! ## 可用 Builtin Workflow
//!
//! | Workflow | 签名 | 说明 |
//! |----------|------|------|
//! | [`Gt`] | `i32 → bool` | 大于阈值 |
//! | [`Lt`] | `i32 → bool` | 小于阈值 |
//! | [`struct@Eq`] | `i32 → bool` | 等于值 |
//! | [`Gte`] | `i32 → bool` | 大于等于阈值 |
//! | [`Lte`] | `i32 → bool` | 小于等于阈值 |
//! | [`Not`] | `bool → bool` | 布尔取反 |
//! | [`And`] | `(bool, bool) → bool` | 逻辑与 |
//! | [`Or`] | `(bool, bool) → bool` | 逻辑或 |
//! | [`IntToString`] | `i32 → String` | 整数转字符串 |
//! | [`ParseInt`] | `String → i32` | 字符串解析整数 |
//! | [`BoolToInt`] | `bool → i32` | 布尔转整数 |
//! | [`llm::LlmComplete`] | `LlmRequest → Result<LlmResponse, LlmError>` | LLM 聊天完成 |
//! | [`llm::LlmCompleteWithTools`] | `LlmRequest → Result<LlmResponse, LlmError>` | LLM 带工具调用 |
//! | [`llm::LlmAgentLoop`] | `LlmRequest → Result<LlmResponse, LlmError>` | LLM Agentic Loop |
//! | [`HttpCall`] | `HttpRequest → Result<HttpResponse, HttpError>` | HTTP 请求（GET/POST/PUT/PATCH/DELETE） |

pub mod conversion;
pub mod http;
pub mod llm;
pub mod logic;

pub use conversion::{BoolToInt, IntToString, ParseInt};
pub use http::{HttpCall, HttpError, HttpRequest, HttpResponse, Method};
pub use llm::{LlmComplete, LlmCompleteWithTools};
pub use logic::{And, Eq, Gte, Gt, Lt, Lte, Not, Or};

use crate::workflow::config::WorkflowFactoryRegistry;
use crate::workflow::definition::into_erased;

/// 将所有 builtin workflow 预注册到 registry。
///
/// 调用后，XML 配置中可直接使用注册名。
/// 参数化的 workflow（如 Gt、Lt）不在此预注册——用户需自行注册具体实例。
pub fn register_builtins(registry: &mut WorkflowFactoryRegistry) {
    registry.register("not", || into_erased(Not));
    registry.register("and", || into_erased(And));
    registry.register("or", || into_erased(Or));
    registry.register("int_to_string", || into_erased(IntToString));
    registry.register("parse_int", || into_erased(ParseInt));
    registry.register("bool_to_int", || into_erased(BoolToInt));
    registry.register("http_get", || into_erased(self::http::http_get()));
    registry.register("http_post", || into_erased(self::http::http_post()));
    registry.register("http_put", || into_erased(self::http::http_put()));
    registry.register("http_patch", || into_erased(self::http::http_patch()));
    registry.register("http_delete", || into_erased(self::http::http_delete()));
}
