//! # 工具注册表
//!
//! 将工作流暴露为 LLM 可调用的工具。
//! 每个工具持有一个独立的 [`ErasedWorkflow`] 实例和 serde 闭包，
//! 桥接 JSON 字符串与类型擦除的工作流。
//!
//! 工具注册表通过 XML 配置的 `<tool>` 元素由 [`ConfigBuilder`](super::config::ConfigBuilder) 构建，
//! 不支持编程式注册。

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::workflow::definition::ErasedWorkflow;
use crate::workflow::error::WorkflowError;
use crate::workflow::model::ExecutionContext;
use crate::workflow::services::llm::{ToolCall, ToolDefinition};

type BoxedValue = Box<dyn Any + Send + Sync>;

/// 工具条目：映射工具名到工作流 + serde 闭包。
pub struct ToolEntry {
    /// 工具定义（名称、描述、参数 schema）。
    pub tool_def: ToolDefinition,
    /// 工具对应的工作流实例。
    pub workflow: Box<dyn ErasedWorkflow>,
    /// 反序列化 JSON 字符串为 `Box<dyn Any>`。
    pub deserialize:
        Box<dyn Fn(&str) -> Result<BoxedValue, WorkflowError> + Send + Sync>,
    /// 序列化 `Box<dyn Any>` 为 JSON 字符串。
    pub serialize_output:
        Box<dyn Fn(&BoxedValue) -> Result<String, WorkflowError> + Send + Sync>,
}

/// 工具注册表，线程安全。
///
/// 由 [`ConfigBuilder`](super::config::ConfigBuilder) 从 XML `<tool>` 元素构建。
/// 持有独立的 `Box<dyn ErasedWorkflow>` 实例，不依赖 `WorkflowManager`。
pub struct ToolRegistry {
    tools: dashmap::DashMap<String, ToolEntry>,
}

impl ToolRegistry {
    /// 创建空注册表。
    pub fn new() -> Self {
        Self {
            tools: dashmap::DashMap::new(),
        }
    }

    /// 注册工具条目。
    pub fn register(&self, entry: ToolEntry) -> Result<(), WorkflowError> {
        let name = entry.tool_def.name.clone();
        if self.tools.contains_key(&name) {
            return Err(WorkflowError::ValidationError(format!(
                "tool '{name}' already registered"
            )));
        }
        self.tools.insert(name, entry);
        Ok(())
    }

    /// 获取所有工具定义（传给 LLM API）。
    pub fn get_tool_definitions(&self) -> Vec<ToolDefinition> {
        self.tools
            .iter()
            .map(|entry| entry.value().tool_def.clone())
            .collect()
    }

    /// 执行工具调用：反序列化参数 → 执行工作流 → 序列化输出。
    pub async fn execute_tool(
        &self,
        tool_call: &ToolCall,
        ctx: &ExecutionContext,
    ) -> Result<String, WorkflowError> {
        let entry = self
            .tools
            .get(&tool_call.name)
            .ok_or_else(|| WorkflowError::ToolNotFound(tool_call.name.clone()))?;

        // 反序列化参数
        let input = (entry.deserialize)(&tool_call.arguments).map_err(|e| {
            WorkflowError::ToolArgumentError {
                tool: tool_call.name.clone(),
                message: e.to_string(),
            }
        })?;

        // 执行工作流 — 将输入包装到 HashMap 参数
        let mut params: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
        params.insert("input".to_string(), Arc::from(input));
        let output = entry
            .workflow
            .execute_erased(params, ctx)
            .await
            .map_err(|e| WorkflowError::ToolOutputError {
                tool: tool_call.name.clone(),
                message: e.to_string(),
            })?;

        // 从 NamespaceOutput 提取 "value" 字段
        let result = output.fields.into_iter()
            .find(|(k, _)| k == "value")
            .map(|(_, v)| v)
            .ok_or_else(|| WorkflowError::ToolOutputError {
                tool: tool_call.name.clone(),
                message: "no 'value' field in tool output".to_string(),
            })?;

        // 序列化输出
        (entry.serialize_output)(&result).map_err(|e| WorkflowError::ToolOutputError {
            tool: tool_call.name.clone(),
            message: e.to_string(),
        })
    }

    /// 注册表是否为空。
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ToolRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.tools.len();
        f.debug_struct("ToolRegistry")
            .field("count", &count)
            .finish()
    }
}
