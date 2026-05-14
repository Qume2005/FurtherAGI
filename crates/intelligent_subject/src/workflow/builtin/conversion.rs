//! # 类型转换 Builtin Workflow
//!
//! 类型转换工作流，内部使用 [`MapFn`] 服务。

use async_trait::async_trait;

use crate::workflow::definition::Workflow;
use crate::workflow::error::WorkflowError;
use crate::workflow::model::ExecutionContext;
use crate::workflow::services::MapFn;

/// 整数转字符串：`i32 → String`。
pub struct IntToString;

#[async_trait]
impl Workflow<i32, String> for IntToString {
    fn name(&self) -> &str {
        "int_to_string"
    }

    async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<String, WorkflowError> {
        let svc = MapFn::new(|x: i32| x.to_string());
        Ok(svc.apply(input))
    }
}

/// 字符串解析为整数：`String → i32`。解析失败返回错误。
pub struct ParseInt;

#[async_trait]
impl Workflow<String, i32> for ParseInt {
    fn name(&self) -> &str {
        "parse_int"
    }

    async fn execute(&self, input: String, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
        let svc = MapFn::new(|s: String| s.parse::<i32>());
        svc.apply(input).map_err(|e: std::num::ParseIntError| {
            WorkflowError::execution(
                crate::workflow::model::NodeId(0),
                format!("parse_int failed: {e}"),
            )
        })
    }
}

/// 布尔转整数：`bool → i32`，true → 1，false → 0。
pub struct BoolToInt;

#[async_trait]
impl Workflow<bool, i32> for BoolToInt {
    fn name(&self) -> &str {
        "bool_to_int"
    }

    async fn execute(&self, input: bool, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
        let svc = MapFn::new(|b: bool| if b { 1 } else { 0 });
        Ok(svc.apply(input))
    }
}
