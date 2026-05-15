//! # 命名空间配置构建错误类型
//!
//! 定义 [`ConfigBuilder`](super::ConfigBuilder) 在解析 XML、查找工作流实现、
//! 构建 DAG 时可能返回的错误。
//!
//! ## 错误类型
//!
//! | 变体 | 场景 | 示例 |
//! |------|------|------|
//! | `ParseError` | XML 格式不合法 | 缺少闭合标签 |
//! | `IoError` | 文件读取失败 | 配置文件不存在 |
//! | `UnknownImpl` | `impl="..."` 引用了未注册的工作流 | `impl="not_registered"` |
//! | `UnknownReference` | `{ref}` 引用了不存在的 result_name | `{missing.value}` |
//! | `PlanError` | DAG 构建失败（环依赖等） | A→B→A 循环引用 |
//! | `MissingEnd` | XML 中缺少 `<end>` 元素 | 只有 `<workflow>` 没有 `<end>` |
//! | `DuplicateResultName` | 多个节点的 `result_name` 重复 | 两个节点都叫 `"a"` |
//!
//! ## 与 WorkflowError 的关系
//!
//! [`ConfigError`] 是配置阶段的错误，构建完成后执行阶段的错误由
//! [`WorkflowError`] 承载。
//! `PlanError` 变体内嵌 `WorkflowError`，用于传播 DAG 构建错误。

use crate::workflow::error::WorkflowError;

/// 配置构建错误。
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// XML 解析错误。
    #[error("XML parse error: {0}")]
    ParseError(#[from] quick_xml::de::DeError),

    /// 文件 I/O 错误。
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// 引用了未注册的工作流实现。
    #[error("unknown workflow impl '{name}' in element '{element}'")]
    UnknownImpl {
        element: String,
        name: String,
    },

    /// 引用了不存在的 result_name。
    #[error("unknown result_name '{name}' referenced in element '{element}'")]
    UnknownReference {
        element: String,
        name: String,
    },

    /// DAG 构建错误（环检测等）。
    #[error("plan error: {0}")]
    PlanError(#[from] WorkflowError),

    /// 缺少必要的 `<end>` 元素。
    #[error("missing <end> element")]
    MissingEnd,

    /// 重复的 result_name。
    #[error("duplicate result_name '{0}'")]
    DuplicateResultName(String),
}
