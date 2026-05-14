//! # 配置构建错误
//!
//! 定义从 XML 配置构建工作流 DAG 时可能出现的错误。
//!
//! ## 功能实现
//!
//! [`ConfigBuildError`] 枚举包含多种变体，覆盖配置构建全流程：
//!
//! - **`ParseError`** — XML 反序列化错误（由 serde + quick-xml 自动产生）
//! - **`IoError`** — 文件读取 I/O 错误（仅 `build_from_file` 路径）
//! - **`UnknownType`** — 节点引用了未注册的类型名
//! - **`UnknownWorkflow`** — 节点引用了未注册的工作流工厂名
//! - **`UnknownNode`** — 边或循环体引用了不存在的节点名
//! - **`UnknownSumMatch`** — 节点引用了未注册的 sum-match 工厂
//! - **`UnknownProductJoin`** — 节点引用了未注册的 product-join 工厂
//! - **`UnknownCloneGather`** — 节点引用了未注册的 clone gather 工厂
//! - **`UnknownReshape`** — 节点引用了未注册的 reshape 函数
//! - **`UnknownDispatch`** — 节点引用了未注册的 dispatch 函数
//! - **`MissingDispatchCount`** — `dispatch` 属性缺少 `dispatch-count`
//! - **`ConflictingAttributes`** — 属性冲突
//! - **`DagError`** — 底层 DAG 构建错误（类型不匹配、环等）
//!
//! ## 实现特色
//!
//! - 通过 `thiserror` 自动实现 `Error` trait 和 `Display`
//! - `#[from]` 自动从 `quick_xml::de::DeError` 和 `std::io::Error` 转换
//! - `UnknownType` 和 `UnknownWorkflow` 同时包含节点名和引用名，便于精确定位问题
//! - `DagError` 封装底层 [`WorkflowError`](WorkflowError)，
//!   保留完整的错误链
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | `thiserror`（错误派生宏）、`quick-xml`（XML 反序列化） |
//! | 内部模块 | [`WorkflowError`] |
//!
//! ## 示例
//!
//! **匹配配置构建错误：**
//!
//! ```rust
//! use intelligent_subject::workflow::config::ConfigBuildError;
//!
//! let xml = r#"<workflow name="test" entry="a" exit="a">
//!   <node name="a" implementation="nonexistent"/>
//! </workflow>"#;
//!
//! let types = intelligent_subject::workflow::config::TypeRegistry::new();
//! let workflows = intelligent_subject::workflow::config::WorkflowFactoryRegistry::new();
//! let builder = intelligent_subject::workflow::config::ConfigBuilder::new(types, workflows);
//!
//! let result = builder.build_from_str(xml);
//! match result {
//!     Err(ConfigBuildError::UnknownWorkflow { node, name }) => {
//!         assert_eq!(node, "a");
//!         assert_eq!(name, "nonexistent");
//!     }
//!     Err(ConfigBuildError::DagError(_)) => { /* DAG 层面错误 */ }
//!     _ => {}
//! }
//! ```

use crate::workflow::error::WorkflowError;
use thiserror::Error;

/// 从 XML 配置构建工作流 DAG 时可能出现的错误。
#[derive(Error, Debug)]
pub enum ConfigBuildError {
    /// XML 反序列化错误。
    #[error("XML parse error: {0}")]
    ParseError(#[from] quick_xml::de::DeError),

    /// 文件 I/O 错误。
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// 节点引用了未注册的类型名。
    #[error("unknown type '{name}' referenced in node '{node}'")]
    UnknownType {
        node: String,
        name: String,
    },

    /// 节点引用了未注册的工作流工厂名。
    #[error("unknown workflow '{name}' referenced in node '{node}'")]
    UnknownWorkflow {
        node: String,
        name: String,
    },

    /// 引用了不存在的节点名。
    #[error("unknown node '{0}'")]
    UnknownNode(String),

    /// 节点引用了未注册的 sum-match 类型名组合。
    #[error("unknown sum-match ok-type='{ok_type}' err-type='{err_type}' in node '{node}'")]
    UnknownSumMatch {
        node: String,
        ok_type: String,
        err_type: String,
    },

    /// 节点引用了未注册的 product-join 工厂名。
    #[error("unknown product-join '{name}' in node '{node}'")]
    UnknownProductJoin {
        node: String,
        name: String,
    },

    /// 节点引用了未注册的 clone scatter-gather 工厂名。
    #[error("unknown clone gather '{name}' in node '{node}'")]
    UnknownCloneGather {
        node: String,
        name: String,
    },

    /// 节点引用了未注册的 reshape 函数名。
    #[error("unknown reshape '{name}' in node '{node}'")]
    UnknownReshape {
        node: String,
        name: String,
    },

    /// 节点引用了未注册的 dispatch 函数名。
    #[error("unknown dispatch '{name}' in node '{node}'")]
    UnknownDispatch {
        node: String,
        name: String,
    },

    /// 底层 DAG 构建错误（类型不匹配、环等）。
    #[error("DAG error: {0}")]
    DagError(#[from] WorkflowError),

    /// 属性冲突：dispatch 需要 dispatch-count。
    #[error("node '{node}': 'dispatch' requires 'dispatch-count'")]
    MissingDispatchCount { node: String },

    /// 属性冲突：不可同时指定。
    #[error("node '{node}': conflicting attributes {attrs}")]
    ConflictingAttributes { node: String, attrs: String },
}
