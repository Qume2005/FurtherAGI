//! # 执行计划数据结构
//!
//! 定义命名空间工作流系统的核心数据结构。
//!
//! ## 功能实现
//!
//! - **[`NodeKind`]** — 4 种节点类型枚举：`Workflow`、`If`、`Loop`、`End`
//! - **[`Node`]** — 命名空间节点，包含参数、依赖信息和子节点
//! - **[`ExecutionPlan`]** — 构建完成的不可变执行计划，含拓扑排序
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 内部模块 | [`ErasedWorkflow`](ErasedWorkflow)、[`ParamValue`](ParamValue)、[`NodeId`](NodeId) |

use std::collections::HashMap;

use crate::workflow::definition::ErasedWorkflow;
use crate::workflow::model::NodeId;

/// 节点参数值（从 config 模块重导出）。
pub use crate::workflow::config::ParamValue;

/// 命名空间工作流中的节点。
pub struct Node {
    /// 节点 ID。
    pub id: NodeId,
    /// 命名空间键名（节点输出存入 `result_name.*` 下）。
    pub result_name: String,
    /// 节点类型。
    pub kind: NodeKind,
    /// 参数列表：参数名 → 值（字面量或引用）。
    pub params: Vec<(String, ParamValue)>,
    /// 依赖的命名空间名列表（构建时从 params 中的引用推导）。
    pub dependencies: Vec<String>,
    /// 子节点 ID 列表（仅 If 和 Loop 使用）。
    pub children: Vec<NodeId>,
    /// 工作流实现（仅 Workflow 节点）。
    pub workflow: Option<Box<dyn ErasedWorkflow>>,
}

/// 节点类型枚举（4 种）。
#[derive(Debug)]
pub enum NodeKind {
    /// 执行一个工作流实现，结果写入命名空间。
    Workflow {
        /// 已注册的工作流工厂名。
        impl_name: String,
    },
    /// 条件分支：引用命名空间中的 bool 值，true 时在子命名空间中执行子节点。
    If {
        /// 谓词引用（如 `"{a.is_positive}"`）。
        predicate: String,
        /// 可选：子节点执行后从子命名空间解析的值，写入父命名空间的 `result_name` 下。
        ///
        /// 例如 `then = "{inner.value}"` 将子命名空间中 `inner.value` 的值
        /// 传播到父命名空间的 `result_name` 键下。
        then: Option<String>,
    },
    /// 固定次数循环。
    Loop {
        /// 初始状态引用（如 `"{some_node.count}"`）。
        state_init: String,
        /// 下一次状态的引用（循环体内的命名空间引用）。
        next_state: String,
        /// 迭代次数。
        count: usize,
    },
    /// 终止工作流，返回 result 引用的值。可提前终止。
    End {
        /// 返回值引用（如 `"{weather_report}"`）。
        result_ref: String,
    },
}

/// 构建完成的不可变执行计划。
pub struct ExecutionPlan {
    /// 所有节点，按 NodeId 索引。
    pub nodes: HashMap<NodeId, Node>,
    /// 拓扑排序后的节点 ID 列表。
    pub topo_order: Vec<NodeId>,
    /// 入口节点 ID。
    pub entry: NodeId,
}

impl std::fmt::Debug for ExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionPlan")
            .field("node_count", &self.nodes.len())
            .field("topo_order", &self.topo_order)
            .field("entry", &self.entry)
            .finish()
    }
}
