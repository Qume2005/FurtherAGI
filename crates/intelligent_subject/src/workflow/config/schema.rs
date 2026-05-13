//! # XML 配置 Schema
//!
//! 定义声明式工作流 DAG 构建的 XML 配置数据模型。
//!
//! ## 功能实现
//!
//! 本模块提供 XML 配置的数据模型，供 [`ConfigBuilder`](super::ConfigBuilder) 解析和构建 DAG：
//!
//! - **[`WorkflowConfig`]** — 顶层配置结构，包含工作流元数据、节点定义列表和边列表
//! - **[`WorkflowMeta`]** — 工作流元数据（名称、入口节点、出口节点）
//! - **[`NodeConfig`]** — 节点定义枚举，8 种变体对应 8 种 [`NodeKind`](crate::workflow::dag::NodeKind)
//! - **[`EdgeConfig`]** — 有向边定义，包含可选的标签（用于条件分支）
//!
//! ## 实现特色
//!
//! - 以 XML 元素名区分 8 种节点类型（`<node>`、`<clone>`、`<conditional>`、`<sum-match>` 等），
//!   无需 `kind` 属性字段
//! - XML 属性天然支持 `type` 关键字，无需 Rust 保留字重命名
//! - `nodes` 使用 `Vec<(String, NodeConfig)>` 保持 XML 文档中的声明顺序
//! - `edges` 可以为空（单节点工作流如 Loop、SubWorkflow 无需边）
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | 无（纯数据结构） |
//! | 内部模块 | 无 |
//!
//! ## 示例
//!
//! **完整的 8 种节点类型 XML 配置：**
//!
//! ```xml
//! <workflow name="full_example" entry="input" exit="output">
//!   <connection name="input" label="入口" type="i32"/>
//!   <node name="process" implementation="double"/>
//!   <conditional name="check" implementation="is_positive"/>
//!   <node name="body_start" implementation="step"/>
//!   <node name="body_end" implementation="step"/>
//!   <loop name="iterate" count="3" body-entry="body_start" body-exit="body_end"/>
//!   <clone name="fan_out" type="i32" output-type="(i32,i32)" gather="my_gather">
//!     <branch implementation="mul_two"/>
//!     <branch implementation="add_one"/>
//!   </clone>
//!   <sum-match name="result_split" ok-type="i32" err-type="String"/>
//!   <product-join name="merge" output-type="(i32,bool)" input-types="i32,bool" join="my_join"/>
//!   <sub-workflow name="sub" workflow="other_pipeline" input-type="i32" output-type="String"/>
//!   <connection name="output" label="出口" type="String"/>
//! </workflow>
//! ```
//!
//! **极简线性管道：**
//!
//! ```xml
//! <workflow name="pipeline" entry="a" exit="b">
//!   <node name="a" implementation="add_one"/>
//!   <node name="b" implementation="add_one"/>
//!   <connect from="a" to="b"/>
//! </workflow>
//! ```

/// XML 工作流配置的顶层结构。
///
/// 由 [`ConfigBuilder`](super::ConfigBuilder) 的 XML 解析器填充，
/// 然后通过三遍构建算法转化为 `WorkflowDag`。
#[derive(Debug)]
pub struct WorkflowConfig {
    /// 工作流元数据（名称、入口节点、出口节点）。
    pub workflow: WorkflowMeta,
    /// 节点定义列表，保持 XML 文档中的声明顺序。
    pub nodes: Vec<(String, NodeConfig)>,
    /// 有向边列表。
    pub edges: Vec<EdgeConfig>,
}

/// 工作流元数据。
#[derive(Debug)]
pub struct WorkflowMeta {
    /// 工作流名称，用作 [`WorkflowId`](crate::workflow::model::WorkflowId)。
    pub name: String,
    /// 入口节点名称（必须存在于 `nodes` 中）。
    pub entry: String,
    /// 出口节点名称（必须存在于 `nodes` 中）。
    pub exit: String,
}

/// 节点定义，通过 XML 元素名区分类型。
///
/// | XML 元素 | 变体 | 说明 |
/// |----------|------|------|
/// | `<node>` | `Workflow` | 工作流实现节点 |
/// | `<clone>` | `Clone` | Scatter-gather 节点：并行分支 + gather 元组输出 |
/// | `<conditional>` | `Conditional` | 条件分支节点 |
/// | `<loop>` | `Loop` | 固定次数循环节点 |
/// | `<sub-workflow>` | `SubWorkflow` | 子工作流引用节点 |
/// | `<connection>` | `Connection` | 命名透传节点 |
/// | `<sum-match>` | `SumMatch` | 和类型拆解节点 |
/// | `<product-join>` | `ProductJoin` | 积类型合并节点 |
#[derive(Debug)]
pub enum NodeConfig {
    /// 工作流实现节点。
    Workflow {
        /// 已注册的工作流工厂名。
        implementation: String,
    },
    /// Scatter-gather 节点：并行运行 N 个分支 workflow，收集结果输出元组。
    Clone {
        /// 输入类型名（如 `"i32"`）。
        type_name: String,
        /// 输出元组类型名。
        output_type_name: String,
        /// 分支 workflow 工厂名列表。
        branches: Vec<String>,
        /// 已注册的 gather 函数名。
        gather_name: String,
    },
    /// 条件分支节点（谓词必须输出 `bool`）。
    Conditional {
        /// 已注册的谓词工作流名。
        implementation: String,
    },
    /// 固定次数循环节点。
    Loop {
        /// 迭代次数。
        count: usize,
        /// 循环体入口节点名。
        body_entry: String,
        /// 循环体出口节点名。
        body_exit: String,
    },
    /// 子工作流引用节点（在注册时解析）。
    SubWorkflow {
        /// 引用的工作流 ID。
        workflow: String,
        /// 子工作流输入类型名。
        input_type: String,
        /// 子工作流输出类型名。
        output_type: String,
    },
    /// 命名透传节点（用于可视化组织）。
    Connection {
        /// 连接点标签。
        label: String,
        /// 透传值的类型名。
        type_name: String,
    },
    /// 和类型拆解节点：接收 `Result<T, E>`，路由到 "ok" 或 "err" 边。
    SumMatch {
        /// Ok 变体的类型名（如 `"i32"`）。
        ok_type_name: String,
        /// Err 变体的类型名（如 `"String"`）。
        err_type_name: String,
    },
    /// 积类型合并节点：收集多个上游值合并为单个输出。
    ProductJoin {
        /// 输出类型名。
        output_type_name: String,
        /// 输入类型名列表（逗号分隔）。
        input_type_names: String,
        /// 已注册的 join 工厂名。
        join_name: String,
    },
}

/// 有向边。
#[derive(Debug)]
pub struct EdgeConfig {
    /// 源节点名称。
    pub from: String,
    /// 目标节点名称。
    pub to: String,
    /// 可选标签，用于条件分支（`"true"` / `"false"`）。
    pub label: Option<String>,
}
