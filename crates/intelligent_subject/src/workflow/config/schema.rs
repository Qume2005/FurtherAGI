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
//! - **[`NodeConfig`]** — 节点定义枚举，10 种变体对应 [`NodeKind`](crate::workflow::dag::NodeKind)
//! - **[`EdgeConfig`]** — 有向边定义，包含可选的标签和结构性操作属性
//!
//! ## 实现特色
//!
//! - 以 XML 元素名区分节点类型，无需 `kind` 属性字段
//! - 结构性节点（Connection、Reshape、Dispatch、SumMatch、ProductJoin）支持属性语法：
//!   在 `<node>` 或 `<connect>` 上以属性声明，由构建器展开为合成节点
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
    /// LLM 工具定义列表。
    pub tools: Vec<ToolConfig>,
}

/// LLM 工具定义。
///
/// 将已注册的工作流暴露为 LLM 可调用的工具。
/// 对应 XML `<tool>` 元素。
///
/// # XML 示例
///
/// ```xml
/// <tool name="weather"
///       description="查询城市天气"
///       implementation="weather_query"
///       input-type="WeatherInput"
///       output-type="WeatherOutput"
///       parameters='{"type":"object","properties":{"city":{"type":"string"}}}'/>
/// ```
#[derive(Debug)]
pub struct ToolConfig {
    /// 工具名称（暴露给 LLM）。
    pub name: String,
    /// 工具描述。
    pub description: String,
    /// 已注册的工作流工厂名。
    pub implementation: String,
    /// 输入类型名（必须通过 `register_tool_type` 注册）。
    pub input_type: String,
    /// 输出类型名（必须通过 `register_tool_type` 注册）。
    pub output_type: String,
    /// 工具参数的 JSON Schema。
    pub parameters: serde_json::Value,
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
/// | `<node>` | `Workflow` | 工作流实现节点，可通过属性附加结构性操作 |
/// | `<clone>` | `Clone` | Scatter-gather 节点：并行分支 + gather 元组输出 |
/// | `<conditional>` | `Conditional` | 条件分支节点 |
/// | `<loop>` | `Loop` | 固定次数循环节点 |
/// | `<sub-workflow>` | `SubWorkflow` | 子工作流引用节点 |
/// | `<connection>` | `Connection` | 命名透传节点（向后兼容，推荐用 `<connect>` 属性） |
/// | `<sum-match>` | `SumMatch` | 和类型拆解节点（向后兼容，推荐用 `<node sum-match>` 属性） |
/// | `<product-join>` | `ProductJoin` | 积类型合并节点（向后兼容，推荐用 `<node join>` 属性） |
/// | `<reshape>` | `Reshape` | 元组重组节点（向后兼容，推荐用 `<connect reshape>` 属性） |
/// | `<dispatch>` | `Dispatch` | 积类型拆分节点（向后兼容，推荐用 `<node dispatch>` 属性） |
///
/// ## 属性语法
///
/// 结构性节点可作为 `<node>` 或 `<connect>` 的属性：
///
/// - `<node implementation="f" dispatch="fn" dispatch-count="2"/>` — 拆分输出
/// - `<node implementation="f" sum-match="i32/String"/>` — 和类型拆解
/// - `<node implementation="f" join="fn"/>` — 积类型合并（收集多输入）
/// - `<connect from="a" to="b" reshape="fn"/>` — 元组重组
/// - `<connect from="a" to="b" type="i32" label="x"/>` — 透传连接
#[derive(Debug)]
pub enum NodeConfig {
    /// 工作流实现节点。
    ///
    /// 可通过可选属性附加结构性操作：
    /// - `dispatch` + `dispatch-count`：输出后立即拆分
    /// - `sum-match`：输出后和类型拆解
    /// - `join`：收集多输入后合并，再执行工作流
    Workflow {
        /// 已注册的工作流工厂名。
        implementation: String,
        /// Dispatch 属性：已注册的 dispatch 函数名。
        dispatch_name: Option<String>,
        /// Dispatch 属性：输出数量。
        dispatch_count: Option<usize>,
        /// SumMatch 属性：格式 `"OkType/ErrType"`（如 `"i32/String"`）。
        sum_match: Option<String>,
        /// ProductJoin 属性：已注册的 join 函数名。
        join_name: Option<String>,
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
    /// 元组重组节点：调整嵌套结构（如 `(A, B, C)` → `(A, (B, C))`）。
    Reshape {
        /// 已注册的 reshape 函数名。
        reshape_name: String,
    },
    /// 积类型拆分节点：将元组拆为多个输出，每条边一个，ProductJoin 的逆操作。
    Dispatch {
        /// 输出数量（必须等于出边数）。
        output_count: usize,
        /// 已注册的 dispatch 函数名。
        dispatch_name: String,
    },
}

/// 有向边。
#[derive(Debug)]
pub struct EdgeConfig {
    /// 源节点名称。
    pub from: String,
    /// 目标节点名称。
    pub to: String,
    /// 可选标签，用于条件分支（`"true"` / `"false"`）或或类型路由（`"ok"` / `"err"`）。
    pub label: Option<String>,
    /// Connection 属性：透传值的类型名（插入合成 Connection 节点）。
    pub type_name: Option<String>,
    /// Reshape 属性：已注册的 reshape 函数名（插入合成 Reshape 节点）。
    pub reshape_name: Option<String>,
}
