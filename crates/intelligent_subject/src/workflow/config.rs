//! # XML 配置驱动的工作流构建
//!
//! 从 XML 配置声明式构建 [`ExecutionPlan`](crate::workflow::dag::ExecutionPlan)。
//!
//! ## XML 元素
//!
//! 根元素为 `<workflow>`，内部包含 4 种子元素：
//!
//! | 元素 | 必填属性 | 说明 |
//! |------|----------|------|
//! | `<workflow>` | `result_name`, `impl` | 执行工作流，结果存入命名空间 |
//! | `<if>` | `predicate` | 条件分支，内部是顺序子流程 |
//! | `<loop>` | `result_name`, `state_init`, `next_state`, `count` | 固定次数循环 |
//! | `<end>` | `result` | 终止工作流并返回结果 |
//!
//! 属性值支持两种形式：
//! - **字面量**：`input="hello"` — 直接传入字符串
//! - **引用**：`input="{a.value}"` — 从命名空间读取上游节点的输出
//!
//! ## 快速开始
//!
//! ```rust
//! use intelligent_subject::workflow::config::ConfigBuilder;
//! use intelligent_subject::workflow::config::WorkflowFactoryRegistry;
//! use intelligent_subject::workflow::definition::from_fn;
//! use intelligent_subject::workflow::model::ExecutionContext;
//! use intelligent_subject::workflow::error::WorkflowError;
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut reg = WorkflowFactoryRegistry::new();
//! reg.register("upper", || {
//!     from_fn("upper", |input: String, _| async move {
//!         Ok::<String, WorkflowError>(input.to_uppercase())
//!     })
//! });
//!
//! let builder = ConfigBuilder::new(reg);
//! let plan = builder.build_from_str(r#"
//!     <workflow>
//!       <workflow result_name="a" impl="upper" input="hello"/>
//!       <end result="{a.value}"/>
//!     </workflow>
//! "#)?;
//! assert_eq!(plan.topo_order.len(), 2);
//! # Ok(())
//! # }
//! ```
//!
//! ## XML 示例集
//!
//! ### 线性管道
//!
//! 节点按依赖顺序依次执行。`b` 引用 `{a.value}`，自动等待 `a` 完成。
//!
//! ```xml
//! <workflow>
//!   <workflow result_name="a" impl="append_x" input="hello"/>
//!   <workflow result_name="b" impl="append_x" input="{a.value}"/>
//!   <end result="{b.value}"/>
//! </workflow>
//! ```
//!
//! 执行流程：`"hello"` → append_x → `"helloX"` → append_x → `"helloXX"` → 返回
//!
//! ### 独立并行
//!
//! 无依赖的节点自动并发执行。`<end>` 只选取 `x` 的值。
//!
//! ```xml
//! <workflow>
//!   <workflow result_name="x" impl="append_x" input="foo"/>
//!   <workflow result_name="y" impl="append_x" input="bar"/>
//!   <end result="{x.value}"/>
//! </workflow>
//! ```
//!
//! 执行流程：x 和 y 并发执行，end 等待两者完成后取 x 的值。
//!
//! ### 条件分支 (`<if>`)
//!
//! `<if>` 的 `predicate` 引用命名空间中的 `bool` 值。为 `true` 时顺序执行子节点。
//!
//! ```xml
//! <workflow>
//!   <workflow result_name="check" impl="is_positive" input="42"/>
//!   <if predicate="{check.value}">
//!     <workflow result_name="msg" impl="format" input="positive!"/>
//!   </if>
//!   <end result="{msg.value}"/>
//! </workflow>
//! ```
//!
//! 执行流程：`is_positive(42)` → `true` → `<if>` 执行 `format("positive!")` → `"Report: positive!"`
//!
//! 当 predicate 为 `false` 时，子节点跳过不执行。
//!
//! ### 固定次数循环 (`<loop>`)
//!
//! `<loop>` 通过 `state_init`（初始状态）和 `next_state`（每次迭代后的状态引用）
//! 在迭代间传递状态。每次迭代中，当前状态通过 `latest_state` 可访问。
//!
//! ```xml
//! <workflow>
//!   <loop result_name="result" state_init="0" next_state="{step.value}" count="3">
//!     <workflow result_name="step" impl="add_one" input="{latest_state}"/>
//!   </loop>
//!   <end result="{result}"/>
//! </workflow>
//! ```
//!
//! 执行流程：初始状态 `"0"` → 迭代 1: `add_one("0")="1"` → 迭代 2: `add_one("1")="2"` →
//! 迭代 3: `add_one("2")="3"` → `"3"` 写入 `result` → 返回
//!
//! ### 多元素组合
//!
//! `<if>` 和 `<loop>` 可以与 `<workflow>` 自由组合：
//!
//! ```xml
//! <workflow>
//!   <workflow result_name="init" impl="identity" input="1"/>
//!   <if predicate="{init.value}">
//!     <loop result_name="sum" state_init="{init.value}" next_state="{accum.value}" count="5">
//!       <workflow result_name="accum" impl="add_one" input="{latest_state}"/>
//!     </loop>
//!   </if>
//!   <end result="{sum}"/>
//! </workflow>
//! ```
//!
//! ## 注册表
//!
//! - [`WorkflowFactoryRegistry`] — 映射工作流名字符串到工厂闭包。
//!   同一个 `impl` 名可以被多个 `<workflow>` 元素引用，每次调用工厂产生独立实例。

pub mod builder;
pub mod error;
pub mod param;
pub mod registry;
pub mod workflow_registry;

pub use builder::ConfigBuilder;
pub use error::ConfigError;
pub use param::{ParamValue, parse_param_value, referenced_namespace};
pub use registry::TypeRegistry;
pub use workflow_registry::WorkflowFactoryRegistry;