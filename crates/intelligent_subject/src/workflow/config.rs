//! # XML 配置驱动的工作流构建
//!
//! 从 XML 配置声明式构建工作流 DAG，然后注册到 `WorkflowManager`。
//!
//! ## 实现特色
//!
//! - 双注册表设计：[`TypeRegistry`] 映射类型名到 `TypeId`，[`WorkflowFactoryRegistry`] 映射工作流名到工厂闭包
//! - 使用 serde + quick-xml 反序列化 XML，`@` 前缀映射属性名
//! - 10 种节点类型通过 XML 元素名区分（`<node>`、`<clone>`、`<conditional>` 等）
//! - 结构性节点支持属性语法：在 `<node>` 或 `<connect>` 上声明，由构建器自动展开为合成节点
//! - [`ConfigBuilder`] 在构建时桥接字符串名到 `TypeId` 和 `Box<dyn ErasedWorkflow>`
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | `quick-xml`（XML 反序列化）、`serde`（反序列化框架）、`thiserror`（错误派生） |
//! | 内部模块 | [`crate::workflow::dag::{DagBuilder, WorkflowDag}`]、[`crate::workflow::model::{NodeId, WorkflowId}`]、[`crate::workflow::error::WorkflowError`] |
//!
//! ## 两个注册表
//!
//! 配置文件用字符串引用类型和工作流，但 DAG 内部需要 `TypeId` 和
//! `Box<dyn ErasedWorkflow>`。通过两个注册表桥接：
//!
//! - [`TypeRegistry`] — 映射类型名字符串到 `TypeId` + `CloneFn`
//! - [`WorkflowFactoryRegistry`] — 映射工作流名字符串到工厂闭包
//!
//! ## XML 配置结构
//!
//! ```xml
//! <workflow name="my_pipeline" entry="add1" exit="mul2">
//!   <node name="add1" implementation="add_one"/>
//!   <node name="mul2" implementation="mul_two"/>
//!   <connect from="add1" to="mul2"/>
//! </workflow>
//! ```
//!
//! ## 节点类型
//!
//! ### Workflow 节点（独立 XML 元素）
//!
//! | XML 元素 | 必填属性 | 说明 |
//! |----------|----------|------|
//! | `<node>` | `name`, `implementation` | 引用注册的工作流工厂 |
//! | `<clone>` | `name`, `type`, `output-type`, `gather` | Scatter-gather：并行分支 + gather |
//! | `<conditional>` | `name`, `implementation` | 谓词工作流，必须输出 `bool` |
//! | `<loop>` | `name`, `count`, `body-entry`, `body-exit` | 循环体节点在同一 DAG 内 |
//! | `<sub-workflow>` | `name`, `workflow`, `input-type`, `output-type` | 引用其他已注册工作流 |
//!
//! ### 结构性节点（支持属性语法）
//!
//! | 操作 | 独立元素语法 | 属性语法 |
//! |------|-------------|---------|
//! | Connection | `<connection name="j" label="x" type="i32"/>` | `<connect ... label="x" type="i32"/>` |
//! | Reshape | `<reshape name="r" reshape="fn"/>` | `<connect ... reshape="fn"/>` |
//! | Dispatch | `<dispatch name="d" output-count="2" dispatch="fn"/>` | `<node ... dispatch="fn" dispatch-count="2"/>` |
//! | SumMatch | `<sum-match ok-type="i32" err-type="String"/>` | `<node ... sum-match="i32/String"/>` |
//! | ProductJoin | `<product-join join="fn"/>` | `<node ... join="fn"/>` |
//!
//! 属性语法由构建器在内部展开为合成 `NodeKind` 节点，DAG 结构不变。
//!
//! ## 完整用法
//!
//! ```rust
//! use intelligent_subject::workflow::config::{ConfigBuilder, TypeRegistry, WorkflowFactoryRegistry};
//! use intelligent_subject::workflow::definition::{Workflow, into_erased};
//! use intelligent_subject::workflow::model::ExecutionContext;
//! use intelligent_subject::workflow::error::WorkflowError;
//! use async_trait::async_trait;
//!
//! struct AddOne;
//! #[async_trait]
//! impl Workflow<i32, i32> for AddOne {
//!     fn name(&self) -> &str { "add_one" }
//!     async fn execute(&self, input: i32, _ctx: &ExecutionContext)
//!         -> Result<i32, WorkflowError> { Ok(input + 1) }
//! }
//!
//! // 1. 创建注册表
//! let types = TypeRegistry::with_primitives();
//! let mut workflows = WorkflowFactoryRegistry::new();
//! workflows.register("add_one", || into_erased(AddOne));
//!
//! // 2. 从 XML 构建 DAG
//! let builder = ConfigBuilder::new(types, workflows);
//! let output = builder.build_from_str(r#"
//!     <workflow name="pipeline" entry="a" exit="b">
//!       <node name="a" implementation="add_one"/>
//!       <node name="b" implementation="add_one"/>
//!       <connect from="a" to="b"/>
//!     </workflow>
//! "#).unwrap();
//!
//! assert_eq!(output.id.as_str(), "pipeline");
//! assert_eq!(output.dag.topo_order().len(), 2);
//! ```

mod builder;
mod error;
mod registry;
mod schema;
mod workflow_registry;

pub use builder::ConfigBuilder;
pub use builder::BuildOutput;
pub use error::ConfigBuildError;
pub use registry::TypeRegistry;
pub use schema::{EdgeConfig, NodeConfig, ToolConfig, WorkflowConfig, WorkflowMeta};
pub use workflow_registry::WorkflowFactoryRegistry;
