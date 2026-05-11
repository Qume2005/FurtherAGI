//! # 配置驱动的工作流构建
//!
//! 从 TOML 配置文件声明式构建工作流 DAG，然后注册到 `WorkflowManager`。
//!
//! ## 两个注册表
//!
//! 配置文件用字符串引用类型和工作流，但 DAG 内部需要 `TypeId` 和
//! `Box<dyn ErasedWorkflow>`。通过两个注册表桥接：
//!
//! - [`TypeRegistry`] — 映射类型名字符串到 `TypeId` + `CloneFn`
//! - [`WorkflowFactoryRegistry`] — 映射工作流名字符串到工厂闭包
//!
//! ## TOML 配置结构
//!
//! ```toml
//! [workflow]
//! name = "my_pipeline"
//! entry = "add1"
//! exit = "mul2"
//!
//! [nodes.add1]
//! kind = "workflow"
//! implementation = "add_one"
//!
//! [nodes.mul2]
//! kind = "workflow"
//! implementation = "mul_two"
//!
//! [[edges]]
//! from = "add1"
//! to = "mul2"
//! ```
//!
//! ## 节点类型
//!
//! | `kind` | 必填字段 | 说明 |
//! |--------|----------|------|
//! | `"workflow"` | `implementation` | 引用注册的工作流工厂 |
//! | `"broadcast"` | `type` | 引用注册的类型名 |
//! | `"conditional"` | `implementation` | 谓词工作流，必须输出 `bool` |
//! | `"loop"` | `count`, `body_entry`, `body_exit` | 循环体节点在同一 DAG 内 |
//! | `"error_handler"` | `paired_with`, `implementation` | 配对节点名 |
//! | `"sub_workflow"` | `workflow`, `input_type`, `output_type` | 引用其他已注册工作流 |
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
//! // 2. 从 TOML 构建 DAG
//! let builder = ConfigBuilder::new(types, workflows);
//! let (workflow_id, dag) = builder.build_from_str(r#"
//!     [workflow]
//!     name = "pipeline"
//!     entry = "a"
//!     exit = "b"
//!
//!     [nodes.a]
//!     kind = "workflow"
//!     implementation = "add_one"
//!
//!     [nodes.b]
//!     kind = "workflow"
//!     implementation = "add_one"
//!
//!     [[edges]]
//!     from = "a"
//!     to = "b"
//! "#).unwrap();
//!
//! // 3. 注册到 WorkflowManager
//! // mgr.register_composite(workflow_id, dag).unwrap();
//! ```

mod builder;
mod error;
mod registry;
mod schema;
mod workflow_registry;

pub use builder::ConfigBuilder;
pub use error::ConfigBuildError;
pub use registry::TypeRegistry;
pub use schema::{EdgeConfig, NodeConfig, WorkflowConfig, WorkflowMeta};
pub use workflow_registry::WorkflowFactoryRegistry;
