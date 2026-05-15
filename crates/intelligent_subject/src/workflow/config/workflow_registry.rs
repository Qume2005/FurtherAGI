//! # 工作流工厂注册表（WorkflowFactoryRegistry）
//!
//! 将字符串工作流名映射到工厂闭包，用于从 XML 配置实例化工作流。
//!
//! ## 功能实现
//!
//! [`WorkflowFactoryRegistry`] 被 [`ConfigBuilder`](super::ConfigBuilder) 用来将 XML 配置中的
//! 工作流名字符串（如 `"add_one"`）解析为 `Box<dyn ErasedWorkflow>` 实例。
//!
//! 因为 `ErasedWorkflow` 不是 `Clone`，当 XML 配置中多个节点引用同一个工作流名时，
//! 工厂模式允许为每个节点创建独立的新实例。
//!
//! ## 实现特色
//!
//! - 工厂模式：每次调用 [`create()`](WorkflowFactoryRegistry::create) 都会调用工厂闭包生成新实例
//! - 支持同一工作流在多个节点中复用（每次创建独立的 `Box<dyn ErasedWorkflow>`）
//! - [`register()`](WorkflowFactoryRegistry::register) 接受泛型 `Fn` 闭包，
//!   必须满足 `Send + Sync + 'static`
//! - [`create()`](WorkflowFactoryRegistry::create) 在名字未注册时返回 `None`，
//!   由 `ConfigBuilder` 生成 `ConfigError::UnknownImpl`
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | 无 |
//! | 内部模块 | [`ErasedWorkflow`] |
//!
//! ## 示例
//!
//! **注册和创建工作流：**
//!
//! ```rust
//! use intelligent_subject::workflow::config::WorkflowFactoryRegistry;
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
//! let mut reg = WorkflowFactoryRegistry::new();
//! reg.register("add_one", || into_erased(AddOne));
//!
//! let wf = reg.create("add_one").unwrap();
//! assert_eq!(wf.name(), "add_one");
//! ```
//!
//! **多次创建独立实例：**
//!
//! ```rust
//! use intelligent_subject::workflow::config::WorkflowFactoryRegistry;
//! use intelligent_subject::workflow::definition::{Workflow, into_erased};
//! use intelligent_subject::workflow::model::ExecutionContext;
//! use intelligent_subject::workflow::error::WorkflowError;
//! use async_trait::async_trait;
//!
//! struct Double;
//! #[async_trait]
//! impl Workflow<i32, i32> for Double {
//!     fn name(&self) -> &str { "double" }
//!     async fn execute(&self, input: i32, _ctx: &ExecutionContext)
//!         -> Result<i32, WorkflowError> { Ok(input * 2) }
//! }
//!
//! let mut reg = WorkflowFactoryRegistry::new();
//! reg.register("double", || into_erased(Double));
//!
//! // 每次创建都是独立的实例
//! let wf1 = reg.create("double").unwrap();
//! let wf2 = reg.create("double").unwrap();
//! assert_eq!(wf1.name(), "double");
//! assert_eq!(wf2.name(), "double");
//!
//! // 未注册的名字返回 None
//! assert!(reg.create("nonexistent").is_none());
//! ```

use std::collections::HashMap;

use crate::workflow::definition::ErasedWorkflow;

/// 工作流工厂注册表：将字符串名映射到工厂闭包。
///
/// 每个条目存储 `Box<dyn Fn() -> Box<dyn ErasedWorkflow>>`，
/// 允许同一实现被多次实例化（因为 `ErasedWorkflow` 不是 `Clone`）。
///
/// # 示例
///
/// ```rust
/// use intelligent_subject::workflow::config::WorkflowFactoryRegistry;
/// use intelligent_subject::workflow::definition::{Workflow, into_erased};
/// use intelligent_subject::workflow::model::ExecutionContext;
/// use intelligent_subject::workflow::error::WorkflowError;
/// use async_trait::async_trait;
///
/// struct AddOne;
/// #[async_trait]
/// impl Workflow<i32, i32> for AddOne {
///     fn name(&self) -> &str { "add_one" }
///     async fn execute(&self, input: i32, _ctx: &ExecutionContext)
///         -> Result<i32, WorkflowError> { Ok(input + 1) }
/// }
///
/// let mut reg = WorkflowFactoryRegistry::new();
/// reg.register("add_one", || into_erased(AddOne));
///
/// let wf = reg.create("add_one").unwrap();
/// assert_eq!(wf.name(), "add_one");
/// ```
pub struct WorkflowFactoryRegistry {
    factories: HashMap<String, Box<dyn Fn() -> Box<dyn ErasedWorkflow> + Send + Sync>>,
}

impl WorkflowFactoryRegistry {
    /// 创建空的注册表。
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// 按名称注册工作流工厂。
    ///
    /// 每次调用 [`create`](Self::create) 时都会执行 `factory` 闭包，
    /// 生成新的 `Box<dyn ErasedWorkflow>` 实例。
    pub fn register<F>(&mut self, name: impl Into<String>, factory: F)
    where
        F: Fn() -> Box<dyn ErasedWorkflow> + Send + Sync + 'static,
    {
        self.factories.insert(name.into(), Box::new(factory));
    }

    /// 按名称创建新的工作流实例。
    ///
    /// 如果该名称未注册工厂，返回 `None`。
    pub fn create(&self, name: &str) -> Option<Box<dyn ErasedWorkflow>> {
        self.factories.get(name).map(|f| f())
    }

    /// 创建预注册所有内置工作流的注册表。
    ///
    /// 调用后，XML 配置可直接引用 `"add_one"`、`"mul_two"`、`"is_positive"` 等名称。
    pub fn with_builtins() -> Self {
        let mut reg = Self::new();
        crate::workflow::builtin::register_builtins(&mut reg);
        reg
    }
}

impl Default for WorkflowFactoryRegistry {
    fn default() -> Self {
        Self::new()
    }
}
