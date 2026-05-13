//! # 工作流工厂注册表（WorkflowFactoryRegistry）
//!
//! 将字符串工作流名映射到工厂闭包，用于从 TOML 配置实例化工作流。
//!
//! ## 功能实现
//!
//! [`WorkflowFactoryRegistry`] 被 [`ConfigBuilder`](super::ConfigBuilder) 用来将 TOML 配置中的
//! 工作流名字符串（如 `"add_one"`）解析为 `Box<dyn ErasedWorkflow>` 实例。
//!
//! 因为 `ErasedWorkflow` 不是 `Clone`，当 TOML 配置中多个节点引用同一个工作流名时，
//! 工厂模式允许为每个节点创建独立的新实例。
//!
//! ## 实现特色
//!
//! - 工厂模式：每次调用 [`create()`](WorkflowFactoryRegistry::create) 都会调用工厂闭包生成新实例
//! - 支持同一工作流在多个节点中复用（每次创建独立的 `Box<dyn ErasedWorkflow>`）
//! - [`register()`](WorkflowFactoryRegistry::register) 接受泛型 `Fn` 闭包，
//!   必须满足 `Send + Sync + 'static`
//! - [`create()`](WorkflowFactoryRegistry::create) 在名字未注册时返回 `None`，
//!   由 `ConfigBuilder` 生成 `ConfigBuildError::UnknownWorkflow`
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | 无 |
//! | 内部模块 | [`crate::workflow::definition::ErasedWorkflow`] |
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

/// Registry mapping string names to workflow factory closures.
///
/// Each entry stores a `Box<dyn Fn() -> Box<dyn ErasedWorkflow>>`, allowing
/// the same implementation to be instantiated multiple times (since
/// `ErasedWorkflow` is not `Clone`).
///
/// # Example
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
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// Register a workflow factory by name.
    ///
    /// The `factory` closure is called each time [`create`](Self::create)
    /// is invoked with this name, producing a fresh `Box<dyn ErasedWorkflow>`.
    pub fn register<F>(&mut self, name: impl Into<String>, factory: F)
    where
        F: Fn() -> Box<dyn ErasedWorkflow> + Send + Sync + 'static,
    {
        self.factories.insert(name.into(), Box::new(factory));
    }

    /// Create a new workflow instance by name.
    ///
    /// Returns `None` if no factory was registered under this name.
    pub fn create(&self, name: &str) -> Option<Box<dyn ErasedWorkflow>> {
        self.factories.get(name).map(|f| f())
    }

    /// Create a registry pre-loaded with all builtin workflows.
    ///
    /// After calling this, XML configs can reference names like
    /// `"add_one"`, `"mul_two"`, `"is_positive"`, etc. directly.
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
