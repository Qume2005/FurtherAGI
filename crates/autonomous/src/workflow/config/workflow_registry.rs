//! Workflow factory registry mapping string names to workflow constructors.
//!
//! Used by [`ConfigBuilder`](super::ConfigBuilder) to instantiate workflows
//! from TOML configuration. Uses factory closures so the same implementation
//! can be referenced in multiple nodes.

use std::collections::HashMap;

use crate::workflow::traits::ErasedWorkflow;

/// Registry mapping string names to workflow factory closures.
///
/// Each entry stores a `Box<dyn Fn() -> Box<dyn ErasedWorkflow>>`, allowing
/// the same implementation to be instantiated multiple times (since
/// `ErasedWorkflow` is not `Clone`).
///
/// # Example
///
/// ```rust
/// use autonomous::workflow::config::WorkflowFactoryRegistry;
/// use autonomous::workflow::traits::{Workflow, into_erased};
/// use autonomous::workflow::types::ExecutionContext;
/// use autonomous::workflow::error::WorkflowError;
/// use async_trait::async_trait;
///
/// struct AddOne;
/// #[async_trait]
/// impl Workflow<i32, i32> for AddOne {
///     fn name(&self) -> &str { "add_one" }
///     async fn execute(&self, input: i32, _ctx: &ExecutionContext<'_>)
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
}

impl Default for WorkflowFactoryRegistry {
    fn default() -> Self {
        Self::new()
    }
}
