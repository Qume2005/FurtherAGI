//! Configuration-driven DAG builder.
//!
//! Reads a [`WorkflowConfig`] (parsed from TOML) and produces a `WorkflowDag`
//! using the provided [`TypeRegistry`] and [`WorkflowFactoryRegistry`].

use std::collections::HashMap;
use std::path::Path;

use crate::workflow::dag::{DagBuilder, WorkflowDag};
use crate::workflow::types::{NodeId, WorkflowId};

use super::error::ConfigBuildError;
use super::type_registry::TypeRegistry;
use super::types::{EdgeConfig, NodeConfig, WorkflowConfig};
use super::workflow_registry::WorkflowFactoryRegistry;

/// Builds a `WorkflowDag` from TOML configuration using registered types and workflows.
///
/// # Example
///
/// ```rust
/// use autonomous::workflow::config::{ConfigBuilder, TypeRegistry, WorkflowFactoryRegistry};
/// use autonomous::workflow::traits::{Workflow, into_erased};
/// use autonomous::workflow::types::ExecutionContext;
/// use autonomous::workflow::error::WorkflowError;
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
/// let types = TypeRegistry::with_primitives();
/// let mut workflows = WorkflowFactoryRegistry::new();
/// workflows.register("add_one", || into_erased(AddOne));
///
/// let builder = ConfigBuilder::new(types, workflows);
/// let toml = r#"
///     [workflow]
///     name = "pipeline"
///     entry = "a"
///     exit = "b"
///
///     [nodes.a]
///     kind = "workflow"
///     implementation = "add_one"
///
///     [nodes.b]
///     kind = "workflow"
///     implementation = "add_one"
///
///     [[edges]]
///     from = "a"
///     to = "b"
/// "#;
/// let (id, dag) = builder.build_from_str(toml).unwrap();
/// assert_eq!(id.as_str(), "pipeline");
/// ```
pub struct ConfigBuilder {
    types: TypeRegistry,
    workflows: WorkflowFactoryRegistry,
}

impl ConfigBuilder {
    /// Create a new config builder with the given registries.
    pub fn new(types: TypeRegistry, workflows: WorkflowFactoryRegistry) -> Self {
        Self { types, workflows }
    }

    /// Build a `WorkflowDag` from a parsed [`WorkflowConfig`].
    pub fn build(&self, config: WorkflowConfig) -> Result<(WorkflowId, WorkflowDag), ConfigBuildError> {
        let workflow_id = WorkflowId::from(&config.workflow.name);
        let mut dag_builder = DagBuilder::new();
        let mut name_map: HashMap<String, NodeId> = HashMap::new();

        // Deferred nodes that reference other nodes by name.
        let mut deferred_loops: Vec<(String, usize, String, String)> = Vec::new();
        let mut deferred_error_handlers: Vec<(String, String, String)> = Vec::new();

        // Pass 1: Add all nodes that don't reference other nodes.
        for (name, node_cfg) in &config.nodes {
            let node_id = match node_cfg {
                NodeConfig::Workflow { implementation } => {
                    let wf = self.workflows.create(implementation).ok_or_else(|| {
                        ConfigBuildError::UnknownWorkflow {
                            node: name.clone(),
                            name: implementation.clone(),
                        }
                    })?;
                    dag_builder.add_workflow(WorkflowId::from(name), wf)
                }
                NodeConfig::Broadcast { type_name } => {
                    let (type_id, clone_fn) = self.types.get(type_name).ok_or_else(|| {
                        ConfigBuildError::UnknownType {
                            node: name.clone(),
                            name: type_name.clone(),
                        }
                    })?;
                    dag_builder.add_broadcast_erased(type_id, clone_fn)
                }
                NodeConfig::Connection { label, type_name } => {
                    let (type_id, _) = self.types.get(type_name).ok_or_else(|| {
                        ConfigBuildError::UnknownType {
                            node: name.clone(),
                            name: type_name.clone(),
                        }
                    })?;
                    dag_builder.add_connection(label, type_id)
                }
                NodeConfig::Conditional { implementation } => {
                    let pred = self.workflows.create(implementation).ok_or_else(|| {
                        ConfigBuildError::UnknownWorkflow {
                            node: name.clone(),
                            name: implementation.clone(),
                        }
                    })?;
                    dag_builder.add_conditional(WorkflowId::from(name), pred)?
                }
                NodeConfig::SubWorkflow {
                    workflow,
                    input_type,
                    output_type,
                } => {
                    let in_ty = self.types.get(input_type).map(|(id, _)| id).ok_or_else(|| {
                        ConfigBuildError::UnknownType {
                            node: name.clone(),
                            name: input_type.clone(),
                        }
                    })?;
                    let out_ty = self.types.get(output_type).map(|(id, _)| id).ok_or_else(|| {
                        ConfigBuildError::UnknownType {
                            node: name.clone(),
                            name: output_type.clone(),
                        }
                    })?;
                    dag_builder.add_sub_workflow(WorkflowId::from(workflow), in_ty, out_ty)
                }
                NodeConfig::Loop {
                    count,
                    body_entry,
                    body_exit,
                } => {
                    // Defer: body nodes may not exist yet.
                    deferred_loops.push((
                        name.clone(),
                        *count,
                        body_entry.clone(),
                        body_exit.clone(),
                    ));
                    continue;
                }
                NodeConfig::ErrorHandler {
                    paired_with,
                    implementation,
                } => {
                    // Defer: paired_with node may not exist yet.
                    deferred_error_handlers.push((
                        name.clone(),
                        paired_with.clone(),
                        implementation.clone(),
                    ));
                    continue;
                }
            };
            name_map.insert(name.clone(), node_id);
        }

        // Pass 2a: Add loop nodes (body nodes now in name_map).
        for (name, count, body_entry, body_exit) in deferred_loops {
            let entry_id = name_map.get(&body_entry).copied().ok_or_else(|| {
                ConfigBuildError::UnknownNode(body_entry.clone())
            })?;
            let exit_id = name_map.get(&body_exit).copied().ok_or_else(|| {
                ConfigBuildError::UnknownNode(body_exit.clone())
            })?;
            let node_id = dag_builder.add_loop(count, entry_id, exit_id)?;
            name_map.insert(name, node_id);
        }

        // Pass 2b: Add error handler nodes (paired_with now in name_map).
        for (name, paired_with, implementation) in deferred_error_handlers {
            let paired_id = name_map.get(&paired_with).copied().ok_or_else(|| {
                ConfigBuildError::UnknownNode(paired_with.clone())
            })?;
            let handler = self.workflows.create(&implementation).ok_or_else(|| {
                ConfigBuildError::UnknownWorkflow {
                    node: name.clone(),
                    name: implementation.clone(),
                }
            })?;
            let node_id = dag_builder.add_error_handler(paired_id, handler)?;
            name_map.insert(name, node_id);
        }

        // Pass 3: Add edges.
        for EdgeConfig { from, to, label } in &config.edges {
            let from_id = name_map.get(from).copied().ok_or_else(|| {
                ConfigBuildError::UnknownNode(from.clone())
            })?;
            let to_id = name_map.get(to).copied().ok_or_else(|| {
                ConfigBuildError::UnknownNode(to.clone())
            })?;
            if let Some(label) = label {
                dag_builder.connect_labeled(from_id, to_id, label)?;
            } else {
                dag_builder.connect(from_id, to_id)?;
            }
        }

        // Set entry and exit.
        let entry_id = name_map.get(&config.workflow.entry).copied().ok_or_else(|| {
            ConfigBuildError::UnknownNode(config.workflow.entry.clone())
        })?;
        let exit_id = name_map.get(&config.workflow.exit).copied().ok_or_else(|| {
            ConfigBuildError::UnknownNode(config.workflow.exit.clone())
        })?;
        dag_builder.set_entry(entry_id)?;
        dag_builder.set_exit(exit_id)?;

        // Build (performs cycle detection).
        let dag = dag_builder.build()?;
        Ok((workflow_id, dag))
    }

    /// Parse a TOML string and build a `WorkflowDag`.
    pub fn build_from_str(&self, toml_str: &str) -> Result<(WorkflowId, WorkflowDag), ConfigBuildError> {
        let config: WorkflowConfig = toml::from_str(toml_str)?;
        self.build(config)
    }

    /// Read a TOML file and build a `WorkflowDag`.
    pub fn build_from_file(
        &self,
        path: &Path,
    ) -> Result<(WorkflowId, WorkflowDag), ConfigBuildError> {
        let toml_str = std::fs::read_to_string(path)?;
        self.build_from_str(&toml_str)
    }
}
