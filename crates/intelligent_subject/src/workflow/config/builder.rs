//! # XML 配置驱动 DAG 构建器
//!
//! 从 XML 配置构建 [`WorkflowDag`](WorkflowDag)，
//! 使用 [`TypeRegistry`](TypeRegistry) 解析类型名，
//! 使用 [`WorkflowFactoryRegistry`](WorkflowFactoryRegistry) 实例化工作流。
//!
//! ## 功能实现
//!
//! [`ConfigBuilder`] 通过 serde + quick-xml 反序列化 XML 配置，
//! 然后通过四遍构建流程将 XML 元素转化为 DAG：
//!
//! 1. **Pass 1.5** — 展开 `<node>` 和 `<connect>` 上的结构性属性为合成节点
//!    （dispatch、sum-match、join、reshape、connection 属性）
//! 2. **Pass 1** — 添加独立节点（`<node>`、`<clone>`、`<conditional>` 等）
//! 3. **Pass 2** — 添加前向引用节点（`<loop>`）
//! 4. **Pass 3** — 添加边（含重写后的合成边）
//!
//! ## 实现特色
//!
//! - 使用 serde 反序列化 XML，`@` 前缀映射属性名
//! - 通过 `$value` + 枚举支持交错排列的异构子元素
//! - 属性语法由 [`expand_attributes`](ConfigBuilder::expand_attributes) 展开为合成节点
//! - 三遍构建策略解决前向引用问题
//! - `build_from_str()` 直接解析 XML 字符串，`build_from_file()` 从文件读取
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | `quick-xml`（XML 反序列化）、`serde`（反序列化框架） |
//! | 内部模块 | [`dag::{DagBuilder, WorkflowDag}`]、[`model::{NodeId, WorkflowId}`]、[`error::WorkflowError`] |
//!
//! ## 示例
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
//! let types = TypeRegistry::with_primitives();
//! let mut workflows = WorkflowFactoryRegistry::new();
//! workflows.register("add_one", || into_erased(AddOne));
//!
//! let builder = ConfigBuilder::new(types, workflows);
//! let xml = r#"
//!     <workflow name="pipeline" entry="a" exit="b">
//!       <node name="a" implementation="add_one"/>
//!       <node name="b" implementation="add_one"/>
//!       <connect from="a" to="b"/>
//!     </workflow>"#;
//! let output = builder.build_from_str(xml).unwrap();
//! assert_eq!(output.id.as_str(), "pipeline");
//! ```

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;

use crate::workflow::dag::{
    CloneFn, DagBuilder, ProductJoinFn, WorkflowDag,
};
use crate::workflow::model::{NodeId, WorkflowId};

use super::error::ConfigBuildError;
use super::registry::TypeRegistry;
use super::schema::{EdgeConfig, NodeConfig, WorkflowConfig, WorkflowMeta};
use super::workflow_registry::WorkflowFactoryRegistry;

// ── XML 反序列化结构体 ──────────────────────────────────────────

/// `<workflow>` 根元素。
///
/// 使用 `$value` 捕获所有子元素，通过 [`ChildXml`] 枚举按元素名区分类型，
/// 支持异构子元素交错排列。
#[derive(Deserialize)]
struct WorkflowXml {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@entry")]
    entry: String,
    #[serde(rename = "@exit")]
    exit: String,

    #[serde(rename = "$value", default)]
    children: Vec<ChildXml>,
}

/// `<workflow>` 子元素枚举，通过元素名映射到对应变体。
#[derive(Deserialize)]
enum ChildXml {
    #[serde(rename = "node")]
    Node(NodeXml),
    #[serde(rename = "clone")]
    Clone(CloneXml),
    #[serde(rename = "conditional")]
    Conditional(ConditionalXml),
    #[serde(rename = "connection")]
    Connection(ConnectionXml),
    #[serde(rename = "loop")]
    LoopNode(LoopXml),
    #[serde(rename = "sub-workflow")]
    SubWorkflow(SubWorkflowXml),
    #[serde(rename = "sum-match")]
    SumMatch(SumMatchXml),
    #[serde(rename = "product-join")]
    ProductJoin(ProductJoinXml),
    #[serde(rename = "reshape")]
    Reshape(ReshapeXml),
    #[serde(rename = "dispatch")]
    Dispatch(DispatchXml),
    #[serde(rename = "tool")]
    Tool(ToolXml),
    #[serde(rename = "connect")]
    Connect(ConnectXml),
}

#[derive(Deserialize)]
struct NodeXml {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@implementation")]
    implementation: String,
    /// Dispatch 属性：已注册的 dispatch 函数名。
    #[serde(rename = "@dispatch")]
    dispatch: Option<String>,
    /// Dispatch 属性：输出数量。
    #[serde(rename = "@dispatch-count")]
    dispatch_count: Option<usize>,
    /// SumMatch 属性：格式 `"OkType/ErrType"`。
    #[serde(rename = "@sum-match")]
    sum_match: Option<String>,
    /// ProductJoin 属性：已注册的 join 函数名。
    #[serde(rename = "@join")]
    join: Option<String>,
}

#[derive(Deserialize)]
struct CloneXml {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@type")]
    type_name: String,
    #[serde(rename = "@output-type")]
    output_type_name: String,
    #[serde(rename = "@gather")]
    gather_name: String,
    #[serde(rename = "$value", default)]
    children: Vec<CloneChildXml>,
}

#[derive(Deserialize)]
enum CloneChildXml {
    #[serde(rename = "branch")]
    Branch(BranchXml),
}

#[derive(Deserialize)]
struct BranchXml {
    #[serde(rename = "@implementation")]
    implementation: String,
}

#[derive(Deserialize)]
struct ConditionalXml {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@implementation")]
    implementation: String,
}

#[derive(Deserialize)]
struct ConnectionXml {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@label")]
    label: String,
    #[serde(rename = "@type")]
    type_name: String,
}

#[derive(Deserialize)]
struct LoopXml {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@count")]
    count: usize,
    #[serde(rename = "@body-entry")]
    body_entry: String,
    #[serde(rename = "@body-exit")]
    body_exit: String,
}

#[derive(Deserialize)]
struct SubWorkflowXml {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@workflow")]
    workflow: String,
    #[serde(rename = "@input-type")]
    input_type: String,
    #[serde(rename = "@output-type")]
    output_type: String,
}

#[derive(Deserialize)]
struct SumMatchXml {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@ok-type")]
    ok_type_name: String,
    #[serde(rename = "@err-type")]
    err_type_name: String,
}

#[derive(Deserialize)]
struct ProductJoinXml {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@output-type")]
    output_type_name: String,
    #[serde(rename = "@input-types")]
    input_type_names: String,
    #[serde(rename = "@join")]
    join_name: String,
}

#[derive(Deserialize)]
struct ReshapeXml {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@reshape")]
    reshape_name: String,
}

#[derive(Deserialize)]
struct DispatchXml {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@output-count")]
    output_count: usize,
    #[serde(rename = "@dispatch")]
    dispatch_name: String,
}

#[derive(Deserialize)]
struct ConnectXml {
    #[serde(rename = "@from")]
    from: String,
    #[serde(rename = "@to")]
    to: String,
    #[serde(rename = "@label")]
    label: Option<String>,
    /// Connection 属性：透传值的类型名。
    #[serde(rename = "@type")]
    type_name: Option<String>,
    /// Reshape 属性：已注册的 reshape 函数名。
    #[serde(rename = "@reshape")]
    reshape: Option<String>,
}

#[derive(Deserialize)]
struct ToolXml {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@description")]
    description: String,
    #[serde(rename = "@implementation")]
    implementation: String,
    #[serde(rename = "@input-type")]
    input_type: String,
    #[serde(rename = "@output-type")]
    output_type: String,
    #[serde(rename = "@parameters")]
    parameters: String,
}

impl From<WorkflowXml> for WorkflowConfig {
    fn from(xml: WorkflowXml) -> Self {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut tools = Vec::new();

        for child in xml.children {
            match child {
                ChildXml::Node(n) => {
                    nodes.push((n.name, NodeConfig::Workflow {
                        implementation: n.implementation,
                        dispatch_name: n.dispatch,
                        dispatch_count: n.dispatch_count,
                        sum_match: n.sum_match,
                        join_name: n.join,
                    }));
                }
                ChildXml::Clone(n) => {
                    let branches: Vec<String> = n.children.into_iter()
                        .map(|c| match c {
                            CloneChildXml::Branch(b) => b.implementation,
                        })
                        .collect();
                    nodes.push((n.name, NodeConfig::Clone {
                        type_name: n.type_name,
                        output_type_name: n.output_type_name,
                        branches,
                        gather_name: n.gather_name,
                    }));
                }
                ChildXml::Conditional(n) => {
                    nodes.push((n.name, NodeConfig::Conditional { implementation: n.implementation }));
                }
                ChildXml::Connection(n) => {
                    nodes.push((n.name, NodeConfig::Connection { label: n.label, type_name: n.type_name }));
                }
                ChildXml::LoopNode(n) => {
                    nodes.push((n.name, NodeConfig::Loop {
                        count: n.count,
                        body_entry: n.body_entry,
                        body_exit: n.body_exit,
                    }));
                }
                ChildXml::SubWorkflow(n) => {
                    nodes.push((n.name, NodeConfig::SubWorkflow {
                        workflow: n.workflow,
                        input_type: n.input_type,
                        output_type: n.output_type,
                    }));
                }
                ChildXml::SumMatch(n) => {
                    nodes.push((n.name, NodeConfig::SumMatch {
                        ok_type_name: n.ok_type_name,
                        err_type_name: n.err_type_name,
                    }));
                }
                ChildXml::ProductJoin(n) => {
                    nodes.push((n.name, NodeConfig::ProductJoin {
                        output_type_name: n.output_type_name,
                        input_type_names: n.input_type_names,
                        join_name: n.join_name,
                    }));
                }
                ChildXml::Reshape(n) => {
                    nodes.push((n.name, NodeConfig::Reshape {
                        reshape_name: n.reshape_name,
                    }));
                }
                ChildXml::Dispatch(n) => {
                    nodes.push((n.name, NodeConfig::Dispatch {
                        output_count: n.output_count,
                        dispatch_name: n.dispatch_name,
                    }));
                }
                ChildXml::Connect(c) => {
                    edges.push(EdgeConfig {
                        from: c.from,
                        to: c.to,
                        label: c.label,
                        type_name: c.type_name,
                        reshape_name: c.reshape,
                    });
                }
                ChildXml::Tool(t) => {
                    let parameters: serde_json::Value = serde_json::from_str(&t.parameters)
                        .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
                    tools.push(super::schema::ToolConfig {
                        name: t.name,
                        description: t.description,
                        implementation: t.implementation,
                        input_type: t.input_type,
                        output_type: t.output_type,
                        parameters,
                    });
                }
            }
        }

        WorkflowConfig {
            workflow: WorkflowMeta { name: xml.name, entry: xml.entry, exit: xml.exit },
            nodes,
            edges,
            tools,
        }
    }
}

// ── ConfigBuilder ──────────────────────────────────────────────

/// Sum-match 工厂：将类型名字符串对映射到解构函数。
struct SumMatchFactory {
    ok_type: TypeId,
    err_type: TypeId,
    destruct_fn: Arc<dyn Fn(Box<dyn Any + Send + Sync>) -> Result<crate::workflow::dag::SumMatchResult, crate::workflow::error::WorkflowError> + Send + Sync>,
}

/// Product-join 工厂：将注册名映射到合并函数及其输入信息。
struct ProductJoinFactory {
    output_type: TypeId,
    input_clone_fns: Vec<CloneFn>,
    join_fn: Arc<dyn Fn(Vec<Box<dyn Any + Send + Sync>>) -> Box<dyn Any + Send + Sync> + Send + Sync>,
}

/// Clone scatter-gather 工厂：将注册名映射到 gather 函数和输出类型。
struct CloneGatherFactory {
    output_type: TypeId,
    gather_fn: Arc<dyn Fn(Vec<Box<dyn Any + Send + Sync>>) -> Box<dyn Any + Send + Sync> + Send + Sync>,
}

/// Reshape 工厂：将注册名映射到 reshape 函数。
struct ReshapeFactory {
    reshape_fn: Arc<dyn Fn(Box<dyn Any + Send + Sync>) -> Box<dyn Any + Send + Sync> + Send + Sync>,
}

/// Dispatch 工厂：将注册名映射到 dispatch 函数。
struct DispatchFactory {
    output_count: usize,
    dispatch_fn: Arc<dyn Fn(Box<dyn Any + Send + Sync>) -> Vec<Box<dyn Any + Send + Sync>> + Send + Sync>,
}

/// 从 XML 配置构建 [`WorkflowDag`]。
///
/// 使用 [`TypeRegistry`] 解析类型名字符串，
/// 使用 [`WorkflowFactoryRegistry`] 实例化工作流，
/// 使用 Sum-match/Product-join 工厂创建和/积类型节点。
///
/// # Example
///
/// ```rust
/// use intelligent_subject::workflow::config::{ConfigBuilder, TypeRegistry, WorkflowFactoryRegistry};
/// use intelligent_subject::workflow::definition::{Workflow, into_erased};
/// use intelligent_subject::workflow::model::ExecutionContext;
/// use intelligent_subject::workflow::error::WorkflowError;
/// use async_trait::async_trait;
///
/// struct Double;
/// #[async_trait]
/// impl Workflow<i32, i32> for Double {
///     fn name(&self) -> &str { "double" }
///     async fn execute(&self, input: i32, _ctx: &ExecutionContext)
///         -> Result<i32, WorkflowError> { Ok(input * 2) }
/// }
///
/// let types = TypeRegistry::with_primitives();
/// let mut wfs = WorkflowFactoryRegistry::new();
/// wfs.register("double", || into_erased(Double));
///
/// let builder = ConfigBuilder::new(types, wfs);
/// let output = builder.build_from_str(r#"
///     <workflow name="test" entry="a" exit="a">
///       <node name="a" implementation="double"/>
///     </workflow>"#).unwrap();
/// assert_eq!(output.id.as_str(), "test");
/// assert_eq!(output.dag.topo_order().len(), 1);
/// ```
pub struct ConfigBuilder {
    types: TypeRegistry,
    workflows: WorkflowFactoryRegistry,
    sum_match_factories: HashMap<(String, String), SumMatchFactory>,
    product_join_factories: HashMap<String, ProductJoinFactory>,
    clone_gather_factories: HashMap<String, CloneGatherFactory>,
    reshape_factories: HashMap<String, ReshapeFactory>,
    dispatch_factories: HashMap<String, DispatchFactory>,
}

impl ConfigBuilder {
    /// Create a new config builder with the given registries.
    pub fn new(types: TypeRegistry, workflows: WorkflowFactoryRegistry) -> Self {
        Self {
            types,
            workflows,
            sum_match_factories: HashMap::new(),
            product_join_factories: HashMap::new(),
            clone_gather_factories: HashMap::new(),
            reshape_factories: HashMap::new(),
            dispatch_factories: HashMap::new(),
        }
    }

    /// Register a sum-match factory for the given ok/err type name pair.
    ///
    /// After registration, `<sum-match ok-type="..." err-type="..."/>` with matching
    /// type names will use this factory to create the destructuring function.
    pub fn register_sum_match<T: Send + Sync + 'static, E: Send + Sync + 'static>(
        &mut self,
        ok_type_name: impl Into<String>,
        err_type_name: impl Into<String>,
    ) {
        let ok_name = ok_type_name.into();
        let err_name = err_type_name.into();
        self.sum_match_factories.insert(
            (ok_name, err_name),
            SumMatchFactory {
                ok_type: TypeId::of::<T>(),
                err_type: TypeId::of::<E>(),
                destruct_fn: Arc::new(crate::workflow::dag::make_sum_match_destruct_fn::<T, E>()),
            },
        );
    }

    /// Register a product-join factory by name.
    ///
    /// After registration, `<product-join join="..."/>` with matching name
    /// will use this factory to create the join function.
    pub fn register_product_join(
        &mut self,
        name: impl Into<String>,
        output_type: TypeId,
        input_clone_fns: Vec<CloneFn>,
        join_fn: ProductJoinFn,
    ) {
        self.product_join_factories.insert(
            name.into(),
            ProductJoinFactory {
                output_type,
                input_clone_fns,
                join_fn: Arc::from(join_fn),
            },
        );
    }

    /// Register a clone scatter-gather factory by name.
    ///
    /// After registration, `<clone gather="...">` with matching name
    /// will use this factory to create the gather function.
    pub fn register_clone_gather(
        &mut self,
        name: impl Into<String>,
        output_type: TypeId,
        gather_fn: ProductJoinFn,
    ) {
        self.clone_gather_factories.insert(
            name.into(),
            CloneGatherFactory {
                output_type,
                gather_fn: Arc::from(gather_fn),
            },
        );
    }

    /// Register a reshape factory by name.
    ///
    /// After registration, `<reshape reshape="..."/>` with matching name
    /// will use this factory to create the reshape function.
    pub fn register_reshape(
        &mut self,
        name: impl Into<String>,
        reshape_fn: crate::workflow::dag::ReshapeFn,
    ) {
        self.reshape_factories.insert(
            name.into(),
            ReshapeFactory {
                reshape_fn: Arc::from(reshape_fn),
            },
        );
    }

    /// Register a dispatch factory by name.
    ///
    /// After registration, `<dispatch dispatch="..."/>` with matching name
    /// will use this factory to create the dispatch function.
    pub fn register_dispatch(
        &mut self,
        name: impl Into<String>,
        output_count: usize,
        dispatch_fn: crate::workflow::dag::DispatchFn,
    ) {
        self.dispatch_factories.insert(
            name.into(),
            DispatchFactory {
                output_count,
                dispatch_fn: Arc::from(dispatch_fn),
            },
        );
    }

    /// Build a `WorkflowDag` from a parsed [`WorkflowConfig`].
    pub fn build(&self, config: WorkflowConfig) -> Result<BuildOutput, ConfigBuildError> {
        let workflow_id = WorkflowId::from(&config.workflow.name);
        let mut dag_builder = DagBuilder::new();
        let mut name_map: HashMap<String, NodeId> = HashMap::new();

        // ── Pass 1.5: Expand attribute-based structural ops into synthetic nodes ──
        //
        // For <node> with dispatch/sum-match/join attributes, or <connect> with
        // reshape/type attributes, we expand them into additional NodeConfig / EdgeConfig
        // entries so that Pass 2+ can treat everything uniformly.

        let (expanded_nodes, expanded_edges) = Self::expand_attributes(config.nodes, config.edges)?;

        // Deferred nodes that reference other nodes by name.
        let mut deferred_loops: Vec<(String, usize, String, String)> = Vec::new();

        // Pass 1: Add all nodes that don't reference other nodes.
        for (name, node_cfg) in &expanded_nodes {
            let node_id = match node_cfg {
                NodeConfig::Workflow { implementation, dispatch_name: _, dispatch_count: _, sum_match: _, join_name: _ } => {
                    let wf = self.workflows.create(implementation).ok_or_else(|| {
                        ConfigBuildError::UnknownWorkflow {
                            node: name.clone(),
                            name: implementation.clone(),
                        }
                    })?;
                    dag_builder.add_workflow(WorkflowId::from(name), wf)
                }
                NodeConfig::Clone { type_name, output_type_name: _, branches, gather_name } => {
                    let (input_type, input_clone_fn) = self.types.get(type_name).ok_or_else(|| {
                        ConfigBuildError::UnknownType {
                            node: name.clone(),
                            name: type_name.clone(),
                        }
                    })?;
                    let branch_workflows: Vec<_> = branches.iter()
                        .map(|impl_name| self.workflows.create(impl_name).ok_or_else(|| {
                            ConfigBuildError::UnknownWorkflow {
                                node: name.clone(),
                                name: impl_name.clone(),
                            }
                        }))
                        .collect::<Result<_, _>>()?;
                    let gather_factory = self.clone_gather_factories.get(gather_name).ok_or_else(|| {
                        ConfigBuildError::UnknownCloneGather {
                            node: name.clone(),
                            name: gather_name.clone(),
                        }
                    })?;
                    let gather_fn = gather_factory.gather_fn.clone();
                    dag_builder.add_scatter_gather(
                        input_type,
                        input_clone_fn,
                        branch_workflows,
                        Box::new(move |vals| gather_fn(vals)),
                        gather_factory.output_type,
                    )
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
                NodeConfig::SumMatch {
                    ok_type_name,
                    err_type_name,
                } => {
                    let factory = self.sum_match_factories.get(&(ok_type_name.clone(), err_type_name.clone())).ok_or_else(|| {
                        ConfigBuildError::UnknownSumMatch {
                            node: name.clone(),
                            ok_type: ok_type_name.clone(),
                            err_type: err_type_name.clone(),
                        }
                    })?;
                    let destruct_fn = factory.destruct_fn.clone();
                    dag_builder.add_sum_match_erased(
                        factory.ok_type,
                        factory.err_type,
                        Box::new(move |input| destruct_fn(input)),
                    )
                }
                NodeConfig::ProductJoin {
                    output_type_name: _,
                    input_type_names: _,
                    join_name,
                } => {
                    let factory = self.product_join_factories.get(join_name).ok_or_else(|| {
                        ConfigBuildError::UnknownProductJoin {
                            node: name.clone(),
                            name: join_name.clone(),
                        }
                    })?;
                    let join_fn = factory.join_fn.clone();
                    dag_builder.add_product_join(
                        factory.output_type,
                        factory.input_clone_fns.clone(),
                        Box::new(move |inputs| join_fn(inputs)),
                    )
                }
                NodeConfig::Reshape { reshape_name } => {
                    let factory = self.reshape_factories.get(reshape_name).ok_or_else(|| {
                        ConfigBuildError::UnknownReshape {
                            node: name.clone(),
                            name: reshape_name.clone(),
                        }
                    })?;
                    let reshape_fn = factory.reshape_fn.clone();
                    dag_builder.add_reshape(Box::new(move |input| reshape_fn(input)))
                }
                NodeConfig::Dispatch { output_count: _, dispatch_name } => {
                    let factory = self.dispatch_factories.get(dispatch_name).ok_or_else(|| {
                        ConfigBuildError::UnknownDispatch {
                            node: name.clone(),
                            name: dispatch_name.clone(),
                        }
                    })?;
                    let dispatch_fn = factory.dispatch_fn.clone();
                    dag_builder.add_dispatch(
                        factory.output_count,
                        Box::new(move |input| dispatch_fn(input)),
                    )
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

        // Pass 3: Add edges.
        for EdgeConfig { from, to, label, type_name: _, reshape_name: _ } in &expanded_edges {
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

        // ── Process <tool> elements (LLM tools) ──
        let tool_registry = self.build_tools(&config.tools)?;

        Ok(BuildOutput {
            id: workflow_id,
            dag,
            tools: tool_registry,
        })
    }

    /// Build tool entries from XML `<tool>` elements.
    fn build_tools(
        &self,
        tool_configs: &[super::schema::ToolConfig],
    ) -> Result<crate::workflow::tool_registry::ToolRegistry, ConfigBuildError> {
        use crate::workflow::tool_registry::ToolEntry;
        use crate::workflow::services::llm::ToolDefinition;

        let registry = crate::workflow::tool_registry::ToolRegistry::new();

        for tc in tool_configs {
            // Create workflow instance from factory.
            let workflow = self.workflows.create(&tc.implementation).ok_or_else(|| {
                ConfigBuildError::UnknownWorkflow {
                    node: tc.name.clone(),
                    name: tc.implementation.clone(),
                }
            })?;

            // Get serde closures from TypeRegistry.
            let (deserialize_ref, _) = self.types.get_serde(&tc.input_type).ok_or_else(|| {
                ConfigBuildError::UnknownType {
                    node: tc.name.clone(),
                    name: format!(
                        "{} (tool input — use register_tool_type)",
                        tc.input_type
                    ),
                }
            })?;

            let (_, serialize_ref) = self.types.get_serde(&tc.output_type).ok_or_else(|| {
                ConfigBuildError::UnknownType {
                    node: tc.name.clone(),
                    name: format!(
                        "{} (tool output — use register_tool_type)",
                        tc.output_type
                    ),
                }
            })?;

            // Wrap Arc closures in Box for ToolEntry compatibility.
            let deserialize_owned = {
                let arc = deserialize_ref.clone();
                Box::new(move |s: &str| arc(s))
                    as Box<dyn Fn(&str) -> Result<Box<dyn std::any::Any + Send + Sync>, crate::workflow::error::WorkflowError> + Send + Sync>
            };
            let serialize_owned = {
                let arc = serialize_ref.clone();
                Box::new(move |v: &Box<dyn std::any::Any + Send + Sync>| arc(v))
                    as Box<dyn Fn(&Box<dyn std::any::Any + Send + Sync>) -> Result<String, crate::workflow::error::WorkflowError> + Send + Sync>
            };

            let tool_def = ToolDefinition {
                name: tc.name.clone(),
                description: tc.description.clone(),
                parameters: tc.parameters.clone(),
            };

            let entry = ToolEntry {
                tool_def,
                workflow,
                deserialize: deserialize_owned,
                serialize_output: serialize_owned,
            };

            registry.register(entry).map_err(ConfigBuildError::DagError)?;
        }

        Ok(registry)
    }

    /// Pass 1.5: Expand structural attributes on `<node>` and `<connect>` into
    /// synthetic `NodeConfig` / `EdgeConfig` entries.
    ///
    /// This allows structural operations (Dispatch, SumMatch, ProductJoin, Reshape,
    /// Connection) to be expressed as attributes while internally converting them
    /// to the same standalone `NodeConfig` variants that the old XML elements use.
    fn expand_attributes(
        nodes: Vec<(String, NodeConfig)>,
        edges: Vec<EdgeConfig>,
    ) -> Result<(Vec<(String, NodeConfig)>, Vec<EdgeConfig>), ConfigBuildError> {
        let mut out_nodes: Vec<(String, NodeConfig)> = Vec::new();
        let mut out_edges: Vec<EdgeConfig> = Vec::new();

        // Track which original node names get a post-dispatch or post-summatch,
        // so we can rewrite edges: original → synthetic instead of original → downstream.
        // Maps original_name → synthetic_dispatch_name (if dispatch attr present)
        let mut dispatch_rewrite: HashMap<String, String> = HashMap::new();
        // Maps original_name → synthetic_summatch_name (if sum-match attr present)
        let mut summatch_rewrite: HashMap<String, String> = HashMap::new();
        // Maps target_name → synthetic_join_name (if join attr present on a node pointing at this one)
        // This is trickier: join is on the TARGET node, meaning "insert a ProductJoin BEFORE me"
        // We rewrite edges: upstream → __join instead of upstream → target, and __join → target.
        let mut join_rewrite: HashMap<String, String> = HashMap::new();

        // Process nodes.
        for (name, cfg) in nodes {
            match &cfg {
                NodeConfig::Workflow {
                    implementation: _,
                    dispatch_name: Some(dn),
                    dispatch_count: Some(dc),
                    sum_match: _,
                    join_name: _,
                } => {
                    // Insert original workflow node (without the structural attrs).
                    out_nodes.push((name.clone(), NodeConfig::Workflow {
                        implementation: match &cfg {
                            NodeConfig::Workflow { implementation, .. } => implementation.clone(),
                            _ => unreachable!(),
                        },
                        dispatch_name: None,
                        dispatch_count: None,
                        sum_match: None,
                        join_name: None,
                    }));
                    // Insert synthetic dispatch node.
                    let synth = format!("__dispatch_{name}");
                    out_nodes.push((synth.clone(), NodeConfig::Dispatch {
                        output_count: *dc,
                        dispatch_name: dn.clone(),
                    }));
                    dispatch_rewrite.insert(name.clone(), synth.clone());
                    // Add connecting edge: original → synthetic
                    out_edges.push(EdgeConfig {
                        from: name.clone(),
                        to: synth,
                        label: None,
                        type_name: None,
                        reshape_name: None,
                    });
                }
                NodeConfig::Workflow {
                    implementation: _,
                    dispatch_name: Some(_),
                    dispatch_count: None,
                    sum_match: _,
                    join_name: _,
                } => {
                    // dispatch without dispatch-count: invalid.
                    return Err(ConfigBuildError::MissingDispatchCount { node: name });
                }
                NodeConfig::Workflow {
                    implementation: _,
                    dispatch_name: None,
                    dispatch_count: None,
                    sum_match: Some(sm),
                    join_name: _,
                } => {
                    // Insert original workflow node.
                    out_nodes.push((name.clone(), NodeConfig::Workflow {
                        implementation: match &cfg {
                            NodeConfig::Workflow { implementation, .. } => implementation.clone(),
                            _ => unreachable!(),
                        },
                        dispatch_name: None,
                        dispatch_count: None,
                        sum_match: None,
                        join_name: None,
                    }));
                    // Insert synthetic sum-match node.
                    let synth = format!("__summatch_{name}");
                    let parts: Vec<&str> = sm.split('/').collect();
                    if parts.len() == 2 {
                        out_nodes.push((synth.clone(), NodeConfig::SumMatch {
                            ok_type_name: parts[0].to_string(),
                            err_type_name: parts[1].to_string(),
                        }));
                        summatch_rewrite.insert(name.clone(), synth.clone());
                        // Add connecting edge: original → synthetic
                        out_edges.push(EdgeConfig {
                            from: name.clone(),
                            to: synth,
                            label: None,
                            type_name: None,
                            reshape_name: None,
                        });
                    } else {
                        // Invalid format, pass through for error reporting.
                        out_nodes.push((name, cfg));
                    }
                }
                NodeConfig::Workflow {
                    implementation: _,
                    dispatch_name: None,
                    dispatch_count: None,
                    sum_match: None,
                    join_name: Some(jn),
                } => {
                    // Join attribute: insert a synthetic ProductJoin BEFORE this node.
                    // We emit the join node first, then the workflow node.
                    // Edges pointing to `name` get rewritten to point to the join node,
                    // and we add an edge join → name.
                    let synth = format!("__join_{name}");
                    out_nodes.push((synth.clone(), NodeConfig::ProductJoin {
                        output_type_name: String::new(),
                        input_type_names: String::new(),
                        join_name: jn.clone(),
                    }));
                    join_rewrite.insert(name.clone(), synth.clone());
                    // Emit the workflow node itself (without join attr).
                    out_nodes.push((name, NodeConfig::Workflow {
                        implementation: match &cfg {
                            NodeConfig::Workflow { implementation, .. } => implementation.clone(),
                            _ => unreachable!(),
                        },
                        dispatch_name: None,
                        dispatch_count: None,
                        sum_match: None,
                        join_name: None,
                    }));
                    // Add synthetic edge: __join → workflow node.
                    out_edges.push(EdgeConfig {
                        from: synth,
                        to: match out_nodes.last() { Some((n, _)) => n.clone(), None => continue },
                        label: None,
                        type_name: None,
                        reshape_name: None,
                    });
                }
                _ => {
                    out_nodes.push((name, cfg));
                }
            }
        }

        // Process edges, rewriting as needed for dispatch/summatch/join synthetics.
        for edge in edges {
            // Check for reshape or connection attributes on the edge itself.
            let has_reshape = edge.reshape_name.is_some();
            let has_conn = edge.type_name.is_some() && edge.label.is_some();

            // Determine effective from/to (after dispatch/summatch rewrite).
            let effective_from = if let Some(synth) = dispatch_rewrite.get(&edge.from) {
                // If the source node had a dispatch attr, edges from it should go to
                // the synthetic dispatch node instead. But the dispatch node fans out
                // to multiple targets — the original edges from the source still define
                // the targets, but they now come FROM the dispatch node.
                synth.clone()
            } else if let Some(synth) = summatch_rewrite.get(&edge.from) {
                synth.clone()
            } else {
                edge.from.clone()
            };

            let effective_to = if let Some(synth) = join_rewrite.get(&edge.to) {
                synth.clone()
            } else {
                edge.to.clone()
            };

            // Handle edge-level structural attributes (reshape, connection).
            if has_reshape {
                // Insert a synthetic Reshape node between from and to.
                let reshape_name = edge.reshape_name.clone().unwrap();
                let synth = format!("__reshape_{}_{}", edge.from, edge.to);
                out_nodes.push((synth.clone(), NodeConfig::Reshape { reshape_name }));
                // from → reshape → to
                out_edges.push(EdgeConfig {
                    from: effective_from,
                    to: synth.clone(),
                    label: edge.label.clone(),
                    type_name: None,
                    reshape_name: None,
                });
                out_edges.push(EdgeConfig {
                    from: synth,
                    to: effective_to,
                    label: None,
                    type_name: None,
                    reshape_name: None,
                });
            } else if has_conn {
                // Insert a synthetic Connection node between from and to.
                let label = edge.label.clone().unwrap_or_default();
                let type_name = edge.type_name.clone().unwrap_or_default();
                let synth = format!("__conn_{}_{}", edge.from, edge.to);
                out_nodes.push((synth.clone(), NodeConfig::Connection { label, type_name }));
                out_edges.push(EdgeConfig {
                    from: effective_from,
                    to: synth.clone(),
                    label: None,
                    type_name: None,
                    reshape_name: None,
                });
                out_edges.push(EdgeConfig {
                    from: synth,
                    to: effective_to,
                    label: None,
                    type_name: None,
                    reshape_name: None,
                });
            } else {
                out_edges.push(EdgeConfig {
                    from: effective_from,
                    to: effective_to,
                    label: edge.label,
                    type_name: None,
                    reshape_name: None,
                });
            }
        }

        Ok((out_nodes, out_edges))
    }
}

/// ConfigBuilder 的构建输出。
///
/// 包含构建好的 DAG 和工具注册表。
#[derive(Debug)]
pub struct BuildOutput {
    /// 工作流 ID。
    pub id: WorkflowId,
    /// 构建好的 DAG。
    pub dag: WorkflowDag,
    /// 工具注册表（从 `<tool>` 元素构建）。
    pub tools: crate::workflow::tool_registry::ToolRegistry,
}

impl ConfigBuilder {
    /// Parse an XML string and build a `WorkflowDag`.
    pub fn build_from_str(&self, xml: &str) -> Result<BuildOutput, ConfigBuildError> {
        let config: WorkflowXml = quick_xml::de::from_str(xml)?;
        self.build(config.into())
    }

    /// Read an XML file and build a `WorkflowDag`.
    pub fn build_from_file(
        &self,
        path: &Path,
    ) -> Result<BuildOutput, ConfigBuildError> {
        let xml = std::fs::read_to_string(path)?;
        self.build_from_str(&xml)
    }
}
