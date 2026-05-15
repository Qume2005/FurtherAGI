//! # XML 配置构建器
//!
//! [`ConfigBuilder`] 将 XML 配置解析为 [`ExecutionPlan`]。
//!
//! 使用前需要先在 [`WorkflowFactoryRegistry`] 中注册工作流工厂，
//! 然后创建 `ConfigBuilder` 并调用 [`build_from_str`](ConfigBuilder::build_from_str)
//! 或 [`build_from_file`](ConfigBuilder::build_from_file)。
//!
//! ## 使用流程
//!
//! ```text
//! 1. 创建 WorkflowFactoryRegistry，注册工作流工厂
//! 2. 创建 ConfigBuilder::new(registry)
//! 3. 调用 builder.build_from_str(xml) 得到 ExecutionPlan
//! 4. 调用 Executor::execute(&plan, &ns, &ctx) 执行
//! ```

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Deserialize;

use crate::workflow::config::{ParamValue, parse_param_value};
use crate::workflow::config::error::ConfigError;
use crate::workflow::config::workflow_registry::WorkflowFactoryRegistry;
use crate::workflow::dag::{ExecutionPlan, PlanBuilder};
use crate::workflow::model::NodeId;

// ── XML 反序列化结构体 ──────────────────────────────────────────

/// XML 根元素。
///
/// ```xml
/// <workflow>
///   <workflow result_name="a" impl="add_one" input="42"/>
///   <end result="{a.value}"/>
/// </workflow>
/// ```
#[derive(Deserialize)]
#[serde(rename = "workflow")]
struct WorkflowXml {
    #[serde(rename = "$value", default)]
    children: Vec<ElementXml>,
}

/// XML 子元素枚举：4 种元素类型。
#[derive(Deserialize)]
enum ElementXml {
    #[serde(rename = "workflow")]
    Workflow(WorkflowElementXml),
    #[serde(rename = "if")]
    If(IfElementXml),
    #[serde(rename = "loop")]
    Loop(LoopElementXml),
    #[serde(rename = "end")]
    End(EndElementXml),
}

/// `<workflow>` 元素。
///
/// 所有非标准属性被视为传给工作流的参数（字面量或 `{ref}` 引用）。
/// `result_name` 和 `impl` 是保留属性，不作为参数传递。
#[derive(Deserialize)]
struct WorkflowElementXml {
    #[serde(rename = "@result_name")]
    result_name: String,
    #[serde(rename = "@impl")]
    impl_name: String,
    /// 其他属性被解析为参数。
    #[serde(flatten)]
    extra_attrs: HashMap<String, String>,
}

/// `<if>` 元素。
///
/// 子元素（`<workflow>`、`<if>`、`<loop>`、`<end>`）在 predicate 为 true 时
/// 在子命名空间中顺序执行。子命名空间退出后自动释放。
///
/// 可选 `result_name` 和 `then` 属性：当 predicate 为 true 且子节点执行完成后，
/// 从子命名空间解析 `then` 表达式的值，写入父命名空间的 `result_name` 下。
#[derive(Deserialize)]
struct IfElementXml {
    #[serde(rename = "@predicate")]
    predicate: String,
    #[serde(rename = "@result_name", default)]
    result_name: Option<String>,
    #[serde(rename = "@then", default)]
    then: Option<String>,
    #[serde(rename = "$value", default)]
    children: Vec<ElementXml>,
}

/// `<loop>` 元素。
///
/// 子元素在每次迭代中执行，命名空间隔离。
/// `state_init` 是初始状态的引用，`next_state` 是每次迭代后的状态引用。
#[derive(Deserialize)]
struct LoopElementXml {
    #[serde(rename = "@result_name")]
    result_name: String,
    #[serde(rename = "@state_init")]
    state_init: String,
    #[serde(rename = "@next_state")]
    next_state: String,
    #[serde(rename = "@count")]
    count: usize,
    #[serde(rename = "$value", default)]
    children: Vec<ElementXml>,
}

/// `<end>` 元素。
///
/// 引用命名空间中的值作为最终结果返回。
#[derive(Deserialize)]
struct EndElementXml {
    #[serde(rename = "@result")]
    result: String,
}

// ── ConfigBuilder ─────────────────────────────────────────────

static IF_COUNTER: AtomicU64 = AtomicU64::new(0);

/// 命名空间工作流 XML 配置构建器。
///
/// 将 4 种 XML 元素解析为 [`ExecutionPlan`]。
///
/// # 示例
///
/// ```rust
/// use std::sync::Arc;
/// use intelligent_subject::workflow::config::ConfigBuilder;
/// use intelligent_subject::workflow::config::WorkflowFactoryRegistry;
/// use intelligent_subject::workflow::definition::{Workflow, into_erased};
/// use intelligent_subject::workflow::model::{ExecutionContext, Namespace};
/// use intelligent_subject::workflow::executor::Executor;
/// use intelligent_subject::workflow::error::WorkflowError;
/// use intelligent_subject::workflow::platform::NullPlatform;
/// use async_trait::async_trait;
///
/// struct AppendX;
/// #[async_trait]
/// impl Workflow<String, String> for AppendX {
///     fn name(&self) -> &str { "append_x" }
///     async fn execute(&self, input: String, _ctx: &ExecutionContext)
///         -> Result<String, WorkflowError> { Ok(format!("{input}X")) }
/// }
///
/// let mut reg = WorkflowFactoryRegistry::new();
/// reg.register("append_x", || into_erased(AppendX));
///
/// let builder = ConfigBuilder::new(reg);
/// let plan = builder.build_from_str(r#"
///     <workflow>
///       <workflow result_name="a" impl="append_x" input="hello"/>
///       <workflow result_name="b" impl="append_x" input="{a.value}"/>
///       <end result="{b.value}"/>
///     </workflow>
/// "#).unwrap();
///
/// let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };
/// let ns = Namespace::new();
/// let rt = tokio::runtime::Runtime::new().unwrap();
/// let result = rt.block_on(Executor::execute(&plan, &ns, &ctx)).unwrap();
/// let val = result.downcast_ref::<String>().unwrap();
/// assert_eq!(val, "helloXX");
/// ```
pub struct ConfigBuilder {
    registry: WorkflowFactoryRegistry,
}

impl ConfigBuilder {
    /// 创建新的构建器。
    pub fn new(registry: WorkflowFactoryRegistry) -> Self {
        Self { registry }
    }

    /// 从 XML 字符串构建 [`ExecutionPlan`]。
    pub fn build_from_str(&self, xml: &str) -> Result<ExecutionPlan, ConfigError> {
        let root: WorkflowXml = quick_xml::de::from_str(xml)?;
        let mut builder = PlanBuilder::new();

        for elem in &root.children {
            Self::build_element(&mut builder, &self.registry, elem)?;
        }

        let plan = builder.build()?;
        Ok(plan)
    }

    /// 从 XML 文件构建 [`ExecutionPlan`]。
    pub fn build_from_file(&self, path: &Path) -> Result<ExecutionPlan, ConfigError> {
        let xml = std::fs::read_to_string(path)?;
        self.build_from_str(&xml)
    }

    /// 递归构建单个 XML 元素，返回新创建的 NodeId。
    fn build_element(
        builder: &mut PlanBuilder,
        registry: &WorkflowFactoryRegistry,
        elem: &ElementXml,
    ) -> Result<NodeId, ConfigError> {
        match elem {
            ElementXml::Workflow(wf) => Self::build_workflow(builder, registry, wf),
            ElementXml::If(if_elem) => Self::build_if(builder, registry, if_elem),
            ElementXml::Loop(lp) => Self::build_loop(builder, registry, lp),
            ElementXml::End(end_elem) => Self::build_end(builder, end_elem),
        }
    }

    /// 构建 `<workflow>` 元素。
    fn build_workflow(
        builder: &mut PlanBuilder,
        registry: &WorkflowFactoryRegistry,
        wf: &WorkflowElementXml,
    ) -> Result<NodeId, ConfigError> {
        let workflow = registry.create(&wf.impl_name).ok_or_else(|| {
            ConfigError::UnknownImpl {
                element: wf.result_name.clone(),
                name: wf.impl_name.clone(),
            }
        })?;

        let params = Self::parse_params(&wf.extra_attrs);

        let node_id = builder.add_workflow(
            &wf.result_name,
            &wf.impl_name,
            params,
            workflow,
        );

        Ok(node_id)
    }

    /// 构建 `<if>` 元素。
    fn build_if(
        builder: &mut PlanBuilder,
        registry: &WorkflowFactoryRegistry,
        if_elem: &IfElementXml,
    ) -> Result<NodeId, ConfigError> {
        // 先构建子节点以收集它们的 ID。
        let mut child_ids: Vec<NodeId> = Vec::new();
        for child in &if_elem.children {
            let id = Self::build_element(builder, registry, child)?;
            child_ids.push(id);
        }

        // result_name：用户指定或自动生成
        let result_name = if_elem.result_name.clone()
            .unwrap_or_else(|| {
                let idx = IF_COUNTER.fetch_add(1, Ordering::Relaxed);
                format!("__if_{idx}")
            });

        let node_id = builder.add_if(
            &result_name,
            &if_elem.predicate,
            child_ids,
            if_elem.then.clone(),
        );

        Ok(node_id)
    }

    /// 构建 `<loop>` 元素。
    fn build_loop(
        builder: &mut PlanBuilder,
        registry: &WorkflowFactoryRegistry,
        lp: &LoopElementXml,
    ) -> Result<NodeId, ConfigError> {
        let mut child_ids: Vec<NodeId> = Vec::new();
        for child in &lp.children {
            let id = Self::build_element(builder, registry, child)?;
            child_ids.push(id);
        }

        let node_id = builder.add_loop(
            &lp.result_name,
            &lp.state_init,
            &lp.next_state,
            lp.count,
            child_ids,
        );

        Ok(node_id)
    }

    /// 构建 `<end>` 元素。
    fn build_end(
        builder: &mut PlanBuilder,
        end_elem: &EndElementXml,
    ) -> Result<NodeId, ConfigError> {
        let node_id = builder.add_end(&end_elem.result);
        Ok(node_id)
    }

    /// 将额外属性解析为 `ParamValue` 列表。
    ///
    /// quick-xml 的 `serde(flatten)` 给属性键加上 `@` 前缀，此处去除。
    fn parse_params(attrs: &HashMap<String, String>) -> Vec<(String, ParamValue)> {
        attrs.iter()
            .map(|(k, v)| {
                let key = k.strip_prefix('@').unwrap_or(k).to_string();
                (key, parse_param_value(v))
            })
            .collect()
    }
}
