//! # 执行引擎
//!
//! [`Executor`] 接收 [`ExecutionPlan`]，按拓扑排序并发执行节点。
//!
//! ## 执行模型
//!
//! 1. 从 [`ExecutionPlan`] 的拓扑排序计算入度
//! 2. 并发执行所有入度为 0 的 ready 节点（`futures::join_all`）
//! 3. 每个节点从命名空间解析参数 → 执行 → 写回命名空间
//! 4. 遇到 `<end>` 立即返回结果（提前终止）
//! 5. 减少下游节点入度，重复直到所有节点完成
//!
//! ## 使用方式
//!
//! ```rust
//! use intelligent_subject::workflow::dag::PlanBuilder;
//! use intelligent_subject::workflow::definition::from_fn;
//! use intelligent_subject::workflow::executor::Executor;
//! use intelligent_subject::workflow::model::{ExecutionContext, Namespace};
//! use intelligent_subject::workflow::config::ParamValue;
//! use intelligent_subject::workflow::error::WorkflowError;
//! use intelligent_subject::workflow::platform::NullPlatform;
//! use std::sync::Arc;
//!
//! # #[tokio::main]
//! # async fn example() -> Result<(), WorkflowError> {
//! let mut builder = PlanBuilder::new();
//! let a = builder.add_workflow(
//!     "a",
//!     "double",
//!     vec![("input".to_string(), ParamValue::Literal("21".to_string()))],
//!     from_fn("double", |input: i32, _| async move {
//!         Ok::<i32, WorkflowError>(input * 2)
//!     }),
//! );
//! builder.add_end("{a.value}");
//!
//! let plan = builder.build()?;
//! let ns = Namespace::new();
//! let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };
//!
//! let result = Executor::execute(&plan, &ns, &ctx).await?;
//! let val = result.downcast_ref::<i32>().unwrap();
//! assert_eq!(*val, 42);
//! # Ok(())
//! # }
//! ```

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::workflow::config::ParamValue;
use crate::workflow::dag::{ExecutionPlan, NodeKind, Node};
use crate::workflow::error::WorkflowError;
use crate::workflow::model::{ExecutionContext, Namespace, NodeId};

/// 叶工作流执行的结果。
pub struct ExecutionResult {
    /// 最终输出（类型擦除）。
    pub output: Box<dyn Any + Send + Sync>,
    /// 输出的 TypeId，用于 downcast。
    pub output_type: std::any::TypeId,
}

impl std::fmt::Debug for ExecutionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionResult")
            .field("output_type", &self.output_type)
            .finish()
    }
}

/// 执行命名空间工作流。
pub struct Executor;

/// 节点执行后的输出类型。
enum NodeOutcome {
    /// 节点完成，结果已写入命名空间。
    Completed,
    /// 遇到 `<end>` 节点，提前终止。
    End(Arc<dyn Any + Send + Sync>),
}

impl Executor {
    /// 执行一个 [`ExecutionPlan`]。
    ///
    /// 返回 `<end>` 节点指定的结果值（`Arc` 包装）。
    pub async fn execute(
        plan: &ExecutionPlan,
        namespace: &Namespace,
        ctx: &ExecutionContext,
    ) -> Result<Arc<dyn Any + Send + Sync>, WorkflowError> {
        // 收集子节点 ID — 子节点仅由父节点（If/Loop）执行，
        // 不参与主执行循环。
        let children: HashSet<NodeId> = plan.nodes.values()
            .flat_map(|n| n.children.iter().copied())
            .collect();

        // 构建依赖图用于入度计算
        let name_to_id: HashMap<String, NodeId> = plan.nodes.iter()
            .filter(|(_, n)| !n.result_name.is_empty())
            .map(|(_, n)| (n.result_name.clone(), n.id))
            .collect();

        let mut in_degree: HashMap<NodeId, usize> = plan.nodes.keys().map(|&id| (id, 0)).collect();
        let mut dependents: HashMap<NodeId, Vec<NodeId>> =
            plan.nodes.keys().map(|&id| (id, Vec::new())).collect();

        for (_, node) in &plan.nodes {
            for dep_name in &node.dependencies {
                if let Some(&dep_id) = name_to_id.get(dep_name) {
                    // 如果依赖是子节点，重定向到其父节点。
                    // 子节点仅由父节点（If/Loop）执行，
                    // 因此下游节点应等待父节点，而非子节点。
                    let target_id = if children.contains(&dep_id) {
                        plan.nodes.values()
                            .find(|n| n.children.contains(&dep_id))
                            .map(|n| n.id)
                            .unwrap_or(dep_id)
                    } else {
                        dep_id
                    };
                    dependents.get_mut(&target_id).unwrap().push(node.id);
                    *in_degree.get_mut(&node.id).unwrap() += 1;
                }
            }
        }

        let mut completed: HashSet<NodeId> = HashSet::new();

        // 预标记所有子节点为已完成，并减少下游依赖的入度。
        // 子节点仅由父节点（If/Loop）执行。
        for &child_id in &children {
            completed.insert(child_id);
            if let Some(deps) = dependents.get(&child_id) {
                for &dep_id in deps {
                    if let Some(deg) = in_degree.get_mut(&dep_id) {
                        *deg = deg.saturating_sub(1);
                    }
                }
            }
        }

        loop {
            let ready: Vec<NodeId> = plan.topo_order.iter()
                .filter(|&&id| {
                    !completed.contains(&id)
                        && in_degree[&id] == 0
                })
                .copied()
                .collect();

            if ready.is_empty() {
                break;
            }

            let futures: Vec<_> = ready.iter().map(|&node_id| {
                Self::execute_node(plan, node_id, namespace, ctx)
            }).collect();

            let results = futures_util::future::join_all(futures).await;

            for (i, outcome) in results.into_iter().enumerate() {
                let node_id = ready[i];
                let outcome = outcome?;

                match outcome {
                    NodeOutcome::End(value) => return Ok(value),
                    NodeOutcome::Completed => {
                        completed.insert(node_id);
                        if let Some(deps) = dependents.get(&node_id) {
                            for &dep_id in deps {
                                if let Some(deg) = in_degree.get_mut(&dep_id) {
                                    *deg = deg.saturating_sub(1);
                                }
                            }
                        }
                    }
                }
            }
        }

        Err(WorkflowError::ValidationError("no <end> node was reached".into()))
    }

    fn execute_node<'a>(
        plan: &'a ExecutionPlan,
        node_id: NodeId,
        namespace: &'a Namespace,
        ctx: &'a ExecutionContext,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<NodeOutcome, WorkflowError>> + Send + 'a>> {
        Box::pin(async move {
        let node = plan.nodes.get(&node_id)
            .ok_or_else(|| WorkflowError::ValidationError(format!("node not found: {node_id:?}")))?;

        let result = match &node.kind {
            NodeKind::Workflow { .. } => {
                Self::execute_workflow_node(node, namespace, ctx).await
            }
            NodeKind::If { predicate, .. } => {
                Self::execute_if_node(plan, node, predicate, namespace, ctx).await
            }
            NodeKind::Loop { state_init, next_state, count, .. } => {
                Self::execute_loop_node(plan, node, state_init, next_state, *count, namespace, ctx).await
            }
            NodeKind::End { result_ref, .. } => {
                Self::execute_end_node(result_ref, namespace)
            }
        };

        result.map_err(|e| Self::wrap_node_error(node, e))
        })
    }

    /// 为节点执行错误附加上下文信息（节点 ID、result_name、节点类型）。
    fn wrap_node_error(node: &Node, err: WorkflowError) -> WorkflowError {
        // 如果已经是 ExecutionError 且带有节点信息，避免重复包装
        if matches!(err, WorkflowError::ExecutionError { .. }) {
            return err;
        }
        let kind_desc = match &node.kind {
            NodeKind::Workflow { impl_name } => format!("workflow(impl={impl_name})"),
            NodeKind::If { predicate } => format!("if(predicate={predicate})"),
            NodeKind::Loop { state_init, count, .. } => format!("loop(state_init={state_init}, count={count})"),
            NodeKind::End { result_ref } => format!("end(result={result_ref})"),
        };
        let name = if node.result_name.is_empty() {
            "<anonymous>".to_string()
        } else {
            node.result_name.clone()
        };
        WorkflowError::ExecutionError {
            node: node.id,
            source: anyhow::anyhow!(
                "节点 '{name}' ({kind_desc}) 执行失败: {err}"
            ),
        }
    }

    async fn execute_workflow_node(
        node: &Node,
        namespace: &Namespace,
        ctx: &ExecutionContext,
    ) -> Result<NodeOutcome, WorkflowError> {
        let wf = node.workflow.as_ref()
            .ok_or_else(|| WorkflowError::ValidationError(
                format!("workflow node '{}' has no implementation", node.result_name),
            ))?;

        // 解析参数
        let mut params: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
        for (name, pv) in &node.params {
            let value = resolve_param(pv, namespace)?;
            params.insert(name.clone(), value);
        }

        let output = wf.execute_erased(params, ctx).await?;

        // 将输出字段写入命名空间
        for (field, value) in output.fields {
            let arc_value: Arc<dyn Any + Send + Sync> = Arc::from(value);
            namespace.set_arc(format!("{}.{}", node.result_name, field), arc_value);
        }

        Ok(NodeOutcome::Completed)
    }

    fn execute_if_node<'a>(
        plan: &'a ExecutionPlan,
        node: &'a Node,
        predicate: &'a str,
        namespace: &'a Namespace,
        ctx: &'a ExecutionContext,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<NodeOutcome, WorkflowError>> + Send + 'a>> {
        Box::pin(async move {
        let pred_pv = crate::workflow::config::parse_param_value(predicate);
        let pred_val = match &pred_pv {
            ParamValue::Reference { namespace: ns, field } => {
                namespace.get_typed::<bool>(&format!("{ns}.{field}"))
                    .ok_or_else(|| WorkflowError::ValidationError(
                        format!("if predicate '{predicate}' not found or not bool"),
                    ))?
            }
            _ => return Err(WorkflowError::ValidationError(
                format!("if predicate must be a reference, got: {predicate}"),
            )),
        };

        if pred_val {
            for &child_id in &node.children {
                let outcome = Self::execute_node(plan, child_id, namespace, ctx).await?;
                if let NodeOutcome::End(value) = outcome {
                    return Ok(NodeOutcome::End(value));
                }
            }
        }

        Ok(NodeOutcome::Completed)
        })
    }

    fn execute_loop_node<'a>(
        plan: &'a ExecutionPlan,
        node: &'a Node,
        state_init: &'a str,
        next_state: &'a str,
        count: usize,
        namespace: &'a Namespace,
        ctx: &'a ExecutionContext,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<NodeOutcome, WorkflowError>> + Send + 'a>> {
        Box::pin(async move {
        // 解析初始状态
        let init_pv = crate::workflow::config::parse_param_value(state_init);
        let mut current_state = resolve_param(&init_pv, namespace)?;

        for _i in 0..count {
            let child_ns = Namespace::new_with_parent(namespace);
            child_ns.set_arc("latest_state", current_state.clone());

            for &child_id in &node.children {
                let outcome = Self::execute_node(plan, child_id, &child_ns, ctx).await?;
                if let NodeOutcome::End(value) = outcome {
                    return Ok(NodeOutcome::End(value));
                }
            }

            // 从子命名空间读取 next_state
            let ns_pv = crate::workflow::config::parse_param_value(next_state);
            current_state = resolve_param(&ns_pv, &child_ns)?;
        }

        // 将最终状态写入命名空间的 result_name 下
        namespace.set_arc(&node.result_name, current_state);

        Ok(NodeOutcome::Completed)
        })
    }

    fn execute_end_node(
        result_ref: &str,
        namespace: &Namespace,
    ) -> Result<NodeOutcome, WorkflowError> {
        let pv = crate::workflow::config::parse_param_value(result_ref);
        let value = resolve_param(&pv, namespace)?;
        Ok(NodeOutcome::End(value))
    }
}

/// 将 ParamValue 解析为命名空间中的 Arc 值。
fn resolve_param(
    pv: &ParamValue,
    namespace: &Namespace,
) -> Result<Arc<dyn Any + Send + Sync>, WorkflowError> {
    match pv {
        ParamValue::Literal(s) => Ok(Arc::new(s.clone())),
        ParamValue::Reference { namespace: ns, field } => {
            // 先尝试 "ns.field"，再尝试 "ns"
            namespace.get(&format!("{ns}.{field}"))
                .or_else(|| namespace.get(ns))
                .ok_or_else(|| WorkflowError::ValidationError(
                    format!("unresolved reference: {ns}.{field}"),
                ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::dag::PlanBuilder;
    use crate::workflow::definition::{into_erased, Workflow};
    use crate::workflow::error::WorkflowError;
    use async_trait::async_trait;

    /// AppendX: String → String, appends "X"
    struct AppendX;
    #[async_trait]
    impl Workflow<String, String> for AppendX {
        fn name(&self) -> &str { "append_x" }
        async fn execute(&self, input: String, _ctx: &ExecutionContext) -> Result<String, WorkflowError> {
            Ok(format!("{input}X"))
        }
    }

    /// Format: String → String, wraps in "Report: {input}"
    struct Format;
    #[async_trait]
    impl Workflow<String, String> for Format {
        fn name(&self) -> &str { "format" }
        async fn execute(&self, input: String, _ctx: &ExecutionContext) -> Result<String, WorkflowError> {
            Ok(format!("Report: {input}"))
        }
    }

    fn make_ctx() -> ExecutionContext {
        ExecutionContext {
            platform: Arc::new(crate::workflow::platform::NullPlatform::new()),
        }
    }

    #[tokio::test]
    async fn end_returns_namespace_value() {
        let namespace = Namespace::new();
        namespace.set("my_result.value", 42i32);

        let outcome = Executor::execute_end_node("{my_result.value}", &namespace).unwrap();
        match outcome {
            NodeOutcome::End(val) => {
                let i = val.downcast_ref::<i32>().unwrap();
                assert_eq!(*i, 42);
            }
            _ => panic!("expected End"),
        }
    }

    #[tokio::test]
    async fn end_returns_top_level_namespace() {
        let namespace = Namespace::new();
        namespace.set("report", "hello".to_string());

        let outcome = Executor::execute_end_node("{report}", &namespace).unwrap();
        match outcome {
            NodeOutcome::End(val) => {
                let s = val.downcast_ref::<String>().unwrap();
                assert_eq!(*s, "hello");
            }
            _ => panic!("expected End"),
        }
    }

    #[tokio::test]
    async fn end_unresolved_reference() {
        let namespace = Namespace::new();
        let result = Executor::execute_end_node("{missing.field}", &namespace);
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("unresolved"));
    }

    /// E2E: linear chain — append_x("hello") → append_x("helloX") → end
    #[tokio::test]
    async fn e2e_linear_pipeline() {
        let mut builder = PlanBuilder::new();
        builder.add_workflow(
            "a",
            "append_x",
            vec![("input".to_string(), ParamValue::Literal("hello".to_string()))],
            into_erased(AppendX),
        );
        builder.add_workflow(
            "b",
            "append_x",
            vec![("input".to_string(), ParamValue::Reference {
                namespace: "a".to_string(),
                field: "value".to_string(),
            })],
            into_erased(AppendX),
        );
        builder.add_end("{b.value}");

        let plan = builder.build().unwrap();
        let ns = Namespace::new();
        let ctx = make_ctx();

        let result = Executor::execute(&plan, &ns, &ctx).await.unwrap();
        let val = result.downcast_ref::<String>().unwrap();
        assert_eq!(*val, "helloXX");
    }

    /// E2E: two independent nodes, end picks one result
    #[tokio::test]
    async fn e2e_independent_nodes() {
        let mut builder = PlanBuilder::new();
        builder.add_workflow(
            "x",
            "append_x",
            vec![("input".to_string(), ParamValue::Literal("foo".to_string()))],
            into_erased(AppendX),
        );
        builder.add_workflow(
            "y",
            "append_x",
            vec![("input".to_string(), ParamValue::Literal("bar".to_string()))],
            into_erased(AppendX),
        );
        builder.add_end("{x.value}");

        let plan = builder.build().unwrap();
        let ns = Namespace::new();
        let ctx = make_ctx();

        let result = Executor::execute(&plan, &ns, &ctx).await.unwrap();
        let val = result.downcast_ref::<String>().unwrap();
        assert_eq!(*val, "fooX");
    }

    /// E2E: string format pipeline
    #[tokio::test]
    async fn e2e_string_pipeline() {
        let mut builder = PlanBuilder::new();
        builder.add_workflow(
            "report",
            "format",
            vec![("input".to_string(), ParamValue::Literal("world".to_string()))],
            into_erased(Format),
        );
        builder.add_end("{report.value}");

        let plan = builder.build().unwrap();
        let ns = Namespace::new();
        let ctx = make_ctx();

        let result = Executor::execute(&plan, &ns, &ctx).await.unwrap();
        let val = result.downcast_ref::<String>().unwrap();
        assert_eq!(*val, "Report: world");
    }

    /// E2E: no <end> node reached → error
    #[tokio::test]
    async fn e2e_no_end_node() {
        let mut builder = PlanBuilder::new();
        builder.add_workflow(
            "a",
            "append_x",
            vec![("input".to_string(), ParamValue::Literal("hi".to_string()))],
            into_erased(AppendX),
        );

        let plan = builder.build().unwrap();
        let ns = Namespace::new();
        let ctx = make_ctx();

        let result = Executor::execute(&plan, &ns, &ctx).await;
        assert!(result.is_err());
        let msg = result.err().unwrap().to_string();
        assert!(msg.contains("no <end>"), "actual error: {msg}");
    }

    /// E2E: 工作流节点执行失败时，错误包含节点上下文
    #[tokio::test]
    async fn e2e_workflow_error_contains_node_context() {
        use crate::workflow::definition::from_fn;

        let mut builder = PlanBuilder::new();
        builder.add_workflow(
            "failing_node",
            "fail",
            vec![("input".to_string(), ParamValue::Literal("test".to_string()))],
            from_fn("fail", |_input: String, _| async move {
                Err::<String, WorkflowError>(WorkflowError::ValidationError("inner error".into()))
            }),
        );
        builder.add_end("{failing_node.value}");

        let plan = builder.build().unwrap();
        let ns = Namespace::new();
        let ctx = make_ctx();

        let result = Executor::execute(&plan, &ns, &ctx).await;
        assert!(result.is_err());
        let msg = result.err().unwrap().to_string();

        // 错误信息应包含节点 result_name 和节点类型
        assert!(msg.contains("failing_node"), "应包含节点名: {msg}");
        assert!(msg.contains("workflow"), "应包含节点类型: {msg}");
        assert!(msg.contains("inner error"), "应包含原始错误: {msg}");
    }

    /// E2E: <end> 引用不存在的值时，错误包含节点上下文
    #[tokio::test]
    async fn e2e_end_unresolved_contains_context() {
        let mut builder = PlanBuilder::new();
        builder.add_workflow(
            "a",
            "append_x",
            vec![("input".to_string(), ParamValue::Literal("hi".to_string()))],
            into_erased(AppendX),
        );
        // <end> 引用了不存在的 missing.value
        builder.add_end("{missing.value}");

        let plan = builder.build().unwrap();
        let ns = Namespace::new();
        let ctx = make_ctx();

        let result = Executor::execute(&plan, &ns, &ctx).await;
        assert!(result.is_err());
        let msg = result.err().unwrap().to_string();
        assert!(msg.contains("end"), "应包含节点类型: {msg}");
        assert!(msg.contains("missing.value"), "应包含引用名: {msg}");
    }
}
