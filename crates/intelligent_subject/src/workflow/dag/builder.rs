//! # 执行计划构建器
//!
//! 流式构建 [`ExecutionPlan`](ExecutionPlan)。
//!
//! ## 功能实现
//!
//! | 方法 | 节点类型 | 说明 |
//! |------|----------|------|
//! | [`add_workflow()`](PlanBuilder::add_workflow) | `Workflow` | 添加工作流节点，自动从参数推导依赖 |
//! | [`add_if()`](PlanBuilder::add_if) | `If` | 添加条件分支节点 |
//! | [`add_loop()`](PlanBuilder::add_loop) | `Loop` | 添加固定次数循环节点 |
//! | [`add_end()`](PlanBuilder::add_end) | `End` | 添加终止节点 |
//! | [`build()`](PlanBuilder::build) | — | Kahn 算法拓扑排序 + 环检测 |
//!
//! ## 实现特色
//!
//! - 从 `{namespace.field}` 引用自动推导依赖边
//! - Kahn 算法保证无环 + 拓扑排序
//! - 无依赖的节点按 NodeId 排序，保证确定性

use std::collections::HashMap;

use crate::workflow::definition::ErasedWorkflow;
use crate::workflow::error::WorkflowError;
use crate::workflow::model::NodeId;

use super::graph::{ExecutionPlan, Node, NodeKind};

use crate::workflow::config::{ParamValue, referenced_namespace};

/// 执行计划构建器。
pub struct PlanBuilder {
    nodes: HashMap<NodeId, Node>,
    next_id: u64,
}

impl PlanBuilder {
    /// 创建一个空的构建器。
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            next_id: 0,
        }
    }

    fn alloc_id(&mut self) -> NodeId {
        let id = NodeId(self.next_id);
        self.next_id += 1;
        id
    }

    /// 返回当前节点数量。
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// 添加一个 Workflow 节点。
    ///
    /// 从 `params` 中自动推导依赖关系。
    pub fn add_workflow(
        &mut self,
        result_name: impl Into<String>,
        impl_name: impl Into<String>,
        params: Vec<(String, ParamValue)>,
        workflow: Box<dyn ErasedWorkflow>,
    ) -> NodeId {
        let result_name = result_name.into();
        let impl_name_str = impl_name.into();
        let dependencies = extract_dependencies(&params);
        let id = self.alloc_id();
        self.nodes.insert(
            id,
            Node {
                id,
                result_name,
                kind: NodeKind::Workflow { impl_name: impl_name_str },
                params,
                dependencies,
                children: Vec::new(),
                workflow: Some(workflow),
            },
        );
        id
    }

    /// 添加一个 If 节点。
    ///
    /// `predicate` 是命名空间引用字符串（如 `"{a.is_positive}"`）。
    /// `children` 是子节点 ID 列表。
    /// `then` 是可选的表达式（如 `"{inner.value}"`），当 predicate 为 true 且
    /// 子节点执行完成后，从子命名空间解析该值并写入父命名空间的 `result_name` 下。
    pub fn add_if(
        &mut self,
        result_name: impl Into<String>,
        predicate: impl Into<String>,
        children: Vec<NodeId>,
        then: Option<String>,
    ) -> NodeId {
        let result_name = result_name.into();
        let predicate_str = predicate.into();
        // Parse predicate as a ParamValue to extract dependency
        let pred_pv = crate::workflow::config::parse_param_value(&predicate_str);
        let mut dependencies = referenced_namespace(&pred_pv)
            .map(|ns| vec![ns.to_string()])
            .unwrap_or_default();

        // 收集子节点的传递依赖：父节点需要等待子节点依赖的上游节点完成
        collect_child_deps(&self.nodes, &children, &mut dependencies);

        let id = self.alloc_id();
        self.nodes.insert(
            id,
            Node {
                id,
                result_name,
                kind: NodeKind::If { predicate: predicate_str, then },
                params: vec![("_predicate".to_string(), pred_pv)],
                dependencies,
                children,
                workflow: None,
            },
        );
        id
    }

    /// 添加一个 Loop 节点。
    ///
    /// `state_init` 和 `next_state` 是命名空间引用。
    /// `children` 是循环体内的子节点 ID 列表。
    pub fn add_loop(
        &mut self,
        result_name: impl Into<String>,
        state_init: impl Into<String>,
        next_state: impl Into<String>,
        count: usize,
        children: Vec<NodeId>,
    ) -> NodeId {
        let result_name = result_name.into();
        let state_init_str = state_init.into();
        let next_state_str = next_state.into();

        let mut deps = Vec::new();
        let init_pv = crate::workflow::config::parse_param_value(&state_init_str);
        if let Some(ns) = referenced_namespace(&init_pv) {
            deps.push(ns.to_string());
        }

        // 收集子节点的传递依赖
        collect_child_deps(&self.nodes, &children, &mut deps);

        let id = self.alloc_id();
        self.nodes.insert(
            id,
            Node {
                id,
                result_name,
                kind: NodeKind::Loop {
                    state_init: state_init_str,
                    next_state: next_state_str,
                    count,
                },
                params: vec![],
                dependencies: deps,
                children,
                workflow: None,
            },
        );
        id
    }

    /// 添加一个 End 节点。
    pub fn add_end(
        &mut self,
        result_ref: impl Into<String>,
    ) -> NodeId {
        let result_ref_str = result_ref.into();
        let pv = crate::workflow::config::parse_param_value(&result_ref_str);
        let dependencies = referenced_namespace(&pv)
            .map(|ns| vec![ns.to_string()])
            .unwrap_or_default();
        let id = self.alloc_id();
        self.nodes.insert(
            id,
            Node {
                id,
                result_name: String::new(), // End nodes don't write to namespace
                kind: NodeKind::End { result_ref: result_ref_str },
                params: vec![("_result".to_string(), pv)],
                dependencies,
                children: Vec::new(),
                workflow: None,
            },
        );
        id
    }

    /// 构建执行计划。
    ///
    /// 执行拓扑排序（Kahn 算法）并检测循环依赖。
    /// 同时校验嵌套作用域：If/Loop 内部节点的 `result_name` 不能被外部节点引用。
    /// 返回第一个节点作为入口。
    pub fn build(self) -> Result<ExecutionPlan, WorkflowError> {
        if self.nodes.is_empty() {
            return Err(WorkflowError::ValidationError("empty execution plan".into()));
        }

        // 构建前校验作用域引用
        self.validate_scope()?;

        // 构建邻接表：result_name → 依赖它的节点 ID 集合
        let name_to_id: HashMap<String, NodeId> = self.nodes.iter()
            .filter(|(_, n)| !n.result_name.is_empty())
            .map(|(_, n)| (n.result_name.clone(), n.id))
            .collect();

        // 计算入度
        let mut in_degree: HashMap<NodeId, usize> = self.nodes.keys().map(|&id| (id, 0)).collect();
        let mut adj: HashMap<NodeId, Vec<NodeId>> = self.nodes.keys().map(|&id| (id, Vec::new())).collect();

        for (_, node) in &self.nodes {
            for dep_name in &node.dependencies {
                if let Some(&dep_id) = name_to_id.get(dep_name) {
                    adj.get_mut(&dep_id).unwrap().push(node.id);
                    *in_degree.get_mut(&node.id).unwrap() += 1;
                }
                // 如果 dep_name 未找到，可能是外部输入（执行时校验）
            }
        }

        // Kahn 算法
        let mut queue: Vec<NodeId> = in_degree.iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(&id, _)| id)
            .collect();

        // 按 NodeId 排序保证确定性
        queue.sort_by_key(|id| id.0);

        let mut topo_order = Vec::with_capacity(self.nodes.len());
        while let Some(node_id) = queue.pop() {
            topo_order.push(node_id);
            if let Some(neighbors) = adj.get(&node_id) {
                for &neighbor in neighbors {
                    let deg = in_degree.get_mut(&neighbor).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        // 有序插入保持确定性
                        let pos = queue.binary_search_by_key(&neighbor.0, |id| id.0).unwrap_err();
                        queue.insert(pos, neighbor);
                    }
                }
            }
        }

        if topo_order.len() != self.nodes.len() {
            return Err(WorkflowError::ValidationError(
                "cycle detected in execution plan dependencies".into(),
            ));
        }

        let entry = topo_order[0];

        Ok(ExecutionPlan {
            nodes: self.nodes,
            topo_order,
            entry,
        })
    }

    /// 校验跨作用域引用：If/Loop 内部节点的 `result_name` 不能被外部节点引用。
    ///
    /// 规则：
    /// - 顶层节点的 `result_name` 全局可见
    /// - If/Loop 子节点的 `result_name` 仅对同级兄弟节点和父节点可见
    fn validate_scope(&self) -> Result<(), WorkflowError> {
        // 1. 构建 child → parent 映射
        let mut child_to_parent: HashMap<NodeId, NodeId> = HashMap::new();
        for (_, node) in &self.nodes {
            for &child_id in &node.children {
                child_to_parent.insert(child_id, node.id);
            }
        }

        // 2. 构建 name → node_id 映射
        let name_to_id: HashMap<String, NodeId> = self.nodes.iter()
            .filter(|(_, n)| !n.result_name.is_empty())
            .map(|(_, n)| (n.result_name.clone(), n.id))
            .collect();

        // 3. 校验每个节点的依赖
        for (_, node) in &self.nodes {
            for dep_name in &node.dependencies {
                if let Some(&dep_id) = name_to_id.get(dep_name) {
                    // dep_id 是 If/Loop 的子节点
                    if let Some(&dep_parent) = child_to_parent.get(&dep_id) {
                        let my_parent = child_to_parent.get(&node.id);
                        let is_sibling = my_parent == Some(&dep_parent);
                        let is_parent = node.id == dep_parent;

                        if !is_sibling && !is_parent {
                            return Err(WorkflowError::ValidationError(
                                format!(
                                    "scope violation: '{}' references '{}' which is inside a nested block",
                                    node.result_name, dep_name
                                )
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl Default for PlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// 从参数值中提取依赖名列表。
fn extract_dependencies(params: &[(String, ParamValue)]) -> Vec<String> {
    let mut deps: Vec<String> = params.iter()
        .filter_map(|(_, pv)| referenced_namespace(pv).map(|s| s.to_string()))
        .collect();
    deps.sort();
    deps.dedup();
    deps
}

/// 收集子节点的传递依赖，追加到 `deps` 中（去重）。
///
/// 递归收集子节点的依赖、孙子节点的依赖等，确保 If/Loop 节点
/// 等待其子节点所需的所有上游节点完成。
///
/// 排除兄弟/后代节点的 result_name（这些依赖在子命名空间内部满足，
/// 父节点不需要等待）。
fn collect_child_deps(
    nodes: &HashMap<NodeId, Node>,
    children: &[NodeId],
    deps: &mut Vec<String>,
) {
    // 收集所有后代节点的 result_name，用于排除兄弟引用
    let descendant_names = collect_descendant_result_names(nodes, children);

    for &child_id in children {
        if let Some(child) = nodes.get(&child_id) {
            for dep in &child.dependencies {
                // 跳过后代节点间的引用（在子命名空间内满足）
                if !descendant_names.contains(dep) && !deps.contains(dep) {
                    deps.push(dep.clone());
                }
            }
            // 递归收集孙子节点的依赖
            collect_child_deps(nodes, &child.children, deps);
        }
    }
}

/// 递归收集所有后代节点的 result_name。
fn collect_descendant_result_names(
    nodes: &HashMap<NodeId, Node>,
    children: &[NodeId],
) -> std::collections::HashSet<String> {
    let mut names = std::collections::HashSet::new();
    for &child_id in children {
        if let Some(child) = nodes.get(&child_id) {
            if !child.result_name.is_empty() {
                names.insert(child.result_name.clone());
            }
            let nested = collect_descendant_result_names(nodes, &child.children);
            names.extend(nested);
        }
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::definition::{into_erased, Workflow};
    use crate::workflow::error::WorkflowError;
    use crate::workflow::model::ExecutionContext;
    use async_trait::async_trait;

    struct Double;
    #[async_trait]
    impl Workflow<i32, i32> for Double {
        fn name(&self) -> &str { "double" }
        async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<i32, WorkflowError> {
            Ok(input * 2)
        }
    }

    #[test]
    fn build_simple_plan() {
        let mut builder = PlanBuilder::new();
        let a = builder.add_workflow(
            "a",
            "double",
            vec![("input".to_string(), ParamValue::Literal("42".to_string()))],
            into_erased(Double),
        );
        let _end = builder.add_end("{a.value}");

        let plan = builder.build().unwrap();
        assert_eq!(plan.topo_order.len(), 2);
        assert_eq!(plan.entry, a);
    }

    #[test]
    fn build_with_dependencies() {
        let mut builder = PlanBuilder::new();
        let a = builder.add_workflow(
            "first",
            "double",
            vec![("input".to_string(), ParamValue::Literal("10".to_string()))],
            into_erased(Double),
        );
        let b = builder.add_workflow(
            "second",
            "double",
            vec![("input".to_string(), ParamValue::Reference {
                namespace: "first".to_string(),
                field: "value".to_string(),
            })],
            into_erased(Double),
        );
        let _end = builder.add_end("{second.value}");

        let plan = builder.build().unwrap();
        // a must come before b in topo order
        let pos_a = plan.topo_order.iter().position(|&id| id == a).unwrap();
        let pos_b = plan.topo_order.iter().position(|&id| id == b).unwrap();
        assert!(pos_a < pos_b);
    }

    #[test]
    fn detect_cycle() {
        let mut builder = PlanBuilder::new();
        builder.add_workflow(
            "a",
            "double",
            vec![("input".to_string(), ParamValue::Reference {
                namespace: "b".to_string(),
                field: "value".to_string(),
            })],
            into_erased(Double),
        );
        builder.add_workflow(
            "b",
            "double",
            vec![("input".to_string(), ParamValue::Reference {
                namespace: "a".to_string(),
                field: "value".to_string(),
            })],
            into_erased(Double),
        );
        builder.add_end("{b.value}");

        let result = builder.build();
        assert!(result.is_err(), "expected cycle detection error");
        let msg = result.err().unwrap().to_string();
        assert!(msg.contains("cycle"));
    }

    #[test]
    fn independent_nodes_concurrent_order() {
        let mut builder = PlanBuilder::new();
        let a = builder.add_workflow(
            "a",
            "double",
            vec![("input".to_string(), ParamValue::Literal("1".to_string()))],
            into_erased(Double),
        );
        let b = builder.add_workflow(
            "b",
            "double",
            vec![("input".to_string(), ParamValue::Literal("2".to_string()))],
            into_erased(Double),
        );
        let _end = builder.add_end("{a.value}");

        let plan = builder.build().unwrap();
        // Both a and b have no deps, end depends on a
        // a and b should come before end
        let pos_end = plan.topo_order.iter().position(|&id| id != a && id != b).unwrap();
        assert!(pos_end == 2); // end is last
    }
}
