//! # DAG 构建器 — 连接与构建
//!
//! 在 [`DagBuilder`](super::DagBuilder) 上实现连接和构建方法。
//!
//! ## 功能实现
//!
//! 本模块提供 DAG 的边创建、入口/出口设置和最终构建方法：
//!
//! - **[`connect()`](super::DagBuilder::connect)** — 连接两个节点，立即校验类型兼容性
//! - **[`connect_labeled()`](super::DagBuilder::connect_labeled)** — 带标签的连接（用于条件分支 `"true"` / `"false"`）
//! - **[`set_entry()`](super::DagBuilder::set_entry)** — 指定 DAG 入口节点
//! - **[`set_exit()`](super::DagBuilder::set_exit)** — 指定 DAG 出口节点
//! - **[`build()`](super::DagBuilder::build)** — 消费 builder，执行环检测，生成不可变 `WorkflowDag`
//!
//! ## 实现特色
//!
//! - 即时类型校验：`connect()` 在调用时检查 `TypeId` 兼容性，而非推迟到 `build()`
//! - `connect_labeled()` 为边添加字符串标签，支持条件分支路由
//! - `build()` 使用 Kahn 算法计算拓扑排序并检测环
//! - 拓扑排序缓存在 [`WorkflowDag`](super::WorkflowDag) 中，执行时直接复用
//! - builder 在 `build()` 中被消费，防止构建后修改
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | 无 |
//! | 内部模块 | [`crate::workflow::error::WorkflowError`]、[`crate::workflow::model::NodeId`] |
//!
//! ## 示例
//!
//! **成功构建流水线：**
//!
//! ```rust
//! use intelligent_subject::workflow::dag::DagBuilder;
//! use intelligent_subject::workflow::error::WorkflowError;
//!
//! let mut builder = DagBuilder::new();
//! let a = builder.add("ns@A", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input + 1)
//! });
//! let b = builder.add("ns@B", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input * 2)
//! });
//!
//! builder.connect(a, b).unwrap();
//! builder.set_entry(a).unwrap();
//! builder.set_exit(b).unwrap();
//!
//! let dag = builder.build().unwrap();
//! assert_eq!(dag.topo_order().len(), 2);
//! ```
//!
//! **环检测：**
//!
//! ```rust
//! use intelligent_subject::workflow::dag::DagBuilder;
//! use intelligent_subject::workflow::error::{WorkflowError};
//!
//! let mut builder = DagBuilder::new();
//! let a = builder.add("ns@A", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input)
//! });
//! let b = builder.add("ns@B", |input: i32| async move {
//!     Ok::<i32, WorkflowError>(input)
//! });
//!
//! builder.connect(a, b).unwrap();
//! builder.connect(b, a).unwrap(); // 形成环
//!
//! let result = builder.build();
//! assert!(matches!(result, Err(WorkflowError::CycleDetected { .. })));
//! ```

use std::collections::{HashMap, HashSet};

use super::super::error::WorkflowError;
use super::super::model::NodeId;
use super::{DagBuilder, Edge, NodeKind, WorkflowDag};
use super::graph::DagParts;

impl DagBuilder {
    /// Connect two nodes. Validates type compatibility immediately.
    pub fn connect(&mut self, from: NodeId, to: NodeId) -> Result<(), WorkflowError> {
        let to_kind = self.nodes.get(&to).map(|n| n.kind.clone());

        // Skip type validation for SumMatch and ProductJoin (heterogeneous I/O)
        let skip_type_check = matches!(
            to_kind,
            Some(NodeKind::SumMatch { .. }) | Some(NodeKind::ProductJoin { .. })
        );

        if !skip_type_check {
            let from_output = self
                .nodes
                .get(&from)
                .ok_or(WorkflowError::NodeNotFound(from))?
                .output_type;

            let to_input = self
                .nodes
                .get(&to)
                .ok_or(WorkflowError::NodeNotFound(to))?
                .input_type;

            if let (Some(out_ty), Some(in_ty)) = (from_output, to_input) {
                if out_ty != in_ty {
                    return Err(WorkflowError::type_mismatch(from, to, in_ty, out_ty));
                }
            }
        }

        self.edges.push(Edge {
            from,
            to,
            label: None,
        });
        Ok(())
    }

    /// Connect two nodes with a label (for conditional branching).
    pub fn connect_labeled(
        &mut self,
        from: NodeId,
        to: NodeId,
        label: impl Into<String>,
    ) -> Result<(), WorkflowError> {
        self.connect(from, to)?;
        self.edges.last_mut().unwrap().label = Some(label.into());
        Ok(())
    }

    /// Set the entry point of the DAG.
    pub fn set_entry(&mut self, node: NodeId) -> Result<(), WorkflowError> {
        if !self.nodes.contains_key(&node) {
            return Err(WorkflowError::NodeNotFound(node));
        }
        self.entry_node = Some(node);
        Ok(())
    }

    /// Set the exit point of the DAG.
    pub fn set_exit(&mut self, node: NodeId) -> Result<(), WorkflowError> {
        if !self.nodes.contains_key(&node) {
            return Err(WorkflowError::NodeNotFound(node));
        }
        self.exit_node = Some(node);
        Ok(())
    }

    /// Build the DAG. Performs cycle detection via Kahn's algorithm
    /// and caches the topological order.
    pub fn build(self) -> Result<WorkflowDag, WorkflowError> {
        let node_count = self.nodes.len();

        // Build adjacency list and in-degree map.
        let mut adj: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        let mut in_degree: HashMap<NodeId, usize> = HashMap::new();

        for node_id in self.nodes.keys() {
            in_degree.insert(*node_id, 0);
            adj.insert(*node_id, Vec::new());
        }

        for edge in &self.edges {
            adj.get_mut(&edge.from).unwrap().push(edge.to);
            *in_degree.get_mut(&edge.to).unwrap() += 1;
        }

        // Kahn's algorithm.
        let mut queue: Vec<NodeId> = in_degree
            .iter()
            .filter(|&(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut topo_order = Vec::with_capacity(node_count);

        while let Some(node_id) = queue.pop() {
            topo_order.push(node_id);
            if let Some(neighbors) = adj.get(&node_id) {
                for &neighbor in neighbors {
                    let deg = in_degree.get_mut(&neighbor).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push(neighbor);
                    }
                }
            }
        }

        if topo_order.len() != node_count {
            // Cycle detected: find nodes not in topo_order.
            let sorted_set: HashSet<NodeId> = topo_order.iter().copied().collect();
            let cycle_nodes: Vec<NodeId> = self
                .nodes
                .keys()
                .filter(|id| !sorted_set.contains(id))
                .copied()
                .collect();
            return Err(WorkflowError::CycleDetected { nodes: cycle_nodes });
        }

        Ok(WorkflowDag::from_parts(DagParts {
            nodes: self.nodes,
            edges: self.edges,
            entry_node: self.entry_node,
            exit_node: self.exit_node,
            topo_order,
            clone_branches: self.clone_branches,
            clone_gather_fns: self.clone_gather_fns,
            clone_input_clone_fns: self.clone_input_clone_fns,
            sum_match_fns: self.sum_match_fns,
            product_join_fns: self.product_join_fns,
            product_join_input_clone_fns: self.product_join_input_clone_fns,
        }))
    }
}
