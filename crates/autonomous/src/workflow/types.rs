//! # 基础类型
//!
//! 定义工作流系统中所有核心数据类型。
//!
//! - [`NodeId`] — DAG 内部节点的唯一标识
//! - [`WorkflowId`] — 已注册工作流的唯一名称
//! - [`State`] — 线程安全的类型化键值存储，用于工作流间共享参数
//! - [`ExecutionContext`] — 运行时上下文，在每次执行时传递给工作流

use std::any::Any;

use dashmap::DashMap;

use crate::workflow::platform::WorkPlatform;

/// DAG 内部节点的唯一标识。
///
/// 由 [`DagBuilder`](super::dag::DagBuilder) 在添加节点时自动分配。
/// 在同一个 DAG 内唯一，不可跨 DAG 使用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

/// 已注册工作流的唯一标识。
///
/// 支持命名空间格式 `namespace@name`（如 `"builtin@AddOne"`），
/// 也支持无命名空间的简单格式（如 `"my_workflow"`）。
///
/// 可通过 `From<&str>` / `From<String>` 创建，也可以用 [`WorkflowId::from_str`]。
///
/// # 示例
///
/// ```
/// use autonomous::workflow::types::WorkflowId;
///
/// let id = WorkflowId::from("builtin@AddOne");
/// assert_eq!(id.namespace(), "builtin");
/// assert_eq!(id.name(), "AddOne");
/// assert_eq!(id.as_str(), "builtin@AddOne");
///
/// let simple: WorkflowId = "my_workflow".into();
/// assert_eq!(simple.namespace(), "");
/// assert_eq!(simple.name(), "my_workflow");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkflowId(pub String);

impl WorkflowId {
    /// 从字符串创建工作流 ID。
    ///
    /// 支持 `"namespace@name"` 或 `"name"` 格式。
    pub fn from_str(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// 以 `&str` 形式获取完整 ID。
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 返回命名空间部分（`@` 之前），无 `@` 则为空串。
    pub fn namespace(&self) -> &str {
        match self.0.split_once('@') {
            Some((ns, _)) => ns,
            None => "",
        }
    }

    /// 返回名称部分（`@` 之后），无 `@` 则为完整字符串。
    pub fn name(&self) -> &str {
        match self.0.split_once('@') {
            Some((_, name)) => name,
            None => &self.0,
        }
    }
}

impl From<&str> for WorkflowId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for WorkflowId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&String> for WorkflowId {
    fn from(s: &String) -> Self {
        Self(s.clone())
    }
}

impl From<&WorkflowId> for WorkflowId {
    fn from(id: &WorkflowId) -> Self {
        id.clone()
    }
}

impl std::fmt::Display for WorkflowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 线程安全的类型化参数存储。
///
/// 使用 [`DashMap`](https://docs.rs/dashmap) 实现，可在多个并发工作流之间安全共享。
/// 所有值都是 `Send + Sync + 'static` 的类型化数据。
///
/// # 示例
///
/// ```
/// use autonomous::workflow::types::State;
///
/// let state = State::new();
/// state.set("counter", 42i32);
/// state.set("label", "hello".to_string());
///
/// assert_eq!(state.get::<i32>("counter"), Some(42));
/// assert_eq!(state.get::<String>("label"), Some("hello".to_string()));
/// ```
pub struct State {
    entries: DashMap<String, Box<dyn Any + Send + Sync>>,
}

impl State {
    /// 创建一个空的 State。
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// 插入一个类型化的值。
    ///
    /// 如果 key 已存在，旧值将被替换。
    pub fn set<T: Send + Sync + 'static>(&self, key: impl Into<String>, value: T) {
        self.entries.insert(key.into(), Box::new(value));
    }

    /// 获取一个类型化的值的克隆。
    ///
    /// 如果 key 不存在或类型不匹配，返回 `None`。
    /// 要求值类型实现 `Clone`。
    pub fn get<T: Send + Sync + 'static>(&self, key: &str) -> Option<T>
    where
        T: Clone,
    {
        self.entries.get(key).and_then(|v| v.value().downcast_ref::<T>().cloned())
    }

    /// 获取条目的引用（不克隆）。
    ///
    /// 返回 [`DashMap`](https://docs.rs/dashmap) 的 `Ref` guard。
    pub fn get_ref<T: Send + Sync + 'static>(&self, key: &str) -> Option<dashmap::mapref::one::Ref<'_, String, Box<dyn Any + Send + Sync>>> {
        self.entries.get(key)
    }

    /// 移除一个 key 并返回其类型化的值。
    ///
    /// 如果 key 不存在或类型不匹配，返回 `None`。
    pub fn remove<T: Send + Sync + 'static>(&self, key: &str) -> Option<T> {
        self.entries.remove(key).and_then(|(_, v)| v.downcast::<T>().ok()).map(|b| *b)
    }

    /// 检查 key 是否存在。
    pub fn contains(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }
}

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

/// 工作流执行时的运行时上下文。
///
/// 在每次工作流执行时创建，提供对共享状态和工作平台的访问。
/// 大多数内建工作流不需要使用平台，只有需要执行外部脚本
/// （如 Python）的工作流才会通过 `platform` 字段调用
/// [`WorkPlatform`] 的方法。
///
/// # 生命周期
///
/// `ExecutionContext<'a>` 的生命周期与执行过程绑定，
/// 不应在执行完成后持有。
pub struct ExecutionContext<'a> {
    /// 托管参数存储。
    pub state: &'a State,
    /// 工作平台（命令执行、文件读写、资源管理）。
    pub platform: &'a dyn WorkPlatform,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_id_new_and_display() {
        let id = WorkflowId::from("my_workflow");
        assert_eq!(id.as_str(), "my_workflow");
        assert_eq!(format!("{id}"), "my_workflow");
    }

    #[test]
    fn workflow_id_namespace() {
        let id = WorkflowId::from("builtin@AddOne");
        assert_eq!(id.namespace(), "builtin");
        assert_eq!(id.name(), "AddOne");
        assert_eq!(id.as_str(), "builtin@AddOne");

        let simple = WorkflowId::from("plain");
        assert_eq!(simple.namespace(), "");
        assert_eq!(simple.name(), "plain");
    }

    #[test]
    fn state_typed_access() {
        let state = State::new();
        state.set("count", 42i32);
        state.set("name", "test".to_string());

        assert_eq!(state.get::<i32>("count"), Some(42));
        assert_eq!(state.get::<String>("name"), Some("test".to_string()));
        assert_eq!(state.get::<i32>("missing"), None);
        assert!(state.contains("count"));
        assert!(!state.contains("missing"));
    }

    #[test]
    fn state_remove() {
        let state = State::new();
        state.set("val", 100i64);
        assert_eq!(state.remove::<i64>("val"), Some(100));
        assert_eq!(state.get::<i64>("val"), None);
    }
}
