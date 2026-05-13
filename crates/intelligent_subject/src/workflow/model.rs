//! # 基础类型
//!
//! 定义工作流系统中所有核心数据类型。
//!
//! - [`NodeId`] — DAG 内部节点的唯一标识
//! - [`WorkflowId`] — 已注册工作流的唯一名称
//! - [`StateStore`] — 带过期时间的 kv 存储，用于工作流间共享状态
//! - [`ExecutionContext`] — 运行时上下文，提供工作平台访问
//!
//! ## 功能实现
//!
//! 本模块为工作流系统提供四个基础构建块：
//!
//! - **[`NodeId`]** — 由 [`DagBuilder`](super::dag::DagBuilder) 在添加节点时自动递增分配的 `u64` 标识，
//!   在同一个 DAG 内唯一，不可跨 DAG 使用。
//! - **[`WorkflowId`]** — 支持命名空间格式 `"namespace@name"` 的字符串标识，
//!   用于在 [`WorkflowManager`](super::workflow_manager::WorkflowManager) 中注册和查找工作流。
//! - **[`StateStore`]** — 基于 `DashMap` 的线程安全 kv 存储，
//!   支持带 TTL 的自动过期，可在多个并发工作流之间共享状态。
//! - **[`ExecutionContext`]** — 运行时上下文，携带 `Arc<dyn WorkPlatform>`，
//!   为需要外部执行环境的工作流提供平台访问。
//!
//! ## 实现特色
//!
//! - [`StateStore`] 使用 `DashMap` 实现无锁并发读写，所有方法只需 `&self`
//! - TTL 过期采用惰性淘汰策略：`get` 时检查过期并自动清除
//! - [`WorkflowId`] 内置命名空间解析（`namespace()` / `name()`），无需额外字符串处理
//! - [`ExecutionContext`] 使用 `Arc` 包装平台，可廉价克隆并 move 进异步闭包
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | `dashmap`（并发 HashMap） |
//! | 内部模块 | `crate::workflow::platform::WorkPlatform` |
//!
//! ## 示例
//!
//! **WorkflowId 命名空间解析：**
//!
//! ```
//! use intelligent_subject::workflow::model::WorkflowId;
//!
//! let id = WorkflowId::from("builtin@AddOne");
//! assert_eq!(id.namespace(), "builtin");
//! assert_eq!(id.name(), "AddOne");
//!
//! let simple: WorkflowId = "my_workflow".into();
//! assert_eq!(simple.namespace(), "");
//! assert_eq!(simple.name(), "my_workflow");
//! ```
//!
//! **StateStore 带类型和 TTL 的存取：**
//!
//! ```
//! use intelligent_subject::workflow::model::StateStore;
//! use std::time::Duration;
//!
//! let store = StateStore::new();
//! store.set("counter", 42i32, None);
//! store.set("cache", "hello".to_string(), Some(Duration::from_secs(60)));
//!
//! assert_eq!(store.get::<i32>("counter"), Some(42));
//! assert_eq!(store.get::<String>("cache"), Some("hello".to_string()));
//! ```
//!
//! **跨工作流共享 StateStore：**
//!
//! ```rust
//! use intelligent_subject::workflow::model::StateStore;
//! use std::sync::Arc;
//!
//! let store = Arc::new(StateStore::new());
//!
//! // 在不同的工作流闭包中 clone Arc<StateStore> 即可共享状态
//! let s1 = store.clone();
//! s1.set("key", "value1".to_string(), None);
//!
//! let s2 = store.clone();
//! assert_eq!(s2.get::<String>("key"), Some("value1".to_string()));
//! ```
//!
//! **创建 ExecutionContext：**
//!
//! ```rust
//! use intelligent_subject::workflow::model::ExecutionContext;
//! use intelligent_subject::workflow::platform::NullPlatform;
//! use std::sync::Arc;
//!
//! let ctx = ExecutionContext { platform: Arc::new(NullPlatform::new()) };
//! assert!(ctx.platform.workspace_root().exists());
//! ```

use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
/// use intelligent_subject::workflow::model::WorkflowId;
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

/// 带过期时间的线程安全 kv 存储。
///
/// 使用 [`DashMap`](https://docs.rs/dashmap) 实现，可在多个并发工作流之间安全共享。
/// 每个条目可选地关联一个 TTL（生存时间），过期后自动失效。
///
/// # 示例
///
/// ```
/// use intelligent_subject::workflow::model::StateStore;
/// use std::time::Duration;
///
/// let store = StateStore::new();
/// store.set("counter", 42i32, None);
/// store.set("cache", "hello".to_string(), Some(Duration::from_secs(60)));
///
/// assert_eq!(store.get::<i32>("counter"), Some(42));
/// assert_eq!(store.get::<String>("cache"), Some("hello".to_string()));
/// ```
pub struct StateStore {
    entries: DashMap<String, (Box<dyn Any + Send + Sync>, Option<Instant>)>,
}

impl StateStore {
    /// 创建一个空的 StateStore。
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// 插入一个类型化的值，可选 TTL。
    ///
    /// 如果 key 已存在，旧值将被替换。
    pub fn set<T: Send + Sync + 'static>(
        &self,
        key: impl Into<String>,
        value: T,
        ttl: Option<Duration>,
    ) {
        let deadline = ttl.map(|d| Instant::now() + d);
        self.entries.insert(key.into(), (Box::new(value), deadline));
    }

    /// 获取一个类型化的值的克隆。
    ///
    /// 如果 key 不存在、类型不匹配或已过期，返回 `None`。
    /// 过期条目会被自动清除。
    pub fn get<T: Clone + Send + Sync + 'static>(&self, key: &str) -> Option<T> {
        let entry = self.entries.get(key)?;
        let (value, deadline) = entry.value();
        if let Some(dl) = deadline {
            if Instant::now() >= *dl {
                drop(entry);
                self.entries.remove(key);
                return None;
            }
        }
        value.downcast_ref::<T>().cloned()
    }

    /// 移除一个 key。
    ///
    /// 返回是否确实移除了一个未过期的条目。
    pub fn remove(&self, key: &str) -> bool {
        self.entries.remove(key).is_some()
    }

    /// 检查 key 是否存在且未过期。
    pub fn contains(&self, key: &str) -> bool {
        self.get::<()>(key).is_some() || self.check_non_clone(key)
    }

    fn check_non_clone(&self, key: &str) -> bool {
        let entry = match self.entries.get(key) {
            Some(e) => e,
            None => return false,
        };
        let (_, deadline) = entry.value();
        if let Some(dl) = deadline {
            if Instant::now() >= *dl {
                drop(entry);
                self.entries.remove(key);
                return false;
            }
        }
        true
    }
}

impl Default for StateStore {
    fn default() -> Self {
        Self::new()
    }
}

/// 工作流执行时的运行时上下文。
///
/// 提供对工作平台的访问。
/// 大多数内建工作流不需要使用平台，只有需要执行外部脚本
/// （如 Python）的工作流才会通过 `platform` 字段调用
/// [`WorkPlatform`] 的方法。
///
/// `platform` 为 `Arc` 包装，可廉价克隆。
/// 在异步闭包中需要平台时，clone `Arc` 后 move 进 async 块即可。
pub struct ExecutionContext {
    /// 工作平台（命令执行、文件读写、资源管理）。
    pub platform: Arc<dyn WorkPlatform>,
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
    fn state_store_typed_access() {
        let store = StateStore::new();
        store.set("count", 42i32, None);
        store.set("name", "test".to_string(), None);

        assert_eq!(store.get::<i32>("count"), Some(42));
        assert_eq!(store.get::<String>("name"), Some("test".to_string()));
        assert_eq!(store.get::<i32>("missing"), None);
        assert!(store.contains("count"));
        assert!(!store.contains("missing"));
    }

    #[test]
    fn state_store_remove() {
        let store = StateStore::new();
        store.set("val", 100i64, None);
        assert!(store.remove("val"));
        assert_eq!(store.get::<i64>("val"), None);
    }

    #[test]
    fn state_store_ttl_expiration() {
        let store = StateStore::new();
        store.set("ephemeral", 1i32, Some(Duration::from_millis(1)));
        assert_eq!(store.get::<i32>("ephemeral"), Some(1));

        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(store.get::<i32>("ephemeral"), None);
        assert!(!store.contains("ephemeral"));
    }
}
