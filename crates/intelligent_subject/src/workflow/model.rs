//! # 基础类型
//!
//! 定义工作流系统中所有核心数据类型。
//!
//! - [`NodeId`] — DAG 内部节点的唯一标识
//! - [`WorkflowId`] — 已注册工作流的唯一名称
//! - [`StateStore`] — 带过期时间的 kv 存储，用于工作流间共享状态
//! - [`Namespace`] — 带作用域链的命名空间，用于节点间数据传递
//! - [`ExecutionContext`] — 运行时上下文，提供工作平台访问

use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::workflow::platform::WorkPlatform;

/// DAG 内部节点的唯一标识。
///
/// 由 [`PlanBuilder`](super::dag::PlanBuilder) 在添加节点时自动分配。
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

/// 带作用域链的命名空间，用于工作流节点间的数据传递。
///
/// 每个工作流节点执行后将结果存入命名空间（如 `beijing_weather.weather`），
/// 后续节点通过 `get()` / `get_typed()` 按名称引用。
///
/// 支持作用域链：子命名空间（如循环体）可以引用父命名空间的值。
/// 查找时先查自身，未命中则沿 parent 链向上查找。
///
/// 内部使用 `Arc<dyn Any + Send + Sync>` 存储值，克隆廉价，可被多个读者安全引用。
///
/// # 示例
///
/// ```
/// use intelligent_subject::workflow::model::Namespace;
///
/// let ns = Namespace::new();
/// ns.set("beijing.weather", "sunny".to_string());
/// ns.set("beijing.temp", 25i32);
///
/// assert_eq!(ns.get_typed::<String>("beijing.weather"), Some("sunny".to_string()));
/// assert_eq!(ns.get_typed::<i32>("beijing.temp"), Some(25));
///
/// // 子作用域可以访问父命名空间
/// let child = Namespace::new_with_parent(&ns);
/// child.set("local.data", 42i32);
/// assert_eq!(child.get_typed::<String>("beijing.weather"), Some("sunny".to_string()));
/// assert_eq!(child.get_typed::<i32>("local.data"), Some(42));
/// ```
pub struct Namespace {
    entries: DashMap<String, Arc<dyn Any + Send + Sync>>,
    parent: Option<Arc<Namespace>>,
}

impl Namespace {
    /// 创建一个空的根命名空间（无 parent）。
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            parent: None,
        }
    }

    /// 创建一个带父作用域的子命名空间。
    ///
    /// 查找时先查自身，未命中则沿 parent 链向上查找。
    pub fn new_with_parent(parent: &Namespace) -> Self {
        Self {
            entries: DashMap::new(),
            parent: Some(Arc::new(Self {
                entries: parent.entries.clone(),
                parent: parent.parent.clone(),
            })),
        }
    }

    /// 存入一个值。
    pub fn set(&self, key: impl Into<String>, value: impl Any + Send + Sync + 'static) {
        self.entries.insert(key.into(), Arc::new(value));
    }

    /// 存入一个已经 `Arc` 包装的值。
    ///
    /// 用于将 `Box<dyn Any>` 转为 `Arc` 后直接存入，避免双重包装。
    pub fn set_arc(&self, key: impl Into<String>, value: Arc<dyn Any + Send + Sync>) {
        self.entries.insert(key.into(), value);
    }

    /// 获取一个值的 `Arc` 引用（无需泛型参数）。
    ///
    /// 查找顺序：自身 → parent → parent.parent → ...
    /// 未找到返回 `None`。
    pub fn get(&self, key: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        if let Some(entry) = self.entries.get(key) {
            return Some(Arc::clone(entry.value()));
        }
        if let Some(ref parent) = self.parent {
            return parent.get(key);
        }
        None
    }

    /// 获取一个强类型值的克隆。
    ///
    /// 内部调用 [`get()`](Self::get)，然后 downcast 为 `T` 并 clone。
    /// 如果 key 不存在或类型不匹配，返回 `None`。
    pub fn get_typed<T: Clone + Send + Sync + 'static>(&self, key: &str) -> Option<T> {
        let arc = self.get(key)?;
        arc.downcast_ref::<T>().cloned()
    }

    /// 移除一个 key（仅从当前作用域）。
    pub fn remove(&self, key: &str) -> bool {
        self.entries.remove(key).is_some()
    }

    /// 检查 key 是否存在（沿作用域链查找）。
    pub fn contains(&self, key: &str) -> bool {
        self.get(key).is_some()
    }
}

impl Default for Namespace {
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

    #[test]
    fn namespace_basic_set_get() {
        let ns = Namespace::new();
        ns.set("weather.temp", 25i32);
        ns.set("weather.condition", "sunny".to_string());

        assert_eq!(ns.get_typed::<i32>("weather.temp"), Some(25));
        assert_eq!(
            ns.get_typed::<String>("weather.condition"),
            Some("sunny".to_string())
        );
        assert_eq!(ns.get_typed::<i32>("missing"), None);
    }

    #[test]
    fn namespace_scoped_lookup() {
        let parent = Namespace::new();
        parent.set("outer.value", 100i32);

        let child = Namespace::new_with_parent(&parent);
        child.set("inner.value", 200i32);

        // 子节点可以读取自己的值
        assert_eq!(child.get_typed::<i32>("inner.value"), Some(200));
        // 子节点可以读取父节点的值
        assert_eq!(child.get_typed::<i32>("outer.value"), Some(100));
        // 父节点不能读取子节点的值
        assert_eq!(parent.get_typed::<i32>("inner.value"), None);
    }

    #[test]
    fn namespace_remove_and_contains() {
        let ns = Namespace::new();
        ns.set("key", 42i32);
        assert!(ns.contains("key"));
        assert!(ns.remove("key"));
        assert!(!ns.contains("key"));
    }
}
