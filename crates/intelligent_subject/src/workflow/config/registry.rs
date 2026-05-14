//! # 类型注册表（TypeRegistry）
//!
//! 将字符串类型名映射到 `TypeId` 和 `CloneFn`。
//!
//! ## 功能实现
//!
//! [`TypeRegistry`] 被 [`ConfigBuilder`](super::ConfigBuilder) 用来将 TOML 配置中的
//! 类型名字符串（如 `"i32"`、`"String"`）解析为 `DagBuilder` 所需的 `TypeId` 和
//! 广播节点克隆函数 `CloneFn`。
//!
//! 还支持注册工具类型的 serde 闭包，
//! 用于 `<tool>` 元素的 JSON 参数反序列化和输出序列化。
//!
//! ## 实现特色
//!
//! - [`with_primitives()`](TypeRegistry::with_primitives) 预注册 14 种常见 Rust 类型：
//!   `i8` ~ `i128`、`u8` ~ `u128`、`f32`、`f64`、`bool`、`String`
//! - `register::<T>()` 自动捕获 `TypeId::of::<T>()` 和 `make_clone_fn::<T>()`
//! - `register_tool_type::<T>()` 额外捕获 serde 闭包（用于 `<tool>` 元素）
//! - `get()` 返回 `(TypeId, CloneFn)` 元组，供 `DagBuilder` 直接使用
//!
//! ## 示例
//!
//! **注册和查找类型：**
//!
//! ```rust
//! use intelligent_subject::workflow::config::TypeRegistry;
//!
//! let mut types = TypeRegistry::new();
//! types.register::<i32>("i32");
//! types.register::<String>("String");
//!
//! let (id, clone_fn) = types.get("i32").unwrap();
//! assert_eq!(id, std::any::TypeId::of::<i32>());
//! ```

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

use crate::workflow::dag::{CloneFn, make_clone_fn};
use crate::workflow::error::WorkflowError;

type BoxedValue = Box<dyn std::any::Any + Send + Sync>;

type DeserializeFn = Arc<dyn Fn(&str) -> Result<BoxedValue, WorkflowError> + Send + Sync>;

type SerializeFn = Arc<dyn Fn(&BoxedValue) -> Result<String, WorkflowError> + Send + Sync>;

struct TypeInfo {
    type_id: TypeId,
    clone_fn: CloneFn,
    deserialize_fn: Option<DeserializeFn>,
    serialize_fn: Option<SerializeFn>,
}

/// Registry mapping string type names to their `TypeId` and clone function.
///
/// # Example
///
/// ```rust
/// use intelligent_subject::workflow::config::TypeRegistry;
///
/// let mut types = TypeRegistry::new();
/// types.register::<i32>("i32");
/// types.register::<String>("String");
///
/// let (id, clone_fn) = types.get("i32").unwrap();
/// assert_eq!(id, std::any::TypeId::of::<i32>());
/// ```
pub struct TypeRegistry {
    types: HashMap<String, TypeInfo>,
}

impl TypeRegistry {
    /// Create an empty type registry.
    pub fn new() -> Self {
        Self {
            types: HashMap::new(),
        }
    }

    /// Create a type registry pre-loaded with common Rust primitive types.
    ///
    /// Registered types: `i8`, `i16`, `i32`, `i64`, `i128`, `u8`, `u16`,
    /// `u32`, `u64`, `u128`, `f32`, `f64`, `bool`, `String`.
    pub fn with_primitives() -> Self {
        let mut reg = Self::new();
        reg.register::<i8>("i8");
        reg.register::<i16>("i16");
        reg.register::<i32>("i32");
        reg.register::<i64>("i64");
        reg.register::<i128>("i128");
        reg.register::<u8>("u8");
        reg.register::<u16>("u16");
        reg.register::<u32>("u32");
        reg.register::<u64>("u64");
        reg.register::<u128>("u128");
        reg.register::<f32>("f32");
        reg.register::<f64>("f64");
        reg.register::<bool>("bool");
        reg.register::<String>("String");
        reg
    }

    /// Register a type by name.
    ///
    /// `T` must implement `Clone + Send + Sync + 'static`.
    /// If the name already exists, it is overwritten.
    pub fn register<T: Clone + Send + Sync + 'static>(&mut self, name: impl Into<String>) {
        let info = TypeInfo {
            type_id: TypeId::of::<T>(),
            clone_fn: make_clone_fn::<T>(),
            deserialize_fn: None,
            serialize_fn: None,
        };
        self.types.insert(name.into(), info);
    }

    /// Register a type for tool use, including serde closures.
    ///
    /// In addition to `Clone`, `T` must implement `DeserializeOwned` and `Serialize`.
    /// Required for types used as tool input/output in XML `<tool>` elements.
    ///
    /// # Example
    ///
    /// ```
    /// use intelligent_subject::workflow::config::TypeRegistry;
    ///
    /// let mut types = TypeRegistry::new();
    /// types.register_tool_type::<serde_json::Value>("JsonValue");
    /// ```
    pub fn register_tool_type<T>(&mut self, name: impl Into<String>)
    where
        T: Clone + serde::de::DeserializeOwned + serde::Serialize + Send + Sync + 'static,
    {
        let deserialize: DeserializeFn = Arc::new(|json_str: &str| {
            serde_json::from_str::<T>(json_str)
                .map(|v| Box::new(v) as BoxedValue)
                .map_err(|e| {
                    WorkflowError::ValidationError(format!(
                        "tool type deserialization failed for '{}': {e}",
                        std::any::type_name::<T>()
                    ))
                })
        });

        let serialize: SerializeFn = Arc::new(|output: &BoxedValue| {
            output
                .downcast_ref::<T>()
                .ok_or_else(|| {
                    WorkflowError::ValidationError("tool type output downcast failed".into())
                })
                .and_then(|v| {
                    serde_json::to_string(v).map_err(|e| {
                        WorkflowError::ValidationError(format!(
                            "tool type serialization failed: {e}"
                        ))
                    })
                })
        });

        let info = TypeInfo {
            type_id: TypeId::of::<T>(),
            clone_fn: make_clone_fn::<T>(),
            deserialize_fn: Some(deserialize),
            serialize_fn: Some(serialize),
        };
        self.types.insert(name.into(), info);
    }

    /// Look up a type by name.
    ///
    /// Returns `(TypeId, CloneFn)` if found, `None` otherwise.
    pub fn get(&self, name: &str) -> Option<(TypeId, CloneFn)> {
        self.types.get(name).map(|info| (info.type_id, info.clone_fn))
    }

    /// Look up serde closures for a tool type by name.
    ///
    /// Returns `(DeserializeFn, SerializeFn)` if the type was registered via
    /// [`register_tool_type`](Self::register_tool_type), `None` otherwise.
    pub fn get_serde(
        &self,
        name: &str,
    ) -> Option<(&DeserializeFn, &SerializeFn)> {
        self.types.get(name).and_then(|info| {
            match (&info.deserialize_fn, &info.serialize_fn) {
                (Some(d), Some(s)) => Some((d, s)),
                _ => None,
            }
        })
    }
}

impl Default for TypeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_lookup() {
        let mut reg = TypeRegistry::new();
        reg.register::<i32>("i32");
        reg.register::<String>("String");

        let (id, _) = reg.get("i32").unwrap();
        assert_eq!(id, TypeId::of::<i32>());

        let (id, _) = reg.get("String").unwrap();
        assert_eq!(id, TypeId::of::<String>());

        assert!(reg.get("f64").is_none());
    }

    #[test]
    fn with_primitives_covers_common_types() {
        let reg = TypeRegistry::with_primitives();

        assert!(reg.get("i32").is_some());
        assert!(reg.get("u64").is_some());
        assert!(reg.get("f64").is_some());
        assert!(reg.get("bool").is_some());
        assert!(reg.get("String").is_some());
        assert!(reg.get("NonExistent").is_none());
    }

    #[test]
    fn clone_fn_works() {
        let reg = TypeRegistry::with_primitives();
        let (_, clone_fn) = reg.get("i32").unwrap();

        let original: Box<dyn std::any::Any + Send + Sync> = Box::new(42i32);
        let cloned = clone_fn(original.as_ref());
        assert_eq!(*cloned.downcast_ref::<i32>().unwrap(), 42);
    }
}
