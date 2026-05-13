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
//! ## 实现特色
//!
//! - [`with_primitives()`](TypeRegistry::with_primitives) 预注册 14 种常见 Rust 类型：
//!   `i8` ~ `i128`、`u8` ~ `u128`、`f32`、`f64`、`bool`、`String`
//! - `register::<T>()` 自动捕获 `TypeId::of::<T>()` 和 `make_clone_fn::<T>()`，
//!   用户只需指定类型参数和名字
//! - `get()` 返回 `(TypeId, CloneFn)` 元组，供 `DagBuilder` 直接使用
//! - 内部使用 `TypeInfo` 结构体封装存储细节，公共 API 简洁
//!
//! ## 依赖
//!
//! | 类别 | 依赖 |
//! |------|------|
//! | 外部 crate | 无 |
//! | 内部模块 | [`crate::workflow::dag::{CloneFn, make_clone_fn}`] |
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
//!
//! **预注册基础类型 + 自定义类型：**
//!
//! ```rust
//! use intelligent_subject::workflow::config::TypeRegistry;
//!
//! let mut types = TypeRegistry::with_primitives();
//! // 自定义类型
//! types.register::<Vec<String>>("VecString");
//!
//! assert!(types.get("i32").is_some());
//! assert!(types.get("VecString").is_some());
//! assert!(types.get("MyCustomType").is_none());
//! ```

use std::any::TypeId;
use std::collections::HashMap;

use crate::workflow::dag::{CloneFn, make_clone_fn};

struct TypeInfo {
    type_id: TypeId,
    clone_fn: CloneFn,
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
        };
        self.types.insert(name.into(), info);
    }

    /// Look up a type by name.
    ///
    /// Returns `(TypeId, CloneFn)` if found, `None` otherwise.
    pub fn get(&self, name: &str) -> Option<(TypeId, CloneFn)> {
        self.types.get(name).map(|info| (info.type_id, info.clone_fn))
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
