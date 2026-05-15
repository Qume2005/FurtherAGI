//! # 参数值解析
//!
//! 解析 XML 属性值中的 `{namespace.field}` 引用语法。
//!
//! ## 语法
//!
//! - 字面量：`city="Beijing"` → `ParamValue::Literal("Beijing")`
//! - 引用：`city="{other.weather}"` → `ParamValue::Reference { namespace: "other", field: "weather" }`
//! - 混合：`template="天气为{beijing.weather}"` → 暂不支持混合，整个值要么是字面量要么是引用
//!
//! ## 引用格式
//!
//! `{namespace.field}` — 花括号包裹，点号分隔命名空间和字段名。

/// XML 属性值的解析结果。
///
/// | 变体 | 来源 | 说明 |
/// |------|------|------|
/// | `Literal(String)` | `city="Beijing"` | 纯字面量，运行时直接使用 |
/// | `Reference` | `city="{other.weather}"` | 命名空间引用，运行时从 Namespace 解析 |
#[derive(Debug, Clone)]
pub enum ParamValue {
    /// 字面量值。
    Literal(String),
    /// 命名空间引用。
    Reference {
        /// 命名空间名（即上游节点的 `result_name`）。
        namespace: String,
        /// 字段名（即上游节点输出的字段名）。
        field: String,
    },
}

/// 解析 XML 属性值为 [`ParamValue`]。
///
/// 规则：
/// - 以 `{` 开头且以 `}` 结尾，中间包含 `.` → `Reference { namespace, field }`
/// - 其他情况 → `Literal`
///
/// # 示例
///
/// ```
/// use intelligent_subject::workflow::config::{ParamValue, parse_param_value};
///
/// // 字面量
/// assert!(matches!(parse_param_value("Beijing"), ParamValue::Literal(s) if s == "Beijing"));
///
/// // 引用
/// let ref_val = parse_param_value("{beijing_weather.weather}");
/// assert!(matches!(ref_val, ParamValue::Reference { namespace, field }
///     if namespace == "beijing_weather" && field == "weather"));
///
/// // 不含点的花括号 → 引用整个命名空间值
/// assert!(matches!(parse_param_value("{report}"), ParamValue::Reference { .. }));
///
/// // 空字符串 → 字面量
/// assert!(matches!(parse_param_value(""), ParamValue::Literal(s) if s.is_empty()));
/// ```
pub fn parse_param_value(s: &str) -> ParamValue {
    if let Some(inner) = s.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
        if inner.is_empty() {
            return ParamValue::Literal(s.to_string());
        }
        if let Some((namespace, field)) = inner.split_once('.') {
            if !namespace.is_empty() && !field.is_empty() {
                return ParamValue::Reference {
                    namespace: namespace.to_string(),
                    field: field.to_string(),
                };
            }
        } else {
            // {name} 不含点 — 引用整个命名空间值
            return ParamValue::Reference {
                namespace: inner.to_string(),
                field: String::new(),
            };
        }
    }
    ParamValue::Literal(s.to_string())
}

/// 从 `ParamValue` 中提取所引用的命名空间名（如果有的话）。
///
/// 返回 `Some(namespace)` 用于依赖图构建，`None` 表示字面量（无依赖）。
pub fn referenced_namespace(value: &ParamValue) -> Option<&str> {
    match value {
        ParamValue::Reference { namespace, .. } => Some(namespace),
        ParamValue::Literal(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_literal() {
        assert!(matches!(parse_param_value("Beijing"), ParamValue::Literal(s) if s == "Beijing"));
        assert!(matches!(parse_param_value("42"), ParamValue::Literal(s) if s == "42"));
        assert!(matches!(parse_param_value(""), ParamValue::Literal(s) if s.is_empty()));
    }

    #[test]
    fn parse_reference() {
        let v = parse_param_value("{beijing_weather.weather}");
        match &v {
            ParamValue::Reference { namespace, field } => {
                assert_eq!(namespace, "beijing_weather");
                assert_eq!(field, "weather");
            }
            _ => panic!("expected Reference, got {v:?}"),
        }
    }

    #[test]
    fn parse_no_dot_is_reference() {
        // {name} without dot — reference to entire namespace value
        let v = parse_param_value("{no_dot}");
        match &v {
            ParamValue::Reference { namespace, field } => {
                assert_eq!(namespace, "no_dot");
                assert_eq!(field, "");
            }
            _ => panic!("expected Reference, got {v:?}"),
        }
    }

    #[test]
    fn parse_empty_parts_is_literal() {
        assert!(matches!(parse_param_value("{.field}"), ParamValue::Literal(_)));
        assert!(matches!(parse_param_value("{ns.}"), ParamValue::Literal(_)));
        assert!(matches!(parse_param_value("{.}"), ParamValue::Literal(_)));
    }

    #[test]
    fn referenced_namespace_test() {
        let lit = ParamValue::Literal("Beijing".to_string());
        assert!(referenced_namespace(&lit).is_none());

        let reff = ParamValue::Reference {
            namespace: "weather".to_string(),
            field: "temp".to_string(),
        };
        assert_eq!(referenced_namespace(&reff), Some("weather"));
    }
}
