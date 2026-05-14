//! # 逻辑判断 Builtin Workflow
//!
//! 条件判断和逻辑运算工作流，内部使用 [`MapFn`]
//! 和 [`PredicateFn`] 服务。

use async_trait::async_trait;

use crate::workflow::definition::Workflow;
use crate::workflow::error::WorkflowError;
use crate::workflow::model::ExecutionContext;
use crate::workflow::services::{MapFn, PredicateFn};

/// 大于：`i32 → bool`，input > threshold 时返回 true。
pub struct Gt {
    pub threshold: i32,
}

#[async_trait]
impl Workflow<i32, bool> for Gt {
    fn name(&self) -> &str {
        "gt"
    }

    async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
        let threshold = self.threshold;
        let svc = PredicateFn::new(move |x: &i32| *x > threshold);
        Ok(svc.check(&input))
    }
}

/// 小于：`i32 → bool`，input < threshold 时返回 true。
pub struct Lt {
    pub threshold: i32,
}

#[async_trait]
impl Workflow<i32, bool> for Lt {
    fn name(&self) -> &str {
        "lt"
    }

    async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
        let threshold = self.threshold;
        let svc = PredicateFn::new(move |x: &i32| *x < threshold);
        Ok(svc.check(&input))
    }
}

/// 等于：`i32 → bool`，input == value 时返回 true。
pub struct Eq {
    pub value: i32,
}

#[async_trait]
impl Workflow<i32, bool> for Eq {
    fn name(&self) -> &str {
        "eq"
    }

    async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
        let value = self.value;
        let svc = PredicateFn::new(move |x: &i32| *x == value);
        Ok(svc.check(&input))
    }
}

/// 大于等于：`i32 → bool`，input >= threshold 时返回 true。
pub struct Gte {
    pub threshold: i32,
}

#[async_trait]
impl Workflow<i32, bool> for Gte {
    fn name(&self) -> &str {
        "gte"
    }

    async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
        let threshold = self.threshold;
        let svc = PredicateFn::new(move |x: &i32| *x >= threshold);
        Ok(svc.check(&input))
    }
}

/// 小于等于：`i32 → bool`，input <= threshold 时返回 true。
pub struct Lte {
    pub threshold: i32,
}

#[async_trait]
impl Workflow<i32, bool> for Lte {
    fn name(&self) -> &str {
        "lte"
    }

    async fn execute(&self, input: i32, _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
        let threshold = self.threshold;
        let svc = PredicateFn::new(move |x: &i32| *x <= threshold);
        Ok(svc.check(&input))
    }
}

/// 布尔取反：`bool → bool`。
pub struct Not;

#[async_trait]
impl Workflow<bool, bool> for Not {
    fn name(&self) -> &str {
        "not"
    }

    async fn execute(&self, input: bool, _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
        let svc = MapFn::new(|b: bool| !b);
        Ok(svc.apply(input))
    }
}

/// 逻辑与：`(bool, bool) → bool`。
///
/// 需要配合 ProductJoin 节点将两个 bool 值合并为 tuple。
pub struct And;

#[async_trait]
impl Workflow<(bool, bool), bool> for And {
    fn name(&self) -> &str {
        "and"
    }

    async fn execute(&self, input: (bool, bool), _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
        Ok(input.0 && input.1)
    }
}

/// 逻辑或：`(bool, bool) → bool`。
///
/// 需要配合 ProductJoin 节点将两个 bool 值合并为 tuple。
pub struct Or;

#[async_trait]
impl Workflow<(bool, bool), bool> for Or {
    fn name(&self) -> &str {
        "or"
    }

    async fn execute(&self, input: (bool, bool), _ctx: &ExecutionContext) -> Result<bool, WorkflowError> {
        Ok(input.0 || input.1)
    }
}
