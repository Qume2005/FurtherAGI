//! 通过 DockerPlatform 在 Docker 容器中执行 Python 脚本。
//!
//! 演示如何：
//! - 创建 DockerPlatform（自动启动容器）
//! - 实现 Workflow<I, O> 并通过 ctx.platform 调用平台方法
//! - 在 DAG 中组合多个 Python 执行节点
//!
//! **前提**：本地运行 Docker daemon，且已拉取 `python:3.12-slim` 镜像。
//!
//! ```sh
//! docker pull python:3.12-slim
//! cargo run -p examples --bin workflow_docker_python
//! ```

use async_trait::async_trait;
use autonomous::workflow::dag::DagBuilder;
use autonomous::workflow::error::WorkflowError;
use autonomous::workflow::executor::Executor;
use autonomous::workflow::platform::{DockerPlatform, WorkPlatform};
use autonomous::workflow::traits::{into_erased, Workflow};
use autonomous::workflow::types::{ExecutionContext, State};
use std::path::Path;

// ── Workflow 1: 在 Docker 中执行 Python 代码 ─────────────────

/// 将 Python 源代码写入容器，执行并返回 stdout。
///
/// 输入：Python 源代码 (String)
/// 输出：stdout 内容 (String)
struct PythonEval;

#[async_trait]
impl Workflow<String, String> for PythonEval {
    fn name(&self) -> &str {
        "python_eval"
    }

    async fn execute(
        &self,
        code: String,
        ctx: &ExecutionContext<'_>,
    ) -> Result<String, WorkflowError> {
        // 将脚本写入平台工作目录
        ctx.platform
            .write_file(Path::new("eval_script.py"), code.as_bytes())
            .await?;

        // 在容器中执行
        let output = ctx.platform
            .run_command("python", &["/workspace/eval_script.py"], &[])
            .await?;

        Ok(output.stdout_string().unwrap_or_default().trim_end().to_string())
    }
}

// ── Workflow 2: 用 Python 做数学计算 ─────────────────────────

/// 接收一个 i32，通过 Python 计算阶乘后返回。
///
/// 输入：整数 (i32)
/// 输出：阶乘字符串 (String)
struct Factorial;

#[async_trait]
impl Workflow<i32, String> for Factorial {
    fn name(&self) -> &str {
        "factorial"
    }

    async fn execute(
        &self,
        n: i32,
        ctx: &ExecutionContext<'_>,
    ) -> Result<String, WorkflowError> {
        let code = format!(
            r#"
import math
result = math.factorial({n})
print(result)
"#
        );

        ctx.platform
            .write_file(Path::new("factorial.py"), code.as_bytes())
            .await?;

        let output = ctx.platform
            .run_command("python", &["/workspace/factorial.py"], &[])
            .await?;

        Ok(output.stdout_string().unwrap_or_default().trim_end().to_string())
    }
}

// ── Workflow 3: Python 字符串处理 ─────────────────────────────

/// 接收字符串，通过 Python 反转并转大写。
///
/// 输入：字符串 (String)
/// 输出：处理后的字符串 (String)
struct ReverseUpper;

#[async_trait]
impl Workflow<String, String> for ReverseUpper {
    fn name(&self) -> &str {
        "reverse_upper"
    }

    async fn execute(
        &self,
        input: String,
        ctx: &ExecutionContext<'_>,
    ) -> Result<String, WorkflowError> {
        let code = format!(
            r#"
text = {input:?}
print(text[::-1].upper())
"#
        );

        ctx.platform
            .write_file(Path::new("transform.py"), code.as_bytes())
            .await?;

        let output = ctx.platform
            .run_command("python", &["/workspace/transform.py"], &[])
            .await?;

        Ok(output.stdout_string().unwrap_or_default().trim_end().to_string())
    }
}

// ── main ─────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Docker Python Workflow 示例 ===\n");

    // 创建 Docker 平台（拉取镜像 → 创建容器 → bind mount 工作目录）
    println!("[1] 创建 DockerPlatform (python:3.12-slim)...");
    let platform = DockerPlatform::create("python:3.12-slim").await?;
    println!("    容器已启动，工作目录: {:?}\n", platform.workspace_root());

    let state = State::new();
    let ctx = ExecutionContext {
        state: &state,
        platform: &platform,
    };

    // ── 示例 A: 直接执行 PythonEval ─────────────────────────
    println!("[2] 直接执行 PythonEval...");
    let python_eval = PythonEval;
    let code = r#"
import sys
print(f"Python version: {sys.version_info.major}.{sys.version_info.minor}")
print("Hello from Docker!")
x = sum(range(1, 101))
print(f"Sum of 1..100 = {x}")
"#.to_string();

    let result = python_eval.execute(code, &ctx).await?;
    println!("    输出:\n{}\n", result);

    // ── 示例 B: 执行 Factorial ──────────────────────────────
    println!("[3] 执行 Factorial(10)...");
    let factorial = Factorial;
    let result = factorial.execute(10, &ctx).await?;
    println!("    10! = {}\n", result);

    // ── 示例 C: DAG 管道 — Factorial → ReverseUpper ─────────
    println!("[4] DAG 管道: Factorial(5) → ReverseUpper...");
    let mut builder = DagBuilder::new();

    let factorial_node = builder.add_workflow(
        "factorial",
        into_erased(Factorial),
    );
    // ReverseUpper 接收 String，Factorial 输出 String，类型匹配
    let reverse_node = builder.add_workflow(
        "reverse_upper",
        into_erased(ReverseUpper),
    );

    builder.connect(factorial_node, reverse_node)?;
    builder.set_entry(factorial_node)?;
    builder.set_exit(reverse_node)?;

    let dag = builder.build()?;

    // 输入 5 → Factorial(5) = "120" → ReverseUpper("120") = "021"
    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await?;
    let output: &String = result.output.downcast_ref::<String>().unwrap();
    println!("    5! = 120, reversed & uppered = {}", output);
    assert_eq!(output, "021");

    // ── 清理 ────────────────────────────────────────────────
    println!("\n[5] 清理 Docker 容器...");
    platform.cleanup().await?;
    println!("    已停止并删除容器。");

    println!("\n=== 全部完成 ===");
    Ok(())
}
