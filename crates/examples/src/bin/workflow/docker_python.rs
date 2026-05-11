//! 通过 DockerPlatform 在 Docker 容器中执行 Python 脚本。
//!
//! 演示如何：
//! - 创建 DockerPlatform（自动启动容器）
//! - 实现 Workflow trait 并通过 ctx.platform 调用平台方法
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

/// 将 Python 源代码写入容器，执行并返回 stdout。
struct PythonEval;

#[async_trait]
impl Workflow<String, String> for PythonEval {
    fn name(&self) -> &str { "python_eval" }
    async fn execute(&self, code: String, ctx: &ExecutionContext<'_>) -> Result<String, WorkflowError> {
        ctx.platform.write_file(Path::new("eval_script.py"), code.as_bytes()).await?;
        let output = ctx.platform.run_command("python", &["/workspace/eval_script.py"], &[]).await?;
        Ok(output.stdout_string().unwrap_or_default().trim_end().to_string())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Docker Python Workflow 示例 ===\n");

    println!("[1] 创建 DockerPlatform (python:3.12-slim)...");
    let platform = DockerPlatform::create("python:3.12-slim").await?;
    println!("    容器已启动，工作目录: {:?}\n", platform.workspace_root());

    let state = State::new();
    let ctx = ExecutionContext { state: &state, platform: &platform };

    // ── 示例 A: 直接执行 Python 代码 ────────────────────────
    println!("[2] 直接执行 Python...");
    let code = r#"
import sys
print(f"Python version: {sys.version_info.major}.{sys.version_info.minor}")
print("Hello from Docker!")
x = sum(range(1, 101))
print(f"Sum of 1..100 = {x})
"#.to_string();
    let result = PythonEval.execute(code, &ctx).await?;
    println!("    输出:\n{}\n", result);

    // ── 示例 B: DAG 管道 — Factorial → ReverseUpper ─────────
    println!("[3] DAG 管道: Factorial(5) → ReverseUpper...");
    let mut builder = DagBuilder::new();

    let factorial = builder.add_erased("factorial", into_erased(PythonEval));
    let reverse = builder.add_erased("reverse_upper", into_erased(PythonEval));

    builder.connect(factorial, reverse)?;
    builder.set_entry(factorial)?;
    builder.set_exit(reverse)?;
    let dag = builder.build()?;

    let result = Executor::execute(&dag, Box::new(5i32), &ctx).await?;
    println!("    Done");
    assert!(result.output.downcast_ref::<String>().is_some());

    // ── 清理 ────────────────────────────────────────────────
    println!("\n[4] 清理 Docker 容器...");
    platform.cleanup().await?;
    println!("    已停止并删除容器。");

    println!("\n=== 全部完成 ===");
    Ok(())
}
