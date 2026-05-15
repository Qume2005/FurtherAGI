//! 通过 DockerPlatform 在 Docker 容器中执行 Python 脚本。
//!
//! 演示如何：
//! - 创建 DockerPlatform（自动启动容器）
//! - 通过 `add_with_ctx` 闭包访问 ctx.platform 调用平台方法
//! - 通过 WorkflowManager 串联多个 Python 执行节点
//!
//! **前提**：本地运行 Docker daemon，且已拉取 `python:3.12-slim` 镜像。
//!
//! ```sh
//! docker pull python:3.12-slim
//! cargo run -p examples --bin workflow_docker_python
//! ```

use intelligent_subject::workflow::error::WorkflowError;
use intelligent_subject::workflow::platform::{DockerPlatform, WorkPlatform};
use intelligent_subject::workflow::model::ExecutionContext;
use intelligent_subject::workflow::workflow_manager::WorkflowManager;
use std::path::Path;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Docker Python Workflow 示例 ===\n");

    println!("[1] 创建 DockerPlatform (python:3.12-slim)...");
    let platform = DockerPlatform::create("python:3.12-slim").await?;
    println!("    容器已启动，工作目录: {:?}\n", platform.workspace_root());

    let platform: Arc<dyn WorkPlatform> = Arc::new(platform);
    let ctx = ExecutionContext { platform: platform.clone() };

    // ── 示例 A: 直接执行 Python 代码 ────────────────────────
    println!("[2] 直接执行 Python...");
    let code = r#"
import sys
print(f"Python version: {sys.version_info.major}.{sys.version_info.minor}")
print("Hello from Docker!")
x = sum(range(1, 101))
print(f"Sum of 1..100 = {x})
"#.to_string();
    let result = {
        let plat = ctx.platform.clone();
        plat.write_file(Path::new("eval_script.py"), code.as_bytes()).await?;
        let output = plat.run_command("python", &["/workspace/eval_script.py"], &[]).await?;
        output.stdout_string().unwrap_or_default().trim_end().to_string()
    };
    println!("    输出:\n{}\n", result);

    // ── 示例 B: WorkflowManager 管道 — Factorial(5) → ReverseUpper ─────────
    println!("[3] WorkflowManager 管道: Factorial(5) → ReverseUpper...");
    let mgr = WorkflowManager::new();

    // 第一个节点：接收 i32，生成计算阶乘的 Python 脚本，返回结果字符串
    mgr.add_with_ctx("factorial", |input: i32, ctx: &ExecutionContext| {
        let plat = ctx.platform.clone();
        async move {
            let code = format!(
                "import math\nprint(math.factorial({input}))\n"
            );
            plat.write_file(Path::new("factorial.py"), code.as_bytes()).await?;
            let output = plat.run_command("python", &["/workspace/factorial.py"], &[]).await?;
            let result = output.stdout_string().unwrap_or_default().trim_end().to_string();
            println!("    factorial({input}) = {result}");
            Ok::<String, WorkflowError>(result)
        }
    })?;

    // 第二个节点：接收上一个节点的字符串输出，将其反转并大写
    mgr.add_with_ctx("reverse_upper", |input: String, ctx: &ExecutionContext| {
        let plat = ctx.platform.clone();
        async move {
            let code = format!(
                "s = {input:?}\nprint(s[::-1].upper())\n"
            );
            plat.write_file(Path::new("reverse.py"), code.as_bytes()).await?;
            let output = plat.run_command("python", &["/workspace/reverse.py"], &[]).await?;
            let result = output.stdout_string().unwrap_or_default().trim_end().to_string();
            println!("    reverse_upper(\"{input}\") = \"{result}\"");
            Ok::<String, WorkflowError>(result)
        }
    })?;

    // 输入 5: factorial(5)=120 → reverse_upper("120")="021"
    let fact_result: String = mgr.execute_typed("factorial", 5i32, &ctx).await?;
    let final_result: String = mgr.execute_typed("reverse_upper", fact_result, &ctx).await?;
    println!("    最终结果: \"{final_result}\"");
    assert_eq!(final_result, "021");

    // ── 清理 ────────────────────────────────────────────────
    println!("\n[4] 清理 Docker 容器...");
    platform.cleanup().await?;
    println!("    已停止并删除容器。");

    println!("\n=== 全部完成 ===");
    Ok(())
}
