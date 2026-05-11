//! DockerPlatform — Docker 容器内执行。
//!
//! 在 Docker 容器中执行命令，通过 bind mount 共享工作目录。
//! 适用于需要沙箱隔离执行 Python 脚本等场景。

use std::path::Path;

use async_trait::async_trait;
use bollard::container::{
    Config, CreateContainerOptions, RemoveContainerOptions, StartContainerOptions,
};
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::Docker;
use tempfile::TempDir;

use super::api::{CommandOutput, PlatformError, WorkPlatform};

/// A work platform backed by a Docker container.
///
/// The container's `/workspace` directory is bind-mounted to a host temporary
/// directory, so files written via `write_file` are immediately visible inside
/// the container, and vice versa.
///
/// # Lifecycle
///
/// 1. [`DockerPlatform::create`] — pulls image (if needed), creates and starts the container
/// 2. `run_command` / `write_file` / `read_file` — execute inside the container
/// 3. `cleanup` — stops and removes the container, deletes the tempdir
///
/// # Example
///
/// ```rust,no_run
/// use autonomous::workflow::platform::DockerPlatform;
/// use autonomous::workflow::platform::WorkPlatform;
///
/// # #[tokio::main]
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let platform = DockerPlatform::create("python:3.12-slim").await?;
///
/// // Write a script
/// platform.write_file(
///     std::path::Path::new("hello.py"),
///     b"print('Hello from Docker!')",
/// ).await?;
///
/// // Run it
/// let output = platform.run_command(
///     "python",
///     &["/workspace/hello.py"],
///     &[],
/// ).await?;
/// println!("{}", output.stdout_string()?);
///
/// // Clean up
/// platform.cleanup().await?;
/// # Ok(())
/// # }
/// ```
pub struct DockerPlatform {
    docker: Docker,
    container_id: String,
    host_workspace: TempDir,
}

impl DockerPlatform {
    /// Create a new Docker platform.
    ///
    /// - Connects to the Docker daemon
    /// - Creates a temporary directory on the host
    /// - Creates and starts a container with the host dir bind-mounted to `/workspace`
    /// - The container runs a long-lived sleep command to stay alive for exec calls
    pub async fn create(image: &str) -> Result<Self, PlatformError> {
        let docker = Docker::connect_with_local_defaults()
            .map_err(|e| PlatformError::Docker(e.to_string()))?;

        let host_workspace = TempDir::new()
            .map_err(PlatformError::Io)?;

        let host_path = host_workspace.path().to_string_lossy().into_owned();

        let options = CreateContainerOptions {
            name: format!("autonomous_{}", std::process::id()),
            ..Default::default()
        };

        let config = Config {
            image: Some(image),
            cmd: Some(vec!["sleep", "infinity"]),
            working_dir: Some("/workspace"),
            host_config: Some(bollard::service::HostConfig {
                binds: Some(vec![format!("{host_path}:/workspace")]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let result = docker
            .create_container(Some(options), config)
            .await
            .map_err(|e| PlatformError::Docker(e.to_string()))?;

        let container_id = result.id;

        docker
            .start_container(&container_id, None::<StartContainerOptions<String>>)
            .await
            .map_err(|e| PlatformError::Docker(e.to_string()))?;

        Ok(Self {
            docker,
            container_id,
            host_workspace,
        })
    }
}

#[async_trait]
impl WorkPlatform for DockerPlatform {
    async fn run_command(
        &self,
        command: &str,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> Result<CommandOutput, PlatformError> {
        let mut full_cmd = vec![command];
        full_cmd.extend(args.iter().map(|s| *s));

        let env_owned: Vec<String> = env
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();

        let exec_config = CreateExecOptions {
            cmd: Some(full_cmd),
            env: if env_owned.is_empty() {
                None
            } else {
                Some(env_owned.iter().map(|s| s.as_str()).collect())
            },
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            working_dir: Some("/workspace"),
            ..Default::default()
        };

        let exec_result = self
            .docker
            .create_exec(&self.container_id, exec_config)
            .await
            .map_err(|e| PlatformError::Docker(e.to_string()))?;

        let start_result = self
            .docker
            .start_exec(&exec_result.id, None)
            .await
            .map_err(|e| PlatformError::Docker(e.to_string()))?;

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        if let StartExecResults::Attached { output, .. } = start_result {
            use bollard::container::LogOutput;
            use futures_util::StreamExt;

            let mut stream = output.boxed();
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(LogOutput::StdOut { message: data }) => stdout.extend_from_slice(&data),
                    Ok(LogOutput::StdErr { message: data }) => stderr.extend_from_slice(&data),
                    Ok(_) => {}
                    Err(e) => return Err(PlatformError::Docker(e.to_string())),
                }
            }
        }

        // Get exit code.
        let exec_inspect = self
            .docker
            .inspect_exec(&exec_result.id)
            .await
            .map_err(|e| PlatformError::Docker(e.to_string()))?;

        let exit_code = exec_inspect
            .exit_code
            .unwrap_or(-1);

        if exit_code != 0 {
            return Err(PlatformError::CommandFailed {
                code: exit_code,
                stderr: String::from_utf8_lossy(&stderr).into_owned(),
            });
        }

        Ok(CommandOutput {
            stdout,
            stderr,
            exit_code,
        })
    }

    async fn write_file(
        &self,
        path: &Path,
        content: &[u8],
    ) -> Result<(), PlatformError> {
        let full_path = self.host_workspace.path().join(path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(full_path, content)?;
        Ok(())
    }

    async fn read_file(
        &self,
        path: &Path,
    ) -> Result<Vec<u8>, PlatformError> {
        let full_path = self.host_workspace.path().join(path);
        Ok(std::fs::read(full_path)?)
    }

    fn workspace_root(&self) -> &Path {
        self.host_workspace.path()
    }

    async fn cleanup(&self) -> Result<(), PlatformError> {
        self.docker
            .stop_container(&self.container_id, None)
            .await
            .map_err(|e| PlatformError::Docker(e.to_string()))?;

        self.docker
            .remove_container(
                &self.container_id,
                None::<RemoveContainerOptions>,
            )
            .await
            .map_err(|e| PlatformError::Docker(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn docker_run_python() {
        let platform = DockerPlatform::create("python:3.12-slim").await.unwrap();

        platform
            .write_file(Path::new("hello.py"), b"print('hello from docker')")
            .await
            .unwrap();

        let output = platform
            .run_command("python", &["/workspace/hello.py"], &[])
            .await
            .unwrap();

        assert_eq!(output.exit_code, 0);
        assert!(String::from_utf8_lossy(&output.stdout).contains("hello from docker"));

        platform.cleanup().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn docker_failed_command() {
        let platform = DockerPlatform::create("python:3.12-slim").await.unwrap();

        let result = platform
            .run_command("python", &["-c", "import sys; sys.exit(1)"], &[])
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            PlatformError::CommandFailed { code, .. } => assert_eq!(code, 1),
            other => panic!("expected CommandFailed, got {other:?}"),
        }

        platform.cleanup().await.unwrap();
    }
}
