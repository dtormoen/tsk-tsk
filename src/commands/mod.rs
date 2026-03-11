use crate::context::AppContext;
use async_trait::async_trait;
use std::error::Error;

pub mod add;
pub mod cancel;
pub mod clean;
pub mod delete;
pub mod docker;
pub mod list;
pub mod retry;
pub mod review;
pub mod run;
pub mod server;
pub mod shell;
pub mod task_args;
pub mod template;

pub use add::AddCommand;
pub use cancel::CancelCommand;
pub use clean::CleanCommand;
pub use delete::DeleteCommand;
pub use list::ListCommand;
pub use retry::RetryCommand;
pub use review::ReviewCommand;
pub use run::RunCommand;
pub use shell::ShellCommand;

#[async_trait]
pub trait Command: Send + Sync {
    async fn execute(&self, ctx: &AppContext) -> Result<(), Box<dyn Error>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_trait_is_object_safe() {
        // This test ensures that the Command trait can be used as a trait object
        fn _assert_object_safe(_: &dyn Command) {}
    }

    #[test]
    fn test_docker_build_command_instantiation() {
        // Test that DockerBuildCommand can be instantiated
        let _cmd = docker::DockerBuildCommand {
            no_cache: false,
            stack: None,
            agent: None,
            project: None,
            dry_run: false,
            proxy_only: false,
        };
    }
}
