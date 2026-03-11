use crate::repo_utils::find_repository_root;
use crate::stdin_utils::{merge_prompt_with_stdin, read_piped_input};
use crate::task::TaskBuilder;
use std::error::Error;
use std::path::{Path, PathBuf};

/// Resolves the deprecated `--description` flag into the `--prompt` value.
///
/// Errors if both `--prompt` and `--description` are provided.
/// Prints a deprecation warning if `--description` is used.
pub fn resolve_deprecation(
    prompt: Option<String>,
    description: Option<String>,
) -> Result<Option<String>, Box<dyn Error>> {
    match (prompt, description) {
        (Some(_), Some(_)) => {
            Err("Cannot use both --prompt and --description flags. Use --prompt/-p instead.".into())
        }
        (None, Some(desc)) => {
            eprintln!(
                "Warning: --description/-d is deprecated. Use --prompt/-p instead. This flag will be removed in a future release."
            );
            Ok(Some(desc))
        }
        (prompt, None) => Ok(prompt),
    }
}

/// Shared task creation arguments extracted from CLI commands.
///
/// Holds the common fields across Add, Run, and Shell commands and provides
/// methods for the shared pre-processing steps (agent validation, prompt
/// resolution, repo root discovery, and TaskBuilder configuration).
#[derive(Default)]
pub struct TaskArgs {
    pub name: Option<String>,
    pub r#type: String,
    pub prompt: Option<String>,
    pub prompt_file: Option<String>,
    pub edit: bool,
    pub agent: Option<String>,
    pub stack: Option<String>,
    pub project: Option<String>,
    pub repo: Option<String>,
    pub no_network_isolation: bool,
    pub dind: bool,
    pub target_branch: Option<String>,
}

impl TaskArgs {
    /// Resolves the task name, defaulting to the task type if not provided.
    pub fn resolved_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| self.r#type.clone())
    }

    /// Parses comma-separated agent string and validates each agent.
    /// Returns the default agent if none specified.
    pub fn parse_and_validate_agents(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let agents: Vec<String> = match &self.agent {
            Some(agent_str) => agent_str.split(',').map(|s| s.trim().to_string()).collect(),
            None => vec![crate::agent::AgentProvider::default_agent().to_string()],
        };

        for agent in &agents {
            if !crate::agent::AgentProvider::is_valid_agent(agent) {
                let available = crate::agent::AgentProvider::list_agents().join(", ");
                return Err(
                    format!("Unknown agent '{agent}'. Available agents: {available}").into(),
                );
            }
        }

        Ok(agents)
    }

    /// Reads piped stdin and merges with the CLI prompt.
    pub fn resolve_prompt(&self) -> Result<Option<String>, Box<dyn Error>> {
        let piped_input = read_piped_input()?;
        Ok(merge_prompt_with_stdin(self.prompt.clone(), piped_input))
    }

    /// Finds the repository root from `--repo` or current directory.
    pub fn resolve_repo_root(&self) -> Result<PathBuf, Box<dyn Error>> {
        let start_path = self.repo.as_deref().unwrap_or(".");
        find_repository_root(Path::new(start_path))
    }

    /// Creates and configures a TaskBuilder with all shared fields.
    ///
    /// Pass `Some(agent)` to set a specific agent, or `None` to let
    /// TaskBuilder resolve via project config / default fallback.
    /// Callers should add command-specific settings (e.g., `parent_id`,
    /// `with_interactive`) before calling `.build()`.
    pub fn configure_builder(
        &self,
        repo_root: PathBuf,
        name: String,
        agent: Option<String>,
        prompt: Option<String>,
    ) -> TaskBuilder {
        TaskBuilder::new()
            .repo_root(repo_root)
            .name(name)
            .task_type(self.r#type.clone())
            .prompt(prompt)
            .prompt_file(self.prompt_file.as_ref().map(PathBuf::from))
            .edit(self.edit)
            .agent(agent)
            .stack(self.stack.clone())
            .project(self.project.clone())
            .network_isolation(!self.no_network_isolation)
            .dind(if self.dind { Some(true) } else { None })
            .target_branch(self.target_branch.clone())
    }
}
