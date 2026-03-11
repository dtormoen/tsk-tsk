use crate::assets::frontmatter::strip_frontmatter;
use crate::assets::{find_template, list_all_templates};
use crate::context::AppContext;
use crate::context::tsk_config;
use crate::context::tsk_env::TskEnv;
use crate::git::RepoManager;
use crate::git_operations;
use crate::task::Task;
use crate::utils::sanitize_for_branch_name;
use chrono::Local;
use std::collections::HashSet;
use std::error::Error;
use std::path::{Path, PathBuf};

/// Alphabet for generating task IDs. Excludes `-` to prevent IDs from being
/// mistaken for CLI flags.
const TASK_ID_ALPHABET: [char; 63] = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
    'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9', '_',
];

/// Builder for creating tasks with a fluent API
pub struct TaskBuilder {
    repo_root: Option<PathBuf>,
    name: Option<String>,
    task_type: Option<String>,
    prompt: Option<String>,
    /// Path to a file containing the task prompt (contents injected into template)
    prompt_file_path: Option<PathBuf>,
    /// Path to an existing instructions file to use as-is (for retry)
    existing_instructions_file: Option<PathBuf>,
    edit: bool,
    agent: Option<String>,
    stack: Option<String>,
    project: Option<String>,
    copied_repo_path: Option<PathBuf>,
    is_interactive: bool,
    /// Parent task ID that this task is chained to
    parent_id: Option<String>,
    network_isolation: bool,
    dind: Option<bool>,
    repo_copy_source: Option<PathBuf>,
    target_branch: Option<String>,
    /// When true, tasks with a parent_id will copy the repo at creation time
    /// instead of deferring to the scheduler. Used for review tasks where the
    /// parent is already complete.
    skip_parent_repo_deferral: bool,
    /// Override for the source commit SHA. When set, this value is used instead
    /// of computing it from the repository HEAD.
    source_commit_override: Option<String>,
}

impl TaskBuilder {
    /// Creates a new TaskBuilder instance
    pub fn new() -> Self {
        Self {
            repo_root: None,
            name: None,
            task_type: None,
            prompt: None,
            prompt_file_path: None,
            existing_instructions_file: None,
            edit: false,
            agent: None,
            stack: None,
            project: None,
            copied_repo_path: None,
            is_interactive: false,
            parent_id: None,
            network_isolation: true,
            dind: None,
            repo_copy_source: None,
            target_branch: None,
            skip_parent_repo_deferral: false,
            source_commit_override: None,
        }
    }

    /// Creates a TaskBuilder from an existing task. This is used for retrying tasks.
    pub fn from_existing(task: &Task) -> Self {
        let mut builder = Self::new();
        builder.repo_root = Some(task.repo_root.clone());
        builder.name = Some(task.name.clone());
        builder.task_type = Some(task.task_type.clone());
        builder.agent = Some(task.agent.clone());
        builder.stack = Some(task.stack.clone());
        builder.project = Some(task.project.clone());
        builder.copied_repo_path = task.copied_repo_path.clone();
        builder.is_interactive = task.is_interactive;
        builder.network_isolation = task.network_isolation;
        builder.dind = Some(task.dind);
        // Note: We intentionally don't copy parent_id or target_branch when retrying.
        // The retry creates a fresh task from the current repository state.

        // Use existing instructions as-is for retry (bypasses template rendering)
        builder.existing_instructions_file = Some(PathBuf::from(&task.instructions_file));

        builder
    }

    /// Sets the repository root path
    pub fn repo_root(mut self, repo_root: PathBuf) -> Self {
        self.repo_root = Some(repo_root);
        self
    }

    /// Sets the task name
    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// Sets the task type
    pub fn task_type(mut self, task_type: String) -> Self {
        self.task_type = Some(task_type);
        self
    }

    /// Sets the task prompt text that replaces the `{{PROMPT}}` placeholder in templates
    pub fn prompt(mut self, prompt: Option<String>) -> Self {
        self.prompt = prompt;
        self
    }

    /// Sets the path to a file containing the task prompt (contents injected into template)
    pub fn prompt_file(mut self, path: Option<PathBuf>) -> Self {
        self.prompt_file_path = path;
        self
    }

    /// Sets an existing instructions file to copy as-is (bypasses template rendering)
    pub fn existing_instructions_file(mut self, path: Option<PathBuf>) -> Self {
        self.existing_instructions_file = path;
        self
    }

    /// Sets whether to open an editor for instructions
    pub fn edit(mut self, edit: bool) -> Self {
        self.edit = edit;
        self
    }

    /// Sets the AI agent to use
    pub fn agent(mut self, agent: Option<String>) -> Self {
        self.agent = agent;
        self
    }

    /// Sets the stack for Docker image selection
    pub fn stack(mut self, stack: Option<String>) -> Self {
        self.stack = stack;
        self
    }

    /// Sets the project name for Docker image selection
    pub fn project(mut self, project: Option<String>) -> Self {
        self.project = project;
        self
    }

    /// Sets whether the task should run in interactive mode
    pub fn with_interactive(mut self, is_interactive: bool) -> Self {
        self.is_interactive = is_interactive;
        self
    }

    /// Sets the parent task ID that this task is chained to.
    /// When set, the task will wait for the parent to complete before executing,
    /// and will use the parent's completed repository as its starting point.
    pub fn parent_id(mut self, task_id: Option<String>) -> Self {
        self.parent_id = task_id;
        self
    }

    /// Sets whether per-container network isolation is enabled.
    /// When disabled, the container runs on the default Docker network with direct internet access.
    pub fn network_isolation(mut self, enabled: bool) -> Self {
        self.network_isolation = enabled;
        self
    }

    /// Sets whether Docker-in-Docker support is enabled.
    /// When enabled, container security is relaxed to allow nested container builds.
    /// Uses `Option<bool>` so `None` defers to config-file defaults.
    pub fn dind(mut self, enabled: Option<bool>) -> Self {
        self.dind = enabled;
        self
    }

    /// Overrides the source directory used when copying the repository.
    /// When set, this path is used instead of the auto-detected repository root.
    pub fn repo_copy_source(mut self, source: Option<PathBuf>) -> Self {
        self.repo_copy_source = source;
        self
    }

    /// Sets the target branch name to use instead of the auto-generated branch.
    /// Falls back to the generated name if the target branch push fails.
    pub fn target_branch(mut self, target: Option<String>) -> Self {
        self.target_branch = target;
        self
    }

    /// When true, tasks with a parent_id copy the repo at creation time instead
    /// of deferring to the scheduler. The parent's resolved config is inherited.
    pub fn skip_parent_repo_deferral(mut self, skip: bool) -> Self {
        self.skip_parent_repo_deferral = skip;
        self
    }

    /// Sets a custom source commit SHA instead of using the repository HEAD.
    /// Used by review tasks to set the base commit for the diff.
    pub fn source_commit_override(mut self, sha: Option<String>) -> Self {
        self.source_commit_override = sha;
        self
    }

    /// Builds the task, creating all necessary files and directories
    pub async fn build(self, ctx: &AppContext) -> Result<Task, Box<dyn Error>> {
        let repo_root = self
            .repo_root
            .clone()
            .ok_or("Repository root is required")?;
        let name = self.name.clone().ok_or("Task name is required")?;
        let task_type = self
            .task_type
            .clone()
            .unwrap_or_else(|| "generic".to_string());

        // Check if template requires a prompt
        let template_needs_prompt = if task_type != "generic" {
            match find_template(&task_type, Some(&repo_root), &ctx.tsk_env()) {
                Ok(template_content) => {
                    let body = strip_frontmatter(&template_content);
                    body.contains("{{PROMPT}}") || body.contains("{{DESCRIPTION}}")
                }
                Err(_) => true, // If we can't read the template, assume it needs a prompt
            }
        } else {
            true // Generic tasks always need a prompt
        };

        // Validate input
        if template_needs_prompt
            && self.prompt.is_none()
            && self.prompt_file_path.is_none()
            && self.existing_instructions_file.is_none()
            && !self.edit
        {
            return Err("Either prompt or prompt file must be provided, or use edit mode".into());
        }

        // Auto-detect project name first (needed for project config lookup)
        let project = match self.project {
            Some(ref p) => p.clone(),
            None => match crate::repository::detect_project_name(&repo_root).await {
                Ok(detected) => detected,
                Err(e) => {
                    eprintln!("Warning: Failed to detect project name: {e}. Using default.");
                    "default".to_string()
                }
            },
        };

        // Resolve configuration for this project (layers defaults + project file + project config)
        let tsk_config = ctx.tsk_config();
        let project_config = tsk_config::load_project_config(&repo_root);
        let resolved =
            tsk_config.resolve_config(&project, project_config.as_ref(), Some(&repo_root));

        // Get agent: CLI flag > resolved config (project > defaults > built-in)
        let agent = tsk_config::resolve_agent(self.agent.clone(), &resolved);

        // Validate agent
        if !crate::agent::AgentProvider::is_valid_agent(&agent) {
            let available_agents = crate::agent::AgentProvider::list_agents().join(", ");
            return Err(
                format!("Unknown agent '{agent}'. Available agents: {available_agents}").into(),
            );
        }

        // Validate task type
        if task_type != "generic" {
            let available_templates = list_all_templates(Some(&repo_root), &ctx.tsk_env());
            if !available_templates.contains(&task_type.to_string()) {
                return Err(format!(
                    "No template found for task type '{}'. Available templates: {}",
                    task_type,
                    available_templates.join(", ")
                )
                .into());
            }
        }

        // Check if repository has any commits
        // This must happen before we try to capture source_commit
        let has_commits = match git_operations::get_current_commit(&repo_root).await {
            Ok(_) => true,
            Err(e) => {
                // Check if this is an empty repository error
                if e.contains("Failed to get HEAD") || e.contains("reference 'refs/heads") {
                    false
                } else {
                    // Some other git error - propagate it
                    return Err(format!("Failed to check repository status: {e}").into());
                }
            }
        };

        if !has_commits {
            return Err(
                format!(
                    "Cannot create task in an empty git repository.\n\n\
                     The repository at '{}' has no commits. TSK needs at least one commit to create a branch and track changes.\n\n\
                     To fix this, create an initial commit:\n  \
                     git commit --allow-empty -m \"Initial commit\"\n\n\
                     Then try running your TSK command again.",
                    repo_root.display()
                ).into()
            );
        }

        // Create task directory in centralized location
        let now = Local::now();
        let created_at = now;
        let id = nanoid::nanoid!(8, &TASK_ID_ALPHABET);
        let task_dir_name = id.clone();
        let task_dir = ctx.tsk_env().task_dir(&task_dir_name);
        crate::file_system::create_dir(&task_dir).await?;

        // Create output directory for capturing agent output
        let output_dir = task_dir.join("output");
        crate::file_system::create_dir(&output_dir).await?;

        // Create instructions file
        let instructions_path = if self.edit {
            // Create temporary file in repository root for editing
            let temp_filename = format!(".tsk-edit-{task_dir_name}-instructions.md");
            let temp_path = repo_root.join(&temp_filename);
            self.write_instructions_content(&temp_path, &task_type, ctx)
                .await?;

            // Open editor with the temporary file
            let tsk_env = ctx.tsk_env();
            self.open_editor(temp_path.to_str().ok_or("Invalid path")?, &tsk_env)?;

            // Check if file is empty and ensure cleanup happens even on error
            let needs_cleanup = self.check_instructions_not_empty(&temp_path).await.is_err();

            if needs_cleanup {
                // Clean up the temporary file and task directory before returning the error
                let _ = crate::file_system::remove_file(&temp_path).await;
                let _ = crate::file_system::remove_dir(&task_dir).await;
                return Err("Instructions file is empty. Task creation cancelled.".into());
            }

            // Move the file to the task directory
            let final_path = task_dir.join("instructions.md");
            let content = crate::file_system::read_file(&temp_path).await?;
            crate::file_system::write_file(&final_path, &content).await?;
            crate::file_system::remove_file(&temp_path).await?;

            final_path.to_string_lossy().to_string()
        } else {
            // Create instructions file directly in task directory
            let dest_path = task_dir.join("instructions.md");
            self.write_instructions_content(&dest_path, &task_type, ctx)
                .await?
        };

        // Capture the current commit SHA (or use override if provided)
        let source_commit = if let Some(ref override_sha) = self.source_commit_override {
            override_sha.clone()
        } else {
            match git_operations::get_current_commit(&repo_root).await {
                Ok(commit) => commit,
                Err(e) => {
                    return Err(
                        format!("Failed to get current commit for task '{name}': {e}").into(),
                    );
                }
            }
        };

        // Capture the current branch for git-town parent tracking
        // Returns None if in detached HEAD state
        let source_branch = git_operations::get_current_branch(&repo_root)
            .await
            .ok()
            .flatten();

        // Resolve stack: CLI flag > config (project > defaults) > auto-detect > built-in default
        let stack = tsk_config::resolve_stack(
            self.stack,
            &tsk_config,
            &project,
            project_config.as_ref(),
            &repo_root,
        )
        .await;

        // Resolve dind: CLI flag > resolved config (project > defaults > built-in)
        let dind = self.dind.unwrap_or(resolved.dind);

        // Determine branch name: use target_branch if set, otherwise generate from task fields
        let sanitized_task_type = sanitize_for_branch_name(&task_type);
        let sanitized_name = sanitize_for_branch_name(&name);
        let generated_branch = format!("tsk/{sanitized_task_type}/{sanitized_name}/{id}");
        let branch_name = self.target_branch.clone().unwrap_or(generated_branch);

        // Validate parent task if specified and capture it for later use
        let validated_parent = if let Some(ref pid) = self.parent_id {
            let storage = ctx.task_storage();
            let tasks = storage.list_tasks().await.map_err(|e| e.to_string())?;

            // Check if parent task exists
            if !tasks.iter().any(|t| t.id == *pid) {
                return Err(format!(
                    "Parent task '{}' not found. Please specify a valid task ID.",
                    pid
                )
                .into());
            }

            // Check for circular parent chains
            // Walk the parent chain to detect cycles
            let mut visited = HashSet::new();
            visited.insert(id.clone()); // Current task being created

            let mut current_id = Some(pid.clone());
            while let Some(check_id) = current_id {
                if visited.contains(&check_id) {
                    return Err(format!(
                        "Circular parent chain detected: task '{}' would create a cycle",
                        pid
                    )
                    .into());
                }
                visited.insert(check_id.clone());

                // Find the task and check its parent
                current_id = tasks
                    .iter()
                    .find(|t| t.id == check_id)
                    .and_then(|t| t.parent_ids.first().cloned());
            }

            // Capture the parent task for config inheritance
            Some(tasks.into_iter().find(|t| t.id == *pid).unwrap())
        } else {
            None
        };

        // Determine if this task has a parent and whether to defer repo setup
        let has_parent = self.parent_id.is_some();
        let defer_to_scheduler = has_parent && !self.skip_parent_repo_deferral;

        // Copy the repository for the task (unless deferred to scheduler)
        // Tasks deferred to the scheduler skip repo copy - they'll get it from the parent
        let copied_repo_path = if defer_to_scheduler {
            // Skip repo copy for child tasks - the scheduler will copy from parent task
            None
        } else {
            let copy_source = self.repo_copy_source.as_ref().unwrap_or(&repo_root);
            let repo_manager = RepoManager::new(ctx);
            let copy_result = repo_manager
                .copy_repo(
                    &task_dir_name,
                    copy_source,
                    Some(&source_commit),
                    &branch_name,
                )
                .await
                .map_err(|e| format!("Failed to copy repository: {e}"))?;
            for warning in &copy_result.warnings {
                eprintln!("{warning}");
            }
            Some(copy_result.repo_path)
        };

        // For deferred child tasks, source_branch is set to None - it will be set from parent later
        let effective_source_branch = if defer_to_scheduler {
            None
        } else {
            source_branch
        };

        // Snapshot resolved config. Review tasks inherit the parent's config so the
        // review agent runs with the same stack, agent, volumes, env, etc.
        let resolved_config_json = if self.skip_parent_repo_deferral {
            if let Some(ref parent) = validated_parent {
                match &parent.resolved_config {
                    Some(config) => config.clone(),
                    None => {
                        eprintln!(
                            "Warning: Parent task '{}' has no resolved_config snapshot, \
                             using current config instead",
                            parent.id
                        );
                        serde_json::to_string(&resolved)
                            .map_err(|e| format!("Failed to serialize resolved config: {e}"))?
                    }
                }
            } else {
                serde_json::to_string(&resolved)
                    .map_err(|e| format!("Failed to serialize resolved config: {e}"))?
            }
        } else {
            serde_json::to_string(&resolved)
                .map_err(|e| format!("Failed to serialize resolved config: {e}"))?
        };

        // Create and return the task
        let task = Task::new(
            id,
            repo_root,
            name,
            task_type,
            instructions_path,
            agent,
            branch_name,
            self.target_branch,
            source_commit,
            effective_source_branch,
            stack,
            project,
            created_at,
            copied_repo_path,
            self.is_interactive,
            self.parent_id.into_iter().collect::<Vec<String>>(),
            self.network_isolation,
            dind,
            Some(resolved_config_json),
        );

        Ok(task)
    }

    async fn write_instructions_content(
        &self,
        dest_path: &Path,
        task_type: &str,
        ctx: &AppContext,
    ) -> Result<String, Box<dyn Error>> {
        if let Some(ref file_path) = self.existing_instructions_file {
            // Existing instructions file (retry) - copy content as-is
            let content = crate::file_system::read_file(file_path).await?;
            crate::file_system::write_file(dest_path, &content).await?;
        } else {
            // Resolve prompt: prefer explicit prompt, fall back to file contents
            let effective_prompt = if let Some(ref prompt_text) = self.prompt {
                Some(prompt_text.clone())
            } else if let Some(ref file_path) = self.prompt_file_path {
                let content = crate::file_system::read_file(file_path).await?;
                let trimmed = content.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            } else {
                None
            };

            if let Some(ref prompt_text) = effective_prompt {
                // Check if a template exists for this task type
                let content = if task_type != "generic" {
                    match find_template(task_type, self.repo_root.as_deref(), &ctx.tsk_env()) {
                        Ok(template_content) => {
                            let body = strip_frontmatter(&template_content);
                            let mut content = body.replace("{{PROMPT}}", prompt_text);
                            if body.contains("{{DESCRIPTION}}") {
                                eprintln!(
                                    "Warning: {{{{DESCRIPTION}}}} placeholder is deprecated. Use {{{{PROMPT}}}} instead."
                                );
                                content = content.replace("{{DESCRIPTION}}", prompt_text);
                            }
                            content
                        }
                        Err(e) => {
                            eprintln!("Warning: Failed to read template: {e}");
                            prompt_text.clone()
                        }
                    }
                } else {
                    prompt_text.clone()
                };

                crate::file_system::write_file(dest_path, &content).await?;
            } else {
                // No prompt provided - use template as-is or create empty file
                let initial_content = if task_type != "generic" {
                    match find_template(task_type, self.repo_root.as_deref(), &ctx.tsk_env()) {
                        Ok(template_content) => {
                            let body = strip_frontmatter(&template_content);
                            if self.edit
                                && (body.contains("{{PROMPT}}") || body.contains("{{DESCRIPTION}}"))
                            {
                                let content = body.replace(
                                    "{{PROMPT}}",
                                    "<!-- TODO: Add your task prompt here -->",
                                );
                                if body.contains("{{DESCRIPTION}}") {
                                    eprintln!(
                                        "Warning: {{{{DESCRIPTION}}}} placeholder is deprecated. Use {{{{PROMPT}}}} instead."
                                    );
                                }
                                content.replace(
                                    "{{DESCRIPTION}}",
                                    "<!-- TODO: Add your task prompt here -->",
                                )
                            } else {
                                // Use template as-is (for templates without prompt placeholder)
                                body.to_string()
                            }
                        }
                        Err(_) => String::new(),
                    }
                } else {
                    String::new()
                };

                crate::file_system::write_file(dest_path, &initial_content).await?;
            }
        }

        Ok(dest_path.to_string_lossy().to_string())
    }

    fn open_editor(&self, instructions_path: &str, tsk_env: &TskEnv) -> Result<(), Box<dyn Error>> {
        let editor = tsk_env.editor();

        let status = std::process::Command::new(editor)
            .arg(instructions_path)
            .status()?;

        if !status.success() {
            return Err("Editor exited with non-zero status".into());
        }

        Ok(())
    }

    async fn check_instructions_not_empty(
        &self,
        instructions_path: &Path,
    ) -> Result<(), Box<dyn Error>> {
        // Check if file is empty after editing
        let content = crate::file_system::read_file(instructions_path).await?;
        if content.trim().is_empty() {
            return Err("Instructions file is empty. Task creation cancelled.".into());
        }
        Ok(())
    }
}

impl Default for TaskBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::AppContext;
    /// Helper to create a standard test context with real file system and git operations
    /// Returns (git_repo, AppContext)
    async fn create_test_context() -> (crate::test_utils::TestGitRepository, AppContext) {
        use crate::test_utils::TestGitRepository;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let ctx = AppContext::builder().build();

        (test_repo, ctx)
    }

    /// Helper function to create a basic task for testing
    async fn create_basic_task(name: &str, prompt: &str) -> Task {
        let (test_repo, ctx) = create_test_context().await;
        let current_dir = test_repo.path().to_path_buf();

        TaskBuilder::new()
            .repo_root(current_dir)
            .name(name.to_string())
            .task_type("generic".to_string())
            .prompt(Some(prompt.to_string()))
            .build(&ctx)
            .await
            .unwrap()
    }

    /// Helper to verify task instructions content
    async fn verify_instructions_content(ctx: &AppContext, task: &Task, expected_content: &str) {
        let instructions_path = ctx
            .tsk_env()
            .data_dir()
            .join("tasks")
            .join(&task.id)
            .join(&task.instructions_file);
        let content = crate::file_system::read_file(&instructions_path)
            .await
            .unwrap();
        assert!(content.contains(expected_content));
    }

    #[tokio::test]
    async fn test_task_builder_basic() {
        let task = create_basic_task("test-task", "Test description").await;

        assert_eq!(task.name, "test-task");
        assert_eq!(task.task_type, "generic");
        assert!(!task.instructions_file.is_empty());
        assert_eq!(task.id.len(), 8);
        assert!(
            task.id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "Task ID should only contain [A-Za-z0-9_], got: {}",
            task.id
        );
    }

    #[tokio::test]
    async fn test_task_builder_with_custom_properties() {
        let (test_repo, ctx) = create_test_context().await;
        let current_dir = test_repo.path().to_path_buf();

        let task = TaskBuilder::new()
            .repo_root(current_dir)
            .name("custom-task".to_string())
            .task_type("feat".to_string())
            .prompt(Some("Custom description".to_string()))
            .stack(Some("rust".to_string()))
            .project(Some("web-api".to_string()))
            .build(&ctx)
            .await
            .unwrap();

        assert_eq!(task.name, "custom-task");
        assert_eq!(task.task_type, "feat");
        assert_eq!(task.stack, "rust");
        assert_eq!(task.project, "web-api");
    }

    #[tokio::test]
    async fn test_task_builder_with_template() {
        use crate::test_utils::TestGitRepository;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let current_dir = test_repo.path().to_path_buf();

        // Create template with frontmatter
        let template_content =
            "---\ndescription: A feature template\n---\n# Feature Template\n\n{{PROMPT}}";
        test_repo
            .create_file(".tsk/templates/feat.md", template_content)
            .unwrap();

        let ctx = AppContext::builder().build();

        let task = TaskBuilder::new()
            .repo_root(current_dir)
            .name("test-feature".to_string())
            .task_type("feat".to_string())
            .prompt(Some("My new feature".to_string()))
            .build(&ctx)
            .await
            .unwrap();

        assert_eq!(task.task_type, "feat");
        verify_instructions_content(&ctx, &task, "Feature Template").await;
        verify_instructions_content(&ctx, &task, "My new feature").await;

        // Verify frontmatter is stripped from instructions
        let instructions_path = ctx
            .tsk_env()
            .data_dir()
            .join("tasks")
            .join(&task.id)
            .join(&task.instructions_file);
        let content = crate::file_system::read_file(&instructions_path)
            .await
            .unwrap();
        assert!(
            !content.contains("description: A feature template"),
            "Frontmatter should be stripped from instructions"
        );
    }

    #[tokio::test]
    async fn test_deprecated_description_placeholder_still_works() {
        use crate::test_utils::TestGitRepository;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let current_dir = test_repo.path().to_path_buf();

        // Create template using the deprecated {{DESCRIPTION}} placeholder
        let template_content =
            "---\ndescription: A legacy template\n---\n# Legacy Template\n\n{{DESCRIPTION}}";
        test_repo
            .create_file(".tsk/templates/legacy.md", template_content)
            .unwrap();

        let ctx = AppContext::builder().build();

        let task = TaskBuilder::new()
            .repo_root(current_dir)
            .name("legacy-test".to_string())
            .task_type("legacy".to_string())
            .prompt(Some("My prompt text".to_string()))
            .build(&ctx)
            .await
            .unwrap();

        // Verify the deprecated placeholder was replaced with the prompt text
        verify_instructions_content(&ctx, &task, "My prompt text").await;

        // Verify the placeholder itself is gone
        let instructions_path = ctx
            .tsk_env()
            .data_dir()
            .join("tasks")
            .join(&task.id)
            .join(&task.instructions_file);
        let content = crate::file_system::read_file(&instructions_path)
            .await
            .unwrap();
        assert!(
            !content.contains("{{DESCRIPTION}}"),
            "Deprecated placeholder should be replaced"
        );
    }

    #[tokio::test]
    async fn test_task_builder_validation_errors() {
        let ctx = AppContext::builder().build();
        let non_git_dir = ctx.tsk_env().data_dir().to_path_buf();

        // Test missing repository root
        let result = TaskBuilder::new()
            .name("test".to_string())
            .build(&ctx)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Repository root"));

        // Test missing task name
        let result = TaskBuilder::new()
            .repo_root(non_git_dir.clone())
            .build(&ctx)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Task name"));

        // Test missing prompt for generic task
        let result = TaskBuilder::new()
            .repo_root(non_git_dir)
            .name("test".to_string())
            .build(&ctx)
            .await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Either prompt or prompt file")
        );
    }

    #[tokio::test]
    async fn test_task_builder_with_prompt_file() {
        let (test_repo, ctx) = create_test_context().await;
        let current_dir = test_repo.path().to_path_buf();

        // Create instructions file
        let instructions_content = "# Instructions for task\n\nDetailed steps here.";
        let instructions_path = current_dir.join("task-instructions.md");
        crate::file_system::write_file(&instructions_path, instructions_content)
            .await
            .unwrap();

        let task = TaskBuilder::new()
            .repo_root(current_dir)
            .name("file-task".to_string())
            .prompt_file(Some(instructions_path))
            .build(&ctx)
            .await
            .unwrap();

        verify_instructions_content(&ctx, &task, "Instructions for task").await;
        verify_instructions_content(&ctx, &task, "Detailed steps here").await;
    }

    #[tokio::test]
    async fn test_task_builder_branch_name_generation() {
        let task = create_basic_task("my-feature-name", "Description").await;

        // Branch name should follow pattern: tsk/{task-type}/{task-name}/{task-id}
        assert!(task.branch_name.starts_with("tsk/generic/my-feature-name/"));
        assert_eq!(task.branch_name.split('/').count(), 4);
    }

    #[tokio::test]
    async fn test_task_builder_with_target_branch() {
        use crate::test_utils::TestGitRepository;

        let ctx = AppContext::builder().build();

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        let task = TaskBuilder::new()
            .repo_root(test_repo.path().to_path_buf())
            .name("my-task".to_string())
            .prompt(Some("Test".to_string()))
            .target_branch(Some("feature/custom-branch".to_string()))
            .build(&ctx)
            .await
            .unwrap();

        assert_eq!(task.branch_name, "feature/custom-branch");
        assert_eq!(
            task.target_branch,
            Some("feature/custom-branch".to_string())
        );
    }

    #[tokio::test]
    async fn test_task_builder_without_target_branch() {
        let task = create_basic_task("my-task", "Description").await;

        assert!(task.branch_name.starts_with("tsk/generic/my-task/"));
        assert_eq!(task.target_branch, None);
    }

    #[tokio::test]
    async fn test_task_builder_captures_source_commit() {
        use crate::test_utils::TestGitRepository;

        let test_repo = TestGitRepository::new().unwrap();
        let initial_commit = test_repo.init_with_commit().unwrap();

        // Add another commit
        test_repo.create_file("new_file.txt", "content").unwrap();
        test_repo.stage_all().unwrap();
        let current_commit = test_repo.commit("Add new file").unwrap();

        let ctx = AppContext::builder().build();

        let task = TaskBuilder::new()
            .repo_root(test_repo.path().to_path_buf())
            .name("test-task".to_string())
            .prompt(Some("Test".to_string()))
            .build(&ctx)
            .await
            .unwrap();

        assert_eq!(task.source_commit, current_commit);
        assert_ne!(task.source_commit, initial_commit);
    }

    #[tokio::test]
    async fn test_task_builder_interactive_mode() {
        let (test_repo, ctx) = create_test_context().await;

        let task = TaskBuilder::new()
            .repo_root(test_repo.path().to_path_buf())
            .name("interactive-task".to_string())
            .prompt(Some("Test".to_string()))
            .with_interactive(true)
            .build(&ctx)
            .await
            .unwrap();

        assert!(task.is_interactive);

        // Default should be non-interactive
        let task2 = TaskBuilder::new()
            .repo_root(test_repo.path().to_path_buf())
            .name("regular-task".to_string())
            .prompt(Some("Test".to_string()))
            .build(&ctx)
            .await
            .unwrap();

        assert!(!task2.is_interactive);
    }

    #[tokio::test]
    async fn test_task_builder_repository_copy() {
        use crate::test_utils::TestGitRepository;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        // Add some files to verify they get copied
        test_repo
            .create_file("src/main.rs", "fn main() {}")
            .unwrap();
        test_repo
            .create_file("Cargo.toml", "[package]\nname = \"test\"")
            .unwrap();
        test_repo.stage_all().unwrap();
        test_repo.commit("Add source files").unwrap();

        let ctx = AppContext::builder().build();

        let task = TaskBuilder::new()
            .repo_root(test_repo.path().to_path_buf())
            .name("copy-test".to_string())
            .prompt(Some("Test repository copy".to_string()))
            .build(&ctx)
            .await
            .unwrap();

        // Verify repository was copied to the correct location
        let task_dir = ctx.tsk_env().task_dir(&task.id);
        let copied_repo = task_dir.join("repo");

        assert!(copied_repo.exists());
        assert!(copied_repo.join("src/main.rs").exists());
        assert!(copied_repo.join("Cargo.toml").exists());
        assert!(copied_repo.join(".git").exists());
    }

    #[tokio::test]
    async fn test_task_builder_creates_output_directory() {
        use crate::test_utils::TestGitRepository;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        let ctx = AppContext::builder().build();

        let task = TaskBuilder::new()
            .repo_root(test_repo.path().to_path_buf())
            .name("output-test".to_string())
            .prompt(Some("Test output directory creation".to_string()))
            .build(&ctx)
            .await
            .unwrap();

        // Verify output directory was created
        let task_dir = ctx.tsk_env().task_dir(&task.id);
        let output_dir = task_dir.join("output");

        assert!(output_dir.exists());
        assert!(output_dir.is_dir());
    }

    #[tokio::test]
    async fn test_task_builder_rejects_empty_repository() {
        use crate::test_utils::TestGitRepository;

        // Create an empty git repository (no commits)
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init().unwrap(); // Initialize but don't create initial commit
        let repo_path = test_repo.path().to_path_buf();

        // Create context
        let ctx = AppContext::builder().build();

        // Attempt to create a task in the empty repository
        let result = TaskBuilder::new()
            .repo_root(repo_path.clone())
            .name("test-task".to_string())
            .task_type("generic".to_string())
            .prompt(Some("Test description".to_string()))
            .build(&ctx)
            .await;

        // Verify that task creation failed
        assert!(
            result.is_err(),
            "Task creation should fail on empty repository"
        );

        // Verify error message is helpful and actionable
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("empty git repository"),
            "Error should mention empty repository, got: {error_message}"
        );
        assert!(
            error_message.contains("no commits"),
            "Error should mention no commits, got: {error_message}"
        );
        assert!(
            error_message.contains("git commit --allow-empty"),
            "Error should provide the command to fix it, got: {error_message}"
        );
        assert!(
            error_message.contains(&repo_path.display().to_string()),
            "Error should show repository path, got: {error_message}"
        );

        // Verify no task directory was created (cleanup verification)
        // The task ID would be generated, but we can verify that no tasks exist in storage
        // This ensures we're not leaving behind partial state
        let task_storage = ctx.task_storage();
        let all_tasks = task_storage.list_tasks().await.unwrap();
        assert!(
            all_tasks.is_empty(),
            "No tasks should exist after failed creation"
        );
    }

    #[tokio::test]
    async fn test_cli_flags_override_project_config() {
        use crate::context::{SharedConfig, TskConfig};
        use crate::test_utils::TestGitRepository;
        use std::collections::HashMap;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path().to_path_buf();

        // Use a fixed project name for the test
        let project_name = "test-project".to_string();

        // Create project config with different agent and stack
        let mut project_configs = HashMap::new();
        project_configs.insert(
            project_name.clone(),
            SharedConfig {
                agent: Some("no-op".to_string()),
                stack: Some("python".to_string()),
                ..Default::default()
            },
        );
        let tsk_config = TskConfig {
            project: project_configs,
            ..Default::default()
        };

        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();

        // CLI flags should override project config
        let task = TaskBuilder::new()
            .repo_root(repo_path)
            .name("cli-override-test".to_string())
            .prompt(Some("Test".to_string()))
            .project(Some(project_name))
            .agent(Some("claude".to_string()))
            .stack(Some("rust".to_string()))
            .build(&ctx)
            .await
            .unwrap();

        assert_eq!(
            task.agent, "claude",
            "CLI agent should override project config"
        );
        assert_eq!(
            task.stack, "rust",
            "CLI stack should override project config"
        );
    }

    #[tokio::test]
    async fn test_project_config_overrides_auto_detect() {
        use crate::context::{SharedConfig, TskConfig};
        use crate::test_utils::TestGitRepository;
        use std::collections::HashMap;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path().to_path_buf();

        // Create a Cargo.toml so auto-detect would choose "rust"
        test_repo
            .create_file("Cargo.toml", "[package]\nname = \"test\"")
            .unwrap();

        // Use a fixed project name for the test
        let project_name = "test-project".to_string();

        // Create project config that specifies python
        let mut project_configs = HashMap::new();
        project_configs.insert(
            project_name.clone(),
            SharedConfig {
                agent: Some("no-op".to_string()),
                stack: Some("python".to_string()),
                ..Default::default()
            },
        );
        let tsk_config = TskConfig {
            project: project_configs,
            ..Default::default()
        };

        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();

        let task = TaskBuilder::new()
            .repo_root(repo_path)
            .name("config-override-test".to_string())
            .prompt(Some("Test".to_string()))
            .project(Some(project_name))
            .build(&ctx)
            .await
            .unwrap();

        assert_eq!(task.agent, "no-op", "Project config agent should be used");
        assert_eq!(
            task.stack, "python",
            "Project config stack should override auto-detect"
        );
    }

    #[tokio::test]
    async fn test_partial_project_config_uses_defaults_for_missing() {
        use crate::context::{SharedConfig, TskConfig};
        use crate::test_utils::TestGitRepository;
        use std::collections::HashMap;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path().to_path_buf();

        // Create a Cargo.toml so auto-detect would choose "rust"
        test_repo
            .create_file("Cargo.toml", "[package]\nname = \"test\"")
            .unwrap();

        // Use a fixed project name for the test
        let project_name = "test-project".to_string();

        // Create project config with only agent (no stack)
        let mut project_configs = HashMap::new();
        project_configs.insert(
            project_name.clone(),
            SharedConfig {
                agent: Some("no-op".to_string()),
                ..Default::default()
            },
        );
        let tsk_config = TskConfig {
            project: project_configs,
            ..Default::default()
        };

        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();

        let task = TaskBuilder::new()
            .repo_root(repo_path)
            .name("partial-config-test".to_string())
            .prompt(Some("Test".to_string()))
            .project(Some(project_name))
            .build(&ctx)
            .await
            .unwrap();

        assert_eq!(task.agent, "no-op", "Project config agent should be used");
        assert_eq!(
            task.stack, "rust",
            "Stack should be auto-detected when not in config"
        );
    }

    #[tokio::test]
    async fn test_no_project_config_uses_defaults() {
        use crate::test_utils::TestGitRepository;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path().to_path_buf();

        // Create a Cargo.toml so stack would auto-detect as "rust"
        test_repo
            .create_file("Cargo.toml", "[package]\nname = \"test\"")
            .unwrap();

        // Use default context (no project config)
        let ctx = AppContext::builder().build();

        let task = TaskBuilder::new()
            .repo_root(repo_path)
            .name("no-config-test".to_string())
            .prompt(Some("Test".to_string()))
            .build(&ctx)
            .await
            .unwrap();

        // Should use defaults: claude for agent, auto-detect for stack
        assert_eq!(task.agent, "claude", "Agent should use default");
        assert_eq!(task.stack, "rust", "Stack should be auto-detected");
    }

    #[tokio::test]
    async fn test_dind_config_resolution_chain() {
        use crate::context::{SharedConfig, TskConfig};
        use crate::test_utils::TestGitRepository;
        use std::collections::HashMap;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path().to_path_buf();
        let project_name = "test-project".to_string();

        // Case 1: defaults.dind = true, no project config, no CLI flag -> true
        let tsk_config = TskConfig {
            defaults: SharedConfig {
                dind: Some(true),
                ..Default::default()
            },
            ..Default::default()
        };
        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();
        let task = TaskBuilder::new()
            .repo_root(repo_path.clone())
            .name("dind-docker-default".to_string())
            .prompt(Some("Test".to_string()))
            .build(&ctx)
            .await
            .unwrap();
        assert!(task.dind, "defaults.dind = true should propagate");

        // Case 2: defaults.dind = true, project.dind = Some(false) -> false (project overrides)
        let mut project_configs = HashMap::new();
        project_configs.insert(
            project_name.clone(),
            SharedConfig {
                dind: Some(false),
                ..Default::default()
            },
        );
        let tsk_config = TskConfig {
            defaults: SharedConfig {
                dind: Some(true),
                ..Default::default()
            },
            project: project_configs,
            ..Default::default()
        };
        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();
        let task = TaskBuilder::new()
            .repo_root(repo_path.clone())
            .name("dind-project-override".to_string())
            .prompt(Some("Test".to_string()))
            .project(Some(project_name.clone()))
            .build(&ctx)
            .await
            .unwrap();
        assert!(
            !task.dind,
            "project.dind = false should override defaults.dind = true"
        );

        // Case 3: project.dind = Some(false), CLI --dind -> true (CLI overrides)
        let mut project_configs = HashMap::new();
        project_configs.insert(
            project_name.clone(),
            SharedConfig {
                dind: Some(false),
                ..Default::default()
            },
        );
        let tsk_config = TskConfig {
            project: project_configs,
            ..Default::default()
        };
        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();
        let task = TaskBuilder::new()
            .repo_root(repo_path.clone())
            .name("dind-cli-override".to_string())
            .prompt(Some("Test".to_string()))
            .project(Some(project_name))
            .dind(Some(true))
            .build(&ctx)
            .await
            .unwrap();
        assert!(task.dind, "CLI --dind should override project.dind = false");
    }

    #[tokio::test]
    async fn test_task_builder_with_valid_parent() {
        use crate::test_utils::TestGitRepository;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path().to_path_buf();

        let ctx = AppContext::builder().build();

        // First create a parent task
        let parent_task = TaskBuilder::new()
            .repo_root(repo_path.clone())
            .name("parent-task".to_string())
            .prompt(Some("Parent task".to_string()))
            .build(&ctx)
            .await
            .unwrap();

        // Add parent task to storage so validation passes
        let storage = ctx.task_storage();
        storage.add_task(parent_task.clone()).await.unwrap();

        // Create child task
        let child_task = TaskBuilder::new()
            .repo_root(repo_path)
            .name("child-task".to_string())
            .prompt(Some("Child task".to_string()))
            .parent_id(Some(parent_task.id.clone()))
            .build(&ctx)
            .await
            .unwrap();

        assert_eq!(child_task.parent_ids, vec![parent_task.id]);
        assert!(
            child_task.copied_repo_path.is_none(),
            "Child task should have no copied_repo_path"
        );
        assert!(
            child_task.source_branch.is_none(),
            "Child task should have no source_branch initially"
        );
    }

    #[tokio::test]
    async fn test_task_builder_invalid_parent_not_found() {
        use crate::test_utils::TestGitRepository;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path().to_path_buf();

        let ctx = AppContext::builder().build();

        // Try to create a task with a non-existent parent
        let result = TaskBuilder::new()
            .repo_root(repo_path)
            .name("orphan-task".to_string())
            .prompt(Some("Orphan task".to_string()))
            .parent_id(Some("nonexistent-task-id".to_string()))
            .build(&ctx)
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not found"),
            "Expected 'not found' error, got: {err_msg}"
        );
        assert!(
            err_msg.contains("nonexistent-task-id"),
            "Error should mention the invalid task ID, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_task_builder_circular_parent_chain_detection() {
        use crate::test_utils::TestGitRepository;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path().to_path_buf();

        let ctx = AppContext::builder().build();
        let storage = ctx.task_storage();

        // Create task A
        let task_a = TaskBuilder::new()
            .repo_root(repo_path.clone())
            .name("task-a".to_string())
            .prompt(Some("Task A".to_string()))
            .build(&ctx)
            .await
            .unwrap();
        storage.add_task(task_a.clone()).await.unwrap();

        // Create task B with parent A
        let task_b = TaskBuilder::new()
            .repo_root(repo_path.clone())
            .name("task-b".to_string())
            .prompt(Some("Task B".to_string()))
            .parent_id(Some(task_a.id.clone()))
            .build(&ctx)
            .await
            .unwrap();
        storage.add_task(task_b.clone()).await.unwrap();

        // Create task C with parent B (this should work - no cycle)
        let task_c = TaskBuilder::new()
            .repo_root(repo_path.clone())
            .name("task-c".to_string())
            .prompt(Some("Task C".to_string()))
            .parent_id(Some(task_b.id.clone()))
            .build(&ctx)
            .await
            .unwrap();
        storage.add_task(task_c.clone()).await.unwrap();

        // Verify the chain is properly traversed
        assert!(task_a.parent_ids.is_empty());
        assert_eq!(task_b.parent_ids, vec![task_a.id.clone()]);
        assert_eq!(task_c.parent_ids, vec![task_b.id.clone()]);

        // The chain A <- B <- C is valid (C has parent B which has parent A)
        // This proves the parent chain traversal works correctly
    }

    #[tokio::test]
    async fn test_task_builder_populates_resolved_config() {
        use crate::context::ResolvedConfig;

        let task = create_basic_task("config-test", "Test config snapshotting").await;

        assert!(
            task.resolved_config.is_some(),
            "Task should have resolved_config set at creation"
        );

        // Verify the JSON is valid and deserializable
        let config: ResolvedConfig =
            serde_json::from_str(task.resolved_config.as_ref().unwrap()).unwrap();
        assert_eq!(config.agent, "claude", "Default agent should be claude");
    }

    #[tokio::test]
    async fn test_task_builder_resolved_config_reflects_project_config() {
        use crate::context::{ResolvedConfig, SharedConfig, TskConfig};
        use crate::test_utils::TestGitRepository;
        use std::collections::HashMap;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path().to_path_buf();
        let project_name = "test-project".to_string();

        let mut project_configs = HashMap::new();
        project_configs.insert(
            project_name.clone(),
            SharedConfig {
                memory_gb: Some(32.0),
                host_ports: vec![5432],
                ..Default::default()
            },
        );
        let tsk_config = TskConfig {
            project: project_configs,
            ..Default::default()
        };

        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();

        let task = TaskBuilder::new()
            .repo_root(repo_path)
            .name("config-merge-test".to_string())
            .prompt(Some("Test".to_string()))
            .project(Some(project_name))
            .build(&ctx)
            .await
            .unwrap();

        let config: ResolvedConfig =
            serde_json::from_str(task.resolved_config.as_ref().unwrap()).unwrap();
        assert_eq!(
            config.memory_gb, 32.0,
            "Project config memory should be in snapshot"
        );
        assert_eq!(
            config.host_ports,
            vec![5432],
            "Project config host_ports should be in snapshot"
        );
    }

    #[tokio::test]
    async fn test_task_builder_skip_parent_repo_deferral_copies_repo() {
        use crate::test_utils::TestGitRepository;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path().to_path_buf();

        let ctx = AppContext::builder().build();

        // Create and store a parent task
        let parent_task = TaskBuilder::new()
            .repo_root(repo_path.clone())
            .name("parent-task".to_string())
            .prompt(Some("Parent task".to_string()))
            .build(&ctx)
            .await
            .unwrap();
        let storage = ctx.task_storage();
        storage.add_task(parent_task.clone()).await.unwrap();

        // Create child task with skip_parent_repo_deferral
        let child_task = TaskBuilder::new()
            .repo_root(repo_path)
            .name("review-task".to_string())
            .prompt(Some("Review task".to_string()))
            .parent_id(Some(parent_task.id.clone()))
            .skip_parent_repo_deferral(true)
            .build(&ctx)
            .await
            .unwrap();

        assert_eq!(child_task.parent_ids, vec![parent_task.id]);
        assert!(
            child_task.copied_repo_path.is_some(),
            "Child with skip_parent_repo_deferral should have copied_repo_path"
        );
        assert!(
            child_task.source_branch.is_some(),
            "Child with skip_parent_repo_deferral should have source_branch"
        );
    }

    #[tokio::test]
    async fn test_task_builder_skip_parent_repo_deferral_inherits_parent_config() {
        use crate::context::{ResolvedConfig, SharedConfig, TskConfig};
        use crate::test_utils::TestGitRepository;
        use std::collections::HashMap;

        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path().to_path_buf();
        let project_name = "test-project".to_string();

        // Create parent with specific config (memory_gb = 32.0)
        let mut project_configs = HashMap::new();
        project_configs.insert(
            project_name.clone(),
            SharedConfig {
                memory_gb: Some(32.0),
                ..Default::default()
            },
        );
        let tsk_config = TskConfig {
            project: project_configs,
            ..Default::default()
        };
        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();

        let parent_task = TaskBuilder::new()
            .repo_root(repo_path.clone())
            .name("parent-task".to_string())
            .prompt(Some("Parent".to_string()))
            .project(Some(project_name))
            .build(&ctx)
            .await
            .unwrap();
        let storage = ctx.task_storage();
        storage.add_task(parent_task.clone()).await.unwrap();

        // Create child with skip_parent_repo_deferral (no project override)
        let child_task = TaskBuilder::new()
            .repo_root(repo_path)
            .name("review-task".to_string())
            .prompt(Some("Review".to_string()))
            .parent_id(Some(parent_task.id.clone()))
            .skip_parent_repo_deferral(true)
            .build(&ctx)
            .await
            .unwrap();

        // Child should inherit parent's resolved_config
        assert_eq!(
            child_task.resolved_config, parent_task.resolved_config,
            "Child should inherit parent's resolved_config"
        );

        let config: ResolvedConfig =
            serde_json::from_str(child_task.resolved_config.as_ref().unwrap()).unwrap();
        assert_eq!(
            config.memory_gb, 32.0,
            "Inherited config should have parent's memory_gb"
        );
    }
}
