use super::Command;
use crate::context::AppContext;
use crate::context::tsk_config::ResolvedConfig;
use crate::git_operations;
use crate::task::TaskBuilder;
use crate::task::TaskStatus;
use async_trait::async_trait;
use std::error::Error;
use std::path::Path;

pub struct ReviewCommand {
    pub task_id: Option<String>,
    pub base: Option<String>,
    pub name: Option<String>,
    pub agent: Option<String>,
    pub stack: Option<String>,
    pub repo: Option<String>,
    pub edit: bool,
    pub no_network_isolation: bool,
    pub dind: bool,
}

#[async_trait]
impl Command for ReviewCommand {
    async fn execute(&self, ctx: &AppContext) -> Result<(), Box<dyn Error>> {
        let repo_root = crate::repo_utils::find_repository_root(std::path::Path::new(
            self.repo.as_deref().unwrap_or("."),
        ))?;

        if self.task_id.is_some() && self.base.is_some() {
            return Err("Cannot specify both a task ID and --base.".into());
        }

        if let Some(ref base) = self.base {
            return self.execute_base_review(ctx, &repo_root, base).await;
        }

        if let Some(ref tid) = self.task_id {
            return self.execute_task_review(ctx, &repo_root, tid).await;
        }

        // Auto-detect mode
        self.execute_auto_detect(ctx, &repo_root).await
    }
}

impl ReviewCommand {
    async fn resolve_config(ctx: &AppContext, repo_root: &Path) -> ResolvedConfig {
        let tsk_config = ctx.tsk_config();
        let project = crate::repository::detect_project_name(repo_root)
            .await
            .unwrap_or_else(|_| "default".to_string());
        let project_config = crate::context::tsk_config::load_project_config(repo_root);
        tsk_config.resolve_config(&project, project_config.as_ref(), Some(repo_root))
    }

    async fn execute_auto_detect(
        &self,
        ctx: &AppContext,
        repo_root: &Path,
    ) -> Result<(), Box<dyn Error>> {
        let branch = git_operations::get_current_branch(repo_root)
            .await
            .map_err(|e| format!("Failed to get current branch: {e}"))?
            .ok_or("Cannot auto-detect: HEAD is detached. Use `tsk review <taskid>` or `tsk review --base <ref>`.")?;

        // Check if branch matches tsk/{type}/{name}/{id}
        if let Some(task_id) = extract_task_id_from_branch(&branch) {
            return self.execute_task_review(ctx, repo_root, &task_id).await;
        }

        // Not a tsk branch — try git-town parent
        let resolved = Self::resolve_config(ctx, repo_root).await;

        if resolved.git_town
            && let Some(parent_branch) =
                git_operations::get_git_town_parent(repo_root, &branch).await?
        {
            let base_sha = git_operations::rev_parse(repo_root, &parent_branch)
                .await
                .map_err(|e| {
                    format!(
                        "Failed to resolve git-town parent branch '{}': {}",
                        parent_branch, e
                    )
                })?;
            return self.execute_base_review(ctx, repo_root, &base_sha).await;
        }

        Err("Could not detect a tsk task from the current branch. Use `tsk review <taskid>` or `tsk review --base <ref>`.".into())
    }

    async fn execute_task_review(
        &self,
        ctx: &AppContext,
        repo_root: &Path,
        task_id: &str,
    ) -> Result<(), Box<dyn Error>> {
        let storage = ctx.task_storage();
        let task = storage
            .get_task(task_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("Task '{}' not found.", task_id))?;

        if task.status != TaskStatus::Complete {
            return Err(format!(
                "Task '{}' is not complete (status: {:?}). Only completed tasks can be reviewed.",
                task_id, task.status
            )
            .into());
        }

        // Find the root task (first non-tsk-review in the parent chain)
        let all_tasks = storage.list_tasks().await.map_err(|e| e.to_string())?;
        let root_task = find_root_task(&task, &all_tasks);

        // Resolve base commit
        let base_commit = self
            .resolve_base_commit(repo_root, root_task, &task, ctx)
            .await?;

        // Determine review chain numbering
        let review_count = count_reviews_for_root(&root_task.id, &all_tasks);
        let review_name = self
            .name
            .clone()
            .unwrap_or_else(|| format!("{}-review{}", root_task.name, review_count + 1));

        // Determine parent_id for chaining
        let parent_id = determine_review_parent(repo_root, &task, &root_task.id, &all_tasks).await;

        // Open editor for review feedback
        let review_content = self
            .get_review_feedback(ctx, repo_root, &base_commit, &task.branch_name)
            .await?;

        if review_content.trim().is_empty() {
            println!("No review feedback provided, skipping.");
            return Ok(());
        }

        // Write review content to a temp file for prompt_file
        let temp_dir = tempfile::tempdir()?;
        let review_file_path = temp_dir.path().join("review-feedback.md");
        std::fs::write(&review_file_path, &review_content)?;

        // Build the review task
        let builder = TaskBuilder::new()
            .repo_root(repo_root.to_path_buf())
            .name(review_name.clone())
            .task_type("tsk-review".to_string())
            .prompt_file(Some(review_file_path))
            .edit(self.edit)
            .agent(self.agent.clone())
            .stack(self.stack.clone())
            .network_isolation(!self.no_network_isolation)
            .dind(if self.dind { Some(true) } else { None })
            .parent_id(Some(parent_id))
            .skip_parent_repo_deferral(true)
            .target_branch(Some(task.branch_name.clone()))
            .source_commit_override(Some(base_commit));

        let review_task = builder.build(ctx).await?;

        storage
            .add_task(review_task.clone())
            .await
            .map_err(|e| e as Box<dyn Error>)?;

        let parent_suffix = if review_task.parent_ids.is_empty() {
            String::new()
        } else {
            format!(" parent:{}", review_task.parent_ids.join(","))
        };
        println!(
            "Queued {} ({}, {}, {}){}",
            review_task.id,
            review_task.task_type,
            review_task.stack,
            review_task.agent,
            parent_suffix
        );

        Ok(())
    }

    async fn execute_base_review(
        &self,
        ctx: &AppContext,
        repo_root: &Path,
        base_ref: &str,
    ) -> Result<(), Box<dyn Error>> {
        let base_sha = git_operations::rev_parse(repo_root, base_ref)
            .await
            .map_err(|e| format!("Failed to resolve ref '{}': {}", base_ref, e))?;

        let review_name = self.name.clone().unwrap_or_else(|| "review1".to_string());

        let head_sha = git_operations::get_current_commit(repo_root)
            .await
            .map_err(|e| format!("Failed to get HEAD: {e}"))?;

        // Open editor for review feedback
        let review_content = self
            .get_review_feedback(ctx, repo_root, &base_sha, &head_sha)
            .await?;

        if review_content.trim().is_empty() {
            println!("No review feedback provided, skipping.");
            return Ok(());
        }

        // Write review content to a temp file
        let temp_dir = tempfile::tempdir()?;
        let review_file_path = temp_dir.path().join("review-feedback.md");
        std::fs::write(&review_file_path, &review_content)?;

        let builder = TaskBuilder::new()
            .repo_root(repo_root.to_path_buf())
            .name(review_name.clone())
            .task_type("tsk-review".to_string())
            .prompt_file(Some(review_file_path))
            .edit(self.edit)
            .agent(self.agent.clone())
            .stack(self.stack.clone())
            .network_isolation(!self.no_network_isolation)
            .dind(if self.dind { Some(true) } else { None })
            .source_commit_override(Some(base_sha));

        let storage = ctx.task_storage();
        let review_task = builder.build(ctx).await?;
        storage
            .add_task(review_task.clone())
            .await
            .map_err(|e| e as Box<dyn Error>)?;

        println!(
            "Queued {} ({}, {}, {})",
            review_task.id, review_task.task_type, review_task.stack, review_task.agent,
        );

        Ok(())
    }

    async fn resolve_base_commit(
        &self,
        repo_root: &Path,
        root_task: &crate::task::Task,
        task: &crate::task::Task,
        ctx: &AppContext,
    ) -> Result<String, Box<dyn Error>> {
        let source_commit = &root_task.source_commit;

        // Check if source_commit is an ancestor of the task's branch HEAD
        if git_operations::is_ancestor(repo_root, source_commit, &task.branch_name)
            .await
            .unwrap_or(false)
        {
            return Ok(source_commit.clone());
        }

        // Try git-town merge-base
        let resolved = Self::resolve_config(ctx, repo_root).await;

        if resolved.git_town
            && let Some(parent_branch) =
                git_operations::get_git_town_parent(repo_root, &task.branch_name).await?
            && let Ok(mb) =
                git_operations::merge_base(repo_root, &parent_branch, &task.branch_name).await
        {
            return Ok(mb);
        }

        // Try source_branch merge-base
        if let Some(ref source_branch) = root_task.source_branch
            && let Ok(mb) =
                git_operations::merge_base(repo_root, source_branch, &task.branch_name).await
        {
            return Ok(mb);
        }

        Err(format!(
            "Could not determine base commit for task '{}'. The source commit '{}' is not an ancestor of the task branch. Use `tsk review --base <ref>` to specify a base manually.",
            task.id, source_commit
        ).into())
    }

    async fn get_review_feedback(
        &self,
        ctx: &AppContext,
        repo_root: &Path,
        base: &str,
        version: &str,
    ) -> Result<String, Box<dyn Error>> {
        let temp_dir = tempfile::tempdir()?;
        let review_file = temp_dir.path().join("review.md");
        std::fs::write(&review_file, "")?;

        let review_file_str = review_file.to_string_lossy().to_string();

        let resolved = Self::resolve_config(ctx, repo_root).await;

        if let Some(ref review_command) = resolved.review_command {
            let cmd = review_command
                .replace("{{base}}", base)
                .replace("{{version}}", version)
                .replace("{{review_file}}", &review_file_str);

            let status = std::process::Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .status()
                .map_err(|e| format!("Failed to execute review command: {e}"))?;

            if !status.success() {
                return Err(format!("Review command exited with status: {}", status).into());
            }
        } else {
            // Use $EDITOR
            let editor = std::env::var("EDITOR")
                .map_err(|_| "No review_command configured and $EDITOR is not set. Set $EDITOR or configure review_command in tsk.toml.")?;

            let status = std::process::Command::new(&editor)
                .arg(&review_file_str)
                .status()
                .map_err(|e| format!("Failed to launch editor '{}': {e}", editor))?;

            if !status.success() {
                return Err(format!("Editor exited with status: {}", status).into());
            }
        }

        let content = std::fs::read_to_string(&review_file)?;
        Ok(content)
    }
}

/// Extract a task ID from a tsk branch name pattern: `tsk/{type}/{name}/{id}`
pub fn extract_task_id_from_branch(branch: &str) -> Option<String> {
    let parts: Vec<&str> = branch.split('/').collect();
    if parts.len() >= 4 && parts[0] == "tsk" {
        Some(parts.last().unwrap().to_string())
    } else {
        None
    }
}

/// Find the root task (first non-tsk-review task) by walking the parent chain
fn find_root_task<'a>(
    task: &'a crate::task::Task,
    all_tasks: &'a [crate::task::Task],
) -> &'a crate::task::Task {
    let mut current = task;
    loop {
        if current.task_type != "tsk-review" {
            return current;
        }
        if let Some(parent_id) = current.parent_ids.first()
            && let Some(parent) = all_tasks.iter().find(|t| t.id == *parent_id)
        {
            current = parent;
            continue;
        }
        // No parent or parent not found — this is the root
        return current;
    }
}

/// Count tsk-review tasks that trace back to a given root task
fn count_reviews_for_root(root_id: &str, all_tasks: &[crate::task::Task]) -> usize {
    all_tasks
        .iter()
        .filter(|t| t.task_type == "tsk-review" && traces_to_root(t, root_id, all_tasks))
        .count()
}

/// Check if a task traces back to the given root task ID through parent chain
fn traces_to_root(
    task: &crate::task::Task,
    root_id: &str,
    all_tasks: &[crate::task::Task],
) -> bool {
    let mut current = task;
    let mut visited = std::collections::HashSet::new();
    loop {
        if current.id == root_id {
            return true;
        }
        if !visited.insert(&current.id) {
            return false; // cycle guard
        }
        if let Some(parent_id) = current.parent_ids.first()
            && let Some(parent) = all_tasks.iter().find(|t| t.id == *parent_id)
        {
            current = parent;
            continue;
        }
        return false;
    }
}

/// Determine which task the review should chain to.
/// If the most recent completed review's HEAD is still an ancestor of the task branch,
/// chain to that review. Otherwise chain to the original task.
async fn determine_review_parent(
    repo_root: &Path,
    reviewed_task: &crate::task::Task,
    root_id: &str,
    all_tasks: &[crate::task::Task],
) -> String {
    // Find completed reviews in the chain, sorted by creation time (most recent first)
    let mut reviews: Vec<&crate::task::Task> = all_tasks
        .iter()
        .filter(|t| {
            t.task_type == "tsk-review"
                && t.status == TaskStatus::Complete
                && traces_to_root(t, root_id, all_tasks)
        })
        .collect();
    reviews.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    if let Some(recent_review) = reviews.first() {
        // Check if the recent review's branch HEAD is an ancestor of the task branch
        if git_operations::is_ancestor(
            repo_root,
            &recent_review.branch_name,
            &reviewed_task.branch_name,
        )
        .await
        .unwrap_or(false)
        {
            return recent_review.id.clone();
        }
    }

    reviewed_task.id.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::AppContext;
    use crate::task::{Task, TaskStatus};
    use crate::test_utils::TestGitRepository;

    #[test]
    fn test_extract_task_id_from_tsk_branch() {
        assert_eq!(
            extract_task_id_from_branch("tsk/feat/my-feature/abc123"),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_task_id_from_branch("tsk/fix/bug-fix/XyZ789"),
            Some("XyZ789".to_string())
        );
    }

    #[test]
    fn test_extract_task_id_non_tsk_branch() {
        assert_eq!(extract_task_id_from_branch("main"), None);
        assert_eq!(extract_task_id_from_branch("feature/something"), None);
        assert_eq!(extract_task_id_from_branch("tsk/only-two"), None);
    }

    #[test]
    fn test_extract_task_id_nested_segments() {
        // Extra segments should still take the last one as the ID
        assert_eq!(
            extract_task_id_from_branch("tsk/feat/my/nested/name/id123"),
            Some("id123".to_string())
        );
    }

    #[test]
    fn test_find_root_task_direct() {
        let task = Task {
            task_type: "feat".to_string(),
            ..Task::test_default()
        };
        let all_tasks = vec![task.clone()];
        let root = find_root_task(&task, &all_tasks);
        assert_eq!(root.id, task.id);
    }

    #[test]
    fn test_find_root_task_through_reviews() {
        let original = Task {
            id: "original".to_string(),
            task_type: "feat".to_string(),
            ..Task::test_default()
        };
        let review1 = Task {
            id: "review1".to_string(),
            task_type: "tsk-review".to_string(),
            parent_ids: vec!["original".to_string()],
            ..Task::test_default()
        };
        let review2 = Task {
            id: "review2".to_string(),
            task_type: "tsk-review".to_string(),
            parent_ids: vec!["review1".to_string()],
            ..Task::test_default()
        };
        let all_tasks = vec![original.clone(), review1.clone(), review2.clone()];
        let root = find_root_task(&review2, &all_tasks);
        assert_eq!(root.id, "original");
    }

    #[test]
    fn test_count_reviews_for_root() {
        let original = Task {
            id: "original".to_string(),
            task_type: "feat".to_string(),
            ..Task::test_default()
        };
        let review1 = Task {
            id: "review1".to_string(),
            task_type: "tsk-review".to_string(),
            parent_ids: vec!["original".to_string()],
            ..Task::test_default()
        };
        let review2 = Task {
            id: "review2".to_string(),
            task_type: "tsk-review".to_string(),
            parent_ids: vec!["review1".to_string()],
            ..Task::test_default()
        };
        let unrelated = Task {
            id: "unrelated".to_string(),
            task_type: "tsk-review".to_string(),
            parent_ids: vec!["other".to_string()],
            ..Task::test_default()
        };
        let all_tasks = vec![
            original.clone(),
            review1.clone(),
            review2.clone(),
            unrelated.clone(),
        ];
        assert_eq!(count_reviews_for_root("original", &all_tasks), 2);
    }

    #[test]
    fn test_traces_to_root() {
        let original = Task {
            id: "original".to_string(),
            task_type: "feat".to_string(),
            ..Task::test_default()
        };
        let review1 = Task {
            id: "review1".to_string(),
            task_type: "tsk-review".to_string(),
            parent_ids: vec!["original".to_string()],
            ..Task::test_default()
        };
        let all_tasks = vec![original.clone(), review1.clone()];
        assert!(traces_to_root(&review1, "original", &all_tasks));
        assert!(!traces_to_root(&review1, "nonexistent", &all_tasks));
    }

    #[tokio::test]
    async fn test_review_non_complete_task_errors() {
        let ctx = AppContext::builder().build();
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        let task = Task {
            id: "running-task".to_string(),
            status: TaskStatus::Running,
            repo_root: test_repo.path().to_path_buf(),
            ..Task::test_default()
        };

        let storage = ctx.task_storage();
        storage.add_task(task).await.unwrap();

        let cmd = ReviewCommand {
            task_id: Some("running-task".to_string()),
            base: None,
            name: None,
            agent: None,
            stack: None,
            repo: Some(test_repo.path().to_string_lossy().to_string()),
            edit: false,
            no_network_isolation: false,
            dind: false,
        };

        let result = cmd.execute(&ctx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not complete"));
    }

    #[tokio::test]
    async fn test_review_nonexistent_task_errors() {
        let ctx = AppContext::builder().build();
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        let cmd = ReviewCommand {
            task_id: Some("nonexistent".to_string()),
            base: None,
            name: None,
            agent: None,
            stack: None,
            repo: Some(test_repo.path().to_string_lossy().to_string()),
            edit: false,
            no_network_isolation: false,
            dind: false,
        };

        let result = cmd.execute(&ctx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_review_mutual_exclusivity() {
        let ctx = AppContext::builder().build();
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        let cmd = ReviewCommand {
            task_id: Some("some-task".to_string()),
            base: Some("main".to_string()),
            name: None,
            agent: None,
            stack: None,
            repo: Some(test_repo.path().to_string_lossy().to_string()),
            edit: false,
            no_network_isolation: false,
            dind: false,
        };

        let result = cmd.execute(&ctx).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot specify both")
        );
    }

    #[tokio::test]
    async fn test_auto_detect_non_tsk_branch_no_git_town_errors() {
        let ctx = AppContext::builder().build();
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        let cmd = ReviewCommand {
            task_id: None,
            base: None,
            name: None,
            agent: None,
            stack: None,
            repo: Some(test_repo.path().to_string_lossy().to_string()),
            edit: false,
            no_network_isolation: false,
            dind: false,
        };

        let result = cmd.execute(&ctx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Could not detect"));
    }

    #[tokio::test]
    async fn test_base_review_invalid_ref_errors() {
        let ctx = AppContext::builder().build();
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        let cmd = ReviewCommand {
            task_id: None,
            base: Some("nonexistent-ref-12345".to_string()),
            name: None,
            agent: None,
            stack: None,
            repo: Some(test_repo.path().to_string_lossy().to_string()),
            edit: false,
            no_network_isolation: false,
            dind: false,
        };

        let result = cmd.execute(&ctx).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to resolve ref")
        );
    }
}
