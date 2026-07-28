use crate::context::{AppContext, TaskStorage};
use crate::task::{Task, TaskBuilder, TaskStatus};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

/// Result of cleaning terminal-state tasks.
#[derive(Debug)]
pub struct CleanResult {
    pub deleted: usize,
    pub skipped: usize,
}

/// Optional overrides applied when retrying a task.
#[derive(Debug, Default)]
pub struct RetryOverrides {
    pub name: Option<String>,
    pub agent: Option<String>,
    pub stack: Option<String>,
    pub project: Option<String>,
    pub parent_id: Option<String>,
    pub dind: Option<bool>,
    pub privileged: Option<bool>,
    pub sudo: Option<bool>,
    pub tailscale: Option<bool>,
    pub devices: Vec<String>,
    pub repo_copy_source: Option<PathBuf>,
}

#[cfg(test)]
use crate::context::tsk_env::TskEnv;

/// Manages task storage operations.
///
/// TaskManager provides an interface for creating and managing tasks including
/// delete, clean, retry, and list operations via TaskStorage.
pub struct TaskManager {
    task_storage: Arc<TaskStorage>,
}

impl TaskManager {
    /// Creates a TaskManager for storage operations (clean, delete, retry, list).
    ///
    /// # Arguments
    ///
    /// * `ctx` - The application context providing dependencies
    ///
    /// # Returns
    ///
    /// Returns a configured TaskManager or an error if initialization fails.
    pub fn new(ctx: &AppContext) -> Result<Self, String> {
        Ok(Self {
            task_storage: ctx.task_storage(),
        })
    }

    /// Delete a specific task and its associated directory.
    ///
    /// Removes the task from storage and deletes its working directory.
    pub async fn delete_task(&self, task_id: &str) -> Result<(), String> {
        // Get the task to find its directory
        let task = self
            .task_storage
            .get_task(task_id)
            .await
            .map_err(|e| format!("Error getting task: {e}"))?;

        if task.is_none() {
            return Err(format!("Task with ID '{task_id}' not found"));
        }

        // Delete from storage first
        self.task_storage
            .delete_task(task_id)
            .await
            .map_err(|e| format!("Error deleting task from storage: {e}"))?;

        // Delete the task directory using the copied_repo_path
        let task = task.unwrap();
        // The task directory is the parent of the copied repo path
        if let Some(ref copied_repo_path) = task.copied_repo_path
            && let Some(task_dir) = copied_repo_path.parent()
            && crate::file_system::exists(task_dir).await.unwrap_or(false)
        {
            crate::file_system::remove_dir(task_dir)
                .await
                .map_err(|e| format!("Error deleting task directory: {e}"))?;
        }

        Ok(())
    }

    /// Delete completed (and optionally failed) tasks that are not parents of active children.
    ///
    /// Removes terminal-state tasks from storage and deletes their working directories.
    /// Skips tasks that are referenced as parents by Queued or Running tasks
    /// to avoid orphaning child tasks that depend on them.
    ///
    /// # Arguments
    ///
    /// * `include_failed` - When true, also clean Failed tasks (not just Complete)
    /// * `min_age` - When Some, only clean tasks whose `completed_at` is older than this duration.
    ///   Tasks without a `completed_at` timestamp are never cleaned when an age filter is set.
    pub async fn clean_tasks(
        &self,
        include_failed: bool,
        min_age: Option<chrono::Duration>,
    ) -> Result<CleanResult, String> {
        // Get all tasks to find directories to delete
        let all_tasks = self
            .task_storage
            .list_tasks()
            .await
            .map_err(|e| format!("Error listing tasks: {e}"))?;

        // Build set of task IDs that are parents of active (Queued/Running) children
        let active_parent_ids: HashSet<String> = all_tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Queued || t.status == TaskStatus::Running)
            .flat_map(|t| t.parent_ids.iter().cloned())
            .collect();

        let now = chrono::Utc::now();

        // Filter for cleanable tasks based on status and age
        let cleanable_tasks: Vec<&Task> = all_tasks
            .iter()
            .filter(|t| {
                let status_match = t.status == TaskStatus::Complete
                    || (include_failed
                        && (t.status == TaskStatus::Failed || t.status == TaskStatus::Cancelled));
                if !status_match {
                    return false;
                }
                if let Some(min_age) = min_age {
                    match t.completed_at {
                        Some(completed_at) => now - completed_at >= min_age,
                        None => false,
                    }
                } else {
                    true
                }
            })
            .collect();

        let (deletable, skipped): (Vec<&Task>, Vec<&Task>) = cleanable_tasks
            .into_iter()
            .partition(|t| !active_parent_ids.contains(&t.id));

        // Delete directories for deletable tasks
        for task in &deletable {
            // The task directory is the parent of the copied repo path
            if let Some(ref copied_repo_path) = task.copied_repo_path
                && let Some(task_dir) = copied_repo_path.parent()
                && crate::file_system::exists(task_dir).await.unwrap_or(false)
                && let Err(e) = crate::file_system::remove_dir(task_dir).await
            {
                eprintln!(
                    "Warning: Failed to delete task directory {}: {}",
                    task.id, e
                );
            }
        }

        // Delete each deletable task individually from storage
        let mut deleted = 0;
        for task in &deletable {
            if let Err(e) = self.task_storage.delete_task(&task.id).await {
                eprintln!("Warning: Failed to delete task {}: {}", task.id, e);
            } else {
                deleted += 1;
            }
        }

        Ok(CleanResult {
            deleted,
            skipped: skipped.len(),
        })
    }

    /// Retry a task by creating a new task with the same instructions.
    ///
    /// Creates a duplicate task with a new ID, optionally allowing instruction editing.
    /// The original task must have been executed (not Queued) to be retried.
    pub async fn retry_task(
        &self,
        task_id: &str,
        edit_instructions: bool,
        overrides: RetryOverrides,
        ctx: &AppContext,
    ) -> Result<String, String> {
        // Retrieve the original task
        let original_task = self
            .task_storage
            .get_task(task_id)
            .await
            .map_err(|e| format!("Error getting task: {e}"))?;

        let original_task = match original_task {
            Some(task) => task,
            None => return Err(format!("Task with ID '{task_id}' not found")),
        };

        // Validate that the task has been executed (not Queued)
        if original_task.status == TaskStatus::Queued {
            return Err("Cannot retry a task that hasn't been executed yet".to_string());
        }

        // Use TaskBuilder to create the new task, leveraging from_existing
        let mut builder = TaskBuilder::from_existing(&original_task);

        builder = builder.edit(edit_instructions);

        // Apply optional overrides
        if let Some(name) = overrides.name {
            builder = builder.name(name);
        }
        if let Some(agent) = overrides.agent {
            builder = builder.agent(Some(agent));
        }
        if let Some(stack) = overrides.stack {
            builder = builder.stack(Some(stack));
        }
        if let Some(project) = overrides.project {
            builder = builder.project(Some(project));
        }
        if let Some(parent_id) = overrides.parent_id {
            builder = builder.parent_id(Some(parent_id));
        }
        if let Some(dind) = overrides.dind {
            builder = builder.dind(Some(dind));
        }
        if let Some(privileged) = overrides.privileged {
            builder = builder.privileged(Some(privileged));
        }
        if let Some(sudo) = overrides.sudo {
            builder = builder.sudo(Some(sudo));
        }
        if let Some(tailscale) = overrides.tailscale {
            builder = builder.tailscale(Some(tailscale));
        }
        if !overrides.devices.is_empty() {
            builder = builder.devices(overrides.devices);
        }
        if let Some(source) = overrides.repo_copy_source {
            builder = builder.repo_copy_source(Some(source));
        }

        let new_task = builder
            .build(ctx)
            .await
            .map_err(|e| format!("Failed to build retry task: {e}"))?;

        // Store the new task
        self.task_storage
            .add_task(new_task.clone())
            .await
            .map_err(|e| format!("Error adding retry task to storage: {e}"))?;

        Ok(new_task.id)
    }

    /// Find all descendant tasks of a given task, in BFS order (children first, then grandchildren).
    pub async fn find_descendant_tasks(&self, task_id: &str) -> Result<Vec<Task>, String> {
        let all_tasks = self
            .task_storage
            .list_tasks()
            .await
            .map_err(|e| format!("Error listing tasks: {e}"))?;

        let mut descendants = Vec::new();
        let mut current_parents = vec![task_id.to_string()];
        let mut visited = HashSet::new();

        while !current_parents.is_empty() {
            let mut next_parents = Vec::new();
            for task in &all_tasks {
                if !visited.contains(&task.id)
                    && task
                        .parent_ids
                        .iter()
                        .any(|pid| current_parents.contains(pid))
                {
                    visited.insert(task.id.clone());
                    descendants.push(task.clone());
                    next_parents.push(task.id.clone());
                }
            }
            current_parents = next_parents;
        }

        Ok(descendants)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TestGitRepository;
    use std::sync::Arc;

    /// Helper function to set up a test environment with TSK environment, git repository, and AppContext
    async fn setup_test_environment() -> anyhow::Result<(Arc<TskEnv>, TestGitRepository, AppContext)>
    {
        // Create a test git repository
        let test_repo = TestGitRepository::new()?;
        test_repo.init_with_commit()?;

        // Create AppContext - automatically gets test defaults
        let ctx = AppContext::builder().build();

        // Get the TSK environment from the context
        let tsk_env = ctx.tsk_env();

        Ok((tsk_env, test_repo, ctx))
    }

    /// Helper function to set up task directory structure with files
    async fn setup_task_directory(
        tsk_env: &TskEnv,
        task_id: &str,
        instructions_content: &str,
    ) -> anyhow::Result<std::path::PathBuf> {
        let task_dir_path = tsk_env.task_dir(task_id);
        std::fs::create_dir_all(&task_dir_path)?;

        let instructions_path = task_dir_path.join("instructions.md");
        std::fs::write(&instructions_path, instructions_content)?;

        Ok(task_dir_path)
    }

    #[tokio::test]
    async fn test_delete_task() {
        use crate::test_utils::TestGitRepository;

        // Create a test git repository
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_root = test_repo.path().to_path_buf();

        // Create AppContext - automatically gets test defaults
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();

        // Create a task
        let task_id = "test-task-123".to_string();
        let task_dir_path = tsk_env.task_dir(&task_id);
        let copied_repo_path = task_dir_path.join("repo");

        // Create the task directory and file
        std::fs::create_dir_all(&task_dir_path).unwrap();
        std::fs::write(task_dir_path.join("test.txt"), "test content").unwrap();

        // Add task via storage API
        let task = Task {
            id: task_id.clone(),
            repo_root,
            branch_name: format!("tsk/{task_id}"),
            copied_repo_path: Some(copied_repo_path),
            ..Task::test_default()
        };
        let storage = ctx.task_storage();
        storage.add_task(task).await.unwrap();

        let task_manager = TaskManager::new(&ctx).unwrap();

        // Delete the task
        let result = task_manager.delete_task(&task_id).await;
        assert!(result.is_ok(), "Failed to delete task: {result:?}");

        // Verify task directory is deleted
        assert!(
            !task_dir_path.exists(),
            "Task directory should have been deleted"
        );

        // Verify task is removed from storage
        let task = task_manager.task_storage.get_task(&task_id).await.unwrap();
        assert!(task.is_none(), "Task should have been deleted from storage");
    }

    #[tokio::test]
    async fn test_clean_tasks() {
        // Set up test environment
        let (config, test_repo, ctx) = setup_test_environment().await.unwrap();
        let repo_root = test_repo.path().to_path_buf();

        // Create tasks with different statuses
        let queued_task_id = "queued-task-123".to_string();
        let completed_task_id = "completed-task-456".to_string();

        // Create task directories and files
        let queued_dir_path =
            setup_task_directory(&config, &queued_task_id, "Queued task instructions")
                .await
                .unwrap();
        let completed_dir_path =
            setup_task_directory(&config, &completed_task_id, "Completed task instructions")
                .await
                .unwrap();

        let queued_task = Task {
            id: queued_task_id.clone(),
            repo_root: repo_root.clone(),
            name: "queued-task".to_string(),
            branch_name: format!("tsk/{queued_task_id}"),
            copied_repo_path: Some(queued_dir_path.join("repo")),
            ..Task::test_default()
        };

        let completed_task = Task {
            id: completed_task_id.clone(),
            repo_root: repo_root.clone(),
            name: "completed-task".to_string(),
            task_type: "fix".to_string(),
            branch_name: format!("tsk/{completed_task_id}"),
            status: TaskStatus::Complete,
            copied_repo_path: Some(completed_dir_path.join("repo")),
            ..Task::test_default()
        };

        // Add tasks via storage API
        let storage = ctx.task_storage();
        storage.add_task(queued_task).await.unwrap();
        storage.add_task(completed_task).await.unwrap();

        // Create TaskManager and clean tasks
        let task_manager = TaskManager::new(&ctx).unwrap();

        let result = task_manager.clean_tasks(false, None).await;
        assert!(result.is_ok(), "Failed to clean tasks: {result:?}");
        let result = result.unwrap();
        assert_eq!(result.deleted, 1);
        assert_eq!(result.skipped, 0);

        // Verify directories are cleaned up
        assert!(
            queued_dir_path.exists(),
            "Queued task directory should still exist"
        );
        assert!(
            !completed_dir_path.exists(),
            "Completed task directory should be deleted"
        );

        // Verify storage was updated
        let remaining_tasks = task_manager.task_storage.list_tasks().await.unwrap();
        assert_eq!(remaining_tasks.len(), 1);
        assert_eq!(remaining_tasks[0].id, queued_task_id);
    }

    #[tokio::test]
    async fn test_retry_task() {
        // Set up test environment
        let (config, test_repo, ctx) = setup_test_environment().await.unwrap();
        let repo_root = test_repo.path().to_path_buf();

        // Create a completed task to retry
        let task_id = "abcd1234".to_string();
        let task_dir_path = config.task_dir(&task_id);
        let instructions_path = task_dir_path.join("instructions.md");

        let completed_task = Task {
            id: task_id.clone(),
            repo_root: repo_root.clone(),
            name: "original-task".to_string(),
            task_type: "generic".to_string(),
            instructions_file: instructions_path.to_string_lossy().to_string(),
            branch_name: format!("tsk/{task_id}"),
            status: TaskStatus::Complete,
            copied_repo_path: Some(task_dir_path.join("repo")),
            ..Task::test_default()
        };

        // Set up task directory with instructions
        let instructions_content =
            "# Original Task Instructions\n\nThis is the original task content.";
        setup_task_directory(&config, &task_id, instructions_content)
            .await
            .unwrap();

        // Add task via storage API
        let storage = ctx.task_storage();
        storage.add_task(completed_task).await.unwrap();

        // Create TaskManager and retry the task
        let task_manager = TaskManager::new(&ctx).unwrap();

        let result = task_manager
            .retry_task(&task_id, false, RetryOverrides::default(), &ctx)
            .await;
        assert!(result.is_ok(), "Failed to retry task: {result:?}");
        let new_task_id = result.unwrap();

        // Verify new task ID format (8 characters)
        assert_eq!(new_task_id.len(), 8);

        // Verify task was added to storage
        let new_task = task_manager
            .task_storage
            .get_task(&new_task_id)
            .await
            .unwrap();
        assert!(new_task.is_some());
        let new_task = new_task.unwrap();
        // Since we removed the "retry-" prefix, name should be the same as original
        assert_eq!(new_task.name, "original-task");
        assert_eq!(new_task.task_type, "generic");
        assert_eq!(new_task.agent, "claude".to_string());
        assert_eq!(new_task.status, TaskStatus::Queued);

        // Verify instructions file was created
        let new_task_dir = config.task_dir(&new_task_id);
        let new_instructions_path = new_task_dir.join("instructions.md");
        assert!(new_instructions_path.exists());

        // Verify instructions content was copied
        let copied_content = std::fs::read_to_string(&new_instructions_path).unwrap();
        assert_eq!(copied_content, instructions_content);
    }

    #[tokio::test]
    async fn test_retry_task_not_found() {
        // Set up test environment
        let (_config, _test_repo, ctx) = setup_test_environment().await.unwrap();

        // Storage starts empty (SQLite creates the DB on first use)

        // Create TaskManager and try to retry a non-existent task
        let task_manager = TaskManager::new(&ctx).unwrap();

        let result = task_manager
            .retry_task("non-existent-task", false, RetryOverrides::default(), &ctx)
            .await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Task with ID 'non-existent-task' not found")
        );
    }

    #[tokio::test]
    async fn test_retry_task_queued_error() {
        // Set up test environment
        let (config, test_repo, ctx) = setup_test_environment().await.unwrap();
        let repo_root = test_repo.path().to_path_buf();

        // Create a queued task (should not be retryable)
        let task_id = "efgh5678".to_string();
        let task_dir = config.task_dir(&task_id);

        // Add queued task via storage API
        let task = Task {
            id: task_id.clone(),
            repo_root,
            name: "queued-task".to_string(),
            branch_name: format!("tsk/{task_id}"),
            copied_repo_path: Some(task_dir.join("repo")),
            ..Task::test_default()
        };
        let storage = ctx.task_storage();
        storage.add_task(task).await.unwrap();

        // Create TaskManager and try to retry a queued task
        let task_manager = TaskManager::new(&ctx).unwrap();

        let result = task_manager
            .retry_task(&task_id, false, RetryOverrides::default(), &ctx)
            .await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Cannot retry a task that hasn't been executed yet")
        );
    }

    #[tokio::test]
    async fn test_clean_tasks_with_id_matching() {
        // Set up test environment
        let (config, test_repo, ctx) = setup_test_environment().await.unwrap();
        let repo_root = test_repo.path().to_path_buf();

        // Create a task with a specific ID
        let task_id = "ijkl9012".to_string();

        // Set up task directory with instructions
        let task_dir_path = setup_task_directory(&config, &task_id, "Test instructions")
            .await
            .unwrap();

        let completed_task = Task {
            id: task_id.clone(),
            repo_root: repo_root.clone(),
            name: "test-feature".to_string(),
            branch_name: format!("tsk/{task_id}"),
            status: TaskStatus::Complete,
            copied_repo_path: Some(task_dir_path.join("repo")),
            ..Task::test_default()
        };

        // Add task via storage API
        let storage = ctx.task_storage();
        storage.add_task(completed_task).await.unwrap();

        // Create TaskManager and clean tasks
        let task_manager = TaskManager::new(&ctx).unwrap();

        let result = task_manager.clean_tasks(false, None).await;
        assert!(result.is_ok(), "Failed to clean tasks: {result:?}");
        let result = result.unwrap();
        assert_eq!(result.deleted, 1);
        assert_eq!(result.skipped, 0);

        // Verify directory was deleted
        assert!(
            !task_dir_path.exists(),
            "Task directory should have been deleted"
        );

        // Verify storage was updated
        let remaining_tasks = task_manager.task_storage.list_tasks().await.unwrap();
        assert_eq!(remaining_tasks.len(), 0);
    }

    #[tokio::test]
    async fn test_clean_skips_parents_with_active_children() {
        // Set up test environment
        let (config, test_repo, ctx) = setup_test_environment().await.unwrap();
        let repo_root = test_repo.path().to_path_buf();

        // Create a completed parent task
        let parent_task_id = "parent-task-001".to_string();
        let parent_dir_path =
            setup_task_directory(&config, &parent_task_id, "Parent task instructions")
                .await
                .unwrap();

        let parent_task = Task {
            id: parent_task_id.clone(),
            repo_root: repo_root.clone(),
            name: "parent-task".to_string(),
            branch_name: format!("tsk/{parent_task_id}"),
            status: TaskStatus::Complete,
            copied_repo_path: Some(parent_dir_path.join("repo")),
            ..Task::test_default()
        };

        // Create a queued child task referencing the parent
        let child_task_id = "child-task-001".to_string();
        let child_dir_path =
            setup_task_directory(&config, &child_task_id, "Child task instructions")
                .await
                .unwrap();

        let child_task = Task {
            id: child_task_id.clone(),
            repo_root: repo_root.clone(),
            name: "child-task".to_string(),
            branch_name: format!("tsk/{child_task_id}"),
            copied_repo_path: Some(child_dir_path.join("repo")),
            parent_ids: vec![parent_task_id.clone()],
            ..Task::test_default()
        };

        // Create another completed task with no children
        let childless_task_id = "childless-task-001".to_string();
        let childless_dir_path =
            setup_task_directory(&config, &childless_task_id, "Childless task instructions")
                .await
                .unwrap();

        let childless_task = Task {
            id: childless_task_id.clone(),
            repo_root: repo_root.clone(),
            name: "childless-task".to_string(),
            task_type: "fix".to_string(),
            branch_name: format!("tsk/{childless_task_id}"),
            status: TaskStatus::Complete,
            copied_repo_path: Some(childless_dir_path.join("repo")),
            ..Task::test_default()
        };

        // Add all tasks via storage API
        let storage = ctx.task_storage();
        storage.add_task(parent_task).await.unwrap();
        storage.add_task(child_task).await.unwrap();
        storage.add_task(childless_task).await.unwrap();

        // Create TaskManager and clean tasks
        let task_manager = TaskManager::new(&ctx).unwrap();

        let result = task_manager.clean_tasks(false, None).await;
        assert!(result.is_ok(), "Failed to clean tasks: {result:?}");
        let result = result.unwrap();
        assert_eq!(result.deleted, 1);
        assert_eq!(result.skipped, 1);

        // Verify parent task still exists in storage (skipped)
        let parent = task_manager
            .task_storage
            .get_task(&parent_task_id)
            .await
            .unwrap();
        assert!(
            parent.is_some(),
            "Parent task should still exist in storage"
        );

        // Verify childless task was deleted from storage
        let childless = task_manager
            .task_storage
            .get_task(&childless_task_id)
            .await
            .unwrap();
        assert!(
            childless.is_none(),
            "Childless task should have been deleted from storage"
        );

        // Verify parent directory still exists
        assert!(
            parent_dir_path.exists(),
            "Parent task directory should still exist"
        );

        // Verify childless task directory was deleted
        assert!(
            !childless_dir_path.exists(),
            "Childless task directory should have been deleted"
        );
    }

    #[tokio::test]
    async fn test_with_storage_no_git_repo() {
        // Create AppContext - automatically gets test defaults
        let ctx = AppContext::builder().build();

        // Storage starts empty (SQLite creates the DB on first use)

        // This should succeed even without being in a git repository
        let result = TaskManager::new(&ctx);
        assert!(
            result.is_ok(),
            "TaskManager::new should work without a git repository"
        );
    }

    #[tokio::test]
    async fn test_clean_tasks_with_age_filter() {
        let (config, test_repo, ctx) = setup_test_environment().await.unwrap();
        let repo_root = test_repo.path().to_path_buf();

        // Create tasks with different statuses and ages
        let old_complete_id = "old-complete-001".to_string();
        let old_failed_id = "old-failed-002".to_string();
        let young_complete_id = "young-complete-003".to_string();
        let queued_id = "queued-004".to_string();
        let old_parent_id = "old-parent-005".to_string();

        let old_complete_dir = setup_task_directory(&config, &old_complete_id, "Old complete task")
            .await
            .unwrap();
        let old_failed_dir = setup_task_directory(&config, &old_failed_id, "Old failed task")
            .await
            .unwrap();
        let young_complete_dir =
            setup_task_directory(&config, &young_complete_id, "Young complete task")
                .await
                .unwrap();
        let queued_dir = setup_task_directory(&config, &queued_id, "Queued task")
            .await
            .unwrap();
        let old_parent_dir = setup_task_directory(&config, &old_parent_id, "Old parent task")
            .await
            .unwrap();

        let ten_days_ago = chrono::Utc::now() - chrono::Duration::days(10);
        let two_days_ago = chrono::Utc::now() - chrono::Duration::days(2);

        // Old Complete task (should be deleted)
        let old_complete = Task {
            id: old_complete_id.clone(),
            repo_root: repo_root.clone(),
            name: "old-complete".to_string(),
            branch_name: format!("tsk/{old_complete_id}"),
            status: TaskStatus::Complete,
            completed_at: Some(ten_days_ago),
            copied_repo_path: Some(old_complete_dir.join("repo")),
            ..Task::test_default()
        };

        // Old Failed task (should be deleted when include_failed=true)
        let old_failed = Task {
            id: old_failed_id.clone(),
            repo_root: repo_root.clone(),
            name: "old-failed".to_string(),
            task_type: "fix".to_string(),
            branch_name: format!("tsk/{old_failed_id}"),
            status: TaskStatus::Failed,
            completed_at: Some(ten_days_ago),
            copied_repo_path: Some(old_failed_dir.join("repo")),
            ..Task::test_default()
        };

        // Young Complete task (should be preserved - too young)
        let young_complete = Task {
            id: young_complete_id.clone(),
            repo_root: repo_root.clone(),
            name: "young-complete".to_string(),
            branch_name: format!("tsk/{young_complete_id}"),
            status: TaskStatus::Complete,
            completed_at: Some(two_days_ago),
            copied_repo_path: Some(young_complete_dir.join("repo")),
            ..Task::test_default()
        };

        // Queued task (should always be preserved), child of old_parent
        let queued = Task {
            id: queued_id.clone(),
            repo_root: repo_root.clone(),
            name: "queued-task".to_string(),
            branch_name: format!("tsk/{queued_id}"),
            copied_repo_path: Some(queued_dir.join("repo")),
            parent_ids: vec![old_parent_id.clone()],
            ..Task::test_default()
        };

        // Old parent task with active child (should be skipped due to parent protection)
        let old_parent = Task {
            id: old_parent_id.clone(),
            repo_root: repo_root.clone(),
            name: "old-parent".to_string(),
            branch_name: format!("tsk/{old_parent_id}"),
            status: TaskStatus::Complete,
            completed_at: Some(ten_days_ago),
            copied_repo_path: Some(old_parent_dir.join("repo")),
            ..Task::test_default()
        };

        let storage = ctx.task_storage();
        storage.add_task(old_complete).await.unwrap();
        storage.add_task(old_failed).await.unwrap();
        storage.add_task(young_complete).await.unwrap();
        storage.add_task(queued).await.unwrap();
        storage.add_task(old_parent).await.unwrap();

        let task_manager = TaskManager::new(&ctx).unwrap();

        let result = task_manager
            .clean_tasks(true, Some(chrono::Duration::days(7)))
            .await;
        assert!(result.is_ok(), "Failed to clean tasks: {result:?}");
        let result = result.unwrap();
        assert_eq!(result.deleted, 2, "Should delete old complete + old failed");
        assert_eq!(
            result.skipped, 1,
            "Should skip old parent with active child"
        );

        // Verify old tasks are deleted
        assert!(
            !old_complete_dir.exists(),
            "Old complete dir should be deleted"
        );
        assert!(!old_failed_dir.exists(), "Old failed dir should be deleted");

        // Verify preserved tasks still exist
        assert!(
            young_complete_dir.exists(),
            "Young complete dir should exist"
        );
        assert!(queued_dir.exists(), "Queued dir should exist");
        assert!(old_parent_dir.exists(), "Old parent dir should exist");

        let remaining = task_manager.task_storage.list_tasks().await.unwrap();
        assert_eq!(remaining.len(), 3);
        let remaining_ids: HashSet<String> = remaining.iter().map(|t| t.id.clone()).collect();
        assert!(remaining_ids.contains(&young_complete_id));
        assert!(remaining_ids.contains(&queued_id));
        assert!(remaining_ids.contains(&old_parent_id));
    }

    #[tokio::test]
    async fn test_find_descendant_tasks() {
        let (config, test_repo, ctx) = setup_test_environment().await.unwrap();
        let repo_root = test_repo.path().to_path_buf();

        // Create a chain: parent (complete) -> child (failed) -> grandchild (failed)
        let parent_id = "parent-001";
        let child_id = "child-002";
        let grandchild_id = "grandchild-003";

        setup_task_directory(&config, parent_id, "Parent instructions")
            .await
            .unwrap();
        setup_task_directory(&config, child_id, "Child instructions")
            .await
            .unwrap();
        setup_task_directory(&config, grandchild_id, "Grandchild instructions")
            .await
            .unwrap();

        let storage = ctx.task_storage();

        storage
            .add_task(Task {
                id: parent_id.to_string(),
                repo_root: repo_root.clone(),
                name: "parent-task".to_string(),
                branch_name: format!("tsk/{parent_id}"),
                status: TaskStatus::Complete,
                ..Task::test_default()
            })
            .await
            .unwrap();

        storage
            .add_task(Task {
                id: child_id.to_string(),
                repo_root: repo_root.clone(),
                name: "child-task".to_string(),
                branch_name: format!("tsk/{child_id}"),
                status: TaskStatus::Failed,
                parent_ids: vec![parent_id.to_string()],
                ..Task::test_default()
            })
            .await
            .unwrap();

        storage
            .add_task(Task {
                id: grandchild_id.to_string(),
                repo_root: repo_root.clone(),
                name: "grandchild-task".to_string(),
                branch_name: format!("tsk/{grandchild_id}"),
                status: TaskStatus::Failed,
                parent_ids: vec![child_id.to_string()],
                ..Task::test_default()
            })
            .await
            .unwrap();

        let task_manager = TaskManager::new(&ctx).unwrap();

        // Find descendants of parent - should return child then grandchild (BFS order)
        let descendants = task_manager.find_descendant_tasks(parent_id).await.unwrap();
        assert_eq!(descendants.len(), 2);
        assert_eq!(descendants[0].id, child_id);
        assert_eq!(descendants[1].id, grandchild_id);

        // Find descendants of grandchild - should return empty
        let descendants = task_manager
            .find_descendant_tasks(grandchild_id)
            .await
            .unwrap();
        assert!(descendants.is_empty());

        // Find descendants of non-existent task - should return empty
        let descendants = task_manager
            .find_descendant_tasks("non-existent")
            .await
            .unwrap();
        assert!(descendants.is_empty());
    }
}
