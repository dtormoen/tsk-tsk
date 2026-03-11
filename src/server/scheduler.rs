use crate::agent::{OAuthTokenStatus, check_oauth_token_validity};
use crate::context::AppContext;
use crate::context::TaskStorage;
use crate::context::docker_client::DockerClient;
use crate::docker::DockerManager;
use crate::git::RepoManager;
use crate::git_operations;
use crate::server::worker_pool::{AsyncJob, JobError, JobResult, WorkerPool};
use crate::task::{Task, TaskStatus};
use crate::task_manager::TaskManager;
use crate::task_runner::{TaskExecutionError, TaskRunner};
use crate::tui::events::{ServerEvent, ServerEventSender};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};

/// Represents the readiness status of a task's parent
#[derive(Debug)]
enum ParentStatus {
    /// Parent task is complete, includes the parent task for repo preparation
    Ready(Box<Task>),
    /// Parent task is still queued or running
    Waiting,
    /// Parent task failed
    Failed(String),
    /// Parent task was cancelled
    Cancelled,
    /// Parent task was not found in storage
    NotFound(String),
}

/// Retry interval for checking OAuth token validity when the token is expired.
const OAUTH_RETRY_INTERVAL: Duration = Duration::from_secs(60);

/// Task scheduler that manages task lifecycle and scheduling decisions
///
/// The scheduler is responsible for:
/// - Selecting tasks from storage for execution
/// - Managing task status transitions
/// - Handling warmup failure wait periods
/// - Proactively checking Claude OAuth token expiration before scheduling
/// - Delegating actual execution to the worker pool
/// - Preventing double-scheduling of tasks
pub struct TaskScheduler {
    context: Arc<AppContext>,
    docker_client: Arc<dyn DockerClient>,
    storage: Arc<TaskStorage>,
    running: Arc<Mutex<bool>>,
    warmup_failure_wait_until: Arc<Mutex<Option<Instant>>>,
    worker_pool: Option<Arc<WorkerPool<TaskJob>>>,
    /// Track task IDs that are currently submitted to the worker pool
    submitted_tasks: Arc<Mutex<HashSet<String>>>,
    quit_signal: Arc<tokio::sync::Notify>,
    quit_when_done: bool,
    last_auto_clean: Instant,
    /// When set, indicates OAuth token is expired and we should wait before retrying
    oauth_wait_until: Option<Instant>,
    /// Whether the "please login" notification has been shown for the current OAuth expiry
    oauth_notification_shown: bool,
    /// Optional channel for sending structured events to the TUI.
    /// When None, events are printed to stdout/stderr instead.
    event_sender: Option<ServerEventSender>,
}

impl TaskScheduler {
    /// Update terminal title based on worker pool status.
    fn update_terminal_title(&self) {
        if let Some(pool) = &self.worker_pool {
            let active = pool.active_workers();
            let total = pool.total_workers();

            if active == 0 {
                self.context
                    .terminal_operations()
                    .set_title(&format!("TSK Server Idle (0/{} workers)", total));
            } else {
                self.context.terminal_operations().set_title(&format!(
                    "TSK Server Running ({}/{} workers)",
                    active, total
                ));
            }
        }
    }

    /// Create a new task scheduler
    pub fn new(
        context: Arc<AppContext>,
        docker_client: Arc<dyn DockerClient>,
        storage: Arc<TaskStorage>,
        quit_when_done: bool,
        quit_signal: Arc<tokio::sync::Notify>,
        event_sender: Option<ServerEventSender>,
    ) -> Self {
        Self {
            context,
            docker_client,
            storage,
            running: Arc::new(Mutex::new(false)),
            warmup_failure_wait_until: Arc::new(Mutex::new(None)),
            worker_pool: None,
            submitted_tasks: Arc::new(Mutex::new(HashSet::new())),
            quit_signal,
            quit_when_done,
            last_auto_clean: Instant::now() - Duration::from_secs(3600),
            oauth_wait_until: None,
            oauth_notification_shown: false,
            event_sender,
        }
    }

    /// Send a structured event through the event channel, or fall back to stdout/stderr
    fn emit(&self, event: ServerEvent) {
        crate::tui::events::emit_or_print(&self.event_sender, event);
    }

    /// Check if a task's parent is ready (complete or non-existent).
    ///
    /// Returns:
    /// - `None` if the task has no parent
    /// - `Some(Ready(parent_task))` if parent is complete
    /// - `Some(Waiting)` if parent is still queued or running
    /// - `Some(Failed(msg))` if parent failed
    /// - `Some(NotFound(id))` if parent doesn't exist in storage
    fn is_parent_ready(task: &Task, all_tasks: &[Task]) -> Option<ParentStatus> {
        let parent_id = task.parent_ids.first()?;

        // Find the parent task
        let parent_task = all_tasks.iter().find(|t| &t.id == parent_id);

        match parent_task {
            None => Some(ParentStatus::NotFound(parent_id.clone())),
            Some(parent) => match parent.status {
                TaskStatus::Complete => Some(ParentStatus::Ready(Box::new(parent.clone()))),
                TaskStatus::Failed => Some(ParentStatus::Failed(format!(
                    "Parent task {} failed",
                    parent_id
                ))),
                TaskStatus::Cancelled => Some(ParentStatus::Cancelled),
                TaskStatus::Queued | TaskStatus::Running => Some(ParentStatus::Waiting),
            },
        }
    }

    /// Prepare a child task for scheduling by copying the repository from the parent task.
    ///
    /// Copies the repo, then persists the updated fields (`copied_repo_path`, `source_commit`,
    /// `source_branch`) via `storage.prepare_child_task`. Returns the authoritative DB row.
    async fn prepare_child_task(&self, task: &Task, parent_task: &Task) -> Result<Task, String> {
        // Get the parent task's copied repo path
        let parent_repo_path = parent_task
            .copied_repo_path
            .as_ref()
            .ok_or_else(|| format!("Parent task {} has no copied_repo_path", parent_task.id))?;

        // Get the HEAD commit from the parent's repo
        let source_commit: String = git_operations::get_current_commit(parent_repo_path)
            .await
            .map_err(|e| format!("Failed to get parent HEAD commit: {e}"))?;

        // Copy the repository from the parent task's folder
        let repo_manager = RepoManager::new(&self.context);
        let copy_result = repo_manager
            .copy_repo(
                &task.id,
                parent_repo_path,
                Some(&source_commit),
                &task.branch_name,
            )
            .await
            .map_err(|e| format!("Failed to copy repo from parent task: {e}"))?;
        let copied_repo_path = copy_result.repo_path;
        for warning in copy_result.warnings {
            self.emit(ServerEvent::WarningMessage(warning));
        }

        // Persist the updated fields and return the authoritative DB row.
        // source_branch is set to the parent's branch name for git-town integration.
        // Copy resolved_config from the parent so execution uses the original
        // config snapshot (prevents agents from modifying .tsk/tsk.toml to
        // loosen security for children).
        self.storage
            .prepare_child_task(
                &task.id,
                copied_repo_path,
                &source_commit,
                &parent_task.branch_name,
                parent_task.resolved_config.as_deref(),
            )
            .await
            .map_err(|e| format!("Failed to update prepared task in storage: {e}"))
    }

    /// Cascade terminal status to child tasks when their parent fails or is cancelled.
    ///
    /// When a parent task fails, children are marked as FAILED.
    /// When a parent task is cancelled, children are marked as CANCELLED.
    async fn cascade_to_child_tasks(
        &self,
        parent_task_id: &str,
        parent_status: &TaskStatus,
        all_tasks: &[Task],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let child_tasks: Vec<&Task> = all_tasks
            .iter()
            .filter(|t| t.parent_ids.contains(&parent_task_id.to_string()))
            .collect();

        for task in child_tasks {
            match parent_status {
                TaskStatus::Cancelled => {
                    self.emit(ServerEvent::WarningMessage(format!(
                        "Marking task {} as cancelled due to parent task cancellation",
                        task.id
                    )));
                    // mark_cancelled only works on RUNNING/QUEUED, which is what children should be
                    let _ = self.storage.mark_cancelled(&task.id).await;
                }
                _ => {
                    self.emit(ServerEvent::WarningMessage(format!(
                        "Marking task {} as failed due to parent task failure",
                        task.id
                    )));
                    // Guarded: mark_failed is a no-op if task is already terminal
                    let _ = self
                        .storage
                        .mark_failed(&task.id, &format!("Parent task {} failed", parent_task_id))
                        .await;
                }
            }
        }

        Ok(())
    }

    /// Check if a task is ready for scheduling.
    ///
    /// A task is ready if:
    /// - It is in Queued status
    /// - It is not already submitted
    /// - It has no parent, OR its parent is complete
    fn is_task_ready_for_scheduling(
        task: &Task,
        all_tasks: &[Task],
        submitted: &HashSet<String>,
    ) -> bool {
        // Must be queued and not already submitted
        if task.status != TaskStatus::Queued || submitted.contains(&task.id) {
            return false;
        }

        // Check parent status
        match Self::is_parent_ready(task, all_tasks) {
            None => true,                             // No parent
            Some(ParentStatus::Ready(_)) => true,     // Parent complete
            Some(ParentStatus::Waiting) => false,     // Still waiting
            Some(ParentStatus::Failed(_)) => false,   // Will be failed by cascade
            Some(ParentStatus::Cancelled) => false,   // Will be cancelled by cascade
            Some(ParentStatus::NotFound(_)) => false, // Will be failed by handler
        }
    }

    /// Checks whether the Claude OAuth token is valid for scheduling.
    ///
    /// If the task uses the Claude agent, reads the OAuth credentials and
    /// verifies the token won't expire within 5 minutes. When the token is
    /// expired or expiring, shows a notification (once per expiry episode)
    /// and sets a 1-minute retry wait. Non-Claude tasks are always allowed.
    ///
    /// Returns `true` if the task can proceed, `false` if it should be skipped.
    fn check_oauth_for_task(&mut self, task: &Task) -> bool {
        if task.agent != "claude" {
            return true;
        }

        // If we're in an OAuth wait period, skip scheduling
        if let Some(wait_until) = self.oauth_wait_until {
            if Instant::now() < wait_until {
                return false;
            }
            // Wait period has elapsed, clear it and re-check the token
            self.oauth_wait_until = None;
        }

        match check_oauth_token_validity() {
            Ok(OAuthTokenStatus::Valid) => true,
            Ok(OAuthTokenStatus::ExpiredOrExpiring) => {
                if !self.oauth_notification_shown {
                    self.context.notification_client().notify(
                        "TSK: Claude Token Expired",
                        "Claude OAuth token is expired or expiring soon. \
                         Run `claude /login` to refresh.",
                    );
                    self.oauth_notification_shown = true;
                }
                self.emit(ServerEvent::WarningMessage(format!(
                    "Claude OAuth token is expired or expiring soon. \
                     Please run `claude /login` to refresh your token. \
                     Trying again in {} seconds.",
                    OAUTH_RETRY_INTERVAL.as_secs()
                )));
                self.oauth_wait_until = Some(Instant::now() + OAUTH_RETRY_INTERVAL);
                false
            }
            Err(e) => {
                // If we can't read the credentials file, allow scheduling to proceed.
                // The agent's own warmup/validate will handle actual auth errors.
                self.emit(ServerEvent::WarningMessage(format!(
                    "Warning: Could not check OAuth token: {e}"
                )));
                true
            }
        }
    }

    /// Start the scheduler and begin processing tasks
    pub async fn start(
        &mut self,
        workers: u32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut running = self.running.lock().await;
        if *running {
            return Err("Scheduler is already running".into());
        }
        *running = true;
        drop(running);

        self.emit(ServerEvent::StatusMessage(format!(
            "Task scheduler started with {} worker(s)",
            workers
        )));

        if self.quit_when_done {
            self.emit(ServerEvent::StatusMessage(
                "Running in quit-when-done mode - will exit when queue is empty".to_string(),
            ));
        }

        // Create the worker pool
        let worker_pool = Arc::new(WorkerPool::<TaskJob>::new(workers as usize));
        self.worker_pool = Some(worker_pool.clone());

        // Set initial idle title
        self.update_terminal_title();

        // Check if we should quit immediately due to empty queue
        if self.quit_when_done {
            let tasks = self.storage.list_tasks().await?;

            let queued_count = tasks
                .iter()
                .filter(|t| t.status == TaskStatus::Queued)
                .count();

            if queued_count == 0 {
                self.emit(ServerEvent::StatusMessage(
                    "Queue is empty at startup. Exiting immediately...".to_string(),
                ));
                self.quit_signal.notify_one();
                self.stop().await;
                // Loop will check running flag and exit immediately
            }
        }

        // Main scheduling loop
        loop {
            // Check if we should continue running
            if !*self.running.lock().await {
                self.emit(ServerEvent::StatusMessage(
                    "Task scheduler stopping, waiting for active tasks to complete...".to_string(),
                ));

                // Shutdown the worker pool and wait for all tasks
                if let Some(pool) = &self.worker_pool {
                    let _ = pool.shutdown().await;
                }

                self.context.terminal_operations().restore_title();
                break;
            }

            // Auto-clean old completed/failed tasks every hour
            const AUTO_CLEAN_INTERVAL: Duration = Duration::from_secs(3600);
            let server_config = &self.context.tsk_config().server;
            if server_config.auto_clean_enabled
                && self.last_auto_clean.elapsed() >= AUTO_CLEAN_INTERVAL
            {
                self.last_auto_clean = Instant::now();
                let min_age = server_config.auto_clean_min_age();
                let task_manager = TaskManager::new(&self.context);
                match task_manager {
                    Ok(tm) => match tm.clean_tasks(true, Some(min_age)).await {
                        Ok(result) if result.deleted > 0 => {
                            self.emit(ServerEvent::StatusMessage(format!(
                                "Auto-clean: removed {} old task(s) ({} skipped)",
                                result.deleted, result.skipped
                            )));
                        }
                        Err(e) => {
                            self.emit(ServerEvent::WarningMessage(format!(
                                "Auto-clean failed: {}",
                                e
                            )));
                        }
                        _ => {}
                    },
                    Err(e) => {
                        self.emit(ServerEvent::WarningMessage(format!(
                            "Auto-clean: failed to create task manager: {}",
                            e
                        )));
                    }
                }
            }

            // Update terminal title to reflect current worker state
            self.update_terminal_title();

            // Poll for completed jobs and process results
            if let Some(pool) = &self.worker_pool {
                let completed_jobs = pool.poll_completed().await;
                for job_result in completed_jobs {
                    match job_result {
                        Ok(result) => {
                            // Remove from submitted tasks
                            self.submitted_tasks.lock().await.remove(&result.job_id);

                            let task_name = match self.storage.get_task(&result.job_id).await {
                                Ok(Some(t)) => t.name,
                                _ => result.job_id.clone(),
                            };

                            if result.success {
                                self.emit(ServerEvent::TaskCompleted {
                                    task_id: result.job_id.clone(),
                                    task_name,
                                });
                                // Re-enable OAuth expiry notifications after a successful task
                                self.oauth_notification_shown = false;
                            } else if let Some(msg) = &result.message {
                                self.emit(ServerEvent::TaskFailed {
                                    task_id: result.job_id.clone(),
                                    task_name,
                                    error: msg.clone(),
                                });
                                // Handle cascading failures for child tasks
                                let tasks = self.storage.list_tasks().await?;
                                let failed_task = tasks.iter().find(|t| t.id == result.job_id);
                                let parent_status = failed_task
                                    .map(|t| &t.status)
                                    .unwrap_or(&TaskStatus::Failed);
                                self.cascade_to_child_tasks(&result.job_id, parent_status, &tasks)
                                    .await?;
                            }
                        }
                        Err(e) => {
                            self.emit(ServerEvent::WarningMessage(format!("Job error: {}", e)));
                        }
                    }
                }
            }

            // Check if we're in a warmup failure wait period
            let wait_until = self.warmup_failure_wait_until.lock().await;
            if let Some(wait_instant) = *wait_until {
                if Instant::now() < wait_instant {
                    let remaining = wait_instant - Instant::now();
                    drop(wait_until);
                    self.emit(ServerEvent::StatusMessage(format!(
                        "Waiting {} seconds due to warmup failure before attempting new tasks...",
                        remaining.as_secs()
                    )));
                    // Sleep for the minimum of remaining time or 60 seconds
                    let sleep_duration = std::cmp::min(remaining, Duration::from_secs(60));
                    sleep(sleep_duration).await;
                    continue;
                }
                drop(wait_until);
                // Clear the wait period
                *self.warmup_failure_wait_until.lock().await = None;
                self.emit(ServerEvent::StatusMessage(
                    "Warmup failure wait period has ended, resuming task processing".to_string(),
                ));
            } else {
                drop(wait_until);
            }

            // Try to schedule a new task if workers are available
            let has_available_workers = self
                .worker_pool
                .as_ref()
                .is_some_and(|p| p.available_workers() > 0);

            if has_available_workers {
                // Look for a queued task that isn't already submitted and has a ready parent
                let tasks = self.storage.list_tasks().await?;

                let submitted = self.submitted_tasks.lock().await.clone();

                // First, handle tasks with missing or failed parents
                for task in &tasks {
                    if task.status != TaskStatus::Queued || submitted.contains(&task.id) {
                        continue;
                    }

                    match Self::is_parent_ready(task, &tasks) {
                        Some(ParentStatus::NotFound(pid)) => {
                            // Mark task as failed - parent doesn't exist
                            self.emit(ServerEvent::WarningMessage(format!(
                                "Task {} has missing parent {}, marking as failed",
                                task.id, pid
                            )));
                            let _ = self
                                .storage
                                .mark_failed(&task.id, &format!("Parent task {} not found", pid))
                                .await;
                        }
                        Some(ParentStatus::Failed(msg)) => {
                            // Mark task as failed - parent failed (cascade)
                            self.emit(ServerEvent::WarningMessage(format!(
                                "Task {} has failed parent, marking as failed",
                                task.id
                            )));
                            let _ = self.storage.mark_failed(&task.id, &msg).await;
                        }
                        Some(ParentStatus::Cancelled) => {
                            // Mark task as cancelled - parent was cancelled (cascade)
                            self.emit(ServerEvent::WarningMessage(format!(
                                "Task {} has cancelled parent, marking as cancelled",
                                task.id
                            )));
                            let _ = self.storage.mark_cancelled(&task.id).await;
                        }
                        _ => {}
                    }
                }

                // Re-fetch tasks after potential status changes
                let tasks = self.storage.list_tasks().await?;

                // Find the first task ready for scheduling
                let queued_task = tasks
                    .iter()
                    .find(|t| Self::is_task_ready_for_scheduling(t, &tasks, &submitted))
                    .cloned();

                if let Some(mut task) = queued_task {
                    // Proactively check OAuth token validity for Claude tasks
                    if !self.check_oauth_for_task(&task) {
                        // Token is expired/expiring; task stays queued
                        continue;
                    }

                    // Check if this is a child task that needs repo preparation
                    if !task.parent_ids.is_empty() && task.copied_repo_path.is_none() {
                        // Find the parent task
                        if let Some(ParentStatus::Ready(parent_task)) =
                            Self::is_parent_ready(&task, &tasks)
                        {
                            self.emit(ServerEvent::StatusMessage(format!(
                                "Preparing {} from parent {}",
                                task.id, parent_task.id
                            )));
                            match self.prepare_child_task(&task, &parent_task).await {
                                Ok(prepared_task) => {
                                    task = prepared_task;
                                }
                                Err(e) => {
                                    self.emit(ServerEvent::WarningMessage(format!(
                                        "Failed to prepare child task {}: {}",
                                        task.id, e
                                    )));
                                    let _ = self
                                        .storage
                                        .mark_failed(
                                            &task.id,
                                            &format!("Failed to prepare repository: {}", e),
                                        )
                                        .await;
                                    continue;
                                }
                            }
                        }
                    }

                    self.emit(ServerEvent::TaskScheduled {
                        task_id: task.id.clone(),
                        task_name: task.name.clone(),
                    });

                    // Update task status to running
                    let running_task = match self.storage.mark_running(&task.id).await {
                        Ok(t) => t,
                        Err(e) => {
                            self.emit(ServerEvent::WarningMessage(format!(
                                "Error marking task as running: {e}"
                            )));
                            continue;
                        }
                    };

                    // Mark task as submitted
                    self.submitted_tasks
                        .lock()
                        .await
                        .insert(running_task.id.clone());

                    // Create and submit the job
                    let job = TaskJob {
                        task: running_task,
                        context: self.context.clone(),
                        docker_client: self.docker_client.clone(),
                        storage: self.storage.clone(),
                        warmup_failure_wait_until: self.warmup_failure_wait_until.clone(),
                        event_sender: self.event_sender.clone(),
                    };

                    // pool is guaranteed to exist since has_available_workers was true
                    let pool = self.worker_pool.as_ref().unwrap();
                    match pool.try_submit(job).await {
                        Ok(Some(_handle)) => {
                            // Job submitted successfully
                        }
                        Ok(None) => {
                            // No workers available (shouldn't happen since we checked)
                            self.emit(ServerEvent::WarningMessage(
                                "Failed to submit job: no workers available".to_string(),
                            ));
                            // Remove from submitted tasks since it didn't actually submit
                            self.submitted_tasks.lock().await.remove(&task.id);

                            // Revert task status
                            let _ = self.storage.reset_to_queued(&task.id).await;
                        }
                        Err(e) => {
                            self.emit(ServerEvent::WarningMessage(format!(
                                "Failed to submit job: {}",
                                e
                            )));
                            // Remove from submitted tasks since it didn't actually submit
                            self.submitted_tasks.lock().await.remove(&task.id);

                            // Revert task status
                            let _ = self.storage.reset_to_queued(&task.id).await;
                        }
                    }
                }
            }

            // Check if we should quit when done
            if self.quit_when_done {
                // Get current task list
                let tasks = self.storage.list_tasks().await?;

                // Count queued tasks
                let queued_count = tasks
                    .iter()
                    .filter(|t| t.status == TaskStatus::Queued)
                    .count();

                // Check: no queued tasks AND no active workers AND not in warmup wait
                let active_workers = if let Some(pool) = &self.worker_pool {
                    pool.active_workers()
                } else {
                    0
                };

                let in_warmup_wait = self.warmup_failure_wait_until.lock().await.is_some();
                let in_oauth_wait = self.oauth_wait_until.is_some();

                if queued_count == 0 && active_workers == 0 && !in_warmup_wait && !in_oauth_wait {
                    self.emit(ServerEvent::StatusMessage(
                        "Queue is empty and no workers active. Shutting down...".to_string(),
                    ));
                    self.quit_signal.notify_one();
                    self.stop().await;
                    // Loop will exit on next iteration when running becomes false
                }
            }

            // Sleep briefly before next scheduling check
            sleep(Duration::from_millis(100)).await;
        }

        Ok(())
    }

    /// Stop the scheduler
    pub async fn stop(&self) {
        *self.running.lock().await = false;
    }

    /// Get a handle to the running flag, allowing external code to stop the scheduler
    pub fn running_flag(&self) -> Arc<Mutex<bool>> {
        self.running.clone()
    }

    /// Get a clone of the submitted task IDs Arc for external use
    pub fn submitted_task_ids(&self) -> Arc<Mutex<HashSet<String>>> {
        self.submitted_tasks.clone()
    }
}

/// Job implementation for executing tasks
pub struct TaskJob {
    task: Task,
    context: Arc<AppContext>,
    docker_client: Arc<dyn DockerClient>,
    storage: Arc<TaskStorage>,
    warmup_failure_wait_until: Arc<Mutex<Option<Instant>>>,
    event_sender: Option<ServerEventSender>,
}

impl AsyncJob for TaskJob {
    async fn execute(self) -> Result<JobResult, JobError> {
        let result = Self::execute_single_task(
            &self.context,
            self.docker_client.clone(),
            &self.task,
            self.event_sender.clone(),
        )
        .await;

        match result {
            Ok(_) => {
                // Task completed successfully
                Ok(JobResult {
                    job_id: self.task.id.clone(),
                    success: true,
                    message: Some(format!("Task {} completed successfully", self.task.name)),
                })
            }
            Err(e) => {
                // Check if this was a warmup failure
                if e.is_warmup_failure {
                    crate::tui::events::emit_or_print(
                        &self.event_sender,
                        ServerEvent::StatusMessage(format!(
                            "Task {} failed during warmup. Setting 1-hour wait period...",
                            self.task.id
                        )),
                    );

                    // Set the wait period
                    let wait_until = Instant::now() + Duration::from_secs(3600); // 1 hour
                    *self.warmup_failure_wait_until.lock().await = Some(wait_until);

                    // Reset task status to QUEUED so it can be retried
                    if let Err(reset_err) = self.storage.reset_to_queued(&self.task.id).await {
                        crate::tui::events::emit_or_print(
                            &self.event_sender,
                            ServerEvent::WarningMessage(format!(
                                "Failed to reset task status to QUEUED: {reset_err}"
                            )),
                        );
                    }
                }

                Ok(JobResult {
                    job_id: self.task.id.clone(),
                    success: false,
                    message: Some(e.message),
                })
            }
        }
    }

    fn job_id(&self) -> String {
        self.task.id.clone()
    }
}

impl TaskJob {
    /// Execute a single task
    async fn execute_single_task(
        context: &AppContext,
        docker_client: Arc<dyn DockerClient>,
        task: &Task,
        event_sender: Option<ServerEventSender>,
    ) -> Result<(), TaskExecutionError> {
        let docker_manager = DockerManager::new(context, docker_client, event_sender.clone());
        let task_runner = TaskRunner::new(context, docker_manager, event_sender);
        task_runner.run_queued(task).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{Task, TaskStatus};
    use crate::test_utils::NoOpDockerClient;
    use crate::test_utils::git_test_utils::TestGitRepository;

    fn test_docker_client() -> Arc<dyn DockerClient> {
        Arc::new(NoOpDockerClient)
    }

    /// Wait for tasks to reach a certain state with timeout.
    ///
    /// Polls the storage periodically to check if the condition is met.
    /// Returns true if condition met, false if timeout reached.
    async fn wait_for_condition<F>(
        storage: &Arc<TaskStorage>,
        timeout_duration: Duration,
        mut condition: F,
    ) -> bool
    where
        F: FnMut(&[Task]) -> bool,
    {
        let deadline = Instant::now() + timeout_duration;

        while Instant::now() < deadline {
            let tasks = storage.list_tasks().await.unwrap();

            if condition(&tasks) {
                return true;
            }

            // Small delay to avoid busy waiting
            sleep(Duration::from_millis(50)).await;
        }

        false
    }

    /// Create a standard test task with given ID.
    fn create_test_task(
        id: &str,
        repo_path: &std::path::Path,
        commit_sha: &str,
        data_dir: &std::path::Path,
    ) -> Task {
        Task {
            id: id.to_string(),
            repo_root: repo_path.to_path_buf(),
            name: format!("task-{id}"),
            task_type: "test".to_string(),
            branch_name: format!("tsk/test/{id}"),
            source_commit: commit_sha.to_string(),
            copied_repo_path: Some(data_dir.join(format!("task-copy-{id}"))),
            ..Task::test_default()
        }
    }

    /// Setup test repository with instructions file.
    fn setup_test_repo() -> Result<(TestGitRepository, String), Box<dyn std::error::Error>> {
        let test_repo = TestGitRepository::new()?;
        test_repo.init_with_commit()?;
        test_repo.create_file("instructions.md", "Test instructions")?;
        test_repo.stage_all()?;
        let commit_sha = test_repo.commit("Add instructions")?;
        Ok((test_repo, commit_sha))
    }

    #[tokio::test]
    async fn test_scheduler_lifecycle() {
        let ctx = AppContext::builder().build();
        let storage = ctx.task_storage();

        let quit_signal = Arc::new(tokio::sync::Notify::new());
        let mut scheduler = TaskScheduler::new(
            Arc::new(ctx),
            test_docker_client(),
            storage,
            false,
            quit_signal,
            None,
        );

        // Test that scheduler starts as not running
        assert!(!*scheduler.running.lock().await);

        // Start scheduler in background with 1 worker
        let sched_handle = tokio::spawn(async move {
            let _ = scheduler.start(1).await;
        });

        // Give the scheduler time to start
        sleep(Duration::from_millis(100)).await;

        // Stop scheduler by aborting the task
        sched_handle.abort();
        let _ = sched_handle.await;
    }

    #[tokio::test]
    async fn test_scheduler_processes_tasks() {
        // Test that scheduler processes tasks without deadlock and can handle multiple tasks
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();

        let (test_repo, commit_sha) = setup_test_repo().unwrap();

        // Create two tasks
        let task1 = create_test_task("task-1", test_repo.path(), &commit_sha, tsk_env.data_dir());
        let task2 = create_test_task("task-2", test_repo.path(), &commit_sha, tsk_env.data_dir());

        // Set up storage with the first task
        let storage = ctx.task_storage();
        storage.add_task(task1.clone()).await.unwrap();

        let quit_signal = Arc::new(tokio::sync::Notify::new());
        let mut scheduler = TaskScheduler::new(
            Arc::new(ctx),
            test_docker_client(),
            storage.clone(),
            false,
            quit_signal,
            None,
        );

        // Start scheduler in background with 1 worker
        let sched_handle = tokio::spawn(async move {
            let _ = scheduler.start(1).await;
        });

        // Wait for first task to complete (not be stuck in RUNNING)
        let task1_processed = wait_for_condition(&storage, Duration::from_secs(5), |tasks| {
            tasks
                .iter()
                .find(|t| t.id == task1.id)
                .map(|t| t.status != TaskStatus::Queued && t.status != TaskStatus::Running)
                .unwrap_or(false)
        })
        .await;

        assert!(
            task1_processed,
            "First task should be processed within timeout"
        );

        // Add second task to verify scheduler continues processing
        storage.add_task(task2.clone()).await.unwrap();

        // Wait for second task to start processing (shows scheduler isn't deadlocked)
        let task2_started = wait_for_condition(&storage, Duration::from_secs(5), |tasks| {
            tasks
                .iter()
                .find(|t| t.id == task2.id)
                .map(|t| t.status != TaskStatus::Queued)
                .unwrap_or(false)
        })
        .await;

        assert!(
            task2_started,
            "Second task should start processing, indicating no deadlock"
        );

        // Stop scheduler by dropping handle (task will abort)
        sched_handle.abort();
        let _ = sched_handle.await;
    }

    #[tokio::test]
    async fn test_warmup_failure_wait_behavior() {
        // Test that the scheduler properly handles warmup failure wait periods
        let ctx = Arc::new(AppContext::builder().build());

        let storage = ctx.task_storage();

        let quit_signal = Arc::new(tokio::sync::Notify::new());
        let scheduler =
            TaskScheduler::new(ctx, test_docker_client(), storage, false, quit_signal, None);

        // Initially no wait period
        assert!(
            scheduler.warmup_failure_wait_until.lock().await.is_none(),
            "Should start with no wait period"
        );

        // Set a wait period in the future
        let future_time = Instant::now() + Duration::from_secs(3600);
        *scheduler.warmup_failure_wait_until.lock().await = Some(future_time);

        // Verify wait period is set and in the future
        let wait_until = *scheduler.warmup_failure_wait_until.lock().await;
        assert!(wait_until.is_some(), "Wait period should be set");
        assert!(
            wait_until.unwrap() > Instant::now(),
            "Wait period should be in the future"
        );

        // Clear the wait period
        *scheduler.warmup_failure_wait_until.lock().await = None;

        // Verify it's cleared
        assert!(
            scheduler.warmup_failure_wait_until.lock().await.is_none(),
            "Wait period should be cleared"
        );
    }

    #[tokio::test]
    async fn test_scheduler_prevents_double_scheduling() {
        // Test that the scheduler doesn't schedule the same task twice
        let ctx = AppContext::builder().build();
        let storage = ctx.task_storage();
        let quit_signal = Arc::new(tokio::sync::Notify::new());
        let scheduler = TaskScheduler::new(
            Arc::new(ctx),
            test_docker_client(),
            storage,
            false,
            quit_signal,
            None,
        );

        // Add a task ID to submitted tasks
        scheduler
            .submitted_tasks
            .lock()
            .await
            .insert("task-1".to_string());

        // Verify it's tracked
        assert!(scheduler.submitted_tasks.lock().await.contains("task-1"));

        // Remove it
        scheduler.submitted_tasks.lock().await.remove("task-1");

        // Verify it's removed
        assert!(!scheduler.submitted_tasks.lock().await.contains("task-1"));
    }

    #[tokio::test]
    async fn test_worker_pool_count_tracking() {
        // Test that the worker pool properly tracks active and available workers
        use crate::server::worker_pool::{AsyncJob, JobError, JobResult, WorkerPool};

        // Simple test job for worker pool testing
        struct SimpleJob {
            id: String,
            should_succeed: bool,
        }

        impl AsyncJob for SimpleJob {
            async fn execute(self) -> Result<JobResult, JobError> {
                sleep(Duration::from_millis(10)).await;
                if self.should_succeed {
                    Ok(JobResult {
                        job_id: self.id,
                        success: true,
                        message: Some("Success".to_string()),
                    })
                } else {
                    Ok(JobResult {
                        job_id: self.id,
                        success: false,
                        message: Some("Failed".to_string()),
                    })
                }
            }

            fn job_id(&self) -> String {
                self.id.clone()
            }
        }

        // Create a worker pool with 3 workers
        let pool = WorkerPool::new(3);

        // Initially all workers should be available
        assert_eq!(pool.total_workers(), 3);
        assert_eq!(pool.available_workers(), 3);
        assert_eq!(pool.active_workers(), 0);

        // Submit 2 jobs
        let job1 = SimpleJob {
            id: "job-1".to_string(),
            should_succeed: true,
        };
        let job2 = SimpleJob {
            id: "job-2".to_string(),
            should_succeed: false,
        };

        pool.try_submit(job1).await.unwrap().unwrap();
        pool.try_submit(job2).await.unwrap().unwrap();

        // Give jobs a moment to start
        sleep(Duration::from_millis(5)).await;

        // Check counts with 2 active jobs
        assert_eq!(pool.total_workers(), 3);
        assert_eq!(pool.available_workers(), 1);
        assert_eq!(pool.active_workers(), 2);

        // Wait for jobs to complete
        sleep(Duration::from_millis(20)).await;

        // Poll completed jobs (this releases the permits)
        let completed = pool.poll_completed().await;
        assert_eq!(completed.len(), 2);

        // All workers should be available again
        assert_eq!(pool.total_workers(), 3);
        assert_eq!(pool.available_workers(), 3);
        assert_eq!(pool.active_workers(), 0);
    }

    /// Create a test task with a parent.
    fn create_child_task(
        id: &str,
        repo_path: &std::path::Path,
        commit_sha: &str,
        parent_id: &str,
    ) -> Task {
        Task {
            id: id.to_string(),
            repo_root: repo_path.to_path_buf(),
            name: format!("task-{id}"),
            task_type: "test".to_string(),
            branch_name: format!("tsk/test/{id}"),
            source_commit: commit_sha.to_string(),
            source_branch: None,
            stack: "default".to_string(),
            project: "default".to_string(),
            copied_repo_path: None,
            parent_ids: vec![parent_id.to_string()],
            ..Task::test_default()
        }
    }

    #[test]
    fn test_is_parent_ready_no_parent() {
        // Task with no parent should return None
        let task = Task {
            id: "task-1".to_string(),
            branch_name: "tsk/test/task-1".to_string(),
            ..Task::test_default()
        };

        let all_tasks = vec![task.clone()];
        let result = TaskScheduler::is_parent_ready(&task, &all_tasks);
        assert!(result.is_none(), "Task with no parent should return None");
    }

    #[test]
    fn test_is_parent_ready_complete() {
        // Create a completed parent task
        let parent_task = Task {
            id: "parent-1".to_string(),
            name: "parent-task".to_string(),
            branch_name: "tsk/test/parent-1".to_string(),
            status: TaskStatus::Complete,
            ..Task::test_default()
        };

        // Create a child task
        let child_task = Task {
            id: "child-1".to_string(),
            name: "child-task".to_string(),
            branch_name: "tsk/test/child-1".to_string(),
            source_branch: None,
            copied_repo_path: None,
            parent_ids: vec!["parent-1".to_string()],
            ..Task::test_default()
        };

        let all_tasks = vec![parent_task.clone(), child_task.clone()];
        let result = TaskScheduler::is_parent_ready(&child_task, &all_tasks);

        match result {
            Some(ParentStatus::Ready(parent)) => {
                assert_eq!(parent.id, "parent-1");
            }
            _ => panic!("Expected ParentStatus::Ready, got {:?}", result),
        }
    }

    #[test]
    fn test_is_parent_ready_waiting() {
        // Create a running parent task
        let parent_task = Task {
            id: "parent-1".to_string(),
            name: "parent-task".to_string(),
            branch_name: "tsk/test/parent-1".to_string(),
            status: TaskStatus::Running,
            ..Task::test_default()
        };

        // Create a child task
        let child_task = Task {
            id: "child-1".to_string(),
            name: "child-task".to_string(),
            branch_name: "tsk/test/child-1".to_string(),
            source_branch: None,
            copied_repo_path: None,
            parent_ids: vec!["parent-1".to_string()],
            ..Task::test_default()
        };

        let all_tasks = vec![parent_task.clone(), child_task.clone()];
        let result = TaskScheduler::is_parent_ready(&child_task, &all_tasks);

        assert!(
            matches!(result, Some(ParentStatus::Waiting)),
            "Expected ParentStatus::Waiting, got {:?}",
            result
        );
    }

    #[test]
    fn test_is_parent_ready_failed() {
        // Create a failed parent task
        let parent_task = Task {
            id: "parent-1".to_string(),
            name: "parent-task".to_string(),
            branch_name: "tsk/test/parent-1".to_string(),
            status: TaskStatus::Failed,
            ..Task::test_default()
        };

        // Create a child task
        let child_task = Task {
            id: "child-1".to_string(),
            name: "child-task".to_string(),
            branch_name: "tsk/test/child-1".to_string(),
            source_branch: None,
            copied_repo_path: None,
            parent_ids: vec!["parent-1".to_string()],
            ..Task::test_default()
        };

        let all_tasks = vec![parent_task.clone(), child_task.clone()];
        let result = TaskScheduler::is_parent_ready(&child_task, &all_tasks);

        match result {
            Some(ParentStatus::Failed(msg)) => {
                assert!(
                    msg.contains("parent-1"),
                    "Error message should mention parent task ID"
                );
            }
            _ => panic!("Expected ParentStatus::Failed, got {:?}", result),
        }
    }

    #[test]
    fn test_is_parent_ready_not_found() {
        // Create a child task with non-existent parent
        let child_task = Task {
            id: "child-1".to_string(),
            name: "child-task".to_string(),
            branch_name: "tsk/test/child-1".to_string(),
            source_branch: None,
            copied_repo_path: None,
            parent_ids: vec!["nonexistent-parent".to_string()],
            ..Task::test_default()
        };

        let all_tasks = vec![child_task.clone()];
        let result = TaskScheduler::is_parent_ready(&child_task, &all_tasks);

        match result {
            Some(ParentStatus::NotFound(id)) => {
                assert_eq!(id, "nonexistent-parent");
            }
            _ => panic!("Expected ParentStatus::NotFound, got {:?}", result),
        }
    }

    #[test]
    fn test_is_task_ready_for_scheduling_no_parent() {
        // Task with no parent should be ready
        let task = Task {
            id: "task-1".to_string(),
            branch_name: "tsk/test/task-1".to_string(),
            ..Task::test_default()
        };

        let all_tasks = vec![task.clone()];
        let submitted = HashSet::new();
        let result = TaskScheduler::is_task_ready_for_scheduling(&task, &all_tasks, &submitted);
        assert!(result, "Task with no parent should be ready");
    }

    #[test]
    fn test_is_task_ready_for_scheduling_already_submitted() {
        // Task that's already submitted should not be ready
        let task = Task {
            id: "task-1".to_string(),
            branch_name: "tsk/test/task-1".to_string(),
            ..Task::test_default()
        };

        let all_tasks = vec![task.clone()];
        let mut submitted = HashSet::new();
        submitted.insert("task-1".to_string());
        let result = TaskScheduler::is_task_ready_for_scheduling(&task, &all_tasks, &submitted);
        assert!(!result, "Already submitted task should not be ready");
    }

    #[test]
    fn test_is_task_ready_for_scheduling_parent_waiting() {
        // Task with running parent should not be ready
        let parent_task = Task {
            id: "parent-1".to_string(),
            name: "parent-task".to_string(),
            branch_name: "tsk/test/parent-1".to_string(),
            status: TaskStatus::Running,
            ..Task::test_default()
        };

        let child_task = Task {
            id: "child-1".to_string(),
            name: "child-task".to_string(),
            branch_name: "tsk/test/child-1".to_string(),
            source_branch: None,
            copied_repo_path: None,
            parent_ids: vec!["parent-1".to_string()],
            ..Task::test_default()
        };

        let all_tasks = vec![parent_task.clone(), child_task.clone()];
        let submitted = HashSet::new();
        let result =
            TaskScheduler::is_task_ready_for_scheduling(&child_task, &all_tasks, &submitted);
        assert!(!result, "Task with running parent should not be ready");
    }

    #[test]
    fn test_is_task_ready_for_scheduling_parent_complete() {
        // Task with completed parent should be ready
        let parent_task = Task {
            id: "parent-1".to_string(),
            name: "parent-task".to_string(),
            branch_name: "tsk/test/parent-1".to_string(),
            status: TaskStatus::Complete,
            ..Task::test_default()
        };

        let child_task = Task {
            id: "child-1".to_string(),
            name: "child-task".to_string(),
            branch_name: "tsk/test/child-1".to_string(),
            source_branch: None,
            copied_repo_path: None,
            parent_ids: vec!["parent-1".to_string()],
            ..Task::test_default()
        };

        let all_tasks = vec![parent_task.clone(), child_task.clone()];
        let submitted = HashSet::new();
        let result =
            TaskScheduler::is_task_ready_for_scheduling(&child_task, &all_tasks, &submitted);
        assert!(result, "Task with completed parent should be ready");
    }

    #[tokio::test]
    async fn test_cascade_failed_to_child_tasks() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();

        let (test_repo, commit_sha) = setup_test_repo().unwrap();

        // Create parent and child tasks
        let parent_task = create_test_task(
            "parent-1",
            test_repo.path(),
            &commit_sha,
            tsk_env.data_dir(),
        );
        let child_task = create_child_task("child-1", test_repo.path(), &commit_sha, "parent-1");

        // Set up storage with both tasks
        let storage = ctx.task_storage();
        storage.add_task(parent_task.clone()).await.unwrap();
        storage.add_task(child_task.clone()).await.unwrap();

        let quit_signal = Arc::new(tokio::sync::Notify::new());
        let scheduler = TaskScheduler::new(
            Arc::new(ctx),
            test_docker_client(),
            storage.clone(),
            false,
            quit_signal,
            None,
        );

        // Get all tasks
        let tasks = storage.list_tasks().await.unwrap();

        // Cascade failure to child tasks
        scheduler
            .cascade_to_child_tasks("parent-1", &TaskStatus::Failed, &tasks)
            .await
            .unwrap();

        // Verify child task is marked as failed
        let child = storage.get_task("child-1").await.unwrap().unwrap();
        assert_eq!(
            child.status,
            TaskStatus::Failed,
            "Child task should be marked as failed"
        );
        assert!(
            child.error_message.as_ref().unwrap().contains("parent-1"),
            "Error message should mention parent task"
        );
    }

    #[tokio::test]
    async fn test_cascade_cancelled_to_child_tasks() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();

        let (test_repo, commit_sha) = setup_test_repo().unwrap();

        let parent_task = create_test_task(
            "parent-2",
            test_repo.path(),
            &commit_sha,
            tsk_env.data_dir(),
        );
        let child_task = create_child_task("child-2", test_repo.path(), &commit_sha, "parent-2");

        let storage = ctx.task_storage();
        storage.add_task(parent_task.clone()).await.unwrap();
        storage.add_task(child_task.clone()).await.unwrap();

        let quit_signal = Arc::new(tokio::sync::Notify::new());
        let scheduler = TaskScheduler::new(
            Arc::new(ctx),
            test_docker_client(),
            storage.clone(),
            false,
            quit_signal,
            None,
        );

        let tasks = storage.list_tasks().await.unwrap();

        // Cascade cancellation to child tasks
        scheduler
            .cascade_to_child_tasks("parent-2", &TaskStatus::Cancelled, &tasks)
            .await
            .unwrap();

        // Verify child task is marked as cancelled (not failed)
        let child = storage.get_task("child-2").await.unwrap().unwrap();
        assert_eq!(
            child.status,
            TaskStatus::Cancelled,
            "Child task should be marked as cancelled when parent is cancelled"
        );
    }

    #[test]
    fn test_precopied_child_is_ready_for_scheduling() {
        // A child task with pre-copied repo (skip_parent_repo_deferral) should be
        // ready for scheduling when its parent is complete, and the scheduler's
        // preparation block (line 555) should be skipped since copied_repo_path is set.
        let parent_task = Task {
            id: "parent-1".to_string(),
            name: "parent-task".to_string(),
            branch_name: "tsk/test/parent-1".to_string(),
            status: TaskStatus::Complete,
            ..Task::test_default()
        };

        let child_task = Task {
            id: "child-1".to_string(),
            name: "review-task".to_string(),
            branch_name: "tsk/test/child-1".to_string(),
            source_branch: Some("main".to_string()),
            copied_repo_path: Some(std::path::PathBuf::from("/tmp/precopied")),
            parent_ids: vec!["parent-1".to_string()],
            ..Task::test_default()
        };

        let all_tasks = vec![parent_task, child_task.clone()];
        let submitted = HashSet::new();

        assert!(
            TaskScheduler::is_task_ready_for_scheduling(&child_task, &all_tasks, &submitted),
            "Pre-copied child with complete parent should be ready"
        );
    }
}
