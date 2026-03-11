use chrono::{DateTime, Local, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::path::PathBuf;

fn default_true() -> bool {
    true
}

// The JSON format (tasks.json) is frozen and will not change. It only ever supported
// a single `parent_id: Option<String>`. JSON task files are migrated to SQLite on
// first run and then renamed to tasks.json.bak, so we only need to read them once.
// These serde helpers bridge the legacy JSON `parent_id` field to the internal
// `parent_ids: Vec<String>` representation.

/// Deserializes a legacy `parent_id: Option<String>` into `Vec<String>`.
fn deserialize_parent_id<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    Ok(opt.into_iter().collect())
}

/// Serializes `Vec<String>` back as `parent_id: Option<String>` for JSON compatibility.
fn serialize_parent_id<S>(ids: &[String], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match ids.first() {
        Some(id) => serializer.serialize_some(id),
        None => serializer.serialize_none(),
    }
}

/// Represents the execution status of a task
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    /// Task is in the queue waiting to be executed
    #[serde(rename = "QUEUED")]
    Queued,
    /// Task is currently being executed
    #[serde(rename = "RUNNING")]
    Running,
    /// Task execution failed
    #[serde(rename = "FAILED")]
    Failed,
    /// Task completed successfully
    #[serde(rename = "COMPLETE")]
    Complete,
    /// Task was intentionally cancelled (by user, shutdown, or signal)
    #[serde(rename = "CANCELLED")]
    Cancelled,
}

/// Represents a TSK task with all required fields for execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Unique identifier for the task (format: YYYY-MM-DD-HHMM-{task_type}-{name})
    pub id: String,
    /// Absolute path to the repository root where the task was created
    pub repo_root: PathBuf,
    /// Human-readable name for the task
    pub name: String,
    /// Type of task (e.g., "feat", "fix", "refactor")
    pub task_type: String,
    /// Path to the instructions file containing task details
    pub instructions_file: String,
    /// AI agent to use for task execution (e.g., "claude")
    pub agent: String,
    /// Current status of the task
    pub status: TaskStatus,
    /// When the task was created
    pub created_at: DateTime<Local>,
    /// When the task started execution (if started)
    pub started_at: Option<DateTime<Utc>>,
    /// When the task completed (if completed)
    pub completed_at: Option<DateTime<Utc>>,
    /// Git branch name for this task (format: tsk/{task-id})
    pub branch_name: String,
    /// Optional target branch name to use instead of auto-generated branch.
    /// When set, the task attempts to push to this branch. Falls back to
    /// the auto-generated name if the push fails (e.g., non-fast-forward).
    #[serde(default)]
    pub target_branch: Option<String>,
    /// Error message if task failed
    pub error_message: Option<String>,
    /// Git commit SHA from which the task was created
    pub source_commit: String,
    /// Git branch from which the task was created (for git-town parent tracking)
    /// None if created from detached HEAD state
    #[serde(default)]
    pub source_branch: Option<String>,
    /// Stack for Docker image selection (e.g., "rust", "python", "default")
    #[serde(alias = "tech_stack")]
    pub stack: String,
    /// Project name for Docker image selection (defaults to "default")
    pub project: String,
    /// Path to the copied repository for this task.
    /// None if the task has a parent and is waiting for it to complete.
    #[serde(default)]
    pub copied_repo_path: Option<PathBuf>,
    /// Whether this task should run in interactive mode
    #[serde(default)]
    pub is_interactive: bool,
    /// Parent task IDs that this task is chained to.
    /// If non-empty, this task will wait for the parent to complete before executing,
    /// and will use the parent's completed repository as its starting point.
    #[serde(
        default,
        rename = "parent_id",
        deserialize_with = "deserialize_parent_id",
        serialize_with = "serialize_parent_id"
    )]
    pub parent_ids: Vec<String>,
    /// Whether per-container network isolation is enabled for this task
    #[serde(default = "default_true")]
    pub network_isolation: bool,
    /// Whether Docker-in-Docker support is enabled (relaxes container security)
    #[serde(default)]
    pub dind: bool,
    /// Serialized JSON of the fully-resolved ResolvedConfig at task creation time.
    /// Used at execution time instead of re-resolving from config files.
    /// None for tasks created before this feature (falls back to live resolution).
    #[serde(default)]
    pub resolved_config: Option<String>,
}

impl Task {
    /// Creates a new Task with all required fields
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        repo_root: PathBuf,
        name: String,
        task_type: String,
        instructions_file: String,
        agent: String,
        branch_name: String,
        target_branch: Option<String>,
        source_commit: String,
        source_branch: Option<String>,
        stack: String,
        project: String,
        created_at: DateTime<Local>,
        copied_repo_path: Option<PathBuf>,
        is_interactive: bool,
        parent_ids: Vec<String>,
        network_isolation: bool,
        dind: bool,
        resolved_config: Option<String>,
    ) -> Self {
        Self {
            id,
            repo_root,
            name,
            task_type,
            instructions_file,
            agent,
            status: TaskStatus::Queued,
            created_at,
            started_at: None,
            completed_at: None,
            branch_name,
            target_branch,
            error_message: None,
            source_commit,
            source_branch,
            stack,
            project,
            copied_repo_path,
            is_interactive,
            parent_ids,
            network_isolation,
            dind,
            resolved_config,
        }
    }

    /// Returns the auto-generated branch name for this task.
    ///
    /// This is the standard `tsk/{type}/{name}/{id}` format, computed
    /// deterministically from existing task fields.
    pub fn generated_branch_name(&self) -> String {
        let sanitized_type = crate::utils::sanitize_for_branch_name(&self.task_type);
        let sanitized_name = crate::utils::sanitize_for_branch_name(&self.name);
        format!("tsk/{sanitized_type}/{sanitized_name}/{}", self.id)
    }
}

// TaskBuilder has been moved to task_builder.rs
// Re-export it for backward compatibility
pub use crate::task_builder::TaskBuilder;

#[cfg(test)]
impl Task {
    /// Creates a Task with sensible defaults for testing.
    ///
    /// Tests should override only the fields relevant to their scenario
    /// using struct update syntax: `Task { field: val, ..Task::test_default() }`.
    pub fn test_default() -> Self {
        Self {
            id: "test-id".to_string(),
            repo_root: PathBuf::from("/test"),
            name: "test-task".to_string(),
            task_type: "feat".to_string(),
            instructions_file: "instructions.md".to_string(),
            agent: "claude".to_string(),
            status: TaskStatus::Queued,
            created_at: chrono::Local::now(),
            started_at: None,
            completed_at: None,
            branch_name: "tsk/feat/test-task/test-id".to_string(),
            target_branch: None,
            error_message: None,
            source_commit: "abc123".to_string(),
            source_branch: Some("main".to_string()),
            stack: "default".to_string(),
            project: "default".to_string(),
            copied_repo_path: Some(PathBuf::from("/test/copied")),
            is_interactive: false,
            parent_ids: vec![],
            network_isolation: true,
            dind: false,
            resolved_config: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_status_serialization() {
        assert_eq!(
            serde_json::to_string(&TaskStatus::Queued).unwrap(),
            "\"QUEUED\""
        );
        assert_eq!(
            serde_json::to_string(&TaskStatus::Running).unwrap(),
            "\"RUNNING\""
        );
        assert_eq!(
            serde_json::to_string(&TaskStatus::Failed).unwrap(),
            "\"FAILED\""
        );
        assert_eq!(
            serde_json::to_string(&TaskStatus::Complete).unwrap(),
            "\"COMPLETE\""
        );
        assert_eq!(
            serde_json::to_string(&TaskStatus::Cancelled).unwrap(),
            "\"CANCELLED\""
        );
    }

    #[test]
    fn test_task_creation() {
        let task = Task::test_default();

        assert_eq!(task.id, "test-id");
        assert_eq!(task.name, "test-task");
        assert_eq!(task.task_type, "feat");
        assert_eq!(task.status, TaskStatus::Queued);
        assert!(task.started_at.is_none());
        assert!(task.completed_at.is_none());
        assert!(task.error_message.is_none());
        assert!(!task.is_interactive);
        assert_eq!(task.source_branch, Some("main".to_string()));
        assert!(task.parent_ids.is_empty());
        assert!(task.copied_repo_path.is_some());
    }

    #[test]
    fn test_task_creation_detached_head() {
        let task = Task {
            source_branch: None,
            ..Task::test_default()
        };

        assert!(task.source_branch.is_none());
    }

    #[test]
    fn test_task_creation_with_parent() {
        let task = Task {
            id: "child-id".to_string(),
            name: "child-task".to_string(),
            branch_name: "tsk/feat/child-task/child-id".to_string(),
            source_branch: None,
            copied_repo_path: None,
            parent_ids: vec!["parent-id".to_string()],
            ..Task::test_default()
        };

        assert_eq!(task.parent_ids, vec!["parent-id"]);
        assert!(task.copied_repo_path.is_none());
        assert!(task.source_branch.is_none());
    }

    #[test]
    fn test_generated_branch_name() {
        let task = Task::test_default();
        assert_eq!(task.generated_branch_name(), "tsk/feat/test-task/test-id");
    }

    #[test]
    fn test_deserialize_parent_id_present() {
        // Simulate a legacy tasks.json entry with "parent_id" field
        let json = r#"{
            "id": "test-id",
            "repo_root": "/test",
            "name": "test",
            "task_type": "feat",
            "instructions_file": "instructions.md",
            "agent": "claude",
            "status": "QUEUED",
            "created_at": "2025-01-01T00:00:00+00:00",
            "branch_name": "tsk/feat/test/test-id",
            "source_commit": "abc123",
            "stack": "rust",
            "project": "test",
            "parent_id": "legacy-parent"
        }"#;
        let task: Task = serde_json::from_str(json).unwrap();
        assert_eq!(task.parent_ids, vec!["legacy-parent"]);
    }

    #[test]
    fn test_deserialize_parent_id_null() {
        let json = r#"{
            "id": "test-id",
            "repo_root": "/test",
            "name": "test",
            "task_type": "feat",
            "instructions_file": "instructions.md",
            "agent": "claude",
            "status": "QUEUED",
            "created_at": "2025-01-01T00:00:00+00:00",
            "branch_name": "tsk/feat/test/test-id",
            "source_commit": "abc123",
            "stack": "rust",
            "project": "test",
            "parent_id": null
        }"#;
        let task: Task = serde_json::from_str(json).unwrap();
        assert!(task.parent_ids.is_empty());
    }

    #[test]
    fn test_deserialize_parent_id_missing() {
        let json = r#"{
            "id": "test-id",
            "repo_root": "/test",
            "name": "test",
            "task_type": "feat",
            "instructions_file": "instructions.md",
            "agent": "claude",
            "status": "QUEUED",
            "created_at": "2025-01-01T00:00:00+00:00",
            "branch_name": "tsk/feat/test/test-id",
            "source_commit": "abc123",
            "stack": "rust",
            "project": "test"
        }"#;
        let task: Task = serde_json::from_str(json).unwrap();
        assert!(task.parent_ids.is_empty());
    }

    #[test]
    fn test_json_round_trip_with_parent() {
        let json = r#"{
            "id": "test-id",
            "repo_root": "/test",
            "name": "test",
            "task_type": "feat",
            "instructions_file": "instructions.md",
            "agent": "claude",
            "status": "QUEUED",
            "created_at": "2025-01-01T00:00:00+00:00",
            "branch_name": "tsk/feat/test/test-id",
            "source_commit": "abc123",
            "stack": "rust",
            "project": "test",
            "parent_id": "parent-123"
        }"#;
        let task: Task = serde_json::from_str(json).unwrap();
        // Round-trip through JSON
        let serialized = serde_json::to_string(&task).unwrap();
        let deserialized: Task = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.parent_ids, vec!["parent-123"]);
    }
}
