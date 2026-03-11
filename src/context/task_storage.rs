use crate::task::Task;
use crate::task::TaskStatus;
use chrono::DateTime;
use rusqlite::Connection;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

fn path_to_string(path: &Path) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    path.to_str()
        .map(|s| s.to_string())
        .ok_or_else(|| format!("Path contains non-UTF-8 characters: {}", path.display()).into())
}

fn task_status_to_str(status: &TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "QUEUED",
        TaskStatus::Running => "RUNNING",
        TaskStatus::Failed => "FAILED",
        TaskStatus::Complete => "COMPLETE",
        TaskStatus::Cancelled => "CANCELLED",
    }
}

fn str_to_task_status(s: &str) -> Result<TaskStatus, Box<dyn std::error::Error + Send + Sync>> {
    match s {
        "QUEUED" => Ok(TaskStatus::Queued),
        "RUNNING" => Ok(TaskStatus::Running),
        "FAILED" => Ok(TaskStatus::Failed),
        "COMPLETE" => Ok(TaskStatus::Complete),
        "CANCELLED" => Ok(TaskStatus::Cancelled),
        _ => Err(format!("Unknown task status: {s}").into()),
    }
}

fn row_to_task(row: &rusqlite::Row) -> rusqlite::Result<Task> {
    let status_str: String = row.get("status")?;
    let created_at_str: String = row.get("created_at")?;
    let started_at_str: Option<String> = row.get("started_at")?;
    let completed_at_str: Option<String> = row.get("completed_at")?;
    let repo_root_str: String = row.get("repo_root")?;
    let copied_repo_path_str: Option<String> = row.get("copied_repo_path")?;
    let is_interactive_int: i32 = row.get("is_interactive")?;
    let network_isolation_int: i32 = row.get("network_isolation")?;
    let dind_int: i32 = row.get("dind")?;

    Ok(Task {
        id: row.get("id")?,
        repo_root: PathBuf::from(repo_root_str),
        name: row.get("name")?,
        task_type: row.get("task_type")?,
        instructions_file: row.get("instructions_file")?,
        agent: row.get("agent")?,
        status: str_to_task_status(&status_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, e)
        })?,
        created_at: DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&chrono::Local))
            .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    7,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
        started_at: started_at_str
            .map(|s| DateTime::parse_from_rfc3339(&s).map(|dt| dt.with_timezone(&chrono::Utc)))
            .transpose()
            .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    8,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
        completed_at: completed_at_str
            .map(|s| DateTime::parse_from_rfc3339(&s).map(|dt| dt.with_timezone(&chrono::Utc)))
            .transpose()
            .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    9,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
        branch_name: row.get("branch_name")?,
        target_branch: row.get("target_branch")?,
        error_message: row.get("error_message")?,
        source_commit: row.get("source_commit")?,
        source_branch: row.get("source_branch")?,
        stack: row.get("stack")?,
        project: row.get("project")?,
        copied_repo_path: copied_repo_path_str.map(PathBuf::from),
        is_interactive: is_interactive_int != 0,
        parent_ids: {
            let raw: Option<String> = row.get("parent_ids")?;
            match raw {
                Some(s) => serde_json::from_str(&s).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        18,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?,
                None => vec![],
            }
        },
        network_isolation: network_isolation_int != 0,
        dind: dind_int != 0,
        resolved_config: row.get("resolved_config")?,
    })
}

/// Read a single task by ID from the database, returning an error if not found.
fn read_task_by_id(
    conn: &Connection,
    id: &str,
) -> Result<Task, Box<dyn std::error::Error + Send + Sync>> {
    let mut stmt = conn.prepare("SELECT * FROM tasks WHERE id = ?1")?;
    let task = stmt
        .query_row(rusqlite::params![id], row_to_task)
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                Box::<dyn std::error::Error + Send + Sync>::from("Task not found")
            }
            other => Box::<dyn std::error::Error + Send + Sync>::from(other),
        })?;
    Ok(task)
}

fn insert_task(
    conn: &Connection,
    task: &Task,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let repo_root = path_to_string(&task.repo_root)?;
    let copied_repo_path = task
        .copied_repo_path
        .as_ref()
        .map(|p| path_to_string(p))
        .transpose()?;
    conn.execute(
        "INSERT INTO tasks (id, repo_root, name, task_type, instructions_file, agent, status, created_at, started_at, completed_at, branch_name, target_branch, error_message, source_commit, source_branch, stack, project, copied_repo_path, is_interactive, parent_ids, network_isolation, dind, resolved_config) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)",
        rusqlite::params![
            task.id,
            repo_root,
            task.name,
            task.task_type,
            task.instructions_file,
            task.agent,
            task_status_to_str(&task.status),
            task.created_at.to_rfc3339(),
            task.started_at.map(|dt| dt.to_rfc3339()),
            task.completed_at.map(|dt| dt.to_rfc3339()),
            task.branch_name,
            task.target_branch,
            task.error_message,
            task.source_commit,
            task.source_branch,
            task.stack,
            task.project,
            copied_repo_path,
            task.is_interactive as i32,
            if task.parent_ids.is_empty() { None::<String> } else { Some(serde_json::to_string(&task.parent_ids).unwrap()) },
            task.network_isolation as i32,
            task.dind as i32,
            task.resolved_config,
        ],
    )?;
    Ok(())
}

/// Attempts to migrate tasks from a legacy `tasks.json` file into the SQLite database.
///
/// Migration runs only when:
/// - `tasks.json` exists in `data_dir`
/// - `tasks.json.bak` does NOT exist (prevents re-migration)
/// - The `tasks` table is empty
///
/// After successful migration, `tasks.json` is renamed to `tasks.json.bak`.
fn migrate_from_json(conn: &Connection, data_dir: &Path) {
    let json_path = data_dir.join("tasks.json");
    let bak_path = data_dir.join("tasks.json.bak");

    if !json_path.exists() {
        return;
    }

    if bak_path.exists() {
        return;
    }

    let count: i64 = match conn.query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Warning: failed to check tasks table during migration: {e}");
            return;
        }
    };
    if count > 0 {
        return;
    }

    let contents = match fs::read_to_string(&json_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Warning: failed to read tasks.json for migration: {e}");
            return;
        }
    };

    let tasks: Vec<Task> = match serde_json::from_str(&contents) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Warning: tasks.json contains invalid data, skipping migration: {e}");
            if let Err(rename_err) = fs::rename(&json_path, &bak_path) {
                eprintln!("Warning: failed to rename tasks.json to tasks.json.bak: {rename_err}");
            }
            return;
        }
    };

    if tasks.is_empty() {
        eprintln!("Migrated 0 tasks from tasks.json to tasks.db");
        if let Err(e) = fs::rename(&json_path, &bak_path) {
            eprintln!("Warning: failed to rename tasks.json to tasks.json.bak: {e}");
        }
        return;
    }

    // Safe: we have exclusive access during construction and transaction() requires &mut
    let tx = match conn.unchecked_transaction() {
        Ok(tx) => tx,
        Err(e) => {
            eprintln!("Warning: failed to begin migration transaction: {e}");
            return;
        }
    };

    for task in &tasks {
        if let Err(e) = insert_task(&tx, task) {
            eprintln!("Warning: failed to migrate task {}: {e}", task.id);
            return; // Transaction will be rolled back on drop
        }
    }

    if let Err(e) = tx.commit() {
        eprintln!("Warning: failed to commit migration transaction: {e}");
        return;
    }

    if let Err(e) = fs::rename(&json_path, &bak_path) {
        eprintln!("Warning: failed to rename tasks.json to tasks.json.bak: {e}");
        return;
    }

    eprintln!("Migrated {} tasks from tasks.json to tasks.db", tasks.len());
}

/// SQLite-backed task storage.
///
/// Stores tasks in a SQLite database with WAL mode and busy_timeout for safe concurrent
/// multi-process access.
/// All database operations are executed via `tokio::task::spawn_blocking` to avoid blocking
/// the async runtime.
///
/// Status mutations use explicit named transition methods that perform targeted
/// SQL updates and return the full updated row, eliminating clone-mutate-replace
/// patterns and ensuring callers always receive the authoritative DB state.
pub struct TaskStorage {
    conn: Arc<StdMutex<Connection>>,
}

impl TaskStorage {
    /// Creates a new `TaskStorage`, opening or creating the database at `db_path`.
    ///
    /// Enables WAL journal mode and a 5-second busy timeout, then creates the `tasks` table
    /// and indexes if they don't exist.
    pub fn new(db_path: PathBuf) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let conn = Connection::open(&db_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                repo_root TEXT NOT NULL,
                name TEXT NOT NULL,
                task_type TEXT NOT NULL,
                instructions_file TEXT NOT NULL,
                agent TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                branch_name TEXT NOT NULL,
                error_message TEXT,
                source_commit TEXT NOT NULL,
                source_branch TEXT,
                stack TEXT NOT NULL,
                project TEXT NOT NULL,
                copied_repo_path TEXT,
                is_interactive INTEGER NOT NULL DEFAULT 0,
                parent_ids TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);",
        )?;

        // Migration: add network_isolation column for existing databases
        let _ = conn.execute_batch(
            "ALTER TABLE tasks ADD COLUMN network_isolation INTEGER NOT NULL DEFAULT 1;",
        );

        // Migration: add dind column for existing databases
        let _ = conn.execute_batch("ALTER TABLE tasks ADD COLUMN dind INTEGER NOT NULL DEFAULT 0;");

        // Migration: add resolved_config column for config snapshotting
        let _ = conn.execute_batch("ALTER TABLE tasks ADD COLUMN resolved_config TEXT;");

        // Migration: add target_branch column for branch targeting
        let _ = conn.execute_batch("ALTER TABLE tasks ADD COLUMN target_branch TEXT;");

        if let Some(data_dir) = db_path.parent() {
            migrate_from_json(&conn, data_dir);
        }

        Ok(Self {
            conn: Arc::new(StdMutex::new(conn)),
        })
    }

    pub async fn add_task(
        &self,
        task: Task,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| format!("Lock error: {e}"))?;
            insert_task(&conn, &task)
        })
        .await??;
        Ok(())
    }

    pub async fn get_task(
        &self,
        id: &str,
    ) -> Result<Option<Task>, Box<dyn std::error::Error + Send + Sync>> {
        let conn = Arc::clone(&self.conn);
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| format!("Lock error: {e}"))?;
            let mut stmt = conn.prepare("SELECT * FROM tasks WHERE id = ?1")?;
            let mut rows = stmt.query_map(rusqlite::params![id], row_to_task)?;
            match rows.next() {
                Some(row) => Ok(Some(row?)),
                None => Ok(None),
            }
        })
        .await?
    }

    pub async fn list_tasks(&self) -> Result<Vec<Task>, Box<dyn std::error::Error + Send + Sync>> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| format!("Lock error: {e}"))?;
            let mut stmt = conn.prepare("SELECT * FROM tasks ORDER BY created_at")?;
            let tasks = stmt
                .query_map([], row_to_task)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(tasks)
        })
        .await?
    }

    pub async fn delete_task(
        &self,
        id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = Arc::clone(&self.conn);
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| format!("Lock error: {e}"))?;
            let rows_affected =
                conn.execute("DELETE FROM tasks WHERE id = ?1", rusqlite::params![id])?;
            if rows_affected == 0 {
                return Err("Task not found".into());
            }
            Ok(())
        })
        .await?
    }

    /// Transition a task to Running status, setting `started_at` to now.
    pub async fn mark_running(
        &self,
        id: &str,
    ) -> Result<Task, Box<dyn std::error::Error + Send + Sync>> {
        let conn = Arc::clone(&self.conn);
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| format!("Lock error: {e}"))?;
            let now = chrono::Utc::now().to_rfc3339();
            let rows_affected = conn.execute(
                "UPDATE tasks SET status = 'RUNNING', started_at = ?1 WHERE id = ?2",
                rusqlite::params![now, id],
            )?;
            if rows_affected == 0 {
                return Err("Task not found".into());
            }
            read_task_by_id(&conn, &id)
        })
        .await?
    }

    /// Transition a task to Complete status, setting `completed_at` to now and `branch_name`.
    ///
    /// Guarded: only updates tasks with status = RUNNING. If the task exists but is
    /// no longer RUNNING (e.g., already CANCELLED), returns the current task as-is (no-op).
    pub async fn mark_complete(
        &self,
        id: &str,
        branch_name: &str,
    ) -> Result<Task, Box<dyn std::error::Error + Send + Sync>> {
        let conn = Arc::clone(&self.conn);
        let id = id.to_string();
        let branch_name = branch_name.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| format!("Lock error: {e}"))?;
            let now = chrono::Utc::now().to_rfc3339();
            let rows_affected = conn.execute(
                "UPDATE tasks SET status = 'COMPLETE', completed_at = ?1, branch_name = ?2 WHERE id = ?3 AND status = 'RUNNING'",
                rusqlite::params![now, branch_name, id],
            )?;
            if rows_affected == 0 {
                // Task may not exist, or may already be in a terminal state (e.g. CANCELLED)
                return read_task_by_id(&conn, &id);
            }
            read_task_by_id(&conn, &id)
        })
        .await?
    }

    /// Transition a task to Failed status, setting `completed_at` to now and `error_message`.
    ///
    /// Guarded: only updates tasks with status RUNNING or QUEUED. If the task exists
    /// but is already in a terminal state (COMPLETE, FAILED, or CANCELLED), returns
    /// the current task as-is (no-op).
    pub async fn mark_failed(
        &self,
        id: &str,
        error_message: &str,
    ) -> Result<Task, Box<dyn std::error::Error + Send + Sync>> {
        let conn = Arc::clone(&self.conn);
        let id = id.to_string();
        let error_message = error_message.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| format!("Lock error: {e}"))?;
            let now = chrono::Utc::now().to_rfc3339();
            let rows_affected = conn.execute(
                "UPDATE tasks SET status = 'FAILED', completed_at = ?1, error_message = ?2 WHERE id = ?3 AND status IN ('RUNNING', 'QUEUED')",
                rusqlite::params![now, error_message, id],
            )?;
            if rows_affected == 0 {
                // Task may not exist, or may already be in a terminal state (e.g. CANCELLED)
                return read_task_by_id(&conn, &id);
            }
            read_task_by_id(&conn, &id)
        })
        .await?
    }

    /// Transition a task to Cancelled status, setting `completed_at` to now.
    ///
    /// Works for both RUNNING and QUEUED tasks. Returns an error if the task
    /// is not found or is already in a terminal state.
    pub async fn mark_cancelled(
        &self,
        id: &str,
    ) -> Result<Task, Box<dyn std::error::Error + Send + Sync>> {
        let conn = Arc::clone(&self.conn);
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| format!("Lock error: {e}"))?;
            let now = chrono::Utc::now().to_rfc3339();
            let rows_affected = conn.execute(
                "UPDATE tasks SET status = 'CANCELLED', completed_at = ?1 WHERE id = ?2 AND status IN ('RUNNING', 'QUEUED')",
                rusqlite::params![now, id],
            )?;
            if rows_affected == 0 {
                // Check if task exists — if so, it's already terminal
                let task = read_task_by_id(&conn, &id)?;
                return Err(format!(
                    "Task {} is already in terminal state: {}",
                    id,
                    task_status_to_str(&task.status)
                ).into());
            }
            read_task_by_id(&conn, &id)
        })
        .await?
    }

    /// Reset a task to Queued status, clearing `started_at`, `completed_at`, and `error_message`.
    pub async fn reset_to_queued(
        &self,
        id: &str,
    ) -> Result<Task, Box<dyn std::error::Error + Send + Sync>> {
        let conn = Arc::clone(&self.conn);
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| format!("Lock error: {e}"))?;
            let rows_affected = conn.execute(
                "UPDATE tasks SET status = 'QUEUED', started_at = NULL, completed_at = NULL, error_message = NULL WHERE id = ?1",
                rusqlite::params![id],
            )?;
            if rows_affected == 0 {
                return Err("Task not found".into());
            }
            read_task_by_id(&conn, &id)
        })
        .await?
    }

    /// Update a child task's repository fields after copying from its parent.
    /// Sets `copied_repo_path`, `source_commit`, `source_branch`, and copies
    /// `resolved_config` from the parent task to preserve the original config snapshot.
    pub async fn prepare_child_task(
        &self,
        id: &str,
        copied_repo_path: PathBuf,
        source_commit: &str,
        source_branch: &str,
        parent_resolved_config: Option<&str>,
    ) -> Result<Task, Box<dyn std::error::Error + Send + Sync>> {
        let conn = Arc::clone(&self.conn);
        let id = id.to_string();
        let copied_repo_path_str = path_to_string(&copied_repo_path)?;
        let source_commit = source_commit.to_string();
        let source_branch = source_branch.to_string();
        let parent_resolved_config = parent_resolved_config.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| format!("Lock error: {e}"))?;
            let rows_affected = conn.execute(
                "UPDATE tasks SET copied_repo_path = ?1, source_commit = ?2, source_branch = ?3, resolved_config = COALESCE(?4, resolved_config) WHERE id = ?5",
                rusqlite::params![copied_repo_path_str, source_commit, source_branch, parent_resolved_config, id],
            )?;
            if rows_affected == 0 {
                return Err("Task not found".into());
            }
            read_task_by_id(&conn, &id)
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::AppContext;
    use crate::task::{Task, TaskStatus};

    #[tokio::test]
    async fn test_crud_operations() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let data_dir = tsk_env.data_dir();

        let db_path = data_dir.join("test_tasks.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "abcd1234".to_string(),
            repo_root: data_dir.to_path_buf(),
            task_type: "feature".to_string(),
            branch_name: "tsk/test-task".to_string(),
            copied_repo_path: Some(data_dir.to_path_buf()),
            ..Task::test_default()
        };

        storage.add_task(task.clone()).await.unwrap();

        let retrieved = storage.get_task(&task.id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "test-task");

        let tasks = storage.list_tasks().await.unwrap();
        assert_eq!(tasks.len(), 1);

        let updated = storage.mark_running(&task.id).await.unwrap();
        assert_eq!(updated.status, TaskStatus::Running);

        let retrieved = storage.get_task(&task.id).await.unwrap().unwrap();
        assert_eq!(retrieved.status, TaskStatus::Running);

        storage.delete_task(&task.id).await.unwrap();
        let retrieved = storage.get_task(&task.id).await.unwrap();
        assert!(retrieved.is_none());

        // Test deleting specific tasks
        let task1 = Task {
            id: "efgh5678".to_string(),
            repo_root: data_dir.to_path_buf(),
            name: "task1".to_string(),
            task_type: "feature".to_string(),
            branch_name: "tsk/task1".to_string(),
            copied_repo_path: Some(data_dir.to_path_buf()),
            ..Task::test_default()
        };
        let task2 = Task {
            id: "ijkl9012".to_string(),
            repo_root: data_dir.to_path_buf(),
            name: "task2".to_string(),
            task_type: "bug-fix".to_string(),
            status: TaskStatus::Complete,
            branch_name: "tsk/task2".to_string(),
            copied_repo_path: Some(data_dir.to_path_buf()),
            ..Task::test_default()
        };
        let task3 = Task {
            id: "mnop3456".to_string(),
            repo_root: data_dir.to_path_buf(),
            name: "task3".to_string(),
            task_type: "refactor".to_string(),
            status: TaskStatus::Failed,
            branch_name: "tsk/task3".to_string(),
            copied_repo_path: Some(data_dir.to_path_buf()),
            ..Task::test_default()
        };

        storage.add_task(task1.clone()).await.unwrap();
        storage.add_task(task2.clone()).await.unwrap();
        storage.add_task(task3.clone()).await.unwrap();

        storage.delete_task(&task2.id).await.unwrap();
        storage.delete_task(&task3.id).await.unwrap();

        let remaining_tasks = storage.list_tasks().await.unwrap();
        assert_eq!(remaining_tasks.len(), 1);
        assert_eq!(remaining_tasks[0].status, TaskStatus::Queued);
    }

    #[tokio::test]
    async fn test_sqlite_round_trip_all_fields() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();

        let db_path = tsk_env.data_dir().join("test_round_trip.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let created_at = chrono::Local::now();
        let started_at = chrono::Utc::now();
        let completed_at = chrono::Utc::now();

        let task = Task {
            id: "round1234".to_string(),
            repo_root: PathBuf::from("/some/repo/root"),
            name: "full-task".to_string(),
            instructions_file: "/tmp/instructions.md".to_string(),
            agent: "codex".to_string(),
            status: TaskStatus::Complete,
            created_at,
            started_at: Some(started_at),
            completed_at: Some(completed_at),
            branch_name: "tsk/feat/full-task/round1234".to_string(),
            error_message: Some("something went wrong".to_string()),
            source_commit: "deadbeef".to_string(),
            source_branch: Some("develop".to_string()),
            stack: "rust".to_string(),
            project: "my-project".to_string(),
            copied_repo_path: Some(PathBuf::from("/copied/repo/path")),
            is_interactive: true,
            parent_ids: vec!["parent5678".to_string()],
            ..Task::test_default()
        };

        storage.add_task(task.clone()).await.unwrap();

        let retrieved = storage.get_task("round1234").await.unwrap().unwrap();

        assert_eq!(retrieved.id, "round1234");
        assert_eq!(retrieved.repo_root, PathBuf::from("/some/repo/root"));
        assert_eq!(retrieved.name, "full-task");
        assert_eq!(retrieved.task_type, "feat");
        assert_eq!(retrieved.instructions_file, "/tmp/instructions.md");
        assert_eq!(retrieved.agent, "codex");
        assert_eq!(retrieved.status, TaskStatus::Complete);
        assert_eq!(
            retrieved.created_at.to_rfc3339(),
            task.created_at.to_rfc3339()
        );
        assert_eq!(
            retrieved.started_at.unwrap().to_rfc3339(),
            started_at.to_rfc3339()
        );
        assert_eq!(
            retrieved.completed_at.unwrap().to_rfc3339(),
            completed_at.to_rfc3339()
        );
        assert_eq!(retrieved.branch_name, "tsk/feat/full-task/round1234");
        assert_eq!(
            retrieved.error_message,
            Some("something went wrong".to_string())
        );
        assert_eq!(retrieved.source_commit, "deadbeef");
        assert_eq!(retrieved.source_branch, Some("develop".to_string()));
        assert_eq!(retrieved.stack, "rust");
        assert_eq!(retrieved.project, "my-project");
        assert_eq!(
            retrieved.copied_repo_path,
            Some(PathBuf::from("/copied/repo/path"))
        );
        assert!(retrieved.is_interactive);
        assert_eq!(retrieved.parent_ids, vec!["parent5678".to_string()]);
        assert_eq!(retrieved.target_branch, None);
    }

    #[tokio::test]
    async fn test_target_branch_round_trip() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();

        let db_path = tsk_env.data_dir().join("test_target_branch.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "target1234".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "feature/my-branch".to_string(),
            target_branch: Some("feature/my-branch".to_string()),
            copied_repo_path: Some(tsk_env.data_dir().to_path_buf()),
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();

        let retrieved = storage.get_task("target1234").await.unwrap().unwrap();
        assert_eq!(
            retrieved.target_branch,
            Some("feature/my-branch".to_string())
        );
        assert_eq!(retrieved.branch_name, "feature/my-branch");
    }

    #[tokio::test]
    async fn test_mark_running() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_mark_running.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "run1234".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/fix/run-task/run1234".to_string(),
            copied_repo_path: Some(tsk_env.data_dir().to_path_buf()),
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();

        let updated = storage.mark_running("run1234").await.unwrap();
        assert_eq!(updated.status, TaskStatus::Running);
        assert!(updated.started_at.is_some());
        assert!(updated.completed_at.is_none());
        assert!(updated.error_message.is_none());
    }

    #[tokio::test]
    async fn test_mark_complete() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_mark_complete.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "comp1234".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/fix/comp-task/comp1234".to_string(),
            copied_repo_path: Some(tsk_env.data_dir().to_path_buf()),
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();

        // First mark running, then complete
        storage.mark_running("comp1234").await.unwrap();
        let updated = storage
            .mark_complete("comp1234", "tsk/fix/new-branch/comp1234")
            .await
            .unwrap();
        assert_eq!(updated.status, TaskStatus::Complete);
        assert!(updated.completed_at.is_some());
        assert_eq!(updated.branch_name, "tsk/fix/new-branch/comp1234");
        // started_at should be preserved from mark_running
        assert!(updated.started_at.is_some());
    }

    #[tokio::test]
    async fn test_mark_failed() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_mark_failed.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "fail1234".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/fix/fail-task/fail1234".to_string(),
            copied_repo_path: Some(tsk_env.data_dir().to_path_buf()),
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();

        storage.mark_running("fail1234").await.unwrap();
        let updated = storage
            .mark_failed("fail1234", "agent crashed")
            .await
            .unwrap();
        assert_eq!(updated.status, TaskStatus::Failed);
        assert!(updated.completed_at.is_some());
        assert_eq!(updated.error_message, Some("agent crashed".to_string()));
        // started_at should be preserved from mark_running
        assert!(updated.started_at.is_some());
    }

    #[tokio::test]
    async fn test_reset_to_queued() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_reset_queued.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "reset1234".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/fix/reset-task/reset1234".to_string(),
            copied_repo_path: Some(tsk_env.data_dir().to_path_buf()),
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();

        // Move through Running -> Failed, then reset
        storage.mark_running("reset1234").await.unwrap();
        storage
            .mark_failed("reset1234", "temporary error")
            .await
            .unwrap();
        let updated = storage.reset_to_queued("reset1234").await.unwrap();
        assert_eq!(updated.status, TaskStatus::Queued);
        assert!(updated.started_at.is_none());
        assert!(updated.completed_at.is_none());
        assert!(updated.error_message.is_none());
    }

    #[tokio::test]
    async fn test_prepare_child_task() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_prepare_child.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "child1234".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/feat/child-task/child1234".to_string(),
            source_commit: "old_commit".to_string(),
            parent_ids: vec!["parent1234".to_string()],
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();

        let repo_path = tsk_env.data_dir().join("copied-repo");
        let updated = storage
            .prepare_child_task(
                "child1234",
                repo_path.clone(),
                "new_commit_sha",
                "tsk/feat/parent-branch/parent1234",
                None,
            )
            .await
            .unwrap();
        assert_eq!(updated.copied_repo_path, Some(repo_path));
        assert_eq!(updated.source_commit, "new_commit_sha");
        assert_eq!(
            updated.source_branch,
            Some("tsk/feat/parent-branch/parent1234".to_string())
        );
        // branch_name should remain unchanged (it's the child's own branch)
        assert_eq!(updated.branch_name, "tsk/feat/child-task/child1234");
    }

    #[tokio::test]
    async fn test_migration_from_json() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        tsk_env.ensure_directories().unwrap();

        let data_dir = tsk_env.data_dir();
        let json_path = data_dir.join("tasks.json");
        let bak_path = data_dir.join("tasks.json.bak");
        let db_path = data_dir.join("migration_test.db");

        // Create a tasks.json with known tasks
        let task = Task {
            id: "migrate1234".to_string(),
            repo_root: data_dir.to_path_buf(),
            name: "migrate-task".to_string(),
            branch_name: "tsk/feat/migrate-task/migrate1234".to_string(),
            copied_repo_path: Some(data_dir.to_path_buf()),
            ..Task::test_default()
        };
        let tasks = vec![task];
        let json = serde_json::to_string_pretty(&tasks).unwrap();
        fs::write(&json_path, &json).unwrap();

        // Construct TaskStorage -- migration should run automatically
        let storage = TaskStorage::new(db_path).unwrap();

        // Verify tasks are in SQLite
        let stored_tasks = storage.list_tasks().await.unwrap();
        assert_eq!(stored_tasks.len(), 1);
        assert_eq!(stored_tasks[0].id, "migrate1234");
        assert_eq!(stored_tasks[0].name, "migrate-task");

        // Verify tasks.json was renamed
        assert!(!json_path.exists());
        assert!(bak_path.exists());

        // Clean up
        let _ = fs::remove_file(&bak_path);
    }

    #[tokio::test]
    async fn test_migration_skipped_when_bak_exists() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        tsk_env.ensure_directories().unwrap();

        let data_dir = tsk_env.data_dir();
        let json_path = data_dir.join("tasks.json");
        let bak_path = data_dir.join("tasks.json.bak");
        let db_path = data_dir.join("migration_bak_test.db");

        // Create both tasks.json and tasks.json.bak
        let task = Task {
            id: "skip1234".to_string(),
            repo_root: data_dir.to_path_buf(),
            name: "skip-task".to_string(),
            branch_name: "tsk/feat/skip-task/skip1234".to_string(),
            copied_repo_path: Some(data_dir.to_path_buf()),
            ..Task::test_default()
        };
        let json = serde_json::to_string_pretty(&vec![task]).unwrap();
        fs::write(&json_path, &json).unwrap();
        fs::write(&bak_path, "old backup").unwrap();

        let storage = TaskStorage::new(db_path).unwrap();

        // Migration should NOT have run -- DB should be empty
        let stored_tasks = storage.list_tasks().await.unwrap();
        assert_eq!(stored_tasks.len(), 0);

        // tasks.json should still exist (not renamed)
        assert!(json_path.exists());

        // Clean up
        let _ = fs::remove_file(&json_path);
        let _ = fs::remove_file(&bak_path);
    }

    #[tokio::test]
    async fn test_migration_skipped_when_db_has_data() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        tsk_env.ensure_directories().unwrap();

        let data_dir = tsk_env.data_dir();
        let json_path = data_dir.join("tasks.json");
        let bak_path = data_dir.join("tasks.json.bak");
        let db_path = data_dir.join("migration_existing_test.db");

        // First, create a storage and add a task to it
        let storage = TaskStorage::new(db_path.clone()).unwrap();
        let existing_task = Task {
            id: "existing1234".to_string(),
            repo_root: data_dir.to_path_buf(),
            name: "existing-task".to_string(),
            branch_name: "tsk/feat/existing-task/existing1234".to_string(),
            copied_repo_path: Some(data_dir.to_path_buf()),
            ..Task::test_default()
        };
        storage.add_task(existing_task).await.unwrap();
        drop(storage);

        // Now create tasks.json with different tasks
        let json_task = Task {
            id: "json5678".to_string(),
            repo_root: data_dir.to_path_buf(),
            name: "json-task".to_string(),
            branch_name: "tsk/feat/json-task/json5678".to_string(),
            copied_repo_path: Some(data_dir.to_path_buf()),
            ..Task::test_default()
        };
        let json = serde_json::to_string_pretty(&vec![json_task]).unwrap();
        fs::write(&json_path, &json).unwrap();

        // Re-open the storage -- migration should NOT run since DB has data
        let storage = TaskStorage::new(db_path).unwrap();
        let stored_tasks = storage.list_tasks().await.unwrap();
        assert_eq!(stored_tasks.len(), 1);
        assert_eq!(stored_tasks[0].id, "existing1234");

        // tasks.json should still exist (not renamed)
        assert!(json_path.exists());

        // Clean up
        let _ = fs::remove_file(&json_path);
        let _ = fs::remove_file(&bak_path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_writes_no_busy_errors() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let data_dir = tsk_env.data_dir().to_path_buf();

        let db_path = tsk_env.data_dir().join("concurrent_test.db");
        let storage1 = Arc::new(TaskStorage::new(db_path.clone()).unwrap());
        let storage2 = Arc::new(TaskStorage::new(db_path).unwrap());

        const TASKS_PER_WRITER: usize = 50;

        let spawn_writer = |storage: Arc<TaskStorage>, dir: PathBuf, writer_id: usize| {
            tokio::spawn(async move {
                for i in 0..TASKS_PER_WRITER {
                    let task = Task {
                        id: format!("w{writer_id}-t{i}"),
                        repo_root: dir.clone(),
                        name: format!("task-{writer_id}-{i}"),
                        branch_name: format!("tsk/feat/task-{writer_id}-{i}/w{writer_id}-t{i}"),
                        copied_repo_path: None,
                        ..Task::test_default()
                    };
                    storage.add_task(task).await.unwrap();
                }
            })
        };

        let h1 = spawn_writer(Arc::clone(&storage1), data_dir.clone(), 0);
        let h2 = spawn_writer(Arc::clone(&storage2), data_dir, 1);
        tokio::try_join!(h1, h2).unwrap();

        let tasks = storage1.list_tasks().await.unwrap();
        assert_eq!(tasks.len(), TASKS_PER_WRITER * 2);
    }

    #[tokio::test]
    async fn test_migration_handles_invalid_json() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        tsk_env.ensure_directories().unwrap();

        let data_dir = tsk_env.data_dir();
        let json_path = data_dir.join("tasks.json");
        let bak_path = data_dir.join("tasks.json.bak");
        let db_path = data_dir.join("migration_invalid_test.db");

        // Create tasks.json with invalid content
        fs::write(&json_path, "not valid json {{{").unwrap();

        let storage = TaskStorage::new(db_path).unwrap();

        // DB should be empty
        let stored_tasks = storage.list_tasks().await.unwrap();
        assert_eq!(stored_tasks.len(), 0);

        // tasks.json should be renamed to .bak even for invalid JSON
        assert!(!json_path.exists());
        assert!(bak_path.exists());

        // Clean up
        let _ = fs::remove_file(&bak_path);
    }

    #[tokio::test]
    async fn test_resolved_config_round_trip() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_resolved_config.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let config_json = r#"{"agent":"codex","stack":"rust","dind":true,"memory_gb":24.0,"cpu":16,"git_town":false,"host_ports":[5432],"setup":null,"stack_config":{},"agent_config":{},"volumes":[],"env":[]}"#;

        let task = Task {
            id: "config1234".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/feat/config-test/config1234".to_string(),
            copied_repo_path: Some(tsk_env.data_dir().to_path_buf()),
            resolved_config: Some(config_json.to_string()),
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();

        let retrieved = storage.get_task("config1234").await.unwrap().unwrap();
        assert_eq!(retrieved.resolved_config, Some(config_json.to_string()));

        // Verify JSON round-trip through ResolvedConfig
        let deserialized: crate::context::ResolvedConfig =
            serde_json::from_str(config_json).unwrap();
        assert_eq!(deserialized.memory_gb, 24.0);
        assert_eq!(deserialized.cpu, 16);
        assert_eq!(deserialized.host_ports, vec![5432]);
    }

    #[tokio::test]
    async fn test_resolved_config_null_backward_compat() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_null_config.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "null1234".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/feat/null-test/null1234".to_string(),
            copied_repo_path: Some(tsk_env.data_dir().to_path_buf()),
            resolved_config: None,
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();

        let retrieved = storage.get_task("null1234").await.unwrap().unwrap();
        assert!(retrieved.resolved_config.is_none());
    }

    #[tokio::test]
    async fn test_prepare_child_task_copies_resolved_config() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_child_config.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let parent_config = r#"{"agent":"claude","stack":"rust","dind":false,"memory_gb":12.0,"cpu":8,"git_town":false,"host_ports":[],"setup":null,"stack_config":{},"agent_config":{},"volumes":[],"env":[]}"#;

        // Child task starts with no resolved_config
        let child = Task {
            id: "child5678".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/feat/child/child5678".to_string(),
            parent_ids: vec!["parent5678".to_string()],
            resolved_config: None,
            ..Task::test_default()
        };
        storage.add_task(child).await.unwrap();

        let repo_path = tsk_env.data_dir().join("child-repo");
        let updated = storage
            .prepare_child_task(
                "child5678",
                repo_path,
                "new_commit",
                "tsk/feat/parent/parent5678",
                Some(parent_config),
            )
            .await
            .unwrap();

        assert_eq!(
            updated.resolved_config,
            Some(parent_config.to_string()),
            "Child should inherit parent's resolved_config"
        );
    }

    #[tokio::test]
    async fn test_mark_cancelled_from_running() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_cancel_running.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "cancel-run1".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/feat/cancel-test/cancel-run1".to_string(),
            copied_repo_path: Some(tsk_env.data_dir().to_path_buf()),
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();
        storage.mark_running("cancel-run1").await.unwrap();

        let updated = storage.mark_cancelled("cancel-run1").await.unwrap();
        assert_eq!(updated.status, TaskStatus::Cancelled);
        assert!(updated.completed_at.is_some());
    }

    #[tokio::test]
    async fn test_mark_cancelled_from_queued() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_cancel_queued.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "cancel-q1".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/feat/cancel-test/cancel-q1".to_string(),
            copied_repo_path: Some(tsk_env.data_dir().to_path_buf()),
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();

        let updated = storage.mark_cancelled("cancel-q1").await.unwrap();
        assert_eq!(updated.status, TaskStatus::Cancelled);
        assert!(updated.completed_at.is_some());
    }

    #[tokio::test]
    async fn test_mark_cancelled_already_terminal() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_cancel_terminal.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "cancel-done1".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/feat/cancel-test/cancel-done1".to_string(),
            copied_repo_path: Some(tsk_env.data_dir().to_path_buf()),
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();
        storage.mark_running("cancel-done1").await.unwrap();
        storage
            .mark_complete("cancel-done1", "tsk/feat/done/cancel-done1")
            .await
            .unwrap();

        // Cancelling an already-complete task should fail
        let result = storage.mark_cancelled("cancel-done1").await;
        assert!(
            result.is_err(),
            "Should error when cancelling a terminal task"
        );
    }

    #[tokio::test]
    async fn test_mark_failed_guard_on_cancelled_task() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_fail_guard.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "guard1".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/feat/guard-test/guard1".to_string(),
            copied_repo_path: Some(tsk_env.data_dir().to_path_buf()),
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();
        storage.mark_running("guard1").await.unwrap();
        storage.mark_cancelled("guard1").await.unwrap();

        // mark_failed should be a no-op (task is already CANCELLED)
        let result = storage
            .mark_failed("guard1", "should not apply")
            .await
            .unwrap();
        assert_eq!(
            result.status,
            TaskStatus::Cancelled,
            "Status should remain CANCELLED"
        );
        assert!(
            result.error_message.is_none(),
            "Error message should not be set by the guarded mark_failed"
        );
    }

    #[tokio::test]
    async fn test_mark_complete_guard_on_cancelled_task() {
        let ctx = AppContext::builder().build();
        let tsk_env = ctx.tsk_env();
        let db_path = tsk_env.data_dir().join("test_complete_guard.db");
        let storage = TaskStorage::new(db_path).unwrap();

        let task = Task {
            id: "guard2".to_string(),
            repo_root: tsk_env.data_dir().to_path_buf(),
            branch_name: "tsk/feat/guard-test/guard2".to_string(),
            copied_repo_path: Some(tsk_env.data_dir().to_path_buf()),
            ..Task::test_default()
        };
        storage.add_task(task).await.unwrap();
        storage.mark_running("guard2").await.unwrap();
        storage.mark_cancelled("guard2").await.unwrap();

        // mark_complete should be a no-op (task is already CANCELLED)
        let result = storage
            .mark_complete("guard2", "tsk/feat/new-branch/guard2")
            .await
            .unwrap();
        assert_eq!(
            result.status,
            TaskStatus::Cancelled,
            "Status should remain CANCELLED"
        );
    }
}
