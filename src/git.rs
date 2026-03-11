use crate::context::AppContext;
use crate::git_operations;
use chrono::{DateTime, Local};
use std::path::{Path, PathBuf};

/// Result of fetching changes back to the main repository.
#[derive(Debug)]
pub struct FetchResult {
    /// Whether the branch has new commits compared to the base.
    pub has_changes: bool,
    /// The actual branch name used, set when `has_changes` is true.
    pub branch_name: Option<String>,
    /// Non-fatal warnings encountered during the fetch (e.g. submodule or git-town issues).
    pub warnings: Vec<String>,
}

/// Result of copying a repository for a task.
#[derive(Debug)]
pub struct CopyResult {
    /// Path to the copied repository.
    pub repo_path: PathBuf,
    /// Non-fatal warnings encountered during the copy (e.g. submodule or LFS issues).
    pub warnings: Vec<String>,
}

/// Information about a git submodule parsed from .gitmodules
#[derive(Debug, Clone)]
struct SubmoduleInfo {
    /// The path to the submodule relative to the repository root
    path: String,
}

/// Manages repository operations including copying, committing, and fetching changes.
///
/// This struct provides high-level repository management functionality, coordinating
/// between file system operations, git operations, and synchronization management.
pub struct RepoManager {
    ctx: AppContext,
}

/// Recursively copy LFS objects from source to destination, skipping files that already exist.
/// LFS objects are content-addressed, so existing files are guaranteed to have identical content.
async fn copy_lfs_objects(src: &Path, dst: &Path) -> Result<(), String> {
    if !src.exists() {
        return Ok(());
    }

    let mut entries = tokio::fs::read_dir(src)
        .await
        .map_err(|e| format!("Failed to read LFS objects dir: {e}"))?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let src_path = entry.path();
        let file_name = entry.file_name();
        let dst_path = dst.join(&file_name);

        let meta = tokio::fs::symlink_metadata(&src_path)
            .await
            .map_err(|e| format!("Failed to stat {}: {e}", src_path.display()))?;

        if meta.is_dir() {
            Box::pin(copy_lfs_objects(&src_path, &dst_path)).await?;
        } else if meta.is_file() && !dst_path.exists() {
            if let Some(parent) = dst_path.parent() {
                crate::file_system::create_dir(parent)
                    .await
                    .map_err(|e| format!("Failed to create LFS dir: {e}"))?;
            }
            crate::file_system::copy_file(&src_path, &dst_path)
                .await
                .map_err(|e| format!("Failed to copy LFS object: {e}"))?;
        }
    }

    Ok(())
}

impl RepoManager {
    /// Creates a new RepoManager from the application context.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The application context providing all required dependencies
    pub fn new(ctx: &AppContext) -> Self {
        Self { ctx: ctx.clone() }
    }

    /// Copy repository for a task using the task ID and repository root
    ///
    /// Creates a task repository by cloning the source repository and overlaying
    /// all non-ignored files from the working directory including:
    /// - Tracked files with their current working directory content (including unstaged changes)
    /// - Staged files (newly added files in the index)
    /// - Untracked files (not ignored)
    /// - The .tsk directory for project-specific configurations
    ///
    /// The clone operation optimizes pack files (1-2 pack files instead of 30+)
    /// while the file overlay ensures the complete state of the repository as shown
    /// by `git status` is preserved.
    ///
    /// Returns a [`CopyResult`] with the path and any non-fatal warnings.
    pub async fn copy_repo(
        &self,
        task_id: &str,
        repo_root: &Path,
        source_commit: Option<&str>,
        branch_name: &str,
    ) -> Result<CopyResult, String> {
        // Use the task ID directly for the directory name
        let task_dir_name = task_id;
        let branch_name = branch_name.to_string();
        let mut warnings = Vec::new();

        // Create the task directory structure in centralized location
        let task_dir = self.ctx.tsk_env().task_dir(task_dir_name);
        let repo_path = task_dir.join("repo");

        // Create directories if they don't exist
        crate::file_system::create_dir(&task_dir)
            .await
            .map_err(|e| format!("Failed to create task directory: {e}"))?;

        // Check if the provided path is in a git repository
        if !git_operations::is_git_repository(repo_root).await? {
            return Err("Not in a git repository".to_string());
        }

        // Use the provided repository root
        let current_dir = repo_root.to_path_buf();

        // Get list of all files that should be copied:
        // 1. All tracked files (from working directory, including unstaged changes)
        // 2. All staged files (including newly added files in the index)
        // 3. All untracked files (not ignored)
        let all_files_to_copy = git_operations::get_all_non_ignored_files(&current_dir).await?;

        // Clone repository with optimized pack files (no hardlinks)
        // This creates an efficient repository copy with 1-2 pack files instead of
        // preserving fragmented pack structure from the source repository
        git_operations::clone_local(&current_dir, &repo_path)
            .await
            .map_err(|e| format!("Failed to clone repository: {e}"))?;

        // Copy .git/modules from source to preserve submodule git data
        // This must happen BEFORE branch creation and file overlay so that
        // submodule .git files have valid targets to point to
        let src_git_common = crate::repo_utils::resolve_git_common_dir(&current_dir)
            .unwrap_or_else(|_| current_dir.join(".git"));
        let src_modules = src_git_common.join("modules");
        let dst_modules = repo_path.join(".git/modules");

        if crate::file_system::exists(&src_modules)
            .await
            .unwrap_or(false)
            && let Err(e) = crate::file_system::copy_dir(&src_modules, &dst_modules).await
        {
            warnings.push(format!(
                "Failed to copy .git/modules: {e}. Submodules may not work correctly."
            ));
        }

        // Copy .git/lfs from source to preserve LFS objects
        // This avoids re-hashing large files when the clean filter runs
        let src_lfs = src_git_common.join("lfs");
        let dst_lfs = repo_path.join(".git/lfs");

        if crate::file_system::exists(&src_lfs).await.unwrap_or(false)
            && let Err(e) = crate::file_system::copy_dir(&src_lfs, &dst_lfs).await
        {
            warnings.push(format!(
                "Failed to copy .git/lfs: {e}. LFS files may need to be re-hashed."
            ));
        }

        // Check if repo uses LFS and whether git-lfs is available
        let gitattributes_path = current_dir.join(".gitattributes");
        let repo_uses_lfs =
            if let Ok(content) = tokio::fs::read_to_string(&gitattributes_path).await {
                content.contains("filter=lfs")
            } else {
                false
            };

        let git_lfs_available = if repo_uses_lfs {
            tokio::process::Command::new("git")
                .args(["lfs", "version"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await
                .is_ok_and(|s| s.success())
        } else {
            false
        };

        if repo_uses_lfs && !git_lfs_available {
            warnings.push(
                "Repository uses git-lfs but git-lfs is not installed. LFS files may not be handled correctly.".to_string()
            );
        }

        // Create a new branch in the cloned repository BEFORE overlaying files
        // This ensures that when source_commit is provided, the checkout doesn't
        // overwrite the working directory files we're about to overlay from the source
        match source_commit {
            Some(commit_sha) => {
                git_operations::create_branch_from_commit(&repo_path, &branch_name, commit_sha)
                    .await?;
            }
            None => {
                git_operations::create_branch(&repo_path, &branch_name).await?;
            }
        }

        // Fix submodule paths after copying .git/modules
        // This must happen BEFORE the file overlay so that when submodule .git files
        // are overlaid, they point to valid locations in .git/modules
        warnings.extend(self.fix_submodule_paths(&repo_path).await);

        // Overlay all non-ignored files from the source working directory
        // This happens AFTER branch creation to preserve unstaged changes over the cloned state
        for file_path in all_files_to_copy {
            // Remove trailing slash if present (git adds it for directories)
            let file_path_str = file_path.to_string_lossy();
            let file_path_clean = if let Some(stripped) = file_path_str.strip_suffix('/') {
                PathBuf::from(stripped)
            } else {
                file_path.clone()
            };

            let src_path = current_dir.join(&file_path_clean);
            let dst_path = repo_path.join(&file_path_clean);

            // Get metadata to check the actual entry type (not following symlinks)
            match tokio::fs::symlink_metadata(&src_path).await {
                Ok(metadata) => {
                    if metadata.is_dir() {
                        // It's an actual directory, copy it recursively
                        // Remove existing directory if present (from clone)
                        if dst_path.exists() {
                            tokio::fs::remove_dir_all(&dst_path).await.map_err(|e| {
                                format!(
                                    "Failed to remove existing directory {}: {e}",
                                    dst_path.display()
                                )
                            })?;
                        }
                        crate::file_system::copy_dir(&src_path, &dst_path)
                            .await
                            .map_err(|e| {
                                format!(
                                    "Failed to copy untracked directory {}: {e}",
                                    src_path.display()
                                )
                            })?;
                    } else if metadata.is_symlink() {
                        // It's a symlink - need special handling
                        // Create parent directory if it doesn't exist
                        if let Some(parent) = dst_path.parent() {
                            crate::file_system::create_dir(parent)
                                .await
                                .map_err(|e| format!("Failed to create parent directory: {e}"))?;
                        }

                        // Remove existing file/symlink if present (from clone)
                        if let Ok(dst_meta) = tokio::fs::symlink_metadata(&dst_path).await {
                            if dst_meta.is_symlink() || dst_meta.is_file() {
                                tokio::fs::remove_file(&dst_path).await.map_err(|e| {
                                    format!(
                                        "Failed to remove existing file {}: {e}",
                                        dst_path.display()
                                    )
                                })?;
                            } else if dst_meta.is_dir() {
                                tokio::fs::remove_dir_all(&dst_path).await.map_err(|e| {
                                    format!(
                                        "Failed to remove existing directory {}: {e}",
                                        dst_path.display()
                                    )
                                })?;
                            }
                        }

                        // Read the symlink target and recreate it
                        let target = tokio::fs::read_link(&src_path).await.map_err(|e| {
                            format!("Failed to read symlink {}: {}", src_path.display(), e)
                        })?;

                        #[cfg(unix)]
                        tokio::fs::symlink(&target, &dst_path).await.map_err(|e| {
                            format!("Failed to create symlink {}: {}", dst_path.display(), e)
                        })?;

                        #[cfg(windows)]
                        {
                            // On Windows, determine if it's a file or directory symlink
                            if let Ok(target_meta) = tokio::fs::metadata(&src_path).await {
                                if target_meta.is_dir() {
                                    tokio::fs::symlink_dir(&target, &dst_path).await.map_err(
                                        |e| {
                                            format!(
                                                "Failed to create directory symlink {}: {}",
                                                dst_path.display(),
                                                e
                                            )
                                        },
                                    )?;
                                } else {
                                    tokio::fs::symlink_file(&target, &dst_path).await.map_err(
                                        |e| {
                                            format!(
                                                "Failed to create file symlink {}: {}",
                                                dst_path.display(),
                                                e
                                            )
                                        },
                                    )?;
                                }
                            } else {
                                // Default to file symlink if we can't determine
                                tokio::fs::symlink_file(&target, &dst_path)
                                    .await
                                    .map_err(|e| {
                                        format!(
                                            "Failed to create symlink {}: {}",
                                            dst_path.display(),
                                            e
                                        )
                                    })?;
                            }
                        }
                    } else {
                        // It's a regular file
                        // Create parent directory if it doesn't exist
                        if let Some(parent) = dst_path.parent() {
                            crate::file_system::create_dir(parent)
                                .await
                                .map_err(|e| format!("Failed to create parent directory: {e}"))?;
                        }

                        // Remove existing file if present (from clone) to ensure overlay
                        if let Ok(dst_meta) = tokio::fs::symlink_metadata(&dst_path).await
                            && (dst_meta.is_file() || dst_meta.is_symlink())
                        {
                            tokio::fs::remove_file(&dst_path).await.map_err(|e| {
                                format!(
                                    "Failed to remove existing file {}: {e}",
                                    dst_path.display()
                                )
                            })?;
                        }

                        // Copy the file
                        crate::file_system::copy_file(&src_path, &dst_path)
                            .await
                            .map_err(|e| {
                                format!("Failed to copy untracked file {}: {e}", src_path.display())
                            })?;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // File doesn't exist - might be because git reported a directory with trailing slash
                    // that we already processed. Skip it.
                    continue;
                }
                Err(e) => {
                    return Err(format!(
                        "Failed to get metadata for {}: {}",
                        src_path.display(),
                        e
                    ));
                }
            }
        }

        // Renormalize files to fix LFS index stat cache after overlay
        if repo_uses_lfs
            && git_lfs_available
            && let Err(e) = git_operations::renormalize(&repo_path).await
        {
            warnings.push(format!(
                "Failed to renormalize LFS files: {e}. LFS files may appear modified in git status."
            ));
        }

        // Copy .tsk directory if it exists (for project-specific Docker configurations)
        // Remove existing .tsk if present (from clone) to ensure overlay
        let tsk_src = current_dir.join(".tsk");
        let tsk_dst = repo_path.join(".tsk");
        if crate::file_system::exists(&tsk_src)
            .await
            .map_err(|e| format!("Failed to check if .tsk exists: {e}"))?
        {
            if tsk_dst.exists() {
                tokio::fs::remove_dir_all(&tsk_dst)
                    .await
                    .map_err(|e| format!("Failed to remove existing .tsk directory: {e}"))?;
            }
            crate::file_system::copy_dir(&tsk_src, &tsk_dst)
                .await
                .map_err(|e| format!("Failed to copy .tsk directory: {e}"))?;
        }

        Ok(CopyResult {
            repo_path,
            warnings,
        })
    }

    /// Commit any uncommitted changes in submodules before the superproject commit.
    /// This ensures submodule changes are captured and the superproject pointer is updated.
    /// Returns a tuple of (committed submodule paths, non-fatal warnings).
    async fn commit_submodule_changes(
        &self,
        repo_path: &Path,
        message: &str,
    ) -> Result<(Vec<String>, Vec<String>), String> {
        let submodules = self.parse_gitmodules(repo_path).await?;

        if submodules.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let mut committed_submodules = Vec::new();
        let mut warnings = Vec::new();

        for submodule in submodules {
            let submodule_path = repo_path.join(&submodule.path);

            // Check if the submodule is a valid git repository
            if !git_operations::is_git_repository(&submodule_path)
                .await
                .unwrap_or(false)
            {
                continue;
            }

            // Check if submodule has uncommitted changes
            let status_output = match git_operations::get_status(&submodule_path).await {
                Ok(output) => output,
                Err(e) => {
                    warnings.push(format!(
                        "Failed to get status for submodule '{}': {}",
                        submodule.path, e
                    ));
                    continue;
                }
            };

            if status_output.trim().is_empty() {
                continue;
            }

            // Add and commit changes in the submodule
            if let Err(e) = git_operations::add_all(&submodule_path).await {
                warnings.push(format!(
                    "Failed to stage changes in submodule '{}': {}",
                    submodule.path, e
                ));
                continue;
            }

            if let Err(e) = git_operations::commit(&submodule_path, message).await {
                warnings.push(format!(
                    "Failed to commit changes in submodule '{}': {}",
                    submodule.path, e
                ));
                continue;
            }

            committed_submodules.push(submodule.path);
        }

        Ok((committed_submodules, warnings))
    }

    /// Commit any uncommitted changes in the repository.
    /// This first commits changes in any submodules, then commits in the superproject.
    /// Returns any non-fatal warnings encountered during the commit process.
    pub async fn commit_changes(
        &self,
        repo_path: &Path,
        message: &str,
    ) -> Result<Vec<String>, String> {
        let mut warnings = Vec::new();

        // First commit any uncommitted changes in submodules
        // This must happen before the superproject commit so the submodule pointers are updated
        match self.commit_submodule_changes(repo_path, message).await {
            Ok((_committed, sub_warnings)) => warnings.extend(sub_warnings),
            Err(e) => {
                warnings.push(format!("Failed to commit submodule changes: {e}"));
                // Continue with superproject commit even if submodule commits fail
            }
        }

        // Check if there are any changes to commit
        let status_output = git_operations::get_status(repo_path).await?;

        if status_output.trim().is_empty() {
            return Ok(warnings);
        }

        // Add all changes
        git_operations::add_all(repo_path).await?;

        // Commit changes
        git_operations::commit(repo_path, message).await?;

        Ok(warnings)
    }

    /// Set git-town parent branch configuration
    ///
    /// This writes to git config without triggering any git operations.
    /// Should be called within a repo lock to prevent concurrent config writes.
    ///
    /// # Arguments
    ///
    /// * `repo_path` - Path to the repository where the config should be set
    /// * `branch_name` - Name of the branch to set the parent for
    /// * `parent_branch` - Name of the parent branch
    pub async fn set_git_town_parent(
        &self,
        repo_path: &Path,
        branch_name: &str,
        parent_branch: &str,
    ) -> Result<(), String> {
        tokio::task::spawn_blocking({
            let repo_path = repo_path.to_owned();
            let branch_name = branch_name.to_owned();
            let parent_branch = parent_branch.to_owned();
            move || -> Result<(), String> {
                let repo = git2::Repository::open(&repo_path)
                    .map_err(|e| format!("Failed to open repository: {e}"))?;

                let mut config = repo
                    .config()
                    .map_err(|e| format!("Failed to get repository config: {e}"))?;

                let key = format!("git-town-branch.{}.parent", branch_name);
                config
                    .set_str(&key, &parent_branch)
                    .map_err(|e| format!("Failed to set git-town parent: {e}"))?;

                Ok(())
            }
        })
        .await
        .map_err(|e| format!("Task join error: {e}"))?
    }

    /// Fetch changes from the copied repository back to the main repository.
    /// Returns a `FetchResult` with `has_changes: false` if the branch has no new commits.
    pub async fn fetch_changes(
        &self,
        repo_path: &Path,
        branch_name: &str,
        repo_root: &Path,
        source_commit: &str,
        source_branch: Option<&str>,
        git_town_enabled: bool,
    ) -> Result<FetchResult, String> {
        // Check if there are any changes by comparing HEAD with source commit
        let current_head = git_operations::get_current_commit(repo_path).await?;
        if current_head == source_commit {
            return Ok(FetchResult {
                has_changes: false,
                branch_name: None,
                warnings: vec![],
            });
        }

        let repo_path_str = repo_path
            .to_str()
            .ok_or_else(|| "Invalid repo path".to_string())?;

        // Use the provided repository root
        let main_repo = repo_root.to_path_buf();

        // Add the copied repository as a remote in the main repository
        let now: DateTime<Local> = Local::now();
        let remote_name = format!("tsk-temp-{}", now.format("%Y-%m-%d-%H%M%S"));

        // Synchronize git operations on the main repository
        let (has_commits, warnings) = self
            .ctx
            .git_sync_manager()
            .with_repo_lock(&main_repo, || async {
                let mut warnings: Vec<String> = Vec::new();

                git_operations::add_remote(&main_repo, &remote_name, repo_path_str).await?;

                // Validate that the branch is accessible before attempting fetch
                if let Err(e) =
                    git_operations::validate_branch_accessible(repo_path, branch_name).await
                {
                    // Clean up remote before returning error
                    let _ = git_operations::remove_remote(&main_repo, &remote_name).await;
                    return Err(format!(
                        "Cannot fetch branch '{}': {}\n\
                         The branch was created but points to an inaccessible commit.\n\
                         This may indicate git object database inconsistency.",
                        branch_name, e
                    ));
                }

                // Fetch the specific branch from the remote
                match git_operations::fetch_branch(&main_repo, &remote_name, branch_name).await {
                    Ok(_) => {
                        // Remove the temporary remote
                        git_operations::remove_remote(&main_repo, &remote_name).await?;

                        // Set git-town parent if enabled and source branch is known
                        if git_town_enabled
                            && let Some(parent) = source_branch
                            && let Err(e) = self
                                .set_git_town_parent(&main_repo, branch_name, parent)
                                .await
                        {
                            warnings.push(format!("Failed to set git-town parent: {e}"));
                        }
                    }
                    Err(e) => {
                        // Remove the temporary remote before returning error
                        let _ = git_operations::remove_remote(&main_repo, &remote_name).await;
                        return Err(e);
                    }
                }

                // Fetch submodule changes back to original submodules
                // This ensures commits made by agents in submodules are available in the original repo
                match self
                    .fetch_submodule_changes(repo_path, repo_root, branch_name)
                    .await
                {
                    Ok(sub_warnings) => warnings.extend(sub_warnings),
                    Err(e) => {
                        warnings.push(format!("Failed to fetch submodule changes: {e}"));
                    }
                }

                // Copy LFS objects from task repo to main repo
                // When an agent modifies LFS-tracked files, new LFS objects are created
                // in the task repo. Without copying them, checking out the tsk branch
                // in the main repo would fail because git-lfs can't find the objects.
                {
                    let task_lfs_objects = repo_path.join(".git/lfs/objects");
                    if task_lfs_objects.exists() {
                        let main_git_common = crate::repo_utils::resolve_git_common_dir(&main_repo)
                            .unwrap_or_else(|_| main_repo.join(".git"));
                        let main_lfs_objects = main_git_common.join("lfs/objects");
                        if let Err(e) = copy_lfs_objects(&task_lfs_objects, &main_lfs_objects).await
                        {
                            warnings.push(format!("Failed to copy LFS objects: {e}"));
                        }
                    }
                }

                // Check if the fetched branch has any commits not in main
                let has_commits =
                    git_operations::has_commits_not_in_base(&main_repo, branch_name, "main")
                        .await?;

                if !has_commits
                    && let Err(e) = git_operations::delete_branch(&main_repo, branch_name).await
                {
                    warnings.push(format!("Failed to delete branch {branch_name}: {e}"));
                }

                Ok::<(bool, Vec<String>), String>((has_commits, warnings))
            })
            .await?;

        Ok(FetchResult {
            has_changes: has_commits,
            branch_name: if has_commits {
                Some(branch_name.to_string())
            } else {
                None
            },
            warnings,
        })
    }

    /// Fix submodule paths after copying .git/modules to the destination.
    /// This finds all .git files (submodule indicators) and fixes their gitdir paths,
    /// then fixes worktree paths in .git/modules/*/config files.
    /// Returns any non-fatal warnings encountered during the fix.
    async fn fix_submodule_paths(&self, repo_path: &Path) -> Vec<String> {
        let mut warnings = Vec::new();

        // Find all .git files that indicate submodules
        let git_files = self.find_submodule_git_files(repo_path).await;

        for git_file in git_files {
            if let Err(e) = self.fix_submodule_gitdir_path(&git_file, repo_path).await {
                warnings.push(format!(
                    "Failed to fix gitdir path in {}: {}. Removing broken .git file.",
                    git_file.display(),
                    e
                ));
                // Remove the broken .git file so the submodule is treated as regular files
                let _ = tokio::fs::remove_file(&git_file).await;
            }
        }

        // Fix worktree paths in .git/modules/*/config files
        match self.fix_module_worktree_paths(repo_path).await {
            Ok(w) => warnings.extend(w),
            Err(e) => {
                warnings.push(format!("Failed to fix module worktree paths: {e}"));
            }
        }

        warnings
    }

    /// Find all .git files (not directories) in the repository that contain gitdir:.
    /// These files indicate submodule directories.
    async fn find_submodule_git_files(&self, repo_path: &Path) -> Vec<PathBuf> {
        let mut result = Vec::new();
        self.find_git_files_recursive(repo_path, repo_path, &mut result)
            .await;
        result
    }

    /// Recursively search for .git files (submodule indicators) in a directory.
    async fn find_git_files_recursive(
        &self,
        base_path: &Path,
        current_path: &Path,
        result: &mut Vec<PathBuf>,
    ) {
        let Ok(mut entries) = tokio::fs::read_dir(current_path).await else {
            return;
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let file_name = path.file_name().unwrap_or_default();

            // Skip the .git directory itself (the main repo's git directory)
            if file_name == ".git" {
                // Check if it's a file (submodule indicator) or directory (main repo)
                // If it's a file with gitdir: prefix, add it to results
                if let Ok(meta) = tokio::fs::symlink_metadata(&path).await
                    && meta.is_file()
                    && let Ok(content) = tokio::fs::read_to_string(&path).await
                    && content.starts_with("gitdir:")
                {
                    result.push(path);
                }
                // If it's a directory, skip it (main repo's .git)
                continue;
            }

            // Recurse into directories (but not symlinks)
            if let Ok(meta) = tokio::fs::symlink_metadata(&path).await
                && meta.is_dir()
                && !meta.file_type().is_symlink()
            {
                Box::pin(self.find_git_files_recursive(base_path, &path, result)).await;
            }
        }
    }

    /// Fix the gitdir path in a submodule .git file if it uses an absolute path.
    async fn fix_submodule_gitdir_path(
        &self,
        git_file: &Path,
        repo_path: &Path,
    ) -> Result<(), String> {
        let content = tokio::fs::read_to_string(git_file)
            .await
            .map_err(|e| format!("Failed to read {}: {}", git_file.display(), e))?;

        let gitdir_line = content.trim();
        if !gitdir_line.starts_with("gitdir: ") {
            return Err(format!("Invalid .git file format: {}", git_file.display()));
        }

        let gitdir_path = gitdir_line.strip_prefix("gitdir: ").unwrap_or("").trim();

        // Check if it's an absolute path (starts with / or contains : for Windows)
        if gitdir_path.starts_with('/') || gitdir_path.contains(':') {
            // Absolute path needs rewriting
            // Extract the module path from the absolute path
            if let Some(modules_pos) = gitdir_path.find("/.git/modules/") {
                let module_path = &gitdir_path[modules_pos + "/.git/modules/".len()..];
                let submodule_dir = git_file
                    .parent()
                    .ok_or_else(|| "No parent directory".to_string())?;
                let depth = submodule_dir
                    .strip_prefix(repo_path)
                    .map_err(|_| "Path not under repo".to_string())?
                    .components()
                    .count();

                let prefix = if depth > 0 {
                    "../".repeat(depth)
                } else {
                    "./".to_string()
                };
                let new_gitdir = format!("{}.git/modules/{}", prefix, module_path);

                tokio::fs::write(git_file, format!("gitdir: {}\n", new_gitdir))
                    .await
                    .map_err(|e| format!("Failed to write {}: {}", git_file.display(), e))?;
            } else {
                return Err(format!(
                    "Could not extract module path from absolute gitdir: {}",
                    gitdir_path
                ));
            }
        }
        // Relative paths should work as-is since the structure is preserved

        Ok(())
    }

    /// Fix worktree paths in .git/modules/*/config files if they use absolute paths.
    /// Returns any warnings encountered during the fix.
    async fn fix_module_worktree_paths(&self, repo_path: &Path) -> Result<Vec<String>, String> {
        let modules_dir = repo_path.join(".git/modules");
        if !modules_dir.exists() {
            return Ok(Vec::new());
        }

        self.fix_worktree_recursive(&modules_dir, repo_path).await
    }

    /// Recursively fix worktree paths in module config files.
    /// Returns any warnings encountered during the fix.
    async fn fix_worktree_recursive(
        &self,
        modules_dir: &Path,
        repo_path: &Path,
    ) -> Result<Vec<String>, String> {
        let mut entries = tokio::fs::read_dir(modules_dir)
            .await
            .map_err(|e| format!("Failed to read {}: {}", modules_dir.display(), e))?;

        let mut warnings = Vec::new();

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();

            // Check metadata to see if it's a directory
            let Ok(meta) = tokio::fs::symlink_metadata(&path).await else {
                continue;
            };

            if !meta.is_dir() {
                continue;
            }

            let config_path = path.join("config");
            if config_path.exists() {
                match self
                    .fix_single_worktree_config(&config_path, repo_path)
                    .await
                {
                    Ok(w) => warnings.extend(w),
                    Err(e) => {
                        warnings.push(format!(
                            "Failed to fix worktree in {}: {}",
                            config_path.display(),
                            e
                        ));
                    }
                }
            }

            // Handle nested modules (sub-submodules)
            let nested_modules = path.join("modules");
            if nested_modules.exists() {
                // Use Box::pin for recursive async call
                let nested_warnings =
                    Box::pin(self.fix_worktree_recursive(&nested_modules, repo_path)).await?;
                warnings.extend(nested_warnings);
            }
        }

        Ok(warnings)
    }

    /// Fix a single module config file's worktree path if it uses an absolute path.
    /// Returns any warnings about absolute paths that could not be automatically fixed.
    async fn fix_single_worktree_config(
        &self,
        config_path: &Path,
        _repo_path: &Path,
    ) -> Result<Vec<String>, String> {
        let content = tokio::fs::read_to_string(config_path)
            .await
            .map_err(|e| format!("Read error: {}", e))?;

        let mut warnings = Vec::new();

        for line in content.lines() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("worktree = ") {
                let worktree_value = trimmed.strip_prefix("worktree = ").unwrap_or("").trim();
                // Check if absolute path
                if worktree_value.starts_with('/') || worktree_value.contains(':') {
                    // Absolute path found - collect warning
                    // Fixing this is complex without knowing the original structure,
                    // so we just warn for now
                    warnings.push(format!(
                        "Absolute worktree path found in {}: {}. Manual fixing may be required.",
                        config_path.display(),
                        worktree_value
                    ));
                }
            }
        }

        Ok(warnings)
    }

    /// Fetch submodule changes from the copied repository back to the original repository.
    /// This ensures that commits made by the agent in submodules are available in the original repo.
    /// The branch_name is used to create matching branch names in the submodule.
    /// Returns any non-fatal warnings encountered during the fetch.
    async fn fetch_submodule_changes(
        &self,
        copied_repo: &Path,
        original_repo: &Path,
        branch_name: &str,
    ) -> Result<Vec<String>, String> {
        let submodules = self.parse_gitmodules(copied_repo).await?;

        if submodules.is_empty() {
            return Ok(Vec::new());
        }

        let mut warnings = Vec::new();

        for submodule in submodules {
            // Wrap each submodule fetch in error handling - collect warnings but don't fail overall
            if let Err(e) = self
                .fetch_single_submodule_changes(copied_repo, original_repo, &submodule, branch_name)
                .await
            {
                warnings.push(format!(
                    "Failed to fetch changes for submodule '{}': {}",
                    submodule.path, e
                ));
            }
        }

        Ok(warnings)
    }

    /// Fetch changes for a single submodule from copied repo to original repo.
    /// The branch_name is used to create a matching branch in the original submodule.
    async fn fetch_single_submodule_changes(
        &self,
        copied_repo: &Path,
        original_repo: &Path,
        submodule: &SubmoduleInfo,
        branch_name: &str,
    ) -> Result<(), String> {
        // Find the module git directory by reading the submodule's .git file
        let copied_submodule_path = copied_repo.join(&submodule.path);
        let copied_git_file = copied_submodule_path.join(".git");

        if !copied_git_file.exists() {
            // Submodule wasn't properly set up in copied repo, skip it
            return Ok(());
        }

        // Read the .git file to find the module directory
        let git_content = tokio::fs::read_to_string(&copied_git_file)
            .await
            .map_err(|e| format!("Failed to read .git file: {}", e))?;

        let gitdir_line = git_content.trim();
        if !gitdir_line.starts_with("gitdir: ") {
            return Err("Invalid .git file format in submodule".to_string());
        }

        let gitdir_rel_path = gitdir_line.strip_prefix("gitdir: ").unwrap_or("").trim();

        // Resolve the gitdir path relative to the submodule directory
        let copied_module_git = if gitdir_rel_path.starts_with('/') {
            // Absolute path (unusual, but handle it)
            PathBuf::from(gitdir_rel_path)
        } else {
            // Relative path - resolve from submodule directory
            copied_submodule_path.join(gitdir_rel_path)
        };

        // Canonicalize to resolve .. components
        let copied_module_git = copied_module_git.canonicalize().map_err(|e| {
            format!(
                "Failed to resolve module git path '{}': {}",
                copied_module_git.display(),
                e
            )
        })?;

        if !copied_module_git.exists() {
            return Err(format!(
                "Module git directory does not exist: {}",
                copied_module_git.display()
            ));
        }

        // Check if original submodule exists and is a valid git repository
        let original_submodule = original_repo.join(&submodule.path);
        if !original_submodule.exists() {
            // Submodule doesn't exist in original - nothing to fetch to
            return Ok(());
        }

        let is_git_repo = git_operations::is_git_repository(&original_submodule)
            .await
            .unwrap_or(false);

        if !is_git_repo {
            // Original submodule is not a valid git repo (might be uninitialized)
            return Ok(());
        }

        // Add the copied module as a remote and fetch all refs
        let now: DateTime<Local> = Local::now();
        let remote_name = format!("tsk-sub-temp-{}", now.format("%Y%m%d-%H%M%S-%3f"));
        let copied_module_git_str = copied_module_git
            .to_str()
            .ok_or("Invalid module git path")?;

        // Add remote
        if let Err(e) =
            git_operations::add_remote(&original_submodule, &remote_name, copied_module_git_str)
                .await
        {
            return Err(format!("Failed to add remote: {}", e));
        }

        // Determine the current branch in the copied submodule (HEAD)
        // This is what we want to fetch - it contains the agent's commits
        let head_output = tokio::process::Command::new("git")
            .current_dir(&copied_submodule_path)
            .args(["rev-parse", "HEAD"])
            .output()
            .await;

        let copied_head_sha = match head_output {
            Ok(output) if output.status.success() => {
                String::from_utf8_lossy(&output.stdout).trim().to_string()
            }
            _ => {
                // Clean up remote and return error
                let _ = git_operations::remove_remote(&original_submodule, &remote_name).await;
                return Err("Failed to get HEAD of copied submodule".to_string());
            }
        };

        // Get the HEAD SHA from the original submodule to check if there are any new commits
        let original_head_output = tokio::process::Command::new("git")
            .current_dir(&original_submodule)
            .args(["rev-parse", "HEAD"])
            .output()
            .await;

        let original_head_sha = match original_head_output {
            Ok(output) if output.status.success() => {
                String::from_utf8_lossy(&output.stdout).trim().to_string()
            }
            _ => {
                // Clean up remote and return error
                let _ = git_operations::remove_remote(&original_submodule, &remote_name).await;
                return Err("Failed to get HEAD of original submodule".to_string());
            }
        };

        // If the HEADs are the same, there are no new commits in this submodule - skip branch creation
        if copied_head_sha == original_head_sha {
            // Clean up remote and return - no branch needed for unchanged submodule
            let _ = git_operations::remove_remote(&original_submodule, &remote_name).await;
            return Ok(());
        }

        // Fetch the HEAD commit and create a branch with the same name as the superproject branch
        // This makes submodule branches match the superproject branch for easier correlation
        // The refspec fetches the specific commit and creates a local branch with the task branch name
        let refspec = format!("+{}:refs/heads/{}", copied_head_sha, branch_name);
        let fetch_result = tokio::process::Command::new("git")
            .current_dir(&original_submodule)
            .args(["fetch", &remote_name, &refspec])
            .output()
            .await;

        // Always clean up the remote, regardless of fetch result
        let _ = git_operations::remove_remote(&original_submodule, &remote_name).await;

        // Check fetch result
        match fetch_result {
            Ok(output) => {
                if !output.status.success() {
                    return Err(format!(
                        "git fetch failed: {}",
                        String::from_utf8_lossy(&output.stderr)
                    ));
                }
            }
            Err(e) => {
                return Err(format!("Failed to execute git fetch: {}", e));
            }
        }

        Ok(())
    }

    /// Parse the .gitmodules file to extract submodule paths.
    async fn parse_gitmodules(&self, repo_path: &Path) -> Result<Vec<SubmoduleInfo>, String> {
        let gitmodules_path = repo_path.join(".gitmodules");
        if !gitmodules_path.exists() {
            return Ok(Vec::new());
        }

        let content = tokio::fs::read_to_string(&gitmodules_path)
            .await
            .map_err(|e| format!("Failed to read .gitmodules: {}", e))?;

        let mut submodules = Vec::new();
        let mut current_path: Option<String> = None;

        for line in content.lines() {
            let line = line.trim();
            if line.starts_with("[submodule ") {
                // Save previous submodule if path was found
                if let Some(path) = current_path.take() {
                    submodules.push(SubmoduleInfo { path });
                }
            } else if let Some(path_value) = line.strip_prefix("path = ") {
                current_path = Some(path_value.to_string());
            }
        }

        // Don't forget the last submodule
        if let Some(path) = current_path {
            submodules.push(SubmoduleInfo { path });
        }

        Ok(submodules)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::AppContext;
    use crate::test_utils::{ExistingGitRepository, TestGitRepository};

    #[tokio::test]
    async fn test_copy_repo_not_in_git_repo() {
        let ctx = AppContext::builder().build();

        // Create a directory that is not a git repo
        let non_git_repo = TestGitRepository::new().unwrap();
        non_git_repo.setup_non_git_directory().unwrap();

        let manager = RepoManager::new(&ctx);

        let result = manager
            .copy_repo(
                "abcd1234",
                non_git_repo.path(),
                None,
                "tsk/test/test-task/abcd1234",
            )
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Not in a git repository");
    }

    #[tokio::test]
    async fn test_set_git_town_parent() {
        // Create a git repository with an initial commit
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        // Create a branch to set the parent for
        let branch_name = "tsk/feat/test-feature/abc123";
        test_repo.checkout_new_branch(branch_name).unwrap();

        let ctx = AppContext::builder().build();
        let manager = RepoManager::new(&ctx);

        // Set the git-town parent
        let result = manager
            .set_git_town_parent(test_repo.path(), branch_name, "main")
            .await;

        assert!(result.is_ok(), "Error: {result:?}");

        // Verify the config was set correctly
        let repo = git2::Repository::open(test_repo.path()).unwrap();
        let config = repo.config().unwrap();
        let key = format!("git-town-branch.{}.parent", branch_name);
        let parent = config.get_string(&key).unwrap();
        assert_eq!(parent, "main");
    }

    #[tokio::test]
    async fn test_set_git_town_parent_idempotent() {
        // Create a git repository with an initial commit
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        let branch_name = "tsk/feat/test-feature/abc123";
        test_repo.checkout_new_branch(branch_name).unwrap();

        let ctx = AppContext::builder().build();
        let manager = RepoManager::new(&ctx);

        // Set the parent twice - should not fail
        let result1 = manager
            .set_git_town_parent(test_repo.path(), branch_name, "main")
            .await;
        assert!(result1.is_ok());

        let result2 = manager
            .set_git_town_parent(test_repo.path(), branch_name, "develop")
            .await;
        assert!(result2.is_ok());

        // Verify the config was updated to the second value
        let repo = git2::Repository::open(test_repo.path()).unwrap();
        let config = repo.config().unwrap();
        let key = format!("git-town-branch.{}.parent", branch_name);
        let parent = config.get_string(&key).unwrap();
        assert_eq!(parent, "develop");
    }

    #[tokio::test]
    async fn test_commit_changes_no_changes() {
        // Create a git repository with an initial commit
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        let ctx = AppContext::builder().build();
        let manager = RepoManager::new(&ctx);

        // Test committing when there are no changes
        let result = manager
            .commit_changes(test_repo.path(), "Test commit")
            .await;

        assert!(result.is_ok(), "Error: {result:?}");
    }

    #[tokio::test]
    async fn test_commit_changes_with_changes() {
        // Create a git repository with uncommitted changes
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        // Modify the existing file to create changes
        test_repo
            .create_file("README.md", "# Test Repository\n\nModified content\n")
            .unwrap();

        let ctx = AppContext::builder().build();
        let manager = RepoManager::new(&ctx);

        let result = manager
            .commit_changes(test_repo.path(), "Test commit")
            .await;

        assert!(result.is_ok(), "Error: {result:?}");
    }

    #[tokio::test]
    async fn test_fetch_changes_no_commits() {
        let ctx = AppContext::builder().build();

        // Create main repository
        let main_repo = TestGitRepository::new().unwrap();
        main_repo.init_with_main_branch().unwrap();

        // Create task repository (simulating a copied repository)
        let task_repo = TestGitRepository::new().unwrap();
        task_repo.clone_from(&main_repo).unwrap();

        // Create a branch in task repo with no new commits
        let branch_name = "tsk/test/test-task/abcd1234";
        task_repo.checkout_new_branch(branch_name).unwrap();

        // Don't add any new commits - just the branch
        // The source_commit is the current HEAD (same as before creating the branch)
        let source_commit = task_repo.get_current_commit().unwrap();

        let manager = RepoManager::new(&ctx);

        // Fetch changes from task repo to main repo (should return false as there are no new commits)
        let result = manager
            .fetch_changes(
                task_repo.path(),
                branch_name,
                main_repo.path(),
                &source_commit,
                Some("main"),
                false,
            )
            .await;

        assert!(result.is_ok(), "Error: {result:?}");
        assert!(
            !result.unwrap().has_changes,
            "Should return false when no new commits"
        );

        // Verify the branch was cleaned up in main repo
        let main_branches = main_repo.branches().unwrap();
        assert!(
            !main_branches.contains(&branch_name.to_string()),
            "Branch should be cleaned up when no commits"
        );
    }

    #[tokio::test]
    async fn test_fetch_changes_with_commits() {
        let ctx = AppContext::builder().build();

        // Create main repository
        let main_repo = TestGitRepository::new().unwrap();
        main_repo.init_with_main_branch().unwrap();

        // Create task repository (simulating a copied repository)
        let task_repo = TestGitRepository::new().unwrap();
        task_repo.clone_from(&main_repo).unwrap();

        // Create a branch in task repo with new commits
        let branch_name = "tsk/test/test-task/efgh5678";
        task_repo.checkout_new_branch(branch_name).unwrap();

        // Capture source_commit before adding new commits
        let source_commit = task_repo.get_current_commit().unwrap();

        // Add a new commit
        task_repo
            .create_file("new_feature.rs", "fn new_feature() {}")
            .unwrap();
        task_repo.stage_all().unwrap();
        task_repo.commit("Add new feature").unwrap();

        let manager = RepoManager::new(&ctx);

        // Fetch changes from task repo to main repo (should return true as there are new commits)
        let result = manager
            .fetch_changes(
                task_repo.path(),
                branch_name,
                main_repo.path(),
                &source_commit,
                Some("main"),
                false,
            )
            .await;

        assert!(result.is_ok(), "Error: {result:?}");
        assert!(
            result.unwrap().has_changes,
            "Should return true when new commits exist"
        );

        // Verify the branch exists in main repo
        let main_branches = main_repo.branches().unwrap();
        assert!(
            main_branches.contains(&branch_name.to_string()),
            "Branch should exist after fetch"
        );
    }

    #[tokio::test]
    async fn test_fetch_changes_with_git_town_enabled() {
        let ctx = AppContext::builder().build();

        // Create main repository with a feature branch
        let main_repo = TestGitRepository::new().unwrap();
        main_repo.init_with_main_branch().unwrap();

        // Create a feature branch (this would be the source branch)
        main_repo.checkout_new_branch("feature-branch").unwrap();
        main_repo.create_file("feature.md", "# Feature\n").unwrap();
        main_repo.stage_all().unwrap();
        main_repo.commit("Add feature file").unwrap();

        // Create task repository (simulating a copied repository)
        let task_repo = TestGitRepository::new().unwrap();
        task_repo.clone_from(&main_repo).unwrap();

        // Create a task branch with new commits
        let branch_name = "tsk/feat/test-task/abc12345";
        task_repo.checkout_new_branch(branch_name).unwrap();

        // Capture source_commit before adding new commits
        let source_commit = task_repo.get_current_commit().unwrap();

        // Add a new commit
        task_repo
            .create_file("task_changes.rs", "fn task_work() {}")
            .unwrap();
        task_repo.stage_all().unwrap();
        task_repo.commit("Task changes").unwrap();

        let manager = RepoManager::new(&ctx);

        // Fetch changes with git_town_enabled = true
        let result = manager
            .fetch_changes(
                task_repo.path(),
                branch_name,
                main_repo.path(),
                &source_commit,
                Some("feature-branch"),
                true, // git_town_enabled
            )
            .await;

        assert!(result.is_ok(), "Error: {result:?}");
        let result = result.unwrap();
        assert!(
            result.has_changes,
            "Should return true when new commits exist"
        );
        assert!(
            result.warnings.is_empty(),
            "No warnings expected on happy path"
        );

        // Verify the branch exists in main repo
        let main_branches = main_repo.branches().unwrap();
        assert!(
            main_branches.contains(&branch_name.to_string()),
            "Branch should exist after fetch"
        );

        // Verify git-town parent was set correctly
        let config_output = main_repo
            .run_git_command(&["config", &format!("git-town-branch.{}.parent", branch_name)])
            .unwrap();
        assert_eq!(
            config_output.trim(),
            "feature-branch",
            "Git-town parent should be set to feature-branch"
        );
    }

    #[tokio::test]
    async fn test_copy_repo_with_source_commit() {
        let ctx = AppContext::builder().build();

        // Create a repository with multiple commits
        let test_repo = TestGitRepository::new().unwrap();
        let first_commit = test_repo.init_with_commit().unwrap();

        // Add more commits
        test_repo
            .create_file("feature1.rs", "fn feature1() {}")
            .unwrap();
        test_repo.stage_all().unwrap();
        test_repo.commit("Add feature1").unwrap();

        test_repo
            .create_file("feature2.rs", "fn feature2() {}")
            .unwrap();
        test_repo.stage_all().unwrap();
        let _latest_commit = test_repo.commit("Add feature2").unwrap();

        let manager = RepoManager::new(&ctx);

        // Copy repo from the first commit
        let task_id = "efgh5678";
        let branch_name = "tsk/test/copy-repo-test/efgh5678";
        let result = manager
            .copy_repo(task_id, test_repo.path(), Some(&first_commit), branch_name)
            .await;

        assert!(result.is_ok());
        let copied_path = result.unwrap().repo_path;

        assert!(copied_path.exists());

        // Verify the working directory contains all current files (preserving working directory state)
        // even though the branch was created from the first commit
        assert!(
            copied_path.join("feature1.rs").exists(),
            "Working directory files should be preserved"
        );
        assert!(
            copied_path.join("feature2.rs").exists(),
            "Working directory files should be preserved"
        );
        assert!(
            copied_path.join("README.md").exists(),
            "Original files should be preserved"
        );

        // Verify the branch was created from the first commit by checking git history
        let copied_repo = TestGitRepository::new().unwrap();
        let _ = std::fs::remove_dir_all(copied_repo.path());
        std::fs::rename(&copied_path, copied_repo.path()).unwrap();

        // The HEAD commit should be the first_commit (since we created branch from it)
        let head_commit = copied_repo.get_head_commit().unwrap();
        assert_eq!(
            head_commit, first_commit,
            "Branch should be created from the specified commit"
        );
    }

    #[tokio::test]
    async fn test_copy_repo_without_source_commit() {
        let ctx = AppContext::builder().build();

        // Create a repository with commits
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        // Add more files
        test_repo
            .create_file("feature.rs", "fn feature() {}")
            .unwrap();
        test_repo.stage_all().unwrap();
        test_repo.commit("Add feature").unwrap();

        let manager = RepoManager::new(&ctx);

        // Copy repo without specifying source commit (should use HEAD)
        let task_id = "ijkl9012";
        let branch_name = "tsk/test/copy-repo-head/ijkl9012";
        let result = manager
            .copy_repo(task_id, test_repo.path(), None, branch_name)
            .await;

        assert!(result.is_ok());
        let copied_path = result.unwrap().repo_path;

        assert!(copied_path.exists());

        // Verify the copied repo has all files from HEAD
        assert!(copied_path.join("README.md").exists());
        assert!(copied_path.join("feature.rs").exists());
    }

    #[tokio::test]
    async fn test_copy_repo_separates_tracked_and_untracked_files() {
        use crate::test_utils::create_files_with_gitignore;

        let ctx = AppContext::builder().build();

        // Create a repository with mixed file types
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init().unwrap();
        create_files_with_gitignore(&test_repo).unwrap();

        let manager = RepoManager::new(&ctx);

        // Copy the repository
        let task_id = "mnop3456";
        let branch_name = "tsk/test/tracked-untracked/mnop3456";
        let result = manager
            .copy_repo(task_id, test_repo.path(), None, branch_name)
            .await;

        assert!(result.is_ok());
        let copied_path = result.unwrap().repo_path;

        // Verify tracked files are copied
        assert!(
            copied_path.join("src/main.rs").exists(),
            "Tracked files should be copied"
        );
        assert!(
            copied_path.join("Cargo.toml").exists(),
            "Tracked files should be copied"
        );
        assert!(
            copied_path.join(".gitignore").exists(),
            "Gitignore should be copied"
        );

        // Verify untracked files are copied
        assert!(
            copied_path.join("src/lib.rs").exists(),
            "Untracked files should be copied"
        );
        assert!(
            copied_path.join("README.md").exists(),
            "Untracked files should be copied"
        );

        // Verify ignored files are NOT copied
        assert!(
            !copied_path.join("debug.log").exists(),
            "Ignored files should not be copied"
        );
        assert!(
            !copied_path.join(".DS_Store").exists(),
            "Ignored files should not be copied"
        );
        assert!(
            !copied_path.join("target").exists(),
            "Ignored directories should not be copied"
        );
        assert!(
            !copied_path.join("tmp").exists(),
            "Ignored directories should not be copied"
        );

        // Verify .tsk directory IS copied even if it would normally be ignored
        assert!(
            copied_path.join(".tsk/config.json").exists(),
            ".tsk directory should always be copied"
        );
        assert!(
            copied_path.join(".tsk/tsk.toml").exists(),
            ".tsk directory should always be copied"
        );
    }

    #[tokio::test]
    async fn test_copy_repo_with_symlinks() {
        let ctx = AppContext::builder().build();

        // Create a repository with symlinks
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init().unwrap();

        // Create some regular files and directories
        test_repo
            .create_file("README.md", "# Test Repository\n")
            .unwrap();
        test_repo
            .create_file("src/main.rs", "fn main() {}")
            .unwrap();
        std::fs::create_dir_all(test_repo.path().join("docs")).unwrap();
        test_repo
            .create_file("docs/guide.md", "# User Guide\n")
            .unwrap();

        // Create symlinks
        #[cfg(unix)]
        {
            use std::os::unix::fs as unix_fs;

            // Create a symlink to a file
            unix_fs::symlink("README.md", test_repo.path().join("README_LINK.md")).unwrap();

            // Create a symlink to a directory
            unix_fs::symlink("docs", test_repo.path().join("documentation")).unwrap();

            // Create an absolute symlink to a file within the repo
            let readme_abs = test_repo.path().join("README.md");
            unix_fs::symlink(&readme_abs, test_repo.path().join("README_ABS.md")).unwrap();

            // Create a nested symlink (symlink inside a directory)
            unix_fs::symlink("../README.md", test_repo.path().join("docs/README_LINK.md")).unwrap();
        }

        #[cfg(windows)]
        {
            // On Windows, create file and directory symlinks
            std::fs::symlink_file("README.md", test_repo.path().join("README_LINK.md")).unwrap();

            std::fs::symlink_dir("docs", test_repo.path().join("documentation")).unwrap();
        }

        // Stage and commit all files including symlinks
        test_repo.stage_all().unwrap();
        test_repo.commit("Initial commit with symlinks").unwrap();

        // Also add an untracked symlink
        #[cfg(unix)]
        {
            use std::os::unix::fs as unix_fs;
            unix_fs::symlink("src/main.rs", test_repo.path().join("main_link.rs")).unwrap();
        }

        #[cfg(windows)]
        {
            std::fs::symlink_file("src/main.rs", test_repo.path().join("main_link.rs")).unwrap();
        }

        let manager = RepoManager::new(&ctx);

        // Copy the repository
        let task_id = "symlink123";
        let branch_name = "tsk/test/symlinks/symlink123";
        let result = manager
            .copy_repo(task_id, test_repo.path(), None, branch_name)
            .await;

        assert!(
            result.is_ok(),
            "Failed to copy repo with symlinks: {:?}",
            result
        );
        let copied_path = result.unwrap().repo_path;

        // Verify regular files were copied
        assert!(copied_path.join("README.md").exists());
        assert!(copied_path.join("src/main.rs").exists());
        assert!(copied_path.join("docs/guide.md").exists());

        // Verify symlinks were preserved as symlinks
        #[cfg(unix)]
        {
            use std::fs;

            // Check tracked symlinks
            let readme_link_meta =
                fs::symlink_metadata(copied_path.join("README_LINK.md")).unwrap();
            assert!(
                readme_link_meta.is_symlink(),
                "README_LINK.md should be a symlink"
            );

            let docs_link_meta = fs::symlink_metadata(copied_path.join("documentation")).unwrap();
            assert!(
                docs_link_meta.is_symlink(),
                "documentation should be a symlink"
            );

            let readme_abs_meta = fs::symlink_metadata(copied_path.join("README_ABS.md")).unwrap();
            assert!(
                readme_abs_meta.is_symlink(),
                "README_ABS.md should be a symlink"
            );

            let nested_link_meta =
                fs::symlink_metadata(copied_path.join("docs/README_LINK.md")).unwrap();
            assert!(
                nested_link_meta.is_symlink(),
                "docs/README_LINK.md should be a symlink"
            );

            // Check untracked symlink
            let untracked_link_meta =
                fs::symlink_metadata(copied_path.join("main_link.rs")).unwrap();
            assert!(
                untracked_link_meta.is_symlink(),
                "main_link.rs should be a symlink"
            );

            // Verify symlink targets are correct
            let readme_target = fs::read_link(copied_path.join("README_LINK.md")).unwrap();
            assert_eq!(readme_target.to_string_lossy(), "README.md");

            let docs_target = fs::read_link(copied_path.join("documentation")).unwrap();
            assert_eq!(docs_target.to_string_lossy(), "docs");

            let nested_target = fs::read_link(copied_path.join("docs/README_LINK.md")).unwrap();
            assert_eq!(nested_target.to_string_lossy(), "../README.md");
        }

        #[cfg(windows)]
        {
            use std::fs;

            // Check symlinks on Windows
            let readme_link_meta =
                fs::symlink_metadata(copied_path.join("README_LINK.md")).unwrap();
            assert!(
                readme_link_meta.is_symlink(),
                "README_LINK.md should be a symlink"
            );

            let docs_link_meta = fs::symlink_metadata(copied_path.join("documentation")).unwrap();
            assert!(
                docs_link_meta.is_symlink(),
                "documentation should be a symlink"
            );

            let untracked_link_meta =
                fs::symlink_metadata(copied_path.join("main_link.rs")).unwrap();
            assert!(
                untracked_link_meta.is_symlink(),
                "main_link.rs should be a symlink"
            );
        }
    }

    #[tokio::test]
    async fn test_copy_repo_includes_tsk_directory() {
        let ctx = AppContext::builder().build();

        // Create a repository with .tsk directory
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        // Create .tsk directory with various files
        test_repo
            .create_file(".tsk/config.json", r#"{"agent": "claude"}"#)
            .unwrap();
        test_repo
            .create_file(".tsk/templates/feat.md", "# Feature Template\n{{PROMPT}}")
            .unwrap();
        test_repo
            .create_file(".tsk/tsk.toml", "stack = \"rust\"\n")
            .unwrap();

        // Add .tsk to .gitignore to test it's still copied
        test_repo.create_file(".gitignore", ".tsk/\n").unwrap();
        test_repo.stage_all().unwrap();
        test_repo
            .commit("Add .tsk directory and gitignore")
            .unwrap();

        let manager = RepoManager::new(&ctx);

        // Copy the repository
        let task_id = "qrst7890";
        let branch_name = "tsk/test/tsk-directory/qrst7890";
        let result = manager
            .copy_repo(task_id, test_repo.path(), None, branch_name)
            .await;

        assert!(result.is_ok());
        let copied_path = result.unwrap().repo_path;

        // Verify .tsk directory and all its contents are copied
        assert!(
            copied_path.join(".tsk").exists(),
            ".tsk directory should be copied"
        );
        assert!(
            copied_path.join(".tsk/config.json").exists(),
            ".tsk/config.json should be copied"
        );
        assert!(
            copied_path.join(".tsk/templates/feat.md").exists(),
            ".tsk/templates should be copied"
        );
        assert!(
            copied_path.join(".tsk/tsk.toml").exists(),
            ".tsk/tsk.toml should be copied"
        );
    }

    #[tokio::test]
    async fn test_copy_repo_includes_unstaged_changes() {
        let ctx = AppContext::builder().build();

        // Create a repository with committed files
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();

        // Create and commit a file with initial content
        test_repo
            .create_file("tracked.txt", "initial content")
            .unwrap();
        test_repo.stage_all().unwrap();
        test_repo.commit("Add tracked file").unwrap();

        // Modify the tracked file (unstaged change)
        test_repo
            .create_file("tracked.txt", "modified content - unstaged")
            .unwrap();

        // Create another file and stage it (staged change)
        test_repo
            .create_file("staged.txt", "staged content")
            .unwrap();
        test_repo.run_git_command(&["add", "staged.txt"]).unwrap();

        // Create an untracked file
        test_repo
            .create_file("untracked.txt", "untracked content")
            .unwrap();

        let manager = RepoManager::new(&ctx);

        // Copy the repository
        let task_id = "unstaged123";
        let branch_name = "tsk/test/unstaged-changes/unstaged123";
        let result = manager
            .copy_repo(task_id, test_repo.path(), None, branch_name)
            .await;

        assert!(result.is_ok());
        let copied_path = result.unwrap().repo_path;

        // Verify the unstaged changes are included (working directory version)
        let tracked_content = std::fs::read_to_string(copied_path.join("tracked.txt")).unwrap();
        assert_eq!(
            tracked_content, "modified content - unstaged",
            "Unstaged changes should be copied (working directory version)"
        );

        // Verify staged file is copied
        let staged_content = std::fs::read_to_string(copied_path.join("staged.txt")).unwrap();
        assert_eq!(
            staged_content, "staged content",
            "Staged files should be copied"
        );

        // Verify untracked file is copied
        let untracked_content = std::fs::read_to_string(copied_path.join("untracked.txt")).unwrap();
        assert_eq!(
            untracked_content, "untracked content",
            "Untracked files should be copied"
        );
    }

    #[tokio::test]
    async fn test_fetch_changes_with_branch_from_specific_commit_no_new_changes() {
        // Test the exact scenario that causes the "object is not a committish" error:
        // 1. Create repo with commits
        // 2. Copy repo and create branch from specific commit
        // 3. Make no changes
        // 4. Try to fetch - should handle gracefully with validation
        let ctx = AppContext::builder().build();

        // Create main repository with multiple commits
        let main_repo = TestGitRepository::new().unwrap();
        let first_commit = main_repo.init_with_main_branch().unwrap();

        // Create second commit
        main_repo.create_file("feature.txt", "new feature").unwrap();
        main_repo.stage_all().unwrap();
        main_repo.commit("Add feature").unwrap();

        let manager = RepoManager::new(&ctx);

        // Copy repo from the first commit (simulates task creation from specific commit)
        let task_id = "commit123";
        let branch_name = "tsk/test/from-commit-no-changes/commit123";
        let result = manager
            .copy_repo(task_id, main_repo.path(), Some(&first_commit), branch_name)
            .await;

        assert!(result.is_ok(), "Failed to copy repo: {:?}", result);
        let copied_repo_path = result.unwrap().repo_path;

        // Don't make any commits in the copied repo - just like the error scenario
        // The branch exists and points to first_commit, but we haven't added any new work

        // Try to fetch changes - should handle gracefully (no new commits)
        // The source_commit is the first_commit (the commit from which the branch was created)
        let fetch_result = manager
            .fetch_changes(
                &copied_repo_path,
                branch_name,
                main_repo.path(),
                &first_commit,
                Some("main"),
                false,
            )
            .await;

        // The fetch should succeed (validation passes) but return false (no new commits)
        assert!(
            fetch_result.is_ok(),
            "Fetch should succeed with validation: {:?}",
            fetch_result
        );
        assert!(
            !fetch_result.unwrap().has_changes,
            "Should return false when no new commits"
        );

        // Verify the branch was cleaned up in main repo
        let main_branches = main_repo.branches().unwrap();
        assert!(
            !main_branches.contains(&branch_name.to_string()),
            "Branch should be cleaned up when no new commits"
        );
    }

    /// Integration test to understand current behavior with git submodules.
    /// This test documents what happens when copying a repo that contains submodules.
    #[tokio::test]
    async fn test_copy_repo_with_submodules_current_behavior() {
        let ctx = AppContext::builder().build();

        // Create a "submodule" repository first (this will be added as a submodule)
        let submodule_repo = TestGitRepository::new().unwrap();
        submodule_repo.init().unwrap();
        submodule_repo
            .create_file(
                "lib.rs",
                "pub fn hello() -> &'static str { \"hello from submodule\" }",
            )
            .unwrap();
        submodule_repo.stage_all().unwrap();
        submodule_repo.commit("Initial submodule commit").unwrap();

        // Create the main "workspace" repository
        let main_repo = TestGitRepository::new().unwrap();
        main_repo.init_with_commit().unwrap();

        // Add the submodule to the main repo
        main_repo
            .add_submodule(&submodule_repo, "libs/mylib")
            .unwrap();
        main_repo.stage_all().unwrap();
        main_repo.commit("Add submodule").unwrap();

        // Verify submodule was added correctly
        assert!(
            main_repo.path().join("libs/mylib/lib.rs").exists(),
            "Submodule files should exist in main repo"
        );
        assert!(
            main_repo.path().join(".gitmodules").exists(),
            ".gitmodules should exist"
        );

        // Verify .git file exists in submodule (points to main repo's .git/modules)
        let submodule_git_path = main_repo.path().join("libs/mylib/.git");
        assert!(
            submodule_git_path.exists(),
            "Submodule .git should exist (as file pointing to main .git/modules)"
        );
        let submodule_git_content = std::fs::read_to_string(&submodule_git_path).unwrap();
        assert!(
            submodule_git_content.starts_with("gitdir:"),
            "Submodule .git should be a gitdir reference"
        );

        // Now copy the repo using TSK's current mechanism
        let manager = RepoManager::new(&ctx);
        let task_id = "submod123";
        let branch_name = "tsk/test/submodules/submod123";
        let result = manager
            .copy_repo(task_id, main_repo.path(), None, branch_name)
            .await;

        assert!(result.is_ok(), "Copy should succeed: {:?}", result);
        let copied_path = result.unwrap().repo_path;

        // Document what happened with the submodule in the copy:

        // Check if .gitmodules was copied
        let gitmodules_copied = copied_path.join(".gitmodules").exists();

        // Check if submodule directory exists
        let submodule_dir_copied = copied_path.join("libs/mylib").exists();

        // Check if submodule files were copied
        let submodule_files_copied = copied_path.join("libs/mylib/lib.rs").exists();

        // Check if the submodule is recognized as a valid git repo
        let submodule_is_git_repo =
            git_operations::is_git_repository(&copied_path.join("libs/mylib"))
                .await
                .unwrap_or(false);

        // Check .git/modules in copied repo (where git stores actual submodule data)
        let modules_dir = copied_path.join(".git/modules");
        let modules_exist = modules_dir.exists();

        // .gitmodules should be copied (it's a regular tracked file)
        assert!(
            gitmodules_copied,
            "CURRENT BEHAVIOR: .gitmodules should be copied as a regular file"
        );

        // Submodule directory and files should exist (copied as regular files)
        assert!(
            submodule_dir_copied && submodule_files_copied,
            "CURRENT BEHAVIOR: Submodule files are copied as regular files"
        );

        // Submodule support is now implemented - submodule should be a valid git repo
        assert!(
            submodule_is_git_repo,
            "Submodule should be recognized as valid git repo with submodule support"
        );

        // The .git/modules directory should exist (copied from source)
        assert!(
            modules_exist,
            ".git/modules should exist after copy with submodule support"
        );
    }

    /// Test with nested submodules (sub-submodule) to understand behavior with problematic nested structures.
    #[tokio::test]
    async fn test_copy_repo_with_nested_submodules_current_behavior() {
        let ctx = AppContext::builder().build();

        // Create the innermost "sub-submodule" repository
        let inner_repo = TestGitRepository::new().unwrap();
        inner_repo.init().unwrap();
        inner_repo
            .create_file("inner.txt", "inner content")
            .unwrap();
        inner_repo.stage_all().unwrap();
        inner_repo.commit("Inner commit").unwrap();

        // Create the middle "submodule" repository with the inner as a submodule
        let middle_repo = TestGitRepository::new().unwrap();
        middle_repo.init().unwrap();
        middle_repo
            .create_file("middle.txt", "middle content")
            .unwrap();
        middle_repo.stage_all().unwrap();
        middle_repo.commit("Middle commit").unwrap();

        // Add inner as submodule of middle
        middle_repo.add_submodule(&inner_repo, "inner").unwrap();
        middle_repo.stage_all().unwrap();
        middle_repo.commit("Add inner submodule").unwrap();

        // Create the main "workspace" repository with middle as a submodule
        let main_repo = TestGitRepository::new().unwrap();
        main_repo.init_with_commit().unwrap();

        // Add middle as submodule of main
        main_repo.add_submodule(&middle_repo, "middle").unwrap();

        // Initialize nested submodules recursively
        main_repo
            .run_git_command(&[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "update",
                "--init",
                "--recursive",
            ])
            .unwrap();

        main_repo.stage_all().unwrap();
        main_repo.commit("Add middle submodule").unwrap();

        // Verify the nested structure was set up correctly
        assert!(
            main_repo.path().join("middle/middle.txt").exists(),
            "Middle submodule files should exist"
        );
        assert!(
            main_repo.path().join("middle/inner/inner.txt").exists(),
            "Inner (nested) submodule files should exist"
        );

        // Copy the repo
        let manager = RepoManager::new(&ctx);
        let task_id = "nested123";
        let branch_name = "tsk/test/nested-submodules/nested123";
        let result = manager
            .copy_repo(task_id, main_repo.path(), None, branch_name)
            .await;

        assert!(result.is_ok(), "Copy should succeed: {:?}", result);
        let copied_path = result.unwrap().repo_path;

        // Check each level
        let middle_is_repo = git_operations::is_git_repository(&copied_path.join("middle"))
            .await
            .unwrap_or(false);
        let inner_is_repo = git_operations::is_git_repository(&copied_path.join("middle/inner"))
            .await
            .unwrap_or(false);

        // Verify files are copied
        assert!(
            copied_path.join("middle/middle.txt").exists(),
            "Middle submodule files should be copied"
        );
        assert!(
            copied_path.join("middle/inner/inner.txt").exists(),
            "Inner submodule files should be copied"
        );

        // Submodule support is now implemented - nested submodules should be valid git repos
        assert!(
            middle_is_repo,
            "Middle submodule should be valid git repo with submodule support"
        );
        assert!(
            inner_is_repo,
            "Inner (nested) submodule should be valid git repo with submodule support"
        );
    }

    /// Test the full round-trip: copy repo with submodule, modify submodule, commit, and fetch back.
    /// This verifies that Phase 1 (copy) and Phase 2 (fetch) work correctly together.
    #[tokio::test]
    async fn test_copy_repo_modify_submodule_and_commit() {
        let ctx = AppContext::builder().build();

        // Create a submodule repository
        let submodule_repo = TestGitRepository::new().unwrap();
        submodule_repo.init_with_main_branch().unwrap();
        submodule_repo
            .create_file("lib.rs", "// original content")
            .unwrap();
        submodule_repo.stage_all().unwrap();
        submodule_repo.commit("Add lib.rs").unwrap();

        // Create main repository with submodule
        let main_repo = TestGitRepository::new().unwrap();
        main_repo.init_with_main_branch().unwrap();

        main_repo.add_submodule(&submodule_repo, "lib").unwrap();
        main_repo.stage_all().unwrap();
        main_repo.commit("Add submodule").unwrap();

        // Capture source_commit before copying
        let source_commit = main_repo.get_current_commit().unwrap();

        // Copy the repo
        let manager = RepoManager::new(&ctx);
        let task_id = "modsubmod";
        let branch_name = "tsk/test/modify-submodule/modsubmod";
        let result = manager
            .copy_repo(task_id, main_repo.path(), None, branch_name)
            .await;

        assert!(result.is_ok(), "Failed to copy repo: {:?}", result);
        let copied_path = result.unwrap().repo_path;

        // Verify the submodule is a valid git repository in the copied repo
        let copied_submodule_is_repo = git_operations::is_git_repository(&copied_path.join("lib"))
            .await
            .unwrap_or(false);
        assert!(
            copied_submodule_is_repo,
            "Copied submodule should be a valid git repository"
        );

        // Modify a file in the submodule directory
        let submodule_file = copied_path.join("lib/lib.rs");
        std::fs::write(&submodule_file, "// modified by TSK agent").unwrap();

        // Commit changes in the submodule first
        let copied_submodule = ExistingGitRepository::new(&copied_path.join("lib")).unwrap();
        copied_submodule.configure_test_user().unwrap();
        copied_submodule.stage_all().unwrap();
        copied_submodule.commit("Modify lib.rs").unwrap();

        // Configure git user for the superproject (needed for commit_changes)
        let copied_superproject = ExistingGitRepository::new(&copied_path).unwrap();
        copied_superproject.configure_test_user().unwrap();

        // Now commit in the superproject (updates submodule pointer)
        let commit_result = manager
            .commit_changes(&copied_path, "Update submodule with changes")
            .await;

        assert!(
            commit_result.is_ok(),
            "Commit should succeed with submodule support: {:?}",
            commit_result
        );

        // Fetch changes back to main repo
        let fetch_result = manager
            .fetch_changes(
                &copied_path,
                branch_name,
                main_repo.path(),
                &source_commit,
                Some("main"),
                false,
            )
            .await;

        assert!(
            fetch_result.is_ok(),
            "Fetch should succeed: {:?}",
            fetch_result
        );
        assert!(
            fetch_result.unwrap().has_changes,
            "Should return true indicating new commits were fetched"
        );

        // Verify the branch exists in main repo
        let main_branches = main_repo.branches().unwrap();
        assert!(
            main_branches.contains(&branch_name.to_string()),
            "Branch should exist in main repo after fetch"
        );
    }

    /// Integration test reproducing the user-reported issue:
    /// After modifying files in both the superproject and submodule,
    /// the submodule's branch should be visible via `git branch` in the original submodule.
    #[tokio::test]
    async fn test_submodule_commits_visible_in_original_repo() {
        let ctx = AppContext::builder().build();

        // Create RepoB (will become a submodule)
        let repo_b = TestGitRepository::new().unwrap();
        repo_b.init_with_main_branch().unwrap();

        // Create RepoA (superproject) with RepoB as submodule
        let repo_a = TestGitRepository::new().unwrap();
        repo_a.init_with_main_branch().unwrap();

        // Add RepoB as submodule
        repo_a.add_submodule(&repo_b, "RepoB").unwrap();
        repo_a.stage_all().unwrap();
        repo_a.commit("Add RepoB submodule").unwrap();

        // Capture source_commit before copying
        let source_commit = repo_a.get_current_commit().unwrap();

        // Step 1: Copy the repo (simulates `tsk shell` starting)
        let manager = RepoManager::new(&ctx);
        let task_id = "submodvisible";
        let branch_name = "tsk/test/submod-visible/submodvisible";
        let copied_path = manager
            .copy_repo(task_id, repo_a.path(), None, branch_name)
            .await
            .expect("Copy should succeed")
            .repo_path;

        // Step 2: Modify both READMEs (simulates user editing files)
        std::fs::write(copied_path.join("README.md"), "# RepoA\nhi").unwrap();
        std::fs::write(copied_path.join("RepoB/README.md"), "# RepoB\nhi").unwrap();

        // Step 3: Commit changes in submodule first
        let copied_submodule = ExistingGitRepository::new(&copied_path.join("RepoB")).unwrap();
        copied_submodule.configure_test_user().unwrap();
        copied_submodule.stage_all().unwrap();
        let submod_commit = copied_submodule.commit("Add hi to RepoB README").unwrap();

        // Configure git user for the superproject (needed for commit_changes)
        let copied_superproject = ExistingGitRepository::new(&copied_path).unwrap();
        copied_superproject.configure_test_user().unwrap();

        // Step 4: Commit changes in superproject
        manager
            .commit_changes(&copied_path, "Add hi to both READMEs")
            .await
            .expect("Commit should succeed");

        // VERIFY: tsk-submodule branch does NOT exist before fetch
        let submodule_in_original = repo_a.path().join("RepoB");
        let branches_before = std::process::Command::new("git")
            .current_dir(&submodule_in_original)
            .args(["branch", "-a"])
            .output()
            .expect("git branch should work");
        let branches_before_str = String::from_utf8_lossy(&branches_before.stdout);
        assert!(
            !branches_before_str.contains("tsk-submodule/"),
            "tsk-submodule branches should NOT exist before fetch"
        );

        // Step 5: Fetch changes back (simulates exiting `tsk shell`)
        manager
            .fetch_changes(
                &copied_path,
                branch_name,
                repo_a.path(),
                &source_commit,
                Some("main"),
                false,
            )
            .await
            .expect("Fetch should succeed");

        // VERIFY: Branch exists in RepoA (superproject)
        let repo_a_branches = repo_a.branches().unwrap();
        assert!(
            repo_a_branches.contains(&branch_name.to_string()),
            "Branch '{}' should exist in RepoA. Available branches: {:?}",
            branch_name,
            repo_a_branches
        );

        // VERIFY: The submodule commit is accessible in the original RepoB
        // The commit should be reachable (objects fetched)
        let commit_exists = std::process::Command::new("git")
            .current_dir(&submodule_in_original)
            .args(["cat-file", "-t", &submod_commit])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        assert!(
            commit_exists,
            "Submodule commit {} should be accessible in original RepoB",
            submod_commit
        );

        // VERIFY: The task branch exists in the submodule (same name as superproject branch)
        // This is the key fix - fetched branches should be visible via `git branch`
        let submodule_branches_output = std::process::Command::new("git")
            .current_dir(&submodule_in_original)
            .args(["branch", "-a"])
            .output()
            .expect("git branch should work");
        let submodule_branches = String::from_utf8_lossy(&submodule_branches_output.stdout);

        println!("Submodule branches after fetch:\n{}", submodule_branches);

        // The branch with the task name should exist in the submodule (matching superproject)
        assert!(
            submodule_branches.contains(branch_name),
            "Branch '{}' should exist in submodule after fetch.\nAvailable branches:\n{}",
            branch_name,
            submodule_branches
        );

        // VERIFY: The commit is accessible via the task branch
        let log_output = std::process::Command::new("git")
            .current_dir(&submodule_in_original)
            .args(["log", "--oneline", "-1", branch_name])
            .output()
            .expect("git log should work");
        let log_message = String::from_utf8_lossy(&log_output.stdout);
        println!("Submodule {} log: {}", branch_name, log_message);

        assert!(
            log_message.contains("Add hi to RepoB"),
            "Submodule branch should point to commit with 'Add hi to RepoB', got: {}",
            log_message
        );
    }

    /// Integration test for the submodule commit bug fix.
    /// This test verifies that uncommitted changes in submodules are automatically
    /// committed by `commit_changes()` before committing the superproject.
    ///
    /// The key difference from `test_submodule_commits_visible_in_original_repo` is that
    /// this test does NOT manually commit in the submodule - it relies on `commit_changes()`
    /// to do it automatically.
    #[tokio::test]
    async fn test_uncommitted_submodule_changes_are_committed() {
        let ctx = AppContext::builder().build();

        // Create RepoB (will become a submodule)
        let repo_b = TestGitRepository::new().unwrap();
        repo_b.init_with_main_branch().unwrap();

        // Create RepoA (superproject) with RepoB as submodule
        let repo_a = TestGitRepository::new().unwrap();
        repo_a.init_with_main_branch().unwrap();

        // Add RepoB as submodule
        repo_a.add_submodule(&repo_b, "RepoB").unwrap();
        repo_a.stage_all().unwrap();
        repo_a.commit("Add RepoB submodule").unwrap();

        // Capture source_commit before copying
        let source_commit = repo_a.get_current_commit().unwrap();

        // Step 1: Copy the repo (simulates `tsk shell` starting)
        let manager = RepoManager::new(&ctx);
        let task_id = "uncommittedsub";
        let branch_name = "tsk/test/uncommitted-submodule/uncommittedsub";
        let copied_path = manager
            .copy_repo(task_id, repo_a.path(), None, branch_name)
            .await
            .expect("Copy should succeed")
            .repo_path;

        // Step 2: Modify both READMEs WITHOUT committing (simulates user editing files)
        // This is the key difference - no manual commits in either repo
        std::fs::write(copied_path.join("README.md"), "# RepoA\nmodified").unwrap();
        std::fs::write(
            copied_path.join("RepoB/README.md"),
            "# RepoB\nmodified by agent",
        )
        .unwrap();

        // Verify submodule has uncommitted changes before commit_changes()
        let copied_submodule_path = copied_path.join("RepoB");
        let status_before = std::process::Command::new("git")
            .current_dir(&copied_submodule_path)
            .args(["status", "--porcelain"])
            .output()
            .expect("git status should work");
        let status_before_str = String::from_utf8_lossy(&status_before.stdout);
        assert!(
            !status_before_str.is_empty(),
            "Submodule should have uncommitted changes before commit_changes()"
        );

        // Configure git user for both superproject and submodule (needed for auto-commit)
        let copied_superproject = ExistingGitRepository::new(&copied_path).unwrap();
        copied_superproject.configure_test_user().unwrap();
        let copied_submodule = ExistingGitRepository::new(&copied_submodule_path).unwrap();
        copied_submodule.configure_test_user().unwrap();

        // Step 3: Call commit_changes() - this should automatically commit submodule changes
        manager
            .commit_changes(&copied_path, "Automatic commit of changes")
            .await
            .expect("Commit should succeed");

        // Verify submodule no longer has uncommitted changes
        let status_after = std::process::Command::new("git")
            .current_dir(&copied_submodule_path)
            .args(["status", "--porcelain"])
            .output()
            .expect("git status should work");
        let status_after_str = String::from_utf8_lossy(&status_after.stdout);
        assert!(
            status_after_str.is_empty(),
            "Submodule should have no uncommitted changes after commit_changes(), got: {}",
            status_after_str
        );

        // Step 4: Fetch changes back (simulates exiting `tsk shell`)
        let fetch_result = manager
            .fetch_changes(
                &copied_path,
                branch_name,
                repo_a.path(),
                &source_commit,
                Some("main"),
                false,
            )
            .await;
        assert!(
            fetch_result.is_ok(),
            "Fetch should succeed: {:?}",
            fetch_result
        );
        assert!(
            fetch_result.unwrap().has_changes,
            "Fetch should return true indicating new commits"
        );

        // VERIFY: Branch exists in RepoA (superproject)
        let repo_a_branches = repo_a.branches().unwrap();
        assert!(
            repo_a_branches.contains(&branch_name.to_string()),
            "Branch '{}' should exist in RepoA. Available branches: {:?}",
            branch_name,
            repo_a_branches
        );

        // VERIFY: The task branch exists in the original RepoB submodule
        let submodule_in_original = repo_a.path().join("RepoB");
        let submodule_branches_output = std::process::Command::new("git")
            .current_dir(&submodule_in_original)
            .args(["branch", "-a"])
            .output()
            .expect("git branch should work");
        let submodule_branches = String::from_utf8_lossy(&submodule_branches_output.stdout);

        println!("Submodule branches after fetch:\n{}", submodule_branches);

        // The branch with the task name should exist in the submodule
        assert!(
            submodule_branches.contains(branch_name),
            "Branch '{}' should exist in submodule after fetch.\nAvailable branches:\n{}",
            branch_name,
            submodule_branches
        );

        // VERIFY: The submodule branch contains the file changes
        let file_content = std::process::Command::new("git")
            .current_dir(&submodule_in_original)
            .args(["show", &format!("{}:README.md", branch_name)])
            .output()
            .expect("git show should work");
        let content = String::from_utf8_lossy(&file_content.stdout);

        assert!(
            content.contains("modified by agent"),
            "Submodule branch should contain 'modified by agent', got: {}",
            content
        );

        // VERIFY: The superproject branch points to the updated submodule commit
        let superproject_submod_ref = std::process::Command::new("git")
            .current_dir(repo_a.path())
            .args(["ls-tree", branch_name, "RepoB"])
            .output()
            .expect("git ls-tree should work");
        let ls_tree_output = String::from_utf8_lossy(&superproject_submod_ref.stdout);
        println!("Superproject submodule ref: {}", ls_tree_output);

        // The ls-tree output should show a commit reference (not a tree)
        // Format: "160000 commit <sha>	RepoB"
        assert!(
            ls_tree_output.contains("160000 commit"),
            "Superproject should reference submodule as a commit, got: {}",
            ls_tree_output
        );
    }

    /// Test that unchanged submodules do NOT get branches created during fetch.
    /// Reproduces the bug where all submodules get branches, even those without changes.
    #[tokio::test]
    async fn test_unchanged_submodule_no_branch_created() {
        let ctx = AppContext::builder().build();

        // Create RepoB (will become a submodule - this one WILL be modified)
        let repo_b = TestGitRepository::new().unwrap();
        repo_b.init_with_main_branch().unwrap();

        // Create RepoC (will become a submodule - this one will NOT be modified)
        let repo_c = TestGitRepository::new().unwrap();
        repo_c.init_with_main_branch().unwrap();

        // Create RepoA (superproject) with both RepoB and RepoC as submodules
        let repo_a = TestGitRepository::new().unwrap();
        repo_a.init_with_main_branch().unwrap();

        // Add RepoB as submodule
        repo_a.add_submodule(&repo_b, "RepoB").unwrap();

        // Add RepoC as submodule
        repo_a.add_submodule(&repo_c, "RepoC").unwrap();

        repo_a.stage_all().unwrap();
        repo_a.commit("Add RepoB and RepoC submodules").unwrap();

        // Capture source_commit before copying
        let source_commit = repo_a.get_current_commit().unwrap();

        // Verify initial structure
        assert!(
            repo_a.path().join("RepoB/README.md").exists(),
            "RepoB README should exist in submodule"
        );
        assert!(
            repo_a.path().join("RepoC/README.md").exists(),
            "RepoC README should exist in submodule"
        );

        // Step 1: Copy the repo
        let manager = RepoManager::new(&ctx);
        let task_id = "unchanged";
        let branch_name = "tsk/test/unchanged-submod/unchanged";
        let copied_path = manager
            .copy_repo(task_id, repo_a.path(), None, branch_name)
            .await
            .expect("Copy should succeed")
            .repo_path;

        // Step 2: Modify ONLY RepoB (not RepoC!)
        std::fs::write(copied_path.join("RepoB/README.md"), "# RepoB\nhi").unwrap();
        // RepoC is NOT modified

        // Step 3: Commit changes (only RepoB has changes)
        let copied_submodule_b = ExistingGitRepository::new(&copied_path.join("RepoB")).unwrap();
        copied_submodule_b.configure_test_user().unwrap();
        copied_submodule_b.stage_all().unwrap();
        copied_submodule_b.commit("Add hi to RepoB README").unwrap();

        // Configure git user for the superproject (needed for commit_changes)
        let copied_superproject = ExistingGitRepository::new(&copied_path).unwrap();
        copied_superproject.configure_test_user().unwrap();

        // Commit in superproject (records updated RepoB submodule pointer)
        manager
            .commit_changes(&copied_path, "Update RepoB")
            .await
            .expect("Commit should succeed");

        // Step 4: Fetch changes back
        manager
            .fetch_changes(
                &copied_path,
                branch_name,
                repo_a.path(),
                &source_commit,
                Some("main"),
                false,
            )
            .await
            .expect("Fetch should succeed");

        // VERIFY: Branch exists in RepoB (the one with changes)
        let submodule_b_in_original = repo_a.path().join("RepoB");
        let branches_b = std::process::Command::new("git")
            .current_dir(&submodule_b_in_original)
            .args(["branch", "-a"])
            .output()
            .expect("git branch should work");
        let branches_b_str = String::from_utf8_lossy(&branches_b.stdout);
        println!("RepoB branches after fetch:\n{}", branches_b_str);

        assert!(
            branches_b_str.contains(branch_name),
            "Branch '{}' SHOULD exist in RepoB (which has changes). Available branches:\n{}",
            branch_name,
            branches_b_str
        );

        // VERIFY: NO branch in RepoC (the one WITHOUT changes)
        let submodule_c_in_original = repo_a.path().join("RepoC");
        let branches_c = std::process::Command::new("git")
            .current_dir(&submodule_c_in_original)
            .args(["branch", "-a"])
            .output()
            .expect("git branch should work");
        let branches_c_str = String::from_utf8_lossy(&branches_c.stdout);
        println!("RepoC branches after fetch:\n{}", branches_c_str);

        assert!(
            !branches_c_str.contains(branch_name),
            "Branch '{}' should NOT exist in RepoC (which has NO changes). This is the bug! Available branches:\n{}",
            branch_name,
            branches_c_str
        );
    }

    #[tokio::test]
    async fn test_commit_changes_preserves_lfs_pointers() {
        // Skip if git-lfs is not available
        if std::process::Command::new("git")
            .args(["lfs", "version"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_or(true, |s| !s.success())
        {
            eprintln!("Skipping test: git-lfs not installed");
            return;
        }

        let ctx = AppContext::builder().build();
        let repo_manager = RepoManager::new(&ctx);

        // Create source repo with LFS tracking
        let source = TestGitRepository::new().unwrap();
        source.init_with_main_branch().unwrap();
        source
            .run_git_command(&["lfs", "install", "--local"])
            .unwrap();
        source
            .create_file(
                ".gitattributes",
                "*.bin filter=lfs diff=lfs merge=lfs -text\n",
            )
            .unwrap();
        source
            .create_file("data.bin", "original binary content\n")
            .unwrap();
        source.stage_all().unwrap();
        source.commit("Add LFS tracked file").unwrap();
        let source_commit = source.get_head_commit().unwrap();

        // Copy repo (this overlays working dir content over the clone)
        let branch_name = "tsk/test/lfs-preserve/abcd1234";
        let repo_path = repo_manager
            .copy_repo(
                "lfs-test-preserve",
                source.path(),
                Some(&source_commit),
                branch_name,
            )
            .await
            .unwrap()
            .repo_path;

        // Verify the working directory is clean after copy_repo
        // (LFS files should not appear as modified)
        let status = git_operations::get_status(&repo_path).await.unwrap();
        assert!(
            status.trim().is_empty(),
            "Working directory should be clean after copy_repo, but got: {status}"
        );

        // Configure git user in the task repo for commits
        let task_repo = ExistingGitRepository::new(&repo_path).unwrap();
        task_repo.configure_test_user().unwrap();

        // The working directory has raw file content (from overlay), but nothing was modified.
        // commit_changes should either produce no commit (since LFS re-cleans to identical pointers)
        // or produce a commit where the .bin file is still an LFS pointer
        let head_before = task_repo.get_current_commit().unwrap();
        repo_manager
            .commit_changes(&repo_path, "test commit")
            .await
            .unwrap();
        let head_after = task_repo.get_current_commit().unwrap();

        if head_before != head_after {
            // A commit was made - verify the .bin blob is an LFS pointer
            let blob_content = task_repo
                .run_git_command(&["cat-file", "-p", "HEAD:data.bin"])
                .unwrap();
            assert!(
                blob_content.starts_with("version https://git-lfs.github.com/spec/v1"),
                "LFS file should be stored as pointer, but got: {}",
                blob_content
            );
        }
        // If no commit was made, that's also correct - the clean filter re-created identical pointers
    }

    #[tokio::test]
    async fn test_commit_changes_with_modified_lfs_file() {
        // Skip if git-lfs is not available
        if std::process::Command::new("git")
            .args(["lfs", "version"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_or(true, |s| !s.success())
        {
            eprintln!("Skipping test: git-lfs not installed");
            return;
        }

        let ctx = AppContext::builder().build();
        let repo_manager = RepoManager::new(&ctx);

        // Create source repo with LFS tracking
        let source = TestGitRepository::new().unwrap();
        source.init_with_main_branch().unwrap();
        source
            .run_git_command(&["lfs", "install", "--local"])
            .unwrap();
        source
            .create_file(
                ".gitattributes",
                "*.bin filter=lfs diff=lfs merge=lfs -text\n",
            )
            .unwrap();
        source
            .create_file("data.bin", "original binary content\n")
            .unwrap();
        source.stage_all().unwrap();
        source.commit("Add LFS tracked file").unwrap();
        let source_commit = source.get_head_commit().unwrap();

        // Copy repo
        let branch_name = "tsk/test/lfs-modified/efgh5678";
        let repo_path = repo_manager
            .copy_repo(
                "lfs-test-modified",
                source.path(),
                Some(&source_commit),
                branch_name,
            )
            .await
            .unwrap()
            .repo_path;

        // Configure git user in the task repo
        let task_repo = ExistingGitRepository::new(&repo_path).unwrap();
        task_repo.configure_test_user().unwrap();

        // Modify the LFS-tracked file
        std::fs::write(repo_path.join("data.bin"), "modified binary content\n").unwrap();

        // Commit changes - should create a commit with a new LFS pointer
        repo_manager
            .commit_changes(&repo_path, "modify lfs file")
            .await
            .unwrap();

        // Verify the committed blob is an LFS pointer
        let blob_content = task_repo
            .run_git_command(&["cat-file", "-p", "HEAD:data.bin"])
            .unwrap();
        assert!(
            blob_content.starts_with("version https://git-lfs.github.com/spec/v1"),
            "Modified LFS file should be stored as pointer, but got: {}",
            blob_content
        );

        // Fetch changes back to the source repo
        let result = repo_manager
            .fetch_changes(
                &repo_path,
                branch_name,
                source.path(),
                &source_commit,
                Some("main"),
                false,
            )
            .await
            .unwrap();

        assert!(result.has_changes, "Branch should have changes");

        // Verify LFS objects were copied to the source repo
        let source_lfs_dir = source.path().join(".git/lfs/objects");
        assert!(
            source_lfs_dir.exists(),
            "LFS objects should have been copied to source repo"
        );
    }
}
