use git2::{Repository, RepositoryOpenFlags};
use std::path::{Path, PathBuf};

/// Check if the given path is within a git repository
pub async fn is_git_repository(repo_path: &Path) -> Result<bool, String> {
    match Repository::open_ext(repo_path, RepositoryOpenFlags::empty(), &[] as &[&Path]) {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

/// Create a branch from HEAD
///
/// # Errors
///
/// Returns an error if:
/// - The repository cannot be opened
/// - HEAD cannot be resolved (e.g., empty repository with no commits)
/// - The branch already exists
/// - The working directory cannot be updated
pub async fn create_branch(repo_path: &Path, branch_name: &str) -> Result<(), String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let branch_name = branch_name.to_owned();
        move || -> Result<(), String> {
            let repo = Repository::open(&repo_path)
                .map_err(|e| format!("Failed to open repository: {e}"))?;

            let head = repo
                .head()
                .map_err(|e| format!("Failed to get HEAD: {e}"))?;

            let commit = head
                .peel_to_commit()
                .map_err(|e| format!("Failed to get commit from HEAD: {e}"))?;

            repo.branch(&branch_name, &commit, false)
                .map_err(|e| format!("Failed to create branch: {e}"))?;

            repo.set_head(&format!("refs/heads/{branch_name}"))
                .map_err(|e| format!("Failed to checkout branch: {e}"))?;

            repo.checkout_head(None)
                .map_err(|e| format!("Failed to update working directory: {e}"))?;

            Ok(())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

pub async fn get_status(repo_path: &Path) -> Result<String, String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        move || -> Result<String, String> {
            let output = std::process::Command::new("git")
                .current_dir(&repo_path)
                .arg("status")
                .arg("--porcelain")
                .output()
                .map_err(|e| format!("Failed to execute git status: {e}"))?;

            if !output.status.success() {
                return Err(format!(
                    "Failed to get repository status: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }

            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

pub async fn add_all(repo_path: &Path) -> Result<(), String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        move || -> Result<(), String> {
            let output = std::process::Command::new("git")
                .current_dir(&repo_path)
                .arg("add")
                .arg("-A")
                .output()
                .map_err(|e| format!("Failed to execute git add: {e}"))?;

            if !output.status.success() {
                return Err(format!(
                    "Failed to add files to index: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }

            Ok(())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Re-normalizes the index so that clean/smudge filters (e.g. git-lfs) are
/// re-applied. This fixes stat-cache mismatches after overlaying working
/// directory files onto a clone.
pub async fn renormalize(repo_path: &Path) -> Result<(), String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        move || -> Result<(), String> {
            let output = std::process::Command::new("git")
                .current_dir(&repo_path)
                .args(["add", "--renormalize", "."])
                .output()
                .map_err(|e| format!("Failed to execute git add --renormalize: {e}"))?;

            if !output.status.success() {
                return Err(format!(
                    "Failed to renormalize files: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }

            Ok(())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

pub async fn commit(repo_path: &Path, message: &str) -> Result<(), String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let message = message.to_owned();
        move || -> Result<(), String> {
            let output = std::process::Command::new("git")
                .current_dir(&repo_path)
                .arg("commit")
                .arg("--no-verify")
                .arg("-m")
                .arg(&message)
                .output()
                .map_err(|e| format!("Failed to execute git commit: {e}"))?;

            if !output.status.success() {
                let combined = format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                if combined.contains("nothing to commit") {
                    return Ok(());
                }
                return Err(format!("Failed to create commit: {combined}"));
            }

            Ok(())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

pub async fn add_remote(repo_path: &Path, remote_name: &str, url: &str) -> Result<(), String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let remote_name = remote_name.to_owned();
        let url = url.to_owned();
        move || -> Result<(), String> {
            let repo = Repository::open(&repo_path)
                .map_err(|e| format!("Failed to open repository: {e}"))?;

            let result = repo.remote(&remote_name, &url);
            match result {
                Ok(_) => Ok(()),
                Err(e) => {
                    if e.code() == git2::ErrorCode::Exists {
                        Ok(())
                    } else {
                        Err(format!("Failed to add remote: {e}"))
                    }
                }
            }
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

pub async fn fetch_branch(
    repo_path: &Path,
    remote_name: &str,
    branch_name: &str,
) -> Result<(), String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let remote_name = remote_name.to_owned();
        let branch_name = branch_name.to_owned();
        move || -> Result<(), String> {
            let repo = Repository::open(&repo_path)
                .map_err(|e| format!("Failed to open repository: {e}"))?;

            let remote = repo
                .find_remote(&remote_name)
                .map_err(|e| format!("Failed to find remote: {e}"))?;

            let url = remote
                .url()
                .ok_or_else(|| "Remote has no URL".to_string())?;

            let output = std::process::Command::new("git")
                .current_dir(&repo_path)
                .arg("fetch")
                .arg("--no-recurse-submodules")
                .arg(url)
                .arg(format!("refs/heads/{branch_name}:refs/heads/{branch_name}"))
                .output()
                .map_err(|e| format!("Failed to execute git fetch: {e}"))?;

            if !output.status.success() {
                return Err(format!(
                    "Failed to fetch changes: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }

            Ok(())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

pub async fn remove_remote(repo_path: &Path, remote_name: &str) -> Result<(), String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let remote_name = remote_name.to_owned();
        move || -> Result<(), String> {
            let repo = Repository::open(&repo_path)
                .map_err(|e| format!("Failed to open repository: {e}"))?;

            repo.remote_delete(&remote_name)
                .map_err(|e| format!("Failed to remove temporary remote: {e}"))?;

            Ok(())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Check if a branch has commits that are not in the base branch
pub async fn has_commits_not_in_base(
    repo_path: &Path,
    branch_name: &str,
    base_branch: &str,
) -> Result<bool, String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let branch_name = branch_name.to_owned();
        let base_branch = base_branch.to_owned();
        move || -> Result<bool, String> {
            let repo = Repository::open(&repo_path)
                .map_err(|e| format!("Failed to open repository: {e}"))?;

            // Get the branch reference
            let branch_ref = format!("refs/heads/{branch_name}");
            let branch = repo
                .find_reference(&branch_ref)
                .map_err(|e| format!("Failed to find branch {branch_name}: {e}"))?;

            let branch_oid = branch
                .target()
                .ok_or_else(|| format!("Branch {branch_name} has no target"))?;

            // Get the base branch reference
            let base_ref = format!("refs/heads/{base_branch}");
            let base = repo
                .find_reference(&base_ref)
                .map_err(|e| format!("Failed to find base branch {base_branch}: {e}"))?;

            let base_oid = base
                .target()
                .ok_or_else(|| format!("Base branch {base_branch} has no target"))?;

            // If they point to the same commit, there are no unique commits
            if branch_oid == base_oid {
                return Ok(false);
            }

            // Check if the branch commit is reachable from the base branch
            // If it is, then there are no unique commits in the branch
            match repo.graph_descendant_of(base_oid, branch_oid) {
                Ok(true) => Ok(false), // branch is behind base, no unique commits
                Ok(false) => Ok(true), // branch has commits not in base
                Err(_) => {
                    // If we can't determine the relationship, assume there are commits
                    // This is safer than assuming there aren't
                    Ok(true)
                }
            }
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Delete a branch
pub async fn delete_branch(repo_path: &Path, branch_name: &str) -> Result<(), String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let branch_name = branch_name.to_owned();
        move || -> Result<(), String> {
            let repo = Repository::open(&repo_path)
                .map_err(|e| format!("Failed to open repository: {e}"))?;

            let mut branch = repo
                .find_branch(&branch_name, git2::BranchType::Local)
                .map_err(|e| format!("Failed to find branch {branch_name}: {e}"))?;

            branch
                .delete()
                .map_err(|e| format!("Failed to delete branch {branch_name}: {e}"))?;

            Ok(())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Get the current commit SHA
///
/// # Errors
///
/// Returns an error if:
/// - The repository cannot be opened
/// - HEAD cannot be resolved (e.g., empty repository with no commits)
/// - The HEAD reference does not point to a valid commit
pub async fn get_current_commit(repo_path: &Path) -> Result<String, String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        move || -> Result<String, String> {
            let repo = Repository::open(&repo_path)
                .map_err(|e| format!("Failed to open repository: {e}"))?;

            let head = repo
                .head()
                .map_err(|e| format!("Failed to get HEAD: {e}"))?;

            let commit = head
                .peel_to_commit()
                .map_err(|e| format!("Failed to get commit from HEAD: {e}"))?;

            Ok(commit.id().to_string())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Create a branch from a specific commit
pub async fn create_branch_from_commit(
    repo_path: &Path,
    branch_name: &str,
    commit_sha: &str,
) -> Result<(), String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let branch_name = branch_name.to_owned();
        let commit_sha = commit_sha.to_owned();
        move || -> Result<(), String> {
            let repo = Repository::open(&repo_path)
                .map_err(|e| format!("Failed to open repository: {e}"))?;

            let oid =
                git2::Oid::from_str(&commit_sha).map_err(|e| format!("Invalid commit SHA: {e}"))?;

            let commit = repo
                .find_commit(oid)
                .map_err(|e| format!("Failed to find commit {commit_sha}: {e}"))?;

            repo.branch(&branch_name, &commit, false)
                .map_err(|e| format!("Failed to create branch: {e}"))?;

            repo.set_head(&format!("refs/heads/{branch_name}"))
                .map_err(|e| format!("Failed to checkout branch: {e}"))?;

            // Force update the working directory to match the commit
            let mut checkout_opts = git2::build::CheckoutBuilder::new();
            checkout_opts.force();
            repo.checkout_head(Some(&mut checkout_opts))
                .map_err(|e| format!("Failed to update working directory: {e}"))?;

            Ok(())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Get list of all non-ignored files in the working directory
/// This includes tracked files (with or without modifications), staged files, and untracked files
pub async fn get_all_non_ignored_files(repo_path: &Path) -> Result<Vec<PathBuf>, String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        move || -> Result<Vec<PathBuf>, String> {
            let repo = Repository::open(&repo_path)
                .map_err(|e| format!("Failed to open repository: {e}"))?;

            let mut opts = git2::StatusOptions::new();
            opts.include_untracked(true)
                .include_ignored(false)
                .include_unmodified(true);

            let statuses = repo
                .statuses(Some(&mut opts))
                .map_err(|e| format!("Failed to get repository status: {e}"))?;

            let mut files = Vec::new();

            for entry in statuses.iter() {
                let status = entry.status();
                if let Some(path) = entry.path()
                    && !status.is_ignored()
                {
                    files.push(PathBuf::from(path));
                }
            }

            Ok(files)
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Validate that a branch exists and points to an accessible commit
///
/// # Errors
///
/// Returns an error if:
/// - The repository cannot be opened
/// - The branch reference does not exist
/// - The branch has no target commit
/// - The target commit object is not accessible in the object database
pub async fn validate_branch_accessible(repo_path: &Path, branch_name: &str) -> Result<(), String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let branch_name = branch_name.to_owned();
        move || -> Result<(), String> {
            let repo = Repository::open(&repo_path)
                .map_err(|e| format!("Failed to open repository: {e}"))?;

            // Check branch exists
            let branch_ref = format!("refs/heads/{branch_name}");
            let reference = repo
                .find_reference(&branch_ref)
                .map_err(|e| format!("Branch '{}' not found: {}", branch_name, e))?;

            // Check branch points to valid commit
            let oid = reference
                .target()
                .ok_or_else(|| format!("Branch '{}' has no target", branch_name))?;

            // Try to find the commit - this validates object accessibility
            repo.find_commit(oid).map_err(|e| {
                format!(
                    "Branch '{}' points to inaccessible commit {}: {}",
                    branch_name, oid, e
                )
            })?;

            Ok(())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Clone a local repository without hardlinks
///
/// This creates an optimized copy of the source repository at the destination path.
/// The clone operation repacks objects efficiently, typically resulting in 1-2 pack files
/// instead of preserving fragmented pack structure.
///
/// # Arguments
///
/// * `source_repo_path` - Path to the source repository to clone
/// * `destination_path` - Path where the cloned repository will be created
///
/// # Errors
///
/// Returns an error if:
/// - The source path is invalid or not a git repository
/// - The destination path cannot be created
/// - The clone operation fails
pub async fn clone_local(source_repo_path: &Path, destination_path: &Path) -> Result<(), String> {
    tokio::task::spawn_blocking({
        let source_repo_path = source_repo_path.to_owned();
        let destination_path = destination_path.to_owned();
        move || -> Result<(), String> {
            let mut builder = git2::build::RepoBuilder::new();
            builder.clone_local(git2::build::CloneLocal::NoLinks);

            builder
                .clone(
                    source_repo_path.to_str().ok_or("Invalid source path")?,
                    &destination_path,
                )
                .map_err(|e| format!("Failed to clone repository: {e}"))?;

            Ok(())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Get the current branch name
///
/// Returns the name of the currently checked out branch, or None if
/// the repository is in a detached HEAD state.
///
/// # Errors
///
/// Returns an error if:
/// - The repository cannot be opened
/// - HEAD cannot be resolved
pub async fn get_current_branch(repo_path: &Path) -> Result<Option<String>, String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        move || -> Result<Option<String>, String> {
            let repo = Repository::open(&repo_path)
                .map_err(|e| format!("Failed to open repository: {e}"))?;

            let head = repo
                .head()
                .map_err(|e| format!("Failed to get HEAD: {e}"))?;

            if head.is_branch() {
                Ok(head.shorthand().map(|s| s.to_string()))
            } else {
                // Detached HEAD state
                Ok(None)
            }
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Get the git-town parent branch for a given branch
pub async fn get_git_town_parent(
    repo_path: &Path,
    branch_name: &str,
) -> Result<Option<String>, String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let branch_name = branch_name.to_owned();
        move || -> Result<Option<String>, String> {
            let repo = Repository::open(&repo_path)
                .map_err(|e| format!("Failed to open repository: {e}"))?;
            let config = repo
                .config()
                .map_err(|e| format!("Failed to get repository config: {e}"))?;
            let key = format!("git-town-branch.{}.parent", branch_name);
            match config.get_string(&key) {
                Ok(parent) => Ok(Some(parent)),
                Err(_) => Ok(None),
            }
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Check if `ancestor` is an ancestor of `descendant`
pub async fn is_ancestor(
    repo_path: &Path,
    ancestor: &str,
    descendant: &str,
) -> Result<bool, String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let ancestor = ancestor.to_owned();
        let descendant = descendant.to_owned();
        move || -> Result<bool, String> {
            let output = std::process::Command::new("git")
                .current_dir(&repo_path)
                .args(["merge-base", "--is-ancestor", &ancestor, &descendant])
                .output()
                .map_err(|e| format!("Failed to execute git merge-base --is-ancestor: {e}"))?;
            Ok(output.status.success())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Compute the merge-base of two refs
pub async fn merge_base(repo_path: &Path, ref1: &str, ref2: &str) -> Result<String, String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let ref1 = ref1.to_owned();
        let ref2 = ref2.to_owned();
        move || -> Result<String, String> {
            let output = std::process::Command::new("git")
                .current_dir(&repo_path)
                .args(["merge-base", &ref1, &ref2])
                .output()
                .map_err(|e| format!("Failed to execute git merge-base: {e}"))?;
            if !output.status.success() {
                return Err(format!(
                    "Failed to compute merge-base: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Resolve a git ref to a commit SHA
pub async fn rev_parse(repo_path: &Path, ref_str: &str) -> Result<String, String> {
    tokio::task::spawn_blocking({
        let repo_path = repo_path.to_owned();
        let ref_str = ref_str.to_owned();
        move || -> Result<String, String> {
            let output = std::process::Command::new("git")
                .current_dir(&repo_path)
                .args(["rev-parse", "--verify", &ref_str])
                .output()
                .map_err(|e| format!("Failed to execute git rev-parse: {e}"))?;
            if !output.status.success() {
                return Err(format!(
                    "Failed to resolve ref '{}': {}",
                    ref_str,
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        }
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))?
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::test_utils::TestGitRepository;

    #[tokio::test]
    async fn test_is_git_repository() {
        // Test with a directory that is not a git repository
        let non_git_dir = TestGitRepository::new().unwrap();
        let is_repo = is_git_repository(non_git_dir.path()).await.unwrap();
        assert!(!is_repo, "Non-git directory should return false");

        // Test with a valid git repository
        let git_dir = TestGitRepository::new().unwrap();
        git_dir.init().unwrap();
        let is_repo = is_git_repository(git_dir.path()).await.unwrap();
        assert!(is_repo, "Git repository should return true");

        // Test with a subdirectory inside a git repository
        let subdir = git_dir.path().join("subdir");
        std::fs::create_dir(&subdir).unwrap();
        let is_repo = is_git_repository(&subdir).await.unwrap();
        assert!(
            is_repo,
            "Subdirectory inside git repository should return true"
        );
    }

    #[tokio::test]
    async fn test_git_operations_with_real_repo() {
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path();

        // After init_with_commit, status should be clean
        let status = get_status(repo_path).await.unwrap();
        assert_eq!(status, "");

        // Create a test file
        test_repo.create_file("test.txt", "Hello, world!").unwrap();

        // Test get_status with untracked file
        let status = get_status(repo_path).await.unwrap();
        assert!(status.contains("?? test.txt"));

        // Test add_all
        add_all(repo_path).await.unwrap();

        // Test get_status after add
        let status = get_status(repo_path).await.unwrap();
        assert!(status.contains("A  test.txt"));

        // Test commit
        commit(repo_path, "Add test file").await.unwrap();

        // Test get_status after commit
        let status = get_status(repo_path).await.unwrap();
        assert_eq!(status, "");

        // Test create_branch
        create_branch(repo_path, "test-branch").await.unwrap();

        // Verify we're on the new branch
        let branch = test_repo.current_branch().unwrap();
        assert_eq!(branch, "test-branch");
    }

    #[tokio::test]
    async fn test_git_operations_remotes() {
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init().unwrap();
        let repo_path = test_repo.path();

        // Test add_remote
        add_remote(repo_path, "origin", "https://github.com/test/repo.git")
            .await
            .unwrap();

        // Test adding the same remote again (should not error)
        add_remote(repo_path, "origin", "https://github.com/test/repo.git")
            .await
            .unwrap();

        // Test remove_remote
        remove_remote(repo_path, "origin").await.unwrap();
    }

    #[tokio::test]
    async fn test_get_current_commit() {
        let test_repo = TestGitRepository::new().unwrap();
        let initial_sha = test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path();

        // Get the current commit via the function under test
        let commit_sha = get_current_commit(repo_path).await.unwrap();
        assert!(!commit_sha.is_empty());
        assert_eq!(commit_sha.len(), 40);

        // Verify it matches the SHA from init_with_commit
        assert_eq!(commit_sha, initial_sha);
    }

    #[tokio::test]
    async fn test_create_branch_from_commit() {
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path();

        // Create first commit (beyond the initial one)
        test_repo.create_file("file1.txt", "First file").unwrap();
        test_repo.stage_all().unwrap();
        test_repo.commit("First commit").unwrap();
        let first_commit_sha = get_current_commit(repo_path).await.unwrap();

        // Create second commit
        test_repo.create_file("file2.txt", "Second file").unwrap();
        test_repo.stage_all().unwrap();
        test_repo.commit("Second commit").unwrap();

        // Create a branch from the first commit
        create_branch_from_commit(repo_path, "feature-from-first", &first_commit_sha)
            .await
            .unwrap();

        // Verify we're on the new branch
        let branch = test_repo.current_branch().unwrap();
        assert_eq!(branch, "feature-from-first");

        // Verify the branch is at the first commit
        let current_sha = get_current_commit(repo_path).await.unwrap();
        assert_eq!(current_sha, first_commit_sha);

        // Verify the second file doesn't exist in the working directory
        assert!(!repo_path.join("file2.txt").exists());
        assert!(repo_path.join("file1.txt").exists());
    }

    #[tokio::test]
    async fn test_get_current_commit_empty_repository() {
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init().unwrap();
        let repo_path = test_repo.path();

        // Attempt to get current commit from empty repository
        let result = get_current_commit(repo_path).await;

        // Verify it returns an error (not a panic)
        assert!(
            result.is_err(),
            "get_current_commit should return error on empty repository"
        );

        // Verify the error message is descriptive
        let error_message = result.unwrap_err();
        assert!(
            error_message.contains("Failed to get HEAD")
                || error_message.contains("Failed to get commit from HEAD"),
            "Error should mention HEAD failure, got: {error_message}"
        );
    }

    #[tokio::test]
    async fn test_create_branch_empty_repository() {
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init().unwrap();
        let repo_path = test_repo.path();

        // Attempt to create a branch in empty repository
        let result = create_branch(repo_path, "test-branch").await;

        // Verify it returns an error (not a panic)
        assert!(
            result.is_err(),
            "create_branch should return error on empty repository"
        );

        // Verify the error message is descriptive
        let error_message = result.unwrap_err();
        assert!(
            error_message.contains("Failed to get commit from HEAD")
                || error_message.contains("Failed to get HEAD")
                || error_message.contains("unborn"),
            "Error should mention HEAD or unborn branch, got: {error_message}"
        );
    }

    #[tokio::test]
    async fn test_clone_local() {
        // Create source repository with commits
        let source_repo = TestGitRepository::new().unwrap();
        source_repo.init_with_commit().unwrap();

        // Create first commit
        source_repo.create_file("file1.txt", "First file").unwrap();
        source_repo.stage_all().unwrap();
        source_repo.commit("First commit").unwrap();
        let first_commit_sha = get_current_commit(source_repo.path()).await.unwrap();

        // Create second commit
        source_repo.create_file("file2.txt", "Second file").unwrap();
        source_repo.stage_all().unwrap();
        source_repo.commit("Second commit").unwrap();
        let second_commit_sha = get_current_commit(source_repo.path()).await.unwrap();

        // Clone the repository
        let dest_repo = TestGitRepository::new().unwrap();
        let dest_path = dest_repo.path().join("cloned_repo");
        clone_local(source_repo.path(), &dest_path).await.unwrap();

        // Verify cloned repository exists and is a valid git repository
        assert!(dest_path.exists(), "Cloned repository should exist");
        assert!(
            dest_path.join(".git").exists(),
            "Cloned repository should have .git directory"
        );

        // Verify the cloned repository has the same HEAD commit
        let cloned_head_sha = get_current_commit(&dest_path).await.unwrap();
        assert_eq!(
            cloned_head_sha, second_commit_sha,
            "Cloned repository should have the same HEAD commit"
        );

        // Verify both files exist in the cloned repository
        assert!(
            dest_path.join("file1.txt").exists(),
            "First file should exist in cloned repo"
        );
        assert!(
            dest_path.join("file2.txt").exists(),
            "Second file should exist in cloned repo"
        );

        // Verify we can access the first commit in the cloned repository
        let first_commit_oid = git2::Oid::from_str(&first_commit_sha).unwrap();
        let cloned_git_repo = git2::Repository::open(&dest_path).unwrap();
        let first_commit = cloned_git_repo.find_commit(first_commit_oid).unwrap();
        assert_eq!(
            first_commit.id().to_string(),
            first_commit_sha,
            "Should be able to access first commit in cloned repo"
        );

        // Verify pack file optimization (should have 1-2 pack files, not 30+)
        let pack_dir = dest_path.join(".git/objects/pack");
        if pack_dir.exists() {
            let pack_files: Vec<_> = std::fs::read_dir(&pack_dir)
                .unwrap()
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry
                        .path()
                        .extension()
                        .and_then(|s| s.to_str())
                        .map(|s| s == "pack")
                        .unwrap_or(false)
                })
                .collect();

            assert!(
                pack_files.len() <= 2,
                "Cloned repository should have at most 2 pack files, found {}",
                pack_files.len()
            );
        }
    }

    #[tokio::test]
    async fn test_get_current_branch_on_branch() {
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path();

        // Get current branch - should be main or master
        let branch = get_current_branch(repo_path).await.unwrap();
        assert!(branch.is_some());
        let branch_name = branch.unwrap();
        assert!(
            branch_name == "main" || branch_name == "master",
            "Expected main or master, got: {}",
            branch_name
        );
    }

    #[tokio::test]
    async fn test_get_current_branch_custom_branch() {
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path();

        // Create and checkout a new branch
        create_branch(repo_path, "feature-branch").await.unwrap();

        // Get current branch - should be feature-branch
        let branch = get_current_branch(repo_path).await.unwrap();
        assert_eq!(branch, Some("feature-branch".to_string()));
    }

    #[tokio::test]
    async fn test_get_current_branch_detached_head() {
        let test_repo = TestGitRepository::new().unwrap();
        test_repo.init_with_commit().unwrap();
        let repo_path = test_repo.path();

        // Detach HEAD at the current commit
        test_repo
            .run_git_command(&["checkout", "--detach"])
            .unwrap();

        // Get current branch - should be None for detached HEAD
        let branch = get_current_branch(repo_path).await.unwrap();
        assert!(
            branch.is_none(),
            "Expected None for detached HEAD, got: {:?}",
            branch
        );
    }
}
