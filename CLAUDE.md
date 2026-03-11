# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Important files:
- @justfile - key development commands. These should be used when possible over raw cargo commands.
- @README.md - user facing documentation for the project. This could cover key user facing details without going into too much detail. Make sure it stays up to date with your changes.

## Architecture Overview

`tsk` implements a command pattern with dependency injection for testability. The core workflow: queue tasks → execute in containers (Docker or Podman) → create git branches for review. `tsk` can run in server mode for continuous task processing across multiple repositories.

### Key Components

- **CLI Commands** (`src/commands/`): Command handlers for all `tsk` subcommands. See README.md for the full command reference.
- **Task Management** (`src/task.rs`, `src/context/task_storage.rs`, `src/task_manager.rs`, `src/task_runner.rs`): `TaskBuilder` for task creation, SQLite-backed `TaskStorage`, task lifecycle (Queued → Running → Complete/Failed/Cancelled). Two execution paths: server-scheduled (via `add`) and inline (via `run`/`shell`).
- **Docker Integration** (`src/docker/`): `DockerImageManager` (layered image builds: base → stack → project → agent), `ProxyManager` (proxy lifecycle with per-configuration instances), `DockerManager` (container execution). Security-first with dropped capabilities and per-container network isolation.
- **Server Mode** (`src/server/`): `TskServer` daemon with `TaskScheduler` and `WorkerPool` for parallel task execution. Parent-aware scheduling, cascading failure handling, and auto-cleanup of old tasks.
- **TUI** (`src/tui/`): Interactive terminal dashboard using ratatui/crossterm. Two-panel layout (task list + log viewer) with `ServerEvent` channel from scheduler.
- **Git Operations** (`src/git.rs`, `src/git_sync.rs`, `src/git_operations.rs`, `src/repo_utils.rs`): Repository cloning with working directory overlay, branch creation, result fetching. Uses git CLI (not libgit2) for commits and post-overlay renormalization to support clean/smudge filters (e.g., Git LFS). `GitSyncManager` for concurrent access safety. Supports submodules, git worktrees, and Git LFS.
- **Storage/Config** (`src/context/`): `AppContext` (DI container), `TskEnv` (directory paths), `TskConfig` (user/project config with layered resolution). See README.md for config file reference.
- **Agents** (`src/agent/`): `Agent` trait for AI agent integration (claude, codex, integ, no-op). Handles command building, validation, warmup, and version tracking.
- **Auto-Detection** (`src/repository.rs`): Stack detection from project files and project name from directory name.
- **Skills Marketplace** (`skills/`, `.claude-plugin/marketplace.json`): Claude Code skills following the Agent Skills open standard.

### Key Design Principles

- **Docker client is lazy**: Not part of `AppContext`; constructed only at command entry points that need containers. Commands like `add`, `list`, `clean`, `delete` work without a Docker daemon.
- **Config snapshotting**: At task creation, the resolved config is serialized and stored with the task. Execution uses the snapshot, not live config files. Chained tasks inherit the parent's snapshot.
- **Dual-lock concurrency**: `ProxySyncManager` and `GitSyncManager` both use in-process `tokio::Mutex` + cross-process `flock(2)` for safe concurrent access across threads and processes.
- **Two output paths**: Task-scoped messages use `TaskLogger` (writes to `agent.log`, optionally prints to stdout). Global messages use `emit_or_print` (sends `ServerEvent` in TUI mode, prints otherwise).
- **Graceful degradation**: Submodule setup failures fall back to regular files. Git-town errors log warnings and continue. Missing config uses defaults.

## Development Workflows

- `just test` — run the test suite
- `just format` — auto-format Rust source
- `just lint` — clippy with warnings as errors
- `just precommit` — format, lint, test, and integration tests (if in a TSK container). Run before committing.
- `just integration-test` — stack layer integration tests (requires Docker/Podman)

## Coding Conventions

- Avoid `#[allow(dead_code)]` directives
- Avoid `unsafe` blocks
- Keep documentation up to date following rustdoc best practices
- Keep CLAUDE.md simple but up to date

### Commit Conventions

Commits use conventional commit prefixes mapped to changelog groups via `release-plz.toml`:

- `feat`: New user-facing functionality (appears in release notes under "added")
- `fix`: Bug fixes to existing behavior (appears in release notes under "fixed")
- `docs`: Documentation-only changes (appears in release notes under "documentation")
- `test`: Adding or updating tests (excluded from release notes)
- `refactor`: Code restructuring with no behavior change (excluded from release notes)
- `chore`: Maintenance tasks like dependency updates, CI config, releases (excluded from release notes)

Use `refactor` when reorganizing code internals without changing what the software does. Use `chore` for non-code changes or tooling. Both are skipped in changelogs, so use `feat` or `fix` if the change is meaningful to users.

For user-facing breaking changes, add `!` after the type (e.g., `feat!:`, `fix!:`). The commit title must explain why the change is breaking so users understand the impact without reading the full diff (e.g., `feat!: rename --workers to --concurrency for clarity`).

### Testing Conventions

- Prefer integration tests using real implementations over mocks
- Use `TestGitRepository` from `test_utils::git_test_utils` for tests requiring git repositories. Use `init_with_main_branch()` to create a repo with a `main` branch and initial commit, `clone_from(source)` to set up a task-clone of another repo, and `add_submodule(source, path)` for submodule test scenarios
- Tests should use temporary directories that are automatically cleaned up
- Make tests thread safe so they can be run in parallel
- Always use `AppContext::builder()` for test setup rather than creating objects contained in the `AppContext` directly
  - Exception: Tests in `src/context/*` that are directly testing TskEnv or TskConfig functionality
  - Use `let ctx = AppContext::builder().build();` to automatically set up test-safe temporary directories and configurations
- Keep tests simple and concise while still testing core functionality. Improving existing tests is always preferred over adding new ones
- Avoid using `#[allow(dead_code)]`
- Limit `#[cfg(test)]` to tests and test utilities
- Stack layer integration tests live in `tests/integration/projects/<stack>/`, each with a `tsk-integ-test.sh` script
  - Optional `tsk-integ-setup.sh`: runs before the initial commit (e.g., for `git lfs install --local`)
  - Optional `tsk-integ-verify.sh`: runs on the task branch after completion to verify results
  - Optional `tsk-integ-stack.txt`: overrides the auto-detected stack name (e.g., `git-lfs` project uses `default` stack)

### Branch and Task Conventions

- Tasks create branches: `tsk/{task-type}/{task-name}/{task-id}` (e.g., `tsk/feat/add-user-auth/a1b2c3d4`) by default. When `target_branch` is set, the task reuses that branch name instead (falling back to the generated name on non-fast-forward).
- Templates support YAML-style frontmatter (`---` delimited) with a `description` field shown in `tsk template list`. Frontmatter is stripped before content reaches agents.
