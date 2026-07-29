# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.10.9](https://github.com/dtormoen/tsk-tsk/compare/v0.10.8...v0.10.9) - 2026-07-29

### Added

- surface warmup failures to `tsk wait` and `tsk add --wait`

### Fixed

- keep long agent commands in foreground in headless mode
- wait indefinitely for Claude background tasks in headless mode
- stop setting PYTHONPATH automatically for the python stack

## [0.10.8](https://github.com/dtormoen/tsk-tsk/compare/v0.10.7...v0.10.8) - 2026-04-03

### Added

- add --branch flag to tsk add, run, and shell commands
- add `tsk wait` command and `--wait` flag on `tsk add`

### Fixed

- initialize submodules when using --branch flag
- prevent --edit from hanging in non-interactive contexts

## [0.10.7](https://github.com/dtormoen/tsk-tsk/compare/v0.10.6...v0.10.7) - 2026-03-30

### Added

- add privileged mode, sudo, and device passthrough for containers

## [0.10.6](https://github.com/dtormoen/tsk-tsk/compare/v0.10.5...v0.10.6) - 2026-03-22

Many Fix/improvements for using `tsk` on Ubuntu/Linux. Fixes container image build
failures caused by UID/GID mismatches on non-macOS hosts, and adds support for rootless
Podman (health checks, iptables fallback, cgroup detection).

### Added

- platform-aware DIND storage with auto-cleanup

### Fixed

- handle pre-existing GIDs in container image builds
- build container images with host UID/GID for non-1000 UID compatibility
- skip Podman resource limits when cgroup controllers unavailable
- support rootless Podman proxy with graceful iptables fallback
- use exec-based proxy health checks for rootless Podman compatibility

## [0.10.5](https://github.com/dtormoen/tsk-tsk/compare/v0.10.4...v0.10.5) - 2026-03-21

### Fixed

- default to worktree state when retrying tasks without a parent repo
- use worktree branch state when launching tasks from git worktrees
- skip desktop notifications on headless Linux environments

## [0.10.4](https://github.com/dtormoen/tsk-tsk/compare/v0.10.3...v0.10.4) - 2026-03-14

### Fixed

- resolve git-lfs integration test failure on Podman
- skip broken symlinks in Docker build context and show full error chain ([#67](https://github.com/dtormoen/tsk-tsk/pull/67))
- suppress task_progress system messages from agent logs

## [0.10.3](https://github.com/dtormoen/tsk-tsk/compare/v0.10.2...v0.10.3) - 2026-03-09

### Documentation

- simplify CLAUDE.md by removing redundancy with README and implementation details

### Fixed

- use config for stack/agent resolution in `tsk docker build`
- prevent LFS-tracked files from appearing dirty after repo copy
- route git operation warnings through TaskLogger instead of stderr
- resolve git worktree paths to main repo root for task creation
- support git-lfs repositories by replacing libgit2 with git CLI for commits

## [0.10.2](https://github.com/dtormoen/tsk-tsk/compare/v0.10.1...v0.10.2) - 2026-03-01

### Added

- update tsk-add skill to use `--prompt-file`

### Fixed

- support Claude Code Agent tool in log processor

## [0.10.1](https://github.com/dtormoen/tsk-tsk/compare/v0.10.0...v0.10.1) - 2026-02-27

### Fixed

- capture Docker build output in TUI log viewer

## [0.10.0](https://github.com/dtormoen/tsk-tsk/compare/v0.9.1...v0.10.0) - 2026-02-26

This release switches to using `--prompt`/`-p` to pass in a prompt to create a task which is more intuitive
and commonly used in other tools than `--description`/`-d`. The GitHub repo was also renamed to `tsk-tsk`
which is just a cosmetic change.

`--description`/`-d` will still work for `tsk add`, just with a deprecation warning. The breaking changes are:
- `-p` used to be the short form of `--parent`. Now you must use `--parent`
- `--prompt` used to allow you to pass a prompt file bypassing the task template which was a confusing behavior.
  Now you can use `--prompt-file` either without specifying a task template to get the old behavior or with a
  task template to inject into a task template. Injecting into a task template is very useful if you use Claude
  Code's plan mode to interactively craft a plan and then tell Claude Code to pass the plan file to `tsk`.

### Added

- [**breaking**] restructure CLI flags: --prompt/-p replaces --description/-d

### Documentation

- update name to tsk-tsk
- update documentation and templates for --prompt/-p rename

## [0.9.1](https://github.com/dtormoen/tsk/compare/v0.9.0...v0.9.1) - 2026-02-24

This release adds an interactive TUI dashboard to `tsk server start` and a `tsk cancel`
command. When running in an interactive terminal, the server now shows a split-pane view
with a task list on the left and a live log viewer on the right. Navigate with vim keys,
mouse, or arrow keys. Cancel and delete tasks directly from the dashboard with `c` and `d`.
When stdout is piped, the server falls back to plain text output as before.

### Added

- interactive TUI dashboard for `tsk server start` with task list, log viewer, mouse support, and keyboard navigation
- `tsk cancel` command for cancelling running or queued tasks
- CANCELLED task status: Ctrl+C during `tsk run` and server shutdown now mark tasks as CANCELLED instead of FAILED
- per-task structured log files (`agent.log`) capturing infrastructure phases and agent output

### Fixed

- prevent proxy race condition between concurrent task startup and shutdown
- fix piped stdin detection on platforms where `is_terminal` was unreliable

## [0.9.0](https://github.com/dtormoen/tsk/compare/v0.8.6...v0.9.0) - 2026-02-23

This is a large release that replaces filesystem-based Docker layer customization with
inline TOML configuration and restructures the config file format. See the migration
guide below if you have an existing `tsk.toml` or custom Dockerfiles.

**Migrating from 0.8.x:**

- **Config format**: The old `[docker]`, `[proxy]`, and `[git_town]` sections are replaced
  by `[defaults]` and `[project.<name>]`. TSK detects the old format and prints a migration
  error with guidance. Move your settings into the new shape — see the
  [Configuration File](README.md#configuration-file) section in the README for examples.
- **Custom Dockerfiles**: Filesystem-based Docker layers (e.g., `.tsk/dockerfiles/project/`,
  `~/.config/tsk/dockerfiles/`) are no longer used. Replace them with `setup`,
  `stack_config.<name>.setup`, or `agent_config.<name>.setup` fields in your `tsk.toml`.
  Run `tsk docker build --dry-run` to preview the generated Dockerfile.
- **Project-level config**: You can now check in shared project defaults at `.tsk/tsk.toml`
  (same shape as `[defaults]`).
- **Claude Code users**: Install the `tsk-config` skill to have Claude walk you through
  the migration interactively: `/plugin marketplace add dtormoen/tsk` then
  `/plugin install tsk-config@dtormoen/tsk`.

### Added

- [**breaking**] restructure config with `[defaults]` / `[project.<name>]` sections and project-level `.tsk/tsk.toml`
- replace filesystem-based Docker layers with inline `setup`, `stack_config`, and `agent_config` fields in tsk.toml
- auto-migrate deprecated tsk.toml config format with actionable error messages
- snapshot resolved config at task creation for reproducible execution
- per-configuration proxy instances with fingerprint-based naming
- add `tsk template show` and `tsk template edit` commands
- add Claude Code skills marketplace with `tsk-config` and `tsk-add` skills
- clean up CLI output across all commands

### Fixed

- resolve proxy IP on agent network for extra_hosts injection
- use unique temp dirs in worktree tests to avoid collisions
- resolve git worktree paths for lock files, project names, and submodules
- trigger rebuild when embedded dockerfiles or templates change
- create Podman storage directory in base image with correct ownership
- always export TSK_PROXY_HOST so containers can reach the proxy
- inject Java proxy config at runtime instead of build time

## [0.8.6](https://github.com/dtormoen/tsk/compare/v0.8.5...v0.8.6) - 2026-02-23

### Added

- add interactive flag to AppContext to fix test hangs in TTY
- prompt to use parent task's repository when retrying a child task
- prompt to retry child tasks when retrying a parent task
- proactively check Claude OAuth token expiry before scheduling tasks
- save docker build logs to task directory on failure

### Fixed

- log OAuth token expiry on every check with retry countdown
- detect is_error field in Claude result messages
- capture docker build output and only display on failure
- remove misleading 'tsk run' hint from add command output

## [0.8.5](https://github.com/dtormoen/tsk/compare/v0.8.4...v0.8.5) - 2026-02-21

### Added

- upgrade base image to Ubuntu 25.10
- alias docker to podman in agent containers for DIND compatibility
- add --dind flag to opt in to DIND security relaxations
- add Podman DIND support for nested container builds

### Fixed

- disable SELinux and increase resource limits for Podman-in-Podman DIND
- set BUILDAH_ISOLATION=chroot for DIND containers to fix nested Podman builds
- preserve executable file permissions during repo copy

## [0.8.4](https://github.com/dtormoen/tsk/compare/v0.8.3...v0.8.4) - 2026-02-19

### Fixed

- stop echoing piped description content in tsk add output
- slim down Docker images by removing unnecessary dependencies

### Other

- update install directions ([#38](https://github.com/dtormoen/tsk/pull/38))

## [0.8.3](https://github.com/dtormoen/tsk/compare/v0.8.2...v0.8.3) - 2026-02-16

### Added

- add --no-network-isolation flag to disable per-container network isolation

### Fixed

- improve Docker image portability and Java proxy support
- send notification on container infrastructure failures
- unify task success/failure into single TaskResult synthesis
- align CLI exit codes with task success/failure status

### Other

- centralize resource cleanup in DockerManager::run_task_container

## [0.8.2](https://github.com/dtormoen/tsk/compare/v0.8.1...v0.8.2) - 2026-02-15

This release adds support for Podman as an alternative to Docker. Enable Podman
by setting the following in your `tsk.toml`:

```toml
[docker]
container_engine = "podman"
```

### Added

- add Podman as alternative container engine for host-machine use
- mount repos at /workspace/{project_name} for project-scoped agent memories
- use `claude auth status` for login validation and warmup

### Other

- remove CLAUDE.md content redundant with README
- document Podman support in README
- add Task::test_default() and migrate test construction to struct update syntax
- use struct update syntax for TskConfig construction in tests

## [0.8.1](https://github.com/dtormoen/tsk/compare/v0.8.0...v0.8.1) - 2026-02-12

### Added

- add configurable auto-cleanup settings to [server] config section
- add override options to retry command (--name, --agent, --stack, --project, --parent)
- expand repo lock scope to cover submodule fetches and post-fetch operations
- replace in-process mutex with cross-process flock(2) file locking in GitSyncManager
- auto-clean old tasks in server mode

### Other

- add section descriptions to README command groups
- remove unused get_full_log from LogProcessor trait

## [0.8.0](https://github.com/dtormoen/tsk/compare/v0.7.1...v0.8.0) - 2026-02-09

There are a lot of under the hood improvements, but the headlines here are:
- Added a `--parent` flag for tasks which allows task chaining. Very powerful
  when combined with a Claude Code skill that teaches Claude how to use TSK.
- Added automatic tsk-proxy container management so it'll shut down when not in use.
- `tsk server start` will now kill running containers when you use ctrl-c to exit.
- Migrated json task storage to sql and renamed task directories to be just their task ID.
- Added the ability to add a description to your task templates via yaml frontmatter.
  Run `tsk template list` to see task descriptions.
- Improved TUI output tables in `tsk list` and `tsk template list` commands.

Task chaining allows you to specify parent child relationships between tasks. TSK will
wait to schedule a child task until the parent completes and the child will start from
the branch that the parent created.

There are two breaking changes:
- The `tsk proxy` commands are removed in favor of automatic proxy container management.
- Migrating to SQL storage and simplifying task directory paths. These changes did not
  cause issues in my testing, however, this is a large change and there may be cases
  that are broken with tasks created before the migration.

### Added

- implement graceful server shutdown with container cleanup
- skip cleaning parent tasks with active children
- track shell and run tasks in the database
- replace IPC-based server stop with SIGTERM via PID file
- remove IPC from add and list commands in favor of direct SQLite access
- simplify storage wrapper from Arc<Mutex<Box<dyn TaskStorage>>> to Arc<dyn TaskStorage>
- add busy_timeout PRAGMA to SQLite storage for safe concurrent access
- run network isolation tests conditionally in precommit
- add TSK_CONTAINER and TSK_TASK_ID env vars to task containers
- add color coding to status column in tsk list
- support multiple parent IDs in database schema
- remove JsonTaskStorage and clean up legacy JSON references
- add automatic migration from tasks.json to SQLite
- wire SqliteTaskStorage as default storage backend
- add rusqlite dependency and implement SqliteTaskStorage
- add bold+underline styling to table headers
- add frontmatter support to task templates
- add duration column to list and replace table borders with aligned columns
- add task dependency chaining with --parent flag

### Other

- remove NotificationClient trait, use concrete type in AppContext
- remove TerminalOperations trait, use concrete type in AppContext
- move DockerBuildLockManager out of AppContext into DockerImageManager
- remove GitOperations trait, extract to free functions
- remove FileSystemOperations trait, extract to free functions
- add comments to justfile recipes and simplify CLAUDE.md dev commands
- [**breaking**] remove `tsk proxy` command
- [**breaking**] remove repo hash prefix from task directory paths
- add task chaining with --parent to README

## [0.7.1](https://github.com/dtormoen/tsk/compare/v0.7.0...v0.7.1) - 2026-02-04

### Fixed

- exclude dash from task ID alphabet to prevent CLI flag conflicts
- prevent warmup from polluting session history and using tools
- better socat behavior with database connections

### Other

- improve network isolation diagram

## [0.7.0](https://github.com/dtormoen/tsk/compare/v0.6.5...v0.7.0) - 2026-02-03

This version fixes mutltiple ways agents could bypass the proxy and as a result, could
potentially break workflows that relied on that ability. Agents containers now have their
own separate networks without any external access other the proxy and the proxy has a
more restrictive firewall configuration.

To help ensure that there is a way for common workflows to work, this release adds the
ability to forward specific ports to localhost so you can run local services like development
databases or service endpoints and expose those specific endpoints. This release also adds an
way to pass environment varibles to agents via the tsk.toml config file without needing to
create/edit dockerfiles.

Here is an example tsk.toml which configures `my-project` to pass env variables for Postgres
and Redis which are running on localhost:

```toml
[proxy]
host_services = [5432, 6379]  # e.g., PostgreSQL, Redis

[project.my-project]
env = [
    # Environment variables passed to the container
    { name = "DATABASE_URL", value = "postgres://tsk-proxy:5432/mydb" },
    { name = "REDIS_URL", value = "redis://tsk-proxy:6379" },
]
```

### Added

- add per-project environment variable configuration
- harden proxy container with iptables firewall and security options
- add host service TCP port forwarding via socat
- [**breaking**] implement per-container network isolation for agents

### Fixed

- correct proxy container permissions for tmpfs and squid operation

### Other

- add network isolation guide with architecture diagrams
- add network isolation test script for container security

## [0.6.5](https://github.com/dtormoen/tsk/compare/v0.6.4...v0.6.5) - 2026-02-02

### Added

- add git-town parent branch tracking support
- automatic proxy lifecycle management

### Fixed

- copy ~/.claude.json into containers before starting
- add required endpoint for tsk shell

### Other

- improve README structure and document new config options

## [0.6.4](https://github.com/dtormoen/tsk/compare/v0.6.3...v0.6.4) - 2026-02-01

### Added

- add --proxy-only flag to docker build command

### Fixed

- use different method for notifications on MacOS to prevent hang

### Other

- upgrade dependencies

## [0.6.3](https://github.com/dtormoen/tsk/compare/v0.6.2...v0.6.3) - 2025-12-13

### Added

- human-friendly Docker resource configuration
- add project-specific configuration with volume mounts
- add TOML configuration file support
- make --name optional for add and run commands

### Fixed

- provide test defaults for git config in CI environments

### Other

- add configuration file documentation to README
- resolve git author dynamically from repository
- rename TskConfig to TskEnv and TskOptions to TskConfig

## [0.6.2](https://github.com/dtormoen/tsk/compare/v0.6.1...v0.6.2) - 2025-12-03

### Added

- skip branch creation when task produces no changes
- add full git submodule support for task execution
- add sound notifications on task completion

### Fixed

- configure git identity in submodule tests for CI
- properly add uv to base image

## [0.6.1](https://github.com/dtormoen/tsk/compare/v0.6.0...v0.6.1) - 2025-11-25

### Added

- add uv to base image
- add quit-when-done mode to server start

### Fixed

- add line buffering to Docker log streaming to prevent parse errors
- resolve git fetch errors when copying repos from specific commits

### Other

- update deps
- *(docker)* simplify agent installation and environment setup

## [0.6.0](https://github.com/dtormoen/tsk/compare/v0.5.4...v0.6.0) - 2025-10-26

This release has a few large changes. First we've added
[codex](https://openai.com/codex/) support! You can now launch codex agents or even
launch multiple agents in parallel:

```bash
tsk add --type feat --name greeting --description "Add a greeting to all TSK commands" --agent claude,codex
```

This will create two tasks that are identical except for `claude` will work on one and
`codex` will work on the other. This is great for comparing the performance of different
agents.

We've also simplified TSK commands:
- `tsk debug` is now called `tsk shell`
- `tsk quick` is now called `tsk run`
- `tsk run` has been removed
- `--timeout` has been removed from a few commands as it was not actually implemented
- The `claude-code` agent has been renamed to `claude` throughout the codebase

### Added

- [**breaking**] simplify command structure for better UX
- add multi-agent support for task creation
- *(codex)* improve log processor output quality
- *(codex)* add JSON log processor with human-readable output
- add codex agent support

### Other

- reorganize and enhance README
- improve documentation
- rename claude-code agent to claude

## [0.5.4](https://github.com/dtormoen/tsk/compare/v0.5.3...v0.5.4) - 2025-10-26

### Added

- add pip and ty to python dockerfile

### Fixed

- handle terminal size and resize in debug command

### Other

- move agent layer after project layer since claude code updates frequently

## [0.5.3](https://github.com/dtormoen/tsk/compare/v0.5.2...v0.5.3) - 2025-10-21

### Fixed

- don't mount .claude.json to avoid corrupting it
- deduplicate consecutive parsing errors in Claude Code log processor
- detect lua before python

## [0.5.2](https://github.com/dtormoen/tsk/compare/v0.5.1...v0.5.2) - 2025-10-01

### Fixed

- handle empty git repositories gracefully

## [0.5.1](https://github.com/dtormoen/tsk/compare/v0.5.0...v0.5.1) - 2025-09-25

This release mainly adds a few new features:
- The outputs while agents are working are much more detailed. They include:
    - The model being used e.g. opus or sonnet
    - The sub-agent that is active
    - The full instructions and outputs of sub-agents
- Adds the ability to pipe in your instructions: `echo "Make a sweet app" | tsk add --type feat --name an-app
- Adds the ability to define a custom proxy configuration

### Added

- add stdin pipe support for task descriptions
- enhance claude-code log processor with task-aware multi-agent output tracking
- add output logging for claude-code agent debugging
- display model name in claude code log processor output
- add support for custom squid proxy configuration

### Fixed

- implement atomic file writes to prevent race conditions

### Other

- simplify Agent trait by consolidating build_command methods
- simplify DockerManager by integrating ProxyManager and using AppContext
- simplify TSK configuration by removing redundant XdgConfig struct
- simplify Docker image management interface
- remove unused additional_files functionality from Docker composer
- remove obsolete tsk-base dockerfile

## [0.5.0](https://github.com/dtormoen/tsk/compare/v0.4.1...v0.5.0) - 2025-09-08

This release should make Docker builds more reliable overall, but it does introduce a
breaking change where docker layer snippets for projects, agents, and stacks which
previously would be stored in e.g. `.tsk/dockerfiles/project/<project_name>/Dockerfile`
are now stored in `.tsk/dockerfiles/project/<project_name>.dockerfile`. See the `.tsk`
or `dockerfiles` directory in `tsk` for examples of how to structure Dockerfile snippets.

### Added

- add Docker build lock manager to prevent concurrent image builds
- implement agent version tracking for automatic Docker rebuilds
- increase CPU and memory limits to 8 CPUs and 12gb
- [**breaking**] simplify Docker image generation with Handlebars templating

### Fixed

- copy working directory versions of files including unstaged changes
- improve lua stack snippet
- allow dots and underscores in project names for Docker layer matching

### Other

- upgrade go version in docker snippet
- extract proxy management into dedicated ProxyManager module
- simplify Docker asset directory structure
- *(server)* replace TaskExecutor with TaskScheduler and WorkerPool

## [0.4.1](https://github.com/dtormoen/tsk/compare/v0.4.0...v0.4.1) - 2025-09-01

### Fixed

- handle non-JSON output gracefully in Claude Code log processor
- rework Python tech-stack to use UV with proper virtual environment
- add support for symlinks in git repository copy operations

## [0.4.0](https://github.com/dtormoen/tsk/compare/v0.3.2...v0.4.0) - 2025-08-25

This release is largely focused on refactoring which will help create a solid foundation
for future planned changes. Some notable improvements are:
- Fix that ensures proxy can be restarted (requires a `tsk docker build`)
- Fixed `tsk debug` so now you can start an interactive session with Claude in a docker 
  container or debug issues with the project docker containers you've created
- tsk checks if claude code is working before launching a container. If this step fails, 
  it typically means you've used up your subscription limits. `tsk` will now retry the 
  task once each hour rather than fail it outright. This can be used for example to 
  queue up a lot of work overnight and tsk will wait until your limit is refilled
- Unit tests are faster, no longer flaky, and are easier to properly isolate to run
  in parallel

### Added

- [**breaking**] fix debug command and add interactive agent support
- [**breaking**] add is_interactive field to Task for controlling execution mode
- add automatic retry for agent warmup failures in server mode

### Fixed

- flaky tests
- add delay to interactive commands to prevent missing initial output
- handle stale PID file in proxy container for clean restarts
- add health check to proxy container and wait for readiness
- correct Claude Code log parser to handle TodoWrite output format
- update Claude Code log processor for new JSON format
- clean up temporary instruction file when task creation is cancelled
- update justfile to better reflect CI linting behavior
- optimize warmup failure wait behavior test to run in <1 second
- remove flaky test

### Other

- enable ignored tests using AppContext and mock docker clients
- use Bollard for interactive containers instead of docker CLI
- use AppContext::builder() for test setup instead of XdgConfig::builder()
- simplify layered asset tests using DRY principles and AppContext
- prevent git config access in tests and use AppContext for test isolation
- update dependencies
- rename xdg variable names to better reflect TskConfig usage
- simplify RepoManager by using AppContext directly
- store AppContext in TaskRunner instead of rebuilding it
- simplify DockerImageManager using AppContext and DRY principles
- make executor tests deterministic and remove redundancy
- move git configuration from DockerImageManager to TskConfig
- extract TaskBuilder to separate module for better code organization
- rename XdgDirectories to TskConfig throughout codebase
- replace XdgConfig::with_paths with AppContext in all tests
- simplify test setup by leveraging AppContext's test defaults
- consolidate Config into XdgDirectories and add test-safe context creation
- replace MockAssetManager with FileSystemAssetManager in layered asset tests
- simplify debug command to use TaskManager for unified task execution
- simplify TaskRunner constructor to accept AppContext
- use copied_repo_path from Task struct to determine task directories
- consolidate TaskManager constructors into single new() method
- remove deprecated new() constructor from ClaudeCodeAgent
- pass Config through AgentProvider to eliminate direct env var access
- configure clippy to disallow env variable setting or removing
- add Config struct to centralize environment variable access
- convert RepositoryContext from trait to stateless functions
- add guidance on testing
- fix flaky warmup failure wait behavior test
- stronger wording around fixing just precommit issues
- remove MockFileSystem from file_system module
- replace MockFileSystem with temporary directories in tests
- replace MockFileSystem with temporary directories in tests
- replace MockFileSystem with temporary directories in tests
- remove repository dependency from tsk list command
- update to rust 1.89

## [0.3.2](https://github.com/dtormoen/tsk/compare/v0.3.1...v0.3.2) - 2025-08-05

### Added

- expand project Dockerfile search to include user-level config
- add bulk operations for retry and delete commands
- upgrade Docker images to Ubuntu 24.04 and resolve GID conflict

### Fixed

- ensure project layer detection uses copied repository in server mode

## [0.3.1](https://github.com/dtormoen/tsk/compare/v0.3.0...v0.3.1) - 2025-07-18

This release fixes an issue where Dockerfiles in projects could conflict with tsk's own 
Docker image generation and improves support for Go projects.

### Added

- expand proxy allowed domains for all supported language package managers

### Fixed

- prevent project Dockerfiles from overriding TSK images

## [0.3.0](https://github.com/dtormoen/tsk/compare/v0.2.0...v0.3.0) - 2025-07-09

### Added

- add logging for when container starts
- add --repo argument to add, quick, and debug commands
- make --description optional for templates without {{DESCRIPTION}} placeholder
- add "ack" task type for debugging
- implement parallel task execution with configurable workers

### Fixed

- resolve clippy warnings and improve code quality
- use task ID instead of timestamp for docker container names
- update dependency that causes server hangs
- force local installs to use the lock file
- remove duplicate task status updates causing server shutdown hang
- resolve TSK server worker management and task completion issues

### Other

- add a demo gif of tsk
- remove unused code and fix dead code warnings
- remove unused test utilities and fix dead code warnings
- remove lib target and consolidate binary-only structure
- implement human-readable branch naming format
- [**breaking**] replace timestamp-based task IDs with 8-character nanoid IDs
- rename -i/--instructions flag to -p/--prompt in CLI commands
- [**breaking**] remove log file saving feature from agents
- restructure CLI to follow noun-verb command pattern

## [0.2.0](https://github.com/dtormoen/tsk/compare/v0.1.0...v0.2.0) - 2025-07-07

### Added

- plan template includes success criteria and out of scope section
- ensure proxy image is built before task execution

### Fixed

- release storage mutex lock after task completion to prevent server deadlock
- remove rust specific language from default templates

### Other

- add comprehensive Docker builds documentation
- migrate task_manager tests from mocks to real implementations
- replace git mock implementations with integration tests
- replace git mock implementations with integration tests
- add repo_path parameter to is_git_repository method
- eliminate unsafe blocks in XdgDirectories tests
- add project specific task templates that mention rust best practices
- update install instructions in README.md

## [0.1.0](https://github.com/dtormoen/tsk/releases/tag/v0.1.0) - 2025-06-27

### Added

- add tech-stack Dockerfiles for Go, Python, Node, Java, and Lua
- improve ClaudeCodeLogProcessor with richer output
- extend copy_repo to include untracked non-ignored files
- add filesystem support for project-specific Dockerfiles
- implement streaming Docker build output
- always rebuild Docker images for tasks
- add project-specific Docker layer for tsk
- configure Rust builds to use external target directory
- optimize copy_repo to only copy git-tracked files
- add project-layer template for creating project-specific Docker layers
- add auto-detection for --tech-stack and --project flags
- [**breaking**] introduce DockerImageManager for centralized Docker image management
- [**breaking**] update branch naming scheme to include task type
- add --dry-run flag to docker-build command
- implement multi-level template configuration scheme for Docker images
- implement multi-level template configuration scheme
- transform TSK into installable binary with embedded assets
- *(docker-build)* add --no-cache flag to force fresh builds
- Add retry functionality to tasks command
- add docker-build command to automate Docker image building
- Add a more useful error message when it can't connect to Docker
- Add terminal window title updates during task execution
- Track source commit when tasks are created
- Update templates to use Conventional Commits spec

### Fixed

- resolve format string issues for new Rust version
- resolve all precommit issues and test failures
- use docker cache by default
- remove git repository requirement for tsk run command
- correct Docker layer composition for user switching and CMD placement
- Update Claude settings to avoid timeouts
- remove just target that's no longer needed
- Make it clear in plan template that no code should be written

### Other

- initial release
- add release-plz to manage versioning
- [**breaking**] upgrade project to Rust 2024 edition
- Create rust.yml
- make output of testing commands less verbose
- make Task::new require created_at and copied_repo_path parameters
- rename project_root to build_root in Docker image manager
- simplify Task creation by removing redundant constructor
- move repository copying from task execution to task creation
- [**breaking**] make Docker components repository-aware for multi-repo support
- [**breaking**] unify Docker container execution methods and improve interactive mode
- unify debug and task execution pathways
- use Bollard API for Docker image building instead of CLI commands
- consolidate unit tests according to Rust best practices
- [**breaking**] make Task struct fields required and remove Option types
- standardize agent naming to use 'claude-code' consistently
- update CLAUDE.md
- streamline README
- cleanup parts of README that are no longer accurate
- *(task)* remove unused repo_root parameter from write_instructions_content
- *(task)* remove instructions() method from TaskBuilder
- *(task)* separate instructions content from file path in TaskBuilder
- *(client)* integrate TskClient into AppContext dependency injection system
- Move terminal operations to AppContext
- Fix server EOF error when handling empty client connections
- Add agent warmup mechanism for pre-container setup
- Fix client EOF error when server closes connection without response
- Transform TSK into a centralized server-based system
- Refactor log processor to be specific to Claude Code agent
- Improve task duration display with human-readable format
- Add desktop notifications for task completion events
- Associate each task with repository root to support multi-repo operations
- Improve plan template
- Improve editor workflow by editing instruction files from repository root
- Avoid creating branches when no commits exist
- Add more detail to plan task
- Improve debug command to start interactive session directly
- Update templates slightly
- Make todo list display more compact using status emojis
- Refactor TSK to support multiple AI agents through extensible provider pattern
- Add TODO update display in log processor
- Unify task storage by removing separate quick-tasks directory
- Consolidate test MockDockerClient implementations into reusable test utilities
- Simplify the code a bit
- Add a planning task type
- Refactor task management with TaskBuilder pattern and extract TaskStorage
- Refactor task execution into dedicated TaskRunner
- WIP, refactoring task_manager
- WIP removing DockerManagerTrait
- Add ripgrep
- Fix unit tests in task_manager.rs to be thread-safe
- Use multiple threads for unit tests again
- Update Docker resource limits
- Refactor git operations to use git2-rs library
- Add some guidance on tests to CLAUDE.md
- Add a refactor template
- Add GitOperations trait to AppContext for improved testability
- Add FileSystemOperations trait and refactor AppContext to use builder pattern
- Update CLAUDE.md
- Clean up log parser issues
- Introduce AppContext for dependency injection
- Implement command pattern for CLI commands
- Add a doc template
- Fix task directory cleanup in clean subcommand
- Add interactive instruction editing with -e flag for add and quick commands
- Add tasks subcommand for managing task list
- Make --type flag optional and support any template-based task types
- Refactor TSK to always use instructions files and add template support
- Show only message type for non-assistant/result logs
- Improve log streaming output formatting
- Parse task result status from Claude Code JSON logs
- Implement real-time log streaming with JSON formatting for TSK
- TSK automated changes for task: fix-precommit
- Refactor shared task execution logic into TaskManager module
- Add list and run subcommands for task management
- Add git config to docker image
- Add 'add' subcommand for queuing tasks
- TSK automated changes for task: instructions-file
- Make it easier to view output
- TSK debug session changes for: stream-output
- Run squid as squid user
- Add command to stop proxy
- Add nginx proxy container
- Better way to manage rust
- Pass in the description of the command
- Add some convenience just commands and attempt to unblock cargo
- Add an entrypoint script
- Launch images with firewall script
- Add setup for firewall rules
- Fix to properly merge branches
- Switch from using worktrees to copying the repo
- Add .claude directory to version control
- Add claude config file
- allow networking
- Include claude directory in launched containers
- Make sure tests that effect docker and git don't have unintended side effects
- Refactor docker.rs to reuse code and pull out constants
- Add debug subcommand
- Claude Code now works in Docker container
- Add ability to launch Docker containers
- Add base Dockerfile
- Add tests for git commands
- Add basic command line functionality
- Add a CLAUDE file
- Add a README that explains the project
- Initial commit
