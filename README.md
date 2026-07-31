# tsk-tsk: keeping your agents out of trouble

Delegate development `tsk` tasks to YOLO mode AI agents running in sandbox containers. `tsk` auto-detects your toolchain and builds container images for you, so most projects require very little setup. Agents work asynchronously and in parallel so you can review their work on your own schedule, respecting your time and attention.

1. **Assign tasks** using templates that automate prompt boilerplate
2. **`tsk` copies your repo** and builds containers with your toolchain automatically
3. **Agents work in YOLO mode** in parallel filesystem and network isolated containers
4. **`tsk` fetches branches** back to your repo for review
5. **Review and merge** on your own schedule

Each agent gets what it needs and nothing more:
- Agent configuration (e.g. `~/.claude` or `~/.codex`)
- A copy of your repo excluding gitignored files (no accidental API key sharing)
- An isolated filesystem (no accidental `rm -rf .git`)
- A configurable domain allowlist (Agents can't share your code on MoltBook)

Each agent runs in an isolated network where all traffic routes through a proxy sidecar, enforcing the domain allowlist. Beyond network restrictions, agents have full control within their container.

Supports Claude Code and Codex coding agents. Docker and Podman container runtimes.

![tsk demo](./docs/images/tsk-demo.gif)

## Installation

### Requirements

- [Rust](https://rustup.rs/) - Rust toolchain and Cargo
- [Docker](https://docs.docker.com/get-docker/) or [Podman](https://podman.io/) - Container runtime
- [Git](https://git-scm.com/downloads) - Version control system
- One of the supported coding agents:
  - [Claude Code](https://docs.anthropic.com/en/docs/claude-code)
  - [Codex](https://openai.com/codex/)
  - Help us support more!

### Install `tsk`

```bash
# Install using cargo
cargo install tsk-ai
# Or build from source!
gh repo clone dtormoen/tsk-tsk
cd tsk-tsk
cargo install --path .
```

**Claude Code users:** Install `tsk` skills to teach Claude how to use `tsk` commands directly in your conversations and help you configure your projects for use with `tsk`:

```bash
/plugin marketplace add dtormoen/tsk-tsk
/plugin install tsk-help@dtormoen/tsk-tsk
/plugin install tsk-config@dtormoen/tsk-tsk
/plugin install tsk-add@dtormoen/tsk-tsk
```

See [Claude Code Skills Marketplace](#claude-code-skills-marketplace) for more details.

## Quick Start Guide

`tsk` can be used in multiple ways. Here are some of the main workflows to get started. Try testing these in the `tsk` repository!

### Interactive Sandboxes

Start up sandbox with an interactive shell so you can work interactively with a coding agent. This is similar to a git worktrees workflow, but provides stronger isolation. `claude` is the default coding agent, but you can also specify `--agent codex` to use `codex`.

```bash
tsk shell
```

The `tsk shell` command will:
- Make a copy of your repo
- Create a new git branch for you to work on
- Start a proxy to limit internet access
- Build and start a container with your stack (go, python, rust, etc.) and agent (default: claude) installed
- Drop you into an interactive shell

After you exit the interactive shell (ctrl-d or `exit`), `tsk` will save any work you've done as a new branch in your original repo.

This workflow is really powerful when used with terminal multiplexers like `tmux` or `zellij`. It allows you to start multiple agents that are working on completely isolated copies of your repository with no opportunity to interfere with each other or access resources outside of the container.

### One-off Fully Autonomous Agent Sandboxes

`tsk` has flags that help you avoid repetitive instructions like "make sure unit tests pass", "update documentation", or "write a descriptive commit message". Consider this command which immediately kicks off an autonomous agent in a sandbox to implement a new feature:

```bash
tsk run --type feat --name greeting --prompt "Add a greeting to all tsk commands."
```

Some important parts of the command:
- `--type` specifies the type of task the agent is working on. Using `tsk` built-in tasks or writing your own can save a lot of boilerplate. Check out [feat.md](./templates/feat.md) for the `feat` type and [templates](./templates) for all task types.
- `--name` will be used in the final git branch to help you remember what task the branch contains.
- `--prompt` is used to fill in the `{{PROMPT}}` placeholder in [feat.md](./templates/feat.md).

Similar to `tsk shell`, the agent will run in a sandbox so it will not interfere with any ongoing work and will create a new branch in your repository in the background once it is done working.

Add `--branch main` to start from a specific branch's committed state instead of your current working tree. This is useful when you want to launch a task from a different branch without switching to it.

After you try this command out, try out these next steps:
- Add the `--edit` flag to edit the full prompt that is sent to the agent.
- Add a custom task type. Use `tsk template list` to see existing task templates and where you can add your own custom tasks.
  - See the [custom templates used by `tsk`](./.tsk/templates) for inspiration.

### Queuing Tasks for Parallel Execution

The `tsk` server allows you to have a single process that manages parallel task execution so you can easily background agents working. First, we start the server set up to handle up to 4 tasks in parallel:

```bash
tsk server start --workers 4
```

Now, in another terminal window, we can quickly queue up multiple tasks:

```bash
# Add a task. Notice the similarity to the `tsk run` command
tsk add --type doc --name tsk-architecture --prompt "Tell me how tsk works"

# Look at the task queue. Your task `tsk-architecture` should be present in the list
tsk list

# Add another task. Notice the short flag names
tsk add -t feat -n greeting -p "Add a silly robot greeting to every tsk command"

# Now there should be two running tasks
tsk list

# Wait for a specific task to finish (blocks until complete)
tsk wait <taskid>

# Or look at all the branches after tasks complete
git branch --format="%(refname:short) - %(subject) (%(committerdate:relative))"
```

After you try this command out, try these next steps:
- Use `tsk add --wait` to queue a task and block until it completes (useful in scripts and CI)
- Add tasks from multiple repositories in parallel
- Start up multiple agents at once
  - Adding `--agent codex` will use `codex` to perform the task
  - Adding `--agent codex,claude` will have `codex` and `claude` do the task in parallel with the same environment and instructions so you can compare agent performance
  - Adding `--agent claude,claude` will have `claude` do the task twice. This can be useful for exploratory changes to get ideas quickly

### Task Chaining

Chain tasks together with `--parent` so a child task starts from where its parent left off:

```bash
# First task: set up the foundation
tsk add -t feat -n add-api -p "Add a REST API endpoint for users"

# Check the task list to get the task ID
tsk list

# Second task: chain it to the first (replace <taskid> with the parent's ID)
tsk add -t feat -n add-tests -p "Add integration tests for the users API" --parent <taskid>
```

Child tasks wait for their parent to complete, then start from the parent's final commit. `tsk list` shows these tasks as `WAITING`. If a parent fails, its children are automatically marked as `FAILED`; if a parent is cancelled, its children are marked as `CANCELLED`. Chains of any length (A → B → C) are supported.

### Create a Simple Task Template

Let's create a very basic way to automate working on GitHub issues:

```bash
# First create the tsk template configuration directory
mkdir -p ~/.config/tsk/templates

# Create a very simple template. Notice the use of the "{{PROMPT}}" placeholder
cat > ~/.config/tsk/templates/issue-bot.md << 'EOF'
Solve the GitHub issue below. Make sure it is tested and write a descriptive commit
message describing the changes after you are done.

{{PROMPT}}
EOF

# Make sure tsk sees the new `issue-bot` task template
tsk template list

# Pipe in some input to start the task
# Piped input automatically replaces the {{PROMPT}} placeholder
gh issue view <issue-number> | tsk add -t issue-bot -n fix-my-issue
```

Now it's easy to solve GitHub issues with a simple task template. Try this with code reviews as well to easily respond to feedback.

## Commands

### Task Commands

Create, manage, and monitor tasks assigned to AI agents.

- `tsk run` - Execute a task immediately (supports `--branch <branch>` to start from a specific branch's HEAD; Ctrl+C marks task as CANCELLED)
- `tsk shell` - Start a sandbox container with an interactive shell (supports `--branch <branch>` to start from a specific branch's HEAD)
- `tsk add` - Queue a task (supports `--parent <taskid>` for task chaining, `--wait` to block until completion, `--branch <branch>` to start from a specific branch's HEAD)
- `tsk list` - View task status and branches
- `tsk wait <task-id>...` - Block until one or more tasks complete (exits non-zero if any task failed, was cancelled, or is blocked by a warmup error)
- `tsk cancel <task-id>...` - Cancel one or more running or queued tasks
- `tsk clean` - Clean up completed tasks
- `tsk delete <task-id>...` - Delete one or more tasks
- `tsk retry <task-id>...` - Retry one or more tasks

`run`, `shell`, `add`, and `retry` also accept `--tailscale` to join the sandbox to your tailnet (see [Tailscale](#tailscale-optional)).

### Server Commands

Manage the `tsk` server daemon for parallel task execution. The server automatically cleans up completed, failed, and cancelled tasks older than 7 days.

- `tsk server start` - Start the `tsk` server daemon
- `tsk server stop` - Stop the running `tsk` server

Graceful shutdown (via `q`, Ctrl+C, or `tsk server stop`) marks any in-progress tasks as CANCELLED.

When running in an interactive terminal, `tsk server start` shows a TUI dashboard with a split-pane view: task list on the left, log viewer on the right. In the task list, active tasks (Running, Queued, Waiting) appear above completed or failed tasks. The log viewer starts at the bottom of the selected task's output and auto-follows new content. Scrolling up pauses follow mode; scrolling back to the bottom resumes it. When stdout is piped or non-interactive (e.g. `tsk server start | cat`), plain text output is used instead.

**TUI Controls:**
- `Left` / `h`: Focus the task list panel
- `Right` / `l`: Focus the log viewer panel
- `Up` / `k`, `Down` / `j`: Navigate tasks or scroll logs (depends on focused panel)
- `Page Up` / `Page Down`: Jump scroll in log viewer
- Click: Select a task or focus a panel
- Mouse scroll: scroll tasks or logs
- Scrollbar click/drag: Jump or scrub through the task list
- `Shift+click` / `Shift+drag`: Select text (bypasses mouse capture for clipboard use)
- `c`: Cancel the selected task (when task panel is focused, only RUNNING/QUEUED tasks)
- `d`: Delete the selected task (when task panel is focused, only terminal-state tasks)
- `q`: Quit the server (graceful shutdown)

### Configuration Commands

Build container images and manage task templates.

- `tsk docker build` - Build required container images
- `tsk template list` - View available task type templates and where they are installed
- `tsk template show <template>` - Display the contents of a template
- `tsk template edit <template>` - Open a template in your editor for customization

Run `tsk help` or `tsk help <command>` for detailed options.

## Configuring `tsk`

`tsk` has 3 levels of configuration in priority order:
- Project level in the `.tsk` folder local to your project
- User level in `~/.config/tsk`
- Built-in configurations

Each configuration directory can contain:
- `templates`: A folder of task template markdown files which can be used via the `-t/--type` flag

### Configuration File

`tsk` can be configured at two levels:

1. **User-level**: `~/.config/tsk/tsk.toml` — global settings, defaults, and per-project overrides
2. **Project-level**: `.tsk/tsk.toml` in your project root — shared project defaults (checked into version control)

Both levels use the same shared config shape. The project-level config only contains shared settings (no `container_engine`, `[server]`, or `[project.<name>]` sections).

**User-level config** (`~/.config/tsk/tsk.toml`):

```toml
# Container engine (top-level setting, user-only)
container_engine = "docker"  # "docker" (default) or "podman"

# Server daemon configuration (user-only)
[server]
auto_clean_enabled = true   # Automatically clean old tasks (default: true)
auto_clean_age_days = 7.0   # Minimum age in days before cleanup (default: 7.0)

# Default settings for all projects (showing built-in defaults)
[defaults]
agent = "claude"             # AI agent: "claude" or "codex"
stack = "default"            # Tech stack (auto-detected from project files)
memory_gb = 12.0             # Container memory limit in GB
cpu = 8                      # Number of CPUs
dind = false                 # Enable Docker-in-Docker support
privileged = false           # Run containers in privileged mode (disables security restrictions)
sudo = false                 # Enable passwordless sudo inside containers
devices = []                 # Device paths to expose (e.g., ["/dev/video0"])
git_town = false             # Enable git-town parent branch tracking
tailscale = false            # Join sandboxes to your Tailscale tailnet (see below)

# Project-specific overrides (matches directory name)
[project.my-go-service]
stack = "go"
memory_gb = 24.0
cpu = 16
setup = '''
USER root
RUN apt-get update && apt-get install -y libssl-dev pkg-config
USER agent
'''
host_ports = [5432, 6379]    # Forward host ports to containers
volumes = [
    # Bind mount: share host directories with containers (supports ~ expansion)
    { host = "~/.cache/go-mod", container = "/go/pkg/mod" },
    # Named volume: container-managed persistent storage (prefixed with tsk-)
    { name = "go-build-cache", container = "/home/agent/.cache/go-build" },
    # Read-only mount: provide artifacts without modification risk
    { host = "~/debug-logs", container = "/debug-logs", readonly = true }
]
env = [
    { name = "DB_PORT", value = "5432" },
    { name = "REDIS_PORT", value = "6379" },
]
```

**Project-level config** (`.tsk/tsk.toml` in project root):

```toml
# Project defaults shared via version control
stack = "rust"
memory_gb = 16.0
host_ports = [5432]
setup = '''
USER root
RUN apt-get update && apt-get install -y cmake
USER agent
'''

[stack_config.rust]
setup = '''
RUN cargo install cargo-nextest
'''
```

The `setup` field injects Dockerfile commands at the project layer position in the Docker image build. Use it for project-specific build dependencies. `stack_config.<name>.setup` and `agent_config.<name>.setup` similarly inject content at the stack and agent layer positions, and can define entirely new stacks or agents (e.g., `stack_config.scala.setup` lets you use `stack = "scala"`). Config-defined layers take priority over embedded Docker layers. Setup commands run as the `agent` user by default — use `USER root` for operations that require elevated privileges (e.g., `apt-get install`) and always switch back to `USER agent` afterwards.

Volume mounts are particularly useful for:
- **Build caches**: Share Go module cache (`/go/pkg/mod`) or Rust target directories to speed up builds
- **Persistent state**: Use named volumes for build caches that persist across tasks
- **Read-only artifacts**: Mount debugging artifacts, config files, or other resources without risk of modification

Environment variables (`env`) let you pass configuration to task containers, such as database URLs or API keys. To connect to host services forwarded through the proxy, use the `TSK_PROXY_HOST` environment variable (set automatically by `tsk`) as the hostname.

The container engine can also be set per-command with the `--container-engine` flag (available on `run`, `shell`, `retry`, `cancel`, `server start`, and `docker build`).

Host ports (`host_ports`) expose host services to task containers. Agents connect to `$TSK_PROXY_HOST:<port>` to reach services running on your host machine (e.g., local databases or dev servers). The `TSK_PROXY_HOST` environment variable is automatically set by `tsk` to the correct proxy container hostname.

When `git_town` is enabled, `tsk` integrates with [git-town](https://www.git-town.com/) by setting the parent branch metadata on task branches, allowing git-town commands like `git town sync` to work correctly with `tsk`-created branches.

Configuration priority: CLI flags > user `[project.<name>]` > project `.tsk/tsk.toml` > user `[defaults]` > auto-detection > built-in defaults

Settings in `[defaults]`, `[project.<name>]`, and `.tsk/tsk.toml` share the same shape. Scalars use first-set in priority order. Lists (`volumes`, `env`, `host_ports`) combine across layers, with higher-priority winning on conflicts (same container path, same env var name, same port). `stack_config`/`agent_config` maps combine all names; for the same name, higher-priority replaces the entire config.

### Customizing the `tsk` Sandbox Environment

Each `tsk` sandbox container image has 4 main parts:
- A [base dockerfile](./dockerfiles/base/default.dockerfile) that includes the OS and a set of basic development tools e.g. `git`, `git-lfs`
- A `stack` snippet that defines language specific build steps. See:
  - [default](./dockerfiles/stack/default.dockerfile) - minimal fallback stack
  - [go](./dockerfiles/stack/go.dockerfile)
  - [java](./dockerfiles/stack/java.dockerfile)
  - [lua](./dockerfiles/stack/lua.dockerfile)
  - [node](./dockerfiles/stack/node.dockerfile)
  - [python](./dockerfiles/stack/python.dockerfile)
  - [rust](./dockerfiles/stack/rust.dockerfile)
- An `agent` snippet that installs an agent, e.g. `claude` or `codex`.
- A `project` snippet that defines project specific build steps (applied last for project-specific customizations). This does nothing by default, but can be used to add extra build steps for your project.

It is very difficult to make these images general purpose enough to cover all repositories. You may need some special customization. If you use Claude Code, the `tsk-config` skill can walk you through configuring `tsk`'s Docker layers for your project (see [Claude Code Skills Marketplace](#claude-code-skills-marketplace) for installation). Otherwise, the recommended approach is to use `setup`, `stack_config`, and `agent_config` fields in your `tsk.toml` to inject custom Dockerfile commands (see [Configuration File](#configuration-file) above).

See [dockerfiles](./dockerfiles) for the built-in dockerfiles.

You can run `tsk docker build --dry-run` to see the dockerfile that `tsk` will dynamically generate for your repository.

See the [Docker Builds Guide](docs/docker-builds.md) for a more in-depth walk through, and the [Network Isolation Guide](docs/network-isolation.md) for details on how `tsk` secures agent network access.

I'm working on improving this part of `tsk` to be as seamless and easy to set up as possible, but it's still a work in progress. I welcome all feedback on how to make this easier and more intuitive!

### Creating Templates

Templates are simply markdown files that get passed to agents. `tsk` additionally adds a convenience `{{PROMPT}}` placeholder that will get replaced by anything you pipe into tsk or pass in via the `-p/--prompt` flag, or by using `--prompt-file <path>` to read from a file. The legacy `{{DESCRIPTION}}` placeholder is still supported but deprecated.

To inspect an existing template, run `tsk template show <template>`. To customize a built-in template, run `tsk template edit <template>` — `tsk` will copy it to `~/.config/tsk/templates/` and open it in your `$EDITOR`.

To create good templates, I would recommend thinking about repetitive tasks that you need agents to do within your codebase like "make sure the unit tests pass", "write a commit message", etc. and encode those in a template file. There are many great prompting guides out there so I'll spare the details here.

### Custom Proxy Configuration

`tsk` uses Squid as a forward proxy to control network access from task containers. You can customize the proxy configuration to allow access to specific services or URLs needed by your project.

**Inline configuration** in tsk.toml (recommended):
```toml
[defaults]
squid_conf = '''
http_port 3128
acl allowed_domains dstdomain .example.com .myapi.dev
http_access allow allowed_domains
http_access deny all
'''
```

**File-based configuration** (path reference):
```toml
[defaults]
squid_conf_path = "~/.config/tsk/squid.conf"

# Or in project-level .tsk/tsk.toml (path relative to project root):
# squid_conf_path = ".tsk/squid.conf"
```

Inline `squid_conf` takes priority over `squid_conf_path`. See the default [`tsk` squid.conf](./dockerfiles/tsk-proxy/squid.conf) as a starting point.

**Per-configuration proxy instances:** Tasks with different proxy configurations (different `host_ports` or `squid_conf`) automatically get separate proxy containers. Tasks with identical proxy config share the same proxy. Proxy containers are named `tsk-proxy-{fingerprint}` where the fingerprint is derived from the proxy configuration.

### Tailscale (optional)

By default a sandbox reaches the internet only through the Squid allowlist. Enable Tailscale when an agent also needs to reach private services on your tailnet (an internal API, a database, a staging box).

> **Read this first — enabling Tailscale changes the trust boundary.** With Tailscale off, a sandbox's egress is bounded by the Squid allowlist. With it **on**, the sandbox can reach anything your **Tailscale ACLs** permit its node to reach, over the tailnet, *without* passing through Squid. The Squid allowlist still governs all non-tailnet traffic, but it no longer governs the tailnet. The sandboxed agent is untrusted and holds `NET_ADMIN`, so treat **your Tailscale ACLs + the auth key's tags as the real security boundary** — not tsk's flags. Scope the key (below) accordingly.

Enable it per command with `--tailscale` (available on `run`, `shell`, `add`, and `retry`) or in `tsk.toml`:

```toml
[defaults]
tailscale = false                          # Off by default

[project.my-service]
tailscale = true
tailscale_auth_key_env = "TS_AUTHKEY"      # Env var holding the auth key (default)
# tailscale_auth_key_file = "~/.config/tsk/ts-authkey"  # Or read it from a file
# tailscale_hostname = "my-service-sandbox"             # Default: tsk-<task-id>
# tailscale_accept_routes = true            # Reach subnet-router routes (default false; see below)
# tailscale_host_aliases = false            # Inject tailnet device names into /etc/hosts (default true)
# tailscale_up_args = "--ssh"               # Extra `tailscale up` flags (isolation-weakening flags rejected)

# --- Recommended: let tsk mint a fresh ephemeral, tagged key per task ---
# Provide EITHER a PAT or an OAuth client (OAuth takes precedence if both set).
# tailscale_api_key_env = "TS_API_KEY"            # env var holding a PAT (tskey-api-...)
# tailscale_api_key_file = "~/.config/tsk/ts-api-key"
# tailscale_oauth_client_id  = "k123..."          # OAuth client id
# tailscale_oauth_secret_env = "TS_OAUTH_SECRET"  # env var holding the OAuth secret
# tailscale_oauth_secret_file = "~/.config/tsk/ts-oauth-secret"
# tailscale_tailnet = "-"                          # default "-" = credential's default tailnet
# tailscale_tags = ["tag:tsk-sandbox"]             # tags for minted keys/nodes (default)
```

#### Recommended auth-key setup (least privilege)

The config example above shows the recommended setup: configure a Tailscale API credential and let tsk mint a fresh key per task (see "Cleanup of sandbox nodes" below). If you'd rather not grant tsk API access, you can instead bring your own key.

In the Tailscale admin console, generate a **reusable, ephemeral, tagged** auth key (e.g. `tag:tsk-sandbox`) and write an ACL granting that tag access to only the specific hosts/ports an agent should reach. You mint this **once** and reuse it across every task — it is the primary control:

- **Reusable** → one key authenticates every sandbox; you don't mint a key per task.
- **Ephemeral** → each sandbox node auto-removes itself a few minutes after the task ends (no stale `tsk-*` nodes piling up on your tailnet). Letting tsk mint the key per task (above) is the more robust way to guarantee this, since a manually-created key only stays ephemeral if you remember to set that flag every time you regenerate it.
- **Tagged + ACL-scoped** → the sandbox is limited to what the tag is allowed, *independently of your personal identity* — the tag is what does the scoping. An **untagged** key (reusable or not) gives every sandbox **your full personal tailnet access**, so always tag it.

Set a sensible expiry on the key and rotate it like any other secret. (The key lives on the trusted host, not in the sandbox — see below.)

Supply the key from your environment (or a key file) — never commit one:

```bash
export TS_AUTHKEY="tskey-auth-..."
tsk run --tailscale -t feat -n sync-schema -p "Sync the schema from the staging database"
```

**Cleanup of sandbox nodes.** Each sandbox joins as a `tsk-<task-id>` node. To keep the admin console from filling up with old nodes, the node must be **ephemeral** — Tailscale then auto-removes it a few minutes after the sandbox stops. The most robust way is to let tsk **mint the key**: configure a Tailscale API credential (a one-click **personal access token**, or an **OAuth client** for non-expiring, tightly-scoped access, via `tailscale_api_key_env` / `tailscale_oauth_client_id`) and tsk creates a fresh single-use, ephemeral, tagged key per task — so nodes are always tagged (`tag:tsk-sandbox` by default) and always auto-remove, with no way to misconfigure them. There's no default env var for the mint credential — minting only kicks in once you configure one of the fields above. If you instead bring your own key (`TS_AUTHKEY` / `tailscale_auth_key_file`), you are responsible for making it **ephemeral and tagged**; a non-ephemeral or untagged key leaves nodes behind and attributes them to your personal identity (the sandbox prints a warning when it detects an untagged node). A manually-generated reusable key remains a practical fallback, but minting is the robust, hands-off option.

#### Scope the sandbox tag with a least-privilege ACL

The sandboxed agent is untrusted and holds `NET_ADMIN`, so **your Tailscale ACL and the `tag:tsk-sandbox` tag are the real security boundary — not tsk's flags.** Before your first Tailscale task, define the tag and scope it tightly.

Tailscale ACLs are **default-deny**: a freshly-tagged `tag:tsk-sandbox` node can reach *nothing* on your tailnet until a rule grants it. The failure mode to guard against is the opposite — an existing **broad** rule (`src: ["*"]`, or an `autogroup` that covers the tag) silently handing every sandbox your whole tailnet. So the job is: grant the tag only the few hosts/ports an agent needs, and make sure no wildcard rule already covers it.

A minimal, default-deny policy (HuJSON, Tailscale admin console → Access Controls):

```jsonc
{
  "tagOwners": {
    // Who may mint/assign keys for the sandbox tag. A PAT's user must be an
    // owner to mint with a PAT; an OAuth client is granted this tag when you
    // create it, and the tag must be defined here either way.
    "tag:tsk-sandbox": ["autogroup:admin"],
  },

  "acls": [
    // Grant the sandbox ONLY what agents actually need — enumerate dst
    // host:port explicitly. Everything else is denied by default.
    {
      "action": "accept",
      "src":    ["tag:tsk-sandbox"],
      "dst":    ["tag:staging-db:5432", "tag:internal-api:443"],
    },

    // ...your other rules...
    // ⚠️  Audit every broad rule: a line like
    //     { "action": "accept", "src": ["*"], "dst": ["*:*"] }
    // (or any autogroup that includes tagged nodes) would give every sandbox
    // your entire tailnet. Exclude tag:tsk-sandbox from such rules.
  ],

  // Do NOT grant the sandbox tag SSH access unless you specifically intend to.
  "ssh": [],
}
```

**Best-practices checklist:**

- **Always tag, never personal.** Use `tag:tsk-sandbox` (or your own tag via `tailscale_tags`); an untagged key gives the sandbox your full personal tailnet access.
- **Default-deny, enumerate up.** Grant the tag only specific `dst` `host:port`s. Don't reuse a broad "allow internal" rule for it.
- **Prefer minting.** A minted key is ephemeral, single-use, and always tagged, so nodes auto-remove and can't be misconfigured. For bring-your-own keys, set an **ephemeral, tagged** key with a sensible expiry and rotate it.
- **Scope the mint credential.** Give the **OAuth client** only the `auth_keys` scope plus ownership of `tag:tsk-sandbox`; give a **PAT** a short expiry and rotate it. The credential stays on the trusted host, never in the sandbox.
- **Keep routes off.** Leave `tailscale_accept_routes` at its default `false` unless a task genuinely needs a whole subnet — accepted routes bypass the Squid allowlist.
- **No SSH/exit-node for the tag.** tsk already rejects exit-node/route-advertising flags; don't re-add reachability through your ACL.

**Quick start:**

1. Add `tag:tsk-sandbox` to `tagOwners` and a narrow `acls` grant (above).
2. Create a mint credential: a **PAT** (admin console → *Settings → Keys → Generate access token*) or an **OAuth client** scoped to `auth_keys` + `tag:tsk-sandbox`.
3. Expose it and point tsk at it — `export TS_API_KEY=tskey-api-...` with `tailscale_api_key_env = "TS_API_KEY"` (or the `tailscale_oauth_*` fields).
4. Run a task: `tsk run --tailscale -t feat -n sync-schema -p "Sync the staging schema"`.

How it works:

- `tsk` installs `tailscale`/`tailscaled` in the sandbox image and starts them before the agent runs. If the tailnet cannot be joined within 60s the task fails rather than running without access.
- The auth key is read at container start from `$TS_AUTHKEY` (or the configured env var), falling back to `tailscale_auth_key_file`. It is passed to the container to join the tailnet, then the wrapper `unset`s it and `exec`s the agent so the **in-container agent** can't recover it from `/proc/<pid>/environ`, and it is never written to the task database or image. It does remain in the container's `Config.Env` — readable by anyone who can `docker inspect` the container **on the host** — for the container's lifetime, so treat host access as trusted and scope the key with a tag + expiry (see above).
- Each sandbox joins as its own node named `tsk-<task-id>` (override with `tailscale_hostname`; names are sanitized to a DNS-safe form).
- **Subnet routes are opt-in.** `tailscale_accept_routes` defaults to `false`, so the sandbox reaches only tailnet **nodes**, not the subnets a subnet-router advertises. Set it to `true` only when you intend the sandbox to reach a whole advertised subnet — those become reachable **over the tailnet, bypassing Squid**.
- **Exit nodes / route advertisement are not supported for sandboxes.** `tailscale_up_args` is appended to `tailscale up`, but isolation-weakening flags (`--exit-node`, `--advertise-exit-node`, `--advertise-routes`, `--accept-routes`, `--accept-dns`, `--netfilter-mode`) are **rejected at task creation** — they would route the sandbox's traffic around the allowlist.
- The proxy allowlist is extended with Tailscale's control plane and relays (`.tailscale.com`, `.tailscale.io`) so `tailscaled` can connect; tailnet traffic (`100.64.0.0/10`, `*.ts.net`) bypasses the proxy. Tailscale tasks get their own proxy container because their proxy configuration differs.
- Containers keep their usual hardening, except that `NET_ADMIN` is granted so `tailscaled` can set up its interface and routes.
- **Kernel vs userspace mode.** On **Linux + Docker with a host `/dev/net/tun`** the container gets a real TUN device, so tailnet IPs are **transparently reachable** (`curl 100.x.y.z` just works; the tailnet is in `NO_PROXY`). (Kernel mode needs a Linux Docker host — Docker Desktop on macOS has no host `/dev/net/tun`, so use Podman there.) On **rootless Podman** (including macOS) a usable TUN device isn't available, so `tailscaled` runs in **userspace mode**. There tsk sets `ALL_PROXY=socks5h://localhost:1055` (the SOCKS5 proxy `tailscaled` exposes) and keeps the tailnet **out** of `NO_PROXY`, so tools that honor `ALL_PROXY` reach the tailnet while HTTP(S) to the internet still goes through Squid. **One caveat:** because internet HTTP(S) must keep flowing through Squid, `HTTP_PROXY` takes precedence over `ALL_PROXY` for `http(s)://` URLs, so a plain `curl http://rainier` is *not* transparently routed in userspace mode — reach tailnet HTTP services with an explicit `curl --socks5-hostname localhost:1055 http://rainier`. Non-HTTP tailnet services (SSH, Postgres, Redis, …) are reached transparently via `ALL_PROXY`. On Docker/Linux (kernel mode) everything, HTTP included, is transparent. The container log prints `using userspace networking` in this mode.
- **Reaching hosts by name.** tsk snapshots your host's `tailscale status` at task creation and injects each tailnet device's name→IP (short label + FQDN) into the container's `/etc/hosts` via `--add-host` (no in-container privilege needed); in userspace mode `tailscaled` resolves peer names too. So `ssh rainier`, `psql -h db …`, and other **non-HTTP** services work by short name. **HTTP(S) is the exception:** proxy bypass is decided on the *hostname string*, and a short label like `rainier` matches neither `.ts.net` nor the tailnet CIDRs, so `curl http://rainier` gets sent to Squid and denied — for HTTP, use the **FQDN** (`curl http://rainier.<tailnet>.ts.net`, which matches `.ts.net`) or the `100.x` IP, or the explicit `--socks5-hostname` form in userspace mode. This is **device names only**, not full MagicDNS: split-DNS/custom domains, search domains, hosts *behind* a subnet router, and devices that join mid-task are **not** covered, and it's a point-in-time snapshot (tailnet IPs are stable, so that's rarely an issue). Set `tailscale_host_aliases = false` to keep the sandbox from learning your device names. Full MagicDNS isn't possible because the non-root agent can't manage `/etc/resolv.conf`.
- `--tailscale` **requires network isolation** to be on (it depends on the isolated-network topology); enabling it with `--no-network-isolation` is rejected at task creation.
- **Rootless Podman note:** the iptables defense-in-depth layer (in the proxy container) is Docker-only. Under rootless Podman a Tailscale sandbox relies solely on the Squid allowlist and your Tailscale ACLs — there is no netfilter backstop.
- **Self-hosted control planes (Headscale) aren't supported** yet: the proxy allowlist only opens Tailscale's SaaS control/relay domains (`.tailscale.com`/`.tailscale.io`), so a custom `--login-server` can't be reached. Exit-node egress is also intentionally unsupported (blocked in `tailscale_up_args`).

**Troubleshooting:** if a task fails at join, the log shows `tsk: tailscaled failed to start:` followed by `/tmp/tailscaled.log`. A missing/expired key surfaces as a `tailscale up` error; a control-plane it can't reach surfaces as the 60s timeout.

## `tsk` Data Directory

`tsk` uses the following directories for storing data while running tasks:
- **~/.local/share/tsk/tasks.db**: SQLite database for task queue and task definitions
- **~/.local/share/tsk/tasks/**: Task directories that get mounted into sandboxes when the agent runs. They contain:
  - **<taskid>/repo**: The repo copy that the agent operates on
  - **<taskid>/output**: Directory containing `agent.log` with structured JSON-lines output including infrastructure phases (image build, agent launch, saving changes, branch result) and processed agent output
  - **<taskid>/instructions.md**: The instructions that were passed to an agent

These default paths follow XDG conventions. You can override them with `tsk`-specific environment variables without affecting other XDG-aware software. Like XDG variables, these specify the base directory; `tsk` appends `/tsk` automatically:
- `TSK_DATA_HOME` - overrides `XDG_DATA_HOME` for `tsk` (default: `~/.local/share`)
- `TSK_RUNTIME_DIR` - overrides `XDG_RUNTIME_DIR` for `tsk` (default: `/tmp`)
- `TSK_CONFIG_HOME` - overrides `XDG_CONFIG_HOME` for `tsk` (default: `~/.config`)

## Claude Code Skills Marketplace

This repository includes a Claude Code skills marketplace with `tsk`-specific skills that teach Claude how to use `tsk` commands. To install:

```bash
# Add the marketplace in Claude Code
/plugin marketplace add dtormoen/tsk-tsk

# Install a skill (e.g. tsk-help, tsk-config, tsk-add)
/plugin install tsk-help@dtormoen/tsk-tsk
```

Skills follow the [Agent Skills](https://agentskills.io) open standard. See the [Skills Marketplace Guide](docs/skill-marketplace.md) for details on available skills, manual installation, and contributing new skills.

## Contributing

This project uses:
- `cargo test` for running tests
- `just precommit` for full CI checks
- `just integration-test` for stack layer integration tests (requires Docker/Podman)
- See [CLAUDE.md](CLAUDE.md) for development guidelines

## License

MIT License - see [LICENSE](LICENSE) file for details.
