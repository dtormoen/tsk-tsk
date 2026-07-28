# Docker Builds in `tsk`

`tsk` uses a sophisticated Docker-based execution environment to run AI agents in isolated, secure containers. This document explains how the Docker build system works, how to customize it, and how to debug common issues.

## Table of Contents

- [Overview](#overview)
- [The Four-Layer Architecture](#the-four-layer-architecture)
- [Docker Image Naming](#docker-image-naming)
- [Customizing Docker Images](#customizing-docker-images)
- [Building Docker Images](#building-docker-images)
- [Tech Stack Auto-Detection](#tech-stack-auto-detection)
- [Security and Isolation](#security-and-isolation)
- [Debugging Docker Issues](#debugging-docker-issues)
- [Common Patterns and Examples](#common-patterns-and-examples)

## Overview

`tsk`'s Docker infrastructure provides:
- **Layered architecture** for efficient image building and caching
- **Automatic tech stack detection** based on repository files
- **Multiple customization points** at project and user levels
- **Security-first design** with non-root execution and network isolation
- **Intelligent fallback** mechanisms for missing configurations

## The Four-Layer Architecture

`tsk` composes Docker images from four distinct layers, each serving a specific purpose. All base dockerfiles are embedded as assets within the `tsk` binary and are automatically extracted when needed, ensuring `tsk` works out-of-the-box without requiring separate configuration files.

### 1. Base Layer
The foundation of all `tsk` containers (`base/default.dockerfile`):
- Ubuntu 25.10 base operating system
- Essential development tools (git, git-lfs, curl, build-essential, ripgrep, etc.)
- Non-root `agent` user (created by renaming the default `ubuntu` user) for security
- Git configuration inherited from host user via build arguments
- Build-time working directory set to `/workspace` (at runtime, `/workspace/{project_name}`)
- Contains placeholders (`{{{STACK}}}`, `{{{PROJECT}}}`, `{{{AGENT}}}`) for layer composition
- Config-driven flags (e.g., `sudo = true`, `tailscale = true`) may inject additional Dockerfile content between layers at build time. Tailscale injects `features/tailscale.dockerfile`, which installs `tailscale`/`tailscaled` and the startup script that joins the tailnet.

### 2. Stack Layer
Language-specific toolchains and runtimes:
- **default**: Minimal additions, used as fallback
- **rust**: Rust toolchain via rustup with Cargo and Just
- **python**: Python 3 with uv package manager, pytest, black, ruff, mypy, and poetry
- **node**: Node.js LTS with npm, pnpm, yarn, TypeScript, ESLint, and Jest
- **go**: Go 1.25.0 with gopls, delve debugger, goimports, and staticcheck
- **java**: OpenJDK 17 with Maven, Gradle, Kotlin, Groovy, and SDKMAN
- **lua**: Neovim with LuaJIT, Luarocks, luacheck, busted, and stylua

### 3. Agent Layer
AI agent installations and configurations:
- **claude**: Claude Code CLI (installed via npm with Node.js 20.x)
- **codex**: Codex CLI (installed via npm with Node.js 20.x)
- **no-op**: Testing/debugging agent (displays instructions without executing)

Agent versions are automatically detected and tracked via the `TSK_AGENT_VERSION` build argument, triggering image rebuilds when agents are upgraded on the host system.

### 4. Project Layer
Project-specific dependencies and optimizations:
- **default**: Empty layer, used as fallback
- **Custom layers**: Created per-project for dependency caching

## Docker Image Naming

`tsk` uses a hierarchical naming convention for Docker images:

```
tsk/{stack}/{agent}/{project}
```

For example:
- `tsk/rust/claude/my-project`
- `tsk/python/claude/default`
- `tsk/node/codex/web-app`

If a specific project layer doesn't exist, `tsk` automatically falls back to the `default` project layer.

## Customizing Docker Images

### Project-Level Customization

Customize Docker images via your project's `.tsk/tsk.toml`:

```toml
# Project-specific build steps (injected at the project layer position)
setup = '''
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src
'''
```

Example for a Node.js project:

```toml
setup = '''
COPY package.json package-lock.json ./
RUN npm ci
'''
```

### User-Level Customization

Add personal preferences in `~/.config/tsk/tsk.toml`:

```toml
# Default settings applied to all projects
[defaults]
setup = '''
RUN apt-get update && apt-get install -y postgresql-client \
    && rm -rf /var/lib/apt/lists/*
'''

# Override or define stack layers
[defaults.stack_config.python]
setup = '''
RUN uv pip install --system ipython
'''

# Per-project overrides
[project.my-project]
setup = '''
COPY requirements.txt ./
RUN uv pip install --system -r requirements.txt
'''
```

User-level customizations are useful for:
- Personal tool preferences
- Custom shell configurations
- Additional development utilities
- Alternative package sources or mirrors

### Configuration Priority

`tsk` resolves Docker layer configuration in this order:
1. **CLI flags**: Command-line arguments take highest priority
2. **User project overrides**: `[project.<name>]` in `~/.config/tsk/tsk.toml`
3. **Project config**: `.tsk/tsk.toml` in the repository
4. **User defaults**: `[defaults]` in `~/.config/tsk/tsk.toml`
5. **Embedded**: Built into the `tsk` binary

Config-defined layers (`setup`, `stack_config`, `agent_config`) take priority over embedded assets.

## Building Docker Images

### Manual Building

Use the `docker build` command to manually build images:

```bash
# Build using resolved settings
tsk docker build

# Specify stack explicitly
tsk docker build --stack rust

# Build for a specific agent
tsk docker build --agent codex

# Build for a specific project
tsk docker build --project my-app

# Build without cache
tsk docker build --no-cache

# Preview the composed Dockerfile
tsk docker build --dry-run
```

### Automatic Building

`tsk` automatically builds the required image when:
- Running a task (`tsk run`)
- Starting an interactive shell (`tsk shell`)
- Adding tasks to the queue (`tsk add`)

## Tech Stack Auto-Detection

`tsk` automatically detects your project's technology stack based on repository files:

| Stack | Detection Files |
|-------|-----------------|
| Rust | `Cargo.toml` |
| Python | `pyproject.toml`, `requirements.txt`, `setup.py` |
| Node.js | `package.json` |
| Go | `go.mod` |
| Java | `pom.xml`, `build.gradle`, `build.gradle.kts` |
| Lua | files ending in `rockspec`, `.luacheckrc`, `init.lua` |
| Default | Used when no specific files found |

Auto-detection is used as a fallback when neither the `--stack` flag nor any config layer (`[project.<name>]`, `.tsk/tsk.toml`, `[defaults]`) specifies a stack.

## Security and Isolation

`tsk` implements multiple security layers:

### Non-Root Execution
- Containers run as the `agent` user (created by renaming the default `ubuntu` user)
- No sudo access by default (enable with `--sudo` flag or `sudo = true` in config)
- Limited filesystem permissions

### Network Isolation
- Each agent runs in a dedicated internal Docker network (`tsk-agent-{task-id}`)
- Internal networks have no external gateway - agents cannot route to the internet directly
- The `tsk-proxy-{fingerprint}` container bridges agent networks to the outside world (per-config proxy instances)
- Proxy build is skipped if proxy is already running (config changes picked up when proxy stops and restarts)
- Proxy automatically stops when no agent containers are connected
- Squid proxy enforces a domain allowlist for AI APIs and package registries
- See [Network Isolation](network-isolation.md) for full details

### Resource Limits and Container Configuration
- Memory limited to 12GB
- CPU quota of 8 CPUs (800000 microseconds)
- Automatic cleanup of stopped containers
- Task containers named: `tsk-{task-id}` (e.g., `tsk-abc123def4`)
- Interactive containers named: `tsk-interactive-{task-id}`

### Volume Mounts
Each container has the following volumes mounted:
- Repository copy: `{repo_path}:/workspace/{project_name}` (read-write)
- Agent configuration: Agent-specific (e.g., `~/.claude:/home/agent/.claude` for Claude)
- Instructions: `{instructions_dir}:/instructions:ro` (read-only)
- Output: `{task_dir}/output:/output` (read-write)

### Capability Dropping
Containers run with minimal Linux capabilities, dropping up to 9 capabilities:
- `NET_ADMIN` - Can't manage network interfaces
- `NET_RAW` - Can't create raw sockets (only when network isolation is enabled)
- `SETPCAP` - Can't change capability sets
- `SYS_ADMIN` - Can't mount filesystems or perform namespace operations
- `SYS_PTRACE` - Can't trace processes
- `DAC_OVERRIDE` - Can't bypass file read/write/execute permissions
- `AUDIT_WRITE` - Can't write audit logs
- `SETUID` - Can't change user IDs (kept when `--dind` or `--sudo` is active)
- `SETGID` - Can't change group IDs (kept when `--dind` or `--sudo` is active)

## Debugging Docker Issues

### Common Issues and Solutions

#### 1. Git Configuration Error
**Error**: "Git user.name is not set"

**Solution**: Git configuration is automatically inherited from your host system. Configure git on your host machine:
```bash
git config --global user.name "Your Name"
git config --global user.email "your@email.com"
```

#### 2. Missing Docker Layer
**Error**: "Stack layer 'xyz' not found"

**Solution**: Check available layers and configuration:
```bash
# Preview the composed Dockerfile to see what layers are resolved
tsk docker build --dry-run

# Embedded stacks (built into tsk): rust, python, node, go, java, lua, default
# You can define custom stacks via stack_config in tsk.toml:
# [defaults.stack_config.xyz]
# setup = '''
# RUN apt-get update && apt-get install -y ...
# '''
```

#### 3. Build Failures
**Error**: Build errors during image creation

**Solutions**:
- Use `--dry-run` to inspect the composed Dockerfile
- Check Docker daemon logs
- Ensure Docker has sufficient disk space
- Try building with `--no-cache`

#### 4. Container Startup Issues
**Error**: Container fails to start or exits immediately

**Solutions**:
- Use `tsk shell` to get an interactive shell for debugging
- Check container logs: `docker logs <container-id>`
- Verify volume mounts and permissions

### Debugging Commands

```bash
# Interactive debugging shell
tsk shell

# View composed Dockerfile
tsk docker build --dry-run

# Check Docker images
docker images | grep tsk

# Inspect running containers
docker ps --filter "label=tsk"

# View proxy logs (replace {fingerprint} with actual value from docker ps)
docker logs tsk-proxy-{fingerprint}

```

## Common Patterns and Examples

### Caching Python Dependencies

In `.tsk/tsk.toml`:
```toml
setup = '''
# Install Python dependencies using uv (recommended)
COPY requirements.txt ./
RUN uv pip install --system -r requirements.txt
'''
```

Alternative patterns:
```toml
setup = '''
# For projects using Poetry
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-dev
'''
```

### Pre-compiling Java Dependencies

In `.tsk/tsk.toml`:
```toml
setup = '''
# For Maven projects
COPY pom.xml ./
RUN mvn dependency:go-offline
'''
```

### Installing Additional Tools

In `~/.config/tsk/tsk.toml`, add tools to a stack layer:
```toml
[defaults.stack_config.python]
setup = '''
# Add tools on top of the embedded Python stack
RUN uv pip install --system ipython

# System packages
RUN apt-get update && apt-get install -y postgresql-client \
    && rm -rf /var/lib/apt/lists/*
'''
```

### Custom Environment Variables

In `.tsk/tsk.toml` or `~/.config/tsk/tsk.toml`:
```toml
env = [
    { name = "DATABASE_URL", value = "postgresql://localhost/myapp_dev" },
    { name = "REDIS_URL", value = "redis://localhost:6379" },
    { name = "NODE_ENV", value = "development" },
]
```

## Best Practices

1. **Use Project Layers for Dependencies**: Cache project dependencies in custom project layers to speed up builds.

2. **Keep Layers Focused**: Each layer should have a single responsibility. Don't mix tech-stack setup with project dependencies.

3. **Minimize Layer Size**: Remove package manager caches and temporary files:
   ```dockerfile
   RUN apt-get update && apt-get install -y package \
       && rm -rf /var/lib/apt/lists/*
   ```

4. **Version Lock Dependencies**: Use lock files (`Cargo.lock`, `package-lock.json`, etc.) for reproducible builds.

5. **Test Locally**: Use `tsk shell` to test your custom layers interactively before running tasks.

6. **Document Custom Layers**: Add comments in your `setup` fields explaining why customizations are needed.

## Troubleshooting Checklist

When encountering issues:

1. ✓ Verify Docker daemon is running
2. ✓ Check available disk space
3. ✓ Ensure git is configured properly
4. ✓ Verify file permissions in `.tsk/` directory
5. ✓ Use `--dry-run` to inspect Dockerfile composition
6. ✓ Try building with `--no-cache`
7. ✓ Check `tsk` logs with `RUST_LOG=debug tsk <command>`
8. ✓ Test with `tsk shell` for interactive debugging

## Further Resources

- [`tsk` README](../README.md) - General `tsk` documentation
- [CLAUDE.md](../CLAUDE.md) - Project conventions and development guide
- Docker documentation - For advanced Docker concepts
- `tsk` GitHub Issues - For reporting bugs or requesting features
