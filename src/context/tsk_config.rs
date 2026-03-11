//! User configuration loaded from tsk.toml
//!
//! This module contains configuration types that are loaded from the user's
//! configuration file (`~/.config/tsk/tsk.toml`). These options allow users
//! to customize container resources, project-specific settings, and shared
//! configuration that can be layered via `[defaults]` and `[project.<name>]`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::path::PathBuf;

use super::tsk_env::TskEnvError;

/// User configuration loaded from tsk.toml
///
/// Contains user-configurable options for TSK, including container engine
/// selection, server settings, shared defaults, and project-specific overrides.
///
/// Configuration is resolved via [`TskConfig::resolve_config`] which layers
/// `[defaults]` and `[project.<name>]` sections with proper merge semantics.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TskConfig {
    /// Container engine to use (top-level setting)
    #[serde(default)]
    pub container_engine: ContainerEngine,
    /// Server daemon configuration
    #[serde(default)]
    pub server: ServerConfig,
    /// Shared default configuration applied to all projects
    #[serde(default)]
    pub defaults: SharedConfig,
    /// Project-specific configurations keyed by project name
    #[serde(default)]
    pub project: HashMap<String, SharedConfig>,
}

impl TskConfig {
    /// Resolve configuration for a specific project by layering defaults and project overrides.
    ///
    /// Resolution order (later layers override earlier):
    /// 1. Built-in defaults
    /// 2. `[defaults]` section
    /// 3. Project-level `.tsk/tsk.toml` (if provided)
    /// 4. `[project.<name>]` section (if present)
    ///
    /// After layer merging, resolves `squid_conf_path` to content if `squid_conf` was
    /// not set by any layer's inline `squid_conf` field.
    pub fn resolve_config(
        &self,
        project_name: &str,
        project_config: Option<&SharedConfig>,
        project_root: Option<&Path>,
    ) -> ResolvedConfig {
        let mut resolved = ResolvedConfig::default();

        // Apply defaults layer
        self.apply_shared_config(&mut resolved, &self.defaults);

        // Apply project-level .tsk/tsk.toml layer (if provided)
        if let Some(config) = project_config {
            self.apply_shared_config(&mut resolved, config);
        }

        // Apply user [project.<name>] layer (highest priority)
        if let Some(user_project_config) = self.project.get(project_name) {
            self.apply_shared_config(&mut resolved, user_project_config);
        }

        // Resolve squid_conf_path to content if squid_conf was not set by any layer
        if resolved.squid_conf.is_none() {
            resolved.squid_conf =
                self.resolve_squid_conf_path(project_name, project_config, project_root);
        }

        resolved
    }

    /// Resolves squid_conf_path to file content, checking config layers in priority order.
    /// Within each layer, squid_conf (already merged as scalar) takes priority over squid_conf_path.
    fn resolve_squid_conf_path(
        &self,
        project_name: &str,
        project_config: Option<&SharedConfig>,
        project_root: Option<&Path>,
    ) -> Option<String> {
        // Priority order: user [project.<name>] > project .tsk/tsk.toml > user [defaults]

        // User project-specific config
        if let Some(config) = self.project.get(project_name)
            && let Some(ref path_str) = config.squid_conf_path
            && let Some(content) = try_read_squid_conf(&expand_tilde(path_str))
        {
            return Some(content);
        }

        // Project-level .tsk/tsk.toml config
        if let Some(config) = project_config
            && let Some(ref path_str) = config.squid_conf_path
        {
            let path = if let Some(root) = project_root {
                root.join(path_str)
            } else {
                PathBuf::from(path_str)
            };
            if let Some(content) = try_read_squid_conf(&path) {
                return Some(content);
            }
        }

        // User defaults
        if let Some(ref path_str) = self.defaults.squid_conf_path
            && let Some(content) = try_read_squid_conf(&expand_tilde(path_str))
        {
            return Some(content);
        }

        None
    }

    /// Apply a SharedConfig layer onto a ResolvedConfig with proper merge semantics.
    ///
    /// - Scalars: override if `Some`
    /// - Lists (`host_ports`, `volumes`, `env`): combine with dedup/conflict resolution
    /// - Maps (`stack_config`, `agent_config`): combine keys, same key replaces entire value
    fn apply_shared_config(&self, resolved: &mut ResolvedConfig, config: &SharedConfig) {
        if let Some(ref agent) = config.agent {
            resolved.agent = agent.clone();
        }
        if let Some(ref stack) = config.stack {
            resolved.stack = stack.clone();
        }
        if let Some(dind) = config.dind {
            resolved.dind = dind;
        }
        if let Some(memory) = config.memory_gb {
            resolved.memory_gb = memory;
        }
        if let Some(cpu) = config.cpu {
            resolved.cpu = cpu;
        }
        if let Some(git_town) = config.git_town {
            resolved.git_town = git_town;
        }
        if let Some(ref setup) = config.setup {
            resolved.setup = Some(setup.clone());
        }
        if let Some(ref squid_conf) = config.squid_conf {
            resolved.squid_conf = Some(squid_conf.clone());
        }
        if let Some(ref review_command) = config.review_command {
            resolved.review_command = Some(review_command.clone());
        }

        // host_ports: combine, deduplicate
        for &port in &config.host_ports {
            if !resolved.host_ports.contains(&port) {
                resolved.host_ports.push(port);
            }
        }

        // volumes: combine, higher-priority wins on container path conflict
        for volume in &config.volumes {
            let container_path = match volume {
                VolumeMount::Bind(b) => &b.container,
                VolumeMount::Named(n) => &n.container,
            };
            resolved.volumes.retain(|v| {
                let existing_path = match v {
                    VolumeMount::Bind(b) => &b.container,
                    VolumeMount::Named(n) => &n.container,
                };
                existing_path != container_path
            });
            resolved.volumes.push(volume.clone());
        }

        // env: combine, higher-priority wins on name conflict
        for env_var in &config.env {
            resolved.env.retain(|e| e.name != env_var.name);
            resolved.env.push(env_var.clone());
        }

        // stack_config: combine names, higher-priority replaces entire struct per name
        for (name, stack_cfg) in &config.stack_config {
            resolved
                .stack_config
                .insert(name.clone(), stack_cfg.clone());
        }

        // agent_config: combine names, higher-priority replaces entire struct per name
        for (name, agent_cfg) in &config.agent_config {
            resolved
                .agent_config
                .insert(name.clone(), agent_cfg.clone());
        }
    }
}

/// Container engine to use for running containers
#[derive(Debug, Clone, Deserialize, PartialEq, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ContainerEngine {
    Docker,
    Podman,
}

impl Default for ContainerEngine {
    fn default() -> Self {
        if std::env::var("TSK_CONTAINER").is_ok() {
            ContainerEngine::Podman
        } else {
            ContainerEngine::Docker
        }
    }
}

/// Shared configuration shape used by both `[defaults]` and `[project.<name>]` sections.
///
/// All fields are optional so they can be layered during resolution.
/// Lists combine across layers; scalars take the highest-priority `Some` value.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct SharedConfig {
    /// Default agent (e.g., "claude", "codex")
    pub agent: Option<String>,
    /// Default stack (e.g., "go", "rust", "python")
    pub stack: Option<String>,
    /// Enable Docker-in-Docker support
    pub dind: Option<bool>,
    /// Container memory limit in gigabytes
    #[serde(alias = "memory_limit_gb")]
    pub memory_gb: Option<f64>,
    /// Number of CPUs available to container
    #[serde(alias = "cpu_limit")]
    pub cpu: Option<u32>,
    /// Enable git-town parent branch tracking
    pub git_town: Option<bool>,
    /// Host service ports to forward from proxy to host
    #[serde(default, alias = "host_services")]
    pub host_ports: Vec<u16>,
    /// Custom setup commands for the container
    pub setup: Option<String>,
    /// Per-stack configuration overrides
    #[serde(default)]
    pub stack_config: HashMap<String, StackConfig>,
    /// Per-agent configuration overrides
    #[serde(default)]
    pub agent_config: HashMap<String, AgentConfig>,
    /// Volume mounts for containers
    #[serde(default)]
    pub volumes: Vec<VolumeMount>,
    /// Environment variables for containers
    #[serde(default)]
    pub env: Vec<EnvVar>,
    /// Inline Squid proxy configuration content (takes priority over squid_conf_path)
    pub squid_conf: Option<String>,
    /// Path to a Squid proxy configuration file
    pub squid_conf_path: Option<String>,
    /// Command to open review files (placeholders: `{{base}}`, `{{version}}`, `{{review_file}}`)
    pub review_command: Option<String>,
}

/// Per-stack configuration (e.g., custom Dockerfile setup commands)
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct StackConfig {
    /// Custom setup commands for this stack layer
    pub setup: Option<String>,
}

/// Per-agent configuration (e.g., custom Dockerfile setup commands)
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct AgentConfig {
    /// Custom setup commands for this agent layer
    pub setup: Option<String>,
}

/// Fully resolved configuration with no optional scalars.
///
/// Produced by [`TskConfig::resolve_config`] after layering `[defaults]`
/// and `[project.<name>]` sections over built-in defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedConfig {
    /// Agent to use (default: "claude")
    pub agent: String,
    /// Stack to use (default: "default")
    pub stack: String,
    /// Docker-in-Docker support (default: false)
    pub dind: bool,
    /// Container memory limit in gigabytes (default: 12.0)
    pub memory_gb: f64,
    /// Number of CPUs available to container (default: 8)
    pub cpu: u32,
    /// Git-town parent branch tracking (default: false)
    pub git_town: bool,
    /// Host service ports to forward from proxy
    pub host_ports: Vec<u16>,
    /// Custom setup commands
    pub setup: Option<String>,
    /// Per-stack configuration overrides
    pub stack_config: HashMap<String, StackConfig>,
    /// Per-agent configuration overrides
    pub agent_config: HashMap<String, AgentConfig>,
    /// Volume mounts for containers
    pub volumes: Vec<VolumeMount>,
    /// Environment variables for containers
    pub env: Vec<EnvVar>,
    /// Resolved Squid proxy configuration content
    pub squid_conf: Option<String>,
    /// Command to open review files (placeholders: `{{base}}`, `{{version}}`, `{{review_file}}`)
    pub review_command: Option<String>,
}

impl Default for ResolvedConfig {
    fn default() -> Self {
        Self {
            agent: "claude".to_string(),
            stack: "default".to_string(),
            dind: false,
            memory_gb: 12.0,
            cpu: 8,
            git_town: false,
            host_ports: Vec::new(),
            setup: None,
            stack_config: HashMap::new(),
            agent_config: HashMap::new(),
            volumes: Vec::new(),
            env: Vec::new(),
            squid_conf: None,
            review_command: None,
        }
    }
}

impl ResolvedConfig {
    /// Convert memory limit from gigabytes to bytes for Docker/Bollard API
    pub fn memory_limit_bytes(&self) -> i64 {
        (self.memory_gb * 1024.0 * 1024.0 * 1024.0) as i64
    }

    /// Convert CPU limit to microseconds per 100ms period for Docker/Bollard API
    ///
    /// Docker uses cpu_quota to limit CPU usage. The value represents microseconds
    /// per 100ms period (cpu_period defaults to 100000 microseconds).
    /// So 100,000 = 1 CPU, 200,000 = 2 CPUs, etc.
    pub fn cpu_quota_microseconds(&self) -> i64 {
        self.cpu as i64 * 100_000
    }

    /// Returns host ports as a sorted, comma-separated string for environment variables.
    ///
    /// Returns empty string if no ports are configured.
    pub fn host_ports_env(&self) -> String {
        let mut ports = self.host_ports.clone();
        ports.sort();
        ports
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns true if any host ports are configured
    pub fn has_host_ports(&self) -> bool {
        !self.host_ports.is_empty()
    }

    /// Extract proxy-specific configuration for fingerprinting and proxy management
    pub fn proxy_config(&self) -> ResolvedProxyConfig {
        ResolvedProxyConfig {
            host_ports: self.host_ports.clone(),
            squid_conf: self.squid_conf.clone(),
        }
    }
}

/// Proxy-specific configuration extracted from ResolvedConfig.
/// Used to determine proxy container identity via fingerprinting.
#[derive(Debug, Clone, Default)]
pub struct ResolvedProxyConfig {
    pub host_ports: Vec<u16>,
    pub squid_conf: Option<String>,
}

impl ResolvedProxyConfig {
    /// Compute a short fingerprint (first 8 hex chars of SHA256) from the proxy config.
    /// Tasks with identical fingerprints share the same proxy container.
    pub fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();

        // Hash sorted host_ports
        let mut ports = self.host_ports.clone();
        ports.sort();
        for port in &ports {
            hasher.update(port.to_string().as_bytes());
            hasher.update(b",");
        }

        // Hash squid_conf content if present
        if let Some(ref conf) = self.squid_conf {
            hasher.update(conf.as_bytes());
        }

        let result = hasher.finalize();
        format!("{:x}", result).chars().take(8).collect()
    }

    /// Container name for this proxy configuration
    pub fn proxy_container_name(&self) -> String {
        format!("tsk-proxy-{}", self.fingerprint())
    }

    /// External network name for this proxy configuration
    pub fn external_network_name(&self) -> String {
        format!("tsk-external-{}", self.fingerprint())
    }

    /// Host ports as comma-separated string for environment variables
    pub fn host_ports_env(&self) -> String {
        let mut ports = self.host_ports.clone();
        ports.sort();
        ports
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Proxy URL for HTTP_PROXY environment variable
    pub fn proxy_url(&self) -> String {
        format!("http://{}:3128", self.proxy_container_name())
    }
}

/// Server daemon configuration
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Enable automatic cleanup of old completed/failed tasks (default: true)
    pub auto_clean_enabled: bool,
    /// Minimum age in days before a task is eligible for auto-cleanup (default: 7.0)
    ///
    /// Supports fractional days (e.g., 0.5 for 12 hours). Negative values are
    /// clamped to 0.
    pub auto_clean_age_days: f64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            auto_clean_enabled: true,
            auto_clean_age_days: 7.0,
        }
    }
}

impl ServerConfig {
    /// Convert `auto_clean_age_days` to a `chrono::Duration`.
    ///
    /// Negative values are clamped to zero (clean immediately on next cycle).
    pub fn auto_clean_min_age(&self) -> chrono::Duration {
        let days = f64::max(0.0, self.auto_clean_age_days);
        let seconds = (days * 86_400.0) as i64;
        chrono::Duration::seconds(seconds)
    }
}

/// Environment variable configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EnvVar {
    /// Environment variable name
    pub name: String,
    /// Environment variable value
    pub value: String,
}

/// Volume mount configuration
///
/// Supports two types of mounts:
/// 1. Bind mounts: Map a host path to a container path
/// 2. Named volumes: Use a Docker-managed named volume
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum VolumeMount {
    /// Bind mount from host filesystem
    Bind(BindMount),
    /// Docker-managed named volume
    Named(NamedVolume),
}

/// Bind mount configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BindMount {
    /// Host path (supports ~ expansion)
    pub host: String,
    /// Container path
    pub container: String,
    /// Read-only flag (default: false)
    #[serde(default)]
    pub readonly: bool,
}

/// Named volume configuration (Docker-managed)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NamedVolume {
    /// Docker volume name (will be prefixed with "tsk-" to avoid conflicts)
    pub name: String,
    /// Container path
    pub container: String,
    /// Read-only flag (default: false)
    #[serde(default)]
    pub readonly: bool,
}

impl BindMount {
    /// Expand ~ in host path to actual home directory
    pub fn expanded_host_path(&self) -> Result<PathBuf, TskEnvError> {
        let expanded = expand_tilde(&self.host);
        // If the path started with ~ but expand_tilde couldn't resolve HOME,
        // the path is returned unchanged — detect this as an error.
        if self.host.starts_with('~') && expanded.as_os_str() == self.host.as_str() {
            return Err(TskEnvError::NoHomeDirectory);
        }
        Ok(expanded)
    }
}

/// Try to read a squid configuration file, warning on failure.
fn try_read_squid_conf(path: &Path) -> Option<String> {
    match std::fs::read_to_string(path) {
        Ok(content) => Some(content),
        Err(e) => {
            eprintln!(
                "Warning: Failed to read squid_conf_path '{}': {e}",
                path.display()
            );
            None
        }
    }
}

/// Expand leading `~` or `~/` in a path string to the user's home directory.
fn expand_tilde(path: &str) -> PathBuf {
    if path == "~" {
        if let Ok(home) = env::var("HOME").or_else(|_| env::var("USERPROFILE")) {
            return PathBuf::from(home);
        }
    } else if let Some(rest) = path.strip_prefix("~/")
        && let Ok(home) = env::var("HOME").or_else(|_| env::var("USERPROFILE"))
    {
        return PathBuf::from(home).join(rest);
    }
    PathBuf::from(path)
}

/// Load TskConfig from a configuration directory
///
/// Attempts to load and parse `tsk.toml` from the given config directory.
/// Returns default configuration if the file doesn't exist or can't be parsed.
/// Detects old configuration format and prints migration guidance.
pub fn load_config(config_dir: &Path) -> TskConfig {
    let config_file = config_dir.join("tsk.toml");
    if config_file.exists() {
        match std::fs::read_to_string(&config_file) {
            Ok(content) => {
                // Check for old config format and migrate if found.
                // Note: must use toml::from_str (document parser), not str::parse
                // (value parser). str::parse::<toml::Value> fails on table headers
                // like [docker] because it expects a single value expression.
                if let Ok(ref value) = toml::from_str::<toml::Value>(&content) {
                    let old_sections: Vec<&str> = ["docker", "proxy", "git_town"]
                        .iter()
                        .filter(|key| value.get(key).is_some())
                        .copied()
                        .collect();
                    if !old_sections.is_empty() {
                        eprintln!(
                            "\x1b[31mWarning: Your tsk.toml uses a deprecated configuration format.\x1b[0m\n\
                             Found deprecated sections: {}\n\n\
                             Support for this format will be removed in a future release.\n\
                             Please migrate your config:\n\
                             - [docker] settings → top-level `container_engine` and [defaults] section\n\
                             - [proxy] host_services → [defaults] host_ports\n\
                             - [git_town] enabled → [defaults] git_town\n\n\
                             See the README for the new configuration format.",
                            old_sections.join(", ")
                        );
                        return migrate_old_config(value);
                    }
                }

                match toml::from_str(&content) {
                    Ok(config) => return config,
                    Err(e) => {
                        eprintln!("Warning: Failed to parse {}: {}", config_file.display(), e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Warning: Failed to read {}: {}", config_file.display(), e);
            }
        }
    }
    TskConfig::default()
}

/// Migrate old-format tsk.toml values to the new `TskConfig` structure.
///
/// Maps:
/// - `[docker].container_engine` → top-level `container_engine`
/// - `[docker].memory_limit_gb` → `[defaults].memory_gb`
/// - `[docker].cpu_limit` / `[docker].cpu_quota` → `[defaults].cpu`
/// - `[docker].dind` → `[defaults].dind`
/// - `[proxy].host_services` → `[defaults].host_ports`
/// - `[git_town].enabled` → `[defaults].git_town`
/// - `[project.<name>]` fields are passed through (old ProjectConfig is a subset of SharedConfig)
fn migrate_old_config(value: &toml::Value) -> TskConfig {
    let mut config = TskConfig::default();

    if let Some(docker) = value.get("docker").and_then(|v| v.as_table()) {
        if let Some(engine) = docker.get("container_engine").and_then(|v| v.as_str()) {
            match engine {
                "podman" => config.container_engine = ContainerEngine::Podman,
                _ => config.container_engine = ContainerEngine::Docker,
            }
        }
        if let Some(mem) = docker
            .get("memory_limit_gb")
            .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
        {
            config.defaults.memory_gb = Some(mem);
        }
        // Accept both cpu_limit and cpu_quota (both legacy field names from old [docker] section)
        if let Some(cpu) = docker
            .get("cpu_limit")
            .or_else(|| docker.get("cpu_quota"))
            .and_then(|v| v.as_integer())
        {
            config.defaults.cpu = Some(cpu as u32);
        }
        if let Some(dind) = docker.get("dind").and_then(|v| v.as_bool()) {
            config.defaults.dind = Some(dind);
        }
    }

    if let Some(proxy) = value.get("proxy").and_then(|v| v.as_table())
        && let Some(services) = proxy.get("host_services").and_then(|v| v.as_array())
    {
        config.defaults.host_ports = services
            .iter()
            .filter_map(|v| v.as_integer().map(|i| i as u16))
            .collect();
    }

    if let Some(git_town) = value.get("git_town").and_then(|v| v.as_table())
        && let Some(enabled) = git_town.get("enabled").and_then(|v| v.as_bool())
    {
        config.defaults.git_town = Some(enabled);
    }

    // Merge any new-format sections that may coexist with old sections
    if let Some(server) = value.get("server")
        && let Ok(s) = server.clone().try_into()
    {
        config.server = s;
    }
    if let Some(defaults) = value.get("defaults")
        && let Ok(d) = defaults.clone().try_into()
    {
        config.defaults = d;
    }
    if let Some(project) = value.get("project")
        && let Ok(p) = project.clone().try_into()
    {
        config.project = p;
    }

    config
}

/// Load project-level configuration from `.tsk/tsk.toml` in the project root.
///
/// Returns `None` if the file doesn't exist or can't be parsed (fail-open with warning).
/// The returned [`SharedConfig`] is intended to be passed to [`TskConfig::resolve_config`]
/// as the project config layer.
pub fn load_project_config(project_root: &Path) -> Option<SharedConfig> {
    let config_file = project_root.join(".tsk").join("tsk.toml");
    if config_file.exists() {
        match std::fs::read_to_string(&config_file) {
            Ok(content) => match toml::from_str(&content) {
                Ok(config) => return Some(config),
                Err(e) => {
                    eprintln!("Warning: Failed to parse {}: {}", config_file.display(), e);
                }
            },
            Err(e) => {
                eprintln!("Warning: Failed to read {}: {}", config_file.display(), e);
            }
        }
    }
    None
}

/// Resolves the stack using the full resolution chain.
///
/// Priority: CLI flag > config layers (user `[project.<name>]` > project `.tsk/tsk.toml`
/// > user `[defaults]`) > auto-detect from project files > `"default"` fallback.
pub async fn resolve_stack(
    cli_stack: Option<String>,
    tsk_config: &TskConfig,
    project_name: &str,
    project_config: Option<&SharedConfig>,
    repo_root: &Path,
) -> String {
    if let Some(stack) = cli_stack {
        return stack;
    }

    let config_stack = tsk_config
        .project
        .get(project_name)
        .and_then(|p| p.stack.clone())
        .or_else(|| project_config.and_then(|pc| pc.stack.clone()))
        .or_else(|| tsk_config.defaults.stack.clone());

    if let Some(stack) = config_stack {
        return stack;
    }

    match crate::repository::detect_stack(repo_root).await {
        Ok(detected) => detected,
        Err(e) => {
            eprintln!("Warning: Failed to detect stack: {e}. Using default.");
            "default".to_string()
        }
    }
}

/// Resolves the agent from CLI flag or resolved config.
///
/// Priority: CLI flag > resolved config agent (which already handles config layer merging).
pub fn resolve_agent(cli_agent: Option<String>, resolved_config: &ResolvedConfig) -> String {
    cli_agent.unwrap_or_else(|| resolved_config.agent.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_resolved_config_default() {
        let resolved = ResolvedConfig::default();
        assert_eq!(resolved.agent, "claude");
        assert_eq!(resolved.stack, "default");
        assert!(!resolved.dind);
        assert_eq!(resolved.memory_gb, 12.0);
        assert_eq!(resolved.cpu, 8);
        assert!(!resolved.git_town);
        assert!(resolved.host_ports.is_empty());
        assert!(resolved.setup.is_none());
        assert!(resolved.stack_config.is_empty());
        assert!(resolved.agent_config.is_empty());
        assert!(resolved.volumes.is_empty());
        assert!(resolved.env.is_empty());
    }

    #[test]
    fn test_resolved_config_conversion_methods() {
        let resolved = ResolvedConfig::default();
        // 12 GB = 12 * 1024 * 1024 * 1024 bytes
        assert_eq!(resolved.memory_limit_bytes(), 12 * 1024 * 1024 * 1024);
        // 8 CPUs = 8 * 100,000 microseconds
        assert_eq!(resolved.cpu_quota_microseconds(), 800_000);

        // Test with custom values
        let custom = ResolvedConfig {
            memory_gb: 5.5,
            cpu: 4,
            host_ports: vec![5432, 6379, 3000],
            ..Default::default()
        };
        assert_eq!(
            custom.memory_limit_bytes(),
            (5.5 * 1024.0 * 1024.0 * 1024.0) as i64
        );
        assert_eq!(custom.cpu_quota_microseconds(), 400_000);
        assert_eq!(custom.host_ports_env(), "3000,5432,6379");
        assert!(custom.has_host_ports());

        // Empty host ports
        assert_eq!(resolved.host_ports_env(), "");
        assert!(!resolved.has_host_ports());
    }

    #[test]
    fn test_tsk_config_default() {
        let config = TskConfig::default();
        assert!(config.project.is_empty());
        assert!(config.defaults.agent.is_none());
        assert!(config.defaults.stack.is_none());
        assert!(config.defaults.host_ports.is_empty());
        assert!(config.server.auto_clean_enabled);
        assert_eq!(config.server.auto_clean_age_days, 7.0);
    }

    #[test]
    fn test_config_from_new_toml_format() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config_dir = temp_dir.path();

        let toml_content = r#"
container_engine = "podman"

[server]
auto_clean_enabled = false
auto_clean_age_days = 14.0

[defaults]
memory_gb = 16.0
cpu = 4
host_ports = [6379]
git_town = true

[project.my-project]
agent = "codex"
stack = "rust"
memory_gb = 24.0
cpu = 16
dind = true
volumes = [
    { host = "~/debug-logs", container = "/debug", readonly = true }
]
env = [
    { name = "RUST_LOG", value = "debug" }
]
"#;
        let mut file = std::fs::File::create(config_dir.join("tsk.toml")).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        let config = load_config(config_dir);

        assert_eq!(config.container_engine, ContainerEngine::Podman);
        assert!(!config.server.auto_clean_enabled);
        assert_eq!(config.server.auto_clean_age_days, 14.0);

        // Check defaults
        assert_eq!(config.defaults.memory_gb, Some(16.0));
        assert_eq!(config.defaults.cpu, Some(4));
        assert_eq!(config.defaults.host_ports, vec![6379]);
        assert_eq!(config.defaults.git_town, Some(true));

        // Check project
        let project = config.project.get("my-project").unwrap();
        assert_eq!(project.agent, Some("codex".to_string()));
        assert_eq!(project.stack, Some("rust".to_string()));
        assert_eq!(project.memory_gb, Some(24.0));
        assert_eq!(project.cpu, Some(16));
        assert_eq!(project.dind, Some(true));
        assert_eq!(project.volumes.len(), 1);
        assert_eq!(project.env.len(), 1);
    }

    #[test]
    fn test_new_format_config_with_old_field_names() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config_dir = temp_dir.path();

        let toml_content = r#"
[defaults]
memory_limit_gb = 16.0
cpu_limit = 4
host_services = [6379]
"#;
        let mut file = std::fs::File::create(config_dir.join("tsk.toml")).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        let config = load_config(config_dir);
        assert_eq!(config.defaults.memory_gb, Some(16.0));
        assert_eq!(config.defaults.cpu, Some(4));
        assert_eq!(config.defaults.host_ports, vec![6379]);
    }

    #[test]
    fn test_resolve_config_merging_scalars() {
        let config = TskConfig {
            defaults: SharedConfig {
                agent: Some("codex".to_string()),
                memory_gb: Some(16.0),
                git_town: Some(true),
                ..Default::default()
            },
            project: HashMap::from([(
                "my-project".to_string(),
                SharedConfig {
                    agent: Some("claude".to_string()),
                    stack: Some("rust".to_string()),
                    cpu: Some(16),
                    dind: Some(true),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        let resolved = config.resolve_config("my-project", None, None);

        // Project overrides defaults
        assert_eq!(resolved.agent, "claude");
        // Project sets stack
        assert_eq!(resolved.stack, "rust");
        // Defaults sets memory (project doesn't override)
        assert_eq!(resolved.memory_gb, 16.0);
        // Project sets cpu
        assert_eq!(resolved.cpu, 16);
        // Project sets dind
        assert!(resolved.dind);
        // Defaults sets git_town
        assert!(resolved.git_town);

        // Non-existent project only gets defaults
        let resolved_other = config.resolve_config("other-project", None, None);
        assert_eq!(resolved_other.agent, "codex");
        assert_eq!(resolved_other.stack, "default");
        assert_eq!(resolved_other.memory_gb, 16.0);
        assert_eq!(resolved_other.cpu, 8); // built-in default
    }

    #[test]
    fn test_resolve_config_merging_host_ports() {
        let config = TskConfig {
            defaults: SharedConfig {
                host_ports: vec![5432, 6379],
                ..Default::default()
            },
            project: HashMap::from([(
                "my-project".to_string(),
                SharedConfig {
                    host_ports: vec![6379, 3000],
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        let resolved = config.resolve_config("my-project", None, None);

        // Combined and deduplicated
        assert_eq!(resolved.host_ports, vec![5432, 6379, 3000]);
    }

    #[test]
    fn test_resolve_config_merging_volumes() {
        let config = TskConfig {
            defaults: SharedConfig {
                volumes: vec![
                    VolumeMount::Bind(BindMount {
                        host: "/host/cache".to_string(),
                        container: "/cache".to_string(),
                        readonly: false,
                    }),
                    VolumeMount::Named(NamedVolume {
                        name: "data".to_string(),
                        container: "/data".to_string(),
                        readonly: false,
                    }),
                ],
                ..Default::default()
            },
            project: HashMap::from([(
                "my-project".to_string(),
                SharedConfig {
                    volumes: vec![
                        // Override /cache with a named volume (different type, same container path)
                        VolumeMount::Named(NamedVolume {
                            name: "project-cache".to_string(),
                            container: "/cache".to_string(),
                            readonly: true,
                        }),
                        // New volume
                        VolumeMount::Bind(BindMount {
                            host: "/host/logs".to_string(),
                            container: "/logs".to_string(),
                            readonly: true,
                        }),
                    ],
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        let resolved = config.resolve_config("my-project", None, None);

        // /data from defaults remains, /cache replaced by project's named volume, /logs added
        assert_eq!(resolved.volumes.len(), 3);

        match &resolved.volumes[0] {
            VolumeMount::Named(n) => {
                assert_eq!(n.name, "data");
                assert_eq!(n.container, "/data");
            }
            _ => panic!("Expected Named volume for /data"),
        }

        match &resolved.volumes[1] {
            VolumeMount::Named(n) => {
                assert_eq!(n.name, "project-cache");
                assert_eq!(n.container, "/cache");
                assert!(n.readonly);
            }
            _ => panic!("Expected Named volume for /cache"),
        }

        match &resolved.volumes[2] {
            VolumeMount::Bind(b) => {
                assert_eq!(b.host, "/host/logs");
                assert_eq!(b.container, "/logs");
                assert!(b.readonly);
            }
            _ => panic!("Expected Bind mount for /logs"),
        }
    }

    #[test]
    fn test_resolve_config_merging_env() {
        let config = TskConfig {
            defaults: SharedConfig {
                env: vec![
                    EnvVar {
                        name: "DATABASE_URL".to_string(),
                        value: "postgres://localhost/db".to_string(),
                    },
                    EnvVar {
                        name: "DEBUG".to_string(),
                        value: "false".to_string(),
                    },
                ],
                ..Default::default()
            },
            project: HashMap::from([(
                "my-project".to_string(),
                SharedConfig {
                    env: vec![
                        EnvVar {
                            name: "DEBUG".to_string(),
                            value: "true".to_string(),
                        },
                        EnvVar {
                            name: "RUST_LOG".to_string(),
                            value: "info".to_string(),
                        },
                    ],
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        let resolved = config.resolve_config("my-project", None, None);

        assert_eq!(resolved.env.len(), 3);
        assert_eq!(resolved.env[0].name, "DATABASE_URL");
        assert_eq!(resolved.env[0].value, "postgres://localhost/db");
        assert_eq!(resolved.env[1].name, "DEBUG");
        assert_eq!(resolved.env[1].value, "true");
        assert_eq!(resolved.env[2].name, "RUST_LOG");
        assert_eq!(resolved.env[2].value, "info");
    }

    #[test]
    fn test_resolve_config_merging_stack_config() {
        let config = TskConfig {
            defaults: SharedConfig {
                stack_config: HashMap::from([
                    (
                        "rust".to_string(),
                        StackConfig {
                            setup: Some("RUN apt-get install -y cmake".to_string()),
                        },
                    ),
                    (
                        "go".to_string(),
                        StackConfig {
                            setup: Some("RUN go install tool".to_string()),
                        },
                    ),
                ]),
                ..Default::default()
            },
            project: HashMap::from([(
                "my-project".to_string(),
                SharedConfig {
                    stack_config: HashMap::from([
                        (
                            "rust".to_string(),
                            StackConfig {
                                setup: Some("RUN cargo install custom-tool".to_string()),
                            },
                        ),
                        (
                            "java".to_string(),
                            StackConfig {
                                setup: Some("RUN apt-get install -y openjdk-17-jdk".to_string()),
                            },
                        ),
                    ]),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        let resolved = config.resolve_config("my-project", None, None);

        assert_eq!(resolved.stack_config.len(), 3);
        assert_eq!(
            resolved.stack_config["rust"].setup,
            Some("RUN cargo install custom-tool".to_string())
        );
        assert_eq!(
            resolved.stack_config["go"].setup,
            Some("RUN go install tool".to_string())
        );
        assert_eq!(
            resolved.stack_config["java"].setup,
            Some("RUN apt-get install -y openjdk-17-jdk".to_string())
        );
    }

    #[test]
    fn test_resolve_config_merging_agent_config() {
        let config = TskConfig {
            defaults: SharedConfig {
                agent_config: HashMap::from([(
                    "claude".to_string(),
                    AgentConfig {
                        setup: Some("RUN npm install -g tool".to_string()),
                    },
                )]),
                ..Default::default()
            },
            project: HashMap::from([(
                "my-project".to_string(),
                SharedConfig {
                    agent_config: HashMap::from([
                        (
                            "claude".to_string(),
                            AgentConfig {
                                setup: Some("RUN pip install custom".to_string()),
                            },
                        ),
                        (
                            "codex".to_string(),
                            AgentConfig {
                                setup: Some("RUN npm install -g codex-tool".to_string()),
                            },
                        ),
                    ]),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        let resolved = config.resolve_config("my-project", None, None);

        assert_eq!(resolved.agent_config.len(), 2);
        assert_eq!(
            resolved.agent_config["claude"].setup,
            Some("RUN pip install custom".to_string())
        );
        assert_eq!(
            resolved.agent_config["codex"].setup,
            Some("RUN npm install -g codex-tool".to_string())
        );
    }

    #[test]
    fn test_old_format_migration() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config_dir = temp_dir.path();

        // Config with old [docker] section should migrate values to defaults
        let toml_content = r#"
[docker]
memory_limit_gb = 8.0
cpu_limit = 4
dind = true
container_engine = "podman"
"#;
        std::fs::write(config_dir.join("tsk.toml"), toml_content).unwrap();
        let config = load_config(config_dir);
        assert_eq!(config.defaults.memory_gb, Some(8.0));
        assert_eq!(config.defaults.cpu, Some(4));
        assert_eq!(config.defaults.dind, Some(true));
        assert_eq!(config.container_engine, ContainerEngine::Podman);

        // Test with [proxy] — host_services should migrate
        let toml_content = r#"
[proxy]
host_services = [5432]
"#;
        std::fs::write(config_dir.join("tsk.toml"), toml_content).unwrap();
        let config = load_config(config_dir);
        assert_eq!(config.defaults.host_ports, vec![5432]);

        // Test with [git_town] — enabled should migrate to git_town bool
        let toml_content = r#"
[git_town]
enabled = true
"#;
        std::fs::write(config_dir.join("tsk.toml"), toml_content).unwrap();
        let config = load_config(config_dir);
        assert_eq!(config.defaults.git_town, Some(true));

        // Test with legacy cpu_quota field name
        let toml_content = r#"
[docker]
cpu_quota = 16
"#;
        std::fs::write(config_dir.join("tsk.toml"), toml_content).unwrap();
        let config = load_config(config_dir);
        assert_eq!(config.defaults.cpu, Some(16));

        // Test with integer memory_limit_gb (no decimal)
        let toml_content = r#"
[docker]
memory_limit_gb = 30
"#;
        std::fs::write(config_dir.join("tsk.toml"), toml_content).unwrap();
        let config = load_config(config_dir);
        assert_eq!(config.defaults.memory_gb, Some(30.0));

        // Test combined old format
        let toml_content = r#"
[docker]
memory_limit_gb = 24.0
cpu_limit = 12
dind = true

[proxy]
host_services = [5432, 6379]

[git_town]
enabled = true

[project.my-project]
agent = "codex"
stack = "go"
"#;
        std::fs::write(config_dir.join("tsk.toml"), toml_content).unwrap();
        let config = load_config(config_dir);
        assert_eq!(config.defaults.memory_gb, Some(24.0));
        assert_eq!(config.defaults.cpu, Some(12));
        assert_eq!(config.defaults.dind, Some(true));
        assert_eq!(config.defaults.host_ports, vec![5432, 6379]);
        assert_eq!(config.defaults.git_town, Some(true));
        let project = config.project.get("my-project").unwrap();
        assert_eq!(project.agent, Some("codex".to_string()));
        assert_eq!(project.stack, Some("go".to_string()));
    }

    #[test]
    fn test_config_missing_toml_uses_defaults() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config = load_config(temp_dir.path());

        let resolved = config.resolve_config("any-project", None, None);
        assert_eq!(resolved.agent, "claude");
        assert_eq!(resolved.stack, "default");
        assert_eq!(resolved.memory_gb, 12.0);
        assert_eq!(resolved.cpu, 8);
        assert!(!resolved.dind);
        assert!(!resolved.git_town);
    }

    #[test]
    fn test_bind_mount_path_expansion() {
        let bind_mount = BindMount {
            host: "~/.cache/go-build".to_string(),
            container: "/home/agent/.cache/go-build".to_string(),
            readonly: false,
        };

        let expanded = bind_mount.expanded_host_path().unwrap();
        assert!(!expanded.to_string_lossy().starts_with("~"));
        assert!(expanded.to_string_lossy().ends_with(".cache/go-build"));

        let bind_mount_home = BindMount {
            host: "~".to_string(),
            container: "/home/agent".to_string(),
            readonly: false,
        };

        let expanded_home = bind_mount_home.expanded_host_path().unwrap();
        assert!(!expanded_home.to_string_lossy().starts_with("~"));
        assert!(!expanded_home.to_string_lossy().is_empty());

        let bind_mount_abs = BindMount {
            host: "/tmp/shared".to_string(),
            container: "/shared".to_string(),
            readonly: true,
        };

        let expanded_abs = bind_mount_abs.expanded_host_path().unwrap();
        assert_eq!(expanded_abs.to_string_lossy(), "/tmp/shared");
    }

    #[test]
    fn test_named_volume_config_from_toml() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config_dir = temp_dir.path();

        let toml_content = r#"
[project.my-go-project]
stack = "go"
volumes = [
    { name = "go-mod-cache", container = "/go/pkg/mod" },
    { name = "go-build-cache", container = "/home/agent/.cache/go-build", readonly = true }
]
"#;
        let mut file = std::fs::File::create(config_dir.join("tsk.toml")).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        let config = load_config(config_dir);

        let go_config = config.project.get("my-go-project").unwrap();
        assert_eq!(go_config.volumes.len(), 2);

        match &go_config.volumes[0] {
            VolumeMount::Named(named) => {
                assert_eq!(named.name, "go-mod-cache");
                assert_eq!(named.container, "/go/pkg/mod");
                assert!(!named.readonly);
            }
            VolumeMount::Bind(_) => panic!("Expected Named volume"),
        }

        match &go_config.volumes[1] {
            VolumeMount::Named(named) => {
                assert_eq!(named.name, "go-build-cache");
                assert_eq!(named.container, "/home/agent/.cache/go-build");
                assert!(named.readonly);
            }
            VolumeMount::Bind(_) => panic!("Expected Named volume"),
        }
    }

    #[test]
    fn test_mixed_volume_config_from_toml() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config_dir = temp_dir.path();

        let toml_content = r#"
[project.mixed-project]
volumes = [
    { host = "~/.cache/shared", container = "/cache" },
    { name = "data-volume", container = "/data" }
]
"#;
        let mut file = std::fs::File::create(config_dir.join("tsk.toml")).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        let config = load_config(config_dir);

        let project_config = config.project.get("mixed-project").unwrap();
        assert_eq!(project_config.volumes.len(), 2);

        match &project_config.volumes[0] {
            VolumeMount::Bind(bind) => {
                assert_eq!(bind.host, "~/.cache/shared");
                assert_eq!(bind.container, "/cache");
            }
            VolumeMount::Named(_) => panic!("Expected Bind mount"),
        }

        match &project_config.volumes[1] {
            VolumeMount::Named(named) => {
                assert_eq!(named.name, "data-volume");
                assert_eq!(named.container, "/data");
            }
            VolumeMount::Bind(_) => panic!("Expected Named volume"),
        }
    }

    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();
        assert!(config.auto_clean_enabled);
        assert_eq!(config.auto_clean_age_days, 7.0);
    }

    #[test]
    fn test_server_config_auto_clean_min_age() {
        let config = ServerConfig::default();
        assert_eq!(config.auto_clean_min_age(), chrono::Duration::days(7));

        let custom = ServerConfig {
            auto_clean_enabled: true,
            auto_clean_age_days: 0.5,
        };
        assert_eq!(
            custom.auto_clean_min_age(),
            chrono::Duration::seconds(43200)
        );
    }

    #[test]
    fn test_server_config_negative_days_clamped() {
        let config = ServerConfig {
            auto_clean_enabled: true,
            auto_clean_age_days: -5.0,
        };
        assert_eq!(config.auto_clean_min_age(), chrono::Duration::zero());
    }

    #[test]
    fn test_server_config_from_toml() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config_dir = temp_dir.path();

        let toml_content = r#"
[server]
auto_clean_enabled = false
auto_clean_age_days = 14.0
"#;
        let mut file = std::fs::File::create(config_dir.join("tsk.toml")).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        let config = load_config(config_dir);
        assert!(!config.server.auto_clean_enabled);
        assert_eq!(config.server.auto_clean_age_days, 14.0);
    }

    #[test]
    fn test_container_engine_default_depends_on_environment() {
        let config = TskConfig::default();
        if std::env::var("TSK_CONTAINER").is_ok() {
            assert_eq!(config.container_engine, ContainerEngine::Podman);
        } else {
            assert_eq!(config.container_engine, ContainerEngine::Docker);
        }
    }

    #[test]
    fn test_stack_config_parsing() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config_dir = temp_dir.path();

        let toml_content = r#"
[defaults.stack_config.scala]
setup = "RUN apt-get install -y scala"

[defaults.stack_config.rust]
setup = "RUN apt-get install -y cmake"

[project.my-project.stack_config.java]
setup = "RUN apt-get install -y openjdk-17-jdk"
"#;
        let mut file = std::fs::File::create(config_dir.join("tsk.toml")).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        let config = load_config(config_dir);

        assert_eq!(config.defaults.stack_config.len(), 2);
        assert_eq!(
            config.defaults.stack_config["scala"].setup,
            Some("RUN apt-get install -y scala".to_string())
        );
        assert_eq!(
            config.defaults.stack_config["rust"].setup,
            Some("RUN apt-get install -y cmake".to_string())
        );

        let project = config.project.get("my-project").unwrap();
        assert_eq!(project.stack_config.len(), 1);
        assert_eq!(
            project.stack_config["java"].setup,
            Some("RUN apt-get install -y openjdk-17-jdk".to_string())
        );
    }

    #[test]
    fn test_agent_config_parsing() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config_dir = temp_dir.path();

        let toml_content = r#"
[defaults.agent_config.claude]
setup = "RUN npm install -g tool"

[project.my-project.agent_config.my-agent]
setup = "RUN pip install custom-tool"
"#;
        let mut file = std::fs::File::create(config_dir.join("tsk.toml")).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        let config = load_config(config_dir);

        assert_eq!(config.defaults.agent_config.len(), 1);
        assert_eq!(
            config.defaults.agent_config["claude"].setup,
            Some("RUN npm install -g tool".to_string())
        );

        let project = config.project.get("my-project").unwrap();
        assert_eq!(project.agent_config.len(), 1);
        assert_eq!(
            project.agent_config["my-agent"].setup,
            Some("RUN pip install custom-tool".to_string())
        );
    }

    #[test]
    fn test_load_project_config() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let project_root = temp_dir.path();

        std::fs::create_dir_all(project_root.join(".tsk")).unwrap();
        let toml_content = r#"
agent = "codex"
stack = "python"
memory_gb = 20.0
host_ports = [8080]
setup = "RUN pip install custom-tool"

[stack_config.python]
setup = "RUN pip install numpy"
"#;
        std::fs::write(project_root.join(".tsk/tsk.toml"), toml_content).unwrap();

        let config = load_project_config(project_root).unwrap();
        assert_eq!(config.agent, Some("codex".to_string()));
        assert_eq!(config.stack, Some("python".to_string()));
        assert_eq!(config.memory_gb, Some(20.0));
        assert_eq!(config.host_ports, vec![8080]);
        assert_eq!(
            config.setup,
            Some("RUN pip install custom-tool".to_string())
        );
        assert_eq!(
            config.stack_config["python"].setup,
            Some("RUN pip install numpy".to_string())
        );
    }

    #[test]
    fn test_load_project_config_missing() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        assert!(load_project_config(temp_dir.path()).is_none());
    }

    #[test]
    fn test_resolve_config_with_project_config_priority() {
        // Test the full 4-layer chain:
        // built-in < defaults < project .tsk/tsk.toml < user [project.<name>]
        let project_config = SharedConfig {
            agent: Some("codex".to_string()),
            memory_gb: Some(20.0),
            cpu: Some(12),
            host_ports: vec![8080],
            env: vec![EnvVar {
                name: "PROJECT_VAR".to_string(),
                value: "from-project-file".to_string(),
            }],
            stack_config: HashMap::from([(
                "python".to_string(),
                StackConfig {
                    setup: Some("RUN pip install project-dep".to_string()),
                },
            )]),
            ..Default::default()
        };

        let config = TskConfig {
            defaults: SharedConfig {
                memory_gb: Some(16.0),
                git_town: Some(true),
                host_ports: vec![5432],
                env: vec![
                    EnvVar {
                        name: "DEFAULT_VAR".to_string(),
                        value: "from-defaults".to_string(),
                    },
                    EnvVar {
                        name: "PROJECT_VAR".to_string(),
                        value: "from-defaults".to_string(),
                    },
                ],
                ..Default::default()
            },
            project: HashMap::from([(
                "my-project".to_string(),
                SharedConfig {
                    agent: Some("claude".to_string()),
                    cpu: Some(16),
                    host_ports: vec![6379],
                    env: vec![EnvVar {
                        name: "USER_VAR".to_string(),
                        value: "from-user-project".to_string(),
                    }],
                    stack_config: HashMap::from([(
                        "python".to_string(),
                        StackConfig {
                            setup: Some("RUN pip install user-dep".to_string()),
                        },
                    )]),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        let resolved = config.resolve_config("my-project", Some(&project_config), None);

        // user [project] overrides project config
        assert_eq!(resolved.agent, "claude");
        // project config overrides defaults
        assert_eq!(resolved.memory_gb, 20.0);
        // user [project] overrides project config
        assert_eq!(resolved.cpu, 16);
        // defaults (no override from project or user project)
        assert!(resolved.git_town);
        // host_ports combined from all layers, deduplicated
        assert!(resolved.host_ports.contains(&5432));
        assert!(resolved.host_ports.contains(&8080));
        assert!(resolved.host_ports.contains(&6379));
        // env: project config overrides PROJECT_VAR from defaults
        assert!(
            resolved
                .env
                .iter()
                .any(|e| e.name == "DEFAULT_VAR" && e.value == "from-defaults")
        );
        assert!(
            resolved
                .env
                .iter()
                .any(|e| e.name == "PROJECT_VAR" && e.value == "from-project-file")
        );
        assert!(
            resolved
                .env
                .iter()
                .any(|e| e.name == "USER_VAR" && e.value == "from-user-project")
        );
        // stack_config: user [project] replaces python setup
        assert_eq!(
            resolved.stack_config["python"].setup,
            Some("RUN pip install user-dep".to_string())
        );
    }

    #[test]
    fn test_resolve_config_project_config_without_user_project() {
        // When there's no user [project.<name>] section, project config should still work
        let project_config = SharedConfig {
            agent: Some("codex".to_string()),
            stack: Some("python".to_string()),
            memory_gb: Some(20.0),
            ..Default::default()
        };

        let config = TskConfig {
            defaults: SharedConfig {
                memory_gb: Some(16.0),
                cpu: Some(4),
                ..Default::default()
            },
            ..Default::default()
        };

        let resolved = config.resolve_config("my-project", Some(&project_config), None);

        // project config overrides defaults for agent and memory
        assert_eq!(resolved.agent, "codex");
        assert_eq!(resolved.stack, "python");
        assert_eq!(resolved.memory_gb, 20.0);
        // defaults still apply for unset fields
        assert_eq!(resolved.cpu, 4);
    }

    #[test]
    fn test_resolved_config_json_round_trip() {
        let config = ResolvedConfig {
            agent: "codex".to_string(),
            stack: "rust".to_string(),
            dind: true,
            memory_gb: 24.0,
            cpu: 16,
            git_town: true,
            host_ports: vec![5432, 6379],
            setup: Some("RUN apt-get install -y cmake".to_string()),
            stack_config: HashMap::from([(
                "rust".to_string(),
                StackConfig {
                    setup: Some("RUN cargo install nextest".to_string()),
                },
            )]),
            agent_config: HashMap::from([(
                "codex".to_string(),
                AgentConfig {
                    setup: Some("RUN pip install tool".to_string()),
                },
            )]),
            volumes: vec![
                VolumeMount::Bind(BindMount {
                    host: "/host/path".to_string(),
                    container: "/container/path".to_string(),
                    readonly: true,
                }),
                VolumeMount::Named(NamedVolume {
                    name: "cache".to_string(),
                    container: "/cache".to_string(),
                    readonly: false,
                }),
            ],
            env: vec![EnvVar {
                name: "DB_URL".to_string(),
                value: "postgres://localhost/db".to_string(),
            }],
            squid_conf: Some("http_port 3128".to_string()),
            review_command: Some("vim {{review_file}}".to_string()),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ResolvedConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.agent, "codex");
        assert_eq!(deserialized.stack, "rust");
        assert!(deserialized.dind);
        assert_eq!(deserialized.memory_gb, 24.0);
        assert_eq!(deserialized.cpu, 16);
        assert!(deserialized.git_town);
        assert_eq!(deserialized.host_ports, vec![5432, 6379]);
        assert_eq!(
            deserialized.setup,
            Some("RUN apt-get install -y cmake".to_string())
        );
        assert_eq!(deserialized.stack_config.len(), 1);
        assert_eq!(
            deserialized.stack_config["rust"].setup,
            Some("RUN cargo install nextest".to_string())
        );
        assert_eq!(deserialized.agent_config.len(), 1);
        assert_eq!(deserialized.volumes.len(), 2);
        assert_eq!(deserialized.env.len(), 1);
        assert_eq!(deserialized.env[0].name, "DB_URL");
        assert_eq!(deserialized.squid_conf, Some("http_port 3128".to_string()));
        assert_eq!(
            deserialized.review_command,
            Some("vim {{review_file}}".to_string())
        );
    }

    #[test]
    fn test_proxy_config_fingerprint_consistent() {
        let proxy = ResolvedProxyConfig {
            host_ports: vec![5432, 6379],
            squid_conf: Some("http_port 3128".to_string()),
        };
        let fp1 = proxy.fingerprint();
        let fp2 = proxy.fingerprint();
        assert_eq!(fp1, fp2);
        assert_eq!(fp1.len(), 8);
        assert!(fp1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_proxy_config_fingerprint_differs_by_host_ports() {
        let a = ResolvedProxyConfig {
            host_ports: vec![5432],
            squid_conf: None,
        };
        let b = ResolvedProxyConfig {
            host_ports: vec![6379],
            squid_conf: None,
        };
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn test_proxy_config_fingerprint_differs_by_squid_conf() {
        let a = ResolvedProxyConfig {
            host_ports: vec![],
            squid_conf: Some("conf-a".to_string()),
        };
        let b = ResolvedProxyConfig {
            host_ports: vec![],
            squid_conf: Some("conf-b".to_string()),
        };
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn test_proxy_config_fingerprint_identical() {
        let a = ResolvedProxyConfig {
            host_ports: vec![6379, 5432],
            squid_conf: Some("http_port 3128".to_string()),
        };
        let b = ResolvedProxyConfig {
            host_ports: vec![5432, 6379],
            squid_conf: Some("http_port 3128".to_string()),
        };
        // Ports are sorted before hashing, so order should not matter
        assert_eq!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn test_proxy_config_container_and_network_names() {
        let proxy = ResolvedProxyConfig {
            host_ports: vec![5432],
            squid_conf: None,
        };
        let fp = proxy.fingerprint();
        assert_eq!(proxy.proxy_container_name(), format!("tsk-proxy-{fp}"));
        assert_eq!(proxy.external_network_name(), format!("tsk-external-{fp}"));
        assert_eq!(proxy.proxy_url(), format!("http://tsk-proxy-{fp}:3128"));
    }

    #[test]
    fn test_proxy_config_host_ports_env() {
        let proxy = ResolvedProxyConfig {
            host_ports: vec![6379, 5432, 3000],
            squid_conf: None,
        };
        // host_ports_env sorts the ports
        assert_eq!(proxy.host_ports_env(), "3000,5432,6379");

        let empty = ResolvedProxyConfig {
            host_ports: vec![],
            squid_conf: None,
        };
        assert_eq!(empty.host_ports_env(), "");
    }

    #[test]
    fn test_squid_conf_inline_resolution() {
        let config = TskConfig {
            defaults: SharedConfig {
                squid_conf: Some("default-squid-conf".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let resolved = config.resolve_config("my-project", None, None);
        assert_eq!(resolved.squid_conf, Some("default-squid-conf".to_string()));
    }

    #[test]
    fn test_squid_conf_path_resolution() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let squid_file = temp_dir.path().join("custom-squid.conf");
        std::fs::write(&squid_file, "http_port 3128\nacl custom src all").unwrap();

        let config = TskConfig {
            defaults: SharedConfig {
                squid_conf_path: Some(squid_file.to_string_lossy().to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let resolved = config.resolve_config("my-project", None, None);
        assert_eq!(
            resolved.squid_conf,
            Some("http_port 3128\nacl custom src all".to_string())
        );
    }

    #[test]
    fn test_squid_conf_project_path_resolution() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let project_root = temp_dir.path();
        let squid_file = project_root.join("proxy.conf");
        std::fs::write(&squid_file, "project-squid-content").unwrap();

        let project_config = SharedConfig {
            squid_conf_path: Some("proxy.conf".to_string()),
            ..Default::default()
        };

        let config = TskConfig::default();
        let resolved =
            config.resolve_config("my-project", Some(&project_config), Some(project_root));
        assert_eq!(
            resolved.squid_conf,
            Some("project-squid-content".to_string())
        );
    }

    #[test]
    fn test_resolved_config_default_squid_conf() {
        let resolved = ResolvedConfig::default();
        assert!(resolved.squid_conf.is_none());
    }

    #[test]
    fn test_resolved_config_proxy_config() {
        let resolved = ResolvedConfig {
            host_ports: vec![5432, 6379],
            squid_conf: Some("custom-conf".to_string()),
            ..Default::default()
        };
        let proxy = resolved.proxy_config();
        assert_eq!(proxy.host_ports, vec![5432, 6379]);
        assert_eq!(proxy.squid_conf, Some("custom-conf".to_string()));
    }

    #[test]
    fn test_squid_conf_project_overrides_defaults() {
        let config = TskConfig {
            defaults: SharedConfig {
                squid_conf: Some("default-conf".to_string()),
                ..Default::default()
            },
            project: HashMap::from([(
                "my-project".to_string(),
                SharedConfig {
                    squid_conf: Some("project-conf".to_string()),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let resolved = config.resolve_config("my-project", None, None);
        assert_eq!(resolved.squid_conf, Some("project-conf".to_string()));
    }

    #[test]
    fn test_squid_conf_inline_wins_over_path_same_layer() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let squid_file = temp_dir.path().join("squid.conf");
        std::fs::write(&squid_file, "file-content").unwrap();

        let config = TskConfig {
            defaults: SharedConfig {
                squid_conf: Some("inline-content".to_string()),
                squid_conf_path: Some(squid_file.to_str().unwrap().to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let resolved = config.resolve_config("test", None, None);
        // Inline squid_conf should win over squid_conf_path
        assert_eq!(resolved.squid_conf, Some("inline-content".to_string()));
    }

    #[test]
    fn test_squid_conf_path_project_overrides_defaults_path() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let default_file = temp_dir.path().join("default-squid.conf");
        let project_file = temp_dir.path().join("project-squid.conf");
        std::fs::write(&default_file, "default-content").unwrap();
        std::fs::write(&project_file, "project-content").unwrap();

        let config = TskConfig {
            defaults: SharedConfig {
                squid_conf_path: Some(default_file.to_str().unwrap().to_string()),
                ..Default::default()
            },
            project: HashMap::from([(
                "my-project".to_string(),
                SharedConfig {
                    squid_conf_path: Some(project_file.to_str().unwrap().to_string()),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let resolved = config.resolve_config("my-project", None, None);
        // User project squid_conf_path should win over defaults squid_conf_path
        assert_eq!(resolved.squid_conf, Some("project-content".to_string()));
    }

    #[tokio::test]
    async fn test_resolve_stack_cli_flag_wins() {
        let config = TskConfig {
            defaults: SharedConfig {
                stack: Some("python".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let tmp = tempfile::tempdir().unwrap();
        let result = resolve_stack(
            Some("go".to_string()),
            &config,
            "my-project",
            None,
            tmp.path(),
        )
        .await;
        assert_eq!(result, "go");
    }

    #[tokio::test]
    async fn test_resolve_stack_config_wins_over_auto_detect() {
        let config = TskConfig {
            defaults: SharedConfig {
                stack: Some("python".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let tmp = tempfile::tempdir().unwrap();
        // Create Cargo.toml so auto-detect would pick "rust"
        std::fs::write(tmp.path().join("Cargo.toml"), "[package]").unwrap();
        let result = resolve_stack(None, &config, "my-project", None, tmp.path()).await;
        assert_eq!(result, "python");
    }

    #[tokio::test]
    async fn test_resolve_stack_auto_detect_when_no_config() {
        let config = TskConfig::default();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("Cargo.toml"), "[package]").unwrap();
        let result = resolve_stack(None, &config, "my-project", None, tmp.path()).await;
        assert_eq!(result, "rust");
    }

    #[tokio::test]
    async fn test_resolve_stack_default_fallback() {
        let config = TskConfig::default();
        let tmp = tempfile::tempdir().unwrap();
        let result = resolve_stack(None, &config, "my-project", None, tmp.path()).await;
        assert_eq!(result, "default");
    }

    #[tokio::test]
    async fn test_resolve_stack_user_project_wins_over_project_file_and_defaults() {
        let config = TskConfig {
            defaults: SharedConfig {
                stack: Some("python".to_string()),
                ..Default::default()
            },
            project: HashMap::from([(
                "my-project".to_string(),
                SharedConfig {
                    stack: Some("go".to_string()),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let project_config = SharedConfig {
            stack: Some("rust".to_string()),
            ..Default::default()
        };
        let tmp = tempfile::tempdir().unwrap();
        let result = resolve_stack(
            None,
            &config,
            "my-project",
            Some(&project_config),
            tmp.path(),
        )
        .await;
        assert_eq!(result, "go");
    }

    #[tokio::test]
    async fn test_resolve_stack_project_file_wins_over_defaults() {
        let config = TskConfig {
            defaults: SharedConfig {
                stack: Some("python".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let project_config = SharedConfig {
            stack: Some("rust".to_string()),
            ..Default::default()
        };
        let tmp = tempfile::tempdir().unwrap();
        let result = resolve_stack(
            None,
            &config,
            "my-project",
            Some(&project_config),
            tmp.path(),
        )
        .await;
        assert_eq!(result, "rust");
    }

    #[test]
    fn test_resolve_agent_cli_flag_wins() {
        let resolved = ResolvedConfig {
            agent: "claude".to_string(),
            ..Default::default()
        };
        assert_eq!(resolve_agent(Some("codex".to_string()), &resolved), "codex");
    }

    #[test]
    fn test_resolve_agent_falls_back_to_config() {
        let resolved = ResolvedConfig {
            agent: "codex".to_string(),
            ..Default::default()
        };
        assert_eq!(resolve_agent(None, &resolved), "codex");
    }

    #[test]
    fn test_resolve_config_review_command() {
        let config = TskConfig {
            defaults: SharedConfig {
                review_command: Some("vim {{review_file}}".to_string()),
                ..Default::default()
            },
            project: HashMap::from([(
                "my-project".to_string(),
                SharedConfig {
                    review_command: Some("code {{review_file}}".to_string()),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        // Defaults apply when no project match
        let resolved_other = config.resolve_config("other-project", None, None);
        assert_eq!(
            resolved_other.review_command,
            Some("vim {{review_file}}".to_string())
        );

        // Project-level config overrides defaults
        let project_config = SharedConfig {
            review_command: Some("nano {{review_file}}".to_string()),
            ..Default::default()
        };
        let resolved_project = config.resolve_config("my-project", Some(&project_config), None);
        // User [project.<name>] overrides project-level config
        assert_eq!(
            resolved_project.review_command,
            Some("code {{review_file}}".to_string())
        );

        // None by default
        let default_config = TskConfig::default();
        let resolved_default = default_config.resolve_config("any", None, None);
        assert!(resolved_default.review_command.is_none());
    }
}
