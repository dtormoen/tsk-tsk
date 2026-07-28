pub mod build_lock_manager;
pub mod composer;
pub mod image_manager;
pub mod layers;
pub mod proxy_manager;
pub mod template_engine;
use crate::agent::task_logger::TaskLogger;
use crate::agent::{Agent, LogProcessor};
use crate::context::AppContext;
use crate::context::ContainerEngine;
use crate::context::VolumeMount;
use crate::context::docker_client::DockerClient;
use crate::context::tsk_config;
use crate::docker::proxy_manager::ProxyManager;
use crate::tui::events::{ServerEvent, ServerEventSender};
use bollard::models::{ContainerCreateBody, DeviceMapping, HostConfig};
use bollard::query_parameters::{LogsOptions, RemoveContainerOptions};
use futures_util::stream::StreamExt;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

const CONTAINER_WORKSPACE_BASE: &str = "/workspace";
const CONTAINER_USER: &str = "agent";
const SECCOMP_DIND_PROFILE: &str = include_str!("seccomp_dind.json");
const PODMAN_STORAGE_PATH: &str = "/home/agent/.local/share/containers/storage";

/// How Podman storage is provided for DIND (Docker-in-Docker) containers.
///
/// Rootless Podman needs a backing filesystem that supports overlay mounts in
/// user namespaces. Not all filesystem types qualify — the kernel requires
/// user.* xattr support on the backing store. We pick the lightest-weight
/// option that works on the current platform:
///
/// 1. **None** (container's own filesystem) — ideal but currently no platform
///    reliably supports overlay-on-overlayfs in user namespaces.
/// 2. **Tmpfs** — ephemeral, no cleanup needed. Works on macOS/Docker Desktop
///    (LinuxKit supports user.* xattrs on tmpfs) but fails on Linux kernels
///    < 6.6 where tmpfs lacks user.* xattr support.
/// 3. **Named volume** — works everywhere (backed by host ext4/xfs). Requires
///    cleanup after the task completes.
///
/// Update the conditions in [`dind_storage_strategy`] as platform support evolves.
enum DindStorage {
    /// No extra mount; use the container's own filesystem.
    #[allow(dead_code)]
    None,
    /// Mount a tmpfs at the storage path (ephemeral, no cleanup).
    Tmpfs,
    /// Mount a named volume at the storage path (cleaned up after the task).
    NamedVolume(String),
}

/// Choose the best DIND storage strategy for the current platform.
fn dind_storage_strategy(task_id: &str) -> DindStorage {
    if cfg!(target_os = "macos") {
        // Docker Desktop / LinuxKit: tmpfs supports user.* xattrs and avoids
        // the execve() bug on FUSE mounts (docker/for-mac#7413).
        DindStorage::Tmpfs
    } else {
        // Linux: tmpfs lacks user.* xattrs on kernels < 6.6, and the
        // container's overlayfs doesn't support nested overlay in userns.
        // Named volume (backed by host ext4/xfs) works on all kernels.
        let kernel_ok = std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .ok()
            .and_then(|r| {
                let mut parts = r.trim().splitn(3, '.');
                let major: u32 = parts.next()?.parse().ok()?;
                let minor: u32 = parts.next()?.parse().ok()?;
                Some(major > 6 || (major == 6 && minor >= 6))
            })
            .unwrap_or(false);
        if kernel_ok {
            DindStorage::Tmpfs
        } else {
            DindStorage::NamedVolume(format!("tsk-dind-{task_id}"))
        }
    }
}

/// Checks whether a cgroup v2 controller (e.g. "cpu", "memory") is delegated
/// to the current user session. Rootless Podman requires controllers to be
/// delegated before it can set resource limits; without this, crun fails with
/// "the requested cgroup controller is not available".
fn cgroup_controller_available(controller: &str) -> bool {
    let uid = unsafe { libc::getuid() };
    // Check the user's systemd service cgroup first, then the user slice
    let paths = [
        format!("/sys/fs/cgroup/user.slice/user-{uid}.slice/user@{uid}.service/cgroup.controllers"),
        format!("/sys/fs/cgroup/user.slice/user-{uid}.slice/cgroup.controllers"),
    ];
    for path in &paths {
        if let Ok(contents) = std::fs::read_to_string(path) {
            return contents.split_whitespace().any(|c| c == controller);
        }
    }
    // Non-cgroup-v2 or non-systemd: assume available (Docker handles this fine)
    true
}

/// Path to the Tailscale startup script installed by the tailscale Docker layer.
const TAILSCALE_STARTUP_SCRIPT: &str = "/usr/local/bin/tsk-tailscale-up";
/// TUN device `tailscaled` uses for kernel networking mode.
const TUN_DEVICE_PATH: &str = "/dev/net/tun";
/// Destinations that must bypass the Squid proxy when Tailscale is enabled:
/// the tailnet CGNAT range (IPv4), the tailnet ULA range (IPv6), and MagicDNS
/// names.
const TAILSCALE_NO_PROXY: &str = "100.64.0.0/10,fd7a:115c:a1e0::/48,.ts.net";
/// SOCKS5 endpoint `tailscaled` exposes in userspace mode (no TUN device), used
/// as `ALL_PROXY` so the tailnet is reachable when kernel routing isn't. Uses
/// `socks5h://` so `tailscaled` resolves tailnet peer names from its netmap
/// (MagicDNS-off notwithstanding); `/etc/hosts` aliases cover the kernel-mode
/// path where no proxy is in play.
const TAILSCALE_USERSPACE_SOCKS5: &str = "socks5h://localhost:1055";

/// Resolves the Tailscale auth key for a task from the configured sources.
///
/// The key is looked up in the environment variable named by
/// `tailscale_auth_key_env` (default `TS_AUTHKEY`), falling back to the contents
/// of `tailscale_auth_key_file`. Keys are read at container start so they are
/// never written to the task's config snapshot.
pub(crate) fn resolve_tailscale_auth_key(
    resolved: &crate::context::ResolvedConfig,
) -> Result<String, String> {
    resolve_tailscale_auth_key_with(resolved, |name| std::env::var(name).ok())
}

/// [`resolve_tailscale_auth_key`] with an injectable environment lookup.
fn resolve_tailscale_auth_key_with(
    resolved: &crate::context::ResolvedConfig,
    env_lookup: impl Fn(&str) -> Option<String>,
) -> Result<String, String> {
    let env_var = resolved.tailscale_auth_key_env_var();

    if let Some(key) = env_lookup(env_var) {
        let key = key.trim().to_string();
        if !key.is_empty() {
            return Ok(key);
        }
    }

    if let Some(ref path) = resolved.tailscale_auth_key_file {
        let path = crate::context::tsk_config::expand_tilde(path);
        let contents = std::fs::read_to_string(&path).map_err(|e| {
            format!(
                "Failed to read tailscale_auth_key_file '{}': {e}",
                path.display()
            )
        })?;
        let key = contents.trim().to_string();
        if key.is_empty() {
            return Err(format!(
                "tailscale_auth_key_file '{}' is empty",
                path.display()
            ));
        }
        return Ok(key);
    }

    Err(format!(
        "Tailscale is enabled but no auth key was found. Set ${env_var} or set \
         tailscale_auth_key_file in tsk.toml."
    ))
}

/// Collects tailnet device name→IP aliases from the host's `tailscale status`,
/// as Docker `ExtraHosts` entries (`name:ip`) written into the container's
/// `/etc/hosts` at creation. This lets a non-root agent reach tailnet devices
/// by name without any in-container privilege. Returns empty if the `tailscale`
/// CLI is missing or errors (non-fatal — names just won't resolve). Skipped
/// under `cfg(test)` so unit tests stay hermetic.
fn tailnet_host_aliases() -> Vec<String> {
    if cfg!(test) {
        return Vec::new();
    }
    let output = match std::process::Command::new("tailscale")
        .args(["status", "--json"])
        .output()
    {
        Ok(o) if o.status.success() => o.stdout,
        _ => return Vec::new(),
    };
    match serde_json::from_slice::<serde_json::Value>(&output) {
        Ok(json) => parse_tailnet_aliases(&json),
        Err(_) => Vec::new(),
    }
}

/// Parses `tailscale status --json` into `name:ip` ExtraHosts entries, emitting
/// both the FQDN and the short host label for each node (Self + Peers).
fn parse_tailnet_aliases(json: &serde_json::Value) -> Vec<String> {
    fn push_node(aliases: &mut Vec<String>, node: &serde_json::Value) {
        let ip = node
            .get("TailscaleIPs")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str());
        let dns = node.get("DNSName").and_then(|v| v.as_str());
        if let (Some(ip), Some(dns)) = (ip, dns) {
            let fqdn = dns.trim_end_matches('.');
            // Defensive: names come from peer-controlled data, so skip anything
            // that isn't a clean DNS name with a parseable IP — a stray `:` or
            // space would otherwise produce a malformed `name:ip` ExtraHosts line
            // that Docker rejects, failing container creation.
            if fqdn.is_empty()
                || ip.parse::<std::net::IpAddr>().is_err()
                || !fqdn
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
            {
                return;
            }
            aliases.push(format!("{fqdn}:{ip}"));
            let short = fqdn.split('.').next().unwrap_or(fqdn);
            if short != fqdn && !short.is_empty() {
                aliases.push(format!("{short}:{ip}"));
            }
        }
    }

    let mut aliases = Vec::new();
    if let Some(self_node) = json.get("Self") {
        push_node(&mut aliases, self_node);
    }
    if let Some(peers) = json.get("Peer").and_then(|v| v.as_object()) {
        for peer in peers.values() {
            push_node(&mut aliases, peer);
        }
    }
    aliases
}

/// Assembles the container's `ExtraHosts`: the proxy container mapping (when
/// network isolation is on) followed by any tailnet device aliases. Returns
/// `None` when there are no entries so the runtime keeps its default `/etc/hosts`.
fn build_extra_hosts(proxy_entry: Option<String>, tailnet_aliases: Vec<String>) -> Option<Vec<String>> {
    let mut hosts = Vec::new();
    if let Some(entry) = proxy_entry {
        hosts.push(entry);
    }
    hosts.extend(tailnet_aliases);
    if hosts.is_empty() {
        None
    } else {
        Some(hosts)
    }
}

/// Prefixes a container command with the Tailscale startup script.
///
/// The sandbox joins the tailnet before the agent starts; if the script fails
/// the container exits rather than running the agent without tailnet access.
/// After the join, `TS_AUTHKEY` is `unset` and the agent command is **`exec`'d**,
/// which replaces the process image *after* the unset so the key is cleared from
/// the kernel-visible `/proc/<pid>/environ`, not just glibc's in-memory `environ`.
/// (A plain `unset` without the exec would leave the key readable in a lingering
/// PID-1 shell whenever the agent command is a pipeline, which it normally is.)
/// The key still lives in the container's `Config.Env` — visible to anyone who
/// can `docker inspect` the container on the host — for the container's lifetime;
/// what this prevents is the *in-container agent* recovering it.
/// An empty command (image default) is returned unchanged.
fn with_tailscale_startup(command: Vec<String>) -> Vec<String> {
    if command.is_empty() {
        return command;
    }

    let prefix =
        format!("{TAILSCALE_STARTUP_SCRIPT} || exit 1\nunset TS_AUTHKEY TSK_TAILSCALE_UP_ARGS");

    // Always `exec` the (quoted) agent command — including the `sh -c <script>`
    // case, which becomes `exec sh -c <script>`. The exec is what guarantees the
    // surviving process's environment is the post-`unset` one.
    let quoted = command
        .iter()
        .map(|arg| shell_quote(arg))
        .collect::<Vec<_>>()
        .join(" ");
    vec![
        "sh".to_string(),
        "-c".to_string(),
        format!("{prefix}\nexec {quoted}"),
    ]
}

/// Wraps an argument in single quotes for safe use inside a `sh -c` script.
fn shell_quote(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', r"'\''"))
}

/// Standard proxy environment variable names forwarded to/from containers.
/// Additional variables like JAVA_TOOL_OPTIONS and TSK_PROXY_HOST are handled separately.
pub(crate) const PROXY_ENV_VARS: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
    "NO_PROXY",
    "no_proxy",
];

fn container_working_dir(project: &str) -> String {
    format!("{CONTAINER_WORKSPACE_BASE}/{project}")
}

/// Resolve config for a task, preferring the DB snapshot over live resolution.
///
/// If the task has a `resolved_config` snapshot (set at creation time), deserialize it.
/// Otherwise, fall back to live resolution from config files (for pre-migration tasks).
pub(crate) fn resolve_config_from_task(
    task: &crate::task::Task,
    ctx: &AppContext,
    event_sender: &Option<crate::tui::events::ServerEventSender>,
) -> crate::context::ResolvedConfig {
    if let Some(ref json) = task.resolved_config {
        match serde_json::from_str(json) {
            Ok(config) => return config,
            Err(e) => {
                crate::tui::events::emit_or_print(
                    event_sender,
                    crate::tui::events::ServerEvent::WarningMessage(format!(
                        "Warning: Failed to deserialize resolved_config for task {}: {e}. Falling back to live resolution.",
                        task.id
                    )),
                );
            }
        }
    }
    // Fallback: live resolution (for tasks created before config snapshotting)
    let project_config = tsk_config::load_project_config(&task.repo_root);
    ctx.tsk_config().resolve_config(
        &task.project,
        project_config.as_ref(),
        Some(&task.repo_root),
    )
}

/// Networking inputs for a task container.
///
/// Groups the proxy wiring with the optional Tailscale auth key so they travel
/// together from proxy acquisition down to container creation.
struct ContainerNetworking<'a> {
    /// Docker network the container joins, when network isolation is enabled
    network_name: Option<&'a str>,
    /// Proxy configuration used for proxy env vars and `extra_hosts`
    proxy_config: Option<&'a crate::context::ResolvedProxyConfig>,
    /// Proxy container IP on the agent network, used for `extra_hosts`
    proxy_ip: Option<&'a str>,
    /// Auth key used to join the tailnet, when Tailscale is enabled
    tailscale_auth_key: Option<&'a str>,
}

/// Manages Docker container execution for TSK tasks.
///
/// This struct handles the lifecycle of task containers including:
/// - Container configuration and creation
/// - Proxy management for network isolation
/// - Log streaming and processing
/// - Container cleanup
pub struct DockerManager {
    ctx: AppContext,
    client: Arc<dyn DockerClient>,
    proxy_manager: ProxyManager,
    event_sender: Option<ServerEventSender>,
}

impl DockerManager {
    /// Creates a new DockerManager with the given application context and Docker client.
    ///
    /// # Arguments
    /// * `ctx` - The application context containing all dependencies
    /// * `client` - The Docker client for container operations
    /// * `event_sender` - Optional TUI event channel for structured output
    pub fn new(
        ctx: &AppContext,
        client: Arc<dyn DockerClient>,
        event_sender: Option<ServerEventSender>,
    ) -> Self {
        let proxy_manager = ProxyManager::new(
            client.clone(),
            ctx.tsk_env(),
            ctx.tsk_config().container_engine.clone(),
            event_sender.clone(),
        );
        Self {
            ctx: ctx.clone(),
            client,
            proxy_manager,
            event_sender,
        }
    }

    /// Route an event through the TUI channel when available, otherwise print directly.
    fn emit(&self, event: ServerEvent) {
        crate::tui::events::emit_or_print(&self.event_sender, event);
    }

    /// Returns the Docker client for use by other components
    pub fn client(&self) -> Arc<dyn DockerClient> {
        Arc::clone(&self.client)
    }

    /// Returns true when TSK is running inside a TSK container with proxy
    /// env vars pointing to `tsk-proxy`.
    ///
    /// In this case, proxy and network isolation are handled by the outer
    /// container's Docker environment, so we skip them for nested containers.
    /// We require proxy env vars to reference `tsk-proxy` to avoid silently
    /// granting unrestricted internet access when `TSK_CONTAINER=1` is set
    /// without a properly configured proxy.
    fn is_nested(&self) -> bool {
        // In tests, always use the normal proxy/network path so mocks work correctly
        if cfg!(test) {
            return false;
        }
        std::env::var("TSK_CONTAINER").is_ok() && Self::has_tsk_proxy_env()
    }

    /// Returns true if at least one proxy env var references the `tsk-proxy` host.
    fn has_tsk_proxy_env() -> bool {
        PROXY_ENV_VARS.iter().any(|var| {
            std::env::var(var)
                .map(|val| val.contains("tsk-proxy"))
                .unwrap_or(false)
        })
    }

    /// Build proxy environment variables for the container.
    ///
    /// When nested inside a TSK container, forwards env vars from the outer container.
    /// Otherwise, sets proxy env vars using the proxy config's container name and URL.
    fn build_proxy_env_vars(
        &self,
        resolved: &crate::context::ResolvedConfig,
        proxy_config: &crate::context::ResolvedProxyConfig,
    ) -> Vec<String> {
        if self.is_nested() {
            // Forward proxy env vars from the outer container's environment.
            // The outer TSK container already has network isolation via Docker.
            let mut env = Vec::new();
            for var in PROXY_ENV_VARS
                .iter()
                .copied()
                .chain(["JAVA_TOOL_OPTIONS", "TSK_PROXY_HOST"])
            {
                if let Ok(val) = std::env::var(var) {
                    env.push(format!("{var}={val}"));
                }
            }
            return env;
        }

        let proxy_url = proxy_config.proxy_url();
        let proxy_container_name = proxy_config.proxy_container_name();

        // Tailscale reachability differs by mode. Docker gets a real TUN device
        // (kernel mode): tailnet IPs are transparently routed, so they bypass the
        // proxy via NO_PROXY. Rootless Podman can't use a TUN, so `tailscaled`
        // runs in userspace mode where the tailnet is reachable ONLY through its
        // SOCKS5 proxy — putting the tailnet in NO_PROXY there forces a routeless
        // direct connection and breaks it. So the bypass is kernel-mode-only, and
        // userspace mode instead points ALL_PROXY at the SOCKS5 listener.
        let tailscale_kernel_mode = resolved.tailscale
            && self.ctx.tsk_config().container_engine == ContainerEngine::Docker;
        let tailscale_userspace_mode = resolved.tailscale && !tailscale_kernel_mode;

        let no_proxy_hosts = if tailscale_kernel_mode {
            format!("localhost,127.0.0.1,{proxy_container_name},{TAILSCALE_NO_PROXY}")
        } else {
            format!("localhost,127.0.0.1,{proxy_container_name}")
        };
        let mut env = vec![
            format!("HTTP_PROXY={proxy_url}"),
            format!("HTTPS_PROXY={proxy_url}"),
            format!("http_proxy={proxy_url}"),
            format!("https_proxy={proxy_url}"),
            format!("NO_PROXY={no_proxy_hosts}"),
            format!("no_proxy={no_proxy_hosts}"),
        ];

        // Userspace mode: reach the tailnet through tailscaled's SOCKS5 proxy.
        // HTTP(S) still go to Squid (HTTP_PROXY takes precedence over ALL_PROXY),
        // so only tailnet/other-scheme traffic rides the SOCKS5 listener.
        if tailscale_userspace_mode {
            env.push(format!("ALL_PROXY={TAILSCALE_USERSPACE_SOCKS5}"));
            env.push(format!("all_proxy={TAILSCALE_USERSPACE_SOCKS5}"));
        }

        // JVM proxy system properties via JAVA_TOOL_OPTIONS
        // Maven and Gradle ignore HTTP_PROXY env vars, so this ensures all JVM
        // processes route through the proxy. Harmless for non-Java containers.
        env.push(format!(
            "JAVA_TOOL_OPTIONS=-Dhttp.proxyHost={pcn} -Dhttp.proxyPort=3128 \
             -Dhttps.proxyHost={pcn} -Dhttps.proxyPort=3128 \
             -Dhttp.nonProxyHosts=localhost|127.0.0.1 \
             -Dhttps.nonProxyHosts=localhost|127.0.0.1",
            pcn = proxy_container_name
        ));

        // Always export the proxy container name so scripts (e.g. network
        // isolation tests) can reach the proxy regardless of host_ports config.
        env.push(format!("TSK_PROXY_HOST={proxy_container_name}"));

        // Add host port environment variables if configured
        if resolved.has_host_ports() {
            env.push(format!("TSK_HOST_PORTS={}", resolved.host_ports_env()));
        }

        env
    }

    /// Remove a container with force option
    async fn remove_container(&self, container_id: &str) -> Result<(), String> {
        self.client
            .remove_container(
                container_id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await
            .map_err(|e| e.to_string())
    }

    /// Build bind volumes for container
    fn build_bind_volumes(
        &self,
        task: &crate::task::Task,
        agent: &dyn Agent,
        resolved: &crate::context::ResolvedConfig,
    ) -> Vec<String> {
        let repo_path = task
            .copied_repo_path
            .as_ref()
            .expect("Task must have copied_repo_path set before container execution");
        let repo_path_str = repo_path
            .to_str()
            .expect("Repository path should be valid UTF-8");
        let working_dir = container_working_dir(&task.project);
        let mut binds = vec![format!("{repo_path_str}:{working_dir}")];

        // Add agent-specific volumes
        for (host_path, container_path, options) in agent.volumes() {
            let bind = if options.is_empty() {
                format!("{host_path}:{container_path}")
            } else {
                format!("{host_path}:{container_path}:{options}")
            };
            binds.push(bind);
        }

        // Add instructions directory mount
        let instructions_file_path = PathBuf::from(&task.instructions_file);
        if let Some(parent) = instructions_file_path.parent() {
            let abs_parent = parent
                .canonicalize()
                .unwrap_or_else(|_| parent.to_path_buf());
            binds.push(format!("{}:/instructions:ro", abs_parent.to_string_lossy()));
        }

        // Add output directory mount
        if let Some(task_dir) = repo_path.parent() {
            let output_dir = task_dir.join("output");
            binds.push(format!("{}:/output", output_dir.to_string_lossy()));
        }

        // Add volume mounts from resolved config (already merged from defaults + project)
        for volume in &resolved.volumes {
            match volume {
                VolumeMount::Bind(bind) => {
                    if let Ok(host_path) = bind.expanded_host_path() {
                        let bind_str = if bind.readonly {
                            format!("{}:{}:ro", host_path.display(), bind.container)
                        } else {
                            format!("{}:{}", host_path.display(), bind.container)
                        };
                        binds.push(bind_str);
                    }
                }
                VolumeMount::Named(named) => {
                    let volume_name = format!("tsk-{}", named.name);
                    let bind_str = if named.readonly {
                        format!("{volume_name}:{}:ro", named.container)
                    } else {
                        format!("{volume_name}:{}", named.container)
                    };
                    binds.push(bind_str);
                }
            }
        }

        binds
    }

    /// Generate container name based on task mode
    fn build_container_name(&self, task: &crate::task::Task) -> String {
        if task.is_interactive {
            format!("tsk-interactive-{}", task.id)
        } else {
            format!("tsk-{}", task.id)
        }
    }

    /// Create a container configuration for both interactive and non-interactive modes.
    ///
    /// # Arguments
    /// * `image` - The Docker image to use
    /// * `task` - The task containing all necessary configuration
    /// * `agent` - The agent to get volumes and environment variables from
    /// * `networking` - Proxy wiring and Tailscale auth key for the container
    fn create_container_config(
        &self,
        image: &str,
        task: &crate::task::Task,
        agent: &dyn Agent,
        networking: &ContainerNetworking<'_>,
    ) -> (ContainerCreateBody, DindStorage) {
        let ContainerNetworking {
            network_name,
            proxy_config,
            proxy_ip: proxy_container_ip,
            tailscale_auth_key,
        } = *networking;
        let resolved = resolve_config_from_task(task, &self.ctx, &self.event_sender);
        let mut binds = self.build_bind_volumes(task, agent, &resolved);
        let instructions_file_path = PathBuf::from(&task.instructions_file);
        let working_dir = container_working_dir(&task.project);

        let mut env_vars = if let Some(pc) = proxy_config {
            self.build_proxy_env_vars(&resolved, pc)
        } else if self.is_nested() {
            // Nested mode: use a default proxy config to forward env vars
            let default_pc = crate::context::ResolvedProxyConfig::default();
            self.build_proxy_env_vars(&resolved, &default_pc)
        } else {
            Vec::new()
        };

        // Add TSK environment variables for container detection
        env_vars.push("TSK_CONTAINER=1".to_string());
        env_vars.push(format!("TSK_TASK_ID={}", task.id));

        // Add agent-specific environment variables
        for (key, value) in agent.environment() {
            env_vars.push(format!("{key}={value}"));
        }

        // Add environment variables from resolved config (already merged from defaults + project)
        for env_var in &resolved.env {
            env_vars.push(format!("{}={}", env_var.name, env_var.value));
        }

        // Use chroot isolation for Podman/Buildah builds inside DIND containers.
        // Full OCI isolation fails in nested user namespaces because the kernel
        // denies devpts mounts (Permission denied). Chroot isolation avoids
        // creating new namespaces or mounting devpts/proc/sysfs during RUN steps,
        // which is safe since we're already inside a container.
        if task.dind {
            env_vars.push("BUILDAH_ISOLATION=chroot".to_string());
        }

        // Tailscale: pass the auth key and node settings to the startup script.
        // The key comes from the host environment or a key file, never from the
        // task's stored config snapshot.
        let tailscale_enabled = resolved.tailscale && tailscale_auth_key.is_some();
        if let Some(auth_key) = tailscale_auth_key.filter(|_| resolved.tailscale) {
            env_vars.push(format!("TS_AUTHKEY={auth_key}"));
            env_vars.push(format!(
                "TSK_TAILSCALE_HOSTNAME={}",
                resolved.tailscale_hostname_for(&task.id)
            ));
            // Sets the *initial* accept-routes posture (default off, so the join
            // reaches tailnet nodes but not advertised subnet routes, which would
            // bypass the Squid allowlist). This is defense-in-depth, not an
            // enforced boundary: the agent owns the tailscaled socket and can
            // re-enable routes at runtime — the real boundary is your ACLs. Opt
            // in with `tailscale_accept_routes = true`.
            env_vars.push(format!(
                "TSK_TAILSCALE_ACCEPT_ROUTES={}",
                resolved.tailscale_accept_routes
            ));
            if let Some(ref up_args) = resolved.tailscale_up_args {
                env_vars.push(format!("TSK_TAILSCALE_UP_ARGS={up_args}"));
            }
        }

        let agent_command = agent.build_command(
            instructions_file_path.to_str().unwrap_or("instructions.md"),
            task.is_interactive,
        );

        let command = if agent_command.is_empty() {
            None
        } else if tailscale_enabled {
            Some(with_tailscale_startup(agent_command))
        } else {
            Some(agent_command)
        };

        let container_engine = &self.ctx.tsk_config().container_engine;

        // Build device mappings from resolved config, expanding glob patterns
        let mut device_mappings: Vec<DeviceMapping> = Vec::new();
        for pattern in &resolved.devices {
            if pattern.contains('*') || pattern.contains('?') || pattern.contains('[') {
                if let Ok(paths) = glob::glob(pattern) {
                    for entry in paths.flatten() {
                        let path_str = entry.to_string_lossy().to_string();
                        device_mappings.push(DeviceMapping {
                            path_on_host: Some(path_str.clone()),
                            path_in_container: Some(path_str),
                            cgroup_permissions: Some("rwm".to_string()),
                        });
                    }
                }
            } else {
                device_mappings.push(DeviceMapping {
                    path_on_host: Some(pattern.clone()),
                    path_in_container: Some(pattern.clone()),
                    cgroup_permissions: Some("rwm".to_string()),
                });
            }
        }

        // Map a TUN device for transparent kernel-mode networking, using the
        // SAME condition that drives the proxy-env mode decision (Docker + a
        // host TUN) so the two never disagree. Rootless Podman can't hand a
        // usable TUN to the container even when the host has one, so it is
        // excluded here and (correctly) runs userspace mode with `ALL_PROXY`.
        if tailscale_enabled
            && *container_engine == ContainerEngine::Docker
            && std::path::Path::new(TUN_DEVICE_PATH).exists()
            && !device_mappings
                .iter()
                .any(|d| d.path_in_container.as_deref() == Some(TUN_DEVICE_PATH))
        {
            device_mappings.push(DeviceMapping {
                path_on_host: Some(TUN_DEVICE_PATH.to_string()),
                path_in_container: Some(TUN_DEVICE_PATH.to_string()),
                cgroup_permissions: Some("rwm".to_string()),
            });
        }

        let devices = if device_mappings.is_empty() {
            None
        } else {
            Some(device_mappings)
        };

        // Security relaxations: seccomp/AppArmor for DIND, SETUID/SETGID for DIND or sudo
        let security_opt = if task.dind {
            let mut security_opts = if *container_engine == ContainerEngine::Podman {
                let seccomp_path = self.ctx.tsk_env().config_dir().join("seccomp_dind.json");
                if std::fs::write(&seccomp_path, SECCOMP_DIND_PROFILE).is_ok() {
                    vec![format!("seccomp={}", seccomp_path.display())]
                } else {
                    vec!["seccomp=unconfined".to_string()]
                }
            } else {
                vec![format!("seccomp={SECCOMP_DIND_PROFILE}")]
            };
            security_opts.push("apparmor=unconfined".to_string());
            // Podman-in-Podman: disable SELinux confinement so nested Podman
            // can fchown stdio file descriptors. Without this, SELinux's
            // container_t context blocks fchown with EACCES, which crun treats
            // as fatal (unlike EPERM which crun ignores). Not needed for
            // Docker-in-Podman since Docker does not apply SELinux container_t.
            if *container_engine == ContainerEngine::Podman {
                security_opts.push("label=disable".to_string());
            }
            Some(security_opts)
        } else {
            None
        };

        // Tailscale's tailscaled needs NET_ADMIN to configure the TUN interface
        // and tailnet routes, so it is granted instead of dropped when enabled.
        let mut cap_drop = vec![
            "SETPCAP".to_string(),
            "SYS_ADMIN".to_string(),
            "SYS_PTRACE".to_string(),
            "DAC_OVERRIDE".to_string(),
            "AUDIT_WRITE".to_string(),
        ];
        let cap_add = if tailscale_enabled {
            Some(vec!["NET_ADMIN".to_string()])
        } else {
            cap_drop.insert(0, "NET_ADMIN".to_string());
            None
        };
        if !task.dind && !resolved.sudo {
            cap_drop.push("SETUID".to_string());
            cap_drop.push("SETGID".to_string());
        }
        if network_name.is_some() {
            cap_drop.push("NET_RAW".to_string());
        }

        // Podman storage: rootless Podman needs a backing filesystem that supports
        // overlay mounts in user namespaces. The container's own overlayfs doesn't
        // qualify, so we provide a tmpfs (or named volume on older Linux kernels).
        // Only needed when Podman will actually run inside the container:
        //   - DIND tasks: user explicitly requested container builds
        //   - Nested containers: inner tsk uses Podman as its container engine
        let has_storage_volume = binds
            .iter()
            .any(|b| b.split(':').nth(1) == Some(PODMAN_STORAGE_PATH));
        let needs_podman_storage = (task.dind || self.is_nested()) && !has_storage_volume;
        let dind_storage = if needs_podman_storage {
            let strategy = dind_storage_strategy(&task.id);
            if let DindStorage::NamedVolume(ref name) = strategy {
                binds.push(format!("{name}:{PODMAN_STORAGE_PATH}"));
            }
            strategy
        } else {
            DindStorage::None
        };

        let config = ContainerCreateBody {
            image: Some(image.to_string()),
            user: Some(CONTAINER_USER.to_string()),
            cmd: command,
            host_config: Some(HostConfig {
                binds: Some(binds),
                // In nested containers, use host networking so the inner container
                // shares the outer container's network namespace and can resolve
                // `tsk-proxy` via Docker's embedded DNS. Without this, Podman's
                // `netns = "none"` in containers.conf gives the inner container
                // no network, causing DNS failures and proxy connection errors.
                network_mode: if self.is_nested() {
                    Some("host".to_string())
                } else {
                    network_name.map(|n| n.to_string())
                },
                memory: if self.is_nested()
                    || (*container_engine == ContainerEngine::Podman
                        && !cgroup_controller_available("memory"))
                {
                    None
                } else {
                    Some(resolved.memory_limit_bytes())
                },
                cpu_quota: if self.is_nested()
                    || (*container_engine == ContainerEngine::Podman
                        && !cgroup_controller_available("cpu"))
                {
                    None
                } else {
                    Some(resolved.cpu_quota_microseconds())
                },
                privileged: if resolved.privileged {
                    Some(true)
                } else {
                    None
                },
                devices,
                cap_add,
                cap_drop: Some(cap_drop),
                security_opt,
                tmpfs: if matches!(dind_storage, DindStorage::Tmpfs) {
                    Some(HashMap::from([(
                        PODMAN_STORAGE_PATH.to_string(),
                        "size=40G,mode=1777".to_string(),
                    )]))
                } else {
                    None
                },
                // Keep the host UID mapping in Podman so bind-mounted files retain
                // correct ownership (rootless Podman remaps UIDs otherwise).
                userns_mode: if *container_engine == ContainerEngine::Podman {
                    Some("keep-id".to_string())
                } else {
                    None
                },
                extra_hosts: build_extra_hosts(
                    match (proxy_config, proxy_container_ip) {
                        (Some(pc), Some(ip)) => Some(format!("{}:{}", pc.proxy_container_name(), ip)),
                        _ => None,
                    },
                    // Tailnet device name→IP aliases (snapshotted from the host's
                    // view) so the agent can reach tailnet hosts by name.
                    if tailscale_enabled && resolved.tailscale_host_aliases {
                        tailnet_host_aliases()
                    } else {
                        Vec::new()
                    },
                ),
                ..Default::default()
            }),
            working_dir: Some(working_dir),
            env: Some(env_vars),
            attach_stdin: Some(task.is_interactive),
            attach_stdout: Some(task.is_interactive),
            attach_stderr: Some(task.is_interactive),
            tty: Some(task.is_interactive),
            open_stdin: Some(task.is_interactive),
            ..Default::default()
        };

        (config, dind_storage)
    }

    /// Run a task container with unified support for both interactive and non-interactive modes.
    ///
    /// Follows a setup → execute → cleanup structure:
    /// 1. **Setup**: Conditionally create an isolated network and connect the proxy
    /// 2. **Execute**: Create, configure, and run the container via [`run_container_inner`]
    /// 3. **Cleanup**: Always remove the container, tear down the network, and stop
    ///    the proxy if idle — regardless of whether execution succeeded or failed
    ///
    /// # Arguments
    /// * `docker_image_tag` - Docker image tag to use
    /// * `task` - The task to execute
    /// * `agent` - The agent to use for the task
    ///
    /// # Returns
    /// * `Ok((output, task_result))` - The container output and synthesized task result
    /// * `Err(String)` - Error message if container infrastructure fails
    pub async fn run_task_container(
        &self,
        docker_image_tag: &str,
        task: &crate::task::Task,
        agent: &dyn Agent,
    ) -> Result<(String, crate::agent::TaskResult), String> {
        // --- Setup: atomically acquire proxy session ---
        // When nested inside a TSK container, skip proxy/network setup since the
        // outer container already provides network isolation.
        let resolved = resolve_config_from_task(task, &self.ctx, &self.event_sender);
        let proxy_config = resolved.proxy_config();

        // Resolve the Tailscale auth key before any container work so a missing
        // key fails fast with an actionable message.
        let tailscale_auth_key = if resolved.tailscale {
            Some(resolve_tailscale_auth_key(&resolved)?)
        } else {
            None
        };

        let proxy_session = if task.network_isolation && !self.is_nested() {
            let suppress_stdout = self.event_sender.is_some();
            let proxy_logger = TaskLogger::from_path(
                &self
                    .ctx
                    .tsk_env()
                    .task_dir(&task.id)
                    .join("output")
                    .join("agent.log"),
                suppress_stdout,
            );
            match self
                .proxy_manager
                .acquire_proxy(&task.id, &proxy_config, &proxy_logger)
                .await
            {
                Ok(session) => Some(session),
                Err(e) => {
                    return Err(format!(
                        "Failed to ensure proxy is running and healthy: {e}. \
                        The task should be retried later when the proxy is available. \
                        Check the status in Docker."
                    ));
                }
            }
        } else {
            None
        };

        // --- Execute: run the container, capturing its ID for cleanup ---
        let (container_id, dind_storage, result) = self
            .run_container_inner(
                docker_image_tag,
                task,
                agent,
                &ContainerNetworking {
                    network_name: proxy_session.as_ref().map(|s| s.network_name.as_str()),
                    proxy_config: proxy_session.as_ref().map(|_| &proxy_config),
                    proxy_ip: proxy_session.as_ref().and_then(|s| s.proxy_ip.as_deref()),
                    tailscale_auth_key: tailscale_auth_key.as_deref(),
                },
            )
            .await;

        // --- Cleanup: always runs regardless of success/failure ---
        if let Some(ref id) = container_id {
            let _ = self.remove_container(id).await;
        }
        if let Some(ref session) = proxy_session {
            self.proxy_manager.release_proxy(session).await;
        }
        if let DindStorage::NamedVolume(ref name) = dind_storage {
            let _ = self.client.remove_volume(name).await;
        }

        result
    }

    /// Execute the container lifecycle without performing resource cleanup.
    ///
    /// Returns the container ID (if one was created) and the DIND storage strategy
    /// alongside the execution result, so the caller can clean up everything in
    /// one place.
    async fn run_container_inner(
        &self,
        docker_image_tag: &str,
        task: &crate::task::Task,
        agent: &dyn Agent,
        networking: &ContainerNetworking<'_>,
    ) -> (
        Option<String>,
        DindStorage,
        Result<(String, crate::agent::TaskResult), String>,
    ) {
        let suppress_stdout = self.event_sender.is_some();
        let (config, dind_storage) =
            self.create_container_config(docker_image_tag, task, agent, networking);
        let container_name = self.build_container_name(task);
        let options = bollard::query_parameters::CreateContainerOptionsBuilder::default()
            .name(&container_name)
            .build();

        let container_id = match self.client.create_container(Some(options), config).await {
            Ok(id) => id,
            Err(e) => return (None, dind_storage, Err(e)),
        };

        // Copy agent files into container before starting
        for (tar_data, dest_path) in agent.files_to_copy() {
            if let Err(e) = self
                .client
                .upload_to_container(&container_id, &dest_path, tar_data)
                .await
            {
                return (Some(container_id), dind_storage, Err(e));
            }
        }

        if let Err(e) = self.client.start_container(&container_id).await {
            return (Some(container_id), dind_storage, Err(e));
        }

        let result = if task.is_interactive {
            println!("\nStarting interactive session...");

            match self.client.attach_container(&container_id).await {
                Ok(()) => {
                    println!("\nInteractive session ended");
                    Ok((
                        String::new(),
                        crate::agent::TaskResult {
                            success: true,
                            message: "Interactive session completed".to_string(),
                            cost_usd: None,
                            duration_ms: None,
                        },
                    ))
                }
                Err(e) => {
                    eprintln!("Interactive session ended with error: {e}");
                    Err(e)
                }
            }
        } else {
            // Open the log file in append mode (task_runner already created it with infrastructure logs)
            let log_file = match self.ctx.tsk_env().open_agent_log(&task.id) {
                Ok(file) => Some(file),
                Err(e) => {
                    self.emit(ServerEvent::WarningMessage(format!(
                        "Warning: Failed to open agent log file: {e}"
                    )));
                    None
                }
            };

            let mut log_processor = agent.create_log_processor(Some(task));
            let output = self
                .stream_container_logs(
                    &container_id,
                    &mut *log_processor,
                    log_file,
                    suppress_stdout,
                )
                .await;
            let task_result = log_processor.get_final_result().cloned();

            match output {
                Ok((output, exit_code)) => {
                    let task_result = match (exit_code, task_result) {
                        // Non-zero exit code: always failure
                        (code, Some(mut r)) if code != 0 => {
                            r.success = false;
                            r
                        }
                        (code, None) if code != 0 => crate::agent::TaskResult {
                            success: false,
                            message: format!("Container exited with status {code}"),
                            cost_usd: None,
                            duration_ms: None,
                        },
                        // Zero exit code: use agent result if available
                        (_, Some(r)) => r,
                        // Zero exit code, no agent result: success
                        (_, None) => crate::agent::TaskResult {
                            success: true,
                            message: "Task completed".to_string(),
                            cost_usd: None,
                            duration_ms: None,
                        },
                    };
                    Ok((output, task_result))
                }
                Err(e) => Err(e),
            }
        };

        (Some(container_id), dind_storage, result)
    }

    /// Stream container logs and process them through the log processor.
    ///
    /// Returns the accumulated log output and the container's exit code.
    /// `Err` is reserved for Docker API / infrastructure failures only.
    async fn stream_container_logs(
        &self,
        container_id: &str,
        log_processor: &mut dyn LogProcessor,
        log_file: Option<std::fs::File>,
        suppress_stdout: bool,
    ) -> Result<(String, i64), String> {
        let mut log_file = log_file;
        // Start a background task to stream logs
        let client_clone = Arc::clone(&self.client);
        let container_id_clone = container_id.to_string();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(100);
        let log_event_sender = self.event_sender.clone();

        let log_task = tokio::spawn(async move {
            let log_options = LogsOptions {
                stdout: true,
                stderr: true,
                follow: true,
                timestamps: false,
                ..Default::default()
            };

            match client_clone
                .logs_stream(&container_id_clone, Some(log_options))
                .await
            {
                Ok(mut stream) => {
                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(log_line) => {
                                if tx.send(log_line).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                crate::tui::events::emit_or_print(
                                    &log_event_sender,
                                    crate::tui::events::ServerEvent::WarningMessage(format!(
                                        "Error streaming logs: {e}"
                                    )),
                                );
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    crate::tui::events::emit_or_print(
                        &log_event_sender,
                        crate::tui::events::ServerEvent::WarningMessage(format!(
                            "Failed to start log streaming: {e}"
                        )),
                    );
                }
            }
        });

        // Collect all logs for return value
        let mut all_logs = String::new();

        // Buffer for accumulating partial lines from Docker chunks
        let mut line_buffer = String::new();

        // Get docker client to avoid temporary value issues
        let docker_client = Arc::clone(&self.client);

        // Create the wait future ONCE and pin it so it persists across loop iterations.
        // This is critical - tokio::select! in a loop drops unselected futures each iteration,
        // so without pinning, wait_container would be called multiple times.
        let wait_future = docker_client.wait_container(container_id);
        tokio::pin!(wait_future);

        // Process logs while container is running
        loop {
            tokio::select! {
                Some(log_chunk) = rx.recv() => {
                    // Keep raw chunks in all_logs for full log capture
                    all_logs.push_str(&log_chunk);

                    // Buffer chunks and process complete lines only
                    line_buffer.push_str(&log_chunk);
                    process_complete_lines(&mut line_buffer, log_processor, log_file.as_mut(), suppress_stdout, &self.event_sender);
                }
                exit_code = &mut wait_future => {
                    let exit_code = exit_code?;

                    // Give a bit of time for remaining logs
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                    // Drain any remaining logs
                    while let Ok(log_chunk) = rx.try_recv() {
                        all_logs.push_str(&log_chunk);
                        line_buffer.push_str(&log_chunk);
                        process_complete_lines(&mut line_buffer, log_processor, log_file.as_mut(), suppress_stdout, &self.event_sender);
                    }

                    // Flush remaining buffer content if non-empty
                    if !line_buffer.trim().is_empty() {
                        line_buffer.push('\n');
                        process_complete_lines(&mut line_buffer, log_processor, log_file.as_mut(), suppress_stdout, &self.event_sender);
                    }

                    // Abort the log task
                    log_task.abort();

                    return Ok((all_logs, exit_code));
                }
            }
        }
    }
}

/// Process complete lines from the buffer and pass them to the log processor.
///
/// This function extracts all complete lines (terminated by newline) from the buffer,
/// processes each through the log processor, and removes them from the buffer.
/// Any partial line (without a trailing newline) remains in the buffer.
///
/// Log lines are serialized as JSON-lines to the log file (for TUI consumption)
/// and rendered as plain text to stdout (for non-TUI mode).
fn process_complete_lines(
    line_buffer: &mut String,
    log_processor: &mut dyn LogProcessor,
    mut log_file: Option<&mut std::fs::File>,
    suppress_stdout: bool,
    event_sender: &Option<ServerEventSender>,
) {
    while let Some(newline_pos) = line_buffer.find('\n') {
        let complete_line = &line_buffer[..newline_pos];
        // Handle CRLF by trimming trailing \r
        let trimmed = complete_line.trim_end_matches('\r');

        if let Some(log_line) = log_processor.process_line(trimmed) {
            if !suppress_stdout {
                println!("{log_line}");
            }
            if let Some(ref mut file) = log_file {
                // Serialize as JSON-lines for structured TUI consumption
                match serde_json::to_string(&log_line) {
                    Ok(json) => {
                        if let Err(e) = writeln!(file, "{json}") {
                            crate::tui::events::emit_or_print(
                                event_sender,
                                ServerEvent::WarningMessage(format!(
                                    "Warning: Failed to write to agent log file: {e}"
                                )),
                            );
                        }
                    }
                    Err(e) => {
                        crate::tui::events::emit_or_print(
                            event_sender,
                            ServerEvent::WarningMessage(format!(
                                "Warning: Failed to serialize log line: {e}"
                            )),
                        );
                    }
                }
            }
        }

        // Use drain() for efficient in-place removal
        line_buffer.drain(..=newline_pos);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::AppContext;
    use crate::context::ResolvedProxyConfig;
    use crate::task::{Task, TaskStatus};
    use crate::test_utils::TrackedDockerClient;
    use std::sync::Arc;

    /// Returns the proxy container name for the default proxy config (no host_ports, no squid_conf).
    fn default_proxy_container_name() -> String {
        ResolvedProxyConfig::default().proxy_container_name()
    }

    fn create_test_task(is_interactive: bool) -> Task {
        let repo_path = PathBuf::from("/tmp/test-repo");
        Task {
            id: "test-task-id".to_string(),
            repo_root: repo_path.clone(),
            task_type: "feature".to_string(),
            instructions_file: "/tmp/test-repo/.tsk/tasks/instructions.md".to_string(),
            status: TaskStatus::Running,
            started_at: Some(chrono::Utc::now()),
            branch_name: "tsk/feature/test-task/test-task-id".to_string(),
            copied_repo_path: Some(repo_path),
            is_interactive,
            ..Task::test_default()
        }
    }

    #[tokio::test]
    async fn test_run_task_container_success() {
        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(false);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_ok());
        let (output, task_result) = result.unwrap();
        assert_eq!(output, "Container logs");
        assert!(task_result.success);
        assert_eq!(task_result.message, "Task completed");

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        assert_eq!(create_calls.len(), 2); // One for proxy, one for task container

        // Check that the command includes the agent command
        let task_container_config = &create_calls[1].1;
        let actual_cmd = task_container_config.cmd.as_ref().unwrap();
        // Command should be sh -c with the agent command
        assert_eq!(actual_cmd.len(), 3);
        assert_eq!(actual_cmd[0], "sh");
        assert_eq!(actual_cmd[1], "-c");
        assert!(actual_cmd[2].contains("claude"));

        // Check that user is set
        assert_eq!(task_container_config.user, Some(CONTAINER_USER.to_string()));

        // Check proxy environment variables (uses fingerprinted proxy container name)
        let pcn = default_proxy_container_name();
        let env = task_container_config.env.as_ref().unwrap();
        assert!(env.contains(&format!("HTTP_PROXY=http://{pcn}:3128")));
        assert!(env.contains(&format!("HTTPS_PROXY=http://{pcn}:3128")));
        assert!(
            env.iter().any(|e| e.starts_with("JAVA_TOOL_OPTIONS=")),
            "JAVA_TOOL_OPTIONS should be set for proxy"
        );

        // Check TSK environment variables
        assert!(env.contains(&"TSK_CONTAINER=1".to_string()));
        assert!(env.contains(&"TSK_TASK_ID=test-task-id".to_string()));
        drop(create_calls); // Release the lock

        let start_calls = mock_client.start_container_calls.lock().unwrap();
        assert_eq!(start_calls.len(), 2); // One for proxy, one for task container
        assert_eq!(start_calls[0], pcn);
        assert_eq!(start_calls[1], "test-container-id-1");

        let wait_calls = mock_client.wait_container_calls.lock().unwrap();
        assert_eq!(wait_calls.len(), 1);
        assert_eq!(wait_calls[0], "test-container-id-1");

        let remove_calls = mock_client.remove_container_calls.lock().unwrap();
        assert_eq!(remove_calls.len(), 2); // One for task container, one for proxy
        assert_eq!(remove_calls[0].0, "test-container-id-1");
        assert_eq!(remove_calls[1].0, pcn);
        drop(remove_calls);

        // Verify upload_to_container was called for agent files
        // Note: In tests, claude.json may or may not exist, so we just verify it was called
        // if the agent has files to copy
        let upload_calls = mock_client.upload_to_container_calls.lock().unwrap();
        // The number of calls depends on whether .claude.json exists in test environment
        // For this test, just verify the method was callable
        for (container_id, dest_path, _tar_data) in upload_calls.iter() {
            assert_eq!(container_id, "test-container-id-1");
            assert_eq!(dest_path, "/home/agent");
        }
    }

    #[tokio::test]
    async fn test_run_task_container_interactive() {
        // Test interactive mode with mock client
        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(true);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        // Interactive mode should succeed with mock client
        assert!(result.is_ok());
        let (output, task_result) = result.unwrap();
        // Interactive mode returns empty output and a success task result
        assert_eq!(output, "");
        assert!(task_result.success);
        assert_eq!(task_result.message, "Interactive session completed");

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        assert_eq!(create_calls.len(), 2); // One for proxy, one for task container

        // Check task container config for interactive mode
        let (options, config) = &create_calls[1];
        assert_eq!(
            options.as_ref().unwrap().name,
            Some("tsk-interactive-test-task-id".to_string())
        );

        // Check interactive-specific settings
        assert_eq!(config.attach_stdin, Some(true));
        assert_eq!(config.attach_stdout, Some(true));
        assert_eq!(config.attach_stderr, Some(true));
        assert_eq!(config.tty, Some(true));
        assert_eq!(config.open_stdin, Some(true));

        // Check TSK environment variables are present in interactive mode too
        let env = config.env.as_ref().unwrap();
        assert!(env.contains(&"TSK_CONTAINER=1".to_string()));
        assert!(env.contains(&"TSK_TASK_ID=test-task-id".to_string()));

        // Verify attach_container was called (indirectly through start_container_calls)
        let start_calls = mock_client.start_container_calls.lock().unwrap();
        assert_eq!(start_calls.len(), 2); // One for proxy, one for task container

        // Verify cleanup happened
        let remove_calls = mock_client.remove_container_calls.lock().unwrap();
        assert_eq!(remove_calls.len(), 2); // Task container + proxy
        assert_eq!(remove_calls[0].0, "test-container-id-1");
    }

    #[tokio::test]
    async fn test_run_task_container_non_zero_exit() {
        // Set up a mock client that will return a non-zero exit code
        let mock_client = Arc::new(TrackedDockerClient {
            exit_code: 1,
            ..Default::default()
        });
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(false);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());

        // Run the task container
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        // Non-zero exit is not an infrastructure error; it returns Ok with a failed TaskResult
        assert!(result.is_ok());
        let (output, task_result) = result.unwrap();
        assert!(!task_result.success);
        assert!(
            task_result
                .message
                .contains("Container exited with status 1")
        );
        assert!(output.contains("Container logs"));

        // Cleanup still happens
        let remove_calls = mock_client.remove_container_calls.lock().unwrap();
        assert_eq!(remove_calls.len(), 2); // Task container + proxy
        assert_eq!(remove_calls[0].0, "test-container-id-1");
        drop(remove_calls);

        // Network cleanup should also happen
        let disconnect_calls = mock_client.disconnect_network_calls.lock().unwrap();
        assert_eq!(disconnect_calls.len(), 1);
        drop(disconnect_calls);

        let remove_network_calls = mock_client.remove_network_calls.lock().unwrap();
        assert_eq!(remove_network_calls.len(), 1);
    }

    #[tokio::test]
    async fn test_run_task_container_network_setup_fails() {
        let mock_client = TrackedDockerClient {
            network_exists: false,
            create_network_error: Some("Docker daemon not running".to_string()),
            ..Default::default()
        };
        let mock_client = Arc::new(mock_client);
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(false);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        // The error message should indicate proxy startup failure
        assert!(error_msg.contains("Failed to ensure proxy is running and healthy"));
        // The error chain should include the network creation failure
        assert!(
            error_msg.contains("Failed to ensure network exists")
                || error_msg.contains("Failed to create network")
        );

        let start_calls = mock_client.start_container_calls.lock().unwrap();
        assert_eq!(start_calls.len(), 0);
    }

    #[tokio::test]
    async fn test_container_configuration() {
        // The agent network name is derived from the task ID in create_test_task()
        let agent_network = "tsk-agent-test-task-id";
        let mock_client = Arc::new(TrackedDockerClient {
            inspect_container_response: serde_json::json!({
                "State": { "Running": true },
                "NetworkSettings": {
                    "Networks": {
                        agent_network: { "IPAddress": "172.18.0.2" }
                    }
                }
            })
            .to_string(),
            ..Default::default()
        });
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(false);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let _ = manager.run_task_container("tsk/base", &task, &agent).await;

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        assert_eq!(create_calls.len(), 2); // One for proxy, one for task container

        // Check proxy container config (proxy manager creates this)
        let pcn = default_proxy_container_name();
        let (proxy_options, _proxy_config) = &create_calls[0];
        assert_eq!(proxy_options.as_ref().unwrap().name, Some(pcn.clone()));

        // Check task container config
        let (options, config) = &create_calls[1];

        assert!(
            options
                .as_ref()
                .unwrap()
                .name
                .as_ref()
                .unwrap()
                .starts_with("tsk-")
        );
        assert_eq!(
            options.as_ref().unwrap().name,
            Some("tsk-test-task-id".to_string())
        );
        assert_eq!(config.image, Some("tsk/base".to_string()));
        assert_eq!(config.working_dir, Some("/workspace/default".to_string()));
        // User is set directly
        assert_eq!(config.user, Some(CONTAINER_USER.to_string()));

        // Check that command includes the agent command
        let actual_cmd = config.cmd.as_ref().unwrap();
        assert_eq!(actual_cmd.len(), 3);
        assert_eq!(actual_cmd[0], "sh");
        assert_eq!(actual_cmd[1], "-c");
        assert!(actual_cmd[2].contains("claude"));

        // No entrypoint anymore
        assert!(config.entrypoint.is_none());

        let host_config = config.host_config.as_ref().unwrap();
        // Network mode should use task-specific isolated network
        assert_eq!(
            host_config.network_mode,
            Some("tsk-agent-test-task-id".to_string())
        );
        // Agent containers should have extra_hosts with proxy hostname -> IP mapping
        let extra_hosts = host_config
            .extra_hosts
            .as_ref()
            .expect("extra_hosts should be set");
        assert_eq!(extra_hosts.len(), 1);
        assert!(extra_hosts[0].contains(&pcn));
        assert!(extra_hosts[0].contains("172.18.0.2"));
        let default_resolved = crate::context::ResolvedConfig::default();
        assert_eq!(
            host_config.memory,
            Some(default_resolved.memory_limit_bytes())
        );
        assert_eq!(
            host_config.cpu_quota,
            Some(default_resolved.cpu_quota_microseconds())
        );

        let binds = host_config.binds.as_ref().unwrap();
        assert_eq!(binds.len(), 4); // workspace, claude dir, instructions, and output
        assert!(binds[0].contains("/tmp/test-repo:/workspace/default"));
        // In test mode, .claude directory is in temp directory
        assert!(binds[1].contains(":/home/agent/.claude"));
        assert!(binds[2].contains(":/instructions:ro"));
        assert!(binds[3].contains(":/output"));

        // Check proxy environment variables (uses fingerprinted proxy container name)
        let env = config.env.as_ref().unwrap();
        assert!(env.contains(&format!("HTTP_PROXY=http://{pcn}:3128")));
        assert!(env.contains(&format!("HTTPS_PROXY=http://{pcn}:3128")));
        assert!(env.contains(&format!("NO_PROXY=localhost,127.0.0.1,{pcn}")));
        assert!(env.contains(&format!("no_proxy=localhost,127.0.0.1,{pcn}")));
        assert!(
            env.iter().any(|e| e.starts_with("JAVA_TOOL_OPTIONS=")),
            "JAVA_TOOL_OPTIONS should be set for proxy"
        );

        // Non-DIND task: security_opt should be None, cap_drop should include SETUID/SETGID
        assert!(
            host_config.security_opt.is_none(),
            "security_opt should be None when dind is disabled"
        );
        let cap_drop = host_config.cap_drop.as_ref().unwrap();
        assert!(
            cap_drop.contains(&"SETUID".to_string()),
            "SETUID should be dropped when dind is disabled"
        );
        assert!(
            cap_drop.contains(&"SETGID".to_string()),
            "SETGID should be dropped when dind is disabled"
        );
        drop(create_calls);

        // Verify network lifecycle operations were called
        let create_internal_network_calls =
            mock_client.create_internal_network_calls.lock().unwrap();
        assert_eq!(create_internal_network_calls.len(), 1);
        assert_eq!(create_internal_network_calls[0], "tsk-agent-test-task-id");
        drop(create_internal_network_calls);

        let connect_calls = mock_client.connect_network_calls.lock().unwrap();
        assert_eq!(connect_calls.len(), 1);
        assert_eq!(
            connect_calls[0],
            (pcn.clone(), "tsk-agent-test-task-id".to_string())
        );
        drop(connect_calls);

        // Verify network cleanup was called
        let disconnect_calls = mock_client.disconnect_network_calls.lock().unwrap();
        assert_eq!(disconnect_calls.len(), 1);
        assert_eq!(
            disconnect_calls[0],
            (pcn, "tsk-agent-test-task-id".to_string())
        );
        drop(disconnect_calls);

        let remove_network_calls = mock_client.remove_network_calls.lock().unwrap();
        assert_eq!(remove_network_calls.len(), 1);
        assert_eq!(remove_network_calls[0], "tsk-agent-test-task-id");
    }

    #[tokio::test]
    async fn test_run_task_container_with_instructions_file() {
        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let mut task = create_test_task(false);
        task.instructions_file = "/tmp/tsk-test/instructions.txt".to_string();
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_ok());

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        assert_eq!(create_calls.len(), 2); // One for proxy, one for task container

        // Check that instructions directory is mounted
        let task_container_config = &create_calls[1].1;
        let host_config = task_container_config.host_config.as_ref().unwrap();
        let binds = host_config.binds.as_ref().unwrap();
        assert_eq!(binds.len(), 4); // workspace, claude dir, instructions, and output
        assert!(binds[2].contains("/tmp/tsk-test:/instructions:ro"));
        assert!(binds[3].contains(":/output"));
    }

    #[tokio::test]
    async fn test_relative_path_conversion() {
        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        // Create a temporary directory to use as base
        let temp_dir = tempfile::TempDir::new().unwrap();
        let absolute_path = temp_dir.path().join("test-repo");

        let mut task = create_test_task(false);
        task.copied_repo_path = Some(absolute_path.clone());
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_ok());

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        assert_eq!(create_calls.len(), 2); // One for proxy, one for task container
        let (_, config) = &create_calls[1]; // Get the task container config, not proxy

        let host_config = config.host_config.as_ref().unwrap();
        let binds = host_config.binds.as_ref().unwrap();
        let repo_bind = &binds[0];

        // Should contain an absolute path (starts with /)
        assert!(repo_bind.starts_with('/'));
        assert!(repo_bind.contains("test-repo"));
        assert!(repo_bind.ends_with(":/workspace/default"));

        // Should also have the claude directory, instructions, and output mounts
        assert_eq!(binds.len(), 4); // workspace, claude dir, instructions, and output
        // In test mode, .claude directory is in temp directory
        assert!(binds[1].contains(":/home/agent/.claude"));
        assert!(binds[2].contains(":/instructions:ro"));
        assert!(binds[3].contains(":/output"));
    }

    #[tokio::test]
    async fn test_project_volume_mounts_bind() {
        use crate::context::{BindMount, SharedConfig, TskConfig, VolumeMount};
        use std::collections::HashMap;

        let mock_client = Arc::new(TrackedDockerClient::default());

        // Create TskConfig with a bind mount for the test project
        let mut project_configs = HashMap::new();
        project_configs.insert(
            "default".to_string(),
            SharedConfig {
                volumes: vec![VolumeMount::Bind(BindMount {
                    host: "/host/cache".to_string(),
                    container: "/container/cache".to_string(),
                    readonly: false,
                })],
                ..Default::default()
            },
        );
        let tsk_config = TskConfig {
            project: project_configs,
            ..Default::default()
        };

        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(false);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_ok());

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let task_container_config = &create_calls[1].1;
        let host_config = task_container_config.host_config.as_ref().unwrap();
        let binds = host_config.binds.as_ref().unwrap();

        // Should have base binds + project bind mount
        assert_eq!(binds.len(), 5);
        assert!(
            binds
                .iter()
                .any(|b| b.contains("/host/cache:/container/cache"))
        );
    }

    #[tokio::test]
    async fn test_project_volume_mounts_named() {
        use crate::context::{NamedVolume, SharedConfig, TskConfig, VolumeMount};
        use std::collections::HashMap;

        let mock_client = Arc::new(TrackedDockerClient::default());

        // Create TskConfig with a named volume for the test project
        let mut project_configs = HashMap::new();
        project_configs.insert(
            "default".to_string(),
            SharedConfig {
                volumes: vec![VolumeMount::Named(NamedVolume {
                    name: "build-cache".to_string(),
                    container: "/container/cache".to_string(),
                    readonly: false,
                })],
                ..Default::default()
            },
        );
        let tsk_config = TskConfig {
            project: project_configs,
            ..Default::default()
        };

        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(false);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_ok());

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let task_container_config = &create_calls[1].1;
        let host_config = task_container_config.host_config.as_ref().unwrap();
        let binds = host_config.binds.as_ref().unwrap();

        // Should have base binds + named volume (prefixed with tsk-)
        assert_eq!(binds.len(), 5);
        assert!(
            binds
                .iter()
                .any(|b| b.contains("tsk-build-cache:/container/cache"))
        );
    }

    #[tokio::test]
    async fn test_project_volume_mounts_readonly() {
        use crate::context::{BindMount, SharedConfig, TskConfig, VolumeMount};
        use std::collections::HashMap;

        let mock_client = Arc::new(TrackedDockerClient::default());

        // Create TskConfig with a readonly bind mount
        let mut project_configs = HashMap::new();
        project_configs.insert(
            "default".to_string(),
            SharedConfig {
                volumes: vec![VolumeMount::Bind(BindMount {
                    host: "/etc/ssl/certs".to_string(),
                    container: "/etc/ssl/certs".to_string(),
                    readonly: true,
                })],
                ..Default::default()
            },
        );
        let tsk_config = TskConfig {
            project: project_configs,
            ..Default::default()
        };

        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(false);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_ok());

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let task_container_config = &create_calls[1].1;
        let host_config = task_container_config.host_config.as_ref().unwrap();
        let binds = host_config.binds.as_ref().unwrap();

        // Should have base binds + readonly bind mount
        assert_eq!(binds.len(), 5);
        assert!(
            binds
                .iter()
                .any(|b| b.contains("/etc/ssl/certs:/etc/ssl/certs:ro"))
        );
    }

    #[tokio::test]
    async fn test_no_project_volumes_when_project_not_configured() {
        // Test that binds don't include project volumes when project is not in config
        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let mut task = create_test_task(false);
        task.project = "unconfigured-project".to_string();
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_ok());

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let task_container_config = &create_calls[1].1;
        let host_config = task_container_config.host_config.as_ref().unwrap();
        let binds = host_config.binds.as_ref().unwrap();

        // Should only have base binds: workspace, claude dir, instructions, output
        assert_eq!(binds.len(), 4);
    }

    #[tokio::test]
    async fn test_project_env_vars() {
        use crate::context::{EnvVar, SharedConfig, TskConfig};
        use std::collections::HashMap;

        let mock_client = Arc::new(TrackedDockerClient::default());

        // Create TskConfig with environment variables for the test project
        let mut project_configs = HashMap::new();
        project_configs.insert(
            "default".to_string(),
            SharedConfig {
                env: vec![
                    EnvVar {
                        name: "DATABASE_URL".to_string(),
                        value: "postgres://tsk-proxy:5432/mydb".to_string(),
                    },
                    EnvVar {
                        name: "DEBUG".to_string(),
                        value: "true".to_string(),
                    },
                ],
                ..Default::default()
            },
        );
        let tsk_config = TskConfig {
            project: project_configs,
            ..Default::default()
        };

        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(false);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_ok());

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let task_container_config = &create_calls[1].1;
        let env = task_container_config.env.as_ref().unwrap();

        // Should have project env vars added
        assert!(
            env.iter()
                .any(|e| e == "DATABASE_URL=postgres://tsk-proxy:5432/mydb")
        );
        assert!(env.iter().any(|e| e == "DEBUG=true"));
    }

    #[tokio::test]
    async fn test_python_stack_does_not_set_pythonpath() {
        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let mut task = create_test_task(false);
        task.stack = "python".to_string();
        task.project = "my-python-app".to_string();
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let _ = manager.run_task_container("tsk/base", &task, &agent).await;

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let task_container_config = &create_calls[1].1;
        let env = task_container_config.env.as_ref().unwrap();

        // PYTHONPATH should never be set automatically, even for the python stack,
        // so projects that don't expect it keep their normal import behavior.
        assert!(!env.iter().any(|e| e.starts_with("PYTHONPATH=")));

        // working_dir still uses the project name
        assert_eq!(
            task_container_config.working_dir,
            Some("/workspace/my-python-app".to_string())
        );
    }

    #[tokio::test]
    async fn test_no_project_env_vars_when_project_not_configured() {
        // Test that env vars don't include project env vars when project is not in config
        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let mut task = create_test_task(false);
        task.project = "unconfigured-project".to_string();
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_ok());

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let task_container_config = &create_calls[1].1;
        let env = task_container_config.env.as_ref().unwrap();

        // Should not have any project-specific env vars (only proxy and agent env vars)
        assert!(
            !env.iter()
                .any(|e| e.starts_with("DATABASE_URL=") || e.starts_with("DEBUG="))
        );
    }

    #[tokio::test]
    async fn test_run_task_container_no_network_isolation() {
        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let mut task = create_test_task(false);
        task.network_isolation = false;
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_ok());

        // Verify NO proxy or network operations occurred
        let create_calls = mock_client.create_container_calls.lock().unwrap();
        assert_eq!(create_calls.len(), 1); // Only task container, no proxy

        let task_config = &create_calls[0].1;

        // No proxy env vars
        let env = task_config.env.as_ref().unwrap();
        assert!(!env.iter().any(|e| e.starts_with("HTTP_PROXY=")));
        assert!(!env.iter().any(|e| e.starts_with("HTTPS_PROXY=")));
        assert!(!env.iter().any(|e| e.starts_with("NO_PROXY=")));
        assert!(!env.iter().any(|e| e.starts_with("no_proxy=")));
        assert!(
            !env.iter().any(|e| e.starts_with("JAVA_TOOL_OPTIONS=")),
            "JAVA_TOOL_OPTIONS should NOT be set when proxy is disabled"
        );

        // TSK env vars should still be present
        assert!(env.contains(&"TSK_CONTAINER=1".to_string()));
        assert!(env.contains(&"TSK_TASK_ID=test-task-id".to_string()));

        // network_mode should be None
        let host_config = task_config.host_config.as_ref().unwrap();
        assert!(
            host_config.network_mode.is_none(),
            "network_mode should be None when isolation is disabled"
        );

        // NET_RAW should NOT be in cap_drop
        let cap_drop = host_config.cap_drop.as_ref().unwrap();
        assert!(
            !cap_drop.contains(&"NET_RAW".to_string()),
            "NET_RAW should not be dropped"
        );
        // But NET_ADMIN should still be dropped
        assert!(
            cap_drop.contains(&"NET_ADMIN".to_string()),
            "NET_ADMIN should still be dropped"
        );
        drop(create_calls);

        // No network operations
        let create_network_calls = mock_client.create_internal_network_calls.lock().unwrap();
        assert_eq!(create_network_calls.len(), 0);
        drop(create_network_calls);

        let connect_calls = mock_client.connect_network_calls.lock().unwrap();
        assert_eq!(connect_calls.len(), 0);
        drop(connect_calls);

        let disconnect_calls = mock_client.disconnect_network_calls.lock().unwrap();
        assert_eq!(disconnect_calls.len(), 0);
        drop(disconnect_calls);

        let remove_network_calls = mock_client.remove_network_calls.lock().unwrap();
        assert_eq!(remove_network_calls.len(), 0);
    }

    #[tokio::test]
    async fn test_cleanup_on_container_create_failure() {
        let mock_client = Arc::new(TrackedDockerClient {
            create_container_error: Some("out of disk space".to_string()),
            ..Default::default()
        });
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(false);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("out of disk space"));

        // Proxy container removed during cleanup
        let remove_calls = mock_client.remove_container_calls.lock().unwrap();
        assert_eq!(remove_calls.len(), 1); // Proxy cleanup
        drop(remove_calls);

        // Network cleanup should still happen (setup succeeded before container create failed)
        let disconnect_calls = mock_client.disconnect_network_calls.lock().unwrap();
        assert_eq!(disconnect_calls.len(), 1);
        drop(disconnect_calls);

        let remove_network_calls = mock_client.remove_network_calls.lock().unwrap();
        assert_eq!(remove_network_calls.len(), 1);
    }

    #[tokio::test]
    async fn test_cleanup_on_start_container_failure() {
        let mock_client = Arc::new(TrackedDockerClient {
            start_container_error: Some("container runtime error".to_string()),
            ..Default::default()
        });
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(false);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("container runtime error"));

        // Container was created, so cleanup should remove it, plus proxy cleanup
        let remove_calls = mock_client.remove_container_calls.lock().unwrap();
        assert_eq!(remove_calls.len(), 2); // Task container + proxy
        assert_eq!(remove_calls[0].0, "test-container-id-1");
        drop(remove_calls);

        // Network cleanup should happen
        let disconnect_calls = mock_client.disconnect_network_calls.lock().unwrap();
        assert_eq!(disconnect_calls.len(), 1);
        drop(disconnect_calls);

        let remove_network_calls = mock_client.remove_network_calls.lock().unwrap();
        assert_eq!(remove_network_calls.len(), 1);
    }

    #[tokio::test]
    async fn test_dind_security_relaxations() {
        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let mut task = create_test_task(false);
        task.dind = true;
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let _ = manager.run_task_container("tsk/base", &task, &agent).await;

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let task_config = &create_calls[1].1;
        let host_config = task_config.host_config.as_ref().unwrap();

        // DIND: security_opt should be set with seccomp profile and apparmor=unconfined
        let security_opt = host_config
            .security_opt
            .as_ref()
            .expect("security_opt should be Some when dind is enabled");
        assert!(
            security_opt.iter().any(|s| s.starts_with("seccomp=")),
            "Should have seccomp profile"
        );
        assert!(
            security_opt.iter().any(|s| s == "apparmor=unconfined"),
            "Should have apparmor=unconfined"
        );

        // DIND: SETUID and SETGID should NOT be in cap_drop
        let cap_drop = host_config.cap_drop.as_ref().unwrap();
        assert!(
            !cap_drop.contains(&"SETUID".to_string()),
            "SETUID should not be dropped when dind is enabled"
        );
        assert!(
            !cap_drop.contains(&"SETGID".to_string()),
            "SETGID should not be dropped when dind is enabled"
        );

        // Other capabilities should still be dropped
        assert!(cap_drop.contains(&"NET_ADMIN".to_string()));
        assert!(cap_drop.contains(&"SYS_ADMIN".to_string()));

        // DIND: BUILDAH_ISOLATION=chroot should be set for nested Podman builds
        let env = task_config.env.as_ref().unwrap();
        assert!(
            env.contains(&"BUILDAH_ISOLATION=chroot".to_string()),
            "BUILDAH_ISOLATION=chroot should be set when dind is enabled"
        );
    }

    #[tokio::test]
    async fn test_non_dind_security_defaults() {
        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(false); // dind defaults to false
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let _ = manager.run_task_container("tsk/base", &task, &agent).await;

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let task_config = &create_calls[1].1;
        let host_config = task_config.host_config.as_ref().unwrap();

        // Non-DIND: security_opt should be None
        assert!(
            host_config.security_opt.is_none(),
            "security_opt should be None when dind is disabled"
        );

        // Non-DIND: SETUID and SETGID should be in cap_drop
        let cap_drop = host_config.cap_drop.as_ref().unwrap();
        assert!(
            cap_drop.contains(&"SETUID".to_string()),
            "SETUID should be dropped when dind is disabled"
        );
        assert!(
            cap_drop.contains(&"SETGID".to_string()),
            "SETGID should be dropped when dind is disabled"
        );

        // Other capabilities should still be dropped
        assert!(cap_drop.contains(&"NET_ADMIN".to_string()));
        assert!(cap_drop.contains(&"SYS_ADMIN".to_string()));
        assert!(cap_drop.contains(&"SYS_PTRACE".to_string()));
        assert!(cap_drop.contains(&"DAC_OVERRIDE".to_string()));
        assert!(cap_drop.contains(&"AUDIT_WRITE".to_string()));

        // Non-DIND: BUILDAH_ISOLATION should not be set
        let env = task_config.env.as_ref().unwrap();
        assert!(
            !env.contains(&"BUILDAH_ISOLATION=chroot".to_string()),
            "BUILDAH_ISOLATION should not be set when dind is disabled"
        );
    }

    #[tokio::test]
    async fn test_sudo_security_relaxations() {
        let mock_client = Arc::new(TrackedDockerClient::default());

        // Create a resolved config with sudo enabled and snapshot it on the task
        let resolved = crate::context::ResolvedConfig {
            sudo: true,
            ..Default::default()
        };
        let config_json = serde_json::to_string(&resolved).unwrap();

        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let mut task = create_test_task(false);
        task.resolved_config = Some(config_json);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let _ = manager.run_task_container("tsk/base", &task, &agent).await;

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let task_config = &create_calls[1].1;
        let host_config = task_config.host_config.as_ref().unwrap();

        // Sudo: SETUID and SETGID should NOT be in cap_drop
        let cap_drop = host_config.cap_drop.as_ref().unwrap();
        assert!(
            !cap_drop.contains(&"SETUID".to_string()),
            "SETUID should not be dropped when sudo is enabled"
        );
        assert!(
            !cap_drop.contains(&"SETGID".to_string()),
            "SETGID should not be dropped when sudo is enabled"
        );

        // Sudo should NOT enable seccomp/apparmor relaxations (those are DIND-only)
        assert!(
            host_config.security_opt.is_none(),
            "security_opt should be None when only sudo is enabled (not dind)"
        );
    }
    /// Builds a task whose config snapshot enables Tailscale with the auth key
    /// read from `key_file`. The env var name is one that is never set so the
    /// test does not depend on the ambient environment.
    fn tailscale_task(key_file: &std::path::Path, hostname: Option<&str>) -> Task {
        let resolved = crate::context::ResolvedConfig {
            tailscale: true,
            tailscale_auth_key_env: Some("TSK_TEST_UNSET_TS_AUTHKEY".to_string()),
            tailscale_auth_key_file: Some(key_file.to_string_lossy().to_string()),
            tailscale_hostname: hostname.map(|h| h.to_string()),
            tailscale_up_args: Some("--ssh".to_string()),
            ..Default::default()
        };
        let mut task = create_test_task(false);
        task.resolved_config = Some(serde_json::to_string(&resolved).unwrap());
        task
    }

    #[tokio::test]
    async fn test_tailscale_container_configuration() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let key_file = temp_dir.path().join("ts-authkey");
        std::fs::write(&key_file, "tskey-auth-test\n").unwrap();

        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = tailscale_task(&key_file, None);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;
        assert!(result.is_ok());

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let task_config = &create_calls[1].1;
        let env = task_config.env.as_ref().unwrap();

        // The auth key is read from the key file and trimmed
        assert!(env.contains(&"TS_AUTHKEY=tskey-auth-test".to_string()));
        // Hostname defaults to tsk-<task-id>
        assert!(env.contains(&"TSK_TAILSCALE_HOSTNAME=tsk-test-task-id".to_string()));
        assert!(env.contains(&"TSK_TAILSCALE_UP_ARGS=--ssh".to_string()));
        // Subnet routes are off unless explicitly opted in
        assert!(env.contains(&"TSK_TAILSCALE_ACCEPT_ROUTES=false".to_string()));

        // Tailnet destinations bypass the Squid proxy
        let no_proxy = env
            .iter()
            .find(|e| e.starts_with("NO_PROXY="))
            .expect("NO_PROXY should be set");
        assert!(no_proxy.contains("100.64.0.0/10"), "got: {no_proxy}");
        assert!(no_proxy.contains("fd7a:115c:a1e0::/48"), "got: {no_proxy}");
        assert!(no_proxy.contains(".ts.net"), "got: {no_proxy}");
        // Kernel mode (Docker) routes the tailnet transparently, so no ALL_PROXY.
        assert!(
            !env.iter().any(|e| e.starts_with("ALL_PROXY=")),
            "kernel mode must not set ALL_PROXY"
        );

        // tailscaled needs NET_ADMIN, so it is granted rather than dropped
        let host_config = task_config.host_config.as_ref().unwrap();
        let cap_drop = host_config.cap_drop.as_ref().unwrap();
        assert!(!cap_drop.contains(&"NET_ADMIN".to_string()));
        assert_eq!(
            host_config.cap_add,
            Some(vec!["NET_ADMIN".to_string()]),
            "NET_ADMIN should be added when Tailscale is enabled"
        );

        // The startup script runs before the agent command
        let cmd = task_config.cmd.as_ref().unwrap();
        assert_eq!(cmd[0], "sh");
        assert_eq!(cmd[1], "-c");
        assert!(
            cmd[2].starts_with("/usr/local/bin/tsk-tailscale-up || exit 1\n"),
            "got: {}",
            cmd[2]
        );
        // The auth key is scrubbed from the env before the agent runs
        assert!(
            cmd[2].contains("unset TS_AUTHKEY"),
            "auth key must be unset before the agent command, got: {}",
            cmd[2]
        );
        assert!(
            cmd[2].contains("claude"),
            "agent command should be preserved"
        );
    }

    #[tokio::test]
    async fn test_tailscale_hostname_and_accept_routes_overrides() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let key_file = temp_dir.path().join("ts-authkey");
        std::fs::write(&key_file, "tskey-auth-test\n").unwrap();

        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        // Custom hostname + accept_routes explicitly enabled
        let resolved = crate::context::ResolvedConfig {
            tailscale: true,
            tailscale_auth_key_env: Some("TSK_TEST_UNSET_TS_AUTHKEY".to_string()),
            tailscale_auth_key_file: Some(key_file.to_string_lossy().to_string()),
            tailscale_hostname: Some("sandbox".to_string()),
            tailscale_accept_routes: true,
            ..Default::default()
        };
        let mut task = create_test_task(false);
        task.resolved_config = Some(serde_json::to_string(&resolved).unwrap());

        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        manager
            .run_task_container("tsk/base", &task, &agent)
            .await
            .unwrap();

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let env = create_calls[1].1.env.as_ref().unwrap();
        assert!(
            env.contains(&"TSK_TAILSCALE_HOSTNAME=sandbox".to_string()),
            "custom hostname should reach the container env"
        );
        assert!(
            env.contains(&"TSK_TAILSCALE_ACCEPT_ROUTES=true".to_string()),
            "accept_routes=true should reach the container env"
        );
    }

    #[tokio::test]
    async fn test_tailscale_userspace_mode_proxy_env() {
        use crate::context::tsk_config::{ContainerEngine, TskConfig};

        let temp_dir = tempfile::TempDir::new().unwrap();
        let key_file = temp_dir.path().join("ts-authkey");
        std::fs::write(&key_file, "tskey-auth-test\n").unwrap();

        // Podman → userspace mode: the tailnet must NOT be in NO_PROXY (no route),
        // and ALL_PROXY must point at the SOCKS5 listener instead.
        let tsk_config = TskConfig {
            container_engine: ContainerEngine::Podman,
            ..Default::default()
        };
        let ctx = AppContext::builder().with_tsk_config(tsk_config).build();
        let mock_client = Arc::new(TrackedDockerClient::default());
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = tailscale_task(&key_file, None);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        manager
            .run_task_container("tsk/base", &task, &agent)
            .await
            .unwrap();

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let env = create_calls[1].1.env.as_ref().unwrap();

        let no_proxy = env
            .iter()
            .find(|e| e.starts_with("NO_PROXY="))
            .expect("NO_PROXY should be set");
        assert!(
            !no_proxy.contains("100.64.0.0/10"),
            "userspace mode must NOT bypass the proxy for the tailnet: {no_proxy}"
        );
        assert!(
            env.contains(&"ALL_PROXY=socks5h://localhost:1055".to_string()),
            "userspace mode should route the tailnet via the SOCKS5 proxy"
        );
    }

    #[tokio::test]
    async fn test_tailscale_disabled_by_default() {
        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let task = create_test_task(false);
        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;
        assert!(result.is_ok());

        let create_calls = mock_client.create_container_calls.lock().unwrap();
        let task_config = &create_calls[1].1;

        let env = task_config.env.as_ref().unwrap();
        assert!(!env.iter().any(|e| e.starts_with("TS_AUTHKEY=")));
        assert!(!env.iter().any(|e| e.contains("100.64.0.0/10")));

        let host_config = task_config.host_config.as_ref().unwrap();
        assert!(host_config.cap_add.is_none());
        assert!(
            host_config
                .cap_drop
                .as_ref()
                .unwrap()
                .contains(&"NET_ADMIN".to_string())
        );

        let cmd = task_config.cmd.as_ref().unwrap();
        assert!(!cmd[2].contains("tsk-tailscale-up"));
    }

    #[tokio::test]
    async fn test_tailscale_without_auth_key_fails_fast() {
        let mock_client = Arc::new(TrackedDockerClient::default());
        let ctx = AppContext::builder().build();
        let manager = DockerManager::new(&ctx, mock_client.clone(), None);

        let resolved = crate::context::ResolvedConfig {
            tailscale: true,
            tailscale_auth_key_env: Some("TSK_TEST_UNSET_TS_AUTHKEY".to_string()),
            ..Default::default()
        };
        let mut task = create_test_task(false);
        task.resolved_config = Some(serde_json::to_string(&resolved).unwrap());

        let agent = crate::agent::ClaudeAgent::with_tsk_env(ctx.tsk_env());
        let result = manager.run_task_container("tsk/base", &task, &agent).await;

        let err = result.unwrap_err();
        assert!(err.contains("TSK_TEST_UNSET_TS_AUTHKEY"), "got: {err}");
        // No containers should have been created
        assert!(
            mock_client
                .create_container_calls
                .lock()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_resolve_tailscale_auth_key_prefers_env_over_file() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let key_file = temp_dir.path().join("ts-authkey");
        std::fs::write(&key_file, "  from-file  ").unwrap();

        let resolved = crate::context::ResolvedConfig {
            tailscale: true,
            tailscale_auth_key_file: Some(key_file.to_string_lossy().to_string()),
            ..Default::default()
        };

        // Default env var name is consulted first
        let key = resolve_tailscale_auth_key_with(&resolved, |name| {
            (name == "TS_AUTHKEY").then(|| " from-env \n".to_string())
        })
        .unwrap();
        assert_eq!(key, "from-env");

        // Falls back to the key file when the env var is unset or empty
        let key = resolve_tailscale_auth_key_with(&resolved, |_| Some(String::new())).unwrap();
        assert_eq!(key, "from-file");

        // A custom env var name is honored
        let custom = crate::context::ResolvedConfig {
            tailscale_auth_key_env: Some("MY_KEY".to_string()),
            ..resolved.clone()
        };
        let key = resolve_tailscale_auth_key_with(&custom, |name| {
            (name == "MY_KEY").then(|| "k".to_string())
        })
        .unwrap();
        assert_eq!(key, "k");

        // Neither source available: actionable error naming the env var
        let no_sources = crate::context::ResolvedConfig {
            tailscale: true,
            ..Default::default()
        };
        let err = resolve_tailscale_auth_key_with(&no_sources, |_| None).unwrap_err();
        assert!(err.contains("TS_AUTHKEY"), "got: {err}");
    }

    #[test]
    fn test_resolve_tailscale_auth_key_whitespace_env_falls_through() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let key_file = temp_dir.path().join("ts-authkey");
        std::fs::write(&key_file, "from-file").unwrap();
        let resolved = crate::context::ResolvedConfig {
            tailscale: true,
            tailscale_auth_key_file: Some(key_file.to_string_lossy().to_string()),
            ..Default::default()
        };
        // A whitespace-only env value is treated as empty and falls to the file.
        let key = resolve_tailscale_auth_key_with(&resolved, |_| Some("   \n".to_string())).unwrap();
        assert_eq!(key, "from-file");
    }

    #[test]
    fn test_resolve_tailscale_auth_key_empty_file_errors() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let key_file = temp_dir.path().join("ts-authkey");
        std::fs::write(&key_file, "   \n\t ").unwrap(); // whitespace only
        let resolved = crate::context::ResolvedConfig {
            tailscale: true,
            tailscale_auth_key_file: Some(key_file.to_string_lossy().to_string()),
            ..Default::default()
        };
        let err = resolve_tailscale_auth_key_with(&resolved, |_| None).unwrap_err();
        assert!(err.contains("is empty"), "got: {err}");
        assert!(err.contains("ts-authkey"), "error should name the file: {err}");
    }

    #[test]
    fn test_resolve_tailscale_auth_key_missing_file_errors() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let missing = temp_dir.path().join("does-not-exist");
        let resolved = crate::context::ResolvedConfig {
            tailscale: true,
            tailscale_auth_key_file: Some(missing.to_string_lossy().to_string()),
            ..Default::default()
        };
        let err = resolve_tailscale_auth_key_with(&resolved, |_| None).unwrap_err();
        assert!(
            err.contains("Failed to read tailscale_auth_key_file"),
            "got: {err}"
        );
    }

    #[test]
    fn test_parse_tailnet_aliases() {
        let json = serde_json::json!({
            "Self": { "DNSName": "tsk-abc.example.ts.net.", "TailscaleIPs": ["100.83.1.1"] },
            "Peer": {
                "nodekey:aaa": {
                    "DNSName": "rainier.example.ts.net.",
                    "TailscaleIPs": ["100.81.17.123", "fd7a::1"]
                },
                "nodekey:bbb": { "DNSName": "db.example.ts.net.", "TailscaleIPs": ["100.90.0.9"] },
                "nodekey:ccc": { "TailscaleIPs": [] }
            }
        });
        let aliases = parse_tailnet_aliases(&json);
        // Each valid node yields FQDN + short label, using the first (IPv4) IP.
        assert!(aliases.contains(&"rainier.example.ts.net:100.81.17.123".to_string()));
        assert!(aliases.contains(&"rainier:100.81.17.123".to_string()));
        assert!(aliases.contains(&"db.example.ts.net:100.90.0.9".to_string()));
        assert!(aliases.contains(&"db:100.90.0.9".to_string()));
        // Self is included so a task can resolve its own node.
        assert!(aliases.contains(&"tsk-abc.example.ts.net:100.83.1.1".to_string()));
        // A node without a DNSName or IP is skipped, not panicked on.
        assert!(!aliases.iter().any(|a| a.contains("ccc")));
    }

    #[test]
    fn test_parse_tailnet_aliases_rejects_malformed() {
        // Peer-controlled names/IPs that would produce a bad ExtraHosts line are skipped.
        let json = serde_json::json!({
            "Peer": {
                "a": { "DNSName": "evil host.ts.net.", "TailscaleIPs": ["100.1.1.1"] }, // space
                "b": { "DNSName": "ok.ts.net.", "TailscaleIPs": ["not-an-ip"] },         // bad IP
                "c": { "DNSName": "bad:name.ts.net.", "TailscaleIPs": ["100.2.2.2"] },   // colon
                "d": { "DNSName": "good.ts.net.", "TailscaleIPs": ["100.3.3.3"] }         // valid
            }
        });
        let aliases = parse_tailnet_aliases(&json);
        assert!(aliases.contains(&"good.ts.net:100.3.3.3".to_string()));
        assert_eq!(aliases.iter().filter(|a| !a.starts_with("good")).count(), 0);
    }

    #[test]
    fn test_build_extra_hosts() {
        // Proxy only (no tailnet aliases) → just the proxy entry.
        assert_eq!(
            build_extra_hosts(Some("tsk-proxy:172.18.0.2".to_string()), vec![]),
            Some(vec!["tsk-proxy:172.18.0.2".to_string()])
        );
        // Proxy + tailnet aliases → both, proxy first.
        assert_eq!(
            build_extra_hosts(
                Some("tsk-proxy:172.18.0.2".to_string()),
                vec!["rainier:100.81.17.123".to_string()]
            ),
            Some(vec![
                "tsk-proxy:172.18.0.2".to_string(),
                "rainier:100.81.17.123".to_string()
            ])
        );
        // Nothing at all → None (keep the runtime's default /etc/hosts).
        assert_eq!(build_extra_hosts(None, vec![]), None);
    }

    #[test]
    fn test_with_tailscale_startup_wraps_commands() {
        // The prefix joins the tailnet, then scrubs the secret from the env so
        // the agent process can't read it out of /proc/self/environ.
        const PREFIX: &str =
            "/usr/local/bin/tsk-tailscale-up || exit 1\nunset TS_AUTHKEY TSK_TAILSCALE_UP_ARGS";

        // `sh -c` commands are exec'd (as `exec sh -c <script>`) so the agent's
        // surviving process image is replaced after the unset.
        let wrapped = with_tailscale_startup(vec![
            "sh".to_string(),
            "-c".to_string(),
            "claude --run".to_string(),
        ]);
        assert_eq!(
            wrapped,
            vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("{PREFIX}\nexec 'sh' '-c' 'claude --run'"),
            ]
        );

        // Other commands are wrapped in a shell with their arguments quoted
        let wrapped = with_tailscale_startup(vec!["my agent".to_string(), "it's".to_string()]);
        assert_eq!(wrapped[0], "sh");
        assert_eq!(wrapped[2], format!("{PREFIX}\nexec 'my agent' 'it'\\''s'"));

        // An empty command (image default) is left untouched
        assert!(with_tailscale_startup(vec![]).is_empty());
    }

    #[test]
    fn test_with_tailscale_startup_quoting_is_injection_safe() {
        // Empty arguments must survive as '' (an empty single-quoted string),
        // never be silently dropped — dropping one shifts the whole argv.
        let wrapped = with_tailscale_startup(vec!["bin".to_string(), String::new()]);
        assert!(
            wrapped[2].ends_with("exec 'bin' ''"),
            "empty arg must render as '', got: {}",
            wrapped[2]
        );

        // Shell metacharacters are neutralized by single-quoting; only the
        // single quote itself needs the '\'' escape.
        let nasty = r#"he said "$x" `whoami` \y ; rm -rf /"#.to_string();
        let wrapped = with_tailscale_startup(vec!["bin".to_string(), nasty.clone()]);
        let expected = format!("exec 'bin' '{}'", nasty.replace('\'', r"'\''"));
        assert!(
            wrapped[2].ends_with(&expected),
            "metachars must be quoted verbatim, got: {}",
            wrapped[2]
        );

        // A 4-element command that merely starts with `sh -c` is NOT the
        // in-place special case — it must take the generic quoting path so the
        // trailing positional args are preserved.
        let wrapped = with_tailscale_startup(vec![
            "sh".to_string(),
            "-c".to_string(),
            "script".to_string(),
            "arg0".to_string(),
        ]);
        assert!(
            wrapped[2].ends_with("exec 'sh' '-c' 'script' 'arg0'"),
            "4-arg sh -c must be fully quoted, got: {}",
            wrapped[2]
        );
    }
}
