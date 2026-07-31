# Tailscale Sandbox Node Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the Tailscale admin console from accumulating leftover `tsk-*` nodes by minting a fresh ephemeral, tagged auth key per sandbox (opt-in, PAT or OAuth), while keeping the existing bring-your-own-key path with an untagged-node warning.

**Architecture:** When a Tailscale API mint credential is configured, tsk calls the Tailscale API at container start to mint a single-use, ephemeral, tagged auth key for that one task; the ephemeral node auto-removes from the tailnet shortly after the sandbox stops. When no mint credential is configured, tsk falls back to today's `TS_AUTHKEY`/key-file resolution and the container prints a warning if the joined node is untagged. The mint credential lives on the host and is never passed into the container or written to the task's config snapshot.

**Tech Stack:** Rust, tokio (async), reqwest (new HTTP dependency, rustls-tls), serde_json, Docker/Podman via bollard, jq (already in the base image) for the in-container tag check.

## Global Constraints

- Language: Rust. Use `just test` (runs `cargo test -q`), `just format` (`cargo fmt`), `just lint` (`cargo clippy --all-targets -- -D warnings` and `cargo clippy -- -D warnings`).
- No `#[allow(dead_code)]` directives. No `unsafe` blocks.
- Conventional commits: `feat:` for the user-facing minting capability, `docs:` for documentation, `test:` for test-only additions.
- New dependency: `reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "json"] }` — rustls (not OpenSSL) to avoid a system libssl dependency.
- Default tag for minted keys/nodes: `tag:tsk-sandbox`.
- Minting is **opt-in**: only attempted when `tailscale_oauth_client_id`, `tailscale_api_key_env`, or `tailscale_api_key_file` is set. There is **no** default env var for the mint credential (unlike `tailscale_auth_key_env`, which defaults to `TS_AUTHKEY`).
- Secrets (PAT, OAuth secret) are read live from the host at container start; only their *references* (env-var names, file paths) may enter the config snapshot.
- Minting happens at container start (in `DockerManager::run_task_container`), not at task creation — consistent with how `resolve_tailscale_auth_key` already reads the key at start.
- Existing bring-your-own-key behavior (`resolve_tailscale_auth_key`) must remain unchanged and is the fallback.

---

### Task 1: Add Tailscale mint config fields

Adds the seven new config fields to both `SharedConfig` (optional, user-facing) and `ResolvedConfig` (resolved), plus the layering-merge logic and `Default`. No accessors or consumers yet — those land in Task 2 — so this task stays clippy-clean on its own (struct fields are consumed by the serde derive and the merge loop).

**Files:**
- Modify: `src/context/tsk_config.rs` — `SharedConfig` (struct ends at `:325`), `resolve_config` merge block (Tailscale merges around `:159-179`), `ResolvedConfig` (ends `:405`), `ResolvedConfig::default` (`tailscale_*` defaults around `:431-437`).
- Test: `src/context/tsk_config.rs` (inline `#[cfg(test)]` module — this is an allowed exception to the AppContext test rule per CLAUDE.md: "Tests in `src/context/*` that are directly testing TskEnv or TskConfig functionality").

**Interfaces:**
- Produces (on `SharedConfig`, all `Option`, `pub`): `tailscale_api_key_env: Option<String>`, `tailscale_api_key_file: Option<String>`, `tailscale_oauth_client_id: Option<String>`, `tailscale_oauth_secret_env: Option<String>`, `tailscale_oauth_secret_file: Option<String>`, `tailscale_tailnet: Option<String>`, `tailscale_tags: Option<Vec<String>>`.
- Produces (on `ResolvedConfig`, same names/types, `pub`, each `#[serde(default)]`).

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` module in `src/context/tsk_config.rs`:

This mirrors the existing `test_tailscale_config_layering` (`:2038`) exactly: write a `tsk.toml`, load it with the module's `load_config(dir)` helper, and resolve with `resolve_config(project, None, None)` (three args).

```rust
#[test]
fn test_tailscale_mint_config_layering() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let config_dir = temp_dir.path();

    let toml_content = r#"
[defaults]
tailscale = true
tailscale_oauth_client_id = "kABC123"
tailscale_oauth_secret_env = "TS_OAUTH_SECRET"
tailscale_tailnet = "example.com"
tailscale_tags = ["tag:tsk-sandbox"]
"#;
    std::fs::write(config_dir.join("tsk.toml"), toml_content).unwrap();
    let config = load_config(config_dir);

    let resolved = config.resolve_config("any-project", None, None);
    assert_eq!(
        resolved.tailscale_oauth_client_id.as_deref(),
        Some("kABC123")
    );
    assert_eq!(
        resolved.tailscale_oauth_secret_env.as_deref(),
        Some("TS_OAUTH_SECRET")
    );
    assert_eq!(resolved.tailscale_tailnet.as_deref(), Some("example.com"));
    assert_eq!(
        resolved.tailscale_tags,
        Some(vec!["tag:tsk-sandbox".to_string()])
    );
    // Unset PAT fields stay None.
    assert!(resolved.tailscale_api_key_env.is_none());
    assert!(resolved.tailscale_api_key_file.is_none());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -q test_tailscale_mint_config_layering`
Expected: FAIL to compile — `SharedConfig`/`ResolvedConfig` have no `tailscale_oauth_client_id` field.

- [ ] **Step 3: Add fields to `SharedConfig`**

In `src/context/tsk_config.rs`, immediately after the `tailscale_up_args` field in `SharedConfig` (`:324`):

```rust
    /// Name of the environment variable holding a Tailscale API access token
    /// (PAT) used to mint a per-task ephemeral auth key. Setting this (or
    /// `tailscale_api_key_file` / `tailscale_oauth_client_id`) opts into minting.
    pub tailscale_api_key_env: Option<String>,
    /// Path to a file containing a Tailscale API access token (PAT), `~`-expanded.
    pub tailscale_api_key_file: Option<String>,
    /// OAuth client ID used to mint per-task ephemeral auth keys. Takes
    /// precedence over the PAT fields when set.
    pub tailscale_oauth_client_id: Option<String>,
    /// Name of the environment variable holding the OAuth client secret.
    pub tailscale_oauth_secret_env: Option<String>,
    /// Path to a file containing the OAuth client secret, `~`-expanded.
    pub tailscale_oauth_secret_file: Option<String>,
    /// Tailnet to mint keys in (default: `-`, the credential's default tailnet).
    pub tailscale_tailnet: Option<String>,
    /// Tags applied to minted keys/nodes (default: `["tag:tsk-sandbox"]`).
    pub tailscale_tags: Option<Vec<String>>,
```

- [ ] **Step 4: Add fields to `ResolvedConfig`**

Immediately after the `tailscale_up_args` field in `ResolvedConfig` (`:404`):

```rust
    /// Env var holding a Tailscale API access token (PAT) for minting.
    #[serde(default)]
    pub tailscale_api_key_env: Option<String>,
    /// File holding a Tailscale API access token (PAT) for minting.
    #[serde(default)]
    pub tailscale_api_key_file: Option<String>,
    /// OAuth client ID for minting per-task ephemeral auth keys.
    #[serde(default)]
    pub tailscale_oauth_client_id: Option<String>,
    /// Env var holding the OAuth client secret.
    #[serde(default)]
    pub tailscale_oauth_secret_env: Option<String>,
    /// File holding the OAuth client secret.
    #[serde(default)]
    pub tailscale_oauth_secret_file: Option<String>,
    /// Tailnet to mint keys in (default resolved via accessor to `-`).
    #[serde(default)]
    pub tailscale_tailnet: Option<String>,
    /// Tags applied to minted keys/nodes (default resolved via accessor).
    #[serde(default)]
    pub tailscale_tags: Option<Vec<String>>,
```

- [ ] **Step 5: Add defaults to `ResolvedConfig::default`**

In the `impl Default for ResolvedConfig` block, immediately after `tailscale_up_args: None,` (`:437`):

```rust
            tailscale_api_key_env: None,
            tailscale_api_key_file: None,
            tailscale_oauth_client_id: None,
            tailscale_oauth_secret_env: None,
            tailscale_oauth_secret_file: None,
            tailscale_tailnet: None,
            tailscale_tags: None,
```

- [ ] **Step 6: Add merge logic in `resolve_config`**

In the Tailscale merge block, immediately after the `tailscale_up_args` merge (`:177-179`):

```rust
        if let Some(ref v) = config.tailscale_api_key_env {
            resolved.tailscale_api_key_env = Some(v.clone());
        }
        if let Some(ref v) = config.tailscale_api_key_file {
            resolved.tailscale_api_key_file = Some(v.clone());
        }
        if let Some(ref v) = config.tailscale_oauth_client_id {
            resolved.tailscale_oauth_client_id = Some(v.clone());
        }
        if let Some(ref v) = config.tailscale_oauth_secret_env {
            resolved.tailscale_oauth_secret_env = Some(v.clone());
        }
        if let Some(ref v) = config.tailscale_oauth_secret_file {
            resolved.tailscale_oauth_secret_file = Some(v.clone());
        }
        if let Some(ref v) = config.tailscale_tailnet {
            resolved.tailscale_tailnet = Some(v.clone());
        }
        if let Some(ref v) = config.tailscale_tags {
            resolved.tailscale_tags = Some(v.clone());
        }
```

Note: this merge loop runs once per config layer (defaults, then project override), so the highest-priority `Some` wins for scalars, and `tailscale_tags` is replaced wholesale by the highest-priority layer that sets it (not combined) — the intended behavior for tags.

- [ ] **Step 7: Run test to verify it passes**

Run: `cargo test -q test_tailscale_mint_config_layering`
Expected: PASS

- [ ] **Step 8: Verify existing config tests still pass**

Run: `cargo test -q --lib context::tsk_config`
Expected: PASS (all existing Tailscale/config tests green)

- [ ] **Step 9: Commit**

```bash
git add src/context/tsk_config.rs
git commit -m "feat: add Tailscale mint credential config fields"
```

---

### Task 2: Mint an ephemeral tagged key per task

Adds the `reqwest` dependency, a new `src/tailscale.rs` module with the mint logic (pure request-building/response-parsing plus the async glue), the `ResolvedConfig` accessors, and wires minting into `run_task_container` behind the mint-credential check. This is the atomic "minting works" deliverable — config accessors, module, and wiring all reference each other, so they land together and the task ends clippy-clean.

**Files:**
- Modify: `Cargo.toml` — add `reqwest`.
- Create: `src/tailscale.rs` — mint module.
- Modify: `src/main.rs:9` area — add `mod tailscale;` alongside `mod docker;`.
- Modify: `src/context/tsk_config.rs` — add three accessors on `ResolvedConfig` near `tailscale_auth_key_env_var` (`:496`).
- Modify: `src/docker/mod.rs:934-938` — branch to minting when a mint credential is configured.
- Test: inline `#[cfg(test)]` in `src/tailscale.rs`; accessor tests inline in `src/context/tsk_config.rs`.

**Interfaces:**
- Consumes (from Task 1): the seven `ResolvedConfig` `tailscale_*` mint fields; `crate::context::tsk_config::expand_tilde` (existing `pub fn`, used at `src/docker/mod.rs:144`); `crate::context::ResolvedConfig`.
- Produces (in `crate::tailscale`, all `pub`):
  - `const DEFAULT_TAILSCALE_TAG: &str = "tag:tsk-sandbox";`
  - `enum MintCredential { Pat(String), OAuth { client_id: String, secret: String } }`
  - `fn resolve_mint_credential(resolved: &ResolvedConfig, env_lookup: impl Fn(&str) -> Option<String>) -> Result<Option<MintCredential>, String>`
  - `fn build_mint_request_body(tags: &[String], expiry_secs: u64, description: &str) -> serde_json::Value`
  - `fn parse_mint_response(body: &str) -> Result<String, String>`
  - `fn parse_oauth_token_response(body: &str) -> Result<String, String>`
  - `async fn mint_tailscale_auth_key(resolved: &ResolvedConfig, task_id: &str) -> Result<String, String>`
- Produces (on `ResolvedConfig`, `pub`): `fn has_tailscale_mint_credential(&self) -> bool`, `fn tailscale_tailnet(&self) -> &str`, `fn tailscale_tags(&self) -> Vec<String>`.

- [ ] **Step 1: Add the reqwest dependency**

In `Cargo.toml` `[dependencies]`, add:

```toml
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "json"] }
```

Run: `cargo build` — Expected: compiles (new dep downloads/builds).

- [ ] **Step 2: Write the failing tests for the pure mint helpers**

Create `src/tailscale.rs` with just the test module first (so the test names exist and fail to compile against the missing functions):

```rust
//! Host-side Tailscale auth-key minting.
//!
//! When a mint credential (PAT or OAuth client) is configured, tsk mints a
//! fresh single-use, ephemeral, tagged auth key per task via the Tailscale API
//! instead of reusing a long-lived key. Ephemeral nodes auto-remove from the
//! tailnet shortly after the sandbox stops, so the admin console does not
//! accumulate `tsk-*` devices.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ResolvedConfig;

    #[test]
    fn test_build_mint_request_body_requests_ephemeral_tagged_single_use() {
        let tags = vec!["tag:tsk-sandbox".to_string()];
        let body = build_mint_request_body(&tags, 300, "tsk sandbox tsk-abc123");
        let create = &body["capabilities"]["devices"]["create"];
        assert_eq!(create["reusable"], serde_json::json!(false));
        assert_eq!(create["ephemeral"], serde_json::json!(true));
        assert_eq!(create["preauthorized"], serde_json::json!(true));
        assert_eq!(create["tags"], serde_json::json!(["tag:tsk-sandbox"]));
        assert_eq!(body["expirySeconds"], serde_json::json!(300));
        assert_eq!(body["description"], serde_json::json!("tsk sandbox tsk-abc123"));
    }

    #[test]
    fn test_parse_mint_response_extracts_key() {
        let body = r#"{"id":"k1","key":"tskey-auth-xyz","expires":"2026-01-01T00:00:00Z"}"#;
        assert_eq!(parse_mint_response(body).unwrap(), "tskey-auth-xyz");
    }

    #[test]
    fn test_parse_mint_response_missing_key_errors() {
        let body = r#"{"id":"k1"}"#;
        assert!(parse_mint_response(body).is_err());
    }

    #[test]
    fn test_parse_mint_response_invalid_json_errors() {
        assert!(parse_mint_response("not json").is_err());
    }

    #[test]
    fn test_parse_oauth_token_response_extracts_access_token() {
        let body = r#"{"access_token":"tskey-api-abc","token_type":"Bearer","expires_in":3600}"#;
        assert_eq!(parse_oauth_token_response(body).unwrap(), "tskey-api-abc");
    }

    #[test]
    fn test_parse_oauth_token_response_missing_token_errors() {
        assert!(parse_oauth_token_response(r#"{"token_type":"Bearer"}"#).is_err());
    }

    #[test]
    fn test_resolve_mint_credential_none_when_unconfigured() {
        let resolved = ResolvedConfig::default();
        let cred = resolve_mint_credential(&resolved, |_| None).unwrap();
        assert!(cred.is_none());
    }

    #[test]
    fn test_resolve_mint_credential_pat_from_env() {
        let resolved = ResolvedConfig {
            tailscale_api_key_env: Some("TS_API_KEY".to_string()),
            ..Default::default()
        };
        let cred = resolve_mint_credential(&resolved, |name| {
            (name == "TS_API_KEY").then(|| "tskey-api-pat".to_string())
        })
        .unwrap();
        assert_eq!(cred, Some(MintCredential::Pat("tskey-api-pat".to_string())));
    }

    #[test]
    fn test_resolve_mint_credential_oauth_takes_precedence() {
        let resolved = ResolvedConfig {
            tailscale_api_key_env: Some("TS_API_KEY".to_string()),
            tailscale_oauth_client_id: Some("kABC".to_string()),
            tailscale_oauth_secret_env: Some("TS_OAUTH_SECRET".to_string()),
            ..Default::default()
        };
        let cred = resolve_mint_credential(&resolved, |name| match name {
            "TS_API_KEY" => Some("tskey-api-pat".to_string()),
            "TS_OAUTH_SECRET" => Some("secret-val".to_string()),
            _ => None,
        })
        .unwrap();
        assert_eq!(
            cred,
            Some(MintCredential::OAuth {
                client_id: "kABC".to_string(),
                secret: "secret-val".to_string(),
            })
        );
    }

    #[test]
    fn test_resolve_mint_credential_oauth_missing_secret_errors() {
        let resolved = ResolvedConfig {
            tailscale_oauth_client_id: Some("kABC".to_string()),
            tailscale_oauth_secret_env: Some("TS_OAUTH_SECRET".to_string()),
            ..Default::default()
        };
        let err = resolve_mint_credential(&resolved, |_| None).unwrap_err();
        assert!(err.contains("OAuth secret"), "unexpected error: {err}");
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -q --lib tailscale`
Expected: FAIL to compile — functions/enum not defined.

- [ ] **Step 4: Implement the module (above the test module)**

Insert this between the module doc comment and the `#[cfg(test)]` block in `src/tailscale.rs`:

```rust
use crate::context::ResolvedConfig;
use crate::context::tsk_config::expand_tilde;

/// Default tag applied to minted keys/nodes. Tagging is what scopes a sandbox's
/// tailnet access to its ACL instead of the operator's personal identity.
pub const DEFAULT_TAILSCALE_TAG: &str = "tag:tsk-sandbox";

const TAILSCALE_API_BASE: &str = "https://api.tailscale.com/api/v2";
const TAILSCALE_OAUTH_TOKEN_URL: &str = "https://api.tailscale.com/api/v2/oauth/token";
/// Minted keys are consumed immediately at container start; a short expiry
/// limits the blast radius if a key leaks before it is used.
const MINT_KEY_EXPIRY_SECS: u64 = 300;

/// A resolved credential for the Tailscale API.
#[derive(Debug, Clone, PartialEq)]
pub enum MintCredential {
    /// Personal access token, used directly as a bearer token.
    Pat(String),
    /// OAuth client credentials, exchanged for a short-lived access token.
    OAuth { client_id: String, secret: String },
}

/// Resolves the configured mint credential, reading secrets from the
/// environment (via `env_lookup`) or from files. Returns `Ok(None)` when no
/// mint credential is configured, so the caller falls back to a bring-your-own
/// key. OAuth takes precedence over a PAT when a client id is set.
///
/// Mirrors `crate::docker::resolve_tailscale_auth_key`: env access is injected
/// for testability; files are read directly.
pub fn resolve_mint_credential(
    resolved: &ResolvedConfig,
    env_lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<MintCredential>, String> {
    if let Some(ref client_id) = resolved.tailscale_oauth_client_id {
        let secret = read_secret(
            resolved.tailscale_oauth_secret_env.as_deref(),
            resolved.tailscale_oauth_secret_file.as_deref(),
            &env_lookup,
        )?
        .ok_or_else(|| {
            "tailscale_oauth_client_id is set but no OAuth secret was found. Set \
             tailscale_oauth_secret_env or tailscale_oauth_secret_file."
                .to_string()
        })?;
        return Ok(Some(MintCredential::OAuth {
            client_id: client_id.clone(),
            secret,
        }));
    }

    if resolved.tailscale_api_key_env.is_some() || resolved.tailscale_api_key_file.is_some() {
        let pat = read_secret(
            resolved.tailscale_api_key_env.as_deref(),
            resolved.tailscale_api_key_file.as_deref(),
            &env_lookup,
        )?
        .ok_or_else(|| {
            "A Tailscale API key source is configured but no key was found. Set the \
             tailscale_api_key_env variable or tailscale_api_key_file contents."
                .to_string()
        })?;
        return Ok(Some(MintCredential::Pat(pat)));
    }

    Ok(None)
}

/// Reads a secret from an env var (by name) then a file, trimming whitespace.
/// Returns `Ok(None)` when no source is configured or the env var is unset/blank;
/// a configured-but-empty file is an error.
fn read_secret(
    env_var: Option<&str>,
    file: Option<&str>,
    env_lookup: &impl Fn(&str) -> Option<String>,
) -> Result<Option<String>, String> {
    if let Some(name) = env_var {
        if let Some(val) = env_lookup(name) {
            let val = val.trim().to_string();
            if !val.is_empty() {
                return Ok(Some(val));
            }
        }
    }
    if let Some(path) = file {
        let path = expand_tilde(path);
        let contents = std::fs::read_to_string(&path).map_err(|e| {
            format!("Failed to read Tailscale secret file '{}': {e}", path.display())
        })?;
        let val = contents.trim().to_string();
        if val.is_empty() {
            return Err(format!("Tailscale secret file '{}' is empty", path.display()));
        }
        return Ok(Some(val));
    }
    Ok(None)
}

/// Builds the JSON body for `POST /tailnet/{tailnet}/keys`, requesting a
/// single-use, ephemeral, pre-authorized, tagged key.
pub fn build_mint_request_body(
    tags: &[String],
    expiry_secs: u64,
    description: &str,
) -> serde_json::Value {
    serde_json::json!({
        "capabilities": {
            "devices": {
                "create": {
                    "reusable": false,
                    "ephemeral": true,
                    "preauthorized": true,
                    "tags": tags,
                }
            }
        },
        "expirySeconds": expiry_secs,
        "description": description,
    })
}

/// Extracts the minted auth key from a `POST .../keys` response body.
pub fn parse_mint_response(body: &str) -> Result<String, String> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| format!("Tailscale mint response was not valid JSON: {e}"))?;
    json.get("key")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("Tailscale mint response had no `key` field: {body}"))
}

/// Extracts the access token from an OAuth `POST /oauth/token` response body.
pub fn parse_oauth_token_response(body: &str) -> Result<String, String> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| format!("Tailscale OAuth response was not valid JSON: {e}"))?;
    json.get("access_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("Tailscale OAuth response had no `access_token`: {body}"))
}

/// Mints a fresh ephemeral, tagged auth key for `task_id` using the configured
/// credential, reading secrets from the process environment. For OAuth it first
/// exchanges the client credentials for a short-lived access token.
///
/// Network-facing glue; request/response shaping is unit-tested via
/// [`build_mint_request_body`], [`parse_mint_response`], and
/// [`parse_oauth_token_response`].
pub async fn mint_tailscale_auth_key(
    resolved: &ResolvedConfig,
    task_id: &str,
) -> Result<String, String> {
    let credential = resolve_mint_credential(resolved, |name| std::env::var(name).ok())?
        .ok_or_else(|| "No Tailscale mint credential configured".to_string())?;

    let tags = resolved.tailscale_tags();
    if tags.is_empty() {
        return Err("tailscale_tags must not be empty when minting: Tailscale requires \
                    minted keys to be tagged."
            .to_string());
    }

    let client = reqwest::Client::new();

    let bearer = match credential {
        MintCredential::Pat(pat) => pat,
        MintCredential::OAuth { client_id, secret } => {
            let resp = client
                .post(TAILSCALE_OAUTH_TOKEN_URL)
                .form(&[
                    ("client_id", client_id.as_str()),
                    ("client_secret", secret.as_str()),
                ])
                .send()
                .await
                .map_err(|e| format!("Tailscale OAuth token request failed: {e}"))?;
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            if !status.is_success() {
                return Err(format!("Tailscale OAuth token request returned {status}: {text}"));
            }
            parse_oauth_token_response(&text)?
        }
    };

    let body = build_mint_request_body(
        &tags,
        MINT_KEY_EXPIRY_SECS,
        &format!("tsk sandbox tsk-{task_id}"),
    );
    let url = format!(
        "{TAILSCALE_API_BASE}/tailnet/{}/keys",
        resolved.tailscale_tailnet()
    );
    let resp = client
        .post(&url)
        .bearer_auth(&bearer)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Tailscale key mint request failed: {e}"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!(
            "Tailscale key mint returned {status}: {text}. Check the credential's scopes \
             (needs auth_keys write) and that it owns the tags {tags:?}."
        ));
    }
    parse_mint_response(&text)
}
```

- [ ] **Step 5: Declare the module**

In `src/main.rs`, next to `mod docker;` (`:9`), add in alphabetical position:

```rust
mod tailscale;
```

- [ ] **Step 6: Add the `ResolvedConfig` accessors**

In `src/context/tsk_config.rs`, immediately after the `tailscale_auth_key_env_var` method (ends around `:499`):

```rust
    /// Whether a Tailscale API mint credential (PAT or OAuth) is configured.
    /// When true, tsk mints a per-task ephemeral key instead of using a
    /// bring-your-own key.
    pub fn has_tailscale_mint_credential(&self) -> bool {
        self.tailscale_oauth_client_id.is_some()
            || self.tailscale_api_key_env.is_some()
            || self.tailscale_api_key_file.is_some()
    }

    /// Tailnet to mint keys in (default `-`, the credential's default tailnet).
    pub fn tailscale_tailnet(&self) -> &str {
        self.tailscale_tailnet.as_deref().unwrap_or("-")
    }

    /// Tags applied to minted keys/nodes (default `["tag:tsk-sandbox"]`).
    pub fn tailscale_tags(&self) -> Vec<String> {
        self.tailscale_tags
            .clone()
            .unwrap_or_else(|| vec![crate::tailscale::DEFAULT_TAILSCALE_TAG.to_string()])
    }
```

- [ ] **Step 7: Add accessor tests**

In the `#[cfg(test)]` module of `src/context/tsk_config.rs`:

```rust
#[test]
fn test_has_tailscale_mint_credential() {
    let none = ResolvedConfig::default();
    assert!(!none.has_tailscale_mint_credential());

    let pat = ResolvedConfig {
        tailscale_api_key_env: Some("TS_API_KEY".to_string()),
        ..Default::default()
    };
    assert!(pat.has_tailscale_mint_credential());

    let oauth = ResolvedConfig {
        tailscale_oauth_client_id: Some("kABC".to_string()),
        ..Default::default()
    };
    assert!(oauth.has_tailscale_mint_credential());
}

#[test]
fn test_tailscale_tailnet_and_tags_defaults() {
    let resolved = ResolvedConfig::default();
    assert_eq!(resolved.tailscale_tailnet(), "-");
    assert_eq!(resolved.tailscale_tags(), vec!["tag:tsk-sandbox".to_string()]);

    let custom = ResolvedConfig {
        tailscale_tailnet: Some("example.com".to_string()),
        tailscale_tags: Some(vec!["tag:ci".to_string()]),
        ..Default::default()
    };
    assert_eq!(custom.tailscale_tailnet(), "example.com");
    assert_eq!(custom.tailscale_tags(), vec!["tag:ci".to_string()]);
}
```

- [ ] **Step 8: Wire minting into `run_task_container`**

In `src/docker/mod.rs`, replace the auth-key resolution block (`:934-938`):

```rust
        let tailscale_auth_key = if resolved.tailscale {
            Some(resolve_tailscale_auth_key(&resolved)?)
        } else {
            None
        };
```

with:

```rust
        let tailscale_auth_key = if resolved.tailscale {
            if resolved.has_tailscale_mint_credential() {
                // Mint a fresh ephemeral, tagged key so the node auto-removes
                // from the tailnet after the sandbox stops.
                Some(crate::tailscale::mint_tailscale_auth_key(&resolved, &task.id).await?)
            } else {
                Some(resolve_tailscale_auth_key(&resolved)?)
            }
        } else {
            None
        };
```

- [ ] **Step 9: Run the new tests and the suite**

Run: `cargo test -q --lib tailscale` — Expected: all mint helper + resolve tests PASS.
Run: `cargo test -q --lib context::tsk_config` — Expected: accessor tests PASS.
Run: `cargo test -q` — Expected: full suite green.

- [ ] **Step 10: Lint clean**

Run: `just lint`
Expected: no warnings (all new items are reachable from the non-test build via the wiring in `run_task_container`).

- [ ] **Step 11: Commit**

```bash
git add Cargo.toml Cargo.lock src/tailscale.rs src/main.rs src/context/tsk_config.rs src/docker/mod.rs
git commit -m "feat: mint per-task ephemeral tagged Tailscale keys"
```

---

### Task 3: Warn when a bring-your-own node is untagged

Adds an in-container warning to the Tailscale startup script: after the node joins, if `tailscale status --json` shows the node has no tags, print a `tsk:` warning that it is using the operator's personal identity and (for non-ephemeral keys) will not auto-remove. Zero extra credential; only relevant on the fallback path (minted nodes are always tagged). jq is already in the base image (`dockerfiles/base/default.dockerfile:21`).

**Files:**
- Modify: `dockerfiles/features/tailscale.dockerfile` — the `printf '%s\n' ... > /usr/local/bin/tsk-tailscale-up` startup-script heredoc, right after the final `tailscale --socket="$SOCKET" status` line.

**Interfaces:**
- Consumes: nothing from other tasks (self-contained shell change).
- Produces: no Rust surface.

- [ ] **Step 1: Verify the tag-check predicate against sample JSON (the test)**

The predicate must exit 0 (warn) for an untagged node and non-zero (no warn) for a tagged node. Verify on the host (jq is installed locally):

Run:
```bash
echo '{"Self":{"Tags":null}}' | jq -e '((.Self.Tags // []) | length) == 0'; echo "untagged exit=$?"
echo '{"Self":{"Tags":["tag:tsk-sandbox"]}}' | jq -e '((.Self.Tags // []) | length) == 0'; echo "tagged exit=$?"
echo '{"Self":{}}' | jq -e '((.Self.Tags // []) | length) == 0'; echo "missing exit=$?"
```
Expected: `untagged exit=0`, `tagged exit=1`, `missing exit=0`.

- [ ] **Step 2: Add the warning to the startup script**

In `dockerfiles/features/tailscale.dockerfile`, the startup script is generated by a `printf '%s\n' '<line>' '<line>' ... > /usr/local/bin/tsk-tailscale-up` command where each line is a single-quoted argument. Immediately after the existing final line argument `'tailscale --socket="$SOCKET" status' \`, insert these argument lines (note: use **double quotes** around the jq filter so it survives the single-quoted printf argument — the filter contains no `$`, so nothing is interpolated at container runtime):

```
    '# Warn on an untagged node: it uses your personal tailnet identity and,' \
    '# with a non-ephemeral key, will not auto-remove. Minted keys are always tagged.' \
    'if tailscale --socket="$SOCKET" status --json | jq -e "((.Self.Tags // []) | length) == 0" >/dev/null 2>&1; then' \
    '    echo "tsk: WARNING - this sandbox node is UNTAGGED and uses your personal tailnet identity."' \
    '    echo "tsk:          Use a tagged, ephemeral auth key or configure key minting so nodes are"' \
    '    echo "tsk:          tagged and auto-remove. See the README Tailscale section."' \
    'fi' \
```

Do not use apostrophes in the echo text — an apostrophe would terminate the single-quoted printf argument. (The lines above are apostrophe-free.)

- [ ] **Step 3: Verify the generated script is well-formed**

Because building the image needs Docker and network, statically verify the edited script block instead: confirm the new lines are balanced single-quoted `printf` args and contain no stray apostrophes.

Run:
```bash
grep -n "UNTAGGED\|Self.Tags\|status --json | jq" dockerfiles/features/tailscale.dockerfile
```
Expected: the four/eight new lines appear, each wrapped as a `'...' \` printf argument.

Optional (only if Docker is available): `tsk docker build --dry-run` succeeds and the emitted Dockerfile contains the warning lines.

- [ ] **Step 4: Commit**

```bash
git add dockerfiles/features/tailscale.dockerfile
git commit -m "feat: warn when a bring-your-own Tailscale node is untagged"
```

---

### Task 4: Document minting and update config reference

Updates the README Tailscale section and config reference so minting is the recommended path (PAT vs OAuth trade-off), ephemeral+tagged is stated as the requirement for the bring-your-own fallback, and any implication that a plain reusable key auto-cleans is removed.

**Files:**
- Modify: `README.md` — the `### Tailscale (optional)` section and the `tsk.toml` Tailscale config example.

**Interfaces:**
- Consumes: field names and defaults from Tasks 1-2 (`tailscale_api_key_env`, `tailscale_api_key_file`, `tailscale_oauth_client_id`, `tailscale_oauth_secret_env`, `tailscale_oauth_secret_file`, `tailscale_tailnet`, `tailscale_tags`; default tag `tag:tsk-sandbox`; default tailnet `-`).
- Produces: no code surface.

- [ ] **Step 1: Update the config example**

In `README.md`, in the `[project.my-service]` Tailscale block (under `### Tailscale (optional)`), add after the existing `tailscale_auth_key_*` lines:

```toml
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

- [ ] **Step 2: Rewrite the cleanup guidance prose**

In the same section, add a paragraph after the "Recommended auth-key setup" guidance:

> **Cleanup of sandbox nodes.** Each sandbox joins as a `tsk-<task-id>` node. To keep the admin console from filling up with old nodes, the node must be **ephemeral** — Tailscale then auto-removes it a few minutes after the sandbox stops. The most robust way is to let tsk **mint the key**: configure a Tailscale API credential (a one-click **personal access token**, or an **OAuth client** for non-expiring, tightly-scoped access via `tailscale_api_key_env` / `tailscale_oauth_client_id`) and tsk creates a fresh single-use, ephemeral, tagged key per task — so nodes are always tagged (`tag:tsk-sandbox` by default) and always auto-remove, with no way to misconfigure them. If you instead bring your own key (`TS_AUTHKEY` / `tailscale_auth_key_file`), you are responsible for making it **ephemeral and tagged**; a non-ephemeral or untagged key leaves nodes behind and attributes them to your personal identity (the sandbox will print a warning when it detects an untagged node). tsk does not mint via short-lived per-task OAuth keys beyond this — a reusable ephemeral key or a minting credential is the practical setup today.

Adjust wording to match the surrounding README voice; remove any existing sentence implying a plain reusable key cleans itself up.

- [ ] **Step 3: Verify the docs**

Run:
```bash
grep -n "tailscale_api_key_env\|tailscale_oauth_client_id\|Cleanup of sandbox nodes\|tag:tsk-sandbox" README.md
cargo run -- --help > /dev/null
```
Expected: the new fields and cleanup paragraph are present; `--help` still runs cleanly.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: document Tailscale key minting and node cleanup"
```

---

### Task 5: Final verification

Runs the project's full gate to confirm the whole change is format-clean, lint-clean, and green before hand-off.

**Files:** none (verification only).

- [ ] **Step 1: Format**

Run: `just format`
Expected: no diff (or apply and re-commit formatting with `chore: cargo fmt` if any).

- [ ] **Step 2: Lint**

Run: `just lint`
Expected: no warnings across `--all-targets` and the default target.

- [ ] **Step 3: Test**

Run: `just test`
Expected: full suite green.

- [ ] **Step 4: Smoke-test the CLI**

Run: `cargo run -- --help > /dev/null`
Expected: exits 0.

- [ ] **Step 5: Commit any fixups**

```bash
git add -A
git commit -m "chore: formatting and lint fixups for Tailscale node cleanup" || echo "nothing to commit"
```

---

## Notes for the implementer

- **Why minting is opt-in and has no default env var:** `tailscale_auth_key_env` defaults to `TS_AUTHKEY`, but the mint credential must be explicitly configured (`tailscale_api_key_env` / `tailscale_oauth_client_id`). This prevents an unrelated `TS_API_KEY` in the environment from silently switching a user onto the minting path.
- **Why the ephemeral property does the cleanup, not a teardown hook:** ephemeral nodes auto-remove after going offline, so cleanup survives tsk crashing or the host rebooting mid-task. This is why the design deliberately has no host-side device-delete call or periodic sweep.
- **The mint call runs on the host**, so it does not interact with the container's Squid allowlist. The container still needs `.tailscale.com` / `.tailscale.io` allowlisted for `tailscaled`, which the existing `tailscale_squid_conf` already handles — no change needed there.
- **Pre-existing leftover nodes** are out of scope for the code; they are deleted manually once (already done during design).
