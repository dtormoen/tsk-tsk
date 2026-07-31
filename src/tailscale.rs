//! Host-side Tailscale auth-key minting.
//!
//! When a mint credential (PAT or OAuth client) is configured, tsk mints a
//! fresh single-use, ephemeral, tagged auth key per task via the Tailscale API
//! instead of reusing a long-lived key. Ephemeral nodes auto-remove from the
//! tailnet shortly after the sandbox stops, so the admin console does not
//! accumulate `tsk-*` devices.

use crate::context::ResolvedConfig;
use crate::context::tsk_config::expand_tilde;

/// Default tag applied to minted keys/nodes. Tagging is what scopes a sandbox's
/// tailnet access to its ACL instead of the operator's personal identity.
pub const DEFAULT_TAILSCALE_TAG: &str = "tag:tsk-sandbox";

const TAILSCALE_API_BASE: &str = "https://api.tailscale.com/api/v2";
const TAILSCALE_OAUTH_TOKEN_URL: &str = "https://api.tailscale.com/api/v2/oauth/token";
/// Minted keys are consumed immediately at container start; a short expiry
/// limits the blast radius if a key leaks before it is used. 10 minutes
/// gives slow hosts (e.g. pulling large images) margin to reach container
/// start before the key expires.
const MINT_KEY_EXPIRY_SECS: u64 = 600;

/// A resolved credential for the Tailscale API.
#[derive(Clone, PartialEq)]
pub enum MintCredential {
    /// Personal access token, used directly as a bearer token.
    Pat(String),
    /// OAuth client credentials, exchanged for a short-lived access token.
    OAuth { client_id: String, secret: String },
}

impl std::fmt::Debug for MintCredential {
    /// Redacts the secret so it never lands in logs or panic/assertion output.
    /// `client_id` is a non-secret identifier and is shown as-is.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MintCredential::Pat(_) => f.debug_tuple("Pat").field(&"<redacted>").finish(),
            MintCredential::OAuth { client_id, .. } => f
                .debug_struct("OAuth")
                .field("client_id", client_id)
                .field("secret", &"<redacted>")
                .finish(),
        }
    }
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

    let pat_configured =
        resolved.tailscale_api_key_env.is_some() || resolved.tailscale_api_key_file.is_some();
    let oauth_secret_configured = resolved.tailscale_oauth_secret_env.is_some()
        || resolved.tailscale_oauth_secret_file.is_some();
    if oauth_secret_configured && !pat_configured {
        return Err(
            "tailscale_oauth_secret_env or tailscale_oauth_secret_file is set but \
             tailscale_oauth_client_id is not. Set tailscale_oauth_client_id alongside the \
             OAuth secret."
                .to_string(),
        );
    }

    if pat_configured {
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
    if let Some(name) = env_var
        && let Some(val) = env_lookup(name)
    {
        let val = val.trim().to_string();
        if !val.is_empty() {
            return Ok(Some(val));
        }
    }
    if let Some(path) = file {
        let path = expand_tilde(path);
        let contents = std::fs::read_to_string(&path).map_err(|e| {
            format!(
                "Failed to read Tailscale secret file '{}': {e}",
                path.display()
            )
        })?;
        let val = contents.trim().to_string();
        if val.is_empty() {
            return Err(format!(
                "Tailscale secret file '{}' is empty",
                path.display()
            ));
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
        return Err(
            "tailscale_tags must not be empty when minting: Tailscale requires \
                    minted keys to be tagged."
                .to_string(),
        );
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
                return Err(format!(
                    "Tailscale OAuth token request returned {status}: {text}"
                ));
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
        assert_eq!(
            body["description"],
            serde_json::json!("tsk sandbox tsk-abc123")
        );
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

    #[test]
    fn test_resolve_mint_credential_oauth_secret_without_client_id_errors() {
        let resolved = ResolvedConfig {
            tailscale_oauth_secret_env: Some("TS_OAUTH_SECRET".to_string()),
            ..Default::default()
        };
        let err = resolve_mint_credential(&resolved, |name| {
            (name == "TS_OAUTH_SECRET").then(|| "secret-val".to_string())
        })
        .unwrap_err();
        assert!(
            err.contains("tailscale_oauth_client_id"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_resolve_mint_credential_oauth_secret_without_client_id_falls_back_to_pat() {
        let resolved = ResolvedConfig {
            tailscale_oauth_secret_env: Some("TS_OAUTH_SECRET".to_string()),
            tailscale_api_key_env: Some("TS_API_KEY".to_string()),
            ..Default::default()
        };
        let cred = resolve_mint_credential(&resolved, |name| match name {
            "TS_OAUTH_SECRET" => Some("secret-val".to_string()),
            "TS_API_KEY" => Some("tskey-api-pat".to_string()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cred, Some(MintCredential::Pat("tskey-api-pat".to_string())));
    }

    #[test]
    fn test_mint_credential_debug_redacts_secret() {
        let debug_str = format!("{:?}", MintCredential::Pat("tskey-secret".to_string()));
        assert!(
            !debug_str.contains("tskey-secret"),
            "debug output leaked the secret: {debug_str}"
        );
        assert!(
            debug_str.contains("redacted"),
            "debug output did not redact: {debug_str}"
        );
    }
}
