# Tailscale sandbox node cleanup — design

**Date:** 2026-07-31
**Status:** Approved design, pending implementation plan
**Branch:** `feat/tailscale-support` (the Tailscale feature is unshipped — not in `main`, not in any tag; latest release is v0.10.8, so there is no backward-compat obligation for the Tailscale path)

## Problem

The just-added Tailscale support (`22887dd`) joins each sandbox to the tailnet as a
node named `tsk-<task-id>` but **never removes it**. The in-container startup script
runs `tailscale up --authkey ...` with no `--ephemeral` (which does not exist as an
`up` flag anyway) and there is no teardown step. Whether a node disappears depends
entirely on whether the auth key is *ephemeral*.

Observed: the console accumulated 6 `tsk-*` nodes, all **non-ephemeral** and
**untagged** — attributed to the operator's personal identity. Non-ephemeral means
Tailscale never auto-removes them; untagged means each sandbox held the operator's
full personal tailnet access (the exact footgun the README warns against).

Root cause: a hand-made key can be misconfigured (non-ephemeral and/or untagged), and
nothing in tsk detects or corrects it.

## Key technical constraints (established during design)

- **Ephemerality is a property of the auth key**, fixed at key-creation time. There is
  **no `tailscale up --ephemeral` flag** — tsk cannot force a node ephemeral from
  inside the container.
- **Ephemeral nodes auto-remove** a few minutes after they go offline (container
  stops). This is the cleanup mechanism, and it is robust even if tsk crashes or the
  host reboots mid-task — nothing host-side needs to fire.
- **Key minting requires the Tailscale API** (`POST /tailnet/{tailnet}/keys`). The
  `tailscale` CLI has **no key-creation command** (it can join, show status, and
  `logout`, but not create keys or delete other devices).
- The mint API accepts **either** a Personal Access Token (PAT) **or** an OAuth
  client — both are bearer credentials against the same endpoint.
- OAuth-minted keys **must** be tagged and the OAuth client must own the tag —
  tagging is structurally enforced for OAuth.

## Approach

Ephemeral-first, minting optional, no OAuth mandate. Two layers:

### Layer 1 — Primary: mint a fresh ephemeral tagged key per task (opt-in)

If the operator configures a Tailscale API credential (PAT **or** OAuth client),
tsk mints a **single-use, ephemeral, tagged** auth key per sandbox at container
start, passes it in as the join key, and discards it.

- The node is **always ephemeral** → auto-removes on disconnect (solves the leftover
  machines problem).
- The node is **always tagged** (`tag:tsk-sandbox` by default) → no more
  personal-identity nodes; sandbox access is bounded by the tag's ACL, not the
  operator's identity.
- The only durable secret is the API credential, which lives on the **host** (trusted)
  and is **never** passed into the container. Only the short-lived minted key enters
  the container — and the existing `unset TS_AUTHKEY` + `exec` handling already keeps
  the in-container agent from recovering it.

Credential types supported (operator picks one):

- **PAT** — used directly as a bearer token. One-click to create; expires (≤90 days,
  operator rotates); carries the creating user's permissions.
- **OAuth client** — `POST /oauth/token` (client-credentials grant) to obtain a
  short-lived access token, then mint. Non-expiring, tightly scopable
  (`auth_keys` + owner of the tag). More setup.

### Layer 2 — Fallback: bring-your-own key (existing path, retained)

For operators who do not want to hand tsk an API credential, the existing
`TS_AUTHKEY` / `tailscale_auth_key_file` path remains. Cleanup then depends on the
operator supplying an **ephemeral, tagged** key (documented as required).

To catch misconfiguration without any extra credential: the container startup script
already runs `tailscale status` after join. Parse `tailscale status --json` for
`Self.Tags`; if empty, print a clear `tsk:` warning that the node is untagged (using
the operator's personal identity) and — because ephemerality can't be reliably
detected client-side — a one-line reminder that the node only auto-cleans if the key
is ephemeral. Warning only; never fails the task.

### De-scoped (deliberately not building)

- **Per-task device-delete API call** and **periodic device sweep** — unnecessary once
  every node is ephemeral. Adds host-side bookkeeping and a delete scope for no gain
  over auto-removal.
- **Mint-only / replace-the-bring-your-own path** — the fallback costs little and
  serves no-API operators.
- **The 6 existing leftover nodes** — one-time manual deletion in the console; nothing
  new accumulates going forward.

## Config surface

New fields, mirroring the existing `tailscale_auth_key_env` / `tailscale_auth_key_file`
naming (secrets referenced by env-var name or file path, never inlined):

```toml
[project.my-service]
tailscale = true

# --- Layer 1: minting (opt-in). Provide PAT *or* OAuth, not both. ---
# PAT:
tailscale_api_key_env  = "TS_API_KEY"          # env var holding a PAT
# tailscale_api_key_file = "~/.config/tsk/ts-api-key"
# OAuth client:
# tailscale_oauth_client_id      = "k123..."   # client id (not secret)
# tailscale_oauth_secret_env     = "TS_OAUTH_SECRET"
# tailscale_oauth_secret_file    = "~/.config/tsk/ts-oauth-secret"

tailscale_tailnet = "-"                          # default "-" = credential's default tailnet
tailscale_tags    = ["tag:tsk-sandbox"]          # tags for minted keys/nodes

# --- Layer 2: bring-your-own key (fallback, unchanged) ---
# tailscale_auth_key_env  = "TS_AUTHKEY"
# tailscale_auth_key_file = "~/.config/tsk/ts-authkey"
```

Resolution precedence at container start (host side):
1. If a mint credential is configured (PAT or OAuth) → mint an ephemeral tagged key.
2. Else fall back to `tailscale_auth_key_env` / `tailscale_auth_key_file`.
3. Else the existing "no key found" error.

## Components / where the code lives

- **`src/docker/mod.rs` — key resolution.** Extend the current
  `resolve_tailscale_auth_key` path: when a mint credential is present, call the new
  minter and return its key; otherwise keep today's env/file behavior. Minting happens
  at **container start** (fresh, short-lived key matched to container lifetime), not at
  task creation — consistent with today's "read the key at start" model. Only the
  credential *references* (env names, tailnet, tags) live in the config snapshot; the
  secret itself is read live from the host.
- **New minter unit** (e.g. `src/tailscale/mint.rs` or a submodule of `docker`):
  one clear job — given a resolved credential + tailnet + tags + node description,
  return a fresh key string. Internally: optional OAuth token exchange, then
  `POST /tailnet/{tailnet}/keys` with
  `capabilities.devices.create = { reusable:false, ephemeral:true, preauthorized:true, tags:[...] }`
  and a short `expirySeconds` (e.g. 300 — the key only needs to be valid at join).
  Injectable HTTP + credential lookup so it is unit-testable without network.
- **`dockerfiles/features/tailscale.dockerfile` — startup script.** After the existing
  `tailscale status`, add the untagged-node warning (parse `--json`, check
  `Self.Tags`). No behavior change to the join itself.
- **`src/context/tsk_config.rs` — config.** Add the new fields + accessors alongside
  the existing Tailscale config, following the layered-resolution pattern.
- **`Cargo.toml` — HTTP client.** No HTTP client is currently a direct dependency; add
  one for the mint call (prefer `reqwest` with `rustls-tls` to avoid an OpenSSL
  system dep). Confirm during implementation whether a transitive client can be reused
  before adding a new dependency.

## Error handling

- Mint failure (bad/expired credential, network, unreachable API) → fail the task with
  a clear, actionable error, consistent with the existing "join within 60s or fail"
  behavior. Do not silently fall back to the bring-your-own key when a mint credential
  was explicitly configured — a misconfigured credential should surface, not be masked.
- Missing tag in bring-your-own mode → warning only (never fails).
- The mint call is an outbound HTTPS request **from the host** to `api.tailscale.com`,
  so it does not interact with the container's Squid allowlist. (The container still
  needs `.tailscale.com` / `.tailscale.io` allowlisted for `tailscaled`, already
  handled.)

## Testing

- **Minter unit tests** (real logic, injected HTTP): OAuth token-exchange path; PAT
  path; correct request body (ephemeral/tagged/non-reusable); error surfaced on
  non-2xx; tailnet defaulting to `-`.
- **Config tests**: precedence (mint credential wins over bring-your-own), env-vs-file
  resolution, tag/tailnet defaults, snapshot round-trip.
- **Startup-script warning**: unit-test the `Self.Tags` parsing that drives the warning
  (reuse the existing `parse_tailnet_aliases` JSON-parsing test style), keeping the
  container hermetic under `cfg(test)`.
- Prefer real implementations over mocks per repo conventions; the only mock boundary
  is the outbound HTTP call.

## Docs

Update the README Tailscale section: minting as the recommended path (PAT vs OAuth
trade-off), ephemeral+tagged as the requirement for the fallback path, and remove any
implication that a plain reusable key auto-cleans. Note the one-time manual deletion of
pre-existing nodes.
