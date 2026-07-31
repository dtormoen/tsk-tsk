# Tailscale support (injected when `tailscale = true`)
#
# Installs the official static tailscale/tailscaled binaries and a small startup
# script that `tsk` runs before the agent command. tailscaled runs as the
# unprivileged `agent` user:
#   - cap_net_admin is granted as a file capability so the TUN device can be set
#     up without root (tsk adds NET_ADMIN back to the container for this).
#   - If no TUN device is available the startup script falls back to Tailscale's
#     userspace networking mode.
USER root
# iptables + iproute2 let tailscaled program its netfilter chains and routing
# table in kernel/TUN mode (without them, `--accept-routes` silently no-ops).
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends iptables iproute2; \
    rm -rf /var/lib/apt/lists/*; \
    arch="$(dpkg --print-architecture)"; \
    version="$(curl -fsSL 'https://pkgs.tailscale.com/stable/?mode=json' | jq -r .TarballsVersion)"; \
    curl -fsSL "https://pkgs.tailscale.com/stable/tailscale_${version}_${arch}.tgz" -o /tmp/tailscale.tgz; \
    tar -xzf /tmp/tailscale.tgz -C /tmp; \
    install -m 0755 "/tmp/tailscale_${version}_${arch}/tailscale" /usr/local/bin/tailscale; \
    install -m 0755 "/tmp/tailscale_${version}_${arch}/tailscaled" /usr/local/sbin/tailscaled; \
    rm -rf /tmp/tailscale.tgz "/tmp/tailscale_${version}_${arch}"; \
    setcap cap_net_admin+eip /usr/local/sbin/tailscaled; \
    mkdir -p /var/lib/tailscale /var/run/tailscale; \
    chown -R agent:agent /var/lib/tailscale /var/run/tailscale

# Startup script: brings the sandbox onto the tailnet before the agent runs.
# TS_AUTHKEY, TSK_TAILSCALE_HOSTNAME, TSK_TAILSCALE_ACCEPT_ROUTES and
# TSK_TAILSCALE_UP_ARGS are supplied by tsk as container environment variables;
# the key is never baked into the image.
RUN printf '%s\n' \
    '#!/bin/sh' \
    '# Brings this sandbox onto the tailnet. Started by tsk when tailscale is enabled.' \
    'set -eu' \
    ': "${TS_AUTHKEY:?TS_AUTHKEY is not set}"' \
    'STATE_DIR=/var/lib/tailscale' \
    'SOCKET=/var/run/tailscale/tailscaled.sock' \
    'mkdir -p "$STATE_DIR" /var/run/tailscale' \
    'if [ -w /dev/net/tun ]; then' \
    '    TUN_ARGS=""' \
    'else' \
    '    echo "tsk: /dev/net/tun is unavailable, using userspace networking"' \
    '    # SOCKS5 (1055) and HTTP (1056) must be distinct ports — binding both to' \
    '    # the same port silently drops one. In userspace mode tsk sets ALL_PROXY' \
    '    # to the SOCKS5 listener so the tailnet is reachable.' \
    '    TUN_ARGS="--tun=userspace-networking --socks5-server=localhost:1055 --outbound-http-proxy-listen=localhost:1056"' \
    'fi' \
    '# shellcheck disable=SC2086' \
    'tailscaled --statedir="$STATE_DIR" --socket="$SOCKET" $TUN_ARGS >/tmp/tailscaled.log 2>&1 &' \
    'i=0' \
    'while [ ! -S "$SOCKET" ] && [ "$i" -lt 30 ]; do sleep 1; i=$((i + 1)); done' \
    'if [ ! -S "$SOCKET" ]; then' \
    '    echo "tsk: tailscaled failed to start:"' \
    '    cat /tmp/tailscaled.log' \
    '    exit 1' \
    'fi' \
    '# Subnet routes are opt-in (tsk sets TSK_TAILSCALE_ACCEPT_ROUTES): accepted' \
    '# routes are reachable over the tailnet, bypassing the Squid allowlist.' \
    'ACCEPT_ROUTES=""' \
    '[ "${TSK_TAILSCALE_ACCEPT_ROUTES:-false}" = "true" ] && ACCEPT_ROUTES="--accept-routes"' \
    '# --timeout makes an unreachable control plane fail the task instead of hanging.' \
    '# shellcheck disable=SC2086' \
    'tailscale --socket="$SOCKET" up --timeout=60s --authkey "$TS_AUTHKEY" \' \
    '    --hostname "${TSK_TAILSCALE_HOSTNAME:-tsk}" \' \
    '    --accept-dns=false $ACCEPT_ROUTES ${TSK_TAILSCALE_UP_ARGS:-}' \
    'tailscale --socket="$SOCKET" status' \
    '# Warn on an untagged node: it uses your personal tailnet identity and,' \
    '# with a non-ephemeral key, will not auto-remove. Minted keys are always tagged.' \
    'if tailscale --socket="$SOCKET" status --json | jq -e "((.Self.Tags // []) | length) == 0" >/dev/null 2>&1; then' \
    '    echo "tsk: WARNING - this sandbox node is UNTAGGED and uses your personal tailnet identity."' \
    '    echo "tsk:          Use a tagged, ephemeral auth key or configure key minting so nodes are"' \
    '    echo "tsk:          tagged and auto-remove. See the README Tailscale section."' \
    'fi' \
    > /usr/local/bin/tsk-tailscale-up && \
    chmod 0755 /usr/local/bin/tsk-tailscale-up
USER agent
