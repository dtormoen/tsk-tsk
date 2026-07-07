# Claude agent layer

RUN curl -fsSL https://claude.ai/install.sh | bash

# Wait indefinitely for background sub-agents in headless mode.
# Override via the `env` config to cap the wait.
ENV CLAUDE_CODE_PRINT_BG_WAIT_CEILING_MS=0

# Keep long commands (e.g. `just precommit`) in the foreground instead of
# auto-backgrounding at the 2-minute default. Headless `-p` kills backgrounded
# shells ~5s after the final result, so a long command must block its turn.
ENV BASH_DEFAULT_TIMEOUT_MS=1800000
ENV BASH_MAX_TIMEOUT_MS=1800000
