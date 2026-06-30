# Claude agent layer

RUN curl -fsSL https://claude.ai/install.sh | bash

# Wait indefinitely for background tasks (sub-agents, bash) in headless mode.
# Override via the `env` config to cap the wait.
ENV CLAUDE_CODE_PRINT_BG_WAIT_CEILING_MS=0
