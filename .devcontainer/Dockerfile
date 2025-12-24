FROM mcr.microsoft.com/devcontainers/typescript-node:1-20-bookworm

# Some CLIs (e.g. codex auth flows) expect Python to be present.
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 \
  && rm -rf /var/lib/apt/lists/*

# Codex CLI (OpenAI)
RUN npm install -g @openai/codex@latest

# Create directories up-front so bind-mount targets exist.
RUN mkdir -p /home/node/.claude /home/node/.codex \
  && chown -R node:node /home/node/.claude /home/node/.codex
