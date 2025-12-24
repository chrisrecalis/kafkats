#!/usr/bin/env bash
set -euo pipefail

if ! command -v pnpm >/dev/null 2>&1; then
  sudo npm install -g pnpm
fi

# If `node_modules` is a Docker volume, it may be root-owned on first create.
if [[ ! -d "node_modules" ]]; then
  sudo mkdir -p node_modules
fi
if [[ ! -w "node_modules" ]]; then
  sudo chown -R "$(id -u)":"$(id -g)" node_modules
fi

pnpm install

if ! command -v claude >/dev/null 2>&1; then
  echo "Claude Code CLI not found in PATH (expected: claude)."
fi

if ! command -v codex >/dev/null 2>&1; then
  echo "Codex CLI not found in PATH (expected: codex)."
fi

if [[ ! -f "$HOME/.claude.json" && -z "${ANTHROPIC_API_KEY:-}" ]]; then
  echo "Claude auth not detected. Either mount ~/.claude.json from the host or set ANTHROPIC_API_KEY on the host."
fi

if [[ ! -f "$HOME/.codex/auth.json" && -z "${OPENAI_API_KEY:-}" ]]; then
  echo "Codex auth not detected. Either mount ~/.codex from the host or set OPENAI_API_KEY on the host."
fi
