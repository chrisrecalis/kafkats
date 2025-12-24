# Dev Container (Claude Code + Codex)

This devcontainer installs:

- Claude Code (`claude`)
- OpenAI Codex CLI (`codex`)

## Host authentication

This devcontainer reuses your host machine auth by bind-mounting:

- `~/.claude` and `~/.claude.json` (Claude Code)
- `~/.codex` (Codex CLI)

If you don't have these yet, run the CLIs once on your host to sign in and create them.

As an alternative (and often simpler in containers), export API keys on your host and the
devcontainer will pass them through:

- `ANTHROPIC_API_KEY`
- `OPENAI_API_KEY`

## Notes from upstream docs

- Codex stores auth in `$CODEX_HOME/auth.json` (default: `~/.codex/auth.json`). Copying or mounting that file is the recommended way to use Codex on a headless machine/container.
- Claude’s docs recommend extra isolation for devcontainers (separate container/user, minimal privileges). They also provide an optional firewall-based “reference devcontainer” for running Claude with fewer prompts.

## Troubleshooting

- If `pnpm install` fails with `EACCES` writing into `node_modules`, rebuild the container. The `postCreate.sh` script also `chown`s the `node_modules` volume on first run.
- If `pnpm` warns about “Ignored build scripts”, run `pnpm approve-builds` and allow the tools you need (commonly `esbuild`).

## Notes

- Mounting auth into a container means any code running in the container can read it. Only
  use this with repos you trust.
