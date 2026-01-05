# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Overview

This is a pnpm monorepo containing a pure-protocol TypeScript Kafka client and streaming library. Unlike other clients, this is not a wrapper around existing libraries - it implements the Kafka protocol directly.

## Packages

| Package | Description |
|---------|-------------|
| `@kafkats/client` | Core Kafka client (producer, consumer, protocol) |
| `@kafkats/flow` | Kafka Streams-like DSL for stream processing |
| `@kafkats/flow-state-lmdb` | Persistent LMDB state stores for flow |
| `@kafkats/codec-zod` | Zod-based serialization codecs |
| `@kafkats/benchmark` | Performance benchmarks vs kafkajs |
| `docs` | VitePress documentation site |

Each package has its own `AGENTS.md` with package-specific guidance. **Read the package's AGENTS.md for testing instructions before running tests.**

## Versioning

All publishable packages (`@kafkats/client`, `@kafkats/flow`, `@kafkats/codec-zod`, `@kafkats/flow-state-lmdb`) are released in lockstep and share the same version number for minor and major releases. When creating changesets, include all four packages with the same bump type.

## Development Commands

```bash
# Install dependencies
pnpm install

# Linting and formatting (from root)
pnpm lint          # Check all packages
pnpm lint:fix      # Fix lint issues
pnpm format        # Format all files

# Package-specific commands (use -C flag)
pnpm -C packages/client build
pnpm -C packages/client test:unit
pnpm -C packages/flow build
pnpm -C packages/flow test

# Documentation
pnpm docs:dev      # Start dev server
pnpm docs:build    # Build static site
```

## Testing

**Read the package-specific `AGENTS.md` for detailed testing instructions.** Each package has different test commands and requirements.

General notes:
- Integration tests use testcontainers to spin up Kafka brokers (Docker required)
- Default parallelism: 3 workers (configurable via `VITEST_MAX_WORKERS`)
- Set `KAFKA_TS_LOG_LEVEL=debug` for verbose client logs

## Code Style

- **Formatting**: Prettier with tabs, 120 char width, single quotes, no semicolons
- **Path aliases**: `@/*` maps to `src/*` in each package
- **Module system**: ESM (`"type": "module"`)
