FROM node:26-trixie-slim AS build

ENV PNPM_HOME=/pnpm
ENV PATH=$PNPM_HOME:$PATH

# Node 25+ no longer bundles corepack; pin pnpm to the workspace packageManager version.
RUN npm install -g pnpm@10.11.0

WORKDIR /app

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc turbo.json ./
COPY packages/client/package.json packages/client/package.json
COPY packages/lag/package.json packages/lag/package.json

RUN pnpm install --frozen-lockfile --filter @kafkats/lag...

COPY packages/client packages/client
COPY packages/lag packages/lag

RUN pnpm --filter @kafkats/lag... build \
	&& pnpm --filter @kafkats/lag deploy --legacy --prod /app/deploy

# Distroless: no shell, package manager, or libc utilities in the final image. Runs as uid 65532.
FROM gcr.io/distroless/nodejs26-debian13:nonroot AS runtime

LABEL org.opencontainers.image.source="https://github.com/chrisrecalis/kafkats"

ENV NODE_ENV=production

WORKDIR /app
COPY --from=build --chown=nonroot:nonroot /app/deploy ./

EXPOSE 9464

# Exec form throughout: distroless has no shell.
HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
	CMD ["/nodejs/bin/node", "-e", "fetch(`http://127.0.0.1:${process.env.KAFKA_LAG_PORT||9464}/healthz`).then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"]

ENTRYPOINT ["/nodejs/bin/node", "dist/cli.js"]
