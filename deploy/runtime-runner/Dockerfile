FROM node:24-bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates git python3 make g++ ripgrep \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/admin-backend
COPY admin-backend/package.json admin-backend/package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund \
    && npm install --global --no-audit --no-fund @deepseek-ai/dsh@0.2.1-rc.2 @earendil-works/pi-coding-agent@1.3.0 \
    && npm cache clean --force

WORKDIR /app
# Explicit allowlist: never send local gateway configs, .env or native homes
# into image layers. Build from the push-channel package root.
COPY admin-backend/src ./admin-backend/src
COPY admin-backend/package.json ./admin-backend/package.json
COPY protocol/index.cjs protocol/plugin.cjs ./protocol/
COPY frontend-demo/index.html frontend-demo/sdk.js frontend-demo/mentions.js frontend-demo/style.css ./frontend-demo/
COPY deploy/runtime-runner/bootstrap.cjs ./deploy/runtime-runner/bootstrap.cjs

ENV HOST=0.0.0.0 PORT=3003
EXPOSE 3003
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD node -e "fetch('http://127.0.0.1:3003/healthz').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"
ENTRYPOINT ["node", "/app/deploy/runtime-runner/bootstrap.cjs"]
CMD ["--config", "/etc/push-gateway/gateway.json"]
