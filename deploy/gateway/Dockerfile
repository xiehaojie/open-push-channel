FROM node:24-bookworm-slim

WORKDIR /app/admin-backend
COPY admin-backend/package.json admin-backend/package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force

WORKDIR /app
COPY admin-backend/src ./admin-backend/src
COPY protocol/index.cjs protocol/plugin.cjs ./protocol/
COPY frontend-demo/index.html frontend-demo/sdk.js frontend-demo/mentions.js frontend-demo/style.css ./frontend-demo/
RUN mkdir -p /var/lib/open-push-channel && chown node:node /var/lib/open-push-channel

ENV HOST=0.0.0.0 PORT=3003
EXPOSE 3003
USER node
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
  CMD node -e "fetch('http://127.0.0.1:3003/healthz').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"
ENTRYPOINT ["node", "/app/admin-backend/src/gateway/server.js"]
CMD ["--config", "/etc/open-push-channel/gateway.json"]
