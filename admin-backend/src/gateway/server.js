const http = require("node:http");
const { createReadStream } = require("node:fs");
const path = require("node:path");
const Koa = require("koa");
const bodyParser = require("koa-bodyparser");
const cors = require("@koa/cors");
const { initWebSocket, runtimeCatalog } = require("../websocket");
const pushController = require("../controllers/pushController");
const { GatewayStore } = require("./store");
const { isLocalGatewayRequest, sameToken } = require("./auth");

function createGatewayServer(options) {
  const app = new Koa();
  if (options?.auth) {
    app.use(cors());
  } else {
    app.use(async (ctx, next) => {
      if (!isLocalGatewayRequest(ctx.req)) {
        ctx.status = 403;
        return;
      }
      await next();
    });
  }
  app.use(bodyParser());
  app.use(async (ctx, next) => {
    // Enforce authorization in the controller so every supported push alias
    // shares the same policy instead of relying on a separate path predicate.
    ctx.state.pushAuthorized =
      !options?.auth ||
      sameToken(ctx.get("Authorization").replace(/^Bearer /, ""), options.auth.pushToken);
    await next();
  });
  app.use(async (ctx) => {
    if (ctx.method === "GET") {
      if (ctx.path === "/api/runtimes") {
        if (
          options?.auth &&
          !sameToken(ctx.get("Authorization").replace(/^Bearer /, ""), options.auth.pushToken)
        ) {
          ctx.status = 401;
          return;
        }
        ctx.body = runtimeCatalog();
        return;
      }
      if (ctx.path === "/healthz") {
        ctx.body = { status: "ok" };
        return;
      }
      if (ctx.path === "/") {
        ctx.redirect("/demo/");
        return;
      }
      const assets = {
        "/demo/": "index.html",
        "/demo/index.html": "index.html",
        "/demo/sdk.js": "sdk.js",
        "/demo/mentions.js": "mentions.js",
        "/demo/style.css": "style.css",
      };
      if (Object.hasOwn(assets, ctx.path)) {
        const filename = assets[ctx.path];
        ctx.type = path.extname(filename);
        ctx.set("Cache-Control", "no-store");
        ctx.body = createReadStream(path.resolve(__dirname, "../../../frontend-demo", filename));
        return;
      }
    }
    if (ctx.method === "POST" && (ctx.path === "/send" || ctx.path === "/api/send")) {
      await pushController.send(ctx);
    }
    if (ctx.method === "POST" && ctx.path === "/api/tasks") {
      if (!ctx.state.pushAuthorized) {
        ctx.status = 401;
        return;
      }
      const body = ctx.request.body;
      const required = ["clientId", "sessionId", "eventId", "content"];
      const optional = ["runtimeId", "agentId"];
      const valid =
        body &&
        typeof body === "object" &&
        !Array.isArray(body) &&
        required.every((field) => typeof body[field] === "string" && body[field].trim()) &&
        optional.every(
          (field) =>
            body[field] === undefined || (typeof body[field] === "string" && body[field].trim()),
        ) &&
        Object.keys(body).every((field) => required.includes(field) || optional.includes(field));
      if (!valid) {
        ctx.status = 400;
        ctx.body = "Invalid task: provide clientId, sessionId, eventId and content";
        return;
      }
      try {
        const receipt = server.submitTask(body);
        ctx.status = receipt ? 202 : 404;
        ctx.body = receipt ?? "Session not found or target mismatch";
      } catch (error) {
        ctx.status = error.status === 403 ? 403 : 400;
        ctx.body = error.message;
      }
    }
  });
  const server = http.createServer(app.callback());
  const configured = options?.databasePath
    ? { ...options, store: new GatewayStore({ path: options.databasePath }) }
    : options;
  initWebSocket(server, configured);
  return server;
}

function installShutdown(server) {
  const stop = () => {
    void server.shutdown().then(
      () => process.exit(0),
      () => process.exit(1),
    );
  };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
}

if (require.main === module) {
  require("dotenv").config();
  const port = process.env.PORT || 3001;
  const host = process.env.HOST || "127.0.0.1";
  const configIndex = process.argv.indexOf("--config");
  if (configIndex !== -1 && !process.argv[configIndex + 1]) {
    throw new Error("--config requires a path");
  }
  const options = require("./runtime-config").loadRuntimeConfig(
    configIndex === -1 ? undefined : process.argv[configIndex + 1],
  );
  if (host !== "127.0.0.1" && host !== "::1" && !options.auth) {
    throw new Error("A non-loopback Push Gateway listener requires gateway auth");
  }
  const server = createGatewayServer(options);
  installShutdown(server);
  server.listen(port, host, () => {
    console.log(`Push gateway listening on http://${host}:${port}`);
  });
}

module.exports = { createGatewayServer, installShutdown };
