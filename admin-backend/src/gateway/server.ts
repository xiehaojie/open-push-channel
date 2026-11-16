import http from "node:http";
import { createReadStream } from "node:fs";
import path from "node:path";
import Koa from "koa";
import dotenv from "dotenv";
import bodyParser from "koa-bodyparser";
import cors from "@koa/cors";
import { initWebSocket, runtimeCatalog } from "../websocket/index.ts";
import type { GatewayServer, TaskPayload, WebSocketOptions } from "../websocket/index.ts";
import pushController from "../controllers/pushController.ts";
import { GatewayStore } from "./store.ts";
import { isLocalGatewayRequest, sameToken } from "./auth.ts";
import { loadRuntimeConfig } from "./runtime-config.ts";
import { isMain } from "./cli-entry.ts";

const assets: Record<string, string> = {
  "/demo/": "index.html",
  "/demo/index.html": "index.html",
  "/demo/sdk.js": "sdk.js",
  "/demo/mentions.js": "mentions.js",
  "/demo/demo.js": "demo.js",
  "/demo/style.css": "style.css",
};

function isTaskPayload(value: unknown): value is TaskPayload {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const body = value as Record<string, unknown>;
  const required = ["clientId", "sessionId", "eventId", "content"];
  const optional = ["runtimeId", "agentId"];
  const hasText = (field: string) => {
    const value = body[field];
    return typeof value === "string" && Boolean(value.trim());
  };
  return (
    required.every(hasText) &&
    optional.every((field) => body[field] === undefined || hasText(field)) &&
    Object.keys(body).every((field) => required.includes(field) || optional.includes(field))
  );
}

function createGatewayServer(options?: WebSocketOptions & { databasePath?: string }) {
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
      if (Object.hasOwn(assets, ctx.path)) {
        const filename = assets[ctx.path];
        ctx.type = path.extname(filename);
        ctx.set("Cache-Control", "no-store");
        ctx.body = createReadStream(
          path.resolve(import.meta.dirname, "../../../frontend-demo/dist", filename),
        );
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
      if (!isTaskPayload(body)) {
        ctx.status = 400;
        ctx.body = "Invalid task: provide clientId, sessionId, eventId and content";
        return;
      }
      try {
        const receipt = server.submitTask(body);
        ctx.status = receipt ? 202 : 404;
        ctx.body = receipt ?? "Session not found or target mismatch";
      } catch (error) {
        ctx.status = (error as { status?: number }).status === 403 ? 403 : 400;
        ctx.body = (error as Error).message;
      }
    }
  });
  const server = http.createServer(app.callback()) as GatewayServer;
  const configured = options?.databasePath
    ? { ...options, store: new GatewayStore({ path: options.databasePath }) }
    : options;
  initWebSocket(server, configured);
  return server;
}

function installShutdown(server: GatewayServer) {
  const stop = () => {
    void server.shutdown().then(
      () => process.exit(0),
      () => process.exit(1),
    );
  };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
}

if (isMain(import.meta)) {
  dotenv.config();
  const port = process.env.PORT || 3001;
  const host = process.env.HOST || "127.0.0.1";
  const configIndex = process.argv.indexOf("--config");
  if (configIndex !== -1 && !process.argv[configIndex + 1]) {
    throw new Error("--config requires a path");
  }
  const options = loadRuntimeConfig(
    configIndex === -1 ? undefined : process.argv[configIndex + 1],
  );
  if (host !== "127.0.0.1" && host !== "::1" && !options.auth) {
    throw new Error("A non-loopback Push Gateway listener requires gateway auth");
  }
  const server = createGatewayServer(options);
  installShutdown(server);
  // Node normalizes numeric strings and socket paths in this overload.
  server.listen(port as number, host, () => {
    console.log(`Push gateway listening on http://${host}:${port}`);
  });
}

export { createGatewayServer, installShutdown };
