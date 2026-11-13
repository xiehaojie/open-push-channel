const assert = require("node:assert/strict");
const { spawn, spawnSync } = require("node:child_process");
const { once } = require("node:events");
const { mkdtempSync, rmSync, writeFileSync } = require("node:fs");
const http = require("node:http");
const { tmpdir } = require("node:os");
const path = require("node:path");
const test = require("node:test");
const WebSocket = require("ws");

void test("the CLI refuses network listening without gateway auth", () => {
  const child = spawnSync(process.execPath, [path.join(__dirname, "server.js")], {
    env: { HOST: "0.0.0.0", PORT: "0", DOTENV_CONFIG_QUIET: "true" },
    cwd: tmpdir(),
    encoding: "utf8",
    timeout: 5000,
  });
  assert.ifError(child.error);
  assert.equal(child.status, 1);
  assert.match(child.stderr, /non-loopback Push Gateway listener requires gateway auth/);
});

void test(
  "npm start runs the gateway without retired admin dependencies or APIs",
  { timeout: 10000 },
  async (t) => {
    const directory = mkdtempSync(path.join(tmpdir(), "push-default-start-"));
    let child = null;
    let closed = null;
    let childClosed = false;
    let native = null;
    const sockets = [];
    t.after(async () => {
      try {
        sockets.forEach((socket) => socket.terminate());
        if (child?.pid && !childClosed) {
          const signal = (name) => {
            try {
              if (process.platform === "win32") {
                return child.kill(name);
              }
              process.kill(-child.pid, name);
              return true;
            } catch (error) {
              if (error.code !== "ESRCH") {
                throw error;
              }
              return false;
            }
          };
          // npm's exit can precede its child. The close event waits for shared
          // stdio too, so wait for it before removing SQLite or escalating.
          const waitForClose = () =>
            new Promise((resolve) => {
              const timer = setTimeout(() => resolve(false), 1000);
              closed.then(() => {
                clearTimeout(timer);
                resolve(true);
              });
            });
          signal("SIGTERM");
          if (!(await waitForClose()) && !childClosed) {
            signal("SIGKILL");
            assert.ok(await waitForClose(), "Test npm process group did not close");
          }
        }
      } finally {
        try {
          if (native?.listening) {
            await new Promise((resolve) => {
              native.close(resolve);
            });
          }
        } finally {
          rmSync(directory, { recursive: true, force: true });
        }
      }
    });
    const nativeRequests = [];
    const nativeErrors = [];
    const replyNative = async (request, response) => {
      assert.equal(request.headers.authorization, "Bearer fixture-webhook");
      let body = "";
      for await (const chunk of request) {
        body += chunk;
      }
      const payload = JSON.parse(body);
      nativeRequests.push(payload);
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      response.end(
        `data: ${JSON.stringify({ type: "content", delta: payload.content })}\n\n` +
          'data: {"type":"done"}\n\n',
      );
    };
    native = http.createServer((request, response) => {
      void replyNative(request, response).catch((/** @type {unknown} */ error) => {
        nativeErrors.push(error);
        response.destroy();
      });
    });
    await new Promise((resolve) => {
      native.listen(0, "127.0.0.1", resolve);
    });
    const backend = path.resolve(__dirname, "../..");
    const config = path.join(directory, "gateway.json");
    writeFileSync(
      config,
      JSON.stringify({
        databasePath: path.join(directory, "gateway.sqlite"),
        runtimes: [
          {
            id: "openclaw",
            type: "openclaw",
            url: `http://127.0.0.1:${native.address().port}/webhook`,
          },
        ],
        auth: {
          clients: [
            {
              id: "owner",
              tokenEnv: "PUSH_START_CLIENT",
              targets: [{ runtimeId: "openclaw", agentId: "main" }],
            },
          ],
          pushTokenEnv: "PUSH_START_SERVICE",
        },
      }),
    );
    const envFile = path.join(directory, ".env");
    writeFileSync(envFile, "");
    const npmUserConfig = path.join(directory, "user.npmrc");
    const npmGlobalConfig = path.join(directory, "global.npmrc");
    writeFileSync(npmUserConfig, "");
    writeFileSync(npmGlobalConfig, "");
    const preload = path.join(directory, "isolate-start.cjs");
    // Keep the real npm command and CLI/config path. Only isolate dotenv's file,
    // reject retired packages, and observe the OS-assigned test port.
    writeFileSync(
      preload,
      `
const Module = require("node:module");
const load = Module._load;
Module._load = function (request, parent, isMain) {
  if (["@koa/router", "bcrypt", "jsonwebtoken", "mysql2"].some((name) => request === name || request.startsWith(name + "/"))) {
    throw new Error("Retired admin dependency loaded: " + request);
  }
  const value = load.call(this, request, parent, isMain);
  if (request === "dotenv") {
    return { ...value, config: (options) => value.config({ ...options, path: process.env.PUSH_START_ENV }) };
  }
  return value;
};
const { Server } = require("node:http");
const listen = Server.prototype.listen;
Server.prototype.listen = function (...args) {
  this.once("listening", () => console.log("PUSH_START_ADDRESS=" + this.address().port));
  return listen.apply(this, args);
};
`,
    );
    child = spawn("npm", ["start", "--", "--config", config], {
      cwd: backend,
      detached: process.platform !== "win32",
      env: {
        PATH: process.env.PATH,
        npm_config_userconfig: npmUserConfig,
        npm_config_globalconfig: npmGlobalConfig,
        npm_config_cache: path.join(directory, "npm-cache"),
        npm_config_logs_dir: path.join(directory, "npm-logs"),
        npm_config_update_notifier: "false",
        PORT: "0",
        HOST: "127.0.0.1",
        PUSH_START_CLIENT: "fixture-client",
        PUSH_START_SERVICE: "fixture-service",
        PUSH_CHANNEL_WEBHOOK_TOKEN: "fixture-webhook",
        PUSH_START_ENV: envFile,
        DOTENV_CONFIG_QUIET: "true",
        NODE_OPTIONS: `--require=${preload}`,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    closed = new Promise((resolve) => {
      child.once("close", () => {
        childClosed = true;
        resolve();
      });
    });
    let output = "";
    const port = await new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`npm start readiness timeout: ${output}`)),
        4000,
      );
      const collect = (chunk) => {
        output += chunk;
        const match = /PUSH_START_ADDRESS=(\d+)/.exec(output);
        if (match) {
          clearTimeout(timer);
          resolve(Number(match[1]));
        }
      };
      child.stdout.on("data", collect);
      child.stderr.on("data", collect);
      child.once("error", (error) => {
        clearTimeout(timer);
        reject(error);
      });
      child.once("exit", (code) => {
        clearTimeout(timer);
        reject(new Error(`npm start exited ${code} before readiness: ${output}`));
      });
    });
    const base = `http://127.0.0.1:${port}`;
    assert.equal((await fetch(`${base}/healthz`)).status, 200);
    const page = await fetch(`${base}/demo/`);
    assert.equal(page.status, 200);
    assert.match(await page.text(), /session-id-input/);
    assert.equal((await fetch(`${base}/api/runtimes`)).status, 401);
    const catalog = await fetch(`${base}/api/runtimes`, {
      headers: { Authorization: "Bearer fixture-service" },
    });
    assert.deepEqual(
      (await catalog.json()).map((runtime) => runtime.id),
      ["openclaw"],
    );
    for (const [method, route] of [
      ["POST", "/api/auth/login"],
      ["POST", "/api/auth/register"],
      ["GET", "/api/auth/session"],
      ["POST", "/api/auth/logout"],
      ["GET", "/api/users"],
      ["POST", "/api/users"],
      ["PUT", "/api/users/1"],
      ["PATCH", "/api/users/1/status"],
      ["POST", "/register"],
      ["POST", "/auth"],
      ["POST", "/api/register"],
      ["POST", "/api/auth"],
      ["POST", "/webhook"],
    ]) {
      assert.equal((await fetch(`${base}${route}`, { method })).status, 404, `${method} ${route}`);
    }
    const rejected = new WebSocket(base.replace("http:", "ws:"));
    sockets.push(rejected);
    await once(rejected, "open");
    const denied = once(rejected, "close");
    rejected.send(JSON.stringify({ type: "register", token: "wrong" }));
    assert.equal((await denied)[0], 4001);
    const socket = new WebSocket(base.replace("http:", "ws:"));
    sockets.push(socket);
    const events = [];
    socket.on("message", (raw) => events.push(JSON.parse(Buffer.from(raw).toString())));
    await once(socket, "open");
    const wait = async (predicate) => {
      const deadline = Date.now() + 1500;
      while (!events.some(predicate)) {
        assert.deepEqual(nativeErrors, []);
        assert.ok(Date.now() < deadline, `Missing ${predicate}`);
        await new Promise((resolve) => {
          setTimeout(resolve, 5);
        });
      }
      return events.find(predicate);
    };
    const send = (value) => socket.send(JSON.stringify(value));
    send({ type: "register", agentId: "spoofed", token: "fixture-client", protocolVersion: 1 });
    assert.equal((await wait((event) => event.type === "registered")).agentId, "owner");
    send({ type: "session.start", sessionId: "chat", runtimeId: "openclaw", agentId: "main" });
    await wait((event) => event.type === "session.started");
    send({ type: "message", sessionId: "chat", queryMessageId: "chat-request", content: "hello" });
    await wait((event) => event.type === "run.completed" && event.requestId === "chat-request");
    assert.ok(
      events.some((event) => event.type === "message.delta" && event.data.delta === "hello"),
    );
    assert.equal(nativeRequests.length, 1);
    const push = (token) =>
      fetch(`${base}/api/send`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({
          clientId: "owner",
          sessionId: "chat",
          runtimeId: "openclaw",
          agentId: "main",
          content: "notification",
          eventId: "notice",
        }),
      });
    assert.equal((await push()).status, 401);
    assert.equal((await push("fixture-service")).status, 200);
    await wait((event) => event.type === "message.delta" && event.data.delta === "notification");
    assert.deepEqual(nativeErrors, []);
    assert.equal(nativeRequests.length, 1, "A notification must not launch another native turn");
  },
);
