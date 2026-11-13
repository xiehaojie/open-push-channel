const assert = require("node:assert/strict");
const {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  existsSync,
  rmSync,
  symlinkSync,
} = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawn, spawnSync } = require("node:child_process");
const { once } = require("node:events");
const net = require("node:net");
const test = require("node:test");

void test("runner provisions distinct native templates once, requires auth, and never starts models", (t) => {
  const directory = mkdtempSync(path.join(os.tmpdir(), "push-bootstrap-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const dataRoot = path.join(directory, "data");
  const templates = path.join(directory, "secret");
  mkdirSync(templates);
  const configPath = path.join(templates, "gateway.json");
  const config = {
    dataRoot,
    runtimes: [
      {
        id: "dsh",
        type: "deepseek-harness",
        agents: ["writer", "reviewer"],
        command: "must-not-spawn",
      },
      { id: "pi", type: "pi", agents: ["writer"], command: "must-not-spawn" },
    ],
  };
  writeFileSync(configPath, JSON.stringify(config));
  const run = (extra = []) =>
    spawnSync(
      process.execPath,
      [path.join(__dirname, "bootstrap.cjs"), "--config", configPath, "--prepare-only", ...extra],
      {
        encoding: "utf8",
        env: {
          ...process.env,
          PUSH_BOOTSTRAP_CLIENT: "fixture-client",
          PUSH_BOOTSTRAP_PUSH: "fixture-push",
        },
      },
    );
  const noAuth = run();
  assert.match(noAuth.stderr, /auth/i);
  assert.equal(existsSync(dataRoot), false);
  config.auth = {
    clients: [
      {
        id: "demo",
        tokenEnv: "PUSH_BOOTSTRAP_CLIENT",
        targets: [{ runtimeId: "pi", agentId: "writer" }],
      },
    ],
    pushTokenEnv: "PUSH_BOOTSTRAP_PUSH",
  };
  writeFileSync(configPath, JSON.stringify(config));
  const missing = run();
  assert.notEqual(missing.status, 0);
  assert.match(missing.stderr, /template/i);
  assert.equal(existsSync(dataRoot), false);
  writeFileSync(path.join(templates, "dsh-profile.yml"), "[]\n");
  writeFileSync(path.join(templates, "pi-models.json"), "{}\n");
  const first = run();
  assert.equal(first.status, 0, first.stderr);
  for (const extra of [["admin-backend/src/gateway/server.js"], ["--unknown"], ["--config"]]) {
    const rejected = run(extra);
    assert.notEqual(rejected.status, 0);
    assert.match(rejected.stderr, /argument|requires a path/i);
  }
  const dshFile = path.join(
    dataRoot,
    "dsh",
    "writer",
    "home",
    ".dsh",
    "profiles",
    "sdk",
    "cordis.patch.yml",
  );
  const piFile = path.join(dataRoot, "pi", "writer", "home", ".pi", "agent", "models.json");
  assert.equal(readFileSync(dshFile, "utf8"), "[]\n");
  assert.equal(readFileSync(piFile, "utf8"), "{}\n");
  writeFileSync(dshFile, "# local customization\n[]\n");
  writeFileSync(piFile, '{"providers":{}}\n');
  const again = run();
  assert.equal(again.status, 0, again.stderr);
  assert.equal(readFileSync(dshFile, "utf8"), "# local customization\n[]\n");
  assert.equal(readFileSync(piFile, "utf8"), '{"providers":{}}\n');
  assert.equal(existsSync(path.join(dataRoot, "dsh", "reviewer", "workspace")), true);
  const outside = path.join(directory, "shared-models.json");
  writeFileSync(outside, "{}\n");
  rmSync(piFile);
  symlinkSync(outside, piFile);
  const aliased = run();
  assert.notEqual(aliased.status, 0);
  assert.match(aliased.stderr, /symbolic link/i);
  assert.equal(readFileSync(outside, "utf8"), "{}\n");
});

void test("runner flags start the fixed gateway script and forward shutdown", async (t) => {
  const directory = mkdtempSync(path.join(os.tmpdir(), "push-startup-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const configPath = path.join(directory, "custom-gateway.json");
  writeFileSync(
    configPath,
    JSON.stringify({
      databasePath: path.join(directory, "gateway.sqlite"),
      runtimes: [{ id: "openclaw", type: "openclaw", url: "http://127.0.0.1:1/webhook" }],
      auth: {
        clients: [
          {
            id: "demo",
            tokenEnv: "PUSH_STARTUP_CLIENT",
            targets: [{ runtimeId: "openclaw", agentId: "main" }],
          },
        ],
        pushTokenEnv: "PUSH_STARTUP_PUSH",
      },
    }),
  );
  const listener = net.createServer();
  await new Promise((resolve) => {
    listener.listen(0, "127.0.0.1", resolve);
  });
  const port = listener.address().port;
  await new Promise((resolve) => {
    listener.close(resolve);
  });
  const child = spawn(
    process.execPath,
    [path.join(__dirname, "bootstrap.cjs"), "--config", configPath],
    {
      cwd: directory,
      env: {
        ...process.env,
        HOST: "127.0.0.1",
        PORT: String(port),
        PUSH_STARTUP_CLIENT: "fixture-client",
        PUSH_STARTUP_PUSH: "fixture-push",
      },
      stdio: ["ignore", "ignore", "pipe"],
    },
  );
  let diagnostics = "";
  child.stderr.on("data", (chunk) => {
    diagnostics += chunk.toString();
  });
  const exited = once(child, "exit");
  t.after(async () => {
    child.kill("SIGTERM");
    const deadline = setTimeout(() => child.kill("SIGKILL"), 1000);
    await exited;
    clearTimeout(deadline);
  });
  const url = `http://127.0.0.1:${port}/healthz`;
  const deadline = Date.now() + 4000;
  let healthy = false;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`Runner exited before healthz: ${diagnostics}`);
    }
    try {
      healthy = (await fetch(url, { signal: AbortSignal.timeout(200) })).ok;
    } catch {}
    if (healthy) {
      break;
    }
    await new Promise((resolve) => {
      setTimeout(resolve, 20);
    });
  }
  assert.equal(healthy, true, diagnostics);
  child.kill("SIGTERM");
  const [exitCode, signal] = await exited;
  assert.equal(exitCode, 0, diagnostics);
  assert.equal(signal, null);
  await assert.rejects(fetch(url, { signal: AbortSignal.timeout(200) }));
});
