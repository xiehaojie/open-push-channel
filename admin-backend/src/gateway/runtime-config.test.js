const assert = require("node:assert/strict");
const { existsSync, mkdtempSync, rmSync, writeFileSync } = require("node:fs");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const { createOpenClawAdapter } = require("../adapters/openclaw");
const { createPluginAdapter } = require("../adapters/plugin");
const { createDefaultGateway } = require("./default");
const { createRuntimeOptions } = require("./runtime-config");

void test("server config declares native commands and capabilities, never accepts executable commands from clients", () => {
  const options = createRuntimeOptions(
    {
      dataRoot: "runtime-data",
      runtimes: [
        { id: "openclaw", type: "openclaw", url: "http://127.0.0.1:3002/webhook" },
        {
          id: "harness",
          type: "deepseek-harness",
          command: "dsh",
          agents: ["main"],
          provider: "deepseek-official",
          model: "deepseek-v4-flash",
        },
        {
          id: "pi",
          type: "pi",
          agents: ["main"],
          command: "pi",
          args: ["--provider", "anthropic"],
        },
      ],
    },
    "/tmp/config",
  );
  assert.deepEqual(
    options.adapters.map((adapter) => adapter.id),
    ["openclaw", "harness", "pi"],
  );
  assert.equal(options.adapters[0].capabilities.mentions, true);
  assert.equal(options.adapters[1].capabilities.cancel, false);
  assert.equal(options.adapters[2].capabilities.cancel, true);
  assert.equal(options.databasePath, "/tmp/config/state/push-gateway.sqlite");
});

void test("invalid server config fails before any native runtime is started", () => {
  for (const config of [
    { runtimes: [] },
    { runtimes: [{ id: "bad", type: "unknown" }] },
    { runtimes: [{ id: "one", type: "pi", args: "--shell" }] },
    { runtimes: [{ id: "one", type: "openclaw", url: "file:///etc/passwd" }] },
    {
      runtimes: [
        { id: "one", type: "pi" },
        { id: "one", type: "pi" },
      ],
    },
  ]) {
    assert.throws(() => createRuntimeOptions(config, "/tmp/config"));
  }
});

void test("OpenClaw adapters send only their configured webhook token", async (t) => {
  const runtimes = [];
  const observed = [];
  for (const id of ["alpha", "beta"]) {
    const server = http.createServer((request, response) => {
      observed.push([id, request.headers.authorization]);
      request.resume();
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      response.end('data: {"type":"done"}\n\n');
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    t.after(() => new Promise((resolve) => server.close(resolve)));
    runtimes.push({
      id,
      type: "openclaw",
      url: `http://127.0.0.1:${server.address().port}/webhook`,
      webhookTokenEnv: `PUSH_CONFIG_WEBHOOK_${id.toUpperCase()}`,
    });
  }
  const tokenNames = runtimes.map((runtime) => runtime.webhookTokenEnv);
  const globalName = "PUSH_CHANNEL_WEBHOOK_TOKEN";
  const previous = new Map([...tokenNames, globalName].map((name) => [name, process.env[name]]));
  t.after(() => {
    for (const [name, value] of previous) {
      if (value === undefined) {
        delete process.env[name];
      } else {
        process.env[name] = value;
      }
    }
  });
  process.env[globalName] = "fixture-global";
  process.env[tokenNames[0]] = "fixture-alpha";
  process.env[tokenNames[1]] = "fixture-beta";

  const { adapters } = createRuntimeOptions({ runtimes });
  for (const adapter of adapters) {
    await adapter.run({ agentId: "main", conversationId: "chat", content: "probe" }, { emit() {} });
  }
  assert.deepEqual(observed, [
    ["alpha", "Bearer fixture-alpha"],
    ["beta", "Bearer fixture-beta"],
  ]);
  assert.throws(
    () =>
      createRuntimeOptions({
        runtimes: [runtimes[0], { ...runtimes[1], webhookTokenEnv: undefined }],
      }),
    /require explicit webhookTokenEnv/,
  );
  process.env[tokenNames[1]] = "fixture-alpha";
  assert.throws(() => createRuntimeOptions({ runtimes }), /webhook tokens must be distinct/);
  delete process.env[tokenNames[1]];
  assert.throws(() => createRuntimeOptions({ runtimes }), /Invalid OpenClaw webhook token/);
});

void test("OpenClaw webhook credentials cannot be reused across gateway roles", (t) => {
  const fixture = {
    PUSH_CHANNEL_WEBHOOK_TOKEN: "fixture-webhook",
    PUSH_CONFIG_ROLE_CLIENT: "fixture-client",
    PUSH_CONFIG_ROLE_PUSH: "fixture-push",
    PUSH_CONFIG_ROLE_PLUGIN: "fixture-plugin",
  };
  const previous = new Map(Object.keys(fixture).map((name) => [name, process.env[name]]));
  t.after(() => {
    for (const [name, value] of previous) {
      if (value === undefined) {
        delete process.env[name];
      } else {
        process.env[name] = value;
      }
    }
  });
  const config = {
    runtimes: [
      { id: "openclaw", type: "openclaw", url: "http://127.0.0.1:1/webhook" },
      {
        id: "native",
        type: "plugin",
        agents: ["main"],
        tokenEnv: "PUSH_CONFIG_ROLE_PLUGIN",
        capabilities: {
          streaming: true,
          tools: false,
          reasoning: false,
          subagents: false,
          cancel: false,
          mentions: false,
          sessionRestore: false,
          steering: false,
          followUp: false,
        },
      },
    ],
    auth: {
      clients: [
        {
          id: "client",
          tokenEnv: "PUSH_CONFIG_ROLE_CLIENT",
          targets: [{ runtimeId: "openclaw", agentId: "main" }],
        },
      ],
      pushTokenEnv: "PUSH_CONFIG_ROLE_PUSH",
    },
  };
  for (const role of [
    "PUSH_CONFIG_ROLE_CLIENT",
    "PUSH_CONFIG_ROLE_PUSH",
    "PUSH_CONFIG_ROLE_PLUGIN",
  ]) {
    Object.assign(process.env, fixture, { [role]: fixture.PUSH_CHANNEL_WEBHOOK_TOKEN });
    assert.throws(
      () => createRuntimeOptions(config),
      /OpenClaw webhook tokens must be distinct from client, push and plugin tokens/,
    );
  }
  assert.throws(
    () =>
      createDefaultGateway({
        auth: { clients: [], pushToken: fixture.PUSH_CHANNEL_WEBHOOK_TOKEN },
      }),
    /OpenClaw webhook tokens must be distinct/,
  );
  const builtIn = createOpenClawAdapter({
    url: "http://127.0.0.1:1/webhook",
    webhookToken: ` ${fixture.PUSH_CHANNEL_WEBHOOK_TOKEN} `,
  });
  assert.throws(
    () =>
      createDefaultGateway({
        adapters: [{ ...builtIn }],
        auth: { clients: [{ token: fixture.PUSH_CHANNEL_WEBHOOK_TOKEN }], pushToken: "other" },
      }),
    /OpenClaw webhook tokens must be distinct/,
  );
  const plugin = createPluginAdapter({
    id: "native",
    agents: ["main"],
    token: fixture.PUSH_CHANNEL_WEBHOOK_TOKEN,
    capabilities: {},
  });
  assert.throws(
    () =>
      createDefaultGateway({
        adapters: [{ ...plugin, run: (...args) => plugin.run(...args) }],
        auth: { clients: [{ token: fixture.PUSH_CHANNEL_WEBHOOK_TOKEN }], pushToken: "other" },
      }),
    /Plugin tokens must be distinct from client and push tokens/,
  );
  assert.throws(
    () => createDefaultGateway({ adapters: [builtIn, plugin] }),
    /OpenClaw webhook tokens must be distinct from client, push and plugin tokens/,
  );
  assert.throws(
    () => createDefaultGateway({ adapters: [plugin, builtIn] }),
    /OpenClaw webhook tokens must be distinct from client, push and plugin tokens/,
  );
  process.env.PUSH_CHANNEL_WEBHOOK_TOKEN = "   ";
  assert.throws(() => createRuntimeOptions({ runtimes: [config.runtimes[0]] }), {
    message: "Invalid OpenClaw webhook Bearer token format",
  });
  assert.throws(() => createDefaultGateway({}), {
    message: "Invalid OpenClaw webhook Bearer token format",
  });
  process.env.PUSH_CHANNEL_WEBHOOK_TOKEN = "invalid fixture";
  assert.throws(() => createDefaultGateway({}), {
    message: "Invalid OpenClaw webhook Bearer token format",
  });
});

void test("credential isolation follows the same UTF-8 equality as authentication", () => {
  const plugin = createPluginAdapter({
    id: "native",
    agents: ["main"],
    token: "\ud800",
    capabilities: {},
  });
  const clients = [{ token: "\ufffd" }];
  assert.throws(
    () =>
      createDefaultGateway({ adapters: [plugin], auth: { clients, pushToken: "fixture-push" } }),
    /Plugin tokens must be distinct from client and push tokens/,
  );
  assert.throws(
    () => createDefaultGateway({ adapters: [plugin], auth: { clients, pushToken: "\ud800" } }),
    /Client and push tokens must be distinct/,
  );
});

void test("gateway composition validates and retains adapters from a one-pass iterable", async (t) => {
  const adapter = createOpenClawAdapter({
    url: "http://127.0.0.1:1/webhook",
    webhookToken: "fixture-webhook",
  });
  function* adapters() {
    yield adapter;
  }
  const gateway = createDefaultGateway({ adapters: adapters(), publish() {} });
  t.after(() => gateway.close());
  assert.equal(gateway.adapter(adapter.id), adapter);
});

void test("auth config strips client, push and webhook tokens from native children", async (t) => {
  const directory = mkdtempSync(path.join(os.tmpdir(), "push-config-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const names = [
    "PUSH_CONFIG_CLIENT_A",
    "PUSH_CONFIG_CLIENT_B",
    "PUSH_CONFIG_SERVER_TOKEN",
    "PUSH_CHANNEL_WEBHOOK_TOKEN",
    "PUSH_CONFIG_WEBHOOK_RUNTIME",
  ];
  const previous = names.map((name) => process.env[name]);
  names.forEach((name, index) => {
    process.env[name] = `test-fixture-${index}`;
  });
  t.after(() =>
    names.forEach((name, index) => {
      if (previous[index] === undefined) {
        delete process.env[name];
      } else {
        process.env[name] = previous[index];
      }
    }),
  );
  const fixture = String.raw`
    require('node:readline').createInterface({input:process.stdin}).on('line', line => {
      const request=JSON.parse(line);
      const send=event=>process.stdout.write(JSON.stringify(event)+'\n');
      send({type:'response',id:request.id,command:request.type,success:true,data:{disposition:'started'}});
      send({type:'message_update',assistantMessageEvent:{type:'text_delta',delta:JSON.stringify({tokens:${JSON.stringify(names)}.map(key=>process.env[key]!==undefined),home:process.env.HOME,dshHome:process.env.DSH_HOME,piHome:process.env.PI_CODING_AGENT_DIR})}});
      send({type:'agent_settled',aborted:false});
    });
  `;
  const command = path.join(directory, "auth-fixture.cjs");
  writeFileSync(command, `#!${process.execPath}\n${fixture}`, { mode: 0o700 });
  const { adapters } = createRuntimeOptions({
    dataRoot: directory,
    runtimes: [
      {
        id: "pi",
        type: "pi",
        agents: ["main"],
        command,
        env: { PUSH_CONFIG_WEBHOOK_RUNTIME: "fixture-override" },
      },
      {
        id: "openclaw",
        type: "openclaw",
        url: "http://127.0.0.1:3002/webhook",
        webhookTokenEnv: "PUSH_CONFIG_WEBHOOK_RUNTIME",
      },
    ],
    auth: {
      clients: names.slice(0, 2).map((name, index) => ({
        id: `client-${index}`,
        tokenEnv: name,
        targets: [{ runtimeId: "pi", agentId: "main" }],
      })),
      pushTokenEnv: names[2],
    },
  });
  t.after(() => adapters[0].close());
  const events = [];
  await adapters[0].run(
    { agentId: "main", conversationId: "chat", content: "probe" },
    { emit: (event) => events.push(event) },
  );
  const observed = JSON.parse(events.find((event) => event.type === "message.delta").data.delta);
  assert.deepEqual(observed.tokens, [false, false, false, false, false]);
  const home = path.join(directory, "pi", "main", "home");
  assert.equal(observed.home, home);
  assert.equal(observed.dshHome, path.join(home, ".dsh"));
  assert.equal(observed.piHome, path.join(home, ".pi", "agent"));
});

void test("auth token environment cannot delete managed native home variables", (t) => {
  const directory = mkdtempSync(path.join(os.tmpdir(), "push-home-auth-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const dataRoot = path.join(directory, "data");
  const names = [
    "HOME",
    "home",
    "USERPROFILE",
    "DSH_HOME",
    "dsh_agents_home",
    "PI_CODING_AGENT_DIR",
    "pi_coding_agent_session_dir",
    "XDG_CONFIG_HOME",
    "xdg_cache_home",
    "XDG_DATA_HOME",
    "XDG_STATE_HOME",
  ];
  const previous = new Map(
    [...names, "PUSH_FIXTURE_UNUSED", "PUSH_FIXTURE_UNUSED_PUSH"].map((name) => [
      name,
      process.env[name],
    ]),
  );
  for (const name of previous.keys()) {
    process.env[name] = `fixture-${name}`;
  }
  t.after(() => {
    for (const [name, value] of previous) {
      if (value === undefined) {
        delete process.env[name];
      } else {
        process.env[name] = value;
      }
    }
  });
  for (const name of names) {
    for (const role of ["client", "push"]) {
      const auth = {
        clients: [
          {
            id: "owner",
            tokenEnv: "PUSH_FIXTURE_UNUSED",
            targets: [{ runtimeId: "pi", agentId: "main" }],
          },
        ],
        pushTokenEnv: "PUSH_FIXTURE_UNUSED_PUSH",
      };
      if (role === "client") {
        auth.clients[0].tokenEnv = name;
      } else {
        auth.pushTokenEnv = name;
      }
      assert.throws(
        () =>
          createRuntimeOptions({
            dataRoot,
            runtimes: [{ id: "pi", type: "pi", agents: ["main"], command: "must-not-start" }],
            auth,
          }),
        /managed home environment/i,
      );
      assert.equal(existsSync(dataRoot), false);
    }
  }
});
