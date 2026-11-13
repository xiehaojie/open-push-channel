const assert = require("node:assert/strict");
const {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const { createRuntimeOptions, loadRuntimeConfig } = require("./runtime-config");
const { PushGateway } = require("./gateway");

const fixture = String.raw`
  const fs = require('node:fs');
  const path = require('node:path');
  let prompts = 0;
  let steering = [];
  let followUp = [];
  const snapshot = () => {
    const file = path.join(process.cwd(), 'owned.txt');
    const previous = fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : '';
    fs.writeFileSync(file, previous + 'x');
    return JSON.stringify({ cwd:process.cwd(), home:process.env.HOME,
      dsh:process.env.DSH_HOME, agents:process.env.DSH_AGENTS_HOME,
      pi:process.env.PI_CODING_AGENT_DIR, previous, pid:process.pid, prompts });
  };
  require('node:readline').createInterface({ input:process.stdin }).on('line', line => {
    const request = JSON.parse(line);
    const send = value => process.stdout.write(JSON.stringify(value)+'\n');
    if (request.jsonrpc) {
      const result = request.method === 'initialize'
        ? { serverInfo:{ name:'deepseek-harness-sdk-runtime' } }
        : request.method === 'session/prompt' ? { messageId:'m' } : {};
      send({ jsonrpc:'2.0', id:request.id, result });
      if (request.method === 'session/prompt') {
        const sessionId = request.params.sessionId;
        for (const event of [
          {type:'assistant/message', data:{message:{id:'m',content:[{type:'text',text:snapshot()}]}}},
          {type:'turn/end',data:{reason:{kind:'completed'}}}
        ]) send({jsonrpc:'2.0',method:'session.event',params:{sessionId,event}});
      }
    } else {
      if (request.type === 'get_state') {
        const index=process.argv.indexOf('--session-id');
        send({type:'response',id:request.id,success:true,data:{sessionId:process.argv[index+1]}});return;
      }
      if (request.type === 'steer' || request.type === 'follow_up') {
        (request.type === 'steer' ? steering : followUp).push(request.message);
        send({type:'queue_update',steering,followUp});
        send({type:'message_update',assistantMessageEvent:{type:'text_delta',delta:JSON.stringify({...JSON.parse(snapshot()),control:request.type,content:request.message})}});
        send({type:'response',id:request.id,success:true,data:{disposition:'queued'}});return;
      }
      if (request.type === 'clear_queue') {
        const discarded={steering,followUp};steering=[];followUp=[];
        send({type:'queue_update',steering,followUp});
        send({type:'response',id:request.id,success:true,data:discarded});return;
      }
      if (request.type === 'abort') {
        send({type:'response',id:request.id,success:true});send({type:'agent_settled',aborted:true});return;
      }
      send({type:'response',id:request.id,command:request.type,success:true,data:{disposition:'started'}});
      if (request.type === 'prompt') {
        prompts++;
        send({type:'message_update',assistantMessageEvent:{type:'text_delta',delta:snapshot()}});
        if(request.message !== 'control-wait') send({type:'agent_settled',aborted:false});
      }
    }
  });
`;

function temporary(t) {
  const directory = realpathSync(mkdtempSync(path.join(os.tmpdir(), "push-workspaces-")));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

function configuration(directory) {
  const command = path.join(directory, "native-fixture.cjs");
  writeFileSync(command, `#!${process.execPath}\n${fixture}`, { mode: 0o700 });
  return {
    dataRoot: path.join(directory, "data"),
    runtimes: ["deepseek-harness", "pi"].map((type) => ({
      id: type,
      type,
      agents: ["writer", "researcher"],
      command,
    })),
  };
}

for (const type of ["deepseek-harness", "pi"]) {
  for (const commandKind of ["dot-relative", "relative", "absolute", "PATH"]) {
    void test(`${type} resolves ${commandKind} executable independently of its agent cwd`, async (t) => {
      const directory = temporary(t);
      const binaryDirectory = path.join(directory, "bin");
      mkdirSync(binaryDirectory);
      const executable = path.join(binaryDirectory, "native-fixture.cjs");
      writeFileSync(executable, `#!${process.execPath}\n${fixture}`, { mode: 0o700 });
      const commands = {
        "dot-relative": "./bin/native-fixture.cjs",
        relative: "bin/native-fixture.cjs",
        absolute: executable,
        PATH: "native-fixture.cjs",
      };
      const config = {
        dataRoot: "data",
        runtimes: [
          {
            id: "native",
            type,
            agents: ["writer"],
            command: commands[commandKind],
            env: { PATH: `${binaryDirectory}${path.delimiter}${process.env.PATH ?? ""}` },
          },
        ],
      };
      const filename = path.join(directory, "gateway.json");
      writeFileSync(filename, JSON.stringify(config));
      const {
        adapters: [adapter],
      } = loadRuntimeConfig(filename);
      t.after(() => adapter.close());
      const events = [];
      await adapter.run(
        { agentId: "writer", clientId: "client", conversationId: "chat", content: "probe" },
        { emit: (event) => events.push(event) },
      );
      const event = events.find((item) =>
        ["message.delta", "message.snapshot"].includes(item.type),
      );
      const result = JSON.parse(event.data.delta ?? event.data.text);
      assert.equal(
        result.cwd,
        path.join(directory, "data", type === "pi" ? "pi" : "dsh", "writer", "workspace"),
      );
    });
  }
}

void test("native agents own stable separate homes and working files, including across restart", async (t) => {
  const directory = temporary(t);
  const config = configuration(directory);
  async function probe(adapter, agentId) {
    const events = [];
    await adapter.run(
      { agentId, clientId: "client", conversationId: "chat", content: "probe" },
      {
        emit: (event) => events.push(event),
      },
    );
    const event = events.find((item) => ["message.delta", "message.snapshot"].includes(item.type));
    return JSON.parse(event.data.delta ?? event.data.text);
  }
  const first = createRuntimeOptions(config, directory);
  t.after(() => Promise.all(first.adapters.map((adapter) => adapter.close())));
  assert.equal(
    existsSync(config.dataRoot),
    false,
    "configuration never starts/provisions native agents",
  );
  for (const adapter of first.adapters) {
    for (const agentId of ["writer", "researcher"]) {
      const result = await probe(adapter, agentId);
      const kind = adapter.id === "pi" ? "pi" : "dsh";
      const root = path.join(config.dataRoot, kind, agentId);
      assert.equal(result.cwd, path.join(root, "workspace"));
      assert.equal(result.home, path.join(root, "home"));
      assert.equal(result.previous, "");
      assert.equal(result.dsh, path.join(root, "home", ".dsh"));
      assert.equal(result.agents, path.join(root, "home", ".agents"));
      assert.equal(result.pi, path.join(root, "home", ".pi", "agent"));
      assert.equal(readFileSync(path.join(root, "workspace", "owned.txt"), "utf8"), "x");
    }
  }
  await Promise.all(first.adapters.map((adapter) => adapter.close()));
  const restored = createRuntimeOptions(config, directory);
  t.after(() => Promise.all(restored.adapters.map((adapter) => adapter.close())));
  for (const adapter of restored.adapters) {
    assert.equal((await probe(adapter, "writer")).previous, "x");
  }
});

void test("native CLI lifecycle and storage arguments are rejected before provisioning", (t) => {
  const directory = temporary(t);
  const base = configuration(directory);
  const cases = {
    pi: [
      ["--session-dir", "/shared"],
      ["--session-dir=/shared"],
      ["--session", "old"],
      ["--session-id=old"],
      ["--no-session"],
      ["--continue"],
      ["-c"],
      ["--resume"],
      ["-r"],
      ["--fork", "old"],
      ["--mode=rpc"],
      ["--help"],
      ["-h"],
      ["--version"],
      ["-v"],
      ["--print"],
      ["-p"],
      ["--list-models"],
      ["--export", "old"],
      ["--"],
      ["install", "package"],
    ],
    "deepseek-harness": [
      ["--profile", "web"],
      ["--profile=web"],
      ["--patch", "/shared/override.yml"],
      ["--patch=/shared/override.yml"],
      ["--from-default-profile", "web"],
      ["--dump-config"],
      ["--dump-default-config"],
      ["--dump-config-schema"],
      ["--help"],
      ["-h"],
      ["--version"],
      ["-V"],
      ["--"],
      ["web"],
      ["plugin", "add", "package"],
    ],
  };
  for (const runtime of base.runtimes) {
    for (const args of cases[runtime.type]) {
      assert.throws(
        () => createRuntimeOptions({ ...base, runtimes: [{ ...runtime, args }] }, directory),
        /argument/i,
      );
      assert.equal(existsSync(base.dataRoot), false);
    }
  }
});

void test("invalid native layouts fail without provisioning directories", (t) => {
  const directory = temporary(t);
  const base = configuration(directory);
  const runtime = base.runtimes[0];
  for (const config of [
    { ...base, dataRoot: undefined },
    { ...base, unknown: true },
    { ...base, runtimes: [{ ...runtime, agents: [] }] },
    { ...base, runtimes: [{ ...runtime, agents: ["../escape"] }] },
    { ...base, runtimes: [{ ...runtime, agents: ["a/b"] }] },
    { ...base, runtimes: [{ ...runtime, agents: ["Writer"] }] },
    { ...base, runtimes: [{ ...runtime, agents: ["writer", "writer"] }] },
    { ...base, runtimes: [{ ...runtime, cwd: "." }] },
    { ...base, runtimes: [{ ...runtime, env: { HOME: "/shared" } }] },
    { ...base, runtimes: [{ ...runtime, env: { dsh_home: "/shared" } }] },
    { ...base, runtimes: [{ ...runtime, env: { PI_CODING_AGENT_DIR: "/shared" } }] },
    { ...base, runtimes: [runtime, { ...runtime, id: "other" }] },
  ]) {
    assert.throws(
      () => createRuntimeOptions(config, directory),
      "reject invalid ownership or config",
    );
    assert.equal(existsSync(base.dataRoot), false);
  }
});

void test("undeclared native agents cannot persist, reopen or admit a run", async (t) => {
  const directory = temporary(t);
  const { adapters } = createRuntimeOptions(configuration(directory), directory);
  const gateway = new PushGateway({ adapters, publish: () => {} });
  t.after(() => gateway.close());
  assert.throws(
    () =>
      gateway.startSession({
        clientId: "client",
        conversationId: "bad",
        runtimeId: "pi",
        agentId: "other",
      }),
    /agent/i,
  );
  assert.equal(gateway.session("client", "bad"), undefined);
  assert.throws(
    () =>
      gateway.submit({
        clientId: "client",
        conversationId: "bad",
        runtimeId: "pi",
        agentId: "other",
        content: "bad",
      }),
    /agent/i,
  );
  const session = gateway.startSession({
    clientId: "client",
    conversationId: "valid",
    runtimeId: "pi",
    agentId: "writer",
  });
  const narrower = createRuntimeOptions(
    {
      ...configuration(directory),
      runtimes: [{ ...configuration(directory).runtimes[1], agents: ["researcher"] }],
    },
    directory,
  ).adapters[0];
  gateway.adapters.set("pi", narrower);
  t.after(() => narrower.close());
  assert.throws(
    () => gateway.startSession({ clientId: "client", conversationId: session.conversationId }),
    /agent/i,
  );
  assert.equal(existsSync(path.join(directory, "data")), false);
});

void test("managed native directory symlinks cannot alias another runtime", async (t) => {
  const directory = temporary(t);
  const config = configuration(directory);
  const pi = path.join(config.dataRoot, "pi", "writer");
  const dsh = path.join(config.dataRoot, "dsh");
  mkdirSync(pi, { recursive: true });
  mkdirSync(dsh, { recursive: true });
  symlinkSync(pi, path.join(dsh, "writer"), "dir");
  const { adapters } = createRuntimeOptions(config, directory);
  t.after(() => Promise.all(adapters.map((adapter) => adapter.close())));
  await assert.rejects(
    adapters[0].run(
      { agentId: "writer", conversationId: "chat", content: "probe" },
      { emit: () => {} },
    ),
    /symbolic link/i,
  );
  assert.equal(existsSync(path.join(pi, "home")), false);
  assert.equal(existsSync(path.join(pi, "workspace")), false);
});

void test("configured per-agent Pi wrappers preserve native controls and isolate client, agent and run ownership", async (t) => {
  const directory = temporary(t);
  const config = configuration(directory);
  config.runtimes.push({ id: "openclaw", type: "openclaw", url: "http://127.0.0.1:1/webhook" });
  const { adapters } = createRuntimeOptions(config, directory);
  const events = [];
  let allStarted;
  const started = new Promise((resolve) => {
    allStarted = resolve;
  });
  const gateway = new PushGateway({
    adapters,
    publish(event) {
      events.push(event);
      if (events.filter((item) => item.type === "message.delta").length === 3) allStarted();
    },
  });
  t.after(() => gateway.close());
  const sessions = [
    ["first", "writer"],
    ["second", "researcher"],
    ["third", "writer"],
  ].map(([clientId, agentId]) =>
    gateway.startSession({ clientId, agentId, runtimeId: "pi", conversationId: "shared" }),
  );
  const runs = sessions.map((session) => gateway.submit({ ...session, content: "control-wait" }));
  await started;
  const control = {
    ...sessions[0],
    runId: runs[0].receipt.runId,
    controlId: "steer",
    operation: "steer",
    content: "adjust-writer",
  };
  assert.equal((await gateway.control(control)).status, "queued");
  assert.equal(
    (
      await gateway.control({
        ...control,
        controlId: "follow",
        operation: "follow_up",
        content: "later-writer",
      })
    ).status,
    "queued",
  );
  await assert.rejects(
    gateway.control({ ...control, controlId: "stale", runId: runs[1].receipt.runId }),
    /active run/i,
  );
  await assert.rejects(
    gateway.control({ ...control, controlId: "foreign", clientId: "second" }),
    /bound session/i,
  );
  const deliveries = events
    .filter((event) => event.type === "message.delta")
    .map((event) => ({ event, native: JSON.parse(event.data.delta) }));
  const nativeControls = deliveries.filter(({ native }) => native.control);
  assert.deepEqual(
    nativeControls.map(({ native }) => native.control),
    ["steer", "follow_up"],
  );
  assert.ok(
    nativeControls.every(
      ({ event, native }) =>
        event.clientId === "first" &&
        event.runId === runs[0].receipt.runId &&
        native.prompts === 1 &&
        native.home === path.join(config.dataRoot, "pi", "writer", "home"),
    ),
  );
  assert.equal(new Set(deliveries.slice(0, 3).map(({ native }) => native.pid)).size, 3);
  assert.equal(events.filter((event) => event.type === "run.started").length, 3);
  for (const runtimeId of ["deepseek-harness", "openclaw"]) {
    assert.equal(gateway.capabilities(runtimeId).steering, false);
    assert.equal(gateway.capabilities(runtimeId).followUp, false);
  }
  await Promise.all(sessions.map((session) => gateway.cancel(session)));
  await Promise.all(runs.map((run) => run.completion));
});
