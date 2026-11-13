const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const vm = require("node:vm");
const test = require("node:test");

const fixture = String.raw`
let buffer="";
const send=record=>process.stdout.write(JSON.stringify(record)+"\n");
const id=process.argv[process.argv.indexOf("--session-id")+1];
process.stdin.on("data",chunk=>{
 buffer+=chunk;let index;
 while((index=buffer.indexOf("\n"))!==-1){
  const record=JSON.parse(buffer.slice(0,index));buffer=buffer.slice(index+1);
  if(record.type==="get_state") send({type:"response",id:record.id,success:true,data:{sessionId:id}});
  if(record.type==="prompt"){
   send({type:"response",id:record.id,success:true,data:{disposition:"started"}});
   if(record.message==="wait")continue;
   if(record.message==="malformed"){process.stdout.write("bad-json\n");continue;}
   send({type:"agent_start"});
   const argv=process.argv.slice(2);
   const keyIndex=argv.indexOf("--api-key");
   const runtimeCredentialMatched=keyIndex!==-1 && argv[keyIndex+1]==="synthetic-runtime-credential";
   if(keyIndex!==-1) argv[keyIndex+1]="[redacted]";
   const args={argv,cwd:process.cwd(),home:process.env.HOME,runtimeCredentialMatched};
   send({type:"tool_execution_start",toolCallId:"read-1",toolName:"read",args:{path:"marker.txt"}});
   send({type:"tool_execution_end",toolCallId:"read-1",toolName:"read",result:{content:[{type:"text",text:"marker"}]},isError:false});
   send({type:"message_update",assistantMessageEvent:{type:"text_delta",delta:"你好\u2028"}});
   send({type:"message_end",message:{role:"assistant",content:[{type:"text",text:JSON.stringify(args)}],stopReason:record.message==="error"?"error":"stop",errorMessage:"private upstream diagnostics"}});
   send({type:"agent_settled",aborted:false});
  }
 }
});
process.stdin.on("end",()=>process.exit(0));
`;

async function setup(
  t,
  activeTools = ["read", "subagent", "custom"],
  extensionFlag = "--extension",
) {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "pi-subagent-unit-"));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const cli = path.join(directory, "pi-fixture.cjs");
  const extensionPath = path.join(__dirname, "pi-subagent.js");
  const extensionArgs = [extensionFlag, extensionPath];
  await fs.writeFile(cli, fixture);
  const children = [];
  const closedChildren = new Set();
  const handlers = new Map();
  const module = { exports: {} };
  const source = await fs.readFile(path.join(__dirname, "pi-subagent.js"), "utf8");
  vm.runInNewContext(
    source,
    {
      require(name) {
        if (name !== "node:child_process") return require(name);
        return {
          spawn(...args) {
            const child = spawn(...args);
            children.push(child);
            child.once("close", () => closedChildren.add(child));
            return child;
          },
        };
      },
      module,
      exports: module.exports,
      process: { ...process, argv: [process.execPath, cli, ...extensionArgs] },
      Buffer,
      AbortController,
    },
    { filename: extensionPath },
  );
  let tool;
  module.exports({
    registerTool(value) {
      tool = value;
    },
    on(name, handler) {
      handlers.set(name, handler);
    },
    getActiveTools() {
      return activeTools;
    },
    getAllTools() {
      return [
        { name: "read", sourceInfo: { path: "builtin:read", source: "builtin" } },
        { name: "write", sourceInfo: { path: "builtin:write", source: "builtin" } },
        { name: "subagent", sourceInfo: { path: "/trusted/extension.js", source: "cli" } },
        { name: "custom", sourceInfo: { path: "/trusted/custom.js", source: "cli" } },
      ];
    },
  });
  assert.equal(tool.name, "subagent");
  assert.deepEqual(Object.keys(tool.parameters.properties), ["task"]);
  return { tool, directory, children, closedChildren, handlers, extensionPath };
}

const context = (cwd) => ({
  cwd,
  model: { provider: "test-provider", id: "test-model" },
  thinkingLevel: "low",
  modelRegistry: {
    getProviderAuthStatus() {
      return { configured: true, source: "environment" };
    },
    getApiKeyForProvider() {
      throw new Error("Environment credentials do not need copying");
    },
  },
});

void test("Pi child inherits only its selected provider runtime credential without exposing it in results", async (t) => {
  const { tool, directory } = await setup(t);
  const updates = [];
  const ctx = {
    ...context(directory),
    modelRegistry: {
      getProviderAuthStatus(provider) {
        assert.equal(provider, "test-provider");
        return { configured: true, source: "runtime" };
      },
      async getApiKeyForProvider(provider) {
        assert.equal(provider, "test-provider");
        return "synthetic-runtime-credential";
      },
    },
  };
  const result = await tool.execute(
    "parent-tool",
    { task: "read" },
    undefined,
    (update) => updates.push(update),
    ctx,
  );
  assert.equal(result.isError, undefined);
  const invocation = JSON.parse(result.content[0].text);
  assert.equal(invocation.runtimeCredentialMatched, true);
  assert.equal(invocation.argv[invocation.argv.indexOf("--api-key") + 1], "[redacted]");
  assert.doesNotMatch(JSON.stringify([result, updates]), /synthetic-runtime-credential/);
});

void test("Pi runtime credential lookup failure is safe and never starts an unauthenticated child", async (t) => {
  for (const throws of [false, true]) {
    const { tool, directory, children } = await setup(t);
    const ctx = {
      ...context(directory),
      modelRegistry: {
        getProviderAuthStatus() {
          return { configured: true, source: "runtime" };
        },
        async getApiKeyForProvider() {
          if (throws) throw new Error("private credential resolver diagnostic");
          return undefined;
        },
      },
    };
    const result = await tool.execute("parent-tool", { task: "read" }, undefined, undefined, ctx);
    assert.equal(result.isError, true);
    assert.equal(children.length, 0);
    assert.doesNotMatch(JSON.stringify(result), /private credential resolver diagnostic/);
  }
});

void test("Pi owned extension executes a separate native session and inherits model/workspace without recursion", async (t) => {
  const { tool, directory, children, closedChildren } = await setup(t);
  const updates = [];
  const result = await tool.execute(
    "parent-tool",
    { task: "read" },
    undefined,
    (update) => updates.push(update.details.pushChannelSubagent),
    context(directory),
  );
  assert.equal(children.length, 1);
  assert.ok(
    children[0].exitCode !== null || children[0].signalCode !== null,
    "tool resolves only after child cleanup",
  );
  assert.ok(
    closedChildren.has(children[0]),
    "tool waits for child stdio close, not only process exit",
  );
  const invocation = JSON.parse(result.content[0].text);
  assert.equal(invocation.cwd, await fs.realpath(directory));
  assert.equal(invocation.home, process.env.HOME);
  assert.ok(invocation.argv.includes("--no-extensions"));
  assert.ok(!invocation.argv.some((arg) => /^(?:--extension|-e)(?:=|$)/.test(arg)));
  assert.equal(invocation.argv[invocation.argv.indexOf("--model") + 1], "test-provider/test-model");
  assert.equal(invocation.argv[invocation.argv.indexOf("--thinking") + 1], "low");
  assert.equal(invocation.argv[invocation.argv.indexOf("--tools") + 1], "read");
  assert.ok(!invocation.argv.includes("--no-tools"));
  assert.ok(!invocation.argv.includes("--api-key"));
  assert.equal(updates[0].event.type, "started");
  assert.equal(updates.at(-1).event.status, "ok");
  assert.ok(
    updates.every(
      (update) => update.version === 1 && update.childSessionId === updates[0].childSessionId,
    ),
  );
  assert.match(updates[0].childSessionId, /^[0-9a-f-]{36}$/);
  assert.equal(
    invocation.argv[invocation.argv.indexOf("--session-id") + 1],
    updates[0].childSessionId,
  );
  assert.ok(updates.some((update) => update.event.type === "tool_execution_end"));
  const nextUpdates = [];
  const nextResult = await tool.execute(
    "second-tool",
    { task: "read" },
    undefined,
    (update) => nextUpdates.push(update.details.pushChannelSubagent),
    context(directory),
  );
  const nextInvocation = JSON.parse(nextResult.content[0].text);
  assert.equal(
    nextInvocation.argv[nextInvocation.argv.indexOf("--session-id") + 1],
    nextUpdates[0].childSessionId,
  );
  assert.notEqual(nextUpdates[0].childSessionId, updates[0].childSessionId);
});

void test("Pi child preserves explicitly active native write access without adding other tools", async (t) => {
  // Inheritance is not a new read-only policy: retain write only when the parent enables it.
  const { tool, directory } = await setup(t, ["read", "write", "subagent"]);
  const result = await tool.execute(
    "parent-tool",
    { task: "read" },
    undefined,
    undefined,
    context(directory),
  );
  const invocation = JSON.parse(result.content[0].text);
  assert.equal(invocation.argv[invocation.argv.indexOf("--tools") + 1], "read,write");
});

void test("Pi child never forwards explicit parent extension flags or paths", async (t) => {
  for (const flag of ["--extension", "-e"]) {
    const { tool, directory, extensionPath } = await setup(t, ["read", "subagent"], flag);
    const result = await tool.execute(
      "parent-tool",
      { task: "read" },
      undefined,
      undefined,
      context(directory),
    );
    const invocation = JSON.parse(result.content[0].text);
    assert.ok(invocation.argv.includes("--no-extensions"));
    assert.ok(!invocation.argv.some((arg) => /^(?:--extension|-e)(?:=|$)/.test(arg)));
    assert.ok(!invocation.argv.some((arg) => arg.includes(extensionPath)));
  }
});

void test("Pi session shutdown waits for owned child close before native parent can exit", async (t) => {
  const { tool, directory, children, closedChildren, handlers } = await setup(t);
  let started;
  const ready = new Promise((resolve) => {
    started = resolve;
  });
  const updates = [];
  const run = tool.execute(
    "parent-tool",
    { task: "wait" },
    undefined,
    (update) => {
      const event = update.details.pushChannelSubagent.event;
      updates.push(event);
      if (event.type === "started") started();
    },
    context(directory),
  );
  await ready;
  assert.equal(typeof handlers.get("session_shutdown"), "function");
  await handlers.get("session_shutdown")({ type: "session_shutdown", reason: "quit" });
  assert.ok(closedChildren.has(children[0]), "shutdown must wait for native child stdio cleanup");
  const result = await run;
  assert.equal(result.isError, true);
  assert.equal(updates.at(-1).status, "cancelled");
});

void test("Pi child with no active native tools cannot regain default write/bash tools", async (t) => {
  const { tool, directory } = await setup(t, ["subagent", "custom"]);
  const result = await tool.execute(
    "parent-tool",
    { task: "read" },
    undefined,
    undefined,
    context(directory),
  );
  const invocation = JSON.parse(result.content[0].text);
  assert.ok(invocation.argv.includes("--no-tools"));
  assert.ok(!invocation.argv.includes("--tools"));
});

void test("Pi owned child cancellation waits for process cleanup and reports cancelled once", async (t) => {
  const { tool, directory, children, closedChildren } = await setup(t);
  const controller = new AbortController();
  const updates = [];
  const run = tool.execute(
    "parent-tool",
    { task: "wait" },
    controller.signal,
    (update) => {
      const event = update.details.pushChannelSubagent.event;
      updates.push(event);
      if (event.type === "started") controller.abort();
    },
    context(directory),
  );
  const result = await run;
  assert.equal(result.isError, true);
  assert.ok(children[0].exitCode !== null || children[0].signalCode !== null);
  assert.ok(closedChildren.has(children[0]));
  assert.deepEqual(
    updates.filter((event) => event.type === "finished").map((event) => event.status),
    ["cancelled"],
  );
});

void test("Pi owned child protocol/provider failures close children and do not leak diagnostics", async (t) => {
  for (const task of ["error", "malformed"]) {
    const { tool, directory, children, closedChildren } = await setup(t);
    const updates = [];
    const result = await tool.execute(
      "parent-tool",
      { task },
      undefined,
      (update) => updates.push(update.details.pushChannelSubagent.event),
      context(directory),
    );
    assert.equal(result.isError, true);
    assert.ok(children[0].exitCode !== null || children[0].signalCode !== null);
    assert.ok(closedChildren.has(children[0]));
    assert.equal(updates.at(-1).status, "error");
    assert.doesNotMatch(JSON.stringify([result, updates]), /private upstream diagnostics/);
  }
});
