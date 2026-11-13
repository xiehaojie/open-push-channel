const assert = require("node:assert/strict");
const test = require("node:test");
const { createDeepSeekAdapter } = require("./deepseek");
const { validateAdapterSignal } = require("../../../protocol/index.cjs");

// Controlled SDK transport peer, not a live provider/model integration.
const fixture = String.raw`
let buffer = "";
const sessions = new Map();
const resumed = new Set();
const send = (record) => process.stdout.write(JSON.stringify({jsonrpc:"2.0",...record})+"\n");
const answer = (command,result={}) => send({id:command.id,result});
const notify = (method,params) => send({method,params});
const event = (sessionId,type,data) => notify("session.event",{sessionId,event:{type,data,seq:1,time:1}});
process.stdin.on("data",chunk=>{
  buffer += chunk;
  let newline;
  while ((newline=buffer.indexOf("\n"))!==-1) {
    const command = JSON.parse(buffer.slice(0,newline)); buffer=buffer.slice(newline+1);
    const params=command.params;
    if (command.method==="initialize") {
      if (!params.cwd || params.provider!=="deepseek-official" || params.model!=="deepseek-v4-flash") process.exit(3);
      answer(command,{serverInfo:{name:process.env.PUSH_TEST_BAD_IDENTITY ? "impostor" : "deepseek-harness-sdk-runtime",version:"0.0.1"}});
    } else if (command.method==="session/resume") {
      if(params.sessionId==="missing" || resumed.has(params.sessionId)) {
        send({id:command.id,error:{code:-32603,message:"Resume rejected: missing or occupied"}});continue;
      }
      resumed.add(params.sessionId);answer(command);
    } else if (command.method==="session/prompt") {
      const content=params.contentBlocks[0].text;
      if(process.env.PUSH_TEST_REQUIRE_RESUME && !resumed.has(params.sessionId)) {
        send({id:command.id,error:{code:-32603,message:"Prompt without explicit restore"}});continue;
      }
      if(content==="restored" && !resumed.has(params.sessionId)) {
        send({id:command.id,error:{code:-32603,message:"Restored prompt requires explicit resume"}});continue;
      }
      if (content==="malformed") {process.stdout.write("invalid\n");continue;}
      if (content==="exit") process.exit(2);
      if (content==="reject") {send({id:command.id,error:{code:-32603,message:"Provider not configured"}});continue;}
      const previous=sessions.get(params.sessionId);
      sessions.set(params.sessionId,{content,turn:(previous?.turn||0)+1});
      answer(command,{messageId:"queued-"+params.sessionId});
    } else if(command.method==="session/wait") {
      const {content,turn}=sessions.get(params.sessionId);
      const root=params.sessionId;
      if(content==="crash-after-accept") process.exit(2);
      if(content==="wait-error") {send({id:command.id,error:{code:-32603,message:"Agent failed before durable terminal"}});continue;}
      if(content==="wait") continue;
      notify("subagent.started",{parentSessionId:"unrelated",childSessionId:"foreign"});
      event("foreign","assistant/message",{message:{id:"foreign",content:[{type:"text",text:"PRIVATE"}]}});
      notify("subagent.started",{parentSessionId:root,childSessionId:root+"-child"});
      notify("subagent.started",{parentSessionId:root+"-child",childSessionId:root+"-grandchild"});
      event(root+"-grandchild","assistant/message",{message:{id:"child-answer",content:[{type:"text",text:"nested answer"}]}});
      event(root+"-child","tool/call",{callId:"child-tool",name:"read",arguments:"{}"});
      event(root+"-child","tool/result",{message:{toolCallId:"child-tool",content:[{type:"text",text:"child output"}],isError:false}});
      notify("subagent.finished",{parentSessionId:root+"-child",childSessionId:root+"-grandchild",agentId:root+"-grandchild",status:"ok",stopReason:"completed",lastAssistantMessage:[{type:"text",text:"nested answer"}]});
      notify("subagent.finished",{parentSessionId:root,childSessionId:root+"-child",agentId:root+"-child",status:content==="child-error"?"error":"ok",stopReason:content==="child-error"?"error":"completed"});
      event(root,"tool/call",{callId:"tool",name:"read",arguments:content==="bad-args"?"not-json":"{\"path\":\"README.md\"}"});
      event(root,"tool/result",{message:{toolCallId:"tool",content:[{type:"text",text:"file output"}],isError:false}});
      const text=content==="inspect-env"?JSON.stringify({
        defaultTokenPresent:process.env.PUSH_CHANNEL_PUSH_TOKEN!==undefined,
        clientTokenPresent:process.env.PUSH_TEST_CLIENT_TOKEN!==undefined,
        pushTokenPresent:process.env.PUSH_TEST_PUSH_TOKEN!==undefined,
        modelCredentialKept:process.env.DEEPSEEK_API_KEY==="test-native-provider-credential"
      }):"你好\u2028"+process.pid+":"+turn;
      const bytes=Buffer.from(JSON.stringify({jsonrpc:"2.0",method:"session.event",params:{sessionId:root,event:{type:"assistant/message",seq:2,time:2,data:{message:{id:"answer-"+turn,content:[{type:"reasoning",text:"Checking"},{type:"text",text}]}}}}})+"\n");
      const split=bytes.indexOf(Buffer.from("你好"))+1;
      process.stdout.write(bytes.subarray(0,split));process.stdout.write(bytes.subarray(split));
      event(root,"turn/end",{reason:content==="error"?{kind:"error",error:{message:"Model failed",code:"UNKNOWN"}}:content==="blocked"?{kind:"blocked"}:{kind:"completed"}});
      setTimeout(()=>answer(command),20);
    } else if(command.method==="shutdown") {answer(command);process.exit(0);}
  }
});
process.stdin.on("end",()=>process.exit(0));
`;

function setup(t, options = {}) {
  const adapter = createDeepSeekAdapter({
    command: process.execPath,
    args: ["-e", fixture, "--"],
    ...options,
  });
  t.after(() => adapter.close());
  return adapter;
}
const request = (content, conversationId = "chat", agentId = "main") => ({
  runtimeId: "deepseek-harness",
  agentId,
  conversationId,
  content,
});
const context = (events) => ({ emit: (event) => events.push(validateAdapterSignal(event)) });

void test("DeepSeek native SDK preserves sessions, waits for descendants and projects committed content", async (t) => {
  const adapter = setup(t);
  assert.equal(adapter.capabilities.streaming, false);
  assert.equal(adapter.capabilities.subagents, true);
  assert.equal(adapter.capabilities.cancel, false);
  const events = [];
  await adapter.run(request("hello"), context(events));
  assert.equal(
    events.some((event) => event.type === "message.delta"),
    false,
  );
  const text = events.find((event) => event.type === "message.snapshot").data.text;
  assert.match(text, /^你好\u2028\d+:1$/u);
  assert.equal(events.find((event) => event.type === "reasoning.delta").data.delta, "Checking");
  assert.equal(events.find((event) => event.type === "tool.completed").data.content, "file output");
  assert.equal(
    events.find((event) => event.type === "subagent.tool.completed").data.content,
    "child output",
  );
  assert.equal(events.filter((event) => event.type === "subagent.started").length, 2);
  assert.equal(
    events.some((event) => JSON.stringify(event).includes("PRIVATE")),
    false,
  );
  assert.ok(
    events.some(
      (event) => event.type === "subagent.result" && event.data.content === "nested answer",
    ),
  );
  const next = [];
  await adapter.run(request("again"), context(next));
  assert.equal(
    next.find((event) => event.type === "message.snapshot").data.text,
    text.replace(/:1$/, ":2"),
  );
  const isolated = [];
  await adapter.run(request("other", "other"), context(isolated));
  assert.match(isolated.find((event) => event.type === "message.snapshot").data.text, /:1$/);
});

void test("DeepSeek root failures, protocol errors and transport loss reject instead of success", async (t) => {
  for (const [content, pattern] of [
    ["error", /Model failed/],
    ["blocked", /blocked/],
    ["reject", /Provider not configured/],
    ["wait-error", /Agent failed/],
    ["exit", /exited/],
    ["malformed", /protocol|JSON/],
  ]) {
    await t.test(content, async (child) => {
      const adapter = setup(child);
      await assert.rejects(adapter.run(request(content), context([])), pattern);
    });
  }
});

void test("DeepSeek preserves malformed tool argument text and explicit child failures", async (t) => {
  const adapter = setup(t);
  const events = [];
  await adapter.run(request("bad-args"), context(events));
  assert.deepEqual(events.find((event) => event.type === "tool.started").data.args, {
    rawArguments: "not-json",
  });
  const failed = [];
  await adapter.run(request("child-error"), context(failed));
  assert.ok(failed.some((event) => event.type === "subagent.failed"));
});

void test("DeepSeek rejects unsupported mentions, wrong server identity, missing executable and shutdown", async (t) => {
  const adapter = setup(t);
  await assert.rejects(
    adapter.run({ ...request("hello"), mentions: [{ agentId: "coder" }] }, context([])),
    /mentions/,
  );
  const wrong = setup(t, { env: { PUSH_TEST_BAD_IDENTITY: "1" } });
  await assert.rejects(wrong.run(request("hello"), context([])), /identity/);
  const missing = setup(t, { command: "/nonexistent/push-test-dsh" });
  await assert.rejects(missing.run(request("hello"), context([])), /start/);
  const controller = new AbortController();
  const pending = adapter.run(request("wait"), { ...context([]), signal: controller.signal });
  controller.abort();
  await assert.rejects(pending, /shutdown|abort/i);
});

void test("DeepSeek explicitly restores native sessions once and never creates after failed restore", async (t) => {
  const adapter = setup(t, { env: { PUSH_TEST_REQUIRE_RESUME: "1" } });
  const restored = { ...request("hello"), nativeSessionId: "native-session", resume: true };
  await adapter.run(restored, context([]));
  await adapter.run(restored, context([]));
  await assert.rejects(
    adapter.run({ ...restored, nativeSessionId: "missing" }, context([])),
    /Resume rejected/,
  );
});

void test("DeepSeek marks prompt admission exactly once and preserves unknown transport outcomes", async (t) => {
  for (const [content, expected] of [
    ["hello", 1],
    ["reject", 0],
    ["exit", 1],
  ]) {
    const adapter = setup(t);
    let accepted = 0;
    const run = adapter.run(request(content), {
      ...context([]),
      onAccepted() {
        accepted++;
      },
    });
    if (content === "reject" || content === "exit") {
      await assert.rejects(run);
    } else {
      await run;
    }
    assert.equal(accepted, expected, content);
  }
  for (const options of [
    { command: "/nonexistent/push-test-dsh-admission" },
    { env: { PUSH_TEST_BAD_IDENTITY: "1" } },
  ]) {
    const adapter = setup(t, options);
    let accepted = 0;
    await assert.rejects(
      adapter.run(request("hello"), {
        ...context([]),
        onAccepted() {
          accepted++;
        },
      }),
    );
    assert.equal(accepted, 0);
  }
  const missing = setup(t);
  let accepted = 0;
  await assert.rejects(
    missing.run(
      { ...request("hello"), nativeSessionId: "missing", resume: true },
      {
        ...context([]),
        onAccepted() {
          accepted++;
        },
      },
    ),
  );
  assert.equal(accepted, 0);
});

void test("DeepSeek records dispatch only after initialize and restore, before prompt write", async (t) => {
  const adapter = setup(t);
  const order = [];
  await adapter.run(request("hello"), {
    ...context([]),
    onDispatch() {
      order.push("dispatch");
    },
    onAccepted() {
      order.push("accepted");
    },
  });
  assert.deepEqual(order, ["dispatch", "accepted"]);
  for (const [options, nativeSessionId] of [
    [{ command: "/nonexistent/push-test-dsh-dispatch" }, "session"],
    [{ env: { PUSH_TEST_BAD_IDENTITY: "1" } }, "session"],
    [{}, "missing"],
  ]) {
    const peer = setup(t, options);
    let dispatches = 0;
    await assert.rejects(
      peer.run(
        { ...request("hello"), nativeSessionId, resume: true },
        {
          ...context([]),
          onDispatch() {
            dispatches++;
          },
        },
      ),
    );
    assert.equal(dispatches, 0);
  }
  const blocked = setup(t);
  let admissions = 0;
  await assert.rejects(
    blocked.run(request("hello"), {
      ...context([]),
      onDispatch() {
        throw new Error("Journal unavailable");
      },
      onAccepted() {
        admissions++;
      },
    }),
    /Journal unavailable/,
  );
  assert.equal(admissions, 0);
  const events = [];
  await blocked.run(request("hello"), context(events));
  assert.match(events.find((event) => event.type === "message.snapshot").data.text, /:1$/);
});

void test("DeepSeek child environment excludes all gateway auth names and retains model credentials", async (t) => {
  const names = ["PUSH_CHANNEL_PUSH_TOKEN", "PUSH_TEST_CLIENT_TOKEN", "PUSH_TEST_PUSH_TOKEN"];
  const previous = names.map((name) => process.env[name]);
  for (const name of names) {
    process.env[name] = "test-inherited-gateway-secret";
  }
  t.after(() => {
    names.forEach((name, index) => {
      if (previous[index] === undefined) {
        delete process.env[name];
      } else {
        process.env[name] = previous[index];
      }
    });
  });
  const adapter = setup(t, {
    excludedEnvKeys: ["PUSH_TEST_CLIENT_TOKEN", "PUSH_TEST_PUSH_TOKEN"],
    env: {
      ...Object.fromEntries(names.map((name) => [name, "test-configured-gateway-secret"])),
      DEEPSEEK_API_KEY: "test-native-provider-credential",
    },
  });
  const events = [];
  await adapter.run(request("inspect-env"), context(events));
  assert.deepEqual(
    JSON.parse(events.find((event) => event.type === "message.snapshot").data.text),
    {
      defaultTokenPresent: false,
      clientTokenPresent: false,
      pushTokenPresent: false,
      modelCredentialKept: true,
    },
  );
});

void test("DeepSeek transient native identities include the client owner", async (t) => {
  const adapter = setup(t);
  for (const clientId of ["first", "second"]) {
    const events = [];
    await adapter.run({ ...request("hello"), clientId }, context(events));
    assert.match(events.find((event) => event.type === "message.snapshot").data.text, /:1$/);
  }
});

void test("DeepSeek replaces a failed peer only for explicit new requests and restores prior ownership", async (t) => {
  const adapter = setup(t);
  const saved = { ...request("crash-after-accept"), nativeSessionId: "saved-native-session" };
  let admissions = 0;
  await assert.rejects(
    adapter.run(saved, {
      ...context([]),
      onAccepted() {
        admissions++;
      },
    }),
    /exited/,
  );
  assert.equal(admissions, 1);
  const unrelated = [];
  const concurrent = [];
  await Promise.all([
    adapter.run(
      { ...request("hello", "unrelated"), nativeSessionId: "new-native-session" },
      context(unrelated),
    ),
    adapter.run(
      { ...request("hello", "concurrent"), nativeSessionId: "concurrent-native-session" },
      context(concurrent),
    ),
  ]);
  assert.match(unrelated.find((event) => event.type === "message.snapshot").data.text, /:1$/);
  assert.equal(
    unrelated.find((event) => event.type === "message.snapshot").data.text,
    concurrent.find((event) => event.type === "message.snapshot").data.text,
    "concurrent new requests must share one replacement process",
  );
  const restored = [];
  await adapter.run({ ...saved, content: "restored", resume: true }, context(restored));
  await adapter.run({ ...saved, content: "restored", resume: true }, context([]));
  assert.equal(admissions, 1, "accepted crashed prompt must not be replayed");
});

void test("DeepSeek fully reaps malformed peers before an explicit fresh conversation", async (t) => {
  const adapter = setup(t);
  await assert.rejects(adapter.run(request("malformed"), context([])), /protocol|JSON/);
  const events = [];
  await adapter.run(request("hello", "fresh"), context(events));
  assert.match(events.find((event) => event.type === "message.snapshot").data.text, /:1$/);
});
