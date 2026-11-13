const assert = require("node:assert/strict");
const test = require("node:test");
const { createPiAdapter } = require("./pi");
const { validateAdapterSignal } = require("../../../protocol/index.cjs");

// Controlled native RPC subprocess; this proves process/framing contracts, not a live model.
const fixture = String.raw`
let buffer = "";
let turns = 0;
let waiting = false;
let extensionActive = false;
let compactionActive = false;
let compactionRace = false;
let steering = [];
let followUp = [];
let afterAbort;
const send = (event) => process.stdout.write(JSON.stringify(event) + "\n");
const response = (command, extra = {}) => send({type:"response",command:command.type,id:command.id,success:true,...extra});
const assistant = (content, stopReason = "stop", errorMessage) => ({role:"assistant",content:[{type:"text",text:content}],stopReason,errorMessage});
process.stdin.on("data", chunk => {
  buffer += chunk;
  let newline;
  while ((newline = buffer.indexOf("\n")) !== -1) {
    const command = JSON.parse(buffer.slice(0,newline));
    buffer = buffer.slice(newline+1);
    if(command.type === "get_state") {
      const createIndex=process.argv.indexOf("--session-id");
      const resumeIndex=process.argv.indexOf("--session");
      const id=process.argv[(createIndex===-1?resumeIndex:createIndex)+1];
      if(resumeIndex!==-1 && id==="missing") process.exit(2);
      if(compactionRace) {
        compactionRace=false;compactionActive=false;
        send({type:"compaction_end",reason:"manual",aborted:false});
        response(command,{data:{sessionId:id,isStreaming:false,isCompacting:true}});continue;
      }
      response(command,{data:{sessionId:process.env.PUSH_TEST_NATIVE_MISMATCH?"wrong":id,isStreaming:extensionActive,isCompacting:compactionActive}});continue;
    }
    if (command.type === "clear_queue") {
      const discarded = {steering,followUp};steering=[];followUp=[];
      send({type:"queue_update",steering,followUp});response(command,{data:discarded});continue;
    }
    if (command.type === "steer" || command.type === "follow_up") {
      if(command.message === "reject-control") {send({type:"response",id:command.id,success:false,error:"Control rejected"});continue;}
      if(command.message === "handled-control") {response(command,{data:{disposition:"handled"}});continue;}
      const enqueue=()=>{
        (command.type === "steer" ? steering : followUp).push(command.message);
        send({type:"queue_update",steering,followUp});
        response(command,{data:{disposition:"queued"}});
      };
      if(command.message === "settle-before-control") {
        send({type:"agent_settled",aborted:false});setTimeout(enqueue,20);
      } else if(command.message === "control-awaits-abort") {
        afterAbort=enqueue;
        send({type:"queue_update",steering,followUp});
      } else enqueue();
      continue;
    }
    if (command.type === "abort") {
      response(command); waiting = false; send({type:"agent_settled",aborted:true});
      if(afterAbort) {const delayed=afterAbort;afterAbort=undefined;setTimeout(delayed,20);}
      continue;
    }
    if (command.type !== "prompt") continue;
    turns++;
    if (command.message === "reject") { send({type:"response",id:command.id,command:"prompt",success:false,error:"Model not found"}); continue; }
    if (command.message === "malformed") { process.stdout.write("not-json\n"); continue; }
    if (command.message === "exit") { process.exit(2); }
    if (command.message === "handled") { response(command,{data:{disposition:"handled"}}); continue; }
    if(command.message.startsWith("handled-compact")) {
      compactionActive=true;
      response(command,{data:{disposition:"handled"}});
      const compact=()=>{compactionActive=false;send({type:"compaction_end",reason:"manual",aborted:false});};
      if(command.message==="handled-compact-fast") compact();
      else if(command.message==="handled-compact-race") compactionRace=true;
      else setTimeout(compact,30);
      continue;
    }
    if (command.message === "handled-run" || command.message === "handled-fast") {
      extensionActive = true;
      response(command,{data:{disposition:"handled"}});
      const work=()=>{
        send({type:"agent_start"});send({type:"message_start",message:assistant("")});
        send({type:"message_update",assistantMessageEvent:{type:"text_delta",contentIndex:0,delta:"extension answer"}});
        send({type:"message_end",message:assistant("extension answer")});
        extensionActive=false;send({type:"agent_settled",aborted:false});
      };
      if(command.message === "handled-fast") work();
      else setTimeout(work,30);
      continue;
    }
    response(command,{data:{disposition:"started"}});
    send({type:"agent_start"});
    if(command.message.startsWith("child")) {
      const wrap=(event,overrides={},toolCallId="delegate-1")=>send({type:"tool_execution_update",toolCallId,toolName:"subagent",partialResult:{content:[],details:{pushChannelSubagent:{version:1,childSessionId:"child-native-1",event,...overrides}}}});
      // Unowned updates cannot introduce a child into this run.
      wrap({type:"started"});
      send({type:"tool_execution_start",toolCallId:"delegate-1",toolName:"subagent",args:{task:"Read the marker"}});
      wrap({type:"started"});
      if(command.message==="child-parent-exit") {process.exit(2);}
      if(command.message==="child-parent-protocol") {process.stdout.write("bad-json\n");continue;}
      wrap({type:"message_update",assistantMessageEvent:{type:"text_delta",delta:"unowned interleaved text"}},{},"foreign-tool");
      wrap({type:"message_update",assistantMessageEvent:{type:"text_delta",delta:"child progress"}});
      wrap({type:"tool_execution_start",toolCallId:"read-1",toolName:"read",args:{path:"marker.txt"}});
      wrap({type:"tool_execution_end",toolCallId:"read-1",toolName:"read",result:{content:[{type:"text",text:"marker"}]},isError:false});
      wrap({type:"message_end",message:assistant("child answer")});
      if(command.message==="child-invalid") wrap({type:"message_update",assistantMessageEvent:{type:"text_delta",delta:[]}});
      wrap({type:"finished",status:command.message==="child-error"?"error":command.message==="child-cancel"?"cancelled":"ok"});
      if(command.message==="child-ended-parent-exit") {process.exit(2);}
      send({type:"tool_execution_end",toolCallId:"delegate-1",toolName:"subagent",result:{content:[{type:"text",text:"child answer"}]},isError:command.message==="child-error"});
      wrap({type:"message_update",assistantMessageEvent:{type:"text_delta",delta:"late stale text"}});
      send({type:"agent_settled",aborted:command.message==="child-cancel"});continue;
    }
    if (command.message === "wait") { waiting = true; continue; }
    if (command.message === "dialog") { send({type:"extension_ui_request",id:"dialog",method:"confirm",title:"Approve?"}); continue; }
    if (command.message === "error") { send({type:"message_end",message:assistant("","error","Provider failed")}); send({type:"agent_settled",aborted:false}); continue; }
    if (command.message === "retry") {
      send({type:"message_end",message:assistant("","error","Retry me")});
      send({type:"agent_end",messages:[],willRetry:true});
      send({type:"auto_retry_start",attempt:1});
    }
    send({type:"message_start",message:assistant("")});
    send({type:"message_update",assistantMessageEvent:{type:"thinking_start",contentIndex:0}});
    send({type:"message_update",assistantMessageEvent:{type:"thinking_delta",contentIndex:0,delta:"Checking"}});
    send({type:"message_update",assistantMessageEvent:{type:"thinking_end",contentIndex:0,content:"Checking"}});
    const text = command.message==="inspect-env" ? JSON.stringify({
      defaultTokenPresent:process.env.PUSH_CHANNEL_PUSH_TOKEN!==undefined,
      clientTokenPresent:process.env.PUSH_TEST_CLIENT_TOKEN!==undefined,
      pushTokenPresent:process.env.PUSH_TEST_PUSH_TOKEN!==undefined,
      modelCredentialKept:process.env.ANTHROPIC_API_KEY==="test-native-provider-credential"
    }) : "你好\u2028" + process.pid + ":" + turns;
    const bytes = Buffer.from(JSON.stringify({type:"message_update",assistantMessageEvent:{type:"text_delta",contentIndex:1,delta:text}})+"\n");
    const split = bytes.indexOf(Buffer.from("你好"))+1;
    process.stdout.write(bytes.subarray(0,split));
    process.stdout.write(bytes.subarray(split));
    send({type:"message_end",message:assistant(text)});
    send({type:"tool_execution_start",toolCallId:"tool-1",toolName:"read",args:{path:"README.md"}});
    send({type:"tool_execution_end",toolCallId:"tool-1",toolName:"read",result:{content:[{type:"text",text:"file output"}]},isError:false});
    send({type:"agent_end",messages:[],willRetry:false});
    setTimeout(()=>send({type:"agent_settled",aborted:false}),20);
  }
});
process.stdin.on("end",()=>process.exit(0));
`;

function setup(t, options = {}) {
  const adapter = createPiAdapter({
    command: process.execPath,
    args: ["-e", fixture, "--"],
    ...options,
  });
  t.after(() => adapter.close());
  return adapter;
}
const request = (content, conversationId = "chat") => ({
  runtimeId: "pi",
  agentId: "main",
  conversationId,
  content,
});
const context = (events) => ({ emit: (event) => events.push(validateAdapterSignal(event)) });

void test("Pi native RPC retains context per conversation and projects text, reasoning and tools", async (t) => {
  const adapter = setup(t);
  assert.deepEqual(adapter.capabilities, {
    streaming: true,
    tools: true,
    reasoning: true,
    subagents: true,
    cancel: true,
    sessionRestore: true,
    steering: true,
    followUp: true,
  });
  const first = [];
  await adapter.run(request("hello"), context(first));
  const text = first.find((event) => event.type === "message.delta").data.delta;
  assert.match(text, /^你好\u2028\d+:1$/u);
  assert.equal(first.find((event) => event.type === "message.snapshot").data.text, text);
  assert.equal(first.find((event) => event.type === "reasoning.delta").data.delta, "Checking");
  assert.equal(first.find((event) => event.type === "tool.completed").data.content, "file output");
  const second = [];
  await adapter.run(request("again"), context(second));
  assert.equal(
    second.find((event) => event.type === "message.delta").data.delta,
    text.replace(/:1$/, ":2"),
  );
  const isolated = [];
  await adapter.run(request("another", "other"), context(isolated));
  assert.notEqual(isolated.find((event) => event.type === "message.delta").data.delta, text);
});

void test("Pi projects only owned native child activity and closes its lifecycle", async (t) => {
  const adapter = setup(t);
  const events = [];
  await adapter.run(request("child"), context(events));
  const children = events.filter((event) => event.type.startsWith("subagent."));
  assert.deepEqual(
    children.map((event) => event.type),
    [
      "subagent.started",
      "subagent.delta",
      "subagent.tool.started",
      "subagent.tool.completed",
      "subagent.message",
      "subagent.result",
      "subagent.ended",
    ],
  );
  assert.ok(children.every((event) => event.data.childSessionId === "child-native-1"));
  assert.equal(children[3].data.content, "marker");
  assert.equal(children.at(-1).data.status, "ok");
});

void test("Pi child failure and cancellation reach terminal child cards", async (t) => {
  const adapter = setup(t);
  for (const [content, status] of [
    ["child-error", "error"],
    ["child-cancel", "cancelled"],
  ]) {
    const events = [];
    const result = await adapter.run(request(content), context(events));
    assert.equal(events.find((event) => event.type === "subagent.ended")?.data.status, status);
    assert.equal(
      events.some((event) => event.type === "subagent.failed"),
      status === "error",
    );
    assert.equal(
      events.some((event) => event.type === "subagent.result"),
      false,
    );
    if (status === "cancelled") assert.deepEqual(result, { status: "cancelled" });
  }
});

void test("Pi rejects malformed owned child deltas rather than publishing invalid events", async (t) => {
  const adapter = setup(t);
  await assert.rejects(adapter.run(request("child-invalid"), context([])), /Pi subagent/);
});

void test("Pi parent process and protocol failures terminate started child cards exactly once", async (t) => {
  for (const content of [
    "child-parent-exit",
    "child-parent-protocol",
    "child-invalid",
    "child-ended-parent-exit",
  ]) {
    const adapter = setup(t);
    const events = [];
    await assert.rejects(adapter.run(request(content), context(events)));
    const terminal = events.filter((event) => event.type === "subagent.ended");
    assert.equal(terminal.length, 1, content);
    const endedBeforeParent = content === "child-ended-parent-exit";
    assert.equal(terminal[0].data.status, endedBeforeParent ? "ok" : "error");
    assert.equal(
      events.filter((event) => event.type === "subagent.failed").length,
      endedBeforeParent ? 0 : 1,
    );
  }
});

void test("Pi waits for agent_settled and lets successful retry replace transient failure", async (t) => {
  const adapter = setup(t);
  const events = [];
  await adapter.run(request("retry"), context(events));
  assert.ok(events.some((event) => event.type === "message.completed"));
  // A second turn can only start once the first native run has really settled.
  await adapter.run(request("hello"), context([]));
});

void test("Pi reports provider, command, process and malformed protocol failures", async (t) => {
  for (const [content, pattern] of [
    ["reject", /Model not found/],
    ["error", /Provider failed/],
    ["exit", /exited/],
    ["malformed", /protocol|JSON/],
    ["dialog", /interaction|UI/],
  ]) {
    await t.test(content, async (subtest) => {
      const adapter = setup(subtest);
      await assert.rejects(adapter.run(request(content), context([])), pattern);
    });
  }
});

void test("Pi handles consumed prompts without waiting for a nonexistent run", async (t) => {
  const adapter = setup(t);
  await adapter.run(request("handled"), context([]));
});

void test("Pi supports native cancellation without cancelling another conversation", async (t) => {
  const adapter = setup(t);
  const active = request("wait");
  const run = adapter.run(active, context([]));
  // cancel() also works while process startup/prompt acceptance is in progress.
  assert.equal(await adapter.cancel(active), true);
  assert.deepEqual(await run, { status: "cancelled" });
  assert.equal(await adapter.cancel(request("wait", "other")), false);
  await adapter.run(request("hello"), context([]));
});

void test("Pi rejects unsupported mentions and startup failures, and shutdown aborts its own process", async (t) => {
  const adapter = setup(t);
  await assert.rejects(
    adapter.run({ ...request("hello"), mentions: [{ agentId: "coder" }] }, context([])),
    /mentions|subagent/,
  );
  const missing = setup(t, { command: "/nonexistent/push-test-pi" });
  await assert.rejects(missing.run(request("hello"), context([])), /start|ENOENT/);
  const controller = new AbortController();
  const running = adapter.run(request("wait"), { ...context([]), signal: controller.signal });
  controller.abort();
  await assert.rejects(running, /abort|shutdown/i);
});

void test("Pi uses exact native creation and restore identities before admitting a prompt", async (t) => {
  const fresh = setup(t);
  await fresh.run(
    { ...request("hello"), nativeSessionId: "native-new", resume: false },
    context([]),
  );
  const restored = setup(t);
  await restored.run(
    { ...request("hello"), nativeSessionId: "native-existing", resume: true },
    context([]),
  );
  const missing = setup(t);
  await assert.rejects(
    missing.run({ ...request("hello"), nativeSessionId: "missing", resume: true }, context([])),
    /exited/,
  );
  const wrong = setup(t, { env: { PUSH_TEST_NATIVE_MISMATCH: "1" } });
  await assert.rejects(
    wrong.run(
      { ...request("hello"), nativeSessionId: "native-existing", resume: true },
      context([]),
    ),
    /identity/,
  );
});

void test("Pi handled extension prompts retain their actual asynchronous run", async (t) => {
  for (const content of ["handled-run", "handled-fast"]) {
    const adapter = setup(t);
    const events = [];
    let accepted = 0;
    await adapter.run(request(content), {
      ...context(events),
      onAccepted() {
        accepted++;
      },
    });
    assert.equal(
      events.find((event) => event.type === "message.snapshot")?.data.text,
      "extension answer",
    );
    assert.equal(accepted, 1);
  }
});

void test("Pi marks native admission once, not on clean rejection or pure handled commands", async (t) => {
  for (const [content, expected] of [
    ["hello", 1],
    ["handled", 0],
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
  const missing = setup(t, { command: "/nonexistent/push-test-pi-admission" });
  let accepted = 0;
  await assert.rejects(
    missing.run(request("hello"), {
      ...context([]),
      onAccepted() {
        accepted++;
      },
    }),
  );
  assert.equal(accepted, 0);
  for (const [nativeSessionId, env] of [
    ["missing", undefined],
    ["native-existing", { PUSH_TEST_NATIVE_MISMATCH: "1" }],
  ]) {
    const adapter = setup(t, { env });
    let admitted = 0;
    await assert.rejects(
      adapter.run(
        { ...request("hello"), nativeSessionId, resume: true },
        {
          ...context([]),
          onAccepted() {
            admitted++;
          },
        },
      ),
    );
    assert.equal(admitted, 0);
  }
});

void test(
  "Pi handled manual compaction finishes without a nonexistent agent_settled",
  { timeout: 1500 },
  async (t) => {
    for (const content of ["handled-compact", "handled-compact-fast", "handled-compact-race"]) {
      const adapter = setup(t);
      await adapter.run(request(content), context([]));
    }
  },
);

void test("Pi records dispatch immediately before prompt write, never for failed preflight", async (t) => {
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
  for (const options of [
    { command: "/nonexistent/push-test-pi-dispatch" },
    { env: { PUSH_TEST_NATIVE_MISMATCH: "1" } },
  ]) {
    const peer = setup(t, options);
    let dispatches = 0;
    await assert.rejects(
      peer.run(
        { ...request("hello"), nativeSessionId: "native-existing", resume: true },
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

void test("Pi child environment excludes gateway secrets even when configured overrides re-add them", async (t) => {
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
      ANTHROPIC_API_KEY: "test-native-provider-credential",
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

void test("Pi process reuse and cancellation are isolated by client as well as conversation", async (t) => {
  const adapter = setup(t);
  const first = { ...request("hello"), clientId: "first", nativeSessionId: "native-first" };
  const second = { ...request("hello"), clientId: "second", nativeSessionId: "native-second" };
  const a = [];
  const b = [];
  await adapter.run(first, context(a));
  await adapter.run(second, context(b));
  const firstText = a.find((event) => event.type === "message.snapshot").data.text;
  const secondText = b.find((event) => event.type === "message.snapshot").data.text;
  assert.match(firstText, /:1$/);
  assert.match(secondText, /:1$/);
  assert.notEqual(firstText, secondText);
  const pending = adapter.run({ ...first, content: "wait" }, context([]));
  assert.equal(await adapter.cancel(second), false);
  assert.equal(await adapter.cancel(first), true);
  assert.deepEqual(await pending, { status: "cancelled" });
});

void test("Pi steers its active run, reports native queue snapshots, and clears residual inputs on abort", async (t) => {
  const adapter = setup(t);
  const active = { ...request("wait"), runId: "active" };
  const events = [];
  const run = adapter.run(active, context(events));
  void run.catch(() => {});
  await assert.rejects(
    adapter.control({ ...active, runId: "stale" }, { operation: "steer", content: "wrong" }),
    /active run/i,
  );
  assert.deepEqual(await adapter.control(active, { operation: "steer", content: "adjust" }), {
    status: "queued",
  });
  assert.deepEqual(await adapter.control(active, { operation: "follow_up", content: "later" }), {
    status: "queued",
  });
  assert.deepEqual(events.filter((event) => event.type === "input.queue").at(-1).data, {
    steering: ["adjust"],
    followUp: ["later"],
  });
  await assert.rejects(
    adapter.control(active, { operation: "steer", content: "reject-control" }),
    /Control rejected/,
  );
  assert.deepEqual(
    await adapter.control(active, { operation: "steer", content: "handled-control" }),
    { status: "handled" },
  );
  assert.equal(await adapter.cancel(active), true);
  assert.deepEqual(await run, { status: "cancelled" });
  const finalQueue = events.filter((event) => event.type === "input.queue").at(-1).data;
  assert.deepEqual(finalQueue, {
    steering: [],
    followUp: [],
    discardedSteering: ["adjust"],
    discardedFollowUp: ["later"],
    reason: "cancelled",
  });
  await adapter.run({ ...active, runId: "next", content: "hello" }, context([]));
});

void test("Pi retains its observer while a native control queues after agent_settled", async (t) => {
  const adapter = setup(t);
  const active = { ...request("wait"), runId: "active" };
  const events = [];
  const run = adapter.run(active, context(events));
  void run.catch(() => {});
  assert.deepEqual(
    await adapter.control(active, { operation: "follow_up", content: "settle-before-control" }),
    { status: "queued" },
  );
  await run;
  const queues = events.filter((event) => event.type === "input.queue");
  assert.deepEqual(queues.at(-1).data, {
    steering: [],
    followUp: [],
    discardedSteering: [],
    discardedFollowUp: ["settle-before-control"],
    reason: "run_settled",
  });
  await assert.rejects(
    adapter.control(active, { operation: "steer", content: "late" }),
    /active run/i,
  );
});

void test("Pi sends abort before a delayed control receipt, then drains and discards its late queue", async (t) => {
  const adapter = setup(t);
  const active = { ...request("wait"), runId: "active" };
  const events = [];
  let hookStarted;
  const hook = new Promise((resolve) => {
    hookStarted = resolve;
  });
  let settled = false;
  const run = adapter.run(active, {
    emit(event) {
      events.push(validateAdapterSignal(event));
      if (event.type === "input.queue") hookStarted();
    },
  });
  void run.then(
    () => {
      settled = true;
    },
    () => {},
  );
  const control = adapter.control(active, {
    operation: "follow_up",
    content: "control-awaits-abort",
  });
  void control.catch(() => {});
  await hook;
  assert.equal(settled, false);
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error("Abort was blocked behind the control hook")), 500);
  });
  t.after(() => clearTimeout(timer));
  const [receipt, cancelled, result] = await Promise.race([
    Promise.all([control, adapter.cancel(active), run]),
    timeout,
  ]);
  clearTimeout(timer);
  assert.equal(receipt.status, "queued");
  assert.equal(cancelled, true);
  assert.deepEqual(result, { status: "cancelled" });
  assert.deepEqual(events.filter((event) => event.type === "input.queue").at(-1).data, {
    steering: [],
    followUp: [],
    discardedSteering: [],
    discardedFollowUp: ["control-awaits-abort"],
    reason: "cancelled",
  });
  await adapter.run({ ...active, runId: "next", content: "hello" }, context([]));
});
