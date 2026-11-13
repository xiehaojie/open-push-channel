import assert from "node:assert/strict";
import test from "node:test";

await import("./mentions.js");

const { extractMentions } = globalThis;

test("extractMentions captures agents after Chinese punctuation", () => {
  assert.deepEqual(
    extractMentions("请 @researcher 调研当前链路，@coder 给出检查清单。"),
    [
      { agentId: "researcher", label: "researcher" },
      { agentId: "coder", label: "coder" },
    ],
  );
});

test("extractMentions deduplicates repeated agents", () => {
  assert.deepEqual(extractMentions("@coder 先看，稍后再让 @coder 汇总"), [
    { agentId: "coder", label: "coder" },
  ]);
});
