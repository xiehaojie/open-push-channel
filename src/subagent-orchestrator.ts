import { randomUUID } from "node:crypto";
import { getPushChannelRuntime } from "./runtime.js";
import {
  bindChildSessionToParent,
  clearChildSessionBinding,
  getWriterForSessionOrChild,
  rememberSubagentDisplay,
} from "./tool-store.js";

export type PushChannelSubagentSpawnParams = {
  task: string;
  agentId: string;
  label?: string;
  parentSessionKey: string;
  channelId: string;
  accountId: string;
  sessionId: string;
  requesterAgentId: string;
};

type SpawnMentionedSubagentImpl = typeof spawnMentionedSubagentDirect;

let spawnMentionedSubagentImpl: SpawnMentionedSubagentImpl = spawnMentionedSubagentDirect;

export async function spawnMentionedSubagent(params: PushChannelSubagentSpawnParams) {
  return await spawnMentionedSubagentImpl(params);
}

async function spawnMentionedSubagentDirect(params: PushChannelSubagentSpawnParams) {
  const agentId = params.agentId.trim();
  const label = params.label?.trim() || agentId;
  const childSessionKey = `agent:${agentId}:subagent:${randomUUID()}`;

  bindChildSessionToParent(childSessionKey, params.parentSessionKey);
  rememberSubagentDisplay(childSessionKey, { agentId, label });
  getWriterForSessionOrChild(childSessionKey)?.({
    type: "subagent_start",
    agentId,
    label,
    childSessionKey,
  });

  try {
    const runtime = getPushChannelRuntime();
    const result = await runtime.subagent.run({
      sessionKey: childSessionKey,
      message: params.task,
      deliver: false,
      idempotencyKey: `push-channel:${params.sessionId}:${agentId}:${randomUUID()}`,
    });
    return {
      status: "accepted" as const,
      runId: result.runId,
      childSessionKey,
    };
  } catch (error) {
    clearChildSessionBinding(childSessionKey);
    return {
      status: "rejected" as const,
      childSessionKey,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

export const testing = {
  setSpawnMentionedSubagentImplForTest(impl?: SpawnMentionedSubagentImpl): void {
    spawnMentionedSubagentImpl = impl ?? spawnMentionedSubagentDirect;
  },
};
