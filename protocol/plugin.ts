import { validateAdapterSignal } from "./index.ts";
import type { Adapter, AdapterSignal, RunRequest } from "./index.ts";

export type PluginCapabilities = Required<NonNullable<Adapter["capabilities"]>>;
export type PluginRequest = RunRequest & {
  clientId: string;
  nativeSessionId: string;
  runId: string;
};
export type NotificationTarget = { clientId: string; conversationId: string };
export type NotificationPayload = { eventId: string } & (
  | { content: string; signal?: never }
  | { signal: AdapterSignal; content?: never }
);
export type NotificationReceipt = { duplicate: boolean; runId: string };

const capabilityNames = [
  "streaming",
  "tools",
  "reasoning",
  "subagents",
  "cancel",
  "mentions",
  "sessionRestore",
  "steering",
  "followUp",
] as const;
export const requestFields = [
  "clientId",
  "runtimeId",
  "agentId",
  "conversationId",
  "nativeSessionId",
  "runId",
  "content",
  "resume",
  "requestId",
  "replyId",
  "origin",
  "mentions",
] as const satisfies readonly (keyof PluginRequest)[];

export function fields(value: unknown, allowed: readonly string[]): Record<string, unknown> {
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    Object.keys(value).some((key) => !allowed.includes(key))
  ) {
    throw new TypeError("Invalid plugin protocol fields");
  }
  return value as Record<string, unknown>;
}

export function text(value: unknown): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new TypeError("Invalid plugin protocol text");
  }
  return value;
}

export function capabilities(input: unknown): PluginCapabilities {
  const value = fields(input, capabilityNames);
  if (Object.values(value).some((flag) => typeof flag !== "boolean")) {
    throw new TypeError("Invalid plugin capabilities");
  }
  return Object.fromEntries(capabilityNames.map((key) => [key, value[key] ?? false])) as PluginCapabilities;
}

export function request(input: unknown): PluginRequest {
  // The request projection deliberately ignores transport-only fields.
  const value = input as Record<string, unknown>;
  const selected = Object.fromEntries(
    requestFields.filter((key) => value[key] !== undefined).map((key) => [key, value[key]]),
  );
  for (const key of [
    "clientId",
    "runtimeId",
    "agentId",
    "conversationId",
    "nativeSessionId",
    "runId",
  ]) {
    text(selected[key]);
  }
  if (
    typeof selected.content !== "string" ||
    (selected.resume !== undefined && typeof selected.resume !== "boolean")
  ) {
    throw new TypeError("Invalid plugin run request");
  }
  for (const key of ["requestId", "replyId"]) {
    if (selected[key] !== undefined) {
      text(selected[key]);
    }
  }
  if (selected.origin !== undefined && selected.origin !== "prompt" && selected.origin !== "push" && selected.origin !== "task") {
    throw new TypeError("Invalid plugin run origin");
  }
  if (selected.mentions !== undefined) {
    if (!Array.isArray(selected.mentions)) {
      throw new TypeError("Invalid plugin mentions");
    }
    for (const mention of selected.mentions) {
      fields(mention, ["agentId", "label"]);
      text(mention.agentId);
      if (mention.label !== undefined && typeof mention.label !== "string") {
        throw new TypeError("Invalid plugin mention label");
      }
    }
  }
  return selected as PluginRequest;
}

export function signal(value: unknown): AdapterSignal {
  fields(value, ["type", "data"]);
  return validateAdapterSignal(value);
}

export function notification(targetValue: unknown, payloadValue: unknown) {
  const target = fields(targetValue, ["clientId", "conversationId"]);
  text(target.clientId);
  text(target.conversationId);
  const payload = fields(payloadValue, ["eventId", "content", "signal"]);
  text(payload.eventId);
  if ((payload.content !== undefined) === (payload.signal !== undefined)) {
    throw new TypeError("Provide one plugin notification content or signal");
  }
  if (payload.content !== undefined) {
    text(payload.content);
  } else {
    signal(payload.signal);
  }
  return { target: target as NotificationTarget, payload: payload as NotificationPayload };
}
