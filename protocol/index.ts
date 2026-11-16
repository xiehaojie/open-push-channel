import { randomUUID } from "node:crypto";
export type Mention = { agentId: string; label?: string };
export type InputControl = {
  runId: string;
  controlId: string;
  operation: "steer" | "follow_up";
  content: string;
};
export type ControlReceipt = Omit<InputControl, "runId"> & {
  status: "dispatching" | "queued" | "handled" | "rejected" | "unknown";
  message?: string;
};
export type InputQueue = {
  steering: string[];
  followUp: string[];
  discardedSteering?: string[];
  discardedFollowUp?: string[];
  reason?: "run_settled" | "cancelled";
};
export type RunRequest = {
  clientId?: string;
  nativeSessionId?: string;
  resume?: boolean;
  runtimeId: string;
  agentId: string;
  conversationId: string;
  content: string;
  mentions?: Mention[];
  requestId?: string;
  replyId?: string;
  origin?: "prompt" | "push" | "task";
};
type Message = { messageId: string };
type ToolResult = { toolCallId: string; toolName?: string; content?: unknown; isError?: boolean };
type ToolCall = { toolCallId: string; toolName: string; args: Record<string, unknown> };
type Child = {
  agentId: string;
  label?: string;
  childSessionId?: string;
  messageId?: string;
  replace?: boolean;
  content?: unknown;
  delta?: string;
  message?: string;
  status?: string;
};
export type EventData = {
  "input.accepted": { content: string };
  "input.control": ControlReceipt;
  "input.queue": InputQueue;
  "run.started": Record<string, never>;
  "run.completed": Record<string, never>;
  "run.failed": { message: string };
  "run.cancelled": Record<string, never>;
  "run.deferred": { message: string };
  "message.started": Message;
  "message.delta": Message & { delta: string };
  "message.snapshot": Message & { text: string };
  "message.completed": Message;
  "reasoning.started": Record<string, never>;
  "reasoning.delta": { delta: string; replace?: boolean };
  "reasoning.completed": Record<string, never>;
  "activity.started": Record<string, never>;
  "activity.completed": Record<string, never>;
  "tool.started": ToolCall;
  "tool.completed": ToolResult;
  "subagent.started": Child;
  "subagent.message": Child;
  "subagent.delta": Child;
  "subagent.tool.started": Child & ToolCall;
  "subagent.tool.completed": Child & ToolResult;
  "subagent.result": Child;
  "subagent.failed": Child;
  "subagent.ended": Child;
};
export type Signal = { [T in keyof EventData]: { type: T; data: EventData[T] } }[keyof EventData];
export type AdapterSignal = Exclude<
  Signal,
  { type: `run.${string}` | "input.accepted" | "input.control" }
>;
export type EventContext = Pick<
  RunRequest,
  "clientId" | "runtimeId" | "agentId" | "conversationId" | "requestId" | "replyId"
> & { runId: string; origin?: "prompt" | "push" | "task"; audience?: "agent" | "conversation" };
export type PushEvent = Signal &
  EventContext & {
    version: 1;
    eventId: string;
    seq: number;
    timestamp: number;
    cursor?: number;
  };
export type Adapter = {
  id: string;
  capabilities?: {
    streaming?: boolean;
    tools?: boolean;
    reasoning?: boolean;
    subagents?: boolean;
    cancel?: boolean;
    mentions?: boolean;
    sessionRestore?: boolean;
    steering?: boolean;
    followUp?: boolean;
  };
  run(
    request: RunRequest & { runId: string },
    options: {
      emit: (signal: AdapterSignal) => void;
      signal?: AbortSignal;
      onAccepted?: () => void;
      onDispatch?: () => void;
    },
  ): Promise<void | { status: "deferred"; message: string } | { status: "cancelled"; message?: string }>;
  cancel?(request: RunRequest & { runId: string }): Promise<void | boolean>;
  control?(
    request: RunRequest & { runId: string },
    input: Pick<InputControl, "operation" | "content">,
  ): Promise<{ status: "queued" | "handled" }>;
  close?(): Promise<void>;
  decodePushEvent?(event: unknown): AdapterSignal[];
};


export const PROTOCOL_VERSION = 1;
const isRecord = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);
const isString = (value: unknown) => typeof value === "string";
const isId = (value: unknown): value is string => isString(value) && value.trim().length > 0;
const isTextList = (value: unknown) => Array.isArray(value) && value.every(isString);
const message = { messageId: isId };
const child = { agentId: isId };
const optionalPayloadFields = {
  messageId: isId,
  toolCallId: isId,
  toolName: isId,
  args: isRecord,
  agentId: isId,
  label: isString,
  childSessionId: isId,
  delta: isString,
  text: isString,
  message: isString,
  status: isString,
  isError: (value: unknown) => typeof value === "boolean",
  replace: (value: unknown) => typeof value === "boolean",
};
const payloads: Record<keyof EventData, Record<string, (value: unknown) => boolean>> = {
  "input.accepted": { content: isString },
  "input.control": {
    controlId: isId,
    operation: (value) => typeof value === "string" && ["steer", "follow_up"].includes(value),
    content: isString,
    status: (value) =>
      typeof value === "string" &&
      ["dispatching", "queued", "handled", "rejected", "unknown"].includes(value),
  },
  "input.queue": { steering: isTextList, followUp: isTextList },
  "run.started": {},
  "run.completed": {},
  "run.failed": { message: isString },
  "run.cancelled": {},
  "run.deferred": { message: isString },
  "message.started": message,
  "message.delta": { ...message, delta: isString },
  "message.snapshot": { ...message, text: isString },
  "message.completed": message,
  "reasoning.started": {},
  "reasoning.delta": { delta: isString },
  "reasoning.completed": {},
  "activity.started": {},
  "activity.completed": {},
  "tool.started": { toolCallId: isId, toolName: isId, args: isRecord },
  "tool.completed": { toolCallId: isId },
  "subagent.started": child,
  "subagent.message": child,
  "subagent.delta": child,
  "subagent.tool.started": { ...child, toolCallId: isId, toolName: isId, args: isRecord },
  "subagent.tool.completed": { ...child, toolCallId: isId },
  "subagent.result": child,
  "subagent.failed": child,
  "subagent.ended": child,
};

export function validateSignal(signal: unknown): Signal {
  if (
    !isRecord(signal) ||
    typeof signal.type !== "string" ||
    !Object.hasOwn(payloads, signal.type) ||
    !isRecord(signal.data)
  ) {
    throw new TypeError("Invalid push protocol event");
  }
  for (const [field, validate] of Object.entries(payloads[signal.type as keyof EventData])) {
    if (!validate(signal.data[field])) {
      throw new TypeError(`Invalid ${signal.type} field: ${field}`);
    }
  }
  for (const [field, validate] of Object.entries(optionalPayloadFields)) {
    if (signal.data[field] !== undefined && !validate(signal.data[field])) {
      throw new TypeError(`Invalid ${signal.type} field: ${field}`);
    }
  }
  if (signal.type === "input.queue") {
    for (const field of ["discardedSteering", "discardedFollowUp"]) {
      if (signal.data[field] !== undefined && !isTextList(signal.data[field])) {
        throw new TypeError(`Invalid input.queue field: ${field}`);
      }
    }
    if (
      signal.data.reason !== undefined &&
      signal.data.reason !== "run_settled" &&
      signal.data.reason !== "cancelled"
    ) {
      throw new TypeError("Invalid input.queue field: reason");
    }
  }
  return signal as Signal;
}

export function validateAdapterSignal(value: unknown): AdapterSignal {
  const signal = validateSignal(value);
  if (signal.type.startsWith("run.") || ["input.accepted", "input.control"].includes(signal.type)) {
    throw new TypeError("Run lifecycle belongs to the gateway");
  }
  return signal as AdapterSignal;
}

export function validateEvent(event: unknown): PushEvent {
  if (!isRecord(event) || event.version !== PROTOCOL_VERSION) {
    throw new TypeError("Unsupported push protocol version");
  }
  validateSignal(event);
  for (const field of ["eventId", "runtimeId", "agentId", "conversationId", "runId"]) {
    if (!isId(event[field])) {
      throw new TypeError(`Invalid push protocol field: ${field}`);
    }
  }
  for (const field of ["clientId", "requestId", "replyId"]) {
    if (event[field] !== undefined && !isId(event[field])) {
      throw new TypeError(`Invalid push protocol field: ${field}`);
    }
  }
  if (
    typeof event.seq !== "number" ||
    !Number.isSafeInteger(event.seq) ||
    event.seq < 1 ||
    !Number.isFinite(event.timestamp)
  ) {
    throw new TypeError("Invalid push protocol sequence or timestamp");
  }
  if (
    event.cursor !== undefined &&
    (typeof event.cursor !== "number" || !Number.isSafeInteger(event.cursor) || event.cursor < 1)
  ) {
    throw new TypeError("Invalid push protocol cursor");
  }
  if (
    event.origin !== undefined &&
    event.origin !== "prompt" &&
    event.origin !== "push" &&
    event.origin !== "task"
  ) {
    throw new TypeError("Invalid push protocol origin");
  }
  if (
    event.audience !== undefined &&
    event.audience !== "agent" &&
    event.audience !== "conversation"
  ) {
    throw new TypeError("Invalid push protocol audience");
  }
  if (event.audience === "agent" && event.origin !== "push") {
    throw new TypeError("Agent-wide delivery requires a proactive push");
  }
  return event as PushEvent;
}

export function createEventWriter(context: EventContext, publish: (event: PushEvent) => void) {
  let seq = 0;
  return (signal: Signal): PushEvent => {
    validateSignal(signal);
    const event = validateEvent({
      ...context,
      version: PROTOCOL_VERSION,
      eventId: randomUUID(),
      seq: ++seq,
      timestamp: Date.now(),
      type: signal.type,
      data: signal.data,
    });
    publish(event);
    return event;
  };
}
