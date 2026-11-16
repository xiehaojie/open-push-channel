import type {
  Adapter,
  ControlReceipt,
  EventContext,
  InputQueue,
  PushEvent,
} from "../protocol/index.ts";

export type Capabilities = NonNullable<Adapter["capabilities"]>;
export type SessionTarget = Pick<EventContext, "runtimeId" | "agentId">;

// The default browser protocol projects canonical events into flat presentation frames.
export type Presentation = {
  sessionId?: string;
  queryMessageId?: string;
  answerMessageId?: string;
  runtimeId?: string;
  runId?: string;
  origin?: EventContext["origin"];
};

type Delivery = Presentation & {
  version?: never;
  conversationId?: never;
  audience?: EventContext["audience"];
  eventId?: string;
  deliveryId?: string;
  cursor?: number;
};

export type ChildPresentation = Presentation & {
  childSessionKey?: string;
  agentId?: string;
  label?: string;
  messageId?: string;
};

export type ToolPresentation = {
  toolCallId?: string;
  toolName?: string;
  args?: Record<string, unknown>;
  content?: unknown;
  isError?: boolean;
};

export type ReceiptDetails = Partial<ControlReceipt> & { sessionId?: string; runId?: string };
type Empty = Record<never, never>;
type LegacyPayloads = {
  pong: Empty;
  registered: {
    agentId: string;
    files?: string[];
    path?: string;
    protocolVersion?: number;
    runtimes?: { id: string; capabilities: Capabilities; agents?: string[] }[];
  };
  "session.started": SessionTarget & { sessionId: string; capabilities: Capabilities };
  run_cancelled: Empty;
  run_started: Empty;
  run_completed: Empty;
  input_control: ReceiptDetails;
  input_queue: InputQueue & { runId: string };
  message_sent: { content?: string; role?: string };
  stream_start: { from?: string };
  stream: { content?: string; role?: string };
  stream_snapshot: { snapshot?: string; role?: string };
  stream_end: Empty;
  reasoning_start: Empty;
  reasoning_delta: { delta?: string; replace?: boolean };
  reasoning_end: Empty;
  tool_call: ToolPresentation;
  tool_result: ToolPresentation;
  tool_start: Empty;
  tool_end: Empty;
  subagent_start: ChildPresentation;
  subagent_message: ChildPresentation & { content?: unknown; replace?: boolean };
  subagent_stream: ChildPresentation & { content?: unknown; delta?: string };
  subagent_tool_call: ChildPresentation & ToolPresentation;
  subagent_tool_result: ChildPresentation & ToolPresentation;
  subagent_result: ChildPresentation & { content?: unknown };
  subagent_error: ChildPresentation & { message?: string };
  subagent_end: ChildPresentation & { status?: string };
  timeout_deferred: { message?: string };
  message: { content?: string };
  error: { message?: string };
  done: Empty;
};

export type LegacyFrame<T extends keyof LegacyPayloads> = Delivery & {
  type: T;
} & LegacyPayloads[T];
type LegacyMessage = { [T in keyof LegacyPayloads]: LegacyFrame<T> }[keyof LegacyPayloads];
export type CanonicalFrame<T extends PushEvent["type"]> = Extract<PushEvent, { type: T }>;
export type GatewayMessage =
  | LegacyMessage
  | (PushEvent & { sessionId?: never; deliveryId?: never });
