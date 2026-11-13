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
  ): Promise<void | { status: "deferred"; message: string } | { status: "cancelled" }>;
  cancel?(request: RunRequest): Promise<void>;
  control?(
    request: RunRequest & { runId: string },
    input: Pick<InputControl, "operation" | "content">,
  ): Promise<{ status: "queued" | "handled" }>;
  close?(): Promise<void>;
  decodePushEvent?(event: unknown): AdapterSignal[];
};
export const PROTOCOL_VERSION: 1;
export function validateSignal(signal: unknown): Signal;
export function validateAdapterSignal(signal: unknown): AdapterSignal;
export function validateEvent(event: unknown): PushEvent;
export function createEventWriter(
  context: EventContext,
  publish: (event: PushEvent) => void,
): (signal: Signal) => PushEvent;
