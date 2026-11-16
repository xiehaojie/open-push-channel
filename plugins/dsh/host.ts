// The subset of DeepSeek Harness 0.2.1-rc.2 services used by this plugin.
// Keep host types structural so the standalone gateway never installs dsh.
export type ContentBlock = { type: string; text?: unknown };
type HostMessage = { id: string; content: readonly ContentBlock[] };
type UserMessage = HostMessage & { role: "user"; source: { kind: "user" } };

export type HostSession = { id: string; seq: number };
export type HostAgent = {
  id: string;
  session: HostSession;
  status: string;
  inbox: {
    nextStep: readonly HostMessage[];
    nextTurn: readonly HostMessage[];
    remove(id: string): unknown;
  };
  steer(message: UserMessage): unknown;
  followup(message: UserMessage): unknown;
  cancel(reason: { kind: "disposed" } | { kind: "hook"; reason: string }, options?: { keepInbox: boolean }): unknown;
  whenIdle(): Promise<unknown>;
};

export type AgentHandle = { agent: HostAgent; dispose(): Promise<void> };
type AgentOptions = { provider: string; model: string };
type OpenOptions = { agentOptions: AgentOptions; signal: AbortSignal };

export type Settlement = {
  seq: number;
  type: "assistant/attempt" | "assistant/message";
  data: { turn: number; step: number; message?: { content?: readonly ContentBlock[] } };
};
export type Terminal = { kind: string; error?: { message?: string } };
type SessionEvent =
  | Settlement
  | { type: "turn/end"; seq: number; data: { reason: Terminal } }
  | { type: "tool/call"; seq: number; data: { arguments: string; callId: string; name: string } }
  | {
      type: "tool/result";
      seq: number;
      data: { message: { toolCallId: string; content: readonly ContentBlock[]; isError?: boolean } };
    };

export type StreamFrame = { revision: number; attemptId: string } & (
  | { type: "start"; turn: number; step: number }
  | { type: "chunk"; index: number; time: number; chunk: { type: string; text?: unknown } }
  | {
      type: "end";
      index: number;
      outcome:
        | { kind: "committed"; seq: number; eventType: Settlement["type"] }
        | { kind: "abandoned" };
    }
);
export type Activation = { id: string; runId: string; provider: string; local?: boolean };
type ActivationEnd = Activation & {
  lastAssistantMessage?: readonly ContentBlock[];
  stopReason: string;
};

export type HostEvents = {
  "agent/assistant-stream": [event: { agent: HostAgent; frame: StreamFrame }];
  "agent/error": [event: { agent: HostAgent; error: unknown }];
  "agent/inbox/inserted": [event: { agent: HostAgent }];
  "agent/inbox/claimed": [event: { agent: HostAgent }];
  "agent/inbox/discarded": [event: { agent: HostAgent }];
  "session/event": [session: HostSession, event: SessionEvent];
  "subagent/start": [activation: Activation];
  "subagent/end": [activation: ActivationEnd];
};

export type HostContext = {
  agents: {
    get(id: string): HostAgent | undefined;
    isOwnedBy(id: string, owner: HostAgent): boolean;
    create(options: OpenOptions & { sessionId: string; meta: { cwd: string } }): Promise<AgentHandle>;
    resume(options: OpenOptions & { resumeSessionId: string }): Promise<AgentHandle>;
  };
  subagents: {
    drainDescendants(agents: readonly HostAgent[]): Promise<void>;
    waitForChildren(agent: HostAgent): Promise<boolean>;
  };
  loader: { await(): Promise<unknown> };
  on<K extends keyof HostEvents>(event: K, handler: (...args: HostEvents[K]) => void): () => void;
  effect(create: () => () => Promise<void>, label: string): unknown;
  provide(name: string, service: object): unknown;
  logger: { warn(message: string): unknown };
};
