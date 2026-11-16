import type { Adapter, EventContext, RunRequest } from "../../../protocol/index.ts";
import type { PluginAdapter } from "../adapters/plugin.ts";

export type RuntimeTarget = Pick<RunRequest, "runtimeId" | "agentId">;
export type GatewayPrincipal = { id: string; token: string; targets: RuntimeTarget[] };
export type GatewayAuth = { clients: GatewayPrincipal[]; pushToken?: string };

export type GatewayAdapter = Omit<Adapter, "cancel"> &
  Partial<Pick<PluginAdapter, "attachPlugin">> & {
    cancel?(
      request: RunRequest & { runId: string },
    ): ReturnType<NonNullable<Adapter["cancel"]>> | undefined;
    agents?: string[];
    assertAgent?(agentId: string): void;
  };

export type SessionAddress = { clientId: string; conversationId: string };
export type GatewaySession = SessionAddress &
  RuntimeTarget & { nativeSessionId: string; nativeStarted: boolean };

export type GatewayContext = Omit<EventContext, "runId">;
export type PushPayload = { eventId?: string; content?: unknown; signal?: unknown; event?: unknown };
export type JournalRequest = GatewayContext &
  SessionAddress &
  Omit<PushPayload, "eventId"> &
  Pick<RunRequest, "mentions">;
