import type { Adapter } from "../protocol/index.cjs";
import type {
  NotificationPayload,
  NotificationReceipt,
  NotificationTarget,
  PluginCapabilities,
  PluginRequest,
} from "../protocol/plugin.cjs";

export function connectPlugin(options: {
  url: string;
  runtimeId: string;
  agentId: string;
  token: string;
  capabilities: Partial<PluginCapabilities>;
  signal?: AbortSignal;
  run(request: PluginRequest, options: Parameters<Adapter["run"]>[1]): ReturnType<Adapter["run"]>;
  control?(
    request: PluginRequest,
    input: Parameters<NonNullable<Adapter["control"]>>[1],
  ): ReturnType<NonNullable<Adapter["control"]>>;
  cancel?(request: PluginRequest): Promise<void>;
}): Promise<{
  close(): Promise<void>;
  notify(target: NotificationTarget, payload: NotificationPayload): Promise<NotificationReceipt>;
}>;
