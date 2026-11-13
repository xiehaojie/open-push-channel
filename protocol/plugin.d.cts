import type { Adapter, AdapterSignal, RunRequest } from "./index.cjs";

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
export function fields(value: unknown, allowed: readonly string[]): Record<string, unknown>;
export function text(value: unknown): string;
export function capabilities(value: unknown): PluginCapabilities;
export function request(value: RunRequest & { runId: string }): PluginRequest;
export const requestFields: readonly string[];
export function signal(value: unknown): AdapterSignal;
export function notification(
  target: unknown,
  payload: unknown,
): {
  target: NotificationTarget;
  payload: NotificationPayload;
};
