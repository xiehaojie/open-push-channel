import { buildJsonChannelConfigSchema } from "openclaw/plugin-sdk/channel-config-schema";
import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";
import type { PushChannelConfig, ResolvedPushChannelAccount } from "./types.js";

export const PUSH_CHANNEL_ID = "push-channel";
export const DEFAULT_PUSH_CHANNEL_ACCOUNT_ID = "default";
export const DEFAULT_PUSH_CHANNEL_LISTEN_HOST = "127.0.0.1";
export const DEFAULT_PUSH_CHANNEL_LISTEN_PORT = 3002;
export const DEFAULT_PUSH_CHANNEL_LISTEN_PATH = "/webhook";

const pushChannelConfigJsonSchema = {
  type: "object",
  additionalProperties: false,
  properties: {
    enabled: { type: "boolean" },
    middlewareUrl: { type: "string" },
    listenHost: { type: "string" },
    listenPort: { type: "integer", minimum: 1, maximum: 65535 },
    listenPath: { type: "string" },
  },
} as const;

export const pushChannelConfigSchema = buildJsonChannelConfigSchema(pushChannelConfigJsonSchema, {
  cacheKey: "push-channel:channel-config",
  uiHints: {
    middlewareUrl: {
      label: "Middleware URL",
      placeholder: "http://localhost:3001",
    },
    listenHost: {
      label: "Listen host",
      placeholder: DEFAULT_PUSH_CHANNEL_LISTEN_HOST,
      help: "Non-loopback listeners require PUSH_CHANNEL_WEBHOOK_TOKEN.",
    },
    listenPort: {
      label: "Listen port",
      placeholder: String(DEFAULT_PUSH_CHANNEL_LISTEN_PORT),
    },
    listenPath: {
      label: "Listen path",
      placeholder: DEFAULT_PUSH_CHANNEL_LISTEN_PATH,
    },
  },
});

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function readOptionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function readOptionalPort(value: unknown): number | undefined {
  return typeof value === "number" && Number.isInteger(value) && value > 0 && value <= 65535
    ? value
    : undefined;
}

function readPushChannelRawConfig(cfg: OpenClawConfig): Record<string, unknown> {
  const channels = isRecord(cfg.channels) ? cfg.channels : {};
  const raw = channels[PUSH_CHANNEL_ID];
  return isRecord(raw) ? raw : {};
}

export function resolvePushChannelConfig(cfg: OpenClawConfig): PushChannelConfig {
  const raw = readPushChannelRawConfig(cfg);
  return {
    enabled: raw.enabled === true,
    middlewareUrl: readOptionalString(raw.middlewareUrl),
    listenHost: readOptionalString(raw.listenHost) ?? DEFAULT_PUSH_CHANNEL_LISTEN_HOST,
    listenPort: readOptionalPort(raw.listenPort) ?? DEFAULT_PUSH_CHANNEL_LISTEN_PORT,
    listenPath: readOptionalString(raw.listenPath) ?? DEFAULT_PUSH_CHANNEL_LISTEN_PATH,
  };
}

export function resolvePushChannelAccount(
  cfg: OpenClawConfig,
  accountId = DEFAULT_PUSH_CHANNEL_ACCOUNT_ID,
): ResolvedPushChannelAccount {
  const config = resolvePushChannelConfig(cfg);
  return {
    accountId,
    enabled: config.enabled,
    configured: Boolean(config.middlewareUrl),
    name: "Push Channel",
    config,
  };
}

export function setPushChannelEnabled(cfg: OpenClawConfig, enabled: boolean): OpenClawConfig {
  const channels = isRecord(cfg.channels) ? cfg.channels : {};
  const existing = readPushChannelRawConfig(cfg);
  return {
    ...cfg,
    channels: {
      ...channels,
      [PUSH_CHANNEL_ID]: {
        ...existing,
        enabled,
      },
    },
  } as OpenClawConfig;
}

export function deletePushChannelConfig(cfg: OpenClawConfig): OpenClawConfig {
  const channels = isRecord(cfg.channels) ? { ...cfg.channels } : {};
  delete channels[PUSH_CHANNEL_ID];
  return {
    ...cfg,
    channels,
  } as OpenClawConfig;
}
