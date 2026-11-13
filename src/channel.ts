import type {
  ChannelGatewayContext,
  ChannelMeta,
  ChannelStatusAdapter,
} from "openclaw/plugin-sdk/channel-contract";
import { createChannelMessageAdapterFromOutbound } from "openclaw/plugin-sdk/channel-message";
import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";
import {
  DEFAULT_PUSH_CHANNEL_ACCOUNT_ID,
  PUSH_CHANNEL_ID,
  deletePushChannelConfig,
  pushChannelConfigSchema,
  resolvePushChannelAccount,
  setPushChannelEnabled,
} from "./config.js";
import { monitorPushChannel } from "./monitor.js";
import { pushChannelOutbound } from "./outbound.js";
import type { ResolvedPushChannelAccount } from "./types.js";

const meta: ChannelMeta = {
  id: PUSH_CHANNEL_ID,
  label: "Push Channel",
  selectionLabel: "Push Channel (Custom)",
  docsPath: "/channels/push-channel",
  docsLabel: "push-channel",
  blurb: "Custom push channel with middleware.",
  order: 99,
};

const pushChannelMessageAdapter = createChannelMessageAdapterFromOutbound({
  id: PUSH_CHANNEL_ID,
  outbound: pushChannelOutbound,
});

type PushChannelProbe = {
  status: "ok" | "not_configured";
  error: string | null;
};

const pushChannelStatus = {
  defaultRuntime: {
    accountId: DEFAULT_PUSH_CHANNEL_ACCOUNT_ID,
    running: false,
    port: null,
  },
  buildChannelSummary: ({ snapshot }) => ({
    status: snapshot.running ? "running" : "idle",
    port: snapshot.port ?? null,
  }),
  probeAccount: async ({ account }) => ({
    status: account.configured ? "ok" : "not_configured",
    error: account.configured ? null : "middlewareUrl not configured",
  }),
  buildAccountSnapshot: (ctx) => ({
    accountId: ctx.account.accountId,
    name: ctx.account.name,
    enabled: ctx.account.enabled,
    configured: ctx.account.configured,
    running: ctx.runtime?.running ?? false,
    port: ctx.runtime?.port ?? ctx.account.config.listenPort,
  }),
} satisfies ChannelStatusAdapter<ResolvedPushChannelAccount, PushChannelProbe>;

export const pushChannelPlugin = {
  id: PUSH_CHANNEL_ID,
  meta,
  capabilities: {
    chatTypes: ["direct"],
    media: false,
    threads: false,
    polls: false,
    reactions: false,
    edit: false,
    reply: false,
  },
  reload: { configPrefixes: [`channels.${PUSH_CHANNEL_ID}`] },
  configSchema: pushChannelConfigSchema,
  config: {
    listAccountIds: (_cfg: OpenClawConfig) => [DEFAULT_PUSH_CHANNEL_ACCOUNT_ID],
    resolveAccount: (cfg: OpenClawConfig, accountId?: string | null) =>
      resolvePushChannelAccount(cfg, accountId ?? DEFAULT_PUSH_CHANNEL_ACCOUNT_ID),
    defaultAccountId: (_cfg: OpenClawConfig) => DEFAULT_PUSH_CHANNEL_ACCOUNT_ID,
    setAccountEnabled: (params: { cfg: OpenClawConfig; accountId: string; enabled: boolean }) =>
      setPushChannelEnabled(params.cfg, params.enabled),
    deleteAccount: (params: { cfg: OpenClawConfig; accountId: string }) =>
      deletePushChannelConfig(params.cfg),
    isConfigured: (acc: ResolvedPushChannelAccount) => acc.configured,
    isEnabled: (acc: ResolvedPushChannelAccount) => acc.enabled,
    describeAccount: (acc: ResolvedPushChannelAccount) => ({
      accountId: acc.accountId,
      name: acc.name,
      enabled: acc.enabled,
      configured: acc.configured,
    }),
  },
  outbound: pushChannelOutbound,
  messaging: {
    targetResolver: {
      looksLikeId: () => true,
    },
  },
  message: pushChannelMessageAdapter,
  gateway: {
    startAccount: async (ctx: ChannelGatewayContext<ResolvedPushChannelAccount>) => {
      return monitorPushChannel({
        config: ctx.cfg,
        runtime: ctx.runtime,
        accountId: ctx.accountId,
        abortSignal: ctx.abortSignal,
        setStatus: ctx.setStatus,
      });
    },
  },
  status: pushChannelStatus,
};
