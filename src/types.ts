export type PushChannelConfig = {
  enabled: boolean;
  middlewareUrl?: string;
  listenHost: string;
  listenPort: number;
  listenPath: string;
};

export type PushChannelMention = {
  agentId: string;
  label?: string;
};

export type ResolvedPushChannelAccount = {
  accountId: string;
  enabled: boolean;
  configured: boolean;
  name?: string;
  config: PushChannelConfig;
};
