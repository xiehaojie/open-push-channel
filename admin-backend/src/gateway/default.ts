import { PushGateway } from "./gateway.ts";
import { createOpenClawAdapter } from "../adapters/openclaw.ts";
import { assertAdapterCredentialIsolation } from "./auth.ts";

import type { GatewayAuth } from "./types.ts";
import type { GatewayOptions } from "./gateway.ts";

type DefaultGatewayOptions = Omit<GatewayOptions, "adapters"> &
  Partial<Pick<GatewayOptions, "adapters">> & { auth?: GatewayAuth };

function createDefaultGateway({ publish, adapters, store, auth }: DefaultGatewayOptions) {
  const selectedAdapters = adapters
    ? [...adapters]
    : [
        createOpenClawAdapter({
          url: process.env.OPENCLAW_WEBHOOK_URL || "http://127.0.0.1:3002/webhook",
          webhookToken: process.env.PUSH_CHANNEL_WEBHOOK_TOKEN,
        }),
      ];
  assertAdapterCredentialIsolation(selectedAdapters, auth);
  return new PushGateway({
    publish,
    store,
    adapters: selectedAdapters,
  });
}

export { createDefaultGateway };
