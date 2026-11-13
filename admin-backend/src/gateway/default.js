const { PushGateway } = require("./gateway");
const { createOpenClawAdapter } = require("../adapters/openclaw");
const { assertAdapterCredentialIsolation } = require("./auth");

function createDefaultGateway({ publish, adapters, store, auth }) {
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

module.exports = { createDefaultGateway };
