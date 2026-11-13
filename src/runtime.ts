import { createPluginRuntimeStore, type PluginRuntime } from "openclaw/plugin-sdk/runtime-store";

const { setRuntime: setPushChannelRuntime, getRuntime: getPushChannelRuntime } =
  createPluginRuntimeStore<PluginRuntime>({
    pluginId: "push-channel",
    errorMessage: "PushChannel runtime not initialized",
  });

export { getPushChannelRuntime, setPushChannelRuntime };
