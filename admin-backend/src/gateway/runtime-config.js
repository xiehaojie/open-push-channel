const { lstatSync, mkdirSync, readFileSync } = require("node:fs");
const path = require("node:path");
const { createOpenClawAdapter } = require("../adapters/openclaw");
const { createPiAdapter } = require("../adapters/pi");
const { createDeepSeekAdapter } = require("../adapters/deepseek");
const { createPluginAdapter } = require("../adapters/plugin");
const { capabilities: pluginCapabilities } = require("../../../protocol/plugin.cjs");
const { assertAdapterCredentialIsolation } = require("./auth");

const record = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
const homeEnvKeys = new Set([
  "HOME",
  "USERPROFILE",
  "DSH_HOME",
  "DSH_AGENTS_HOME",
  "PI_CODING_AGENT_DIR",
  "PI_CODING_AGENT_SESSION_DIR",
  "XDG_CONFIG_HOME",
  "XDG_CACHE_HOME",
  "XDG_DATA_HOME",
  "XDG_STATE_HOME",
]);
const reservedNativeArguments = {
  pi: new Set([
    "--session-dir",
    "--session",
    "--session-id",
    "--no-session",
    "--continue",
    "-c",
    "--resume",
    "-r",
    "--fork",
    "--mode",
    "--help",
    "-h",
    "--version",
    "-v",
    "--print",
    "-p",
    "--list-models",
    "--export",
  ]),
  "deepseek-harness": new Set([
    "--profile",
    "--patch",
    "--from-default-profile",
    "--dump-config",
    "--dump-default-config",
    "--dump-config-schema",
    "--help",
    "-h",
    "--version",
    "-V",
  ]),
};

function assertNativeArguments(type, args = []) {
  // Session identity/storage and RPC lifecycle belong to the gateway. Native
  // selectors, one-shot commands and '--' would override or hide appended args.
  const positionalCommand = args.length > 0 && !args[0].startsWith("-");
  const reserved = args.some(
    (arg) => arg === "--" || reservedNativeArguments[type].has(arg.split("=")[0]),
  );
  if (positionalCommand || reserved) {
    throw new Error("Native arguments cannot override gateway-owned session, storage or lifecycle");
  }
}
function keys(value, allowed, label) {
  if (!record(value) || Object.keys(value).some((key) => !allowed.includes(key))) {
    throw new Error(`Unknown or invalid ${label} field`);
  }
}
function text(value, label) {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`Invalid ${label}`);
  }
  return value;
}

function assertNativeHome({ home, cwd }) {
  const ownedRoot = path.dirname(path.dirname(home));
  const managedPaths = [
    home,
    cwd,
    path.join(home, ".agents"),
    path.join(home, ".dsh", "profiles", "sdk", "cordis.patch.yml"),
    path.join(home, ".pi", "agent", "models.json"),
    path.join(home, ".pi", "agent", "sessions"),
    path.join(home, ".config"),
    path.join(home, ".cache"),
    path.join(home, ".local", "share"),
    path.join(home, ".local", "state"),
  ];
  const checked = new Set();
  for (const target of managedPaths) {
    let current = ownedRoot;
    for (const segment of ["", ...path.relative(ownedRoot, target).split(path.sep)]) {
      current = path.join(current, segment);
      if (checked.has(current)) {
        continue;
      }
      checked.add(current);
      // Reject aliasing only at managed roots/configs, not arbitrary project
      // content. System ancestors such as macOS /var remain outside this check.
      if (lstatSync(current, { throwIfNoEntry: false })?.isSymbolicLink()) {
        throw new Error("Managed native paths must not be a symbolic link");
      }
    }
  }
}

function createRuntimeOptions(config, directory = process.cwd()) {
  if (!record(config) || !Array.isArray(config.runtimes) || !config.runtimes.length) {
    throw new Error("Configure at least one runtime");
  }
  keys(config, ["runtimes", "dataRoot", "databasePath", "auth"], "gateway config");
  const dataRoot =
    config.dataRoot === undefined
      ? undefined
      : path.resolve(directory, text(config.dataRoot, "dataRoot"));
  const openclawRuntimes = config.runtimes.filter((runtime) => runtime?.type === "openclaw");
  const defaultWebhookToken = process.env.PUSH_CHANNEL_WEBHOOK_TOKEN;
  const excludedEnvKeys = [
    "PUSH_CHANNEL_WEBHOOK_TOKEN",
    ...openclawRuntimes.map((runtime) => runtime.webhookTokenEnv),
    config.auth?.pushTokenEnv,
    ...(Array.isArray(config.auth?.clients)
      ? config.auth.clients.map((client) => client?.tokenEnv)
      : []),
    ...config.runtimes
      .filter((runtime) => runtime?.type === "plugin")
      .map((runtime) => runtime.tokenEnv),
  ].filter((key) => typeof key === "string");
  const hasNativeRuntime = config.runtimes.some((runtime) =>
    ["pi", "deepseek-harness"].includes(runtime?.type),
  );
  // Native adapters remove auth keys after applying their own environment.
  // Reject collisions so credential filtering cannot erase the isolated homes.
  if (hasNativeRuntime && excludedEnvKeys.some((key) => homeEnvKeys.has(key.toUpperCase()))) {
    throw new Error("Auth tokens cannot use managed home environment variables");
  }
  const ids = new Set();
  const owners = new Set();
  const nativeHomes = [];
  const adapters = config.runtimes.map((runtime) => {
    if (!record(runtime)) {
      throw new Error("Invalid runtime config");
    }
    const id = text(runtime.id, "runtime id");
    if (ids.has(id)) {
      throw new Error("Duplicate runtime id");
    }
    ids.add(id);
    if (runtime.type === "plugin") {
      keys(runtime, ["id", "type", "agents", "tokenEnv", "capabilities"], "plugin runtime");
      if (
        !Array.isArray(runtime.agents) ||
        !runtime.agents.length ||
        runtime.agents.some((agent) => typeof agent !== "string" || !agent.trim()) ||
        new Set(runtime.agents).size !== runtime.agents.length
      ) {
        throw new Error("Plugin runtime needs distinct explicit agents");
      }
      const token = text(
        process.env[text(runtime.tokenEnv, "plugin token environment name")],
        "plugin token environment value",
      );
      return createPluginAdapter({
        id,
        agents: [...runtime.agents],
        token,
        capabilities: pluginCapabilities(runtime.capabilities),
      });
    }
    if (runtime.type === "openclaw") {
      keys(runtime, ["id", "type", "url", "webhookTokenEnv"], "OpenClaw runtime");
      const url = new URL(text(runtime.url, "OpenClaw url"));
      if (!["http:", "https:"].includes(url.protocol)) {
        throw new Error("OpenClaw url must use HTTP(S)");
      }
      if (
        openclawRuntimes.length > 1 &&
        defaultWebhookToken &&
        runtime.webhookTokenEnv === undefined
      ) {
        throw new Error(
          "Multiple OpenClaw runtimes require explicit webhookTokenEnv when the global token is set",
        );
      }
      const webhookToken =
        runtime.webhookTokenEnv === undefined
          ? defaultWebhookToken
          : text(
              process.env[text(runtime.webhookTokenEnv, "OpenClaw webhook token environment name")],
              "OpenClaw webhook token environment value",
            );
      return createOpenClawAdapter({ id, url: url.href, webhookToken });
    }
    if (!["pi", "deepseek-harness"].includes(runtime.type)) {
      throw new Error("Unknown runtime type");
    }
    keys(
      runtime,
      [
        "id",
        "type",
        "command",
        "args",
        "env",
        "agents",
        ...(runtime.type === "deepseek-harness" ? ["provider", "model"] : []),
      ],
      "native runtime",
    );
    if (!dataRoot) {
      throw new Error("Native runtimes require dataRoot");
    }
    if (
      !Array.isArray(runtime.agents) ||
      !runtime.agents.length ||
      runtime.agents.some(
        (agentId) => typeof agentId !== "string" || !/^[a-z0-9][a-z0-9_-]{0,127}$/.test(agentId),
      )
    ) {
      throw new Error("Native runtime agents must be explicit safe path segments");
    }
    for (const agentId of runtime.agents) {
      const owner = JSON.stringify([runtime.type, agentId]);
      if (owners.has(owner)) {
        throw new Error("Duplicate native agent workspace owner");
      }
      owners.add(owner);
    }
    if (
      runtime.args !== undefined &&
      (!Array.isArray(runtime.args) || runtime.args.some((arg) => typeof arg !== "string"))
    ) {
      throw new Error("Runtime args must be a string array");
    }
    assertNativeArguments(runtime.type, runtime.args);
    if (
      runtime.env !== undefined &&
      (!record(runtime.env) ||
        Object.values(runtime.env).some((value) => typeof value !== "string"))
    ) {
      throw new Error("Runtime env must contain strings");
    }
    if (Object.keys(runtime.env ?? {}).some((key) => homeEnvKeys.has(key.toUpperCase()))) {
      throw new Error("Native home environment is owned by the gateway");
    }
    const command =
      runtime.command === undefined ? undefined : text(runtime.command, "runtime command");
    // spawn resolves relative executables against its per-agent cwd. Bind path-like
    // commands to the config directory first; bare names must still use PATH.
    const relativeCommand =
      command !== undefined && !path.isAbsolute(command) && /[\\/]/.test(command);
    const options = {
      id,
      command: relativeCommand ? path.resolve(directory, command) : command,
      args: runtime.args,
      provider: runtime.provider === undefined ? undefined : text(runtime.provider, "provider"),
      model: runtime.model === undefined ? undefined : text(runtime.model, "model"),
      excludedEnvKeys,
    };
    const createAdapter = runtime.type === "pi" ? createPiAdapter : createDeepSeekAdapter;
    const agents = new Map(
      runtime.agents.map((agentId) => {
        const kind = runtime.type === "pi" ? "pi" : "dsh";
        const root = path.join(dataRoot, kind, agentId);
        const home = path.join(root, "home");
        const cwd = path.join(root, "workspace");
        const env = {
          ...runtime.env,
          HOME: home,
          USERPROFILE: home,
          DSH_HOME: path.join(home, ".dsh"),
          DSH_AGENTS_HOME: path.join(home, ".agents"),
          PI_CODING_AGENT_DIR: path.join(home, ".pi", "agent"),
          PI_CODING_AGENT_SESSION_DIR: path.join(home, ".pi", "agent", "sessions"),
          XDG_CONFIG_HOME: path.join(home, ".config"),
          XDG_CACHE_HOME: path.join(home, ".cache"),
          XDG_DATA_HOME: path.join(home, ".local", "share"),
          XDG_STATE_HOME: path.join(home, ".local", "state"),
        };
        nativeHomes.push({ kind, home, cwd });
        return [
          agentId,
          { home, cwd, adapter: createAdapter({ ...options, cwd, env }), prepared: false },
        ];
      }),
    );
    const assertAgent = (agentId) => {
      if (!agents.has(agentId)) {
        throw new Error("Agent is not declared for this runtime");
      }
    };
    let closed = false;
    return {
      id,
      agents: [...agents.keys()],
      capabilities: agents.values().next().value.adapter.capabilities,
      assertAgent,
      async run(request, context) {
        assertAgent(request.agentId);
        if (closed) {
          throw new Error("Runtime is closed");
        }
        const agent = agents.get(request.agentId);
        if (!agent.prepared) {
          // Per-agent homes protect native credentials/history from ambient process
          // defaults. They are stable across sessions; directories are not a sandbox.
          assertNativeHome(agent);
          mkdirSync(agent.home, { recursive: true, mode: 0o700 });
          mkdirSync(agent.cwd, { recursive: true, mode: 0o700 });
          agent.prepared = true;
        }
        return agent.adapter.run(request, context);
      },
      cancel(request) {
        assertAgent(request.agentId);
        return agents.get(request.agentId).adapter.cancel?.(request);
      },
      control(request, input) {
        // Controls must follow the same agent owner as run(); otherwise the
        // advertised native capability is unreachable or targets another home.
        assertAgent(request.agentId);
        if (closed) {
          throw new Error("Runtime is closed");
        }
        const adapter = agents.get(request.agentId).adapter;
        if (!adapter.control) {
          throw new Error("Runtime does not support this input control");
        }
        return adapter.control(request, input);
      },
      async close() {
        closed = true;
        await Promise.all([...agents.values()].map(({ adapter }) => adapter.close()));
      },
    };
  });
  let auth;
  if (config.auth !== undefined) {
    if (
      !record(config.auth) ||
      !Array.isArray(config.auth.clients) ||
      !config.auth.clients.length
    ) {
      throw new Error("Auth needs at least one client");
    }
    keys(config.auth, ["clients", "pushTokenEnv"], "auth config");
    const clientIds = new Set();
    const clients = config.auth.clients.map((client) => {
      if (!record(client) || !Array.isArray(client.targets) || !client.targets.length) {
        throw new Error("Auth client needs explicit targets");
      }
      keys(client, ["id", "tokenEnv", "targets"], "auth client");
      const id = text(client.id, "client id");
      const token = text(
        process.env[text(client.tokenEnv, "client token environment name")],
        "client token environment value",
      );
      if (clientIds.has(id)) {
        throw new Error("Auth clients must have distinct ids and tokens");
      }
      clientIds.add(id);
      const targets = client.targets.map((target) => {
        if (!record(target) || !ids.has(target.runtimeId)) {
          throw new Error("Auth target runtime is not configured");
        }
        keys(target, ["runtimeId", "agentId"], "auth target");
        adapters.find((adapter) => adapter.id === target.runtimeId).assertAgent?.(target.agentId);
        return { runtimeId: target.runtimeId, agentId: text(target.agentId, "target agent id") };
      });
      return { id, token, targets };
    });
    const pushToken = text(
      process.env[text(config.auth.pushTokenEnv, "push token environment name")],
      "push token environment value",
    );
    auth = { clients, pushToken };
  }
  assertAdapterCredentialIsolation(adapters, auth);
  return {
    adapters,
    nativeHomes,
    databasePath: path.resolve(
      directory,
      config.databasePath === undefined
        ? "state/push-gateway.sqlite"
        : text(config.databasePath, "databasePath"),
    ),
    ...(auth ? { auth } : {}),
  };
}

function loadRuntimeConfig(filename) {
  if (!filename) {
    return createRuntimeOptions({
      runtimes: [
        {
          id: "openclaw",
          type: "openclaw",
          url: process.env.OPENCLAW_WEBHOOK_URL || "http://127.0.0.1:3002/webhook",
        },
      ],
    });
  }
  const absolute = path.resolve(filename);
  return createRuntimeOptions(JSON.parse(readFileSync(absolute, "utf8")), path.dirname(absolute));
}

module.exports = { assertNativeHome, createRuntimeOptions, loadRuntimeConfig };
