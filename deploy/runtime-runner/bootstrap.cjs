const { spawn } = require("node:child_process");
const { constants, copyFileSync, existsSync, mkdirSync } = require("node:fs");
const path = require("node:path");
const {
  assertNativeHome,
  loadRuntimeConfig,
} = require("../../admin-backend/src/gateway/runtime-config");

function prepare(configPath) {
  const options = loadRuntimeConfig(configPath);
  if (!options.auth) {
    throw new Error("Network runtime runner requires gateway auth");
  }
  const directory = path.dirname(path.resolve(configPath));
  const files = options.nativeHomes.map(({ kind, home, cwd }) => ({
    home,
    cwd,
    source: path.join(directory, kind === "dsh" ? "dsh-profile.yml" : "pi-models.json"),
    target:
      kind === "dsh"
        ? path.join(home, ".dsh", "profiles", "sdk", "cordis.patch.yml")
        : path.join(home, ".pi", "agent", "models.json"),
  }));
  // Check every required template before creating any persistent directories.
  // Existing agent config is owner state: restart must never overwrite it.
  for (const file of files) {
    assertNativeHome(file);
    const { source, target } = file;
    if (!existsSync(target) && !existsSync(source)) {
      throw new Error(`Missing native template: ${path.basename(source)}`);
    }
  }
  for (const { source, target, home, cwd } of files) {
    mkdirSync(home, { recursive: true, mode: 0o700 });
    mkdirSync(cwd, { recursive: true, mode: 0o700 });
    if (existsSync(target)) {
      continue;
    }
    mkdirSync(path.dirname(target), { recursive: true, mode: 0o700 });
    copyFileSync(source, target, constants.COPYFILE_EXCL);
  }
}

function main(args) {
  let configPath = "/etc/push-gateway/gateway.json";
  let prepareOnly = false;
  for (let index = 0; index < args.length; index++) {
    if (args[index] === "--config") {
      const filename = args[++index];
      if (!filename || filename.startsWith("-")) {
        throw new Error("--config requires a path");
      }
      configPath = filename;
    } else if (args[index] === "--prepare-only") {
      prepareOnly = true;
    } else {
      throw new Error("Unknown runtime runner argument");
    }
  }
  prepare(configPath);
  if (prepareOnly) {
    return;
  }
  const serverScript = path.resolve(__dirname, "../../admin-backend/src/gateway/server.js");
  const child = spawn(process.execPath, [serverScript, "--config", configPath], {
    stdio: "inherit",
    shell: false,
  });
  // Kubernetes signals PID 1; forward shutdown to the server so its queue and
  // native children finish cleanup before the container exits.
  for (const signal of ["SIGTERM", "SIGINT"]) {
    process.on(signal, () => child.kill(signal));
  }
  child.on("error", () => {
    console.error("Failed to start push gateway server");
    process.exitCode = 1;
  });
  child.on("exit", (code) => {
    process.exitCode = code ?? 1;
  });
}

if (require.main === module) {
  try {
    main(process.argv.slice(2));
  } catch (error) {
    // Config files contain secret references: report the validation message only.
    console.error(error instanceof SyntaxError ? "Invalid gateway JSON config" : error.message);
    process.exitCode = 1;
  }
}

module.exports = { prepare };
