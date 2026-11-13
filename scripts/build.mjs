import { readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

const root = new URL("../", import.meta.url);
const sources = await readdir(new URL("src/", root), { recursive: true });
const entryPoints = [
  "index.ts",
  ...sources
    .filter((file) => file.endsWith(".ts") && !file.endsWith(".test.ts") && !file.endsWith(".d.ts"))
    .sort()
    .map((file) => `src/${file}`),
];

// Preserve the relative paths used by OpenClaw's lazy channel/runtime entries.
await build({
  absWorkingDir: fileURLToPath(root),
  entryPoints,
  outbase: ".",
  outdir: "dist",
  platform: "node",
  format: "esm",
  target: "node22",
});
