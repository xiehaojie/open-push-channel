import { copyFile, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { stripTypeScriptTypes } from "node:module";
import { fileURLToPath } from "node:url";

const output = new URL("dist/", import.meta.url);
const pageFiles = ["index.html", "style.css"];
const scripts = ["sdk", "mentions", "demo"];

await mkdir(output, { recursive: true });
const staging = await mkdtemp(fileURLToPath(new URL(".build-", output)));
const staged = (name: string) => `${staging}/${name}`;
try {
  await Promise.all([
    ...pageFiles.map((name) => copyFile(new URL(name, import.meta.url), staged(name))),
    ...scripts.map(async (name) => {
      const source = await readFile(new URL(`${name}.ts`, import.meta.url), "utf8");
      await writeFile(staged(`${name}.js`), stripTypeScriptTypes(source));
    }),
  ]);
  // Tests and running gateways can read assets while another process builds.
  for (const name of [...scripts.map((script) => `${script}.js`), ...pageFiles]) {
    await rename(staged(name), new URL(name, output));
  }
} finally {
  await rm(staging, { recursive: true, force: true });
}
