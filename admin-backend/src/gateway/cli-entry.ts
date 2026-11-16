import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

export function isMain(meta: ImportMeta): boolean {
  const entry = process.argv[1];
  if (!entry) return false;
  try {
    return realpathSync(entry) === realpathSync(fileURLToPath(meta.url));
  } catch {
    return false;
  }
}
