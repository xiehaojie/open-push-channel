const assert = require("node:assert/strict");
const { execFile } = require("node:child_process");
const { join } = require("node:path");
const test = require("node:test");
const { promisify } = require("node:util");

const execute = promisify(execFile);
const script = join(__dirname, "benchmark-push.ts");

void test("push benchmark measures deliveries through the authenticated gateway", { timeout: 45000 }, async () => {
  const { stdout } = await execute(process.execPath, [script, "5", "3", "10"], {
    timeout: 30000,
    killSignal: "SIGKILL",
  });
  const result = JSON.parse(stdout);
  assert.equal(result.node, process.version);
  assert.equal(result.platform, process.platform);
  assert.equal(result.arch, process.arch);
  assert.equal(result.clients, 5);
  assert.equal(result.rounds, 3);
  assert.equal(result.historicalSessions, 10);
  assert.equal(result.deliveries, 15);
  assert.ok(Number.isFinite(result.deliveriesPerSecond) && result.deliveriesPerSecond > 0);
  const percentiles = [result.deliveryMs.p50, result.deliveryMs.p95, result.deliveryMs.p99];
  assert.ok(percentiles.every((value) => Number.isFinite(value) && value >= 0));
  assert.ok(percentiles[0] <= percentiles[1] && percentiles[1] <= percentiles[2]);
});

void test("push benchmark refuses invalid workload sizes", { timeout: 30000 }, async () => {
  for (const [args, label] of [
    [["0", "1", "0"], "clients"],
    [["1.5", "1", "0"], "clients"],
    [["1", "0", "0"], "rounds"],
    [["1", "1", "-1"], "historical sessions"],
  ]) {
    await assert.rejects(
      execute(process.execPath, [script, ...args], { timeout: 5000, killSignal: "SIGKILL" }),
      (error) => error.code === 1 && error.stderr.includes(`${label} must be an integer`),
    );
  }
});
