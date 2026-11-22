# Measure notification delivery locally

The existing push benchmark runs a real, authenticated gateway on an ephemeral loopback port with temporary SQLite storage. Each simulated client gets its own token, WebSocket connection and conversation. It sends notifications through `POST /api/send` and checks their conversation, origin and content on WebSocket. Its fixture runtime must not execute an agent. No model credentials, installed agent runtime or existing gateway are required.

From the repository root, use Node.js 22.19+ on the 22.x line or Node.js 24+:

```sh
npm ci
npm --prefix admin-backend ci
node scripts/benchmark-push.ts 25 10 0
```

The positional arguments are client count, measured rounds and historical session count. They default to `25`, `10` and `0`. Clients and rounds must be positive integers; historical sessions may be zero. Begin with small values on a development machine.

The script creates all connections and session bindings, then sends one warmup notification to each client. It excludes setup and warmup from its measured rounds. Within each round it sends one notification per client concurrently, then waits for all HTTP acknowledgements and matching WebSocket deliveries before starting the next round. The default workload measures 250 deliveries. The temporary database is removed on normal completion or a handled failure.

The final JSON contains:

| Field | Meaning |
| --- | --- |
| `node`, `platform`, `arch` | Runtime and operating environment |
| `clients`, `rounds`, `historicalSessions` | Requested workload |
| `deliveries` | Number of measured, matching notifications; excludes warmup |
| `deliveriesPerSecond` | Measured deliveries divided by elapsed time for all rounds, including HTTP response handling and WebSocket delivery |
| `deliveryMs.p50`, `p95`, `p99` | Nearest-rank latency percentiles, rounded to two decimal places |

Latency starts immediately before preparing each HTTP request and ends when the receiving WebSocket observes its first matching `message.delta`. It includes request preparation, HTTP, gateway processing, SQLite work and event-loop scheduling. It does not measure model generation or the time to `run.completed`.

To compare results with a populated session registry, change only the third argument:

```sh
node scripts/benchmark-push.ts 25 10 1000
```

This seeds offline session bindings before gateway startup. It does not seed event history, exercise history replay, or add connected clients. Seed time and startup time are excluded from the measurement.

## Run the CI smoke check

```sh
node --test scripts/benchmark-push.test.cts
```

The check runs five clients for three measured rounds with ten offline session bindings. It verifies 15 deliveries, workload metadata, finite throughput and ordered latency percentiles. CI runs it on Node.js 22.19.0, 24.0.0 and the current 24.x version. It checks correctness without imposing hardware-specific throughput or latency thresholds.

## Interpret results

Keep the same Node version, workload, hardware, filesystem and background load when comparing runs. Repeat runs and retain their JSON together with the tested commit. The generator and server share one process on one machine, so the numbers describe this local workload. They are not a production capacity estimate, service-level guarantee, or a comparison with another server. This benchmark does not cover slow readers, network faults, reconnect replay, model concurrency or multiple gateway replicas.

The guidance follows [Spinify's benchmark documentation](https://github.com/PlugFox/spinify/blob/fa9b24a6ad926c47e55c37dd30c156556dd49ddd/README.md#benchmarks) and its [benchmark controller](https://github.com/PlugFox/spinify/blob/fa9b24a6ad926c47e55c37dd30c156556dd49ddd/example/benchmark/lib/src/benchmark_controller.dart), which record the test environment, received messages and timing. Spinify is MIT-licensed and measures a Dart SDK's WebSocket echo workload. It is a methodology reference, not a dependency or a directly comparable result: this script measures the gateway's HTTP-to-SQLite-to-WebSocket path.
