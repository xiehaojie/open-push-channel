# Call the gateway from a Python backend

Use Python 3.11 or later. This example uses only the Python standard library; it does not require a Python package installation. Follow the [gateway quick start](../../README.md#quick-start) to create the `demo` client's `quickstart` conversation on `openclaw/main`. Set `PUSH_CHANNEL_PUSH_TOKEN` to the gateway's **server** token in your backend environment.

From the repository root, send a notification:

```sh
python3 examples/python-backend/send.py --mode send \
  --client demo --session quickstart --runtime openclaw --agent main \
  --event quickstart-python-status-v1 --content 'Ticket resolved.'
```

Or start a task with a different business event ID:

```sh
python3 examples/python-backend/send.py --mode task \
  --client demo --session quickstart --runtime openclaw --agent main \
  --event quickstart-python-summary-v1 --content 'Summarize this conversation.'
```

HTTP 200 acknowledges a notification; HTTP 202 acknowledges an accepted or already known task. The task command prints `runId`, `status`, and `duplicate` as JSON. Acceptance does not mean the task has finished. Observe the returned `runId` over WebSocket, for example with the [Python event listener](../python-events/README.md). `--runtime` may be omitted when the gateway can use the existing session binding.

If a call fails after sending, it may still have taken effect. Retry the same operation with the same event ID, target and content; use a new ID for a new business event. The example never retries automatically. HTTP 401 indicates a server token problem; HTTP 404 indicates a missing conversation or target mismatch. Keep the token on your server and verify your application's caller can act on the target conversation. See the [integration guide](../../docs/integration.md).

Use `--gateway` for a trusted HTTP or HTTPS base URL, and use HTTPS across networks. The example rejects redirects and URL credentials, queries and fragments. It calls the gateway directly without using proxy environment variables. Its 10-second socket timeout bounds blocking socket operations, not total wall-clock execution; an application requiring an overall deadline should enforce one in its job runner. Error output omits credentials and response bodies.

The retry and redirect choices draw on [pycent's retry guidance](https://github.com/centrifugal/pycent/blob/1dd4b90a27f50aacb53767250b2c66778327625e/README.md#retries) and [HTTP client behavior](https://github.com/centrifugal/pycent/blob/1dd4b90a27f50aacb53767250b2c66778327625e/README.md#http-client-tracing-and-metrics). pycent is MIT-licensed and targets Centrifugo's API; it is a reference, not a dependency or a compatible Open Push Channel client.

## Verify the example

With Node.js 24, Python 3.11 or later, and gateway dependencies installed:

```sh
npm --prefix admin-backend ci
PYTHON=python3 node --test examples/python-backend/send.test.cts
```

The test runs the CLI against an authenticated gateway with temporary SQLite storage and a fixture agent. It checks notification delivery, Unicode, task receipts/results, idempotent retries, HTTP 400/401/404 errors, rejected redirects, invalid URLs and malformed task receipts. No model credentials or running gateway are needed. CI runs it alongside the Python event listener on Python 3.11.
