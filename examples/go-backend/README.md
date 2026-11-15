# Call the gateway from a Go backend

This example uses only the Go standard library. It shows how a trusted server can deliver a notification or request a task for an existing conversation. Use Go 1.19 or later and follow the [gateway quick start](../../README.md#quick-start) to create the `demo` client's `quickstart` conversation bound to `openclaw/main`. Run the commands from the repository root in a shell with the gateway's `PUSH_CHANNEL_PUSH_TOKEN`.

Send a status update:

```sh
go run examples/go-backend/main.go -mode send \
  -client demo -session quickstart -runtime openclaw -agent main \
  -event quickstart-go-status-v1 -content 'Ticket resolved.'
```

Request a summary using a different business event ID:

```sh
go run examples/go-backend/main.go -mode task \
  -client demo -session quickstart -runtime openclaw -agent main \
  -event quickstart-go-summary-v1 -content 'Summarize this conversation.'
```

With `send`, HTTP 200 means the gateway accepted the notification. With `task`, HTTP 202 means the gateway accepted or already knows the task. Use the returned `runId` to follow execution over WebSocket. If a request times out, reuse the same event ID, target and content when retrying the same operation. The example returns errors without automatically retrying. If you see HTTP 401, check the server token. If you see HTTP 404, check that the session exists for the specified client, runtime and agent.

Use `-gateway` to pass a trusted gateway base URL and use HTTPS when calling across networks. Keep the server token on the backend. In your own application, verify the caller's permission to act on the target conversation before sending a notification or task. See the [integration guide](../../docs/integration.md) for auth and replay semantics.

The server-side example follows the approach used in [Centrifugo's Go SDK v4.0.0](https://github.com/centrifugal/gocent/tree/v4.0.0#example), with bounded HTTP calls and caller-owned idempotency keys.
