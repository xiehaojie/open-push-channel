# Read and replay gateway events from Python

Use Python 3.11 or later. Follow the [gateway quick start](../../README.md#quick-start) and set `PUSH_GATEWAY_CLIENT_TOKEN` to the **client** token used by the `demo` client.

From the repository root, install the dependency and start listening:

```sh
python3 -m venv /tmp/open-push-channel-events
source /tmp/open-push-channel-events/bin/activate
python -m pip install -r examples/python-events/requirements.txt
python examples/python-events/listen.py --session quickstart
```

The listener opens the `quickstart` conversation on `openclaw/main`. It creates the binding if the session doesn't exist. Use `--runtime` and `--agent` to select another authorized target. For a gateway across the network, pass its trusted `wss://` URL with `--gateway`.

Get a sample event by sending a notification from a separate terminal using the [Go backend example](../go-backend/README.md) or the quick start's HTTP request. Each output line is a JSON event with `type`, `conversationId`, and `cursor`. The listener prints conversation events, including replies, run results and targeted notifications. It does not start a task.

To resume after a disconnect, keep the cursor of the last event your application successfully processed. Rerun the listener with the same client token, gateway and session binding, replacing `42` with that saved cursor:

```sh
python examples/python-events/listen.py --session quickstart --after-cursor 42
```

The gateway replays the saved cursor inclusively, so the listener skips that boundary event before printing newer ones. Omit `--after-cursor` for a complete replay. Use `--count N` to exit after N events, or Ctrl-C to stop. The example leaves checkpoint storage and reconnect policy to the caller.

The cursor pattern draws on [Centrifuge's Python client](https://github.com/centrifugal/centrifuge-python/blob/8b9e7d3756a159ba0886fe9332a35f76145780eb/centrifuge/client.py). The request fields and replay boundaries follow the [Open Push Channel protocol](../../docs/protocol.md#断线重启与去重).

## Verify the example

With the Python environment above activated and the gateway dependencies installed, run this integration test from the repository root:

```sh
PYTHON=python node --test examples/python-events/listen.test.cts
```

The test launches the CLI against a temporary, authenticated gateway with SQLite. It verifies notification delivery, offline replay, skipping the inclusive boundary, complete replay and rejected credentials. CI runs this scenario on Python 3.11.
