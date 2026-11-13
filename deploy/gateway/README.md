# Run the gateway in Docker

The gateway image serves the WebSocket and HTTP endpoints and the browser demo. Run OpenClaw or a native dsh plugin separately. The [runtime runner image](../runtime-runner/README.md) also installs the dsh and Pi executables for managed RPC.

## Connect OpenClaw on the Docker host

The example uses Docker Desktop's host address. On Docker Engine for Linux, add `--add-host=host.docker.internal:host-gateway` to the run command. If both services are on the same Docker network, replace the URL in [gateway.example.json](gateway.example.json) with the OpenClaw container's DNS name.

1. Generate three separate secrets in your terminal.

   ```sh
   export PUSH_GATEWAY_CLIENT_TOKEN="$(openssl rand -hex 32)"
   export PUSH_CHANNEL_PUSH_TOKEN="$(openssl rand -hex 32)"
   export PUSH_CHANNEL_WEBHOOK_TOKEN="$(openssl rand -hex 32)"
   ```

2. Configure the installed OpenClaw plugin to listen on an address reachable from the container and send notifications back through port 3003.

   ```sh
   openclaw config set channels.push-channel.enabled true
   openclaw config set channels.push-channel.listenHost 0.0.0.0
   openclaw config set channels.push-channel.middlewareUrl http://127.0.0.1:3003
   ```

   Provide `PUSH_CHANNEL_WEBHOOK_TOKEN` and `PUSH_CHANNEL_PUSH_TOKEN` to the OpenClaw process before starting it. Use a firewall or private network to restrict port 3002 to the gateway. Use TLS if the webhook crosses an untrusted network. If OpenClaw runs in a separate container, use the gateway's service address for `middlewareUrl`.

3. Build and run from the repository root.

   ```sh
   docker build -f deploy/gateway/Dockerfile -t open-push-channel-gateway:local .
   docker run --rm --name open-push-channel \
     -p 127.0.0.1:3003:3003 \
     --mount "type=bind,source=$PWD/deploy/gateway/gateway.example.json,target=/etc/open-push-channel/gateway.json,readonly" \
     --mount type=volume,source=open-push-channel-state,target=/var/lib/open-push-channel \
     -e PUSH_GATEWAY_CLIENT_TOKEN \
     -e PUSH_CHANNEL_PUSH_TOKEN \
     -e PUSH_CHANNEL_WEBHOOK_TOKEN \
     open-push-channel-gateway:local
   ```

4. Run `curl --fail http://127.0.0.1:3003/healthz` to check gateway startup. Open [the demo](http://127.0.0.1:3003/demo/), connect as `demo` with `PUSH_GATEWAY_CLIENT_TOKEN`, start an OpenClaw session, and send a message.

The named volume keeps conversation bindings, event history, and notification receipts across container restarts. Backup and permissions for this volume are separate from OpenClaw workspace and history. For native dsh mode, add a `type: "plugin"` runtime using the [plugin example](../../admin-backend/gateway.plugins.example.json) and pass its token environment variable into the gateway container.
