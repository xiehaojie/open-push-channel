const { createGatewayServer } = require("../server.ts");

// An aborted test must not leave a gateway process holding the temporary DB.
process.once("disconnect", () => process.exit(0));
process.once("message", ({ auth, databasePath }) => {
  const server = createGatewayServer({
    auth,
    databasePath,
    adapters: [{
      id: "pi",
      async run(request, { emit, onAccepted }) {
        onAccepted();
        emit({ type: "message.started", data: { messageId: "partial" } });
        emit({ type: "message.delta", data: { messageId: "partial", delta: "before crash" } });
        // emit returns after SQLite commit: killing now loses no acknowledged progress.
        process.send({ type: "inflight", runId: request.runId });
        await new Promise(() => {});
      },
    }],
  });
  server.listen(0, "127.0.0.1", () => {
    process.send({ type: "ready", port: server.address().port });
  });
});
