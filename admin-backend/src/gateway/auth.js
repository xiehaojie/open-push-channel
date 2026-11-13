const { timingSafeEqual } = require("node:crypto");

const adapterCredentials = new WeakMap();

function tokenKey(token) {
  // sameToken compares UTF-8 bytes; lone surrogates encode as replacement bytes.
  return typeof token === "string" && token ? Buffer.from(token).toString() : undefined;
}

function registerAdapterCredential(callable, role, token) {
  const key = tokenKey(token);
  if (key !== undefined) {
    adapterCredentials.set(callable, { role, token: key });
  }
}

function assertAdapterCredentialIsolation(adapters, auth) {
  const clientTokens = new Set();
  const pushToken = tokenKey(auth?.pushToken);
  for (const client of auth?.clients ?? []) {
    const token = tokenKey(client.token);
    if (token === undefined) {
      continue;
    }
    if (token === pushToken) {
      throw new Error("Client and push tokens must be distinct");
    }
    if (clientTokens.has(token)) {
      throw new Error("Auth clients must have distinct ids and tokens");
    }
    clientTokens.add(token);
  }
  clientTokens.add(pushToken);
  const seen = new Map();
  for (const adapter of adapters) {
    // Native authentication belongs to the attachment method even when run is wrapped.
    const credentials = [
      adapterCredentials.get(adapter.run),
      adapterCredentials.get(adapter.attachPlugin),
    ].filter(Boolean);
    for (const { role, token } of credentials) {
      const previousRole = seen.get(token);
      if (role === "webhook" && previousRole === "webhook") {
        throw new Error("OpenClaw webhook tokens must be distinct");
      }
      if (role === "plugin" && previousRole === "plugin") {
        throw new Error("Plugin tokens must be distinct");
      }
      if (previousRole || (role === "webhook" && clientTokens.has(token))) {
        throw new Error(
          "OpenClaw webhook tokens must be distinct from client, push and plugin tokens",
        );
      }
      if (clientTokens.has(token)) {
        throw new Error("Plugin tokens must be distinct from client and push tokens");
      }
      seen.set(token, role);
    }
  }
}

function sameToken(received, expected) {
  if (typeof received !== "string" || typeof expected !== "string" || !expected) {
    return false;
  }
  const left = Buffer.from(received);
  const right = Buffer.from(expected);
  return left.length === right.length && timingSafeEqual(left, right);
}

function authenticate(auth, token) {
  return auth.clients.find((client) => sameToken(token, client.token));
}

function isLocalGatewayRequest(request) {
  const { localAddress, localPort, encrypted } = request.socket;
  if ((localAddress !== "127.0.0.1" && localAddress !== "::1") || !localPort) {
    return false;
  }
  const scheme = encrypted ? "https" : "http";
  const literal = localAddress === "::1" ? "[::1]" : localAddress;
  const defaultPort = encrypted ? 443 : 80;
  const port = localPort === defaultPort ? "" : `:${localPort}`;
  const host = request.headers.host?.toLowerCase();
  // The socket defines the trusted destination. A matching Host and Origin on
  // an unrelated DNS alias do not make that alias a loopback origin.
  if (host !== `${literal}${port}` && host !== `localhost${port}`) {
    return false;
  }
  const origin = request.headers.origin ?? request.headers["sec-websocket-origin"];
  return origin === undefined || origin === `${scheme}://${host}`;
}

module.exports = {
  sameToken,
  authenticate,
  isLocalGatewayRequest,
  registerAdapterCredential,
  assertAdapterCredentialIsolation,
};
