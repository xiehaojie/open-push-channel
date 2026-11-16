import { timingSafeEqual } from "node:crypto";

import type { IncomingMessage } from "node:http";
import type { TLSSocket } from "node:tls";
import type { GatewayAdapter, GatewayAuth } from "./types.ts";

type CredentialRole = "webhook" | "plugin";
type Credential = { role: CredentialRole; token: string };
const adapterCredentials = new WeakMap<object, Credential>();

function tokenKey(token: unknown) {
  // sameToken compares UTF-8 bytes; lone surrogates encode as replacement bytes.
  return typeof token === "string" && token ? Buffer.from(token).toString() : undefined;
}

function registerAdapterCredential(callable: object, role: CredentialRole, token: unknown) {
  const key = tokenKey(token);
  if (key !== undefined) {
    adapterCredentials.set(callable, { role, token: key });
  }
}

function assertAdapterCredentialIsolation(adapters: Iterable<GatewayAdapter>, auth?: GatewayAuth) {
  const clientTokens = new Set<string | undefined>();
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
  const seen = new Map<string, CredentialRole>();
  for (const adapter of adapters) {
    // Native authentication belongs to the attachment method even when run is wrapped.
    const credentials = [
      adapterCredentials.get(adapter.run),
      adapter.attachPlugin ? adapterCredentials.get(adapter.attachPlugin) : undefined,
    ].filter((value) => value !== undefined);
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

function sameToken(received: unknown, expected: unknown) {
  if (typeof received !== "string" || typeof expected !== "string" || !expected) {
    return false;
  }
  const left = Buffer.from(received);
  const right = Buffer.from(expected);
  return left.length === right.length && timingSafeEqual(left, right);
}

function authenticate(auth: GatewayAuth, token: unknown) {
  return auth.clients.find((client) => sameToken(token, client.token));
}

function isLocalGatewayRequest(request: IncomingMessage) {
  const { localAddress, localPort } = request.socket;
  const encrypted = (request.socket as Partial<TLSSocket>).encrypted;
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

export {
  sameToken,
  authenticate,
  isLocalGatewayRequest,
  registerAdapterCredential,
  assertAdapterCredentialIsolation,
};
