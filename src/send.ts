import { randomUUID } from "node:crypto";
import * as http from "node:http";
import * as https from "node:https";
import { URL } from "node:url";

function resolveSendUrl(middlewareUrl: string): URL {
  const baseUrl = middlewareUrl.endsWith("/") ? middlewareUrl : `${middlewareUrl}/`;
  return new URL("send", baseUrl);
}

async function postPushPayload(
  middlewareUrl: string,
  payload: Record<string, unknown>,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const url = resolveSendUrl(middlewareUrl);
    const requestModule = url.protocol === "https:" ? https : http;
    const postData = JSON.stringify(payload);

    const options = {
      hostname: url.hostname,
      port: url.port,
      path: url.pathname + url.search,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(postData),
        Connection: "close",
        ...(process.env.PUSH_CHANNEL_PUSH_TOKEN
          ? { Authorization: `Bearer ${process.env.PUSH_CHANNEL_PUSH_TOKEN}` }
          : {}),
      },
    };

    const req = requestModule.request(options, (res) => {
      res.on("data", () => {});
      res.on("end", () => {
        if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
          resolve();
        } else {
          reject(new Error(`Failed to send push message: ${res.statusCode} ${res.statusMessage}`));
        }
      });
    });

    req.on("error", (error) => {
      reject(error);
    });

    req.write(postData);
    req.end();
  });
}

export async function sendPushMessage(params: {
  middlewareUrl: string;
  agentId: string;
  content: string;
  sessionId?: string;
  retries?: number;
}) {
  const { middlewareUrl, agentId, content, sessionId, retries = 3 } = params;
  // A retry repeats delivery, not a new notification. Persisted gateway admission
  // uses this stable id if the first HTTP response was lost after committing.
  const eventId = randomUUID();

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const payload: Record<string, string> = { agentId, content, eventId };
      if (sessionId) {
        payload.sessionId = sessionId;
      }
      await postPushPayload(middlewareUrl, payload);
      return;
    } catch (error) {
      if (attempt === retries) {
        throw error;
      }
      await new Promise((resolve) => {
        setTimeout(resolve, 1000);
      });
    }
  }
}

export async function sendPushEvent(params: {
  middlewareUrl: string;
  agentId: string;
  event: Record<string, unknown>;
  sessionId?: string;
  retries?: number;
}) {
  const { middlewareUrl, agentId, event, sessionId, retries = 3 } = params;
  const eventId = randomUUID();

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const payload: Record<string, unknown> = { agentId, event, eventId };
      if (sessionId) {
        payload.sessionId = sessionId;
      }
      await postPushPayload(middlewareUrl, payload);
      return;
    } catch (error) {
      if (attempt === retries) {
        throw error;
      }
      await new Promise((resolve) => {
        setTimeout(resolve, 1000);
      });
    }
  }
}
