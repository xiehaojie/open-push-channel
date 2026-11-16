import type { Context } from "koa";
import { publishPush } from "../websocket/index.ts";

class PushController {
  async send(ctx: Context) {
    if (ctx.state?.pushAuthorized === false) {
      ctx.status = 401;
      ctx.body = "Unauthorized";
      return;
    }
    const body = ctx.request.body as {
      agentId?: unknown;
      sessionId?: unknown;
      runtimeId?: string;
      clientId?: unknown;
      eventId?: unknown;
      content?: unknown;
      signal?: unknown;
      event?: unknown;
    } | undefined;
    const agentId = typeof body?.agentId === "string" ? body.agentId.trim() : "";
    const sessionId = typeof body?.sessionId === "string" ? body.sessionId.trim() : "";
    if (!agentId || !body || (!body.content && !body.event && !body.signal)) {
      ctx.status = 400;
      ctx.body = "Missing agentId or content/signal/event";
      return;
    }
    if (body.eventId !== undefined && (typeof body.eventId !== "string" || !body.eventId.trim())) {
      ctx.status = 400;
      ctx.body = "Invalid eventId";
      return;
    }
    if (
      body.clientId !== undefined &&
      (typeof body.clientId !== "string" || !body.clientId.trim())
    ) {
      ctx.status = 400;
      ctx.body = "Invalid clientId";
      return;
    }
    try {
      const { clientId, eventId } = body;
      const sent = publishPush({ ...body, agentId, sessionId, clientId, eventId });
      ctx.status = sent ? 200 : 404;
      ctx.body = sent ? "Sent" : "Agent not found";
    } catch (error) {
      ctx.status = 400;
      ctx.body = (error as Error).message;
    }
  }
}

export default new PushController();
