const { publishPush } = require("../websocket/index");

class PushController {
  async send(ctx) {
    if (ctx.state?.pushAuthorized === false) {
      ctx.status = 401;
      ctx.body = "Unauthorized";
      return;
    }
    const body = ctx.request.body;
    const agentId = typeof body?.agentId === "string" ? body.agentId.trim() : "";
    const sessionId = typeof body?.sessionId === "string" ? body.sessionId.trim() : "";
    if (!agentId || (!body.content && !body.event && !body.signal)) {
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
      const sent = publishPush({ ...body, agentId, sessionId });
      ctx.status = sent ? 200 : 404;
      ctx.body = sent ? "Sent" : "Agent not found";
    } catch (error) {
      ctx.status = 400;
      ctx.body = error.message;
    }
  }
}

module.exports = new PushController();
