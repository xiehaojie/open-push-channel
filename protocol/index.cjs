const { randomUUID } = require("node:crypto");

const PROTOCOL_VERSION = 1;
const isRecord = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
const isString = (value) => typeof value === "string";
const isId = (value) => isString(value) && value.trim().length > 0;
const isTextList = (value) => Array.isArray(value) && value.every(isString);
const message = { messageId: isId };
const child = { agentId: isId };
const optionalPayloadFields = {
  messageId: isId,
  toolCallId: isId,
  toolName: isId,
  args: isRecord,
  agentId: isId,
  label: isString,
  childSessionId: isId,
  delta: isString,
  text: isString,
  message: isString,
  status: isString,
  isError: (value) => typeof value === "boolean",
  replace: (value) => typeof value === "boolean",
};
const payloads = {
  "input.accepted": { content: isString },
  "input.control": {
    controlId: isId,
    operation: (value) => ["steer", "follow_up"].includes(value),
    content: isString,
    status: (value) => ["dispatching", "queued", "handled", "rejected", "unknown"].includes(value),
  },
  "input.queue": { steering: isTextList, followUp: isTextList },
  "run.started": {},
  "run.completed": {},
  "run.failed": { message: isString },
  "run.cancelled": {},
  "run.deferred": { message: isString },
  "message.started": message,
  "message.delta": { ...message, delta: isString },
  "message.snapshot": { ...message, text: isString },
  "message.completed": message,
  "reasoning.started": {},
  "reasoning.delta": { delta: isString },
  "reasoning.completed": {},
  "activity.started": {},
  "activity.completed": {},
  "tool.started": { toolCallId: isId, toolName: isId, args: isRecord },
  "tool.completed": { toolCallId: isId },
  "subagent.started": child,
  "subagent.message": child,
  "subagent.delta": child,
  "subagent.tool.started": { ...child, toolCallId: isId, toolName: isId, args: isRecord },
  "subagent.tool.completed": { ...child, toolCallId: isId },
  "subagent.result": child,
  "subagent.failed": child,
  "subagent.ended": child,
};

function validateSignal(signal) {
  if (!isRecord(signal) || !Object.hasOwn(payloads, signal.type) || !isRecord(signal.data)) {
    throw new TypeError("Invalid push protocol event");
  }
  for (const [field, validate] of Object.entries(payloads[signal.type])) {
    if (!validate(signal.data[field])) {
      throw new TypeError(`Invalid ${signal.type} field: ${field}`);
    }
  }
  for (const [field, validate] of Object.entries(optionalPayloadFields)) {
    if (signal.data[field] !== undefined && !validate(signal.data[field])) {
      throw new TypeError(`Invalid ${signal.type} field: ${field}`);
    }
  }
  if (signal.type === "input.queue") {
    for (const field of ["discardedSteering", "discardedFollowUp"]) {
      if (signal.data[field] !== undefined && !isTextList(signal.data[field])) {
        throw new TypeError(`Invalid input.queue field: ${field}`);
      }
    }
    if (
      signal.data.reason !== undefined &&
      !["run_settled", "cancelled"].includes(signal.data.reason)
    ) {
      throw new TypeError("Invalid input.queue field: reason");
    }
  }
  return signal;
}

function validateAdapterSignal(signal) {
  validateSignal(signal);
  if (signal.type.startsWith("run.") || ["input.accepted", "input.control"].includes(signal.type)) {
    throw new TypeError("Run lifecycle belongs to the gateway");
  }
  return signal;
}

function validateEvent(event) {
  if (!isRecord(event) || event.version !== PROTOCOL_VERSION) {
    throw new TypeError("Unsupported push protocol version");
  }
  validateSignal(event);
  for (const field of ["eventId", "runtimeId", "agentId", "conversationId", "runId"]) {
    if (!isId(event[field])) {
      throw new TypeError(`Invalid push protocol field: ${field}`);
    }
  }
  for (const field of ["clientId", "requestId", "replyId"]) {
    if (event[field] !== undefined && !isId(event[field])) {
      throw new TypeError(`Invalid push protocol field: ${field}`);
    }
  }
  if (!Number.isSafeInteger(event.seq) || event.seq < 1 || !Number.isFinite(event.timestamp)) {
    throw new TypeError("Invalid push protocol sequence or timestamp");
  }
  if (event.cursor !== undefined && (!Number.isSafeInteger(event.cursor) || event.cursor < 1)) {
    throw new TypeError("Invalid push protocol cursor");
  }
  if (event.origin !== undefined && !["prompt", "push", "task"].includes(event.origin)) {
    throw new TypeError("Invalid push protocol origin");
  }
  if (
    event.audience !== undefined &&
    event.audience !== "agent" &&
    event.audience !== "conversation"
  ) {
    throw new TypeError("Invalid push protocol audience");
  }
  if (event.audience === "agent" && event.origin !== "push") {
    throw new TypeError("Agent-wide delivery requires a proactive push");
  }
  return event;
}

function createEventWriter(context, publish) {
  let seq = 0;
  return (signal) => {
    validateSignal(signal);
    const event = validateEvent({
      ...context,
      version: PROTOCOL_VERSION,
      eventId: randomUUID(),
      seq: ++seq,
      timestamp: Date.now(),
      type: signal.type,
      data: signal.data,
    });
    publish(event);
    return event;
  };
}

module.exports = {
  PROTOCOL_VERSION,
  validateSignal,
  validateAdapterSignal,
  validateEvent,
  createEventWriter,
};
