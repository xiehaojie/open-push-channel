const { validateAdapterSignal } = require("./index.cjs");

const capabilityNames = [
  "streaming",
  "tools",
  "reasoning",
  "subagents",
  "cancel",
  "mentions",
  "sessionRestore",
  "steering",
  "followUp",
];
const requestFields = [
  "clientId",
  "runtimeId",
  "agentId",
  "conversationId",
  "nativeSessionId",
  "runId",
  "content",
  "resume",
  "requestId",
  "replyId",
  "origin",
  "mentions",
];

function fields(value, allowed) {
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    Object.keys(value).some((key) => !allowed.includes(key))
  ) {
    throw new TypeError("Invalid plugin protocol fields");
  }
  return value;
}

function text(value) {
  if (typeof value !== "string" || !value.trim()) {
    throw new TypeError("Invalid plugin protocol text");
  }
  return value;
}

function capabilities(value) {
  fields(value, capabilityNames);
  if (Object.values(value).some((flag) => typeof flag !== "boolean")) {
    throw new TypeError("Invalid plugin capabilities");
  }
  return Object.fromEntries(capabilityNames.map((key) => [key, value[key] ?? false]));
}

function request(value) {
  const selected = Object.fromEntries(
    requestFields.filter((key) => value[key] !== undefined).map((key) => [key, value[key]]),
  );
  for (const key of [
    "clientId",
    "runtimeId",
    "agentId",
    "conversationId",
    "nativeSessionId",
    "runId",
  ]) {
    text(selected[key]);
  }
  if (
    typeof selected.content !== "string" ||
    (selected.resume !== undefined && typeof selected.resume !== "boolean")
  ) {
    throw new TypeError("Invalid plugin run request");
  }
  for (const key of ["requestId", "replyId"]) {
    if (selected[key] !== undefined) {
      text(selected[key]);
    }
  }
  if (selected.origin !== undefined && !["prompt", "push", "task"].includes(selected.origin)) {
    throw new TypeError("Invalid plugin run origin");
  }
  if (selected.mentions !== undefined) {
    if (!Array.isArray(selected.mentions)) {
      throw new TypeError("Invalid plugin mentions");
    }
    for (const mention of selected.mentions) {
      fields(mention, ["agentId", "label"]);
      text(mention.agentId);
      if (mention.label !== undefined && typeof mention.label !== "string") {
        throw new TypeError("Invalid plugin mention label");
      }
    }
  }
  return selected;
}

function signal(value) {
  fields(value, ["type", "data"]);
  return validateAdapterSignal(value);
}

function notification(target, payload) {
  fields(target, ["clientId", "conversationId"]);
  text(target.clientId);
  text(target.conversationId);
  fields(payload, ["eventId", "content", "signal"]);
  text(payload.eventId);
  if ((payload.content !== undefined) === (payload.signal !== undefined)) {
    throw new TypeError("Provide one plugin notification content or signal");
  }
  if (payload.content !== undefined) {
    text(payload.content);
  } else {
    signal(payload.signal);
  }
  return { target, payload };
}

module.exports = { fields, text, capabilities, request, requestFields, signal, notification };
