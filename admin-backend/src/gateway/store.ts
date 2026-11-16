import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { isDeepStrictEqual } from "node:util";
import {
  DummyDriver,
  Kysely,
  SqliteAdapter,
  SqliteIntrospector,
  SqliteQueryCompiler,
} from "kysely";
import type { Compilable, Generated } from "kysely";
import type { SQLInputValue } from "node:sqlite";
import { validateEvent } from "../../../protocol/index.ts";
import type { PushEvent } from "../../../protocol/index.ts";
import type { GatewaySession, JournalRequest } from "./types.ts";

type ControlEvent = Extract<PushEvent, { type: "input.control" }>;
type StoredEvent = PushEvent & { cursor: number };
type GatewaySchema = {
  sessions: Omit<GatewaySession, "nativeStarted"> & { nativeStarted: number };
  runs: {
    runId: string;
    clientId: string;
    conversationId: string;
    requestId: string | null;
    requestJson: string;
    status: string;
  };
  events: {
    cursor: Generated<number>;
    eventId: string;
    clientId: string;
    conversationId: string;
    runId: string;
    seq: number;
    eventJson: string;
  };
};

const terminalTypes = new Set(["run.completed", "run.failed", "run.cancelled", "run.deferred"]);
const bindingFields = ["clientId", "conversationId", "runtimeId", "agentId", "nativeSessionId"] as const;
const executionInput = ({
  runtimeId,
  agentId,
  content,
  mentions = [],
  event,
  signal,
  origin = "prompt",
}: JournalRequest) => ({
  runtimeId,
  agentId,
  content,
  mentions,
  event,
  signal,
  origin,
});

class GatewayStore {
  readonly database: DatabaseSync;
  readonly query: Kysely<GatewaySchema>;
  constructor({ path = ":memory:" } = {}) {
    if (path !== ":memory:") {
      mkdirSync(dirname(path), { recursive: true });
    }
    this.database = new DatabaseSync(path);
    this.query = new Kysely<GatewaySchema>({
      dialect: {
        createAdapter: () => new SqliteAdapter(),
        createDriver: () => new DummyDriver(),
        createIntrospector: (db) => new SqliteIntrospector(db),
        createQueryCompiler: () => new SqliteQueryCompiler(),
      },
    });
    try {
      this.migrate();
    } catch (error) {
      this.database.close();
      throw error;
    }
  }

  migrate() {
    const version = Number(this.database.prepare("PRAGMA user_version").get()?.user_version);
    if (version > 1) {
      throw new Error("Unsupported gateway database version");
    }
    if (version === 1) {
      return;
    }
    this.transaction(() => {
      this.database.exec(`
        CREATE TABLE sessions (
          conversationId TEXT NOT NULL,
          clientId TEXT NOT NULL,
          runtimeId TEXT NOT NULL,
          agentId TEXT NOT NULL,
          nativeSessionId TEXT NOT NULL,
          nativeStarted INTEGER NOT NULL,
          PRIMARY KEY(clientId, conversationId)
        );
        CREATE TABLE runs (
          runId TEXT PRIMARY KEY,
          clientId TEXT NOT NULL,
          conversationId TEXT NOT NULL,
          requestId TEXT,
          requestJson TEXT NOT NULL,
          status TEXT NOT NULL,
          UNIQUE(clientId, conversationId, requestId)
        );
        CREATE TABLE events (
          cursor INTEGER PRIMARY KEY AUTOINCREMENT,
          eventId TEXT NOT NULL UNIQUE,
          clientId TEXT NOT NULL,
          conversationId TEXT NOT NULL,
          runId TEXT NOT NULL,
          seq INTEGER NOT NULL,
          eventJson TEXT NOT NULL
        );
        CREATE INDEX events_replay ON events(clientId, conversationId, cursor);
        CREATE INDEX events_run_sequence ON events(runId, seq);
        PRAGMA user_version = 1;
      `);
    });
  }

  private execute<Row>(query: Compilable<Row>): Row[] {
    // Kysely owns all query construction. This bridge preserves synchronous
    // commit-before-publish semantics using node:sqlite rather than its async driver.
    const { sql, parameters } = query.compile();
    const statement = this.database.prepare(sql);
    const values = parameters as readonly SQLInputValue[];
    if (statement.columns().length) {
      return statement.all(...values) as Row[];
    }
    statement.run(...values);
    return [];
  }

  transaction<T>(action: () => T): T {
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const result = action();
      this.database.exec("COMMIT");
      return result;
    } catch (error) {
      this.database.exec("ROLLBACK");
      throw error;
    }
  }

  sessions() {
    return this.execute(
      this.query.selectFrom("sessions").selectAll().orderBy("clientId").orderBy("conversationId"),
    ).map((session) =>
      Object.assign({}, session, { nativeStarted: Boolean(session.nativeStarted) }),
    );
  }

  saveSession(session: GatewaySession) {
    this.transaction(() => {
      const existing = this.execute(
        this.query
          .selectFrom("sessions")
          .selectAll()
          .where("clientId", "=", session.clientId)
          .where("conversationId", "=", session.conversationId),
      )[0];
      if (existing) {
        if (bindingFields.some((field) => existing[field] !== session[field])) {
          throw new Error("Session binding is immutable; start a new session");
        }
        if (session.nativeStarted && !existing.nativeStarted) {
          this.execute(
            this.query
              .updateTable("sessions")
              .set({ nativeStarted: 1 })
              .where("clientId", "=", session.clientId)
              .where("conversationId", "=", session.conversationId),
          );
        }
        return;
      }
      this.execute(
        this.query
          .insertInto("sessions")
          .values({
            clientId: session.clientId,
            conversationId: session.conversationId,
            runtimeId: session.runtimeId,
            agentId: session.agentId,
            nativeSessionId: session.nativeSessionId,
            nativeStarted: Number(Boolean(session.nativeStarted)),
          }),
      );
    });
  }

  claimRun(request: JournalRequest, runId: string, initialEvents: PushEvent[] = []) {
    return this.transaction(() => {
      if (request.requestId !== undefined) {
        const existing = this.execute(
          this.query
            .selectFrom("runs")
            .selectAll()
            .where("clientId", "=", request.clientId)
            .where("conversationId", "=", request.conversationId)
            .where("requestId", "=", request.requestId),
        )[0];
        if (existing) {
          if (
            !isDeepStrictEqual(
              executionInput(JSON.parse(existing.requestJson)),
              executionInput(request),
            )
          ) {
            throw new Error(
              "requestId conflict: execution input differs from the accepted request",
            );
          }
          return { fresh: false as const, runId: existing.runId, status: existing.status, events: [] };
        }
      }
      this.execute(
        this.query.insertInto("runs").values({
          runId,
          clientId: request.clientId,
          conversationId: request.conversationId,
          requestId: request.requestId ?? null,
          requestJson: JSON.stringify(request),
          status: "accepted",
        }),
      );
      // Admission and its replayable envelopes share one commit. A failed batch
      // must not consume the request ID or expose a partial notification.
      const context = { ...request, runId };
      const events = initialEvents.map((event) => {
        validateEvent(event);
        if (
          (["runId", "clientId", "conversationId", "runtimeId", "agentId", "requestId"] as const).some(
            (field) => event[field] !== context[field],
          )
        ) {
          throw new Error("Initial event does not match the admitted request");
        }
        return this.insertEvent(event);
      });
      return { fresh: true as const, runId, events };
    });
  }

  markDispatched(runId: string) {
    this.execute(
      this.query
        .updateTable("runs")
        .set({ status: "dispatching" })
        .where("runId", "=", runId)
        .where("status", "=", "accepted"),
    );
  }

  appendEvent(event: PushEvent) {
    validateEvent(event);
    return this.transaction(() => this.insertEvent(event));
  }

  insertEvent(event: PushEvent): StoredEvent {
    // Accepted events always carry an owner; SQLite enforces the non-null column.
    const clientId = event.clientId!;
    const inserted = this.execute(
      this.query
        .insertInto("events")
        .values({
          eventId: event.eventId,
          clientId,
          conversationId: event.conversationId,
          runId: event.runId,
          seq: event.seq,
          eventJson: JSON.stringify(event),
        })
        .returning("cursor"),
    )[0];
    if (terminalTypes.has(event.type)) {
      this.execute(
        this.query
          .updateTable("runs")
          .set({ status: event.type.slice(4) })
          .where("runId", "=", event.runId)
          .where("clientId", "=", clientId)
          .where("conversationId", "=", event.conversationId),
      );
    }
    return { ...event, cursor: Number(inserted.cursor) };
  }

  events(clientId: string, conversationId: string, afterCursor = 0): StoredEvent[] {
    return this.execute(
      this.query
        .selectFrom("events")
        .select(["eventJson", "cursor"])
        .where("clientId", "=", clientId)
        .where("conversationId", "=", conversationId)
        .where("cursor", ">", afterCursor)
        .orderBy("cursor"),
    ).map(({ eventJson, cursor }) =>
      Object.assign(JSON.parse(eventJson) as PushEvent, { cursor: Number(cursor) }),
    );
  }

  controlReceipt(clientId: string, conversationId: string, controlId: string) {
    // Reuse the indexed conversation journal and return one matching receipt;
    // control IDs never authorize lookup across a client's binding.
    const record = this.execute(
      this.query
        .selectFrom("events")
        .select(["eventJson", "cursor"])
        .where("clientId", "=", clientId)
        .where("conversationId", "=", conversationId)
        .where((eb) =>
          eb(eb.fn("json_extract", ["eventJson", eb.val("$.type")]), "=", "input.control"),
        )
        .where((eb) =>
          eb(eb.fn("json_extract", ["eventJson", eb.val("$.data.controlId")]), "=", controlId),
        )
        .orderBy("cursor", "desc")
        .limit(1),
    )[0];
    return record
      ? { ...(JSON.parse(record.eventJson) as ControlEvent), cursor: Number(record.cursor) }
      : undefined;
  }

  recoverInterrupted() {
    return this.transaction(() => {
      const interrupted = this.execute(
        this.query
          .selectFrom("runs")
          .selectAll()
          .where("status", "in", ["accepted", "dispatching"]),
      );
      return interrupted.map((run) => {
        const request = JSON.parse(run.requestJson) as JournalRequest;
        // Dispatch intent belongs only to native execution, whether chat or an
        // external task. Push notifications commit their terminal batch atomically
        // and never dispatch; queued/preflight work must not poison a fresh session.
        if (run.status === "dispatching") {
          this.execute(
            this.query
              .updateTable("sessions")
              .set({ nativeStarted: 1 })
              .where("conversationId", "=", request.conversationId)
              .where("clientId", "=", request.clientId)
              .where("runtimeId", "=", request.runtimeId)
              .where("agentId", "=", request.agentId),
          );
        }
        const last = this.execute(
          this.query
            .selectFrom("events")
            .select("seq")
            .where("runId", "=", run.runId)
            .orderBy("seq", "desc")
            .limit(1),
        )[0];
        let seq = last?.seq ?? 0;
        const controls = new Map(
          this.execute(
            this.query
              .selectFrom("events")
              .select("eventJson")
              .where("runId", "=", run.runId)
              .where("clientId", "=", run.clientId)
              .where("conversationId", "=", run.conversationId)
              .where((eb) =>
                eb(eb.fn("json_extract", ["eventJson", eb.val("$.type")]), "=", "input.control"),
              )
              .orderBy("seq"),
          ).map(({ eventJson }) => {
            const event = JSON.parse(eventJson) as ControlEvent;
            return [event.data.controlId, event];
          }),
        );
        // A dispatch receipt lost at restart cannot be retried safely. Persist
        // its uncertainty before the parent terminal so replay never stays pending.
        for (const control of controls.values()) {
          if (control.data.status !== "dispatching") {
            continue;
          }
          this.insertEvent({
            ...control,
            eventId: randomUUID(),
            seq: ++seq,
            timestamp: Date.now(),
            data: {
              ...control.data,
              status: "unknown",
              message: "Gateway restarted; control outcome is unknown. It was not replayed.",
            },
          });
        }
        // Admission is a durable receipt, not a restartable job contract. Even
        // undispatched work may have started native session setup. Fail interrupted
        // work visibly; only an explicit new request may execute after a restart.
        const event = validateEvent({
          version: 1,
          eventId: randomUUID(),
          runtimeId: request.runtimeId,
          agentId: request.agentId,
          clientId: request.clientId,
          conversationId: request.conversationId,
          runId: run.runId,
          ...(request.requestId ? { requestId: request.requestId } : {}),
          ...(request.replyId ? { replyId: request.replyId } : {}),
          origin: request.origin ?? "prompt",
          audience: request.audience ?? "conversation",
          seq: seq + 1,
          timestamp: Date.now(),
          type: "run.failed",
          data: {
            message:
              "Gateway restarted; execution outcome is unknown. The request was not replayed.",
          },
        });
        return this.insertEvent(event);
      });
    });
  }

  close() {
    this.database.close();
  }
}

export { GatewayStore };
