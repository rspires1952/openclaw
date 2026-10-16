import fs from "node:fs";
import path from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { createTempDirTracker, useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import * as nodeSqlite from "../infra/node-sqlite.js";
import type { EngineConnection } from "../infra/sql-connection.js";
import { revokeSqliteDatabaseAdmissionsForPath } from "../infra/sqlite-database-admission.js";
import { SQLITE_IDLE_HANDLE_TTL_MS } from "../infra/sqlite-handle-lifecycle.js";
import { createDeferredCore } from "../shared/deferred.js";
import {
  assertNoOpenClawAgentDatabaseLeasesReadOnly,
  readActiveOpenClawAgentDatabaseLeasesReadOnly,
} from "./openclaw-agent-db-lease.js";
import { retainOpenClawAgentDatabaseReadOnly } from "./openclaw-agent-db-readonly.js";
import {
  borrowOpenClawAgentDatabase,
  clearOpenClawAgentDatabaseOpenFailure,
  closeOpenClawAgentDatabaseByPath as closeAgent,
  closeOpenClawAgentDatabasesForTest,
  getOpenClawAgentDatabaseIfOpen as cachedAgent,
  listOpenClawRegisteredAgentDatabases as registeredAgents,
  openOpenClawAgentDatabase as openAgent,
  recordOpenClawAgentDatabaseOpenFailure,
  settleOpenClawAgentDatabaseWorkerClose,
  withOpenClawAgentDatabaseAdmission as withAdmission,
  withOpenClawAgentDatabaseAsync as withAgent,
  runOpenClawAgentWriteTransaction,
  type OpenClawAgentDatabaseWriteAdmission,
} from "./openclaw-agent-db.js";
import {
  closeOpenClawStateDatabaseForTest,
  openOpenClawStateDatabase,
} from "./openclaw-state-db.js";
import { claimOpenClawStateOwnership } from "./openclaw-state-ownership-operations.js";

function interceptOpen(pathname: string, opened: (connection: DatabaseSync) => void) {
  const nativeOpen = nodeSqlite.openNodeSqliteDatabase;
  return vi.spyOn(nodeSqlite, "openNodeSqliteDatabase").mockImplementation((location, config) => {
    const connection = nativeOpen(location, config);
    if (location === pathname) {
      opened(connection);
    }
    return connection;
  });
}

function idle(db: EngineConnection, isOpen: boolean, elapsed = SQLITE_IDLE_HANDLE_TTL_MS) {
  vi.advanceTimersByTime(elapsed);
  expect(db.isOpen).toBe(isOpen);
}

describe("agent database lease acquisition owner", () => {
  const tempDirs = createTempDirTracker();
  const connections: DatabaseSync[] = [];
  const admitted: OpenClawAgentDatabaseWriteAdmission = async (run) => run(() => {});

  afterEach(() => {
    vi.restoreAllMocks();
    closeOpenClawAgentDatabasesForTest();
    for (const connection of connections.splice(0)) {
      if (connection.isOpen) {
        connection.close();
      }
    }
    closeOpenClawStateDatabaseForTest();
    vi.unstubAllEnvs();
    tempDirs.cleanup();
  });

  function createOwner() {
    const stateDir = tempDirs.make("agent-lease-owner-");
    const nextStateDir = tempDirs.make("agent-lease-next-");
    const env: NodeJS.ProcessEnv = { OPENCLAW_STATE_DIR: stateDir };
    const state = openOpenClawStateDatabase({ env });
    const leases = () => state.db.prepare("SELECT lease_id FROM agent_database_leases").all();
    return { stateDir, nextStateDir, env, leases };
  }

  it.each([true, false])("observes live leases before maintenance (cached: %s)", (cached) => {
    const { env } = createOwner();
    const agent = openAgent({ agentId: "main", env });
    if (!cached) {
      closeOpenClawStateDatabaseForTest();
    }

    expect(readActiveOpenClawAgentDatabaseLeasesReadOnly({ env })).toEqual([
      expect.objectContaining({ agent_id: "main", path: agent.path, owner_pid: process.pid }),
    ]);
    expect(() => assertNoOpenClawAgentDatabaseLeasesReadOnly({ env })).toThrow(
      "database is still open in process",
    );

    expect(closeAgent(agent.path)).toBe(true);
    if (!cached) {
      closeOpenClawStateDatabaseForTest();
    }
    expect(readActiveOpenClawAgentDatabaseLeasesReadOnly({ env })).toEqual([]);
    expect(() => assertNoOpenClawAgentDatabaseLeasesReadOnly({ env })).not.toThrow();
  });

  it("rejects initial authority denial without creating a database or changing its directory", async () => {
    const owner = createOwner();
    const directory = path.join(owner.stateDir, "custom");
    fs.mkdirSync(directory);
    fs.chmodSync(directory, 0o750);
    const mode = fs.statSync(directory).mode;
    const options = {
      agentId: "worker",
      env: owner.env,
      path: path.join(directory, "agent.sqlite"),
    };
    const denied = new Error("synthetic write authority revoked");
    const operation = vi.fn();

    await expect(
      withAdmission(
        options,
        async (run) =>
          run(() => {
            throw denied;
          }),
        operation,
      ),
    ).rejects.toBe(denied);

    expect(operation).not.toHaveBeenCalled();
    expect(fs.readdirSync(directory)).toEqual([]);
    expect(fs.statSync(directory).mode).toBe(mode);
    expect(owner.leases()).toEqual([]);
    expect(cachedAgent(options)).toBeUndefined();
    await expect(withAdmission(options, admitted, (database) => database.agentId)).resolves.toBe(
      options.agentId,
    );
  });

  it("unwinds an unfinished physical open when the scheduler rejects the next permit", async () => {
    const owner = createOwner();
    const options = { agentId: "worker", env: owner.env };
    const original = openAgent(options);
    original.db.exec(`
      INSERT INTO cache_entries (scope,key,value_json,expires_at,updated_at)
        VALUES ('admission','retained','{"retained":true}',100,1);
      DROP INDEX idx_agent_cache_expiry;
    `);
    expect(closeAgent(original.path)).toBe(true);
    closeOpenClawAgentDatabasesForTest(owner.stateDir);
    revokeSqliteDatabaseAdmissionsForPath(original.path);
    const nativeOpen = nodeSqlite.openNodeSqliteDatabase;
    let opened: DatabaseSync | undefined;
    const open = interceptOpen(original.path, (connection) => {
      opened = connection;
      connections.push(connection);
    });
    const lostScheduler = new Error("synthetic parent closed during database admission");
    let permits = 0;
    const scheduler: OpenClawAgentDatabaseWriteAdmission = async (run) => {
      if (++permits === 2) {
        expect(owner.leases()).toHaveLength(1);
        expect(opened?.isOpen).toBe(true);
        throw lostScheduler;
      }
      return run(() => {});
    };
    const operation = vi.fn();

    await expect(withAdmission(options, scheduler, operation)).rejects.toBe(lostScheduler);

    expect(permits).toBe(2);
    expect(operation).not.toHaveBeenCalled();
    expect(opened?.isOpen).toBe(false);
    expect(cachedAgent(options)).toBeUndefined();
    expect(owner.leases()).toEqual([]);
    open.mockRestore();
    {
      using retained = nativeOpen(original.path, { readOnly: true });
      expect(
        retained.prepare("SELECT sql FROM sqlite_schema WHERE name='idx_agent_cache_expiry'").get(),
      ).toBeUndefined();
      expect(retained.prepare("SELECT * FROM cache_entries").all()).toEqual([
        {
          scope: "admission",
          key: "retained",
          value_json: '{"retained":true}',
          blob: null,
          expires_at: 100,
          updated_at: 1,
        },
      ]);
    }
    await expect(withAdmission(options, admitted, (database) => database.agentId)).resolves.toBe(
      options.agentId,
    );
  });

  it("resets a recreated fixture root without retiring another root", () => {
    const now = vi.spyOn(Date, "now").mockReturnValue(1_000);
    const owner = createOwner();
    const env = owner.env;
    const otherEnv = { OPENCLAW_STATE_DIR: owner.nextStateDir };
    const closed = openAgent({ agentId: "closed", env });
    closeAgent(closed.path);
    const active = openAgent({ agentId: "active", env });
    const other = openAgent({ agentId: "other", env: otherEnv });
    const failedPath = path.join(owner.stateDir, "failed.sqlite");
    const otherFailedPath = path.join(owner.nextStateDir, "failed.sqlite");
    const failure = new Error("fixture open failure");
    recordOpenClawAgentDatabaseOpenFailure(failedPath, failure);
    recordOpenClawAgentDatabaseOpenFailure(otherFailedPath, failure);

    closeOpenClawAgentDatabasesForTest(owner.stateDir);

    expect(active.db.isOpen).toBe(false);
    expect(owner.leases()).toEqual([]);
    expect(other.db.isOpen).toBe(true);
    expect(openAgent({ agentId: "other", env: otherEnv })).toBe(other);
    expect(() => openAgent({ agentId: "failed", env: otherEnv, path: otherFailedPath })).toThrow(
      failure,
    );
    expect(openAgent({ agentId: "failed", env, path: failedPath }).db.isOpen).toBe(true);

    now.mockReturnValue(2_000);
    revokeSqliteDatabaseAdmissionsForPath(closed.path);
    openAgent({ agentId: closed.agentId, env });
    expect(registeredAgents({ env }).find((entry) => entry.path === closed.path)).toMatchObject({
      lastSeenAt: 2_000,
    });
  });

  it.each([
    ["ambient environment", true, false, false],
    ["removed external supervision marker", false, true, false],
    ["Worker settlement after environment mutation", false, false, true],
  ] as const)("releases the original lease with %s", (_kind, ambient, external, worker) => {
    const owner = createOwner();
    if (external) {
      owner.env.OPENCLAW_SUPERVISOR_MODE = "external";
      claimOpenClawStateOwnership("fixture-supervisor", { env: owner.env });
    }
    vi.stubEnv("OPENCLAW_STATE_DIR", owner.stateDir);
    vi.stubEnv("OPENCLAW_AGENT_DIR", undefined);
    const database = openAgent({
      agentId: "main",
      ...(ambient ? {} : { env: owner.env }),
    });
    expect(owner.leases()).toHaveLength(1);
    try {
      if (external) {
        delete owner.env.OPENCLAW_SUPERVISOR_MODE;
      } else {
        owner.env.OPENCLAW_STATE_DIR = owner.nextStateDir;
        vi.stubEnv("OPENCLAW_STATE_DIR", owner.nextStateDir);
      }
      expect(fs.readdirSync(owner.nextStateDir)).toEqual([]);
      if (worker) {
        expect(settleOpenClawAgentDatabaseWorkerClose(database.path)).toEqual({
          errors: [],
          settled: true,
        });
      } else if (ambient) {
        closeOpenClawAgentDatabasesForTest();
      } else {
        expect(closeAgent(database.path)).toBe(true);
      }
      expect(database.db.isOpen).toBe(false);
      expect(owner.leases()).toEqual([]);
      expect(fs.readdirSync(owner.nextStateDir)).toEqual([]);
    } finally {
      owner.env.OPENCLAW_STATE_DIR = owner.stateDir;
      if (external) {
        owner.env.OPENCLAW_SUPERVISOR_MODE = "external";
      }
      vi.stubEnv("OPENCLAW_STATE_DIR", owner.stateDir);
    }
  });

  it("releases a retained failed-open handle in its original store", () => {
    const owner = createOwner();
    const database = openAgent({ agentId: "main", env: owner.env });
    expect(closeAgent(database.path)).toBe(true);
    const open = interceptOpen(database.path, (connection) => {
      open.mockRestore();
      vi.spyOn(connection, "close").mockImplementationOnce(() => {
        // Change the caller's environment after claim, during failed-open cleanup.
        owner.env.OPENCLAW_STATE_DIR = owner.nextStateDir;
        throw new Error("fixture close failure");
      });
    });
    try {
      expect(() => openAgent({ agentId: "other", env: owner.env, path: database.path })).toThrow(
        "fixture close failure",
      );
      open.mockRestore();
      expect(owner.leases()).toHaveLength(1);
      expect(closeAgent(database.path)).toBe(true);
      expect(owner.leases()).toEqual([]);
      expect(fs.readdirSync(owner.nextStateDir)).toEqual([]);
    } finally {
      open.mockRestore();
      owner.env.OPENCLAW_STATE_DIR = owner.stateDir;
    }
  });
});

describe("openclaw agent database handle cache", () => {
  const tempDirs = useAutoCleanupTempDirTracker(afterAll);
  let env: NodeJS.ProcessEnv;

  beforeAll(() => {
    env = { OPENCLAW_STATE_DIR: fs.realpathSync(tempDirs.make("openclaw-agent-db-cache-")) };
  });

  const registrationFor = (pathname: string) =>
    registeredAgents({ env }).find((entry) => entry.path === pathname);

  beforeEach(() => {
    closeOpenClawAgentDatabasesForTest();
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  });

  afterEach(() => {
    closeOpenClawAgentDatabasesForTest();
    closeOpenClawStateDatabaseForTest();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("does not count periodic WAL maintenance as database activity", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    const database = openAgent({ agentId: "idle-maintenance", env });
    await vi.advanceTimersByTimeAsync(SQLITE_IDLE_HANDLE_TTL_MS);
    expect(database.db.isOpen).toBe(false);
  });

  it("retains concurrent admissions through the transaction's borrower handoff", async () => {
    const entered = createDeferredCore();
    const proceed = createDeferredCore();
    const transferred: ReturnType<typeof borrowOpenClawAgentDatabase>[] = [];
    let operations = 0;
    const admitted = ["adoption-first", "adoption-second"].map((agentId) => {
      const options = { agentId, env };
      return withAgent(options, async (database) => {
        if (++operations === 2) {
          entered.resolve();
        }
        await proceed.promise;
        return runOpenClawAgentWriteTransaction((current) => {
          expect(current.db).toBe(database.db);
          transferred.push(borrowOpenClawAgentDatabase(options));
          return database;
        }, options);
      });
    });
    const finished = Promise.all(admitted);
    try {
      await Promise.race([entered.promise, finished]);
      vi.advanceTimersByTime(SQLITE_IDLE_HANDLE_TTL_MS);
      proceed.resolve();
      const databases = await finished;
      vi.advanceTimersByTime(SQLITE_IDLE_HANDLE_TTL_MS);
      expect(databases.every((database) => database.db.isOpen)).toBe(true);
      for (const borrowed of transferred) {
        borrowed.release();
      }
      vi.advanceTimersByTime(SQLITE_IDLE_HANDLE_TTL_MS - 1);
      expect(databases.every((database) => database.db.isOpen)).toBe(true);
      vi.advanceTimersByTime(1);
      expect(databases.every((database) => !database.db.isOpen)).toBe(true);
    } finally {
      proceed.resolve();
      await Promise.allSettled(admitted);
      for (const borrowed of transferred) {
        borrowed.release();
      }
    }
  });

  it("does not invoke an admitted operation after explicit disposal revokes its handle", async () => {
    const target = openAgent({ agentId: "revoked-operation", env });
    const operation = vi.fn();
    const result = withAgent({ agentId: target.agentId, env }, operation);
    closeAgent(target.path);
    await expect(result).rejects.toThrow(/closed|revoked/);
    expect(operation).not.toHaveBeenCalled();
  });

  it("keeps an open transaction through expiry and evicts after it finishes", () => {
    const database = openAgent({ agentId: "transaction", env });
    database.db.exec("BEGIN IMMEDIATE");
    try {
      idle(database.db, true);
      expect(database.db.isTransaction).toBe(true);
    } finally {
      database.db.exec("ROLLBACK");
    }
    idle(database.db, false);
  });

  it("pins a completion's exact database until its final claim is released", () => {
    const options = { agentId: "completion", env };
    const database = openAgent(options);
    const retained = retainOpenClawAgentDatabaseReadOnly(options);
    if (!retained.found) {
      throw new Error("expected the cached database");
    }
    try {
      idle(database.db, true);
      expect(retained.claim.isCurrent()).toBe(true);
      retained.claim.release();
      idle(database.db, true, SQLITE_IDLE_HANDLE_TTL_MS - 1);
      idle(database.db, false, 1);
      expect(retained.claim.isCurrent()).toBe(false);
    } finally {
      retained.claim.release();
    }
  });

  it("retries failed lease cleanup without retaining a closed handle forever", () => {
    const database = openAgent({ agentId: "lease-retry", env });
    const { db: state } = openOpenClawStateDatabase({ env });
    state.exec(`CREATE TEMP TRIGGER fail_agent_lease_release BEFORE DELETE ON agent_database_leases
      BEGIN SELECT RAISE(ABORT, 'blocked lease release'); END`);
    try {
      expect(() => closeAgent(database.path)).toThrow("blocked lease release");
      expect(database.db.isOpen).toBe(false);
      state.exec("DROP TRIGGER fail_agent_lease_release");
      vi.advanceTimersByTime(SQLITE_IDLE_HANDLE_TTL_MS);
      const current = openOpenClawStateDatabase({ env });
      expect(
        current.db
          .prepare("SELECT lease_id FROM agent_database_leases WHERE agent_id = ?")
          .all(database.agentId),
      ).toEqual([]);
    } finally {
      if (state.isOpen) {
        state.exec("DROP TRIGGER IF EXISTS fail_agent_lease_release");
      }
      closeAgent(database.path);
    }
  });

  it("reuses an evicted database and repairs a replacement while preserving rows and discovery", () => {
    const options = { agentId: "durability", env };
    const evicted = openAgent(options);
    evicted.db
      .prepare(
        "INSERT INTO auth_profile_state (state_key, state_json, updated_at) VALUES (?, ?, ?)",
      )
      .run("cache-eviction", JSON.stringify({ preserved: true }), 42);
    const registration = registrationFor(evicted.path);
    expect(registration).toBeDefined();
    idle(evicted.db, false);
    const cachedReopen = openAgent(options);
    expect(cachedReopen).not.toBe(evicted);
    expect(registrationFor(evicted.path)).toEqual(registration);
    idle(cachedReopen.db, false);
    fs.copyFileSync(evicted.path, `${evicted.path}.replacement`);
    fs.renameSync(`${evicted.path}.replacement`, evicted.path);
    {
      using divergent = new (nodeSqlite.requireNodeSqlite().DatabaseSync)(evicted.path);
      divergent.exec("ALTER TABLE session_nodes DROP COLUMN project_id;");
    }
    const reopened = openAgent(options);
    expect(reopened).not.toBe(cachedReopen);
    expect(
      reopened.db
        .prepare("PRAGMA table_info(session_nodes)")
        .all()
        .some((row) => row.name === "project_id"),
    ).toBe(true);
    expect(
      reopened.db
        .prepare("SELECT state_json, updated_at FROM auth_profile_state WHERE state_key = ?")
        .get("cache-eviction"),
    ).toEqual({ state_json: JSON.stringify({ preserved: true }), updated_at: 42 });
    expect(registrationFor(evicted.path)).toMatchObject({
      agentId: options.agentId,
      path: evicted.path,
      schemaVersion: registration!.schemaVersion,
    });
  });

  it("revokes on quarantine and opens a new native handle after quarantine is cleared", () => {
    const options = { agentId: "quarantine", env };
    const database = openAgent(options);
    const open = vi.spyOn(nodeSqlite, "openNodeSqliteDatabase");
    const error = new Error("synthetic quarantine");
    expect(recordOpenClawAgentDatabaseOpenFailure(database.path, error)).toBe(true);
    expect(database.db.isOpen).toBe(false);
    expect(() => openAgent(options)).toThrow(error);
    clearOpenClawAgentDatabaseOpenFailure(database.path, { env });
    expect(openAgent(options).db.isOpen).toBe(true);
    expect(open.mock.calls.filter(([pathname]) => pathname === database.path)).toHaveLength(1);
  });
});
