import { channel } from "node:diagnostics_channel";
import path from "node:path";
import { getEnvironmentData, setEnvironmentData, Worker } from "node:worker_threads";
import pg from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import {
  commitDaemonRuntimePin,
  readDaemonRuntimePin,
  readDaemonRuntimePinForInstall,
  readDaemonRuntimePinMigrationPending,
  withDaemonRuntimePinMigration,
} from "../daemon/runtime-pin-state.js";
import { withDaemonRuntimePinStore } from "../daemon/runtime-pin-store.js";
import { withEngineStoreAdmission } from "../infra/database-engine/admission-scope.js";
import { installDatabaseEngineSelection } from "../infra/database-engine/context.js";
import { resetDatabaseEngineSelectionForTest } from "../infra/database-engine/context.test-support.js";
import { openEngineStore } from "../infra/database-engine/open.js";
import { getPostgresPhysicalFacts } from "../infra/database-engine/physical-facts.js";
import { resolveDatabaseFamilyEngines } from "../infra/database-engine/registry.js";
import { openNodeSqliteDatabase } from "../infra/node-sqlite.js";
import {
  resolveRuntimeWorkerThreadExecArgv,
  resolveRuntimeWorkerUrl,
} from "../infra/runtime-worker-url.js";
import type { EngineConnection } from "../infra/sql-connection.js";
import {
  captureSqliteDatabaseAdmissions,
  retireSqliteDatabaseAdmissionForPath,
} from "../infra/sqlite-database-admission.js";
import { admitSqliteSchema } from "../infra/sqlite-schema-facts.js";
import { migrateDaemonRuntimePins } from "../infra/state-migrations.daemon-runtime-pins.js";
import { OPENCLAW_STATE_SCHEMA_VERSION } from "./openclaw-state-db-contract.js";
import {
  admitStatePostgres,
  inspectStateSqlite,
  stateEngineSchema,
} from "./openclaw-state-db-engine.js";
import {
  closeOpenClawStateDatabaseForTest,
  openOpenClawStateDatabase,
  runOpenClawStateWriteTransaction,
} from "./openclaw-state-db.js";
import { statePostgresRuntime } from "./postgres-schema.generated/runtime.js";
import { ensureUserBackgroundSchema } from "./user-background.store.js";

const configured: unknown = getEnvironmentData("openclaw.test.state.conformance.connection");
const connection = typeof configured === "string" ? configured : undefined;
const tempDirs = useAutoCleanupTempDirTracker(afterEach);
type Result = { namespace: string; admissionId: string; catalogReads: number; threadId: number };

describe.skipIf(!connection)("PostgreSQL shared-state owner", () => {
  it("migrates version 25 to 26 while preserving heads, lazy fragments and schema admission", async () => {
    const directory = tempDirs.make("state-layout-postgres-");
    const pathname = path.join(directory, "state.sqlite");
    const options = { path: pathname, env: { ...process.env, OPENCLAW_STATE_DIR: directory } };
    const client = new pg.Client({ connectionString: connection });
    await client.connect();
    installDatabaseEngineSelection({
      effectiveEngines: resolveDatabaseFamilyEngines({ database: { engine: "postgres" } }),
      postgres: { connection: connection!, schemaPrefix: "state_layout_conformance" },
    });
    let namespace: string | undefined;
    const withVersion26 = (run: (db: EngineConnection) => void) =>
      withEngineStoreAdmission(() => {
        using anchor = openNodeSqliteDatabase(pathname);
        const selected = openEngineStore({
          anchor,
          schema: {
            ...stateEngineSchema,
            ...stateEngineSchema.postgresMigrations!.find((step) => step.version === 27)!
              .fromSchema,
            version: 26,
            postgresMigrations: stateEngineSchema.postgresMigrations!.filter(
              (step) => step.version <= 26,
            ),
          },
          input: { connection: connection!, schemaPrefix: "state_layout_conformance" },
          inspectSqlite: () => inspectStateSqlite(anchor),
          prepareAnchor() {},
          openSqlite() {
            throw new Error("Expected PostgreSQL state layout fixture");
          },
          admitPostgres: admitStatePostgres,
          existingOnly: true,
        });
        try {
          run(selected);
        } finally {
          if (selected !== anchor) {
            selected.close();
          }
        }
      });
    try {
      const facts = withEngineStoreAdmission(() =>
        runOpenClawStateWriteTransaction(({ db }) => {
          ensureUserBackgroundSchema(options);
          db.prepare(`INSERT INTO session_state_heads
            (session_key, agent_id, last_sequence, pruned_max_sequence, updated_at)
            VALUES (?, ?, ?, ?, ?)`).run("shared-key", "main", 7, 3, 11);
          db.prepare(`INSERT INTO session_state_heads
            (session_key, agent_id, last_sequence, pruned_max_sequence, updated_at)
            VALUES (?, ?, ?, ?, ?)`).run("shared-key", "other", 19, 5, 23);
          return getPostgresPhysicalFacts(db);
        }, options),
      );
      if (!facts) {
        throw new Error("State layout fixture did not select PostgreSQL");
      }
      namespace = facts.namespace;
      const retained = await client.query(
        `SELECT * FROM "${namespace}".session_state_heads ORDER BY agent_id`,
      );
      closeOpenClawStateDatabaseForTest();
      retireSqliteDatabaseAdmissionForPath(pathname);
      // v25 has the same PostgreSQL catalog; only its published versions differ.
      await client.query(`BEGIN;
        SET LOCAL search_path TO "${namespace}";
        ${statePostgresRuntime.clientCredentialSource.sql}
        UPDATE schema_meta SET schema_version = 25;
        UPDATE config_machine_state SET value_json = '25'
          WHERE state_key = 'state.schema.contentVersion';
        UPDATE openclaw_schema_meta
          SET version = 25, fingerprint = NULL, ddl_hash = NULL, server_major = NULL;
        COMMIT;`);
      withVersion26((db) => {
        expect(getPostgresPhysicalFacts(db)?.version).toBe(26);
        expect(
          db.prepare("SELECT schema_version FROM schema_meta WHERE meta_key = 'primary'").get(),
        ).toEqual({ schema_version: 26 });
        expect(
          db
            .prepare(
              "SELECT value_json FROM config_machine_state WHERE state_key = 'state.schema.contentVersion'",
            )
            .get(),
        ).toEqual({ value_json: "26" });
        expect(
          db
            .prepare(
              "SELECT version, fingerprint, ddl_hash, server_major FROM openclaw_schema_meta",
            )
            .get(),
        ).toEqual({
          version: 26,
          fingerprint: expect.stringMatching(/^[0-9a-f]{64}$/),
          ddl_hash: expect.stringMatching(/^[0-9a-f]{64}$/),
          server_major: facts.capabilities.serverMajor,
        });
      });
      expect(
        (await client.query(`SELECT * FROM "${namespace}".session_state_heads ORDER BY agent_id`))
          .rows,
      ).toEqual(retained.rows);
      expect(
        (await client.query(`SELECT * FROM "${namespace}".user_background_images`)).rows,
      ).toEqual([]);
      closeOpenClawStateDatabaseForTest();
      retireSqliteDatabaseAdmissionForPath(pathname);
      withVersion26((db) => {
        expect(getPostgresPhysicalFacts(db)?.version).toBe(26);
      });
    } finally {
      closeOpenClawStateDatabaseForTest();
      retireSqliteDatabaseAdmissionForPath(pathname);
      resetDatabaseEngineSelectionForTest();
      try {
        if (namespace) {
          await client.query(`DROP SCHEMA "${namespace}" CASCADE`);
        }
      } finally {
        await client.end();
      }
    }
  });

  it("bootstraps, retains lazy fragments and admission, and refuses an older schema without captured custody", async () => {
    const pathname = path.join(tempDirs.make("state-postgres-"), "state.sqlite");
    const run = async (mode: "bootstrap" | "restart" | "migrate") => {
      const anchor = openNodeSqliteDatabase(pathname);
      admitSqliteSchema(anchor);
      const url = resolveRuntimeWorkerUrl({
        currentModuleUrl: import.meta.url,
        sourceWorkerName: "openclaw-state-db.postgres.test-support",
        distWorkerPath: "state/openclaw-state-db.postgres.test-support.js",
      });
      const previous = getEnvironmentData("openclaw.test.state.connection");
      let worker: Worker;
      try {
        setEnvironmentData("openclaw.test.state.connection", connection);
        worker = new Worker(url, {
          execArgv: resolveRuntimeWorkerThreadExecArgv(url),
          workerData: {
            path: pathname,
            mode,
            admissions: captureSqliteDatabaseAdmissions(undefined, { location: pathname }),
          },
        });
      } finally {
        setEnvironmentData("openclaw.test.state.connection", previous);
      }
      try {
        return await new Promise<Result>((resolve, reject) => {
          let result: Result | { error: string } | undefined;
          worker.on("message", (message: Result | { error: string }) => {
            result = message;
          });
          worker.once("error", reject);
          worker.once("exit", (code) => {
            if (code !== 0 || !result) {
              reject(new Error(`State worker exited ${code} without a result`));
            } else if ("error" in result) {
              reject(new Error(result.error));
            } else {
              resolve(result);
            }
          });
        });
      } finally {
        await worker.terminate();
        anchor.close();
        retireSqliteDatabaseAdmissionForPath(pathname);
      }
    };
    const client = new pg.Client({ connectionString: connection });
    await client.connect();
    let namespace: string | undefined;
    try {
      const cold = await run("bootstrap");
      namespace = cold.namespace;
      expect(cold.threadId).not.toBe(0);
      await client.query(`SET search_path = "${namespace}"`);
      const restart = await run("restart");
      expect(restart.namespace).toBe(cold.namespace);
      expect(restart.admissionId).not.toBe(cold.admissionId);
      await client.query(`UPDATE "${namespace}".schema_meta SET schema_version = $1`, [
        OPENCLAW_STATE_SCHEMA_VERSION + 1,
      ]);
      await expect(run("restart")).rejects.toThrow("PostgreSQL state metadata mismatch");
      await client.query(`UPDATE "${namespace}".schema_meta SET schema_version = $1`, [
        OPENCLAW_STATE_SCHEMA_VERSION,
      ]);
      await client.query(`DROP TABLE "${namespace}".secret_store_entries`);
      await expect(run("restart")).rejects.toThrow("PostgreSQL schema drift detected");
      await client.query(
        statePostgresRuntime.fragments.find((fragment) => fragment.id === "secret_store_entries")!
          .sql,
      );
      await client.query(`BEGIN;
        ${statePostgresRuntime.clientCredentialSource.sql}
        UPDATE openclaw_schema_meta SET version = 24, fingerprint = NULL, ddl_hash = NULL, server_major = NULL WHERE store = 'state';
        UPDATE schema_meta SET schema_version = 24 WHERE meta_key = 'primary';
        UPDATE config_machine_state SET value_json = '24'
          WHERE state_key = 'state.schema.contentVersion';
        INSERT INTO subagent_runs
          (run_id, child_session_key, requester_session_key, created_at, payload_json)
          VALUES ('retained-child', 'agent:main:subagent:retained', 'agent:main:main', 1,
            '{"completion":{"resultText":"Interrupted by Gateway restart"}}');
        COMMIT;`);
      const retained = (await client.query("SELECT * FROM subagent_runs")).rows;
      await expect(run("migrate")).rejects.toThrow(
        "Client credentials require backed-up migration custody",
      );
      expect((await client.query("SELECT * FROM subagent_runs")).rows).toEqual(retained);
      expect(
        (
          await client.query(
            "SELECT version::int AS version FROM openclaw_schema_meta WHERE store = 'state'",
          )
        ).rows,
      ).toEqual([{ version: 24 }]);
    } finally {
      if (!namespace) {
        const anchor = openNodeSqliteDatabase(pathname);
        try {
          const present = anchor
            .prepare("SELECT 1 FROM sqlite_master WHERE name = 'openclaw_engine_anchor'")
            .get();
          const value = present
            ? anchor.prepare("SELECT schema FROM openclaw_engine_anchor").get()?.schema
            : undefined;
          if (typeof value === "string") {
            namespace = value;
          }
        } finally {
          anchor.close();
        }
      }
      if (namespace) {
        await client.query(`DROP SCHEMA IF EXISTS "${namespace.replaceAll('"', '""')}" CASCADE`);
      }
      await client.end();
    }
  });
});

describe.skipIf(!connection)("PostgreSQL local runtime pins", () => {
  const directories = useAutoCleanupTempDirTracker(afterAll);
  const client = new pg.Client({ connectionString: connection });
  let env: NodeJS.ProcessEnv;
  let namespace: string;

  beforeAll(async () => {
    await client.connect();
    env = { ...process.env, OPENCLAW_STATE_DIR: directories.make("pin-postgres-") };
    installDatabaseEngineSelection({
      effectiveEngines: resolveDatabaseFamilyEngines({ database: { engine: "postgres" } }),
      postgres: { connection: connection!, schemaPrefix: "pin_conformance" },
    });
    namespace = withEngineStoreAdmission(() =>
      runOpenClawStateWriteTransaction(
        ({ db }) => {
          const facts = getPostgresPhysicalFacts(db);
          if (!facts) {
            throw new Error("Runtime pin fixture did not select PostgreSQL");
          }
          return facts.namespace;
        },
        { env },
      ),
    );
  });

  afterAll(async () => {
    closeOpenClawStateDatabaseForTest();
    resetDatabaseEngineSelectionForTest();
    try {
      if (namespace) {
        await client.query(`DROP SCHEMA "${namespace}" CASCADE`);
      }
    } finally {
      await client.end();
    }
  });

  it("keeps status, install and update intent local without accessing PostgreSQL pins", async () => {
    const statements: { engine: string; sql: string }[] = [];
    const observation = channel("openclaw.database.statement");
    const observe = (event: unknown) => {
      if (
        event &&
        typeof event === "object" &&
        "engine" in event &&
        typeof event.engine === "string" &&
        "sql" in event &&
        typeof event.sql === "string"
      ) {
        statements.push({ engine: event.engine, sql: event.sql });
      }
    };
    observation.subscribe(observe);
    try {
      // Prove capture observes the selected shared-state owner before pin operations.
      withEngineStoreAdmission(() =>
        runOpenClawStateWriteTransaction(
          ({ db }) =>
            db
              .prepare("SELECT state_key FROM config_machine_state WHERE state_key = ?")
              .get("state.schema.contentVersion"),
          { env },
        ),
      );
      expect(
        statements.some(
          ({ engine, sql }) => engine === "postgres" && sql.includes("config_machine_state"),
        ),
      ).toBe(true);
      statements.length = 0;

      const scope = { kind: "gateway" as const, env };
      const pin = { runtime: "node" as const, path: "/runtime tools/node" };
      const command = { programArguments: [pin.path, "/app/openclaw.mjs", "gateway"] };
      expect(readDaemonRuntimePinMigrationPending(env)).toBe(false);
      expect(readDaemonRuntimePin(scope, command).stored).toBe(false);
      expect(
        withDaemonRuntimePinStore(env, false, (db) =>
          db.prepare("SELECT * FROM daemon_runtime_pin_migration").get(),
        ),
      ).toEqual({
        version: 1,
        source_state: "none/unshipped",
        phase: "complete",
        updated_at: expect.any(Number),
      });
      expect(await migrateDaemonRuntimePins(env)).toEqual({ changes: [], warnings: [] });
      await withDaemonRuntimePinMigration(env, () => {
        const expected = readDaemonRuntimePinForInstall(scope, command, true);
        commitDaemonRuntimePin(scope, { expected, pin }, command);
      });
      // The native CLI projection deliberately skips config/engine startup.
      resetDatabaseEngineSelectionForTest();
      const status = readDaemonRuntimePin(scope, command);
      expect(status).toMatchObject({ stored: true, pin, definition: expect.any(String) });
      expect(
        await withDaemonRuntimePinMigration(env, () => readDaemonRuntimePin(scope, command)),
      ).toEqual(status);
      expect(readDaemonRuntimePinMigrationPending(env)).toBe(false);
      expect(
        statements.filter(
          ({ engine, sql }) => engine === "postgres" || sql.includes("config_machine_state"),
        ),
      ).toEqual([]);
    } finally {
      observation.unsubscribe(observe);
    }
  });
});
