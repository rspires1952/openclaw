import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import * as deferredMigrations from "../infra/deferred-plugin-migrations.js";
import { acquireStartupMigrationLeaseWithWait } from "../infra/startup-migration-checkpoint.js";
import { clearPluginMetadataLifecycleCaches } from "../plugins/plugin-metadata-lifecycle.js";
import { withArtifactPreservingStateReads } from "../state/openclaw-state-db-readonly.js";
import {
  closeOpenClawStateDatabaseAsync,
  closeOpenClawStateDatabaseForTest,
  openOpenClawStateDatabase,
} from "../state/openclaw-state-db.js";
import { createConfigIoContext } from "./io.context.js";
import { createConfigIO } from "./io.factory.js";
import {
  captureConfigHealthStateStore,
  patchConfigHealthEntryToStore,
  readConfigHealthStateFromStore,
} from "./io.health-state.js";
import * as healthOwner from "./io.health-state.js";
import { observeConfigSnapshotSync } from "./io.observe.js";
import { normalizeConfigIoDeps } from "./io.read-helpers.js";
import type { ConfigIoFactoryOptions } from "./io.types.js";

const tempDirs = useAutoCleanupTempDirTracker((cleanup) =>
  afterEach(async () => {
    vi.restoreAllMocks();
    clearPluginMetadataLifecycleCaches();
    await closeOpenClawStateDatabaseAsync();
    closeOpenClawStateDatabaseForTest();
    cleanup();
  }),
);

function manifest(root: string) {
  return fs
    .readdirSync(root, { recursive: true, encoding: "utf8" })
    .toSorted()
    .filter((entry) => fs.statSync(path.join(root, entry)).isFile())
    .map((entry) => [
      entry,
      createHash("sha256")
        .update(fs.readFileSync(path.join(root, entry)))
        .digest("hex"),
    ]);
}

function clobberFiles(root: string) {
  return fs.readdirSync(root).filter((name) => name.startsWith("openclaw.json.clobbered."));
}

function expectOriginalIntact(root: string, configPath: string, original: string) {
  expect(fs.readFileSync(configPath, "utf8")).toBe(original);
  expect(clobberFiles(root)).toEqual([]);
}

function expectClobbered(root: string, original: string) {
  const clobbered = clobberFiles(root);
  expect(clobbered).toHaveLength(1);
  expect(fs.readFileSync(path.join(root, clobbered[0]!), "utf8")).toBe(original);
}

function failingFs(
  methods: readonly string[],
  shouldFail: (method: string, args: unknown[]) => Error | undefined,
) {
  function intercept<T extends object>(owner: T, suffix = ""): T {
    return new Proxy(owner, {
      get(target, key, receiver) {
        const original: unknown = Reflect.get(target, key, receiver);
        if (key === "promises") {
          return intercept(fs.promises, "Async");
        }
        const method = `${String(key)}${suffix}`;
        if (!methods.includes(method) || typeof original !== "function") {
          return original;
        }
        return (...args: unknown[]) => {
          const error = shouldFail(method, args);
          if (error && suffix) {
            return Promise.reject(error);
          }
          if (error) {
            throw error;
          }
          return Reflect.apply(original, target, args);
        };
      },
    });
  }
  return intercept(fs);
}

function fixture(options: ConfigIoFactoryOptions = {}) {
  const root = tempDirs.make("openclaw-prepared-config-recovery-");
  const configPath = path.join(root, "openclaw.json");
  const original = '{ "update": { "channel": "beta" } }\n';
  const backup = JSON.stringify({
    gateway: { mode: "local", port: 18720 },
    env: { vars: { RECOVERY_MARKER: "backup" } },
  });
  fs.writeFileSync(configPath, original);
  fs.writeFileSync(`${configPath}.bak`, backup);
  const env = {
    HOME: root,
    USERPROFILE: root,
    OPENCLAW_CONFIG_PATH: configPath,
    OPENCLAW_STATE_DIR: root,
    OPENCLAW_DISABLE_BUNDLED_PLUGINS: "1",
    VITEST: "true",
  };
  const databasePath = openOpenClawStateDatabase({ env }).path;
  closeOpenClawStateDatabaseForTest();
  const createIO = (overrides: ConfigIoFactoryOptions = {}) =>
    createConfigIO({
      env,
      configPath,
      homedir: () => root,
      observe: false,
      logger: { warn: vi.fn(), error: vi.fn() },
      ...options,
      ...overrides,
    });
  const read = (target = configPath) => fs.readFileSync(target, "utf8");
  return {
    root,
    configPath,
    original,
    backup,
    databasePath,
    env,
    io: createIO(),
    createIO,
    read,
    expectUnchanged: () => expectOriginalIntact(root, configPath, original),
    expectRestored: () => {
      expect(read()).toBe(backup);
      expectClobbered(root, original);
    },
    expectPreserved: () => {
      expectOriginalIntact(root, configPath, original);
      expect(read(`${configPath}.bak`)).toBe(backup);
    },
  };
}

async function prepare(io: ReturnType<typeof createConfigIO>) {
  const current = await withArtifactPreservingStateReads(() => io.readConfigFileSnapshot());
  return io.prepareConfigRecovery(current);
}

async function recovery(io: ReturnType<typeof createConfigIO>) {
  const plan = await prepare(io);
  if (!plan) {
    throw new Error("Expected a prepared recovery");
  }
  return plan;
}

describe("prepared config recovery", () => {
  it.each(["sync", "async"] as const)("prepares without metadata (%s)", async (mode) => {
    const root = tempDirs.make("openclaw-backup-preparation-io-");
    const context = createConfigIoContext({
      configPath: path.join(root, "openclaw.json"),
      env: { HOME: root, OPENCLAW_STATE_DIR: root },
      observe: false,
    });
    const unavailable = Object.assign(new Error("migration metadata unavailable"), {
      code: "EIO",
    });
    const candidate = {
      raw: '{"gateway":{"mode":"local"}}',
      parsed: { gateway: { mode: "local" } },
    };
    vi.spyOn(deferredMigrations, "readDeferredPluginMigrations").mockImplementation(() => {
      throw unavailable;
    });
    vi.spyOn(deferredMigrations, "readDeferredPluginMigrationsAsync").mockRejectedValue(
      unavailable,
    );
    const prepared =
      mode === "sync"
        ? context.prepareRecoveryBackupCandidate(candidate)
        : await context.prepareRecoveryBackupCandidateAsync(candidate);
    expect(prepared).toMatchObject({
      ok: true,
      candidate: { ...candidate, config: candidate.parsed },
    });
  });

  it.each(["absent", "unparseable", "externally-owned"] as const)(
    "refuses %s recovery input",
    async (kind) => {
      const { root, configPath, io, expectUnchanged } = fixture();
      const backupPath = `${configPath}.bak`;
      const bytes = "{ not JSON5";
      if (kind === "absent") {
        fs.unlinkSync(backupPath);
      } else if (kind === "unparseable") {
        fs.writeFileSync(backupPath, bytes);
      } else {
        io.env.OPENCLAW_CONFIG_READONLY = "1";
      }
      const before = kind === "externally-owned" ? manifest(root) : undefined;
      await expect(prepare(io)).resolves.toBeNull();
      expectUnchanged();
      if (kind === "unparseable") {
        expect(fs.readFileSync(backupPath, "utf8")).toBe(bytes);
      }
      if (kind === "externally-owned") {
        expect(manifest(root)).toEqual(before);
      }
    },
  );

  it.each(["sync-read", "sync-stat", "async-read", "async-stat"] as const)(
    "surfaces %s backup I/O errors",
    async (phase) => {
      const readError = Object.assign(new Error("backup device unavailable"), { code: "EIO" });
      let armed = false;
      const method = {
        "sync-read": "readFileSync",
        "sync-stat": "statSync",
        "async-read": "readFileAsync",
        "async-stat": "statAsync",
      }[phase];
      const { io, expectPreserved } = fixture({
        fs: failingFs(
          ["readFileSync", "statSync", "readFileAsync", "statAsync"],
          (called, [target]) =>
            armed && called === method && String(target).endsWith(".bak") ? readError : undefined,
        ),
      });
      const plan = await recovery(io);
      armed = true;
      await expect(plan.apply()).rejects.toBe(readError);
      expectPreserved();
    },
  );

  it("surfaces unavailable full backup validation", async () => {
    let validations = 0;
    const { io, expectPreserved } = fixture({
      measure: async (name, run) => {
        if (name === "config.snapshot.read.validate" && ++validations === 2) {
          throw Object.assign(new Error("backup validation storage unavailable"), { code: "EIO" });
        }
        return await run();
      },
    });
    await expect(prepare(io)).rejects.toMatchObject({ code: "CONFIG_READ_FAILED" });
    expectPreserved();
  });

  it.each(["live", "completed", "during-apply"] as const)(
    "preserves %s observation",
    async (phase) => {
      const { root, configPath, env, io, expectUnchanged } = fixture();
      const plan = await recovery(io);
      const logger = { warn: vi.fn(), error: vi.fn() };
      const deps = normalizeConfigIoDeps({ env, homedir: () => root, logger });
      using newer = phase === "live" ? captureConfigHealthStateStore(deps, configPath) : undefined;
      const snapshot = await newer?.read();
      if (newer && !snapshot) {
        throw new Error("Expected the newer observation to be current");
      }
      if (phase === "completed") {
        observeConfigSnapshotSync(deps, await io.readConfigFileSnapshot());
      }
      const health = phase === "completed" ? readConfigHealthStateFromStore(deps) : undefined;
      if (phase === "completed") {
        expect(health?.entries?.[configPath]?.lastObservedSuspiciousSignature).toBeTruthy();
        expect(logger.warn).toHaveBeenCalledTimes(1);
      }
      await expect(
        plan.apply(
          phase === "during-apply"
            ? () => {
                patchConfigHealthEntryToStore(deps, configPath, {
                  lastObservedSuspiciousSignature: "newer-observation",
                });
              }
            : undefined,
        ),
      ).rejects.toMatchObject({
        name: "ConfigMutationConflictError",
        retryable: false,
      });
      expectUnchanged();
      if (newer && snapshot) {
        expect(newer.isCurrent()).toBe(true);
        await newer.update({ lastObservedSuspiciousSignature: "newer-observation" }, snapshot);
      }
      if (phase === "completed") {
        expect(readConfigHealthStateFromStore(deps)).toEqual(health);
        expect(logger.warn).toHaveBeenCalledTimes(1);
      } else {
        expect(
          readConfigHealthStateFromStore(deps).entries?.[configPath]
            ?.lastObservedSuspiciousSignature,
        ).toBe("newer-observation");
      }
    },
  );

  it.each(["async", "sync"] as const)("tolerates stat failure (%s)", async (mode) => {
    const { configPath, backup, createIO } = fixture();
    const backupPath = `${configPath}.bak`;
    const statError = Object.assign(new Error("EACCES: stat denied"), { code: "EACCES" });
    const io = createIO({
      observe: true,
      fs: failingFs(["statSync", "statAsync"], (method, [target]) =>
        target === backupPath && ["statSync", "statAsync"].includes(method) ? statError : undefined,
      ),
    });

    const recovered =
      mode === "async"
        ? (await io.readConfigFileSnapshot({ recoverSuspicious: true })).config
        : io.loadConfig();

    expect(recovered.gateway?.mode).toBe("local");
    expect(fs.readFileSync(configPath, "utf8")).toBe(backup);
  });

  it("recovers with unavailable health reads", async () => {
    const capture = healthOwner.captureConfigHealthStateStore;
    const unavailable = (store: ReturnType<typeof capture>): ReturnType<typeof capture> => ({
      ...store,
      read: async () => ({ state: {}, basis: null }),
      captureContinuation: () => unavailable(store.captureContinuation()),
    });
    vi.spyOn(healthOwner, "captureConfigHealthStateStore").mockImplementation((...args) =>
      unavailable(capture(...args)),
    );
    const { root, configPath, env, io, expectRestored } = fixture();
    const plan = await recovery(io);
    await plan.apply();
    expectRestored();
    const health = readConfigHealthStateFromStore({
      env,
      homedir: () => root,
      logger: { warn: vi.fn() },
    });
    expect(health.entries?.[configPath]).toBeUndefined();
  });

  it.each(["full", "core-only"] as const)(
    "previews, then restores (%s)",
    async (pluginValidation) => {
      const { root, configPath, backup, databasePath, env, io, expectRestored } = fixture({
        pluginValidation,
      });
      // No sidecars are excluded: a read must not create even an empty WAL.
      expect(fs.existsSync(`${databasePath}-wal`)).toBe(false);
      const before = manifest(root);
      const plan = await recovery(io);
      expect(plan.snapshot).toMatchObject({
        raw: backup,
        path: configPath,
        valid: true,
        config: {
          gateway: { mode: "local", port: 18720 },
          agents: { defaults: { compaction: { mode: "safeguard" } } },
        },
      });
      expect(env).not.toHaveProperty("RECOVERY_MARKER");
      expect(manifest(root)).toEqual(before);

      await plan.apply();
      expectRestored();
      const persisted = await io.readConfigFileSnapshotWithPluginMetadata();
      expect(persisted.snapshot).toEqual(plan.snapshot);
    },
  );

  it("refuses backup identity drift without writes", async () => {
    const { root, configPath, backup, io } = fixture();
    const plan = await recovery(io);
    const replacement = path.join(root, "replacement");
    fs.writeFileSync(replacement, backup);
    fs.renameSync(replacement, `${configPath}.bak`);
    const beforeApply = manifest(root);
    await expect(plan.apply()).rejects.toThrow("config recovery source changed since preparation");
    expect(manifest(root)).toEqual(beforeApply);
  });

  it.each(["backup", "lease"] as const)("refuses %s archive drift", async (changedSource) => {
    const { root, configPath, original, env, createIO } = fixture();
    const lease =
      changedSource === "lease"
        ? await acquireStartupMigrationLeaseWithWait({ env, timeoutMs: 0 })
        : undefined;
    const changedPath = `${configPath}.bak`;
    const concurrentRaw = '{ "gateway": { "mode": "local", "port": 18721 } }\n';
    const io = createIO({
      fs: {
        ...fs,
        promises: {
          ...fs.promises,
          writeFile: async (pathname, data, options) => {
            await fs.promises.writeFile(pathname, data, options);
            if (typeof pathname === "string" && pathname.startsWith(`${configPath}.clobbered.`)) {
              if (lease) {
                lease.release();
              } else {
                await fs.promises.writeFile(changedPath, concurrentRaw);
              }
            }
          },
        },
      },
    });
    const plan = await recovery(io);
    await expect(plan.apply(lease?.heartbeat)).rejects.toThrow(
      lease
        ? "startup migration lease was lost"
        : "config recovery source changed since preparation",
    );
    if (!lease) {
      expect(fs.readFileSync(changedPath, "utf8")).toBe(concurrentRaw);
    }
    expect(fs.readFileSync(configPath, "utf8")).toBe(original);
    expectClobbered(root, original);
  });

  it.each(["archive", "replacement"])("retains original on %s failure", async (phase) => {
    const warn = vi.fn();
    let configPath = "";
    const error =
      phase === "archive"
        ? Object.assign(new Error("snapshot disk full"), { code: "ENOSPC" })
        : Object.assign(new Error("recovery replacement denied"), { code: "EACCES" });
    const fixtureState = fixture({
      logger: { warn, error: vi.fn() },
      fs: failingFs([phase === "archive" ? "writeFileAsync" : "renameAsync"], (method, args) => {
        const [source, target] = args;
        const fails =
          phase === "archive"
            ? method === "writeFileAsync" &&
              typeof source === "string" &&
              source.includes(".clobbered.")
            : method === "renameAsync" && target === configPath;
        return fails ? error : undefined;
      }),
    });
    configPath = fixtureState.configPath;
    const plan = await recovery(fixtureState.io);
    await expect(plan.apply()).rejects.toThrow(
      phase === "archive" ? "Config backup recovery deferred" : error.message,
    );
    if (phase === "archive") {
      fixtureState.expectPreserved();
      expect(warn).toHaveBeenCalledExactlyOnceWith(expect.stringContaining("recovery deferred"));
    } else {
      expect(fixtureState.read()).toBe(fixtureState.original);
      expect(clobberFiles(fixtureState.root)).toHaveLength(1);
    }
  });
});
