import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import JSON5 from "json5";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { prepareLegacyConfigMigrationRuntime } from "../commands/doctor/shared/legacy-config-migrate.test-support.js";
import { AsyncWorkScope } from "../shared/async-work-scope.js";
import { createDeferredCore } from "../shared/deferred.js";
import {
  closeOpenClawStateDatabaseAsync,
  closeOpenClawStateDatabaseForTest,
} from "../state/openclaw-state-db.js";
import * as configAudit from "./io.audit.js";
import { listConfigAuditRecordsForTests } from "./io.audit.test-support.js";
import {
  readConfigHealthStateFromStore as readHealth,
  patchConfigHealthEntryToStore as patchHealth,
} from "./io.health-state.js";
import { createConfigIO } from "./io.js";
import {
  maybeRecoverSuspiciousConfigRead as recoverAsync,
  maybeRecoverSuspiciousConfigReadSync as recoverSync,
  promoteConfigSnapshotToLastKnownGoodCore as promote,
  recoverConfigFromLastKnownGoodCore as restoreLastGood,
} from "./io.observe-recovery.js";
import {
  clobberedUpdateChannelConfig,
  clobberedUpdateChannelRaw,
  largeRecoverableCoreConfig,
  recoverableCoreConfig,
  recoverableTelegramConfig,
} from "./io.observe-recovery.test-support.js";
import * as configObserveState from "./io.observe-state.js";
import { createConfigIoWorkerFixture } from "./io.worker.test-support.js";
import type { ConfigFileSnapshot } from "./types.js";

type ObserveRecoveryDeps = Parameters<typeof recoverAsync>[0]["deps"];
const approveRecoveryCandidate = <T extends { raw: string; parsed: unknown }>(candidate: T) => ({
  ok: true as const,
  candidate,
});

function lastGoodPath(configPath: string): string {
  return `${configPath}.last-good`;
}

function failingRecoveryFs(
  mode: string,
  operation: "chmod" | "rename",
  configPath: string,
  error: Error,
) {
  const method = `${operation}${mode === "async" ? "" : "Sync"}`;
  function intercept<T extends object>(owner: T): T {
    return new Proxy(owner, {
      get(target, key, receiver) {
        const original: unknown = Reflect.get(target, key, receiver);
        if (key !== method || typeof original !== "function") {
          return original;
        }
        return (...args: unknown[]) => {
          if (args[operation === "rename" ? 1 : 0] !== configPath) {
            return Reflect.apply(original, target, args);
          }
          if (mode === "async") {
            return Promise.reject(error);
          }
          throw error;
        };
      },
    });
  }
  return mode === "async" ? { ...fs, promises: intercept(fs.promises) } : intercept(fs);
}

describe("config observe recovery", () => {
  let fixtureRoot = "";
  let homeCaseId = 0;
  let restoreMigrationRuntime: (() => void) | undefined;
  const workerFixture = createConfigIoWorkerFixture();

  let home = "";
  beforeEach(async () => {
    home = path.join(fixtureRoot, `case-${homeCaseId++}`);
    await fsp.mkdir(home, { recursive: true });
  });

  beforeAll(async () => {
    fixtureRoot = await fsp.mkdtemp(path.join(os.tmpdir(), "openclaw-config-observe-recovery-"));
    await workerFixture.setup(fixtureRoot);
    restoreMigrationRuntime = await prepareLegacyConfigMigrationRuntime();
  });

  afterAll(async () => {
    restoreMigrationRuntime?.();
    await workerFixture.close();
    closeOpenClawStateDatabaseForTest();
    await fsp.rm(fixtureRoot, { recursive: true, force: true });
  });

  afterEach(async () => {
    await closeOpenClawStateDatabaseAsync();
    closeOpenClawStateDatabaseForTest();
  });

  async function seedConfigBackup(configPath: string, config: Record<string, unknown>) {
    await fsp.mkdir(path.dirname(configPath), { recursive: true });
    const { raw } = await writeConfigRaw(configPath, config);
    await fsp.copyFile(configPath, `${configPath}.bak`);
    return raw;
  }

  async function writeConfigRaw(configPath: string, config: Record<string, unknown>) {
    const raw = `${JSON.stringify(config, null, 2)}\n`;
    await fsp.writeFile(configPath, raw, "utf-8");
    return { raw, parsed: config };
  }

  async function writeClobberedUpdateChannel(configPath: string) {
    await fsp.writeFile(configPath, clobberedUpdateChannelRaw, "utf-8");
    return {
      raw: clobberedUpdateChannelRaw,
      parsed: clobberedUpdateChannelConfig,
    };
  }

  async function observeEvents(auditPath: string): Promise<Record<string, unknown>[]> {
    const stateDir = path.dirname(path.dirname(auditPath));
    return listConfigAuditRecordsForTests({
      env: { OPENCLAW_STATE_DIR: stateDir },
      homedir: () => stateDir,
    }).filter((event) => event.event === "config.observe");
  }

  async function clobbers(configPath: string) {
    const prefix = `${path.basename(configPath)}.clobbered.`;
    return (await fsp.readdir(path.dirname(configPath))).filter((entry) =>
      entry.startsWith(prefix),
    );
  }

  async function expectPathMissing(targetPath: string) {
    await expect(fsp.stat(targetPath)).rejects.toMatchObject({ code: "ENOENT" });
  }

  async function expectRaw(configPath: string, raw: string) {
    expect(await fsp.readFile(configPath, "utf8")).toBe(raw);
  }

  async function expectUntouched(configPath: string, raw: string) {
    await expectRaw(configPath, raw);
    expect(await clobbers(configPath)).toEqual([]);
  }

  async function recover(mode: string, input: Parameters<typeof recoverAsync>[0]) {
    const work = new AsyncWorkScope();
    try {
      return await work.run(() => (mode === "async" ? recoverAsync(input) : recoverSync(input)));
    } finally {
      await work.drain();
    }
  }

  function warnMessages(warn: ReturnType<typeof vi.fn>): string[] {
    return warn.mock.calls.map(([message]) => String(message));
  }

  function expectWarnContaining(warn: ReturnType<typeof vi.fn>, expected: string) {
    expect(warnMessages(warn).join("\n")).toContain(expected);
  }

  function expectWarnNotContaining(warn: ReturnType<typeof vi.fn>, expected: string) {
    expect(warnMessages(warn).join("\n")).not.toContain(expected);
  }

  async function lastEvent(auditPath: string): Promise<Record<string, unknown> | undefined> {
    return (await observeEvents(auditPath)).at(-1);
  }

  function createTestConfigIO(
    fixtureHome: string,
    warn = vi.fn(),
    options: { env?: NodeJS.ProcessEnv; observe?: boolean } = {},
  ) {
    const configPath = path.join(fixtureHome, ".openclaw", "openclaw.json");
    const error = vi.fn();
    // Keep recovery validation out of host/workspace plugin state. Preserve the
    // caller's env identity because rollback tests inspect that exact object.
    const env = options.env ?? ({} as NodeJS.ProcessEnv);
    env.HOME ??= fixtureHome;
    env.USERPROFILE ??= fixtureHome;
    env.OPENCLAW_CONFIG_PATH ??= configPath;
    env.OPENCLAW_STATE_DIR ??= path.join(fixtureHome, ".openclaw");
    env.OPENCLAW_DISABLE_BUNDLED_PLUGINS ??= "1";
    env.VITEST ??= "true";
    const deps = { fs, json5: JSON5, env, homedir: () => fixtureHome, logger: { warn, error } };
    return {
      configPath,
      warn,
      deps,
      io: createConfigIO({ ...deps, configPath, observe: options.observe }),
    };
  }

  async function clobberedIO(
    backup: Record<string, unknown>,
    options: Parameters<typeof createTestConfigIO>[2] = {},
    current: Record<string, unknown> = { meta: { lastTouchedVersion: "2026.5.28" } },
  ) {
    const fixture = createTestConfigIO(home, vi.fn(), options);
    const backupRaw = await seedConfigBackup(fixture.configPath, backup);
    const clobbered = await writeConfigRaw(fixture.configPath, current);
    return { ...fixture, backupRaw, clobbered };
  }

  async function clobberedDeps(backup: Record<string, unknown> = recoverableTelegramConfig) {
    const fixture = makeDeps(home);
    const backupRaw = await seedConfigBackup(fixture.configPath, backup);
    const clobbered = await writeClobberedUpdateChannel(fixture.configPath);
    return {
      ...fixture,
      backupRaw,
      clobbered,
      input: {
        deps: fixture.deps,
        configPath: fixture.configPath,
        ...clobbered,
        prepareBackup: approveRecoveryCandidate,
      },
    };
  }

  async function brokenLastGood(config: Record<string, unknown> = recoverableCoreConfig) {
    const fixture = makeDeps(home);
    const snapshot = await makeSnapshot(fixture.configPath, config);
    await expect(
      promote({
        deps: fixture.deps,
        snapshot,
        logger: fixture.deps.logger,
      }),
    ).resolves.toBe(true);
    const brokenRaw = "{ gateway: { mode: 123 } }\n";
    await fsp.writeFile(fixture.configPath, brokenRaw);
    return {
      ...fixture,
      snapshot,
      brokenRaw,
      input: {
        deps: fixture.deps,
        snapshot: invalidSnapshot(snapshot, brokenRaw),
        reason: "test-invalid-config",
        prepareCandidate: approveRecoveryCandidate,
      },
    };
  }

  async function makeSnapshot(configPath: string, config: Record<string, unknown>) {
    await fsp.mkdir(path.dirname(configPath), { recursive: true });
    const { raw } = await writeConfigRaw(configPath, config);
    return {
      path: configPath,
      exists: true,
      raw,
      parsed: config,
      sourceConfig: config,
      resolved: config,
      valid: true,
      runtimeConfig: config,
      config,
      issues: [],
      warnings: [],
      legacyIssues: [],
    } satisfies ConfigFileSnapshot;
  }

  function invalidSnapshot(snapshot: ConfigFileSnapshot, raw: string): ConfigFileSnapshot {
    return {
      ...snapshot,
      raw,
      parsed: { gateway: { mode: 123 } },
      valid: false,
      issues: [{ path: "gateway.mode", message: "Expected string" }],
    };
  }

  function makeDeps(fixtureHome: string, warn = vi.fn()) {
    const configPath = path.join(fixtureHome, ".openclaw", "openclaw.json");
    return {
      deps: {
        fs,
        json5: JSON5,
        env: {} as NodeJS.ProcessEnv,
        homedir: () => fixtureHome,
        logger: { warn },
      },
      configPath,
      auditPath: path.join(fixtureHome, ".openclaw", "logs", "config-audit.jsonl"),
      warn,
    };
  }

  it.each(["async", "sync"] as const)("warns on backup chmod failure (%s)", async (mode) => {
    const { deps, configPath, warn, input } = await clobberedDeps();
    const error = Object.assign(new Error("EPERM: chmod denied"), { code: "EPERM" });
    const recovered = await recover(mode, {
      ...input,
      deps: { ...deps, fs: failingRecoveryFs(mode, "chmod", configPath, error) },
    });
    expect(recovered.parsed).toEqual(recoverableTelegramConfig);
    expectWarnContaining(
      warn,
      `Config permission hardening failed (backup restore): ${configPath}: EPERM: chmod denied`,
    );
    expectWarnContaining(warn, `Config auto-restored from backup: ${configPath}`);
  });

  it("rereads committed backup after health admission closes", async () => {
    const { io, configPath, warn, backupRaw } = await clobberedIO(largeRecoverableCoreConfig);
    const auditPath = path.join(home, ".openclaw", "logs", "config-audit.jsonl");
    const captureAppender = configAudit.captureConfigAuditAppender;
    let closedAfterRestore = false;
    const audit = vi
      .spyOn(configAudit, "captureConfigAuditAppender")
      .mockImplementation((...params) => {
        const append = captureAppender(...params);
        return async (record) => {
          await append(record);
          if (
            !closedAfterRestore &&
            record.event === "config.observe" &&
            record.restoredFromBackup
          ) {
            expect(await fsp.readFile(configPath, "utf-8")).toBe(backupRaw);
            closedAfterRestore = true;
            await closeOpenClawStateDatabaseAsync();
          }
        };
      });
    try {
      const snapshot = await io.readConfigFileSnapshot({ recoverSuspicious: true });
      expect(closedAfterRestore).toBe(true);
      expect(snapshot).toMatchObject({
        valid: true,
        raw: backupRaw,
        config: {
          gateway: {
            mode: "local",
          },
        },
      });
      expect(snapshot.config.gateway?.trustedProxies).toEqual(
        largeRecoverableCoreConfig.gateway.trustedProxies,
      );
      expect(await fsp.readFile(configPath, "utf-8")).toBe(backupRaw);
      expectWarnContaining(warn, "Config health-state write failed:");
      const events = await observeEvents(auditPath);
      expect(events).toEqual([expect.objectContaining({ restoredFromBackup: true })]);
      await closeOpenClawStateDatabaseAsync();
      expect((await io.readConfigFileSnapshot({ recoverSuspicious: true })).raw).toBe(backupRaw);
    } finally {
      audit.mockRestore();
    }
  });

  it.each(["load", "snapshot"])("%s recovery clears discarded config env", async (mode) => {
    const env = {} as NodeJS.ProcessEnv;
    const { io } = await clobberedIO(
      recoverableCoreConfig,
      { env },
      {
        meta: { lastTouchedVersion: "2026.5.28" },
        env: { vars: { OPENCLAW_CLOBBER_ONLY: "bad" } },
      },
    );

    const config =
      mode === "load"
        ? io.loadConfig()
        : (await io.readConfigFileSnapshot({ recoverSuspicious: true })).config;

    expect(config.gateway?.mode).toBe("local");
    expect(env.OPENCLAW_CLOBBER_ONLY).toBeUndefined();
  });

  it("does not recover with observation disabled", async () => {
    const { io, configPath, clobbered } = await clobberedIO(recoverableCoreConfig, {
      observe: false,
    });
    const auditPath = path.join(home, ".openclaw", "logs", "config-audit.jsonl");

    const snapshot = await io.readConfigFileSnapshot({ recoverSuspicious: true });

    expect(snapshot.valid).toBe(true);
    expect(snapshot.config.gateway?.mode).toBeUndefined();
    await expect(fsp.readFile(configPath, "utf-8")).resolves.toBe(clobbered.raw);
    await expectPathMissing(auditPath);
  });

  it("does not replace include roots with stale backups", async () => {
    const { io, configPath } = createTestConfigIO(home);
    const auditPath = path.join(home, ".openclaw", "logs", "config-audit.jsonl");
    const includedConfig = largeRecoverableCoreConfig;
    await seedConfigBackup(configPath, includedConfig);
    await fsp.writeFile(
      path.join(path.dirname(configPath), "base.json5"),
      `${JSON.stringify(includedConfig, null, 2)}\n`,
      "utf-8",
    );
    const includeRootRaw = `{\n  "$include": "./base.json5"\n}\n`;
    await fsp.writeFile(configPath, includeRootRaw, "utf-8");

    const snapshot = await io.readConfigFileSnapshot({ recoverSuspicious: true });

    expect(snapshot.valid).toBe(true);
    expect(snapshot.config.gateway?.mode).toBe("local");
    await expect(fsp.readFile(configPath, "utf-8")).resolves.toBe(includeRootRaw);
    const observe = await lastEvent(auditPath);
    expect(observe?.restoredFromBackup).toBe(false);
  });

  it.each([
    { kind: "bind alias", backup: { gateway: { mode: "local", bind: "localhost" } } },
    {
      kind: "OTel grpc",
      backup: {
        gateway: { mode: "local" },
        diagnostics: { otel: { enabled: true, protocol: "grpc", traces: true } },
      },
    },
    {
      kind: "invalid model",
      backup: {
        gateway: { mode: "local" },
        env: { vars: { OPENCLAW_BACKUP_ONLY: "stale" } },
        agents: { defaults: { model: 123 } },
      },
    },
  ])("leaves $kind backup repair to Doctor without leaking env", async ({ kind, backup }) => {
    const env: NodeJS.ProcessEnv = {};
    const {
      io,
      configPath,
      warn,
      backupRaw: seededRaw,
      clobbered,
    } = await clobberedIO(backup, { env });
    const backupRaw =
      kind === "bind alias"
        ? '{\n  // historical bind alias\n  gateway: { mode: "local", bind: "localhost" }\n}\n'
        : seededRaw;
    if (kind === "bind alias") {
      await fsp.writeFile(`${configPath}.bak`, backupRaw);
    }
    const snapshot =
      kind === "OTel grpc"
        ? undefined
        : await io.readConfigFileSnapshot({ recoverSuspicious: true });
    const config = snapshot?.config ?? (await io.loadConfigAsync());
    if (snapshot) {
      expect(snapshot.valid).toBe(true);
    }
    expect(config.gateway?.mode).toBeUndefined();
    if (kind === "OTel grpc") {
      expect(config.diagnostics?.otel).toBeUndefined();
    }
    await expectUntouched(configPath, clobbered.raw);
    await expectRaw(`${configPath}.bak`, backupRaw);
    expect(env.OPENCLAW_BACKUP_ONLY).toBeUndefined();
    if (kind === "bind alias") {
      expectWarnNotContaining(warn, "Config auto-restored");
    }
  });

  it("passes resolved backups to caller policy", async () => {
    const { io, configPath } = createTestConfigIO(home);
    await fsp.mkdir(path.dirname(configPath), { recursive: true });
    await fsp.writeFile(
      path.join(path.dirname(configPath), "future-meta.json5"),
      '{ meta: { lastTouchedVersion: "9999.1.1" } }\n',
      "utf-8",
    );
    await seedConfigBackup(configPath, {
      $include: "./future-meta.json5",
      gateway: { mode: "local" },
    });
    const clobbered = await writeConfigRaw(configPath, {});
    let candidateVersion: string | undefined;
    let currentConfig: Record<string, unknown> | undefined;

    const allowSuspiciousRecovery = vi.fn(
      (candidate: ConfigFileSnapshot["config"], current: ConfigFileSnapshot["config"]) => {
        candidateVersion = candidate.meta?.lastTouchedVersion;
        currentConfig = current;
        return false;
      },
    );
    await io.readConfigFileSnapshot({ recoverSuspicious: true, allowSuspiciousRecovery });
    await io.readConfigFileSnapshot({ recoverSuspicious: true, allowSuspiciousRecovery });
    expect(allowSuspiciousRecovery).toHaveBeenCalledTimes(2);
    expect(await clobbers(configPath)).toHaveLength(0);

    expect(candidateVersion).toBe("9999.1.1");
    expect(currentConfig).toBeDefined();
    expect(currentConfig?.meta).toBeUndefined();
    await expect(fsp.readFile(configPath, "utf-8")).resolves.toBe(clobbered.raw);
  });

  it("refuses missing gateway mode despite stale health", async () => {
    const { deps, configPath, auditPath } = makeDeps(home);
    const snapshot = await makeSnapshot(configPath, recoverableTelegramConfig);
    await promote({ deps, snapshot, logger: deps.logger });
    await fsp.writeFile(
      `${configPath}.bak`,
      `${JSON.stringify({ meta: { lastTouchedVersion: "2026.4.22" } })}\n`,
      "utf-8",
    );
    const clobbered = await writeClobberedUpdateChannel(configPath);
    const prepareBackup = vi.fn(approveRecoveryCandidate);
    const input = { deps, configPath, ...clobbered, prepareBackup };

    const recovered = await recoverAsync(input);

    expect(prepareBackup).not.toHaveBeenCalled();
    expect(recovered).toEqual(clobbered);
    await expect(fsp.readFile(configPath, "utf-8")).resolves.toBe(clobbered.raw);
    await expect(observeEvents(auditPath)).resolves.toEqual([]);
  });

  it.each(["async", "sync"] as const)("defers locked backup recovery (%s)", async (mode) => {
    const { configPath, auditPath, warn, backupRaw, clobbered, input } =
      await clobberedDeps(recoverableCoreConfig);
    const lockPath = `${configPath}.clobber.lock`;
    await fsp.mkdir(lockPath);
    await recover(mode, input);

    await expectUntouched(configPath, clobbered.raw);
    expect(await observeEvents(auditPath)).toEqual([]);
    expect(warn).toHaveBeenCalledExactlyOnceWith(expect.stringContaining("recovery deferred"));
    await fsp.rmdir(lockPath);

    await recover(mode, input);

    expect(await fsp.readFile(configPath, "utf-8")).toBe(backupRaw);
    expect(await clobbers(configPath)).toHaveLength(1);
    expect((await observeEvents(auditPath)).at(-1)?.restoredFromBackup).toBe(true);
  });

  it("defers locked prefix recovery", async () => {
    const { io, configPath, warn } = createTestConfigIO(home, vi.fn(), { observe: false });
    const cleanRaw = `${JSON.stringify({ gateway: { mode: "local" } })}\n`;
    const pollutedRaw = `status output\n${cleanRaw}`;
    await fsp.mkdir(path.dirname(configPath), { recursive: true });
    await fsp.writeFile(configPath, pollutedRaw);
    const snapshot = await io.readConfigFileSnapshot();
    expect(snapshot.valid).toBe(false);
    const lockPath = `${configPath}.clobber.lock`;
    await fsp.mkdir(lockPath);

    await expect(io.recoverConfigFromJsonRootSuffix(snapshot)).resolves.toBe(false);

    await expectUntouched(configPath, pollutedRaw);
    expect(warn).toHaveBeenCalledExactlyOnceWith(expect.stringContaining("recovery deferred"));
    await fsp.rmdir(lockPath);

    await expect(io.recoverConfigFromJsonRootSuffix(snapshot)).resolves.toBe(true);
    expect(await fsp.readFile(configPath, "utf-8")).toBe(cleanRaw);
    const clobbered = await clobbers(configPath);
    expect(clobbered).toHaveLength(1);
    expect(await fsp.readFile(path.join(path.dirname(configPath), clobbered[0]!), "utf-8")).toBe(
      pollutedRaw,
    );
  });

  it("defers locked last-good recovery", async () => {
    const { deps, configPath, auditPath, warn, snapshot, brokenRaw, input } = await brokenLastGood({
      gateway: { mode: "local", auth: { mode: "token", token: "secret-token" } },
      channels: { discord: { enabled: true, dmPolicy: "pairing" } },
    });
    await expectRaw(lastGoodPath(configPath), snapshot.raw);
    const healthBefore = readHealth(deps);
    const lockPath = `${configPath}.clobber.lock`;
    await fsp.mkdir(lockPath);

    await expect(restoreLastGood(input)).resolves.toBe(false);

    await expectUntouched(configPath, brokenRaw);
    expect(await observeEvents(auditPath)).toEqual([]);
    expect(readHealth(deps)).toEqual(healthBefore);
    expect(warn).toHaveBeenCalledExactlyOnceWith(expect.stringContaining("recovery deferred"));
    await fsp.rmdir(lockPath);

    await expect(restoreLastGood(input)).resolves.toBe(true);
    await expectRaw(configPath, snapshot.raw);
    expectWarnContaining(warn, "Config auto-restored from last-known-good:");
    expectWarnContaining(warn, "Rejected validation details: gateway.mode: Expected string.");
    expect(await lastEvent(auditPath)).toMatchObject({
      restoredFromBackup: true,
      restoredBackupPath: lastGoodPath(configPath),
    });
  });

  it("recovery uses canonical metadata fingerprints", async () => {
    const { deps, configPath } = makeDeps(home);
    const backup = { meta: { authoredBy: "operator" }, gateway: { mode: "local" } };
    await seedConfigBackup(configPath, backup);
    const clobbered = await writeConfigRaw(configPath, { gateway: { mode: "local" } });
    const input = { deps, configPath, ...clobbered, prepareBackup: approveRecoveryCandidate };

    const recovered = await recoverAsync(input);

    expect(recovered.parsed).toEqual(backup);
  });

  it.each(["async", "sync"] as const)("retries failed atomic replacement (%s)", async (mode) => {
    const { deps, configPath, auditPath, warn, clobbered, input } = await clobberedDeps();
    if (process.platform !== "win32") {
      await fsp.chmod(configPath, 0o644);
    }
    const copyError = Object.assign(new Error("EACCES: permission denied"), { code: "EACCES" });
    const retry = (recoveryDeps: ObserveRecoveryDeps) =>
      recover(mode, { ...input, deps: recoveryDeps });
    const recovered = await retry({
      ...deps,
      fs: failingRecoveryFs(mode, "rename", configPath, copyError),
    });

    expect((recovered.parsed as { gateway?: { mode?: string } }).gateway?.mode).toBe("local");
    await expect(fsp.readFile(configPath, "utf-8")).resolves.toBe(clobbered.raw);
    expectWarnContaining(warn, "Config auto-restore from backup failed:");
    expectWarnNotContaining(warn, "Config auto-restored from backup:");
    if (mode === "sync") {
      expectWarnContaining(warn, "EACCES: permission denied");
    }

    const firstEvents = await observeEvents(auditPath);
    expect(firstEvents).toEqual([
      expect.objectContaining({
        restoredFromBackup: false,
        valid: false,
        restoreErrorCode: "EACCES",
        restoreErrorMessage: "EACCES: permission denied",
      }),
    ]);
    const retryResult = await retry(deps);
    expect((retryResult.parsed as { gateway?: { mode?: string } }).gateway?.mode).toBe("local");
    await expect(fsp.readFile(configPath, "utf-8")).resolves.not.toBe(clobbered.raw);
    const retryEvents = await observeEvents(auditPath);
    expect(retryEvents).toHaveLength(2);
    expect(retryEvents[1]?.restoredFromBackup).toBe(true);
    if (process.platform !== "win32") {
      expect((await fsp.stat(configPath)).mode & 0o777).toBe(0o600);
    }
  });

  it("restores the exact backup bytes approved by preparation", async () => {
    const { configPath, input, backupRaw: approvedRaw } = await clobberedDeps();
    const backupPath = `${configPath}.bak`;
    const replacementRaw = `${JSON.stringify({ gateway: { mode: "remote" } }, null, 2)}\n`;

    await recoverAsync({
      ...input,
      prepareBackup: (candidate) => {
        fs.writeFileSync(backupPath, replacementRaw, "utf-8");
        return { ok: true, candidate };
      },
    });

    await expect(fsp.readFile(backupPath, "utf-8")).resolves.toBe(replacementRaw);
    await expect(fsp.readFile(configPath, "utf-8")).resolves.toBe(approvedRaw);
  });

  it.each([
    { mode: "OPENCLAW_CONFIG_READONLY", operation: "promotion" },
    { mode: "OPENCLAW_NIX_MODE", operation: "restoration" },
  ])(
    "$mode skips last-known-good $operation without source writes",
    async ({ mode, operation }) => {
      const { deps, configPath } = makeDeps(home);
      const snapshot = await makeSnapshot(configPath, recoverableCoreConfig);
      if (operation === "restoration") {
        await expect(promote({ deps, snapshot })).resolves.toBe(true);
      }
      deps.env[mode] = "1";
      if (operation === "promotion") {
        await expect(promote({ deps, snapshot })).resolves.toBe(false);
        await expectPathMissing(lastGoodPath(configPath));
        expect(await fsp.readFile(configPath, "utf-8")).toBe(snapshot.raw);
      } else {
        const brokenRaw = "{ gateway: { mode: 123 } }\n";
        await fsp.writeFile(configPath, brokenRaw, "utf-8");
        await expect(
          restoreLastGood({
            deps,
            snapshot: invalidSnapshot(snapshot, brokenRaw),
            reason: "test-readonly-config",
            prepareCandidate: approveRecoveryCandidate,
          }),
        ).resolves.toBe(false);
        expect(await fsp.readFile(configPath, "utf-8")).toBe(brokenRaw);
        expect(await fsp.readFile(lastGoodPath(configPath), "utf-8")).toBe(snapshot.raw);
      }
      await expect(clobbers(configPath)).resolves.toHaveLength(0);
    },
  );

  it("preserves active config on last-good policy rejection", async () => {
    const { configPath, warn, brokenRaw, input } = await brokenLastGood({
      gateway: { mode: "local" },
    });
    const restored = await restoreLastGood({
      ...input,
      prepareCandidate: () => ({
        ok: false,
        reason: "candidate cannot converge under the current schema",
      }),
    });

    expect(restored).toBe(false);
    await expect(fsp.readFile(configPath, "utf-8")).resolves.toBe(brokenRaw);
    await expect(clobbers(configPath)).resolves.toHaveLength(0);
    expectWarnContaining(warn, "candidate cannot converge under the current schema");
  });

  it("leaves plugin schema evolution to Doctor", async () => {
    const { deps, configPath, warn } = makeDeps(home);
    const staleSnapshot = await makeSnapshot(configPath, recoverableCoreConfig);
    expect(await promote({ deps, snapshot: staleSnapshot })).toBe(true);
    const active = await writeConfigRaw(configPath, {
      gateway: { mode: "local" },
      plugins: { entries: { "lossless-claw": { config: { cacheAwareCompaction: true } } } },
    });
    expect(
      await restoreLastGood({
        deps,
        snapshot: {
          ...staleSnapshot,
          ...active,
          valid: false,
          issues: [
            {
              path: "plugins.entries.lossless-claw.config.cacheAwareCompaction",
              message: "invalid config: must NOT have additional properties",
            },
          ],
        },
        reason: "reload-invalid-config",
        prepareCandidate: approveRecoveryCandidate,
      }),
    ).toBe(false);
    expect(await fsp.readFile(configPath, "utf-8")).toBe(active.raw);
    expectWarnContaining(warn, "Config last-known-good recovery skipped");
  });

  it("refuses to promote redacted secret placeholders", async () => {
    const warn = vi.fn();
    const { deps, configPath } = makeDeps(home, warn);
    const snapshot = await makeSnapshot(configPath, {
      gateway: { mode: "local", auth: { mode: "token", token: "***" } },
    });

    await expect(promote({ deps, snapshot, logger: deps.logger })).resolves.toBe(false);
    await expectPathMissing(lastGoodPath(configPath));
    expectWarnContaining(warn, "Config last-known-good promotion skipped");
  });
  it("preserves concurrent promotions across configs", async () => {
    const first = createTestConfigIO(home);
    const secondPath = path.join(home, ".openclaw", "second.json");
    const options = first.deps;
    await fsp.mkdir(path.dirname(first.configPath), { recursive: true });
    const raw = JSON.stringify({
      meta: { lastTouchedVersion: "2026.9.4" },
      gateway: { mode: "local" },
    });
    await fsp.writeFile(first.configPath, raw);
    await fsp.writeFile(secondPath, raw);
    const readUnobserved = (configPath: string) =>
      createConfigIO({ ...options, configPath, observe: false }).readConfigFileSnapshot();
    const snapshotA = await readUnobserved(first.configPath);
    const snapshotB = await readUnobserved(secondPath);
    const old = configObserveState.createConfigHealthFingerprint({
      raw,
      parsed: snapshotA.parsed,
      stat: fs.statSync(first.configPath),
      observedAt: "2000-01-01T00:00:00.000Z",
    });
    patchHealth(options, first.configPath, { lastPromotedGood: old });
    patchHealth(options, secondPath, {
      lastKnownGood: old,
      lastPromotedGood: old,
    });
    const entered = createDeferredCore();
    const release = createDeferredCore();
    const readFingerprint = configObserveState.readConfigFingerprintForPath;
    const spy = vi
      .spyOn(configObserveState, "readConfigFingerprintForPath")
      .mockImplementation(async (deps, candidatePath) => {
        const result = await readFingerprint(deps, candidatePath);
        if (candidatePath === `${first.configPath}.bak`) {
          entered.resolve();
          await release.promise;
        }
        return result;
      });
    const pending = first.io.readConfigFileSnapshot();
    try {
      await Promise.race([
        entered.promise,
        pending.then(() => {
          throw new Error("Observation completed before its backup read");
        }),
      ]);
      expect(await first.io.promoteConfigSnapshotToLastKnownGood(snapshotA)).toBe(true);
      expect(
        await createConfigIO({
          ...options,
          configPath: secondPath,
        }).promoteConfigSnapshotToLastKnownGood(snapshotB),
      ).toBe(true);
      const promoted = readHealth(options);
      expect(promoted.entries?.[first.configPath]?.lastPromotedGood).not.toEqual(old);
      release.resolve();
      expect((await pending).valid).toBe(true);
      await closeOpenClawStateDatabaseAsync();
      const settled = readHealth(options);
      expect(settled.entries?.[first.configPath]?.lastPromotedGood).toEqual(
        promoted.entries?.[first.configPath]?.lastPromotedGood,
      );
      expect(settled.entries?.[secondPath]).toEqual(promoted.entries?.[secondPath]);
      expect(settled.entries?.[first.configPath]?.lastObservedSuspiciousSignature).toBeNull();
    } finally {
      release.resolve();
      await pending;
      spy.mockRestore();
    }
  });
});
