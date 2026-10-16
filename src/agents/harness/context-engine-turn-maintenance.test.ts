import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { describe, expect, it, vi } from "vitest";
import { upsertSessionEntryCore } from "../../config/sessions/session-accessor.js";
import { LegacyContextEngine } from "../../context-engine/legacy.js";
import { registerContextEngineInRegistry } from "../../context-engine/registry.js";
import type { ContextEngine } from "../../context-engine/types.js";
import { createEmptyPluginRegistry } from "../../plugins/registry-empty.js";
import { PluginRegistryInspectionResources } from "../../plugins/registry-inspection-resources.js";
import { retireInspectionInstances } from "../../plugins/registry-inspection.test-support.js";
import { withPluginRuntimeRegistryScope } from "../../plugins/runtime/gateway-request-scope.js";
import { createPluginRecord } from "../../plugins/status.test-helpers.js";
import { resetCommandQueueStateForTest } from "../../process/command-queue.test-support.js";
import { createUserTurnTranscriptRecorder } from "../../sessions/user-turn-transcript.js";
import { createDeferredCore } from "../../shared/deferred.js";
import { openOpenClawAgentDatabase } from "../../state/openclaw-agent-db.js";
import { withStateDirEnv } from "../../test-helpers/state-dir-env.js";
import { waitForDeferredTurnMaintenanceForSession } from "../embedded-agent-runner/context-engine-maintenance.js";
import { createContextEngineLogicalTurnLease } from "./context-engine-logical-turn.js";
import {
  drainPendingContextEngineTurnsBeforeRun,
  finalizeAcceptedContextEngineTurn,
  type ContextEngineTurnAttemptFacts,
} from "./context-engine-turn-attempt.js";
import { appendTurnMessage, createTurnFacts } from "./context-engine-turn.test-support.js";

const unchanged = { changed: false, bytesFreed: 0, rewrittenEntries: 0 };
let fixtureSequence = 0;

type TurnOptions = {
  outcome?: "committed" | "duplicate" | "failed";
  maintenanceFails?: boolean;
  maintenanceInfo?: Pick<ContextEngine["info"], "turnMaintenanceMode">;
  deferAdmission?: boolean;
};

// The real lease must retain the engine's resource through deferred maintenance.
async function withAcceptedTurn(
  run: (fixture: Awaited<ReturnType<typeof createAcceptedTurn>>) => Promise<void>,
  options: TurnOptions = {},
) {
  await withStateDirEnv("openclaw-accepted-turn-maintenance-", async ({ stateDir }) => {
    resetCommandQueueStateForTest();
    const fixture = await createAcceptedTurn(stateDir, options);
    try {
      await run(fixture);
    } finally {
      fixture.releaseMaintenance.resolve();
      await waitForDeferredTurnMaintenanceForSession(fixture.facts.sessionKey);
      await fixture.lease.dispose();
      await fixture.source.release();
      await fixture.sourceDisposed.promise;
      resetCommandQueueStateForTest();
    }
  });
}

// Each fixture has a distinct registration so a deliberately failed hook's
// quarantine cannot change another case's selected engine.
async function createAcceptedTurn(
  stateDir: string,
  {
    outcome = "committed",
    maintenanceFails = false,
    maintenanceInfo = { turnMaintenanceMode: "background" },
    deferAdmission = false,
  }: TurnOptions,
) {
  const engineId = `accepted-maintenance-${fixtureSequence++}`;
  const target = {
    agentId: "main",
    sessionId: engineId,
    sessionKey: `agent:main:${engineId}`,
    storePath: path.join(stateDir, "sessions.json"),
  };
  await upsertSessionEntryCore(target, { sessionId: target.sessionId, updatedAt: 1 });
  const admitted = await appendTurnMessage(target, "user", "Synthetic question", 1_000);
  const terminal = await appendTurnMessage(
    target,
    "assistant",
    "Synthetic answer",
    2_000,
    admitted?.messageId,
  );
  if (!admitted?.anchor || !terminal?.anchor) {
    throw new Error("Expected a durable accepted transcript range");
  }
  const admission = {
    ...admitted.anchor,
    logicalTurnId: `${engineId}-turn`,
    role: "user" as const,
  };
  const database = openOpenClawAgentDatabase({ agentId: "main", path: admission.storePath });
  const facts: ContextEngineTurnAttemptFacts = {
    ...createTurnFacts({ admission, terminal: terminal.anchor }, target),
    runtimeContext: { provider: "fixture", modelId: "fixture-model", tokenBudget: 900 },
  };

  // The logical lease must retain this database after its supplying inspection
  // retires, including while deferred maintenance is waiting on external work.
  const probe = new DatabaseSync(path.join(stateDir, "engine-resource.sqlite"));
  const source = new PluginRegistryInspectionResources(retireInspectionInstances);
  const registry = createEmptyPluginRegistry();
  registry.plugins.push(
    createPluginRecord({ id: engineId, source: path.join(stateDir, "plugin") }),
  );
  source.attach(registry);
  const sourceDisposed = createDeferredCore();
  source.register(engineId, {
    id: "probe",
    dispose() {
      probe.close();
      sourceDisposed.resolve();
    },
  });
  const releaseMaintenance = createDeferredCore();
  const maintenanceStarted = createDeferredCore();
  const events: string[] = [];
  const resourceReads: unknown[] = [];
  const commitTurn = vi.fn<NonNullable<ContextEngine["commitTurn"]>>(async () => {
    events.push("commit");
    if (outcome === "failed") {
      throw new Error("Synthetic commit failure");
    }
    return { status: outcome };
  });
  const maintain = vi.fn<NonNullable<ContextEngine["maintain"]>>(async () => {
    events.push("maintenance-started");
    resourceReads.push(probe.prepare("SELECT 42 AS value").get()?.value);
    maintenanceStarted.resolve();
    await releaseMaintenance.promise;
    resourceReads.push(probe.prepare("SELECT 42 AS value").get()?.value);
    events.push("maintenance-settled");
    if (maintenanceFails) {
      throw new Error("Synthetic maintenance failure");
    }
    return unchanged;
  });
  const dispose = vi.fn(async () => {
    resourceReads.push(probe.prepare("SELECT 42 AS value").get()?.value);
    events.push("engine-disposed");
  });
  const engine: ContextEngine = {
    info: {
      id: engineId,
      name: "Accepted maintenance fixture",
      ...maintenanceInfo,
      transcriptSemantics: {
        currentTurnFence: "before-current-turn-entry-v1",
        turnAdvancementIdempotency: "atomic-idempotent-v1",
      },
    },
    ingest: async () => ({ ingested: true }),
    assemble: async ({ messages }) => ({ messages, estimatedTokens: 0 }),
    compact: async () => ({ ok: true, compacted: false }),
    commitTurn,
    maintain,
    dispose,
  };
  registerContextEngineInRegistry(registry, "legacy", () => new LegacyContextEngine(), "core");
  source.runRegistration(engineId, () => {
    registerContextEngineInRegistry(registry, engineId, () => engine, `plugin:${engineId}`);
  });
  const config = {
    plugins: { slots: { contextEngine: engineId } },
    session: { store: target.storePath },
  };
  const lease = await withPluginRuntimeRegistryScope(registry, () =>
    createContextEngineLogicalTurnLease({
      identity: { runId: `${engineId}-run`, sessionId: target.sessionId },
      config,
    }),
  );
  const warn = vi.fn();
  const message = { role: "user" as const, content: "Synthetic question", timestamp: 1_000 };
  const recorder = createUserTurnTranscriptRecorder({ message, target: async () => undefined });
  await drainPendingContextEngineTurnsBeforeRun({
    admission: deferAdmission ? undefined : admission,
    ...(deferAdmission ? { recorder, sessionTarget: target } : {}),
    lease,
    warn,
  });
  lease.begin();
  await source.release();
  const pendingTurn = () =>
    database.db
      .prepare("SELECT attempt_count FROM context_engine_turn_outbox WHERE advancement_key = ?")
      .get(admission.logicalTurnId);
  return {
    facts,
    lease,
    config,
    source,
    sourceDisposed,
    probe,
    releaseMaintenance,
    maintenanceStarted,
    commitTurn,
    maintain,
    dispose,
    events,
    resourceReads,
    pendingTurn,
    warn,
    async persistAdmission() {
      recorder.markRuntimePersisted(message, admission);
      await recorder.waitForRuntimePersistence();
    },
  };
}

describe("durable accepted-turn maintenance handoff", () => {
  it.each([false, true])("disposes admission safely (late persistence=%s)", async (late) => {
    await withAcceptedTurn(
      async (fixture) => {
        if (!late) {
          expect(fixture.pendingTurn()).toEqual({ attempt_count: 0 });
        }
        await fixture.lease.dispose();
        if (late) {
          await fixture.persistAdmission();
        } else {
          await fixture.lease.dispose();
        }
        expect(fixture.pendingTurn()).toBeUndefined();
        expect(fixture.commitTurn).not.toHaveBeenCalled();
        if (!late) {
          expect(fixture.warn).toHaveBeenCalledExactlyOnceWith(
            expect.stringContaining("discarded unaccepted turn advancement"),
          );
          expect(fixture.dispose).toHaveBeenCalledOnce();
        }
      },
      { deferAdmission: late, maintenanceInfo: late ? {} : { turnMaintenanceMode: "background" } },
    );
  });

  it.each([
    { mode: "foreground", outcome: "committed", maintenanceFails: false },
    { mode: undefined, outcome: "committed", maintenanceFails: false },
    { mode: "background", outcome: "committed", maintenanceFails: false },
    { mode: "background", outcome: "duplicate", maintenanceFails: true },
  ] as const)(
    "hands off $mode maintenance and retains resources ($outcome, failure=$maintenanceFails)",
    async ({ mode, outcome, maintenanceFails }) => {
      const background = mode === "background";
      await withAcceptedTurn(
        async (fixture) => {
          const finalized = finalizeAcceptedContextEngineTurn(fixture).then(() => {
            if (!background) {
              fixture.events.push("finalized");
            }
            return "finalized";
          });
          try {
            if (background) {
              await finalized;
              expect(
                await Promise.race([
                  fixture.maintenanceStarted.promise.then(() => "started"),
                  waitForDeferredTurnMaintenanceForSession(fixture.facts.sessionKey).then(
                    () => "settled",
                  ),
                ]),
              ).toBe("started");
              expect(fixture.maintain).toHaveBeenCalledOnce();
              expect(fixture.maintain.mock.calls[0]?.[0]).toMatchObject({
                sessionId: fixture.facts.sessionIdUsed,
                sessionKey: fixture.facts.sessionKey,
                runtimeContext: {
                  ...fixture.facts.runtimeContext,
                  allowDeferredCompactionExecution: true,
                },
              });
              await fixture.lease.dispose();
              expect(fixture.dispose).not.toHaveBeenCalled();
            } else {
              expect(
                await Promise.race([
                  fixture.maintenanceStarted.promise.then(() => "maintenance"),
                  finalized,
                ]),
              ).toBe("maintenance");
              for (const hook of [fixture.commitTurn, fixture.maintain]) {
                expect(hook.mock.calls[0]?.[0].runtimeSettings).toMatchObject({
                  model: { provider: "fixture", resolved: "fixture-model" },
                  limits: { promptTokenBudget: 900 },
                });
              }
              const runtime = fixture.maintain.mock.calls[0]?.[0].runtimeContext;
              expect(runtime).toMatchObject({
                ...fixture.facts.runtimeContext,
                sessionTarget: {
                  ...fixture.facts.sessionTarget,
                  storePath: fixture.facts.boundary.admission.storePath,
                },
                llm: { complete: expect.any(Function) },
                rewriteTranscriptEntries: expect.any(Function),
              });
              expect(runtime?.allowDeferredCompactionExecution).toBeUndefined();
              // This capability reopens the durable target without a live SessionManager.
              expect(await runtime?.rewriteTranscriptEntries?.({ replacements: [] })).toEqual({
                ...unchanged,
                reason: "no replacements requested",
              });
            }
            expect(fixture.commitTurn).toHaveBeenCalledOnce();
            expect(fixture.pendingTurn()).toBeUndefined();
            expect(fixture.events).toEqual(["commit", "maintenance-started"]);
            expect(fixture.probe.isOpen).toBe(true);
          } finally {
            fixture.releaseMaintenance.resolve();
            await finalized;
          }
          if (!background) {
            await fixture.lease.dispose();
          }
          await waitForDeferredTurnMaintenanceForSession(fixture.facts.sessionKey);
          await fixture.sourceDisposed.promise;
          expect(fixture.events).toEqual([
            "commit",
            "maintenance-started",
            "maintenance-settled",
            ...(!background ? ["finalized"] : []),
            "engine-disposed",
          ]);
          expect(fixture.resourceReads).toEqual([42, 42, 42]);
          expect(fixture.dispose).toHaveBeenCalledOnce();
          expect(fixture.probe.isOpen).toBe(false);
          expect(fixture.pendingTurn()).toBeUndefined();
        },
        { outcome, maintenanceFails, maintenanceInfo: { turnMaintenanceMode: mode } },
      );
    },
  );

  it.each(["commit-failed", "aborted", "yieldAborted", "promptError"] as const)(
    "does not schedule maintenance for %s",
    async (failure) => {
      await withAcceptedTurn(
        async (fixture) => {
          const facts =
            failure === "commit-failed" ? fixture.facts : { ...fixture.facts, [failure]: true };
          await finalizeAcceptedContextEngineTurn({ ...fixture, facts, warn: () => {} });
          await fixture.lease.dispose();
          await waitForDeferredTurnMaintenanceForSession(fixture.facts.sessionKey);
          expect(fixture.maintain).not.toHaveBeenCalled();
          expect(fixture.dispose).toHaveBeenCalledOnce();
          expect(fixture.commitTurn).toHaveBeenCalledTimes(failure === "commit-failed" ? 1 : 0);
          if (failure === "commit-failed") {
            expect(fixture.pendingTurn()).toMatchObject({ attempt_count: 1 });
          } else {
            expect(fixture.pendingTurn()).toBeUndefined();
          }
        },
        { outcome: failure === "commit-failed" ? "failed" : "committed" },
      );
    },
  );
});
