// Session reset cleanup tests protect ACP metadata resets, active run shutdown,
// hook emission, thread bindings, and browser/MCP cleanup side effects.
import path from "node:path";
import { afterEach, expect, test, vi } from "vitest";
import {
  awaitGateBeforeSettlement,
  createDeferred,
  withinTest,
} from "../../test/helpers/promise.js";
import {
  isSessionEntryDataSql,
  observeHostDataSql,
} from "../../test/helpers/sqlite-statement-execution-counter.js";
import { seedCanonicalAcpSessionMeta } from "../acp/runtime/session-meta-fixture.test-support.js";
import { readAcpSessionEntry } from "../acp/runtime/session-meta.js";
import { listRegisteredAgentHarnesses, registerAgentHarness } from "../agents/harness/registry.js";
import { restoreRegisteredAgentHarnesses } from "../agents/harness/registry.test-support.js";
import * as preparedModelRuntime from "../agents/prepared-model-runtime.js";
import { loadSessionEntry, upsertSessionEntryCore } from "../config/sessions/session-accessor.js";
import { memorySessionActorOwners } from "../config/sessions/session-actor-memory-owner.js";
import { RESET_PARENT_GRANT_FIXTURE } from "../config/sessions/session-lineage.test-support.js";
import type { InternalSessionEntry, SessionAcpMeta } from "../config/sessions/types.js";
import { peekSystemEvents } from "../infra/system-events.js";
import { enqueueSystemEvent } from "../plugin-sdk/system-event-runtime.js";
import { createEmptyPluginRegistry } from "../plugins/registry-empty.js";
import { withSessionTurn } from "../sessions/session-controller.admission.js";
import {
  beginSessionEffect,
  captureSessionTarget,
  isSessionMutationActive,
  runSessionMutation,
} from "../sessions/session-controller.lifecycle.js";
import {
  submitSessionControllerTask,
  releaseSessionControllerClaim,
} from "../sessions/session-controller.mailbox.js";
import { createReplyOperation } from "../sessions/session-controller.operation.js";
import { runExclusiveSessionLifecycle } from "../sessions/session-lifecycle-admission.test-support.js";
import { resolveIncognitoOpenClawAgentSqlitePath } from "../state/openclaw-agent-db.paths.js";
import { closeOpenClawStateDatabaseForTest } from "../state/openclaw-state-db.js";
import { disposeSessionReadContexts } from "./server-methods/sessions-read-cache.test-support.js";
import {
  expectResetAcpState,
  resolvedAcpMeta,
} from "./server.sessions.reset-cleanup.test-support.js";
import { embeddedRunMock, testState, writeSessionStore } from "./test-helpers.js";
import {
  setupGatewaySessionsHandlerTestHarness,
  bootstrapCacheMocks,
  subagentLifecycleHookMocks,
  subagentLifecycleHookState,
  threadBindingMocks,
  acpRuntimeMocks,
  acpManagerMocks,
  browserSessionTabMocks,
  bundleMcpRuntimeMocks,
  writeSingleLineSession,
  sessionStoreEntry,
  expectActiveRunCleanup,
  directSessionReq,
  getGatewayConfigModule,
  getSessionsHandlers,
  sessionHookMocks,
} from "./test/server-sessions.test-helpers.js";

const { createSessionStoreDir, seedActiveMainSession } = setupGatewaySessionsHandlerTestHarness();

afterEach(async () => {
  await disposeSessionReadContexts();
  closeOpenClawStateDatabaseForTest();
});

async function seedMainSession() {
  const seeded = await createSessionStoreDir();
  await writeSingleLineSession(seeded.dir, "sess-main", "hello");
  await writeSessionStore({ entries: { main: sessionStoreEntry("sess-main") } });
  return seeded;
}
async function seedWaitingActiveMainSession() {
  const seeded = await seedActiveMainSession();
  const target = captureSessionTarget({
    storeScope: seeded.storePath,
    sessionKey: "agent:main:main",
    aliases: ["main", "sess-main"],
    incarnation: "sess-main",
    agentId: "main",
  });
  const started = createDeferred();
  const activeRun = withSessionTurn(
    { sessionKey: target.sessionKey, sessionId: "sess-main", target },
    async (_operation, signal) => {
      started.resolve();
      await new Promise<void>((resolve) => {
        signal.addEventListener("abort", () => resolve());
      });
    },
  );
  await started.promise;
  return { ...seeded, activeRun };
}

async function resetMainSession() {
  return await directSessionReq<{
    ok: true;
    key: string;
    entry: { lifecycleRevision?: string; sessionId: string };
  }>("sessions.reset", {
    key: "main",
  });
}

function installAcpRuntimeBackendWithFreshSession() {
  const prepareFreshSession = vi.fn(async () => {});
  acpRuntimeMocks.getAcpRuntimeBackend.mockReturnValue({
    id: "acpx",
    runtime: {
      prepareFreshSession,
    },
  });
  return prepareFreshSession;
}

test("sessions.reset retires an unbound memory session without host session SQL", async () => {
  await createSessionStoreDir();
  const location = {
    agentId: "main",
    path: resolveIncognitoOpenClawAgentSqlitePath({ agentId: "main" }),
  };
  const sessionKey = "agent:main:dashboard:incognito-reset-source";
  const scope = { agentId: "main", storePath: location.path, sessionKey };
  try {
    await upsertSessionEntryCore(scope, {
      sessionId: "reset-source",
      lifecycleRevision: "reset-source-lifecycle",
      updatedAt: 1,
      incognito: true,
    });
    const sql = observeHostDataSql();
    try {
      const result = await directSessionReq<{ deleted: boolean }>("sessions.reset", {
        key: sessionKey,
      });
      expect(result).toMatchObject({ ok: true, payload: { deleted: true } });
      expect(loadSessionEntry(scope)).toBeUndefined();
      expect(sql.queries.filter(isSessionEntryDataSql)).toEqual([]);
    } finally {
      sql.restore();
    }
  } finally {
    memorySessionActorOwners.closeDatabase(location);
  }
});

test("sessions.reset preempts the controller run and clears queues", async () => {
  const { activeRun, storePath } = await seedWaitingActiveMainSession();
  const parentGrant = RESET_PARENT_GRANT_FIXTURE;
  const preferences = {
    responseUsage: "tokens",
    pinnedAt: 123,
    label: "Operator session",
    category: "Operator group",
    icon: "🦞",
    boardFace: "dashboard",
    boardPresentation: "expanded",
    visibility: "draft",
  } satisfies Partial<InternalSessionEntry>;
  await upsertSessionEntryCore(
    { storePath, sessionKey: "agent:main:main" },
    { ...parentGrant, ...preferences },
  );
  enqueueSystemEvent("stale event via alias", { sessionKey: "main" });
  enqueueSystemEvent("stale event via canonical key", { sessionKey: "agent:main:main" });
  enqueueSystemEvent("stale event via session id", { sessionKey: "sess-main" });
  const waitCallCountAtSnapshotClear: number[] = [];
  bootstrapCacheMocks.clearBootstrapSnapshot.mockImplementation(() => {
    waitCallCountAtSnapshotClear.push(embeddedRunMock.waitCalls.length);
  });

  const reset = await resetMainSession();
  expect(reset.ok).toBe(true);
  await activeRun;
  expect(reset.payload?.key).toBe("agent:main:main");
  expect(reset.payload?.entry.sessionId).toBe("sess-main");
  expect(reset.payload?.entry.lifecycleRevision).toEqual(expect.any(String));
  expect(reset.payload?.entry).not.toHaveProperty("sessionDiffBaselineCapture");
  expect(reset.payload?.entry).toMatchObject(preferences);
  expect(
    loadSessionEntry({ agentId: "main", sessionKey: "agent:main:main", storePath }) as
      | InternalSessionEntry
      | undefined,
  ).toMatchObject({
    sessionId: "sess-main",
    ...parentGrant,
    ...preferences,
    sessionDiffBaselineCapture: {
      version: 1,
      captureId: expect.any(String),
      status: "pending",
    },
  });
  expectActiveRunCleanup("agent:main:main", ["main", "agent:main:main", "sess-main"], "main");
  expect(peekSystemEvents("agent:main:main")).toStrictEqual([]);
  expect(peekSystemEvents("agent:main:sess-main")).toStrictEqual([]);
  expect(bundleMcpRuntimeMocks.retireSessionMcpRuntime).toHaveBeenCalledWith({
    sessionId: "sess-main",
    reason: "gateway-session-cleanup",
    preserveActiveLeases: true,
    retainAcrossReuse: false,
    onError: expect.any(Function),
  });
  expect(bundleMcpRuntimeMocks.retireSessionMcpRuntime).toHaveBeenCalledTimes(1);
  expect(bundleMcpRuntimeMocks.disposeSessionMcpRuntime).toHaveBeenCalledWith("sess-main");
  expect(waitCallCountAtSnapshotClear).toEqual([0]);
  expect(browserSessionTabMocks.closeTrackedBrowserTabsForSessions).toHaveBeenCalledTimes(1);
  const closeTabsCall = browserSessionTabMocks.closeTrackedBrowserTabsForSessions.mock
    .calls[0] as unknown as [{ sessionKeys?: string[]; onWarn?: unknown }] | undefined;
  const closeTabsParams = closeTabsCall?.[0];
  expect(closeTabsParams?.sessionKeys).toEqual(["agent:main:main", "agent:main:sess-main"]);
  expect(typeof closeTabsParams?.onWarn).toBe("function");
  expect(subagentLifecycleHookMocks.runSubagentEnded).toHaveBeenCalledTimes(1);
  expect(subagentLifecycleHookMocks.runSubagentEnded).toHaveBeenCalledWith(
    {
      targetSessionKey: "agent:main:main",
      targetKind: "acp",
      reason: "session-reset",
      sendFarewell: true,
      outcome: "reset",
    },
    {
      childSessionKey: "agent:main:main",
    },
  );
  expect(threadBindingMocks.unbindThreadBindingsBySessionKey).toHaveBeenCalledTimes(1);
  expect(threadBindingMocks.unbindThreadBindingsBySessionKey).toHaveBeenCalledWith({
    targetSessionKey: "agent:main:main",
    reason: "session-reset",
  });
});

test("sessions.reset preserves its unavailable result when controller preemption times out", async () => {
  const { storePath } = await seedActiveMainSession();
  const target = captureSessionTarget({
    storeScope: storePath,
    sessionKey: "agent:main:main",
    aliases: ["main", "sess-main"],
    incarnation: "sess-main",
    agentId: "main",
  });
  const claim = await submitSessionControllerTask(target.sessionKey, {
    target,
    start: (mailboxClaim) => {
      createReplyOperation({
        sessionKey: target.sessionKey,
        sessionId: "sess-main",
        target,
        resetTriggered: false,
        mailboxClaim,
      });
    },
  });
  const operation = claim.operation!;
  const preemptAttempted = createDeferred();
  vi.spyOn(operation, "abort").mockImplementation(() => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    preemptAttempted.resolve();
    return false;
  });
  const reset = resetMainSession();
  void reset.catch(() => {});
  try {
    await preemptAttempted.promise;
    expect(isSessionMutationActive(storePath, target.aliases)).toBe(true);
    await vi.advanceTimersByTimeAsync(15_000);
    vi.useRealTimers();

    expect(await reset).toEqual({
      ok: false,
      error: {
        code: "UNAVAILABLE",
        message: "Session main is still active; try again in a moment.",
      },
    });
    expect(loadSessionEntry({ sessionKey: target.sessionKey, storePath })?.sessionId).toBe(
      "sess-main",
    );
    expect(bundleMcpRuntimeMocks.retireSessionMcpRuntime).not.toHaveBeenCalled();
    expect(browserSessionTabMocks.closeTrackedBrowserTabsForSessions).not.toHaveBeenCalled();
  } finally {
    operation.complete();
    releaseSessionControllerClaim(claim);
    await claim.settlement.promise;
    await reset.catch(() => undefined);
    vi.useRealTimers();
  }
});

test("sessions.reset clears retained native conversations from every execution owner", async () => {
  const registeredHarnesses = listRegisteredAgentHarnesses();
  const current = new Map([["sess-main", "gateway conversation"]]);
  const firstModel = new Map([
    ["sess-main", "first model context"],
    ["sibling", "keep me"],
  ]);
  const secondModel = new Map([["sess-main", "second model context"]]);
  const harness = (history: Map<string, string>) => ({
    id: "reset-observer",
    label: "Reset observer",
    supports: () => ({ supported: false }),
    runAttempt: async () => {
      throw new Error("not used");
    },
    reset: async ({ sessionId }: { sessionId?: string }) => {
      if (sessionId) {
        history.delete(sessionId);
      }
    },
  });
  registerAgentHarness(harness(current));
  const registries = [firstModel, secondModel].map((history) => {
    const registry = createEmptyPluginRegistry();
    registry.agentHarnesses.push({
      pluginId: "native-fixture",
      source: "test",
      harness: harness(history),
    });
    return registry;
  });
  let retained = false;
  const acquire = vi
    .spyOn(preparedModelRuntime, "acquireAgentRuntimeCleanupRegistries")
    .mockImplementation(async () => {
      retained = true;
      return {
        registries,
        async [Symbol.asyncDispose]() {
          retained = false;
        },
      };
    });
  try {
    await seedWaitingActiveMainSession();
    const response = await resetMainSession();
    expect(response.ok).toBe(true);
    expect(current.has("sess-main")).toBe(false);
    expect(firstModel.has("sess-main")).toBe(false);
    expect(secondModel.has("sess-main")).toBe(false);
    expect(firstModel.get("sibling")).toBe("keep me");
    expect(retained).toBe(false);
  } finally {
    acquire.mockRestore();
    restoreRegisteredAgentHarnesses(registeredHarnesses);
  }
});

test("sessions.reset interrupts work admitted before runtime registration", async () => {
  const { storePath } = await seedActiveMainSession();
  let interrupted = false;
  let releaseAdmission = () => {};
  const admissionLease = await beginSessionEffect({
    scope: storePath,
    identities: ["agent:main:main", "sess-main"],
    assertAllowed: () => {},
    onInterrupt: () => {
      interrupted = true;
      releaseAdmission();
    },
  });
  releaseAdmission = admissionLease.release;

  const reset = await resetMainSession();
  expect(reset.ok).toBe(true);
  expect(interrupted).toBe(true);
});

test("sessions.reset does not interrupt the admission that initiates it", async () => {
  const { storePath } = await seedActiveMainSession();
  let interrupted = false;
  const admissionLease = await beginSessionEffect({
    scope: storePath,
    identities: ["agent:main:main", "sess-main"],
    assertAllowed: () => {},
    onInterrupt: () => {
      interrupted = true;
    },
  });

  try {
    const reset = await admissionLease.run(resetMainSession);
    expect(reset.ok).toBe(true);
    expect(interrupted).toBe(false);
  } finally {
    admissionLease.release();
  }
});

test("sessions.reset rejects an active lifecycle mutation without interrupting admitted work", async () => {
  const { storePath } = await seedActiveMainSession();
  let interrupted = false;
  const admissionLease = await beginSessionEffect({
    scope: storePath,
    identities: ["agent:main:main", "sess-main"],
    assertAllowed: () => {},
    onInterrupt: () => {
      interrupted = true;
    },
  });
  const { promise: mutationReleased, resolve: releaseMutation } = createDeferred();
  const { promise: mutationStarted, resolve: markMutationStarted } = createDeferred();
  const blocker = runSessionMutation({
    scope: storePath,
    identities: ["agent:main:main", "sess-main"],
    run: async () => {
      markMutationStarted();
      await mutationReleased;
    },
  });
  try {
    await mutationStarted;
    const { performGatewaySessionReset } = await import("./session-reset-service.js");
    const assertCurrent = vi.fn(() => {
      throw new Error("stale lifecycle");
    });
    const reset = await performGatewaySessionReset({
      key: "main",
      reason: "reset",
      commandSource: "gateway:agent",
      workerPlacementContext: {},
      assertCurrent,
    });
    expect(reset).toMatchObject({
      ok: false,
      error: {
        code: "UNAVAILABLE",
        message: expect.stringContaining("lifecycle mutation in progress"),
      },
    });
    expect(assertCurrent).not.toHaveBeenCalled();
    expect(interrupted).toBe(false);
  } finally {
    releaseMutation();
    admissionLease.release();
    await blocker;
  }
});

test("sessions.reset finishes after lifecycle rotation during destructive cleanup", async () => {
  const { dir } = await createSessionStoreDir();
  await writeSingleLineSession(dir, "sess-main", "hello");
  const prepareFreshSession = installAcpRuntimeBackendWithFreshSession();
  await writeSessionStore({
    entries: {
      main: sessionStoreEntry("sess-main"),
    },
  });
  seedCanonicalAcpSessionMeta({
    sessionKey: "agent:main:main",
    lifecycleRevision: undefined,
    meta: resolvedAcpMeta({
      recordId: "agent:main:main",
      backendSessionId: "backend-session-1",
      runtimeOptions: {
        runtimeMode: "auto",
        timeoutSeconds: 30,
      },
    }),
  });
  let lifecycleCurrent = true;
  acpManagerMocks.closeSession.mockImplementationOnce(async () => {
    lifecycleCurrent = false;
  });
  const { performGatewaySessionReset } = await import("./session-reset-service.js");

  const reset = await performGatewaySessionReset({
    key: "main",
    reason: "new",
    commandSource: "gateway:agent",
    workerPlacementContext: {},
    assertCurrent: () => {
      if (!lifecycleCurrent) {
        throw new Error("stale lifecycle");
      }
    },
  });

  expect(reset.ok).toBe(true);
  expectResetAcpState(readAcpSessionEntry({ sessionKey: "agent:main:main" })?.acp);
  expect(prepareFreshSession).not.toHaveBeenCalled();
});

test("sessions.reset rejects a concurrent archive during lifecycle rotation", async () => {
  const { dir, storePath } = await createSessionStoreDir();
  const sessionKey = "agent:main:subagent:archive-race";
  await writeSingleLineSession(dir, "sess-archive-race", "hello");
  await writeSessionStore({
    entries: {
      [sessionKey]: sessionStoreEntry("sess-archive-race", { lifecycleRevision: "before-reset" }),
    },
  });
  const { promise: hookReleased, resolve: releaseHook } = createDeferred();
  const { promise: hookStarted, resolve: markHookStarted } = createDeferred();
  sessionHookMocks.triggerInternalHook.mockImplementationOnce(async () => {
    markHookStarted();
    await hookReleased;
  });
  const { performGatewaySessionReset } = await import("./session-reset-service.js");

  const resetPromise = performGatewaySessionReset({
    key: sessionKey,
    reason: "new",
    commandSource: "gateway:sessions.reset",
    workerPlacementContext: {},
  });
  await hookStarted;
  const archivePromise = directSessionReq("sessions.patch", {
    key: sessionKey,
    archived: true,
    expectedSessionId: "sess-archive-race",
    expectedLifecycleRevision: "before-reset",
  });
  releaseHook();

  const [reset, archived] = await Promise.all([resetPromise, archivePromise]);
  expect(reset.ok).toBe(true);
  expect(archived).toMatchObject({
    ok: false,
    error: { message: `Session ${sessionKey} changed before patch. Retry.` },
  });
  const entry = loadSessionEntry({ storePath, sessionKey });
  expect(entry?.archivedAt).toBeUndefined();
  expect(entry?.sessionId).toBe("sess-archive-race");
});

test("sessions.patch rejects an archive queued behind a rotated session", async () => {
  const { storePath } = await createSessionStoreDir();
  const sessionKey = "agent:main:subagent:queued-archive-race";
  const initialSessionId = "sess-queued-archive-race";
  const replacementSessionId = "sess-after-queued-reset";
  await writeSessionStore({
    entries: { [sessionKey]: sessionStoreEntry(initialSessionId) },
  });
  // Resolve the lazy handler/config imports before queue ordering begins.
  await Promise.all([getSessionsHandlers(), getGatewayConfigModule()]);
  let releaseBlocker = () => {};
  const { promise: blockerStarted, resolve: markBlockerStarted } = createDeferred();
  const blocker = runExclusiveSessionLifecycle({
    scope: storePath,
    identities: [sessionKey, initialSessionId],
    run: async () => {
      markBlockerStarted();
      await new Promise<void>((resolve) => {
        releaseBlocker = resolve;
      });
    },
  });
  await blockerStarted;
  const queuedReset = runSessionMutation({
    scope: storePath,
    identities: [sessionKey, initialSessionId],
    run: async () => {
      await writeSessionStore({
        entries: {
          [sessionKey]: sessionStoreEntry(replacementSessionId),
        },
      });
    },
  });
  await new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
  const archivePromise = directSessionReq("sessions.patch", {
    key: sessionKey,
    archived: true,
    expectedSessionId: initialSessionId,
  });
  await new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
  releaseBlocker();

  const archived = (await Promise.all([blocker, queuedReset, archivePromise]))[2];
  expect(archived).toMatchObject({
    ok: false,
    error: { message: `Session ${sessionKey} changed before patch. Retry.` },
  });
  const entry = loadSessionEntry({ storePath, sessionKey });
  expect(entry?.archivedAt).toBeUndefined();
  expect(entry?.sessionId).toBe(replacementSessionId);
});

test("sessions.reset closes child ACP runtime handles spawned from the parent", async () => {
  const { dir } = await createSessionStoreDir();
  await writeSingleLineSession(dir, "sess-main", "hello");
  installAcpRuntimeBackendWithFreshSession();

  await writeSessionStore({
    entries: {
      main: sessionStoreEntry("sess-main"),
      "acp-child-1": sessionStoreEntry("sess-child-1", {
        spawnedBy: "agent:main:main",
      }),
      "not-acp-child": sessionStoreEntry("sess-not-acp-child", {
        spawnedBy: "agent:main:main",
      }),
      "unrelated-acp-child": sessionStoreEntry("sess-unrelated-acp-child", {
        spawnedBy: "agent:main:other",
      }),
      "acp-grandchild": sessionStoreEntry("sess-grandchild", {
        parentSessionKey: "agent:main:acp-child-1",
      }),
    },
  });
  seedCanonicalAcpSessionMeta({
    sessionKey: "agent:main:main",
    meta: resolvedAcpMeta({
      recordId: "agent:main:main",
      backendSessionId: "backend-session-main",
    }),
  });
  seedCanonicalAcpSessionMeta({
    sessionKey: "agent:main:acp-child-1",
    meta: resolvedAcpMeta({
      recordId: "agent:main:acp-child-1",
      backendSessionId: "backend-session-child-1",
      runtimeSessionName: "runtime:child-1",
      mode: "oneshot",
    }),
  });
  for (const child of ["acp-grandchild", "unrelated-acp-child"]) {
    seedCanonicalAcpSessionMeta({
      sessionKey: `agent:main:${child}`,
      meta: resolvedAcpMeta({
        recordId: `agent:main:${child}`,
        backendSessionId: `backend-session-${child}`,
        mode: "oneshot",
      }),
    });
  }

  const reset = await directSessionReq<{ ok: true }>("sessions.reset", {
    key: "main",
  });
  expect(reset.ok).toBe(true);

  // The parent and its spawned ACP child are both closed; without child cleanup
  // the child's claude-agent-acp process is orphaned on parent reset (#68916).
  const closedKeys = (
    acpManagerMocks.closeSession.mock.calls as unknown as Array<[{ sessionKey?: string }]>
  ).map((call) => call[0]?.sessionKey);
  expect(closedKeys).toContain("agent:main:main");
  expect(closedKeys).toContain("agent:main:acp-child-1");
  expect(closedKeys).not.toContain("agent:main:not-acp-child");
  expect(closedKeys).not.toContain("agent:main:unrelated-acp-child");
  expect(closedKeys).not.toContain("agent:main:acp-grandchild");
});

test("sessions.reset closes a spawned ACP child that lives in a different agent store", async () => {
  const stateDir = process.env.OPENCLAW_STATE_DIR;
  if (!stateDir) {
    throw new Error("OPENCLAW_STATE_DIR is required for gateway session tests");
  }
  // Per-agent store layout: ACP children live under the target agent's own
  // store file, which is different from the parent's store.
  testState.sessionConfig = {
    store: path.join(stateDir, "agents", "{agentId}", "sessions", "sessions.json"),
  };
  testState.agentsConfig = {
    ownership: "explicit",
    entries: { main: {}, codex: {} },
  };
  testState.agentConfig = { systemAgent: { agentId: "main" } };
  const mainStorePath = path.join(stateDir, "agents", "main", "sessions", "sessions.json");
  const codexStorePath = path.join(stateDir, "agents", "codex", "sessions", "sessions.json");
  await writeSessionStore({
    agentId: "main",
    entries: {
      main: sessionStoreEntry("sess-main"),
    },
    storePath: mainStorePath,
  });
  await writeSessionStore({
    agentId: "codex",
    entries: {
      "agent:codex:acp:cross-store-child": sessionStoreEntry("sess-codex-child", {
        spawnedBy: "agent:main:main",
      }),
    },
    storePath: codexStorePath,
  });
  seedCanonicalAcpSessionMeta({
    sessionKey: "agent:main:main",
    meta: {
      backend: "acpx",
      agent: "codex",
      runtimeSessionName: "runtime:main",
      mode: "persistent",
      state: "idle",
      lastActivityAt: Date.now(),
    },
  });
  seedCanonicalAcpSessionMeta({
    sessionKey: "agent:codex:acp:cross-store-child",
    meta: {
      backend: "acpx",
      agent: "codex",
      runtimeSessionName: "runtime:codex-child",
      mode: "oneshot",
      state: "idle",
      lastActivityAt: Date.now(),
    },
  });

  const reset = await directSessionReq<{ ok: true }>("sessions.reset", { key: "main" });
  expect(reset.ok).toBe(true);

  // The child in the codex store is closed even though it is not in the main
  // (parent) store — cleanup enumerates the combined cross-agent store.
  const closedKeys = (
    acpManagerMocks.closeSession.mock.calls as unknown as Array<[{ sessionKey?: string }]>
  ).map((call) => call[0]?.sessionKey);
  expect(closedKeys).toContain("agent:codex:acp:cross-store-child");
});

test("sessions.reset closes child ACP runtimes concurrently so stuck children do not serialize cleanup", async ({
  signal,
}) => {
  const { dir } = await createSessionStoreDir();
  await writeSingleLineSession(dir, "sess-main", "hello");
  acpRuntimeMocks.getAcpRuntimeBackend.mockReturnValue({
    id: "acpx",
    runtime: { prepareFreshSession: vi.fn(async () => {}) },
  });

  const childAcp = (recordId: string): SessionAcpMeta => ({
    backend: "acpx",
    agent: "codex",
    runtimeSessionName: `runtime:${recordId}`,
    identity: {
      state: "resolved",
      acpxRecordId: recordId,
      acpxSessionId: `backend-${recordId}`,
      source: "status",
      lastUpdatedAt: Date.now(),
    },
    mode: "oneshot",
    cwd: "/tmp/acp-session",
    state: "idle",
    lastActivityAt: Date.now(),
  });

  await writeSessionStore({
    entries: {
      main: sessionStoreEntry("sess-main"),
      // Mix the two real lineage fields: ACP spawns record `spawnedBy`,
      // subagent spawns record `parentSessionKey`; both must be cleaned up.
      "acp-child-1": sessionStoreEntry("sess-c1", {
        spawnedBy: "agent:main:main",
      }),
      "acp-child-2": sessionStoreEntry("sess-c2", {
        spawnedBy: "agent:main:main",
      }),
      "acp-child-3": sessionStoreEntry("sess-c3", {
        parentSessionKey: "agent:main:main",
      }),
    },
  });
  for (const sessionKey of [
    "agent:main:main",
    "agent:main:acp-child-1",
    "agent:main:acp-child-2",
    "agent:main:acp-child-3",
  ]) {
    seedCanonicalAcpSessionMeta({
      sessionKey,
      meta: childAcp(sessionKey),
    });
  }

  // Parent cancel resolves immediately; child cancels hang until released. With
  // sequential cleanup only the first child would dispatch; concurrent cleanup
  // dispatches all three before any resolves.
  const childrenEntered = createDeferred();
  const releaseChildren: Array<() => void> = [];
  acpManagerMocks.cancelSession.mockImplementation(async (...args: unknown[]) => {
    const req = args[0] as { sessionKey?: string } | undefined;
    if (req?.sessionKey === "agent:main:main") {
      return;
    }
    await new Promise<void>((resolve) => {
      releaseChildren.push(resolve);
      if (releaseChildren.length === 3) {
        childrenEntered.resolve();
      }
    });
  });

  const resetPromise = directSessionReq<{ ok: true }>("sessions.reset", { key: "main" });
  try {
    await withinTest(
      awaitGateBeforeSettlement(
        childrenEntered.promise,
        resetPromise,
        "Reset settled before all child cancellations entered",
      ),
      signal,
    );
    const childCancels = (
      acpManagerMocks.cancelSession.mock.calls as unknown as Array<[{ sessionKey?: string }]>
    ).filter((call) => call[0]?.sessionKey?.startsWith("agent:main:acp-child"));
    expect(childCancels.length).toBe(3);

    for (const release of releaseChildren) {
      release();
    }
    const reset = await resetPromise;
    expect(reset.ok).toBe(true);
  } finally {
    acpManagerMocks.cancelSession.mockImplementation(async () => {});
    for (const release of releaseChildren) {
      release();
    }
    await resetPromise.catch(() => {});
  }
});

test("sessions.reset directly unbinds thread bindings when hooks are unavailable", async () => {
  await seedMainSession();
  subagentLifecycleHookState.hasSubagentEndedHook = false;

  const reset = await directSessionReq<{ ok: true; key: string }>("sessions.reset", {
    key: "main",
  });
  expect(reset.ok).toBe(true);
  expect(subagentLifecycleHookMocks.runSubagentEnded).not.toHaveBeenCalled();
  expect(threadBindingMocks.unbindThreadBindingsBySessionKey).toHaveBeenCalledTimes(1);
  expect(threadBindingMocks.unbindThreadBindingsBySessionKey).toHaveBeenCalledWith({
    targetSessionKey: "agent:main:main",
    reason: "session-reset",
  });
});
