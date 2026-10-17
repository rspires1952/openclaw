import { setImmediate } from "node:timers/promises";
import { afterEach, expect, it, vi } from "vitest";
import { createDeferred, withinTest } from "../../../test/helpers/promise.js";
import { createAgentRunRestartAbortError } from "../../agents/run-termination.js";
import { SESSION_RESTART_RECOVERY_TOMBSTONE_ERROR_CODE } from "../../config/sessions/lifecycle.js";
import {
  deleteSessionEntryLifecycle,
  loadSessionEntry,
  replaceSessionEntry,
} from "../../config/sessions/session-accessor.js";
import type { InternalSessionEntry as SessionEntry } from "../../config/sessions/types.js";
import { getAgentEventLifecycleGeneration } from "../../infra/agent-events.js";
import { resetDiagnosticRunActivityForTest } from "../../logging/diagnostic-run-activity.js";
import {
  createReplyOperation,
  getSessionControllerOperation,
} from "../../sessions/session-controller.js";
import {
  getTerminalSessionWorkAdmissionRelease,
  interruptSessionControllerEffects,
  runSessionMutation,
} from "../../sessions/session-controller.lifecycle.js";
import {
  claimSessionControllerTask,
  releaseSessionControllerClaim,
  reserveSessionControllerSource,
  retireSessionControllerInput,
} from "../../sessions/session-controller.mailbox.js";
import { testing } from "../../sessions/session-controller.test-support.js";
import { runWithReplyOperationLifecycleAdmission } from "./reply-turn-admission.js";
import { admitTestReplyTurn, createSessionStore } from "./reply-turn-admission.test-support.js";

const releaseMocks = vi.hoisted(() => ({
  beforeRelease: vi.fn(async () => {}),
  schedule: vi.fn(),
}));
vi.mock(
  "../../agents/main-session-recovery/main-session-recovery-store.js",
  async (importOriginal) => {
    const actual =
      await importOriginal<
        typeof import("../../agents/main-session-recovery/main-session-recovery-store.js")
      >();
    return {
      ...actual,
      releaseMainSessionRecoveryOwner: async (
        lease: Parameters<typeof actual.releaseMainSessionRecoveryOwner>[0],
      ) => {
        await releaseMocks.beforeRelease();
        return await actual.releaseMainSessionRecoveryOwner(lease);
      },
    };
  },
);
vi.mock(
  "../../agents/main-session-recovery/main-session-recovery-owner-release.js",
  async (importOriginal) => ({
    ...(await importOriginal<
      typeof import("../../agents/main-session-recovery/main-session-recovery-owner-release.js")
    >()),
    scheduleMainSessionRecoveryPendingTarget: releaseMocks.schedule,
  }),
);

const sessionKey = "agent:main:telegram:topic:admission";
const sessionId = "original-session";
const scope = { sessionKey, sessionId };
const sourceKey = "agent:main:telegram:slash:source";
function admit(overrides: Partial<Parameters<typeof admitTestReplyTurn>[0]> = {}) {
  return admitTestReplyTurn({ ...scope, ...overrides });
}
function operation(overrides: Partial<Parameters<typeof createReplyOperation>[0]> = {}) {
  return createReplyOperation({ ...scope, resetTriggered: false, ...overrides });
}
function owned(result: Awaited<ReturnType<typeof admit>>) {
  expect(result.status).toBe("owned");
  if (result.status !== "owned") {
    throw new Error("Expected reply ownership");
  }
  return result.operation;
}
function store(entry: Partial<SessionEntry> = {}) {
  return createSessionStore({ [sessionKey]: { sessionId, updatedAt: 100, ...entry } });
}
function interruptedEntry(): SessionEntry {
  return {
    sessionId,
    updatedAt: 100,
    status: "interrupted",
    abortedLastRun: true,
    mainRestartRecovery: { cycleId: "cycle-1", revision: 1, chargedAttempts: 0 },
  };
}
function tombstoneEntry(): SessionEntry {
  return {
    sessionId,
    updatedAt: 100,
    mainRestartRecovery: {
      cycleId: "cycle-1",
      revision: 4,
      chargedAttempts: 3,
      tombstone: {
        reason: "automatic recovery exhausted",
        recoveredSessionId: "dashboard-successor",
        recoveredSessionKey: "agent:main:dashboard:successor",
      },
    },
  };
}
async function holdMutation(storePath: string, run: () => Promise<unknown> = async () => {}) {
  const started = createDeferred();
  const release = createDeferred();
  const mutation = runSessionMutation({
    scope: storePath,
    identities: [sessionKey, sessionId],
    run: async () => {
      started.resolve();
      await release.promise;
      await run();
    },
  });
  await started.promise;
  return async () => {
    release.resolve();
    await mutation;
  };
}
function holdRecoveryRelease() {
  const started = createDeferred();
  const release = createDeferred();
  releaseMocks.beforeRelease.mockImplementationOnce(async () => {
    started.resolve();
    await release.promise;
  });
  return { started: started.promise, release: release.resolve };
}
async function expectRecoveryReleased(storePath: string, key = sessionKey) {
  await vi.waitFor(() =>
    expect(
      loadSessionEntry({ storePath, sessionKey: key })?.mainRestartRecovery?.foregroundClaims,
    ).toBeUndefined(),
  );
}
function interrupt(
  storePath: string,
  run: () => Promise<void>,
  reason: Error = new Error("Synthetic user interruption"),
) {
  const target = { scope: storePath, identities: [sessionKey, sessionId] };
  return runSessionMutation({
    ...target,
    prepare: async () => {
      await interruptSessionControllerEffects({ ...target, reason });
    },
    run,
  });
}
afterEach(async () => {
  if (vi.isFakeTimers()) {
    await vi.runOnlyPendingTimersAsync();
    vi.useRealTimers();
  }
  testing.resetSessionControllerForTest();
  resetDiagnosticRunActivityForTest();
  releaseMocks.beforeRelease.mockClear();
  releaseMocks.schedule.mockClear();
});

it("rejects a reply when an archive commits before admission", async () => {
  const storePath = store();
  const release = await holdMutation(storePath, () =>
    replaceSessionEntry({ sessionKey, storePath }, { sessionId, updatedAt: 100, archivedAt: 100 }),
  );
  const admission = admit({ storePath });
  await release();
  await expect(admission).rejects.toMatchObject({
    name: "SessionWorkStartError",
    reason: "archived",
    retryable: false,
    message: `Session "${sessionKey}" is archived. Restore it before starting new work.`,
  });
});
it("rejects a reply when deletion commits before admission", async () => {
  const storePath = store();
  const release = await holdMutation(storePath, () =>
    deleteSessionEntryLifecycle({
      storePath,
      archiveTranscript: false,
      target: { canonicalKey: sessionKey, storeKeys: [sessionKey] },
    }),
  );
  const admission = admit({ storePath, expectedSessionId: sessionId });
  await release();
  await expect(admission).rejects.toMatchObject({ code: "SESSION_WORK_START_CHANGED" });
});
it("waits for recovery release before admitting a queued successor", async () => {
  const storePath = store(interruptedEntry());
  const owner = owned(await admit({ storePath, expectedSessionId: sessionId }));
  const release = holdRecoveryRelease();
  owner.complete();
  await release.started;
  let settled = false;
  const successor = admit({
    storePath,
    expectedSessionId: sessionId,
    kind: "queued_followup",
  }).then((result) => {
    settled = true;
    return result;
  });
  await Promise.resolve();
  expect(settled).toBe(false);
  await expect(
    admit({ storePath, expectedSessionId: sessionId, kind: "heartbeat" }),
  ).resolves.toEqual({ status: "skipped", reason: "active-run" });
  release.release();
  owned(await successor).complete();
  await expectRecoveryReleased(storePath);
});
it("preserves a source recovery identity after adopting a distinct target session", async () => {
  const storePath = createSessionStore({
    [sourceKey]: interruptedEntry(),
    [sessionKey]: { sessionId: "target-session", updatedAt: 100 },
  });
  const source = owned(
    await admit({ sessionKey: sourceKey, storePath, expectedSessionId: sessionId }),
  );
  const adopted = owned(
    await admit({
      storePath,
      expectedSessionId: "target-session",
      waitForActive: false,
      adoptOperation: source,
    }),
  );
  adopted.updateSessionId("target-session");
  expect(adopted).toBe(source);
  expect(adopted.key).toBe(sessionKey);
  expect(adopted.sessionId).toBe("target-session");
  const targets = [sourceKey, sessionKey].map((key) => ({ scope: storePath, identities: [key] }));
  expect(targets.map(getTerminalSessionWorkAdmissionRelease)).toEqual([false, false]);
  adopted.freezeAbort();
  const terminalReleases = targets.map((target) => {
    const released = getTerminalSessionWorkAdmissionRelease(target);
    if (released === false) {
      throw new Error("Committed reply admission is still classified as live");
    }
    return released;
  });
  const release = holdRecoveryRelease();
  adopted.complete();
  await release.started;
  let settled = false;
  const successor = admit({ sessionKey: sourceKey, storePath, expectedSessionId: sessionId }).then(
    (result) => {
      settled = true;
      return result;
    },
  );
  await Promise.resolve();
  expect(settled).toBe(false);
  release.release();
  await Promise.all(terminalReleases);
  const next = owned(await successor);
  expect(next.sessionId).toBe(sessionId);
  next.complete();
  await expectRecoveryReleased(storePath, sourceKey);
});
it("admits an explicit reset without reopening its restart tombstone", async () => {
  const archivedAt = Date.now() - 1000;
  const storePath = store({ ...tombstoneEntry(), archivedAt, status: "failed" });
  const admitted = owned(
    await admit({
      storePath,
      expectedSessionId: sessionId,
      resetTriggered: true,
      allowRestartTombstoneReset: true,
    }),
  );
  expect(loadSessionEntry({ storePath, sessionKey })).toMatchObject({
    sessionId,
    archivedAt,
    mainRestartRecovery: { tombstone: { recoveredSessionId: "dashboard-successor" } },
  });
  admitted.complete();
});
it("does not treat resetTriggered alone as restart-tombstone authority", async () => {
  await expect(
    admit({
      storePath: store(tombstoneEntry()),
      expectedSessionId: sessionId,
      resetTriggered: true,
    }),
  ).rejects.toMatchObject({
    code: SESSION_RESTART_RECOVERY_TOMBSTONE_ERROR_CODE,
    message: expect.stringMatching(/ended during restart recovery/i),
  });
});
it.each([true, false])(
  "requires terminal evidence before retiring recovery fences at visible admission: %s",
  async (terminal) => {
    const storePath = store({
      abortedLastRun: false,
      restartRecoveryRuns: [{ runId: "stale-run", lifecycleGeneration: "stale-generation" }],
      ...(terminal ? { restartRecoveryTerminalRunIds: ["stale-run"] } : {}),
    });
    const before = loadSessionEntry({ storePath, sessionKey });
    expect(before?.status).toBeUndefined();
    if (!terminal) {
      await expect(admit({ storePath, expectedSessionId: sessionId })).rejects.toMatchObject({
        code: "SESSION_WORK_START_CHANGED",
      });
      expect(loadSessionEntry({ storePath, sessionKey })).toEqual(before);
      return;
    }
    const admitted = owned(await admit({ storePath, expectedSessionId: sessionId }));
    expect(admitted.sessionId).toBe(sessionId);
    const entry = loadSessionEntry({ storePath, sessionKey });
    expect(entry?.restartRecoveryRuns).toBeUndefined();
    expect(entry?.mainRestartRecovery).toBeUndefined();
    admitted.complete();
  },
);
it("schedules released recovery only after retained admission exits", async () => {
  const storePath = store(interruptedEntry());
  const blocker = operation();
  const reservation = operation({ sessionKey: sourceKey, sessionId: "source-session" });
  const result = await admit({
    storePath,
    sessionId: reservation.sessionId,
    expectedSessionId: sessionId,
    waitForActive: false,
    retainLifecycleAdmissionOnActive: true,
    adoptOperation: reservation,
  });
  expect(result).toMatchObject({ status: "skipped", reason: "active-run" });
  expect(releaseMocks.schedule).not.toHaveBeenCalled();
  expect(loadSessionEntry({ storePath, sessionKey })).not.toHaveProperty(
    "mainRestartRecovery.foregroundClaims",
  );
  if (result.status === "skipped") {
    result.lifecycleAdmission?.release();
  }
  await vi.waitFor(() =>
    expect(releaseMocks.schedule).toHaveBeenCalledWith({ ...scope, storePath }),
  );
  blocker.complete();
  reservation.complete();
});
it("excludes the initiating reply admission from an in-band lifecycle mutation", async () => {
  const storePath = store();
  const admitted = owned(await admit({ storePath, expectedSessionId: sessionId }));
  await runWithReplyOperationLifecycleAdmission(admitted, () =>
    interrupt(storePath, async () => {}),
  );
  expect(admitted.abortSignal.aborted).toBe(false);
  admitted.complete();
});
it("skips an aborted reply waiting behind a lifecycle mutation", async () => {
  const storePath = store();
  const release = await holdMutation(storePath);
  const controller = new AbortController();
  const admission = admit({ storePath, upstreamAbortSignal: controller.signal });
  controller.abort();
  await release();
  await expect(admission).resolves.toEqual({ status: "skipped", reason: "aborted" });
});
it.for([false, true])(
  "adopts a source-keyed command reservation and preserves restart=%s on interruption",
  async (restart, { signal }) => {
    const storePath = store();
    const reservation = operation({ sessionKey: sourceKey, sessionId: "source-session" });
    expect(
      owned(
        await admit({
          storePath,
          sessionId: reservation.sessionId,
          expectedSessionId: sessionId,
          waitForActive: false,
          adoptOperation: reservation,
        }),
      ),
    ).toBe(reservation);
    expect(reservation.key).toBe(sessionKey);
    expect(getSessionControllerOperation(sourceKey)).toBeUndefined();
    expect(getSessionControllerOperation(sessionKey)).toBe(reservation);
    reservation.setPhase("running");
    const aborted = createDeferred();
    reservation.abortSignal.addEventListener("abort", () => aborted.resolve(), { once: true });
    let mutationRan = false;
    const mutation = interrupt(
      storePath,
      async () => {
        mutationRan = true;
      },
      restart ? createAgentRunRestartAbortError() : undefined,
    );
    try {
      await withinTest(aborted.promise, signal);
      expect(reservation.result).toEqual({
        kind: "aborted",
        code: restart ? "aborted_for_restart" : "aborted_by_user",
      });
      expect(mutationRan).toBe(false);
    } finally {
      reservation.complete();
      await mutation;
    }
    expect(mutationRan).toBe(true);
  },
);
it("skips adoption without waiting when the target run slot is owned", async () => {
  const blocker = operation();
  blocker.setPhase("running");
  const storePath = store({
    restartRecoveryRuns: [
      { runId: "active-run", lifecycleGeneration: getAgentEventLifecycleGeneration() },
    ],
  });
  const before = loadSessionEntry({ storePath, sessionKey });
  const reservation = operation({ sessionKey: sourceKey, sessionId: "source-session" });
  const result = await admit({
    storePath,
    sessionId: reservation.sessionId,
    expectedSessionId: sessionId,
    waitForActive: false,
    adoptOperation: reservation,
  });
  expect(result).toMatchObject({
    status: "skipped",
    reason: "active-run",
    activeOperation: blocker,
  });
  expect(reservation.key).toBe(sourceKey);
  expect(getSessionControllerOperation(sourceKey)).toBe(reservation);
  expect(getSessionControllerOperation(sessionKey)).toBe(blocker);
  expect(reservation.result).toBeNull();
  expect(loadSessionEntry({ storePath, sessionKey })).toEqual(before);
  blocker.complete();
  reservation.complete();
});

it("binds the originating transcript leaf to the admitted operation", async () => {
  const admitted = owned(await admit({ originatingLeafEntryId: "leaf-before-run" }));
  expect(admitted.originatingLeafEntryId).toBe("leaf-before-run");
  admitted.complete();
});

it.each([false, true])(
  "rechecks a reset committed before admission, expected-session=%s",
  async (expected) => {
    const storePath = store();
    const release = await holdMutation(storePath, () =>
      replaceSessionEntry(
        { sessionKey, storePath },
        {
          sessionId: "reset-session",
          updatedAt: 200,
        },
      ),
    );
    const admission = admit({ storePath, expectedSessionId: expected ? sessionId : undefined });
    await release();
    if (expected) {
      await expect(admission).rejects.toMatchObject({ code: "SESSION_WORK_START_CHANGED" });
    } else {
      const result = owned(await admission);
      expect(result.sessionId).toBe("reset-session");
      result.complete();
    }
  },
);

for (const outcome of ["retired", "cancelled"] as const) {
  it(`waits without spinning on raw source preparation until ${outcome}`, async ({ signal }) => {
    const input = reserveSessionControllerSource(sessionKey, { policy: { mode: "followup" } });
    const abort = new AbortController();
    let settled = false;
    const pending = admit({ upstreamAbortSignal: abort.signal }).then((result) => {
      settled = true;
      return result;
    });
    try {
      await withinTest(setImmediate(), signal);
      expect(settled).toBe(false);
      if (outcome === "cancelled") {
        abort.abort();
      } else {
        retireSessionControllerInput(input);
      }
      const result = await withinTest(pending, signal);
      if (outcome === "cancelled") {
        expect(result).toEqual({ status: "skipped", reason: "aborted" });
        expect(input.phase).toBe("preparing");
      } else {
        owned(result).complete();
      }
    } finally {
      abort.abort();
      retireSessionControllerInput(input);
      const result = await pending;
      if (result.status === "owned") {
        result.operation.complete();
      }
      await input.settlement.promise;
    }
  });
}

it("resumes raw-source admission after writer fencing without waiting for producer cleanup", async ({
  signal,
}) => {
  const input = reserveSessionControllerSource(sessionKey, { policy: { mode: "followup" } });
  const abort = new AbortController();
  const pending = admit({ upstreamAbortSignal: abort.signal });
  let sourceOperation: ReturnType<typeof operation> | undefined;
  try {
    await withinTest(setImmediate(), signal);
    const claim = await claimSessionControllerTask(input, () => {});
    sourceOperation = operation({ mailboxClaim: claim });
    sourceOperation.registerTerminalProducerFence(async () => true);
    expect(await sourceOperation.retireAdmissionAfterFencing()).toBe(true);
    expect(sourceOperation.admissionRetired).toBe(true);
    expect(input.phase).toBe("claimed");
    const successor = owned(await withinTest(pending, signal));
    expect(getSessionControllerOperation(sessionKey)).toBe(successor);
    expect(input.phase).toBe("claimed");
    successor.complete();
  } finally {
    abort.abort();
    sourceOperation?.complete();
    if (input.claim) {
      releaseSessionControllerClaim(input.claim);
    } else {
      retireSessionControllerInput(input);
    }
    const result = await pending;
    if (result.status === "owned") {
      result.operation.complete();
    }
    await input.settlement.promise;
  }
});
