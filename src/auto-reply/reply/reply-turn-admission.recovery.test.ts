import { setImmediate } from "node:timers/promises";
import { afterEach, expect, it, vi } from "vitest";
import {
  awaitGateBeforeSettlement,
  createDeferred,
  withinTest,
} from "../../../test/helpers/promise.js";
import { runWithAgentCommandRecoveryOwner } from "../../agents/agent-command-recovery-owner.js";
import * as recoveryLifecycle from "../../agents/main-session-recovery/main-session-recovery-lifecycle.js";
import * as recoveryOwnerRelease from "../../agents/main-session-recovery/main-session-recovery-owner-release.js";
import * as recoveryStore from "../../agents/main-session-recovery/main-session-recovery-store.js";
import { resolveRestartResendReservationId } from "../../agents/main-session-recovery/main-session-restart-recovery-shared.js";
import * as restartRecovery from "../../agents/main-session-recovery/main-session-restart-recovery.js";
import { loadSessionEntry, replaceSessionEntry } from "../../config/sessions/session-accessor.js";
import * as sessionAccessor from "../../config/sessions/session-accessor.js";
import type { InternalSessionEntry as SessionEntry } from "../../config/sessions/types.js";
import type { GatewayRecoveryRuntime } from "../../gateway/server-instance-runtime.types.js";
import type { GatewayRequestContext } from "../../gateway/server-methods/types.js";
import { getAgentEventLifecycleGeneration } from "../../infra/agent-events.js";
import {
  createReplyOperation,
  getSessionControllerOperation,
} from "../../sessions/session-controller.js";
import {
  captureSessionControllerSettlement,
  captureSessionTarget,
} from "../../sessions/session-controller.lifecycle.js";
import {
  claimSessionControllerTask,
  releaseSessionControllerClaim,
  reserveSessionControllerSource,
  retireSessionControllerInput,
  type SessionControllerMailboxClaim,
} from "../../sessions/session-controller.mailbox.js";
import { getSessionControllerEntryForOperation } from "../../sessions/session-controller.state.js";
import { testing } from "../../sessions/session-controller.test-support.js";
import { admitTestReplyTurn, createSessionStore } from "./reply-turn-admission.test-support.js";

type Admission = Awaited<ReturnType<typeof admitTestReplyTurn>>;
const sessionKey = "agent:main:main";
const sessionId = "interrupted-session";
const disposals: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const dispose of disposals.splice(0)) {
    await dispose();
  }
  testing.resetSessionControllerForTest();
  vi.restoreAllMocks();
});
function createRecoveryGatewayContext() {
  const recoveryRuntime: GatewayRecoveryRuntime = {
    prepareRestartRecovery: () => undefined,
    dispatchSessionMethod: vi.fn(),
    dispatchAgent: vi.fn(),
    waitForAgent: vi.fn(),
    sendRecoveryNotice: vi.fn(),
  };
  // The recovery boundary supplies execution; admission consumes these capabilities.
  return { getRuntimeConfig: () => ({}), recoveryRuntime } as GatewayRequestContext;
}
function complete(result: Admission | undefined) {
  if (result?.status === "owned") {
    result.operation.complete();
  }
}
function owned(result: Admission) {
  expect(result.status).toBe("owned");
  if (result.status !== "owned") {
    throw new Error("Fixture requires an admitted reply operation");
  }
  return result;
}
function observe(pending: Promise<Admission>) {
  const outcome: { result?: Admission; failure?: unknown } = {};
  const settled = pending.then(
    (result) => {
      outcome.result = result;
    },
    (failure: unknown) => {
      outcome.failure = failure;
    },
  );
  return Object.assign(outcome, { settled });
}
function recoveryFixture(overrides: Partial<SessionEntry> = {}) {
  const entry: SessionEntry = {
    sessionId,
    updatedAt: 100,
    status: "interrupted",
    abortedLastRun: true,
    ...overrides,
  };
  const storePath = createSessionStore({ [sessionKey]: entry });
  const scope = { scope: storePath, identities: [sessionKey, sessionId] };
  const abort = new AbortController();
  const pending: Promise<Admission>[] = [];
  const results: Admission[] = [];
  const cleanup: (() => void | Promise<void>)[] = [];
  disposals.push(async () => {
    abort.abort();
    for (const release of cleanup) {
      await release();
    }
    results.forEach(complete);
    for (const admission of pending) {
      complete(await admission.catch(() => undefined));
    }
  });
  const admit = (request: Partial<Parameters<typeof admitTestReplyTurn>[0]> = {}) => {
    const admission = admitTestReplyTurn({
      sessionKey,
      sessionId,
      expectedSessionId: sessionId,
      storePath,
      ...request,
    });
    pending.push(admission);
    void admission.then(
      (result) => results.push(result),
      () => {},
    );
    return admission;
  };
  const reserve = (policy: "followup" | "interrupt" = "followup", reservationId?: string) => {
    const input = reserveSessionControllerSource(sessionKey, {
      reservationId,
      policy: { mode: policy },
      target: captureSessionTarget({ storeScope: storePath, sessionKey, incarnation: sessionId }),
      adapter: { signal: abort.signal },
    });
    cleanup.push(async () => {
      input.claim?.operation?.complete();
      if (input.claim) {
        releaseSessionControllerClaim(input.claim);
        await input.claim.settlement.promise;
      } else {
        retireSessionControllerInput(input);
        await input.settlement.promise;
      }
    });
    return input;
  };
  const claim = async (policy: "followup" | "interrupt" = "followup") =>
    await claimSessionControllerTask(reserve(policy), () => {});
  return {
    entry,
    storePath,
    scope,
    abort,
    cleanup,
    admit,
    reserve,
    claim,
    wait: (request: Parameters<typeof admit>[0]) =>
      observe(admit({ upstreamAbortSignal: abort.signal, ...request })),
    read: () => loadSessionEntry({ storePath, sessionKey }),
    write: (value: SessionEntry) => replaceSessionEntry({ storePath, sessionKey }, value),
  };
}

it("keeps deferred owner release retries from retaining a successor", async () => {
  const deferredReleases: Promise<void>[] = [];
  const schedule = recoveryLifecycle.scheduleMainSessionRecoveryMutation;
  const scheduled = vi
    .spyOn(recoveryLifecycle, "scheduleMainSessionRecoveryMutation")
    .mockImplementation((params) => {
      const settled = createDeferred();
      deferredReleases.push(settled.promise);
      schedule({
        ...params,
        onSuccess: async (result) => {
          await params.onSuccess(result);
          settled.resolve();
        },
      });
    });
  const pendingTarget = vi
    .spyOn(recoveryOwnerRelease, "scheduleMainSessionRecoveryPendingTarget")
    .mockImplementation(() => {});
  let restoreAccessor: (() => void) | undefined;
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    const f = recoveryFixture({
      mainRestartRecovery: { cycleId: "cycle-1", revision: 1, chargedAttempts: 0 },
    });
    const owner = owned(await f.admit());
    const apply = sessionAccessor.applySessionEntryReplacements;
    const failedWrites = Array.from({ length: 3 }, () => createDeferred());
    let failures = 0;
    const accessorSpy = vi
      .spyOn(sessionAccessor, "applySessionEntryReplacements")
      .mockImplementation(async (params) => {
        const failedWrite = failedWrites[failures];
        if (failedWrite) {
          failures += 1;
          failedWrite.resolve();
          throw new Error("SQLite session entry changed before replacement");
        }
        return await apply(params);
      });
    restoreAccessor = () => accessorSpy.mockRestore();
    owner.operation.complete();
    const successor = f.admit();
    for (const [index, failedWrite] of failedWrites.entries()) {
      await failedWrite.promise;
      if (index < failedWrites.length - 1) {
        await vi.advanceTimersByTimeAsync(25 * 2 ** index);
      }
    }
    // Join real worker I/O without advancing later retry timers.
    const admitted = await successor;
    expect(deferredReleases).toHaveLength(1);
    accessorSpy.mockRestore();
    owned(admitted);
    const released = captureSessionControllerSettlement(f.scope);
    expect(released).toBeDefined();
    complete(admitted);
    await released;
  } finally {
    try {
      restoreAccessor?.();
      // Start deferred repair without firing unrelated database lease deadlines.
      await vi.advanceTimersByTimeAsync(1_000);
      await Promise.all(deferredReleases);
    } finally {
      scheduled.mockRestore();
      pendingTarget.mockRestore();
      vi.useRealTimers();
    }
  }
});

it("keeps selected input behind the recovered turn without dispatching recovery twice", async () => {
  const delivery = {
    restartRecoveryDeliveryRunId: "old-channel-claim",
    restartRecoveryDeliverySourceRunId: "old-channel-source",
  };
  const f = recoveryFixture({
    ...delivery,
    restartRecoveryDeliveryContext: { channel: "discord", to: "synthetic-channel" },
    restartRecoverySourceIngress: "channel",
  });
  const context = createRecoveryGatewayContext();
  const selected = await f.claim();
  const recoveryStarted = createDeferred();
  let recovered: ReturnType<typeof createReplyOperation> | undefined;
  let recoveryClaim: SessionControllerMailboxClaim | undefined;
  const retry = vi
    .spyOn(restartRecovery, "retryRestartAbortedMainSessionRecovery")
    .mockImplementationOnce(async (request) => {
      expect(request).toMatchObject({
        expectedSessionId: sessionId,
        expectedRecoveryRunId: "old-channel-claim",
        lane: "main",
        expectedRecoverySourceRunId: "old-channel-source",
        gatewayRuntime: context.recoveryRuntime,
      });
      const input = request.controllerInput;
      if (!input) {
        throw new Error("Recovery must receive the selected claim's predecessor");
      }
      input.custody.rpcAdopted = true;
      recoveryClaim = await claimSessionControllerTask(input, () => {});
      recovered = createReplyOperation({
        sessionKey,
        sessionId,
        resetTriggered: false,
        mailboxClaim: recoveryClaim,
        target: captureSessionTarget({
          storeScope: f.storePath,
          sessionKey,
          incarnation: sessionId,
        }),
      });
      await f.write({
        ...f.entry,
        abortedLastRun: false,
        restartRecoveryRuns: [
          {
            runId: "old-channel-claim",
            lifecycleGeneration: getAgentEventLifecycleGeneration(),
          },
        ],
      });
      recoveryStarted.resolve();
      return { started: 1, settled: 0, failed: 0, skipped: 0 };
    });
  let settled = false;
  const pending = f
    .admit({ mailboxClaim: selected, resolveGatewayContext: () => context })
    .finally(() => {
      settled = true;
    });
  try {
    await recoveryStarted.promise;
    await setImmediate();
    expect(settled).toBe(false);
    expect(retry).toHaveBeenCalledOnce();
    expect(recoveryClaim?.mailbox.claim).toBe(recoveryClaim);
    await f.write({ sessionId, updatedAt: Date.now(), status: "done" });
    recovered?.complete();
    if (recoveryClaim) {
      releaseSessionControllerClaim(recoveryClaim);
    }
    const visible = owned(await pending);
    expect(selected.mailbox.claim).toBe(selected);
    expect(visible.operation).not.toBe(recovered);
    complete(visible);
    expect(retry).toHaveBeenCalledOnce();
  } finally {
    recovered?.complete();
    if (recoveryClaim) {
      releaseSessionControllerClaim(recoveryClaim);
    }
  }
});

it.each([
  { kind: "queued_followup", failed: false },
  { kind: "visible", failed: true },
] as const)(
  "settles or defers $kind input according to recovery failure: $failed",
  async ({ kind, failed }) => {
    const f = recoveryFixture({
      restartRecoveryDeliveryRunId: "interrupted-claim",
      restartRecoveryDeliverySourceRunId: "interrupted-source",
    });
    const context = createRecoveryGatewayContext();
    const retryEntered = createDeferred();
    const retry = vi
      .spyOn(restartRecovery, "retryRestartAbortedMainSessionRecovery")
      .mockImplementation(async () => {
        retryEntered.resolve();
        return { started: 0, settled: 0, failed: failed ? 1 : 0, skipped: failed ? 0 : 1 };
      });
    const outcome = f.wait({ resolveGatewayContext: () => context, kind });
    await Promise.race([
      retryEntered.promise,
      outcome.settled.then(() => {
        throw new Error("Admission settled before recovery dispatch");
      }),
    ]);
    expect(retry).toHaveBeenCalledOnce();
    expect(retry.mock.calls[0]?.[0].lane).toBe(kind === "visible" ? "main" : undefined);
    await setImmediate();
    expect(f.read()).toMatchObject(f.entry);
    expect(getSessionControllerOperation(sessionKey)).toBeUndefined();
    if (failed) {
      await outcome.settled;
      expect(outcome.failure).toMatchObject({
        message: expect.stringMatching(/restart recovery failed/i),
      });
      expect(outcome.result).toBeUndefined();
    } else {
      expect(outcome.failure).toBeUndefined();
      await outcome.settled;
      expect(outcome.result).toEqual({ status: "skipped", reason: "active-run" });
    }
    expect(retry).toHaveBeenCalledOnce();
  },
);

it.each(["started", "cancelled", "replaced"] as const)(
  "joins reserved startup recovery before selected visible input: %s",
  async (outcome) => {
    const f = recoveryFixture({
      mainRestartRecovery: {
        cycleId: "cycle-1",
        revision: 1,
        chargedAttempts: 1,
        reservation: {
          runId: "startup-recovery",
          attempt: 1,
          lifecycleGeneration: getAgentEventLifecycleGeneration(),
        },
      },
    });
    const selected = await f.claim();
    const startup = f.reserve(
      "followup",
      resolveRestartResendReservationId({
        sessionId,
        cycleId: "cycle-1",
        attempt: 1,
      }),
    );
    const retry = vi.spyOn(restartRecovery, "retryRestartAbortedMainSessionRecovery");
    const context = createRecoveryGatewayContext();
    const admission = f.wait({
      mailboxClaim: selected,
      resolveGatewayContext: () => context,
      waitForActive: false,
    });
    await vi.waitFor(() => expect(selected.mailbox.claim).toBeUndefined());
    expect(admission.result).toBeUndefined();
    expect(admission.failure).toBeUndefined();
    expect(retry).not.toHaveBeenCalled();
    if (outcome === "cancelled") {
      f.abort.abort();
    } else {
      await f.write({
        sessionId: outcome === "replaced" ? "replacement-session" : sessionId,
        updatedAt: Date.now(),
        abortedLastRun: false,
        status: "done",
      });
      retireSessionControllerInput(startup);
    }
    await admission.settled;
    if (outcome === "replaced") {
      expect(admission.failure).toMatchObject({ code: "SESSION_WORK_START_CHANGED" });
    } else {
      expect(admission.failure).toBeUndefined();
      expect(admission.result).toMatchObject(
        outcome === "started" ? { status: "owned" } : { status: "skipped", reason: "aborted" },
      );
    }
    expect(retry).not.toHaveBeenCalled();
  },
);

it("does not consume startup recovery's source while monitoring", async () => {
  const f = recoveryFixture({ status: undefined, abortedLastRun: undefined });
  const startup = f.reserve();
  const result = await f.admit({ kind: "heartbeat" });
  expect(result).toMatchObject({ status: "skipped", reason: "active-run" });
  expect(startup.phase).toBe("preparing");
  expect(startup.retirementRequested).toBeFalsy();
  expect(f.read()?.sessionId).toBe(sessionId);
});

async function busyReply(withRecoveryFence = true) {
  const fixture = recoveryFixture({ status: undefined, abortedLastRun: false });
  const reply = owned(await fixture.admit());
  const replyReleased = captureSessionControllerSettlement(fixture.scope);
  if (!replyReleased) {
    throw new Error("Reply admission must retain its lifecycle lease");
  }
  const target = {
    sessionAgentId: "main",
    isNewSession: false,
    sessionId,
    sessionKey,
    storePath: fixture.storePath,
  };
  const lifecycleGeneration = getAgentEventLifecycleGeneration();
  const fence = [{ runId: "busy-reply", lifecycleGeneration }];
  if (withRecoveryFence) {
    await fixture.write({
      ...fixture.entry,
      restartRecoveryRuns: fence,
      restartRecoveryDeliveryRunId: "busy-reply",
    });
  }
  const commands: Promise<unknown>[] = [];
  type Command = Parameters<typeof runWithAgentCommandRecoveryOwner<typeof target, string>>[0];
  return {
    target,
    controller: fixture.abort,
    fence,
    read: fixture.read,
    finish: async (retainFence = false) => {
      if (!retainFence) {
        await fixture.write({ ...fixture.entry, status: "done" });
      }
      reply.operation.complete();
      await replyReleased;
    },
    queued: () =>
      getSessionControllerEntryForOperation(reply.operation).mailbox?.entries.filter(
        (input) => input.phase !== "consumed",
      ).length ?? 0,
    command: (runId: string, overrides: Partial<Command> = {}) => {
      const command = runWithAgentCommandRecoveryOwner({
        lifecycleGeneration,
        mode: "claim",
        opts: { message: runId, runId, abortSignal: fixture.abort.signal },
        prepare: async () => target,
        run: async () => runId,
        ...overrides,
      });
      commands.push(command);
      void command.catch(() => {});
      return command;
    },
    cleanup: async () => {
      fixture.abort.abort();
      reply.operation.complete();
      await Promise.allSettled(commands);
      await replyReleased;
    },
  };
}

it.for(["recovery fence", "worker-owned input"] as const)(
  "keeps two accepted commands in FIFO order behind a reply with %s",
  async (ownership, { signal }) => {
    const fixture = await busyReply(ownership === "recovery fence");
    const releaseFirst = createDeferred();
    const firstStarted = createDeferred();
    const order: string[] = [];
    const claim = vi.spyOn(recoveryStore, "claimMainSessionRecoveryOwner");
    try {
      const first = fixture.command("first", {
        run: async () => {
          order.push("first");
          firstStarted.resolve();
          await releaseFirst.promise;
          return "first";
        },
      });
      await vi.waitFor(() => expect(fixture.queued()).toBe(1));
      const second = fixture.command("second", {
        run: async () => {
          order.push("second");
          return "second";
        },
      });
      await vi.waitFor(() => expect(fixture.queued()).toBe(2));
      expect(order).toEqual([]);
      expect(claim).not.toHaveBeenCalled();
      await fixture.finish();
      await withinTest(firstStarted.promise, signal);
      expect(order).toEqual(["first"]);
      releaseFirst.resolve();
      await expect(withinTest(Promise.all([first, second]), signal)).resolves.toEqual([
        "first",
        "second",
      ]);
      expect(order).toEqual(["first", "second"]);
    } finally {
      releaseFirst.resolve();
      await fixture.cleanup();
    }
  },
);

it.for(["cancelled", "retained fence"] as const)(
  "does not execute the waiting command after %s",
  async (outcome, { signal }) => {
    const fixture = await busyReply();
    const run = vi.fn(async () => "ran");
    try {
      const command = fixture.command("waiting", { run });
      await vi.waitFor(() => expect(fixture.queued()).toBe(1));
      if (outcome === "cancelled") {
        fixture.controller.abort();
      } else {
        await fixture.finish(true);
      }
      await expect(withinTest(command, signal)).rejects.toMatchObject(
        outcome === "cancelled" ? { name: "AbortError" } : { code: "SESSION_WORK_START_CHANGED" },
      );
      expect(run).not.toHaveBeenCalled();
      expect(fixture.read()?.restartRecoveryRuns).toEqual(fixture.fence);
    } finally {
      await fixture.cleanup();
    }
  },
);

it("keeps a successor behind an acquired turn's delayed durable claim", async ({ signal }) => {
  const fixture = await busyReply();
  const returnClaim = createDeferred();
  const claimed = createDeferred();
  const claim = recoveryStore.claimMainSessionRecoveryOwner;
  vi.spyOn(recoveryStore, "claimMainSessionRecoveryOwner").mockImplementation(async (params) => {
    const result = await claim(params);
    if (params.runId === "racing") {
      claimed.resolve();
      await returnClaim.promise;
    }
    return result;
  });
  const run = vi.fn(async () => "ran");
  try {
    const command = fixture.command("racing", { run });
    await vi.waitFor(() => expect(fixture.queued()).toBe(1));
    await fixture.finish();
    await withinTest(
      awaitGateBeforeSettlement(claimed.promise, command, "Command skipped its durable claim"),
      signal,
    );
    const successor = fixture.command("successor", { run });
    await vi.waitFor(() => expect(fixture.queued()).toBe(2));
    expect(run).not.toHaveBeenCalled();
    returnClaim.resolve();
    await expect(withinTest(Promise.all([command, successor]), signal)).resolves.toEqual([
      "ran",
      "ran",
    ]);
    expect(run).toHaveBeenCalledTimes(2);
  } finally {
    returnClaim.resolve();
    await fixture.cleanup();
  }
});

it("returns the foreground recovery claim and releases it when visible reply work clears", async () => {
  const f = recoveryFixture({
    mainRestartRecovery: { cycleId: "cycle-1", revision: 1, chargedAttempts: 2 },
  });
  const admission = owned(await f.admit());
  const claimedEntry = f.read();
  complete(admission);
  await vi.waitFor(() => expect(f.read()?.mainRestartRecovery?.foregroundClaims).toBeUndefined());
  expect(claimedEntry?.mainRestartRecovery).toMatchObject({
    foregroundClaims: { tokens: [expect.any(String)] },
  });
  expect(admission.sessionEntry).toMatchObject({
    mainRestartRecovery: { foregroundClaims: claimedEntry?.mainRestartRecovery?.foregroundClaims },
  });
  expect(f.read()).toMatchObject({
    sessionId,
    status: "interrupted",
    abortedLastRun: true,
    mainRestartRecovery: { cycleId: "cycle-1", chargedAttempts: 2 },
  });
});

it("settles a committed recovery claim without replay when preparation changes", async ({
  signal,
}) => {
  const f = recoveryFixture({
    mainRestartRecovery: { cycleId: "cycle-1", revision: 1, chargedAttempts: 0 },
  });
  const predecessor = createReplyOperation({
    sessionKey,
    sessionId,
    resetTriggered: false,
    target: captureSessionTarget({ storeScope: f.storePath, sessionKey, incarnation: sessionId }),
  });
  const claimed = createDeferred();
  const release = createDeferred();
  const claim = recoveryStore.claimMainSessionRecoveryOwner;
  const claimSpy = vi
    .spyOn(recoveryStore, "claimMainSessionRecoveryOwner")
    .mockImplementation(async (params) => {
      const result = await claim(params);
      claimed.resolve();
      await release.promise;
      return result;
    });
  const pending = f.admit({ expectedSessionId: undefined });
  try {
    await withinTest(
      awaitGateBeforeSettlement(
        claimed.promise,
        pending,
        "Admission completed before recovery claimed ownership",
      ),
      signal,
    );
    expect(f.read()?.mainRestartRecovery).toMatchObject({
      foregroundClaims: { tokens: [expect.any(String)] },
    });
    predecessor.complete();
    release.resolve();
    await expect(withinTest(pending, signal)).rejects.toMatchObject({
      code: "SESSION_WORK_START_CHANGED",
    });
    expect(claimSpy).toHaveBeenCalledOnce();
    expect(f.read()?.mainRestartRecovery?.foregroundClaims).toBeUndefined();
    expect(getSessionControllerOperation(sessionKey)).toBeUndefined();
  } finally {
    release.resolve();
    predecessor.complete();
    await pending.catch(() => {});
  }
});

it("hands an interrupt to ordinary recovery ownership when the owed resend cannot be retired", async () => {
  const f = recoveryFixture({
    restartRecoveryDeliveryRunId: "interrupted-claim",
    restartRecoveryDeliverySourceRunId: "interrupted-source",
    mainRestartRecovery: {
      cycleId: "cycle-1",
      revision: 1,
      chargedAttempts: 1,
      foregroundClaims: {
        lifecycleGeneration: getAgentEventLifecycleGeneration(),
        tokens: ["deferred-release"],
      },
    },
  });
  const mailboxClaim = await f.claim("interrupt");
  const commit = recoveryStore.commitMainSessionRecovery;
  let retirements = 0;
  vi.spyOn(recoveryStore, "commitMainSessionRecovery").mockImplementation(async (params) => {
    if (params.command.kind === "interrupt_owed" && ++retirements > 1) {
      f.abort.abort();
    }
    return await commit(params);
  });
  const admission = owned(await f.admit({ mailboxClaim, upstreamAbortSignal: f.abort.signal }));
  expect(retirements).toBe(1);
  expect(f.read()).toMatchObject({
    abortedLastRun: true,
    status: "interrupted",
    mainRestartRecovery: {
      chargedAttempts: 1,
      foregroundClaims: { tokens: expect.arrayContaining(["deferred-release"]) },
    },
  });
  expect(f.read()?.mainRestartRecovery?.foregroundClaims?.tokens).toHaveLength(2);
  complete(admission);
});

it("preserves settled recovery delivery residue while monitoring", async () => {
  const f = recoveryFixture({
    abortedLastRun: false,
    restartRecoveryDeliveryRunId: "completed-recovery",
    restartRecoveryRuns: [
      { runId: "completed-recovery", lifecycleGeneration: getAgentEventLifecycleGeneration() },
    ],
  });
  const result = owned(await f.admit({ kind: "heartbeat" }));
  expect(f.read()).toMatchObject(f.entry);
  expect(f.read()?.mainRestartRecovery).toBeUndefined();
  complete(result);
});
