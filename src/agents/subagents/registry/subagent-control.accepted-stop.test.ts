// Preserve module setup before modules that consume it.
// oxfmt-ignore
import { runSubagentStateWorkerOperation, useSubagentControlFixture } from "./subagent-control.test-support.js";
import { expectDefined } from "@openclaw/normalization-core/expect";
import { beforeEach, expect, it, vi } from "vitest";
import {
  awaitGateBeforeSettlement,
  createDeferred,
  withinTest,
} from "../../../../test/helpers/promise.js";
import { getRuntimeConfig } from "../../../config/config.js";
import { setRuntimeConfigSnapshot } from "../../../config/runtime-snapshot.js";
import { replaceSessionEntry } from "../../../config/sessions/session-accessor.js";
import { readSessionEntryReadOnlyInWorker } from "../../../config/sessions/session-entry-read-runtime.js";
import type { SessionEntry } from "../../../config/sessions/types.js";
import { createAgentAdmissionController } from "../../../gateway/agent-turn/agent-admission-controller.js";
import { createAgentDedupeLifecycle } from "../../../gateway/agent-turn/agent-dedupe-lifecycle.js";
import { registerChatAbortController } from "../../../gateway/chat-abort.js";
import { createDirectChatContext } from "../../../gateway/server-chat.agent-events.test-helpers.js";
import { emitAgentEvent, getAgentEventLifecycleGeneration } from "../../../infra/agent-events.js";
import { createEmptyPluginRegistry } from "../../../plugins/registry-empty.js";
import { PluginRegistryInspectionResources } from "../../../plugins/registry-inspection-resources.js";
import { retireInspectionInstances } from "../../../plugins/registry-inspection.test-support.js";
import {
  bindGatewayContextResolver,
  clearGatewayContextResolver,
  getGatewayContextResolver,
  withPluginRuntimeGatewayContextResolver,
} from "../../../plugins/runtime/gateway-request-scope.js";
import { withSessionTurn } from "../../../sessions/session-controller.admission.js";
import {
  beginSessionEffect,
  captureSessionTarget,
  getSessionMutationCount,
  getSessionControllerWorkCount,
} from "../../../sessions/session-controller.lifecycle.js";
import {
  retireSessionControllerInput,
  trackSessionControllerSourceWork,
} from "../../../sessions/session-controller.mailbox.js";
import {
  getRpcSource,
  type RpcSourceRef,
} from "../../../sessions/session-controller.rpc-sources.js";
import { markReplyOperationExecutionStarted } from "../../../sessions/session-controller.state.js";
import { rpcSourceTesting } from "../../../sessions/session-lifecycle-admission.test-support.js";
import { isSubagentRegistryWriteCommand } from "../../subagent-test-fixtures.test-helpers.js";
import * as subagentKillSettlement from "./subagent-control-kill-settlement.js";
import { prepareSubagentKillSession } from "./subagent-control-session.js";
import { killAllControlledSubagentRuns, killSubagentRunAdmin } from "./subagent-control.js";
import { SUBAGENT_ENDED_REASON_KILLED } from "./subagent-lifecycle-events.js";
import {
  captureSubagentExecution,
  retireSubagentGatewayBinding,
} from "./subagent-registry-execution-cleanup.js";
import { subagentRuns } from "./subagent-registry-memory.js";
import { markSubagentRunTerminated, registerSubagentRun } from "./subagent-registry.js";
import { writeSubagentSessionEntry } from "./subagent-registry.persistence.test-support.js";
import { rowToSubagentRunRecord } from "./subagent-registry.store.codec.js";
import { resolveSubagentSessionStatus } from "./subagent-session-metrics.js";

const fixture = useSubagentControlFixture();
const executionJoinObservers = new Map<RpcSourceRef, () => void>();
beforeEach(() => {
  executionJoinObservers.clear();
  const settleSources = subagentKillSettlement.settleSubagentKillSources;
  vi.spyOn(subagentKillSettlement, "settleSubagentKillSources").mockImplementation((params) => {
    if (params.execution) {
      executionJoinObservers.get(params.execution)?.();
    }
    return settleSources(params);
  });
  setRuntimeConfigSnapshot({ agents: { defaults: { workspace: fixture.stateDir } } });
});

async function seedExecutionTarget(
  name: string,
  task: string,
  lifecycleRevision?: string,
  sessionKey = `agent:main:subagent:${name}`,
) {
  const sessionId = `${name}-session`;
  const runId = `${name}-run`;
  const storePath = await writeSubagentSessionEntry({
    stateDir: fixture.stateDir,
    agentId: "main",
    sessionKey,
    defaultSessionId: sessionId,
    lifecycleRevision,
  });
  const sessionEntry = await readSessionEntryReadOnlyInWorker({
    agentId: "main",
    storePath,
    sessionKey,
  });
  await registerSubagentRun({
    runId,
    childSessionKey: sessionKey,
    childAgentId: "main",
    sessionEntry,
    requesterSessionKey: "agent:main:main",
    requesterAgentId: "main",
    requesterDisplayKey: "main",
    task,
    cleanup: "keep",
    expectsCompletionMessage: false,
  });
  const context = createDirectChatContext({ getRuntimeConfig });
  const resolveGatewayContext = () => context;
  bindGatewayContextResolver(subagentRuns.get(runId)!, resolveGatewayContext);
  return { sessionKey, sessionId, sessionEntry, runId, storePath, context, resolveGatewayContext };
}

it.each([
  { original: undefined, current: undefined, matches: true },
  { original: "original-revision", current: "original-revision", matches: true },
  { original: undefined, current: "successor-revision", matches: false },
  { original: "original-revision", current: undefined, matches: false },
])(
  "retains cleanup custody only for the recorded revision ($original -> $current)",
  async ({ original, current, matches }) => {
    const { sessionKey, sessionId, sessionEntry, runId, storePath } = await seedExecutionTarget(
      "cleanup-revision",
      "Keep exact execution cleanup custody",
      original,
    );
    const entry = subagentRuns.get(runId)!;
    expect(entry.childSessionIdentity).toEqual({ sessionId, lifecycleRevision: original });
    const registration = registerChatAbortController({
      target: captureSessionTarget({
        storeScope: storePath,
        sessionKey,
        agentId: "main",
        incarnation: sessionId,
      }),
      runId,
      sessionKey,
      sessionId,
      kind: "agent",
      timeoutMs: 60_000,
    });
    const releaseTail = createDeferred();
    const { completion: execution } = await startSourceExecution(
      registration.entry!,
      () => releaseTail.promise,
      registration.cleanup,
    );
    try {
      retireSubagentGatewayBinding(entry);
      await replaceSessionEntry(
        { agentId: "main", storePath, sessionKey },
        { ...sessionEntry!, lifecycleRevision: current },
      );
      const preparingSession = prepareSubagentKillSession(
        getRuntimeConfig(),
        sessionKey,
        () => {},
        undefined,
        "main",
        entry.childSessionIdentity,
      );
      if (!matches) {
        await expect(preparingSession).rejects.toThrow(
          "Subagent original session incarnation is unresolved or changed",
        );
        return;
      }
      const session = await preparingSession;
      try {
        const cleanup = captureSubagentExecution({ entry, session });
        expect(cleanup).toBeDefined();
        expect(cleanup?.execution).toBe(registration.entry);
        expect(cleanup?.execution.input.settlement.promise).toBe(
          registration.entry?.input.settlement.promise,
        );
      } finally {
        await session.release();
      }
    } finally {
      releaseTail.resolve();
      await execution;
      registration.cleanup();
    }
  },
);

async function startSourceExecution(
  entry: RpcSourceRef,
  run: () => Promise<void>,
  cleanup: () => void,
) {
  const started = createDeferred();
  const target = entry.input.target!;
  const completion = withSessionTurn(
    {
      sessionKey: target.sessionKey,
      sessionId: target.incarnation,
      agentId: target.agentId,
      target,
      controllerInput: entry.input,
    },
    async (operation) => {
      markReplyOperationExecutionStarted(expectDefined(operation, "claimed source operation"));
      started.resolve();
      try {
        await run();
      } finally {
        cleanup();
      }
    },
  );
  void completion.catch(() => {});
  trackSessionControllerSourceWork(entry.input, completion);
  await awaitGateBeforeSettlement(
    started.promise,
    completion,
    "Source did not enter its real controller turn",
  );
  return { completion };
}

function observeExecutionJoin(entry: RpcSourceRef, selected: () => void) {
  const settlement = entry.input.settlement;
  const completion = settlement.promise;
  let state: "pending" | "fulfilled" | "rejected" = "pending";
  void completion.then(
    () => {
      state = "fulfilled";
    },
    () => {
      state = "rejected";
    },
  );
  // Observe the cleanup owner joining this source, not earlier reads while capturing it.
  executionJoinObservers.set(entry, selected);
  return {
    get status() {
      return state;
    },
  };
}

it.each(["abort", "interruption", "replacement", "already terminal"] as const)(
  "joins accepted native Stop cleanup before refusing revoked publication (%s)",
  async (mode) => {
    const { sessionKey, sessionId, runId, storePath } = await seedExecutionTarget(
      "accepted-stop",
      "retain the selected cancellation outcome",
      "accepted-stop-revision",
    );
    const registration = registerChatAbortController({
      target: captureSessionTarget({
        storeScope: storePath,
        sessionKey,
        agentId: "main",
        incarnation: sessionId,
      }),
      runId,
      sessionKey,
      sessionId,
      kind: "agent",
      timeoutMs: 60_000,
    });
    const owner = registration.entry!;
    const releaseTail = createDeferred();
    const selectedTail = createDeferred();
    const { completion: execution } = await startSourceExecution(
      owner,
      async () => {
        await releaseTail.promise;
        registration.cleanup();
      },
      registration.cleanup,
    );
    let observingStop = false;
    const settlement = observeExecutionJoin(owner, () => {
      if (observingStop) {
        selectedTail.resolve();
      }
    });
    const admission =
      mode === "interruption"
        ? await beginSessionEffect({
            scope: storePath,
            identities: [sessionKey, sessionId],
            assertAllowed: () => {},
            onInterrupt: () => {
              registration.controller.abort();
              admission?.release();
              return { runId };
            },
          })
        : undefined;
    let termination: ReturnType<typeof markSubagentRunTerminated> | undefined;
    let pending: ReturnType<typeof killSubagentRunAdmin> | undefined;
    let callerAuthorized = true;
    const onResult = vi.fn(() => undefined);
    try {
      if (mode === "already terminal") {
        termination = markSubagentRunTerminated({ runId, reason: "killed" });
        expect(await termination).toBe(1);
      }
      observingStop = true;
      pending = killSubagentRunAdmin(
        { cfg: getRuntimeConfig(), sessionKey, expectedRunId: runId, onResult },
        {
          assertCurrent: () => {
            if (!callerAuthorized) {
              throw new Error("Caller control revoked during raw execution settlement.");
            }
          },
        },
      );
      await awaitGateBeforeSettlement(
        selectedTail.promise,
        pending,
        "Stop returned before joining its exact raw execution",
      );
      expect(settlement.status).toBe("pending");
      expect(getSessionMutationCount()).toBe(0);
      expect(owner.input.abortSignal.aborted).toBe(mode !== "already terminal");
      callerAuthorized = false;
      if (mode === "replacement") {
        rpcSourceTesting.deleteExpected(runId, owner);
        registerChatAbortController({
          target: owner.input.target,
          runId,
          sessionKey,
          sessionId,
          kind: "agent",
          timeoutMs: 60_000,
        });
      }
      releaseTail.resolve();
      await expect(pending).rejects.toThrow("Caller control revoked");
      expect(settlement.status).toBe("fulfilled");
      expect(subagentRuns.get(runId)?.execution.status).toBe("terminal");
      expect(subagentRuns.get(runId)?.endedReason).toBe(SUBAGENT_ENDED_REASON_KILLED);
      expect(onResult).not.toHaveBeenCalled();
      if (mode === "replacement") {
        expect(getRpcSource(runId)?.input.abortSignal.aborted).toBe(false);
      } else if (mode === "already terminal") {
        expect(owner.input.abortSignal.aborted).toBe(false);
      }
    } finally {
      releaseTail.resolve();
      admission?.release();
      await Promise.allSettled([termination, pending, execution]);
      registration.cleanup();
      if (mode === "replacement") {
        const replacement = getRpcSource(runId);
        if (replacement) {
          retireSessionControllerInput(replacement.input);
        }
      }
      expect(getSessionControllerWorkCount()).toBe(0);
      expect(getSessionMutationCount()).toBe(0);
    }
  },
);

it.for(["canonical", "raw"] as const)(
  "joins only the cancelled %s source when its abort revokes caller authority",
  async (kind, { signal }) => {
    const { sessionKey, sessionId, runId, storePath } = await seedExecutionTarget(
      "partial-revocation",
      "Settle accepted cancellation without waiting for untouched preparation",
      "partial-revocation-revision",
      kind === "raw" ? "global" : undefined,
    );
    const target = captureSessionTarget({
      storeScope: storePath,
      sessionKey,
      agentId: "main",
      incarnation: sessionId,
    });
    const cancelled = registerChatAbortController({
      target,
      runId,
      sessionKey,
      sessionId,
      kind: "agent",
      timeoutMs: 60_000,
    });
    const untouched = registerChatAbortController({
      target,
      runId: `${runId}-untouched`,
      sessionKey,
      sessionId,
      kind: "agent",
      timeoutMs: 60_000,
    });
    const entered = createDeferred();
    const aborted = createDeferred();
    const joinedCancelled = createDeferred();
    const releaseCancelled = createDeferred();
    const releaseUntouched = createDeferred();
    let callerAuthorized = true;
    cancelled.controller.signal.addEventListener(
      "abort",
      () => {
        callerAuthorized = false;
        aborted.resolve();
      },
      { once: true },
    );
    const cancelledWork = withSessionTurn(
      { sessionKey, agentId: "main", target, controllerInput: cancelled.entry!.input },
      async (operation) => {
        expect(operation).toBeUndefined();
        entered.resolve();
        try {
          await releaseCancelled.promise;
        } finally {
          cancelled.cleanup();
        }
      },
    );
    trackSessionControllerSourceWork(cancelled.entry!.input, cancelledWork);
    const untouchedWork = releaseUntouched.promise.then(() => untouched.cleanup());
    trackSessionControllerSourceWork(untouched.entry!.input, untouchedWork);
    const cancelledSettlement = observeExecutionJoin(cancelled.entry!, () => {
      if (!callerAuthorized) {
        joinedCancelled.resolve();
      }
    });
    const untouchedSettlement = observeExecutionJoin(untouched.entry!, () => {});
    let pending: ReturnType<typeof killSubagentRunAdmin> | undefined;
    const onResult = vi.fn(() => undefined);
    try {
      await withinTest(
        awaitGateBeforeSettlement(entered.promise, cancelledWork, "Preparation did not start"),
        signal,
      );
      expect(cancelled.entry!.input.claim?.operation).toBeUndefined();
      expect(cancelled.entry!.input.claim).toBeDefined();
      expect(untouched.entry!.input.claim).toBeUndefined();
      pending = killSubagentRunAdmin(
        { cfg: getRuntimeConfig(), sessionKey, agentId: "main", expectedRunId: runId, onResult },
        {
          assertCurrent: () => {
            if (!callerAuthorized) {
              throw new Error("Caller control revoked by accepted source cancellation.");
            }
          },
        },
      );
      await withinTest(
        awaitGateBeforeSettlement(aborted.promise, pending, "Stop returned before cancellation"),
        signal,
      );
      await withinTest(
        awaitGateBeforeSettlement(
          joinedCancelled.promise,
          pending,
          "Stop returned before joining its accepted source",
        ),
        signal,
      );
      expect(cancelledSettlement.status).toBe("pending");
      expect(untouchedSettlement.status).toBe("pending");
      expect(untouched.controller.signal.aborted).toBe(false);
      releaseCancelled.resolve();
      await expect(withinTest(pending, signal)).rejects.toThrow("Caller control revoked");
      expect(cancelledSettlement.status).toBe("fulfilled");
      expect(untouchedSettlement.status).toBe("pending");
      expect(untouched.controller.signal.aborted).toBe(false);
      expect(untouched.entry!.input.retirementRequested).not.toBe(true);
      expect(onResult).not.toHaveBeenCalled();
    } finally {
      releaseCancelled.resolve();
      releaseUntouched.resolve();
      await Promise.allSettled([pending, cancelledWork, untouchedWork]);
      cancelled.cleanup();
      untouched.cleanup();
      expect(getSessionControllerWorkCount()).toBe(0);
      expect(getSessionMutationCount()).toBe(0);
    }
  },
);

it.each(["declined", "throws"] as const)(
  "joins a Stop accepted by the session callback (outcome: %s)",
  async (mode) => {
    const { sessionKey, sessionId, runId, storePath } = await seedExecutionTarget(
      "callback-stop",
      "retain callback cancellation until raw disposal settles",
      "callback-stop-revision",
    );
    const registration = registerChatAbortController({
      target: captureSessionTarget({
        storeScope: storePath,
        sessionKey,
        agentId: "main",
        incarnation: sessionId,
      }),
      runId,
      sessionKey,
      sessionId,
      kind: "agent",
      timeoutMs: 60_000,
    });
    const owner = registration.entry!;
    const releaseTail = createDeferred();
    const selectedTail = createDeferred();
    const { completion: execution } = await startSourceExecution(
      owner,
      async () => {
        await releaseTail.promise;
        registration.cleanup();
      },
      registration.cleanup,
    );
    const settlement = observeExecutionJoin(owner, selectedTail.resolve);
    const pending = killSubagentRunAdmin(
      { cfg: getRuntimeConfig(), sessionKey, expectedRunId: runId },
      {
        assertCurrent: () => {},
        beforeSessionKill: () => {
          registration.controller.abort();
          if (mode === "declined") {
            return false;
          }
          throw new Error("callback failed after Stop");
        },
      },
    );
    try {
      await awaitGateBeforeSettlement(
        selectedTail.promise,
        pending,
        "Stop returned before joining its exact raw execution",
      );
      expect(settlement.status).toBe("pending");
      expect(getSessionMutationCount()).toBe(0);
      expect(owner.input.abortSignal.aborted).toBe(true);
      releaseTail.resolve();
      const result = await pending;
      expect(result).toMatchObject({ found: true, killed: false });
      if (mode !== "declined") {
        expect(result).toHaveProperty("error", expect.any(String));
      }
    } finally {
      releaseTail.resolve();
      await Promise.allSettled([pending, execution]);
      registration.cleanup();
      expect(getSessionControllerWorkCount()).toBe(0);
      expect(getSessionMutationCount()).toBe(0);
    }
  },
);

it("joins an execution registered while its accepted Stop awaits kill-claim persistence", async () => {
  const { sessionKey, sessionId, sessionEntry, runId, storePath, context, resolveGatewayContext } =
    await seedExecutionTarget(
      "late-execution-stop",
      "join the exact execution that accepts cancellation",
      "late-execution-stop-revision",
    );
  const lifecycleGeneration = getAgentEventLifecycleGeneration();
  const io = { emitAcceptance: vi.fn(), emitFinal: vi.fn() };
  const agentDedupeKeys = [`agent:${runId}`];
  const dedupeLifecycle = createAgentDedupeLifecycle({
    cfg: getRuntimeConfig(),
    request: { message: "Continue admitted work", idempotencyKey: runId },
    runId,
    agentDedupeKeys,
    suppressVisibleSessionEffects: false,
    context,
    io,
  });
  dedupeLifecycle.reserve(sessionKey, "main");
  dedupeLifecycle.bindSessionTarget({ sessionKey, sessionId, agentId: "main" });
  const claimCommitted = createDeferred();
  const releaseClaim = createDeferred();
  const interrupted = createDeferred();
  const releaseTail = createDeferred();
  const selectedTail = createDeferred();
  const onAbort = () => interrupted.resolve();
  let heldClaim = false;
  let admission: Awaited<ReturnType<typeof beginSessionEffect>> | undefined;
  let registration: ReturnType<typeof registerChatAbortController> | undefined;
  let execution: Promise<void> | undefined;
  let pending: ReturnType<typeof killSubagentRunAdmin> | undefined;
  let stopSettled = false;
  const onResult = vi.fn(() => undefined);
  try {
    // The fence blocks new admission. This existing lease is adopted after capture.
    admission = await beginSessionEffect({
      scope: storePath,
      identities: [sessionKey, sessionId],
      resolveGatewayContext,
      assertAllowed: () => {},
    });
    let admissionEntry: SessionEntry | undefined = sessionEntry;
    const nativeAdmission = createAgentAdmissionController({
      runId,
      agentDedupeKeys,
      expectedSession: { handoffId: admission.createHandoff(), sessionId },
      context,
      io,
      dedupeLifecycle,
      getRequestedSessionKey: () => sessionKey,
      getResolvedSessionKey: () => sessionKey,
      getResolvedSessionId: () => sessionId,
      getSessionEntry: () => admissionEntry,
      setSessionEntry: (entry) => {
        admissionEntry = entry;
      },
      getResolvedSessionAgentId: () => "main",
      getAgentId: () => "main",
      getSessionPersisted: () => true,
      getSupersededSessionId: () => undefined,
      setAdmittedSessionId: (value) => expect(value).toBe(sessionId),
    });
    fixture.worker.mockImplementation((stateContext, operation, options) =>
      runSubagentStateWorkerOperation(
        stateContext,
        (scope) =>
          operation({
            execute: async (command, commandOptions) => {
              const row = isSubagentRegistryWriteCommand(command)
                ? command.input.values.find((candidate) => candidate.run_id === runId)
                : undefined;
              const claim = row && rowToSubagentRunRecord(row)?.killIntent;
              const holdClaim = Boolean(claim) && !heldClaim;
              heldClaim ||= holdClaim;
              const receipt = await scope.execute(command, commandOptions);
              if (holdClaim) {
                claimCommitted.resolve();
                await releaseClaim.promise;
              }
              return receipt;
            },
          }),
        options,
      ),
    );
    pending = killSubagentRunAdmin({
      cfg: getRuntimeConfig(),
      sessionKey,
      expectedRunId: runId,
      onResult,
    });
    void pending.then(
      () => {
        stopSettled = true;
      },
      () => {
        stopSettled = true;
      },
    );
    await awaitGateBeforeSettlement(
      claimCommitted.promise,
      pending,
      "Stop returned before the native kill claim committed",
    );
    expect(getRpcSource(runId)).toBeUndefined();
    expect(getSessionMutationCount()).toBe(1);
    await nativeAdmission.acquire(storePath);
    expect(nativeAdmission.getAdmission()).toBe(admission);
    expect(nativeAdmission.assertAllowed()).toMatchObject({ sessionId });
    expect(nativeAdmission.hasOutcome()).toBe(false);
    const activeRegistration = registerChatAbortController({
      target: captureSessionTarget({
        storeScope: storePath,
        sessionKey,
        agentId: "main",
        incarnation: sessionId,
      }),
      runId,
      sessionKey,
      sessionId,
      lifecycleGeneration,
      kind: "agent",
      timeoutMs: 60_000,
    });
    registration = activeRegistration;
    nativeAdmission.setAdmittedRunAbort(activeRegistration);
    const owner = activeRegistration.entry!;
    owner.input.abortSignal.addEventListener("abort", onAbort, { once: true });
    // Request preparation is already running under the adopted effect; cancellation
    // must retain its raw disposal even though no model turn has started yet.
    execution = (async () => {
      try {
        await interrupted.promise;
        activeRegistration.cleanup();
        nativeAdmission.release();
        await releaseTail.promise;
      } finally {
        activeRegistration.cleanup();
      }
    })();
    trackSessionControllerSourceWork(owner.input, execution);
    const settlement = observeExecutionJoin(owner, selectedTail.resolve);
    releaseClaim.resolve();
    await awaitGateBeforeSettlement(
      selectedTail.promise,
      pending,
      "Stop returned before joining its late execution",
    );
    expect(owner.input.abortSignal.aborted).toBe(true);
    expect(owner.adapter.abortStopReason).toBe("rpc");
    expect(settlement.status).toBe("pending");
    expect(getSessionControllerWorkCount()).toBe(0);
    expect(getSessionMutationCount()).toBe(0);
    expect(stopSettled).toBe(false);
    expect(onResult).not.toHaveBeenCalled();
    releaseTail.resolve();
    await expect(pending).resolves.toMatchObject({ found: true, killed: true });
    await execution;
    expect(settlement.status).toBe("fulfilled");
    expect(subagentRuns.get(runId)?.endedReason).toBe(SUBAGENT_ENDED_REASON_KILLED);
    expect(getRpcSource(runId)).toBeUndefined();
    expect(onResult).toHaveBeenCalledOnce();
  } finally {
    releaseClaim.resolve();
    interrupted.resolve();
    releaseTail.resolve();
    admission?.release();
    await Promise.allSettled([pending, execution]);
    registration?.controller.signal.removeEventListener("abort", onAbort);
    registration?.cleanup();
    fixture.worker.mockImplementation(runSubagentStateWorkerOperation);
    dedupeLifecycle.clearUnaccepted();
    expect(getSessionControllerWorkCount()).toBe(0);
    expect(getSessionMutationCount()).toBe(0);
  }
});

it.each([
  { mode: "completed", startsAfterCleanup: false },
  { mode: "self-disposal", startsAfterCleanup: true },
  { mode: "self-disposal during wake cancellation", startsAfterCleanup: true },
  { mode: "self-disposal after registry binding loss", startsAfterCleanup: true },
  { mode: "completed callback error", startsAfterCleanup: true },
  { mode: "retained error", startsAfterCleanup: true },
  { mode: "controller replaced", startsAfterCleanup: true },
] as const)(
  "normal terminal cleanup preserves exact Stop ownership ($mode, afterCleanup=$startsAfterCleanup)",
  async ({ mode, startsAfterCleanup }) => {
    const registryBindingLoss = mode === "self-disposal after registry binding loss";
    const retiresDuringStop =
      mode === "self-disposal during wake cancellation" || registryBindingLoss;
    const selfDisposal = mode === "self-disposal" || retiresDuringStop;
    const completed = mode === "completed" || selfDisposal;
    if (retiresDuringStop) {
      fixture.wake.mockResolvedValue(false);
    }
    const requester = "agent:main:main";
    const key = (id: string) => `agent:main:subagent:${id}`;
    const controller = {
      controllerSessionKey: requester,
      controllerAgentId: "main",
      callerSessionKey: requester,
      callerIsSubagent: false,
      controlScope: "children" as const,
    };
    let rootStorePath = "";
    for (const id of ["root", "child", "healthy"]) {
      const storePath = await writeSubagentSessionEntry({
        stateDir: fixture.stateDir,
        agentId: "main",
        sessionKey: key(id),
        defaultSessionId: `${id}-session`,
        lifecycleRevision: `${id}-revision`,
      });
      if (id === "root") {
        rootStorePath = storePath;
      }
      await registerSubagentRun({
        runId: id,
        childSessionKey: key(id),
        childAgentId: "main",
        requesterSessionKey: id === "child" ? key("root") : requester,
        requesterAgentId: "main",
        requesterDisplayKey: requester,
        task: "retained cleanup ownership",
        cleanup: "keep",
        collect: id !== "root" || !retiresDuringStop,
        sessionEntry: await readSessionEntryReadOnlyInWorker({
          agentId: "main",
          storePath,
          sessionKey: key(id),
        }),
        expectsCompletionMessage: false,
      });
    }
    const root = subagentRuns.get("root")!;
    const context = createDirectChatContext({ getRuntimeConfig });
    const resolver = () => context;
    bindGatewayContextResolver(root, resolver);
    const registrationParams = {
      target: captureSessionTarget({
        storeScope: rootStorePath,
        sessionKey: key("root"),
        agentId: "main",
        incarnation: "root-session",
      }),
      runId: "root",
      sessionKey: key("root"),
      sessionId: "root-session",
      kind: "agent" as const,
      timeoutMs: 60_000,
    };
    const registration = withPluginRuntimeGatewayContextResolver(resolver, () =>
      registerChatAbortController(registrationParams),
    );
    expect(getGatewayContextResolver(registration.entry!.input)).toBe(resolver);
    const releaseTail = createDeferred();
    const tailSelected = createDeferred();
    const cleanupEntered = createDeferred();
    const releaseCleanup = createDeferred();
    const failure = new Error("raw disposal callback failed");
    let callbackCompleted = false;
    let pending: ReturnType<typeof killAllControlledSubagentRuns> | undefined;
    let stopReturned = false;
    let bindingLost = false;
    const stop = () =>
      killAllControlledSubagentRuns({
        cfg: getRuntimeConfig(),
        controller,
        runs: [root],
        ...(retiresDuringStop ? { suppressTaskDelivery: true } : {}),
      });
    let resources: PluginRegistryInspectionResources | undefined;
    if (mode === "completed callback error") {
      const inspection = new PluginRegistryInspectionResources(retireInspectionInstances);
      resources = inspection;
      inspection.attach(createEmptyPluginRegistry());
      inspection.runRegistration("completed-cleanup", () => {
        inspection.register("completed-cleanup", {
          id: "callback",
          dispose: async () => {
            await releaseTail.promise;
            callbackCompleted = true;
            throw failure;
          },
        });
      });
    }
    const { completion: rawExecution } = await startSourceExecution(
      registration.entry!,
      async () => {
        registration.cleanup();
        await releaseTail.promise;
        if (mode === "retained error") {
          throw failure;
        }
        if (selfDisposal) {
          const selected = expectDefined(subagentRuns.get(root.runId), "terminal cleanup row");
          expect(getGatewayContextResolver(selected)).toBe(
            retiresDuringStop ? resolver : undefined,
          );
          if (retiresDuringStop) {
            expect(selected.requesterSettleWake).toBeDefined();
          }
          if (registryBindingLoss) {
            fixture.worker.mockImplementation((stateContext, operation, options) =>
              runSubagentStateWorkerOperation(
                stateContext,
                (scope) =>
                  operation({
                    execute: async (command, commandOptions) => {
                      const row = isSubagentRegistryWriteCommand(command)
                        ? command.input.values.find((candidate) => candidate.run_id === root.runId)
                        : undefined;
                      const next = row && rowToSubagentRunRecord(row);
                      const receipt = await scope.execute(command, commandOptions);
                      if (next?.suppressCompletionDelivery && !next.requesterSettleWake) {
                        const current = expectDefined(subagentRuns.get(root.runId), "wake owner");
                        expect(getGatewayContextResolver(current)).toBe(resolver);
                        // Retiring the registry projection does not revoke the controller input.
                        clearGatewayContextResolver(current);
                        bindingLost = true;
                      }
                      return receipt;
                    },
                  }),
                options,
              ),
            );
          }
          pending = stop();
          const result = await pending;
          stopReturned = true;
          const retired = expectDefined(subagentRuns.get(root.runId), "retired cleanup row");
          expect(getGatewayContextResolver(retired)).toBeUndefined();
          expect(retired.requesterSettleWake).toBeUndefined();
          expect(settlement.status).toBe("pending");
          expect(getRpcSource(root.runId)).toBe(registration.entry);
          expect(getGatewayContextResolver(registration.entry!.input)).toBe(resolver);
          if (registryBindingLoss) {
            expect(bindingLost).toBe(true);
          }
          if (result.status === "error") {
            throw new Error(result.error);
          }
        }
        await resources?.release();
      },
      registration.cleanup,
    );
    const execution = rawExecution.catch((error: unknown) => error);
    let observingStop = false;
    const settlement = observeExecutionJoin(registration.entry!, () => {
      if (observingStop) {
        tailSelected.resolve();
      }
    });
    fixture.cleanup.mockImplementationOnce(async () => {
      cleanupEntered.resolve();
      await releaseCleanup.promise;
    });
    const startStop = async () => {
      observingStop = true;
      pending = stop();
      await awaitGateBeforeSettlement(
        tailSelected.promise,
        pending.then(() => {
          stopReturned = true;
        }),
        "Stop returned before its retired execution settled",
      );
    };
    try {
      emitAgentEvent({
        runId: root.runId,
        stream: "lifecycle",
        data: {
          phase: "end",
          endedAt: Date.now(),
          terminalReply: { disposition: "visible", text: "completed" },
        },
      });
      await cleanupEntered.promise;
      expect(subagentRuns.get(root.runId)?.execution.outcome?.status).toBe("ok");
      expect(getGatewayContextResolver(subagentRuns.get(root.runId)!)).toBe(resolver);
      if (!startsAfterCleanup) {
        await startStop();
      }
      releaseCleanup.resolve();
      await fixture.settle();
      expect(subagentRuns.get(root.runId)?.cleanupCompletedAt).toBeTypeOf("number");
      expect(getGatewayContextResolver(subagentRuns.get(root.runId)!)).toBe(
        retiresDuringStop ? resolver : undefined,
      );
      expect(settlement.status).toBe("pending");
      expect(resolveSubagentSessionStatus(subagentRuns.get("child"))).toBe("running");
      if (startsAfterCleanup && !selfDisposal) {
        await startStop();
      }
      expect(stopReturned).toBe(false);
      expect(settlement.status).toBe("pending");
      expect(resolveSubagentSessionStatus(subagentRuns.get("child"))).toBe("running");
      if (mode === "controller replaced") {
        rpcSourceTesting.deleteExpected(root.runId, registration.entry!);
        registerChatAbortController(registrationParams);
      }
      releaseTail.resolve();
      if (selfDisposal) {
        expect(await execution).toBeUndefined();
        expect(stopReturned).toBe(true);
      }
      const result = await pending;
      const executionResult = await execution;
      const cleanupCompleted = completed || mode === "completed callback error";
      expect(settlement.status).toBe(
        mode === "retained error" || mode === "completed callback error" ? "rejected" : "fulfilled",
      );
      expect(result).toMatchObject({
        status: completed ? "ok" : "error",
        killed: cleanupCompleted ? 1 : 0,
      });
      if (mode === "completed callback error") {
        expect(callbackCompleted).toBe(true);
        expect(result).toHaveProperty("error", expect.stringContaining(failure.message));
      } else if (mode === "retained error") {
        expect(executionResult).toBe(failure);
        expect(result).toHaveProperty("error", expect.stringContaining(failure.message));
      } else if (!cleanupCompleted) {
        expect(result).toHaveProperty("error", expect.stringContaining("owner changed"));
      }
      expect(resolveSubagentSessionStatus(subagentRuns.get("child"))).toBe(
        cleanupCompleted ? "killed" : "running",
      );
      expect(resolveSubagentSessionStatus(subagentRuns.get("healthy"))).toBe("running");
      if (mode === "controller replaced") {
        expect(getRpcSource(root.runId)).not.toBe(registration.entry);
      }
    } finally {
      releaseCleanup.resolve();
      releaseTail.resolve();
      await Promise.allSettled([pending, execution]);
      fixture.worker.mockImplementation(runSubagentStateWorkerOperation);
      registration.cleanup();
      if (mode === "controller replaced") {
        const replacement = getRpcSource(root.runId);
        if (replacement) {
          retireSessionControllerInput(replacement.input);
        }
      }
      await resources?.release().catch(() => {});
    }
  },
);
