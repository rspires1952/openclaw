import { describe, expect, it, vi } from "vitest";
import { createQueueTestRun } from "../auto-reply/reply/queue.test-helpers.js";
import { enqueueFollowupRun } from "../auto-reply/reply/queue/enqueue.js";
import { setRuntimeConfigSnapshot } from "../config/runtime-snapshot.js";
import { resolveSessionStorePathCore } from "../config/sessions/paths.js";
import {
  appendTranscriptMessageSync,
  loadTranscriptEventsSync,
  replaceSessionEntry,
} from "../config/sessions/session-accessor.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { onAgentEvent } from "../infra/agent-events.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { captureSessionTarget } from "../sessions/session-controller.lifecycle.js";
import {
  bindSessionControllerSource,
  retireSessionControllerSourceCancellation,
  retireSessionControllerInput,
} from "../sessions/session-controller.mailbox.js";
import type { RpcSourceRef } from "../sessions/session-controller.rpc-sources.js";
import {
  isRpcSourceQueued,
  setRpcSourceProjectSessionActive,
  updateRpcSourceSessionId,
} from "../sessions/session-controller.rpc-sources.js";
import { rpcSourceTesting } from "../sessions/session-lifecycle-admission.test-support.js";
import { AsyncWorkScope } from "../shared/async-work-scope.js";
import { createDeferredCore } from "../shared/deferred.js";
import { ensureProfileForEmail } from "../state/user-profiles.js";
import { withOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import { registerChatAbortController } from "./chat-abort.js";
import { retainGatewayOperatorRun } from "./operator-run-cancellation.js";
import { createChatRunState } from "./server-chat-state.js";
import { createChatSendWorkAdmission } from "./server-methods/chat-send-work-admission.js";
import { claimRpcSourceForTest } from "./test-helpers.rpc-source.js";

async function withCancellationFixture(
  run: (fixture: Awaited<ReturnType<typeof createCancellationFixture>>) => Promise<void>,
): Promise<void> {
  await withOpenClawTestState({ label: "operator-run-cancellation" }, async (state) => {
    const cfg: OpenClawConfig = {
      agents: { defaults: { workspace: state.workspaceDir }, entries: { main: {} } },
    };
    await state.writeConfig(cfg);
    setRuntimeConfigSnapshot(cfg);
    const fixture = await createCancellationFixture(cfg);
    try {
      await run(fixture);
    } finally {
      fixture.release();
      await fixture.execution.drain();
      fixture.cleanup();
    }
  });
}

async function createCancellationFixture(cfg: OpenClawConfig) {
  const profile = ensureProfileForEmail("guest-cancellation@example.test");
  const scope = {
    agentId: "main",
    sessionId: "guest-and-staff-session",
    sessionKey: "agent:main:guest-and-staff",
  };
  await replaceSessionEntry(scope, { sessionId: scope.sessionId, updatedAt: 1 });
  const savedInput = { role: "user", content: [{ type: "text", text: "Saved work stays here." }] };
  expect(
    appendTranscriptMessageSync(scope, { eventId: "saved-input", message: savedInput }),
  ).toMatchObject({ ok: true });
  const chatRunState = createChatRunState();
  const execution = new AsyncWorkScope();
  const logGateway = createSubsystemLogger("test/operator-run-cancellation");
  const warn = vi.spyOn(logGateway, "warn").mockImplementation(() => {});
  const context: Parameters<typeof retainGatewayOperatorRun>[0]["context"] = {
    chatRunState,
    agentRunSeq: new Map(),
    broadcast: vi.fn(),
    nodeSendToSession: vi.fn(),
    removeChatRun: (sessionId, runId, sessionKey) =>
      chatRunState.registry.remove(sessionId, runId, sessionKey),
    cancelRunBoundApprovals: vi.fn().mockResolvedValue(0),
    getRuntimeConfig: () => cfg,
    trackExecution: (work) => execution.track(work),
    logGateway,
  };
  const registrations: Array<ReturnType<typeof registerChatAbortController>> = [];
  const releases: Array<() => void> = [];
  const claimReleases = new Map<RpcSourceRef, () => void>();
  const register = async (
    runId: string,
    projection: Pick<
      Parameters<typeof registerChatAbortController>[0],
      "controlUiVisible" | "projectSessionActive"
    > = {},
    queued = false,
    preparing = false,
  ) => {
    const registration = registerChatAbortController({
      ...scope,
      target: captureSessionTarget({
        storeScope: resolveSessionStorePathCore(cfg.session?.store, { agentId: scope.agentId }),
        sessionKey: scope.sessionKey,
        agentId: scope.agentId,
        incarnation: scope.sessionId,
      }),
      runId,
      timeoutMs: 60_000,
      ownerConnId: "same-person-connection",
      ...projection,
    });
    if (!registration.registered) {
      throw new Error("fixture run was not registered");
    }
    registrations.push(registration);
    if (!queued && !preparing) {
      const releaseClaim = await claimRpcSourceForTest(registration.entry);
      releases.push(releaseClaim);
      claimReleases.set(registration.entry, releaseClaim);
    }
    return registration;
  };
  const retain = async (signal: AbortSignal, runId: string, entry: RpcSourceRef) => {
    const retained = await retainGatewayOperatorRun({
      context,
      runId,
      entry,
      client: {
        connect: {
          minProtocol: 1,
          maxProtocol: 1,
          client: { id: "test", version: "test", platform: "test", mode: "test" },
          role: "operator",
          scopes: ["operator.sessions.write"],
        },
        internal: { operatorRoleActor: { kind: "operator", profileId: profile.id } },
      },
      sourceAuthority: { signal, assertCurrent: () => signal.throwIfAborted() },
    });
    if (!retained.authority) {
      throw new Error("fixture operator source was not retained");
    }
    releases.push(retained.release);
    return retained;
  };
  return {
    scope,
    context,
    execution,
    settle: () =>
      AsyncWorkScope.runWhenAllIdle(
        () => [execution],
        () => {},
      ),
    warn,
    register,
    finishClaim: async (entry: RpcSourceRef) => {
      const claim = entry.input.claim;
      claimReleases.get(entry)?.();
      await claim?.settlement.promise;
    },
    retain,
    release: () => {
      for (const release of releases) {
        release();
      }
    },
    cleanup: () => {
      for (const registration of registrations) {
        registration.cleanup();
      }
      for (const entry of rpcSourceTesting.values()) {
        retireSessionControllerInput(entry.input);
      }
      rpcSourceTesting.clear();
      chatRunState.clear();
      warn.mockRestore();
    },
  };
}

describe("operator access cancellation", () => {
  it.each([false, true])(
    "stops only its run and settles persistence (hidden progress refresh: %s)",
    async (hiddenRefresh) => {
      await withCancellationFixture(async (f) => {
        const source = new AbortController();
        const guest = await f.register(
          "guest-run",
          hiddenRefresh ? { controlUiVisible: false, projectSessionActive: false } : {},
        );
        const staff = await f.register("staff-run", {}, false, true);
        const savedTranscript = loadTranscriptEventsSync(f.scope);
        f.context.chatRunState.getOrCreate("guest-run").buffer = "The guest's saved progress.";
        f.context.chatRunState.getOrCreate("staff-run").buffer = "Staff work continues.";
        (await f.retain(source.signal, "guest-run", guest.entry)).armCancellation();
        const terminalWrite = createDeferredCore();
        const cancellationObserved = createDeferredCore();
        let sourceClosedAtAbort = false;
        const unsubscribe = onAgentEvent((event) => {
          if (event.runId !== "guest-run" || event.stream !== "lifecycle") {
            return;
          }
          sourceClosedAtAbort = source.signal.aborted;
          guest.entry.adapter.projectSessionTerminalObservedAt = Date.now();
          guest.entry.adapter.projectSessionTerminalPersistence = terminalWrite.promise.then(() => {
            guest.entry.adapter.projectSessionTerminalPending = false;
          });
          cancellationObserved.resolve();
        });
        let drain: Promise<void> | undefined;
        try {
          source.abort();
          await Promise.race([
            cancellationObserved.promise,
            f.settle().then(() => {
              throw new Error("cancellation finished without aborting its registered run");
            }),
          ]);
          let drained = false;
          drain = f.settle().then(() => {
            drained = true;
          });
          await Promise.resolve();
          expect(drained).toBe(false);
          expect(sourceClosedAtAbort).toBe(true);
          expect(guest.controller.signal.aborted).toBe(true);
          expect(guest.entry.adapter.abortDiagnosticReason).toBe("authority-revoked");
          expect(staff.controller.signal.aborted).toBe(false);
          terminalWrite.resolve();
          await Promise.resolve();
          expect(drained).toBe(false);
          await f.finishClaim(guest.entry);
          await drain;
          expect(f.context.chatRunState.resolveBuffer("staff-run").text).toBe(
            "Staff work continues.",
          );
          if (hiddenRefresh) {
            expect(loadTranscriptEventsSync(f.scope)).toEqual(savedTranscript);
            expect(f.context.broadcast).not.toHaveBeenCalled();
            expect(f.context.nodeSendToSession).not.toHaveBeenCalled();
          } else {
            expect(loadTranscriptEventsSync(f.scope)).toEqual(
              expect.arrayContaining([
                ...savedTranscript,
                expect.objectContaining({
                  message: expect.objectContaining({
                    role: "assistant",
                    content: [{ type: "text", text: "The guest's saved progress." }],
                  }),
                }),
              ]),
            );
          }
          expect(f.warn).not.toHaveBeenCalled();
        } finally {
          terminalWrite.resolve();
          await f.finishClaim(guest.entry);
          unsubscribe();
          await drain;
        }
      });
    },
  );

  it.each(["persisting", "persisted"] as const)(
    "preserves an accepted terminal result while %s",
    async (phase) => {
      await withCancellationFixture(async (f) => {
        const source = new AbortController();
        const guest = await f.register("terminal-run");
        const savedTranscript = loadTranscriptEventsSync(f.scope);
        const acceptedMessage = {
          role: "assistant",
          content: [{ type: "text", text: "The already accepted result." }],
        };
        f.context.chatRunState.getOrCreate("terminal-run").buffer = "The already accepted result.";
        const terminalWrite = createDeferredCore();
        setRpcSourceProjectSessionActive(guest.entry, false);
        guest.entry.adapter.projectSessionTerminalPending = true;
        guest.entry.adapter.projectSessionTerminalObservedAt = Date.now();
        const persistence = terminalWrite.promise.then(() => {
          expect(
            appendTranscriptMessageSync(f.scope, {
              eventId: "accepted-result",
              message: acceptedMessage,
            }),
          ).toMatchObject({ ok: true });
          guest.entry.adapter.projectSessionTerminalPending = false;
          guest.entry.adapter.projectSessionTerminalPersistence = undefined;
          guest.entry.adapter.projectSessionTerminalPersisted = true;
        });
        guest.entry.adapter.projectSessionTerminalPersistence = persistence;
        (await f.retain(source.signal, "terminal-run", guest.entry)).armCancellation();
        try {
          if (phase === "persisted") {
            terminalWrite.resolve();
            await persistence;
          }
          source.abort();
          await f.settle();
          expect(guest.controller.signal.aborted).toBe(false);
          expect(rpcSourceTesting.get("terminal-run")).toBe(guest.entry);
          expect(guest.entry.input.claim?.released).toBe(false);
          expect(f.context.chatRunState.resolveBuffer("terminal-run").text).toBe(
            "The already accepted result.",
          );
          expect(f.context.broadcast).not.toHaveBeenCalled();
          terminalWrite.resolve();
          await persistence;
          expect(loadTranscriptEventsSync(f.scope)).toEqual([
            ...savedTranscript,
            expect.objectContaining({
              id: "accepted-result",
              message: expect.objectContaining(acceptedMessage),
            }),
          ]);
          expect(f.warn).not.toHaveBeenCalled();
        } finally {
          terminalWrite.resolve();
          await persistence;
        }
      });
    },
  );

  it.each([
    { collect: false, activeAdmission: false },
    { collect: true, activeAdmission: true },
  ])(
    "retains queued custody across collect=$collect and active admission=$activeAdmission",
    async ({ collect, activeAdmission }) => {
      await withCancellationFixture(async (f) => {
        const source = new AbortController();
        const guest = await f.register("queued-guest", {}, true);
        const staff = await f.register("queued-staff", {}, true);
        const retained = await f.retain(source.signal, "queued-guest", guest.entry);
        const work = createChatSendWorkAdmission({
          admission: { release: () => {} },
          releaseCallerAuthority: retained.release,
          logGateway: f.context.logGateway,
        });
        const releaseQueue = work.retain();
        for (const [runId, registration] of [
          ["queued-guest", guest],
          ["queued-staff", staff],
        ] as const) {
          const run = createQueueTestRun({ prompt: runId });
          run.run = { ...run.run, config: f.context.getRuntimeConfig(), ...f.scope };
          run.abortSignal = registration.entry.input.abortSignal;
          bindSessionControllerSource(registration.entry.input, run);
          enqueueFollowupRun(f.scope.sessionKey, run, { mode: "followup" }, "none");
          expect(isRpcSourceQueued(registration.entry)).toBe(true);
          if (!activeAdmission) {
            registration.cleanup();
          }
        }
        retained.armCancellation();
        work.release();
        if (collect) {
          retireSessionControllerSourceCancellation(guest.entry.input);
        }
        try {
          source.abort();
          await f.settle();
          expect(guest.controller.signal.aborted).toBe(!collect);
          expect(guest.entry.adapter.abortDiagnosticReason).toBe(
            collect ? undefined : "authority-revoked",
          );
          expect(isRpcSourceQueued(guest.entry)).toBe(false);
          expect(staff.controller.signal.aborted).toBe(false);
          expect(rpcSourceTesting.has("queued-staff")).toBe(true);
          expect(rpcSourceTesting.get("queued-guest")).toBe(
            collect || activeAdmission ? guest.entry : undefined,
          );
          expect(f.context.broadcast).not.toHaveBeenCalled();
        } finally {
          releaseQueue();
        }
      });
    },
  );

  it("does not retire a replacement that reuses a completed run ID", async () => {
    await withCancellationFixture(async (f) => {
      const source = new AbortController();
      const original = await f.register("reused-run");
      (await f.retain(source.signal, "reused-run", original.entry)).armCancellation();
      await f.finishClaim(original.entry);
      original.cleanup();
      const replacement = await f.register("reused-run");
      source.abort();
      await f.settle();
      expect(replacement.controller.signal.aborted).toBe(false);
      expect(rpcSourceTesting.get("reused-run")).toBe(replacement.entry);
      expect(f.context.broadcast).not.toHaveBeenCalled();
    });
  });

  it.each(["released", "already-aborted"] as const)(
    "leaves its run untouched after source release or rejected admission (%s)",
    async (state) => {
      await withCancellationFixture(async (f) => {
        const source = new AbortController();
        const guest = await f.register("bound-run");
        if (state === "already-aborted") {
          source.abort(new Error("operator source already ended"));
          await expect(f.retain(source.signal, "bound-run", guest.entry)).rejects.toThrow(
            "operator source already ended",
          );
        } else {
          const retained = await f.retain(source.signal, "bound-run", guest.entry);
          retained.armCancellation();
          retained.release();
          source.abort();
        }
        await f.settle();
        expect(guest.controller.signal.aborted).toBe(false);
      });
    },
  );

  it.each([false, true])(
    "leaves input custody untouched until execution owns cancellation (retired: %s)",
    async (retired) => {
      await withCancellationFixture(async (f) => {
        const source = new AbortController();
        const guest = await f.register("admitted-input");
        const staff = await f.register("independent-backing-run", {}, false, true);
        const retained = await f.retain(source.signal, "admitted-input", guest.entry);
        const transcript = loadTranscriptEventsSync(f.scope);
        if (retired) {
          retained.armCancellation();
          retained.retireCancellation();
        }

        source.abort();
        await f.settle();
        expect(guest.controller.signal.aborted).toBe(false);
        expect(rpcSourceTesting.get("admitted-input")).toBe(guest.entry);
        expect(f.context.broadcast).not.toHaveBeenCalled();
        expect(loadTranscriptEventsSync(f.scope)).toEqual(transcript);
        expect(() => retained.authority?.assertCurrent()).toThrow();

        const aborted = createDeferredCore();
        guest.controller.signal.addEventListener("abort", () => aborted.resolve(), { once: true });
        retained.armCancellation();
        if (!retired) {
          await aborted.promise;
          await f.finishClaim(guest.entry);
        }
        await f.settle();
        expect(guest.controller.signal.aborted).toBe(!retired);
        expect(staff.controller.signal.aborted).toBe(false);
        expect(rpcSourceTesting.get("independent-backing-run")).toBe(staff.entry);
      });
    },
  );

  it("does not append a retained run's partial after a session reset", async () => {
    await withCancellationFixture(async (f) => {
      const source = new AbortController();
      const guest = await f.register("reset-before-cancellation");
      f.context.chatRunState.getOrCreate("reset-before-cancellation").buffer =
        "Progress from the previous session lifecycle.";
      const retained = await f.retain(source.signal, "reset-before-cancellation", guest.entry);
      retained.armCancellation();
      await replaceSessionEntry(f.scope, {
        sessionId: f.scope.sessionId,
        updatedAt: 2,
        lifecycleRevision: "reset-lifecycle",
      });
      const transcript = loadTranscriptEventsSync(f.scope);

      const aborted = createDeferredCore();
      guest.controller.signal.addEventListener("abort", () => aborted.resolve(), { once: true });
      source.abort();
      await aborted.promise;
      await f.finishClaim(guest.entry);
      await f.settle();

      expect(guest.controller.signal.aborted).toBe(true);
      expect(loadTranscriptEventsSync(f.scope)).toEqual(transcript);
      expect(f.warn).toHaveBeenCalled();
    });
  });

  it("still stops its exact run if partial transcript capture fails", async () => {
    await withCancellationFixture(async (f) => {
      const source = new AbortController();
      const guest = await f.register("failed-partial-capture");
      updateRpcSourceSessionId(guest.entry, "retired-session");
      f.context.chatRunState.getOrCreate("failed-partial-capture").buffer =
        "Preserve this progress.";
      (await f.retain(source.signal, "failed-partial-capture", guest.entry)).armCancellation();
      const aborted = createDeferredCore();
      guest.controller.signal.addEventListener("abort", () => aborted.resolve(), { once: true });
      source.abort();
      await aborted.promise;
      await f.finishClaim(guest.entry);
      await f.settle();
      expect(guest.controller.signal.aborted).toBe(true);
      expect(f.warn).toHaveBeenCalledWith(
        expect.stringContaining("Aborted partial transcript session changed before persistence"),
      );
    });
  });

  it("retains cancellation and partial persistence after immediate run cleanup releases its listener", async () => {
    await withCancellationFixture(async (f) => {
      const source = new AbortController();
      const guest = await f.register("settled-run");
      f.context.chatRunState.getOrCreate("settled-run").buffer =
        "Keep the canceled run's progress.";
      const retained = await f.retain(source.signal, "settled-run", guest.entry);
      retained.armCancellation();
      source.abort();
      await f.finishClaim(guest.entry);
      guest.cleanup();
      retained.release();
      const replacement = await f.register("settled-run");
      await f.settle();
      expect(guest.controller.signal.aborted).toBe(true);
      expect(replacement.controller.signal.aborted).toBe(false);
      expect(loadTranscriptEventsSync(f.scope)).toContainEqual(
        expect.objectContaining({
          message: expect.objectContaining({
            content: [{ type: "text", text: "Keep the canceled run's progress." }],
          }),
        }),
      );
    });
  });

  it("does not treat a retargeted source as authority to write after session replacement", async () => {
    await withCancellationFixture(async (f) => {
      const source = new AbortController();
      const guest = await f.register("rotated-partial-capture");
      const retained = await f.retain(source.signal, "rotated-partial-capture", guest.entry);
      retained.armCancellation();
      const originalTranscript = loadTranscriptEventsSync(f.scope);
      const rotatedScope = { ...f.scope, sessionId: "rotated-session" };
      await replaceSessionEntry(f.scope, { sessionId: rotatedScope.sessionId, updatedAt: 2 });
      const rotatedTranscript = loadTranscriptEventsSync(rotatedScope);
      updateRpcSourceSessionId(guest.entry, rotatedScope.sessionId);
      f.context.chatRunState.getOrCreate("rotated-partial-capture").buffer =
        "Preserve this progress.";
      const aborted = createDeferredCore();
      guest.controller.signal.addEventListener("abort", () => aborted.resolve(), { once: true });
      source.abort();
      await aborted.promise;
      await f.finishClaim(guest.entry);
      await f.settle();
      expect(guest.controller.signal.aborted).toBe(true);
      expect(loadTranscriptEventsSync(f.scope)).toEqual(originalTranscript);
      expect(loadTranscriptEventsSync(rotatedScope)).toEqual(rotatedTranscript);
      expect(f.warn).toHaveBeenCalledWith(
        expect.stringContaining("Aborted partial transcript session changed before persistence"),
      );
    });
  });
});
