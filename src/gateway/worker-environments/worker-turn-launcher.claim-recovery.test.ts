import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { awaitGateBeforeSettlement, createDeferred } from "../../../test/helpers/promise.js";
import { createEmbeddedRunLaneController } from "../../agents/embedded-agent-runner/run/lane-controller.js";
import { abortAndDrainEmbeddedAgentRun } from "../../agents/embedded-agent-runner/runs.js";
import {
  installSessionPlacementAdmissionProvider,
  withSessionPlacementTurnAdmission,
  withLocalSessionPlacementTurnSettlement,
  type SessionPlacementTurnParams,
} from "../../agents/session-placement-admission.js";
import { resolveSessionPlacementTurnSettlementAssertion } from "../../agents/session-placement-forced-terminal-settlement.js";
import { makeAgentAssistantMessage } from "../../agents/test-helpers/agent-message-fixtures.js";
import { getAgentEventLifecycleGeneration } from "../../infra/agent-events.js";
import { withSessionTurn } from "../../sessions/session-controller.admission.js";
import type { ReplyOperation } from "../../sessions/session-controller.contracts.js";
import {
  createReplyOperation,
  waitForReplyRunSuccessorAdmission,
} from "../../sessions/session-controller.js";
import { closeStateDatabaseForTest } from "../../test-utils/database-cleanup.js";
import { WorkerNodeLostError } from "../../worker/worker-node-loss.js";
import type { WorkerSessionTurnClaim } from "./placement-store.js";
import {
  advancePlacementFixtureToActive,
  writePlacementEnvironmentFixture,
} from "./placement-test-fixtures.js";
import { createWorkerSessionPlacementGate } from "./placement-worker-gate.js";
import { canRedispatchFailedWorkerPlacement } from "./session-placement-lifecycle.js";
import type { WorkerTurnTunnelHandle } from "./tunnel-contract.js";
import { createWorkerPlacementRedispatch } from "./worker-placement-redispatch.js";
import {
  ENVIRONMENT_ID,
  OWNER_EPOCH,
  MANIFEST_REF,
  attachedEnvironment,
  credential,
  measureLaunchTurn,
  readLaunchToolNames,
  openSessionManager,
  root,
  seedActivePlacement,
  SESSION_ID,
  SESSION_KEY,
  cleanupWorkerTurnLauncherTest,
  createWorkerSessionTurnPlacementProvider,
  createWorkerTurnTunnel,
  reconcileUnchangedLocalWorkspace,
  type WorkerTurnEnvironmentService,
  type WorkerTurnLauncherOptions,
  database,
  placements,
  sessionTarget,
  setupWorkerTurnLauncherTest,
  turn,
  unusedEnvironments,
} from "./worker-turn-launcher.test-support.js";
import { createWorkerWorkspaceOperationCoordinator } from "./workspace-operation-coordinator.js";

afterAll(closeStateDatabaseForTest);

function createLane(initialParams: SessionPlacementTurnParams) {
  let params = initialParams;
  let generation = getAgentEventLifecycleGeneration();
  return createEmbeddedRunLaneController({
    getParams: () => params,
    setParams: (value) => {
      params = value;
    },
    getLifecycleGeneration: () => generation,
    setLifecycleGeneration: (value) => {
      generation = value;
    },
    initialQueuedLifecycleGeneration: generation,
    globalLane: "claim-recovery-global",
  });
}

describe("local claim recovery before backend registration", () => {
  beforeEach(setupWorkerTurnLauncherTest);
  afterEach(() => cleanupWorkerTurnLauncherTest({ reuseReadWorkers: true }));

  it.each(["local", "standalone", "remote"] as const)(
    "admits a queued %s child after its inherited parent claim closes",
    async (execution) => {
      const provider = createWorkerSessionTurnPlacementProvider({
        environments: unusedEnvironments(),
        placements,
      });
      const gate = createDeferred();
      const localTask = vi.fn(async () => {
        if (execution === "local") {
          expect(placements.get(SESSION_ID)?.turnClaim?.runId).toBe("child-run");
        }
        return { meta: { durationMs: 1 } };
      });
      const remoteTask = vi.fn<typeof provider.executeTurn>(
        async (_claim, _params, _runLocal, onAdmitted) => {
          onAdmitted?.();
          return { meta: { durationMs: 2 } };
        },
      );
      let uninstall = installSessionPlacementAdmissionProvider(provider);
      let child: Promise<unknown> | undefined;
      let assertParent: (() => void) | undefined;
      try {
        await provider.executeLocalTurn(
          {
            sessionId: "parent-session",
            sessionKey: "agent:main:parent",
            agentId: "main",
            runId: "parent-run",
          },
          async () => {
            assertParent = resolveSessionPlacementTurnSettlementAssertion();
            const lane = createLane(turn("child-run"));
            child = lane.enqueueSession(async () => {
              await gate.promise;
              lane.throwIfAborted();
              return lane.enqueueGlobal(localTask);
            });
          },
        );
        expect(assertParent).toBeTypeOf("function");
        expect(assertParent).toThrow("settlement is closed");
        if (execution !== "local") {
          uninstall();
          uninstall = () => {};
        }
        if (execution === "remote") {
          uninstall = installSessionPlacementAdmissionProvider({
            assertCompactionSuccessorAllowed: () => {},
            executeLocalTurn: provider.executeLocalTurn,
            executeTurn: remoteTask,
          });
        }
        gate.resolve();
        await expect(child).resolves.toMatchObject({
          meta: { durationMs: execution === "remote" ? 2 : 1 },
        });
        expect(localTask).toHaveBeenCalledTimes(execution === "remote" ? 0 : 1);
        expect(remoteTask).toHaveBeenCalledTimes(execution === "remote" ? 1 : 0);
        expect((await placements.getAsync(SESSION_ID))?.turnClaim ?? null).toBeNull();
      } finally {
        gate.resolve();
        await Promise.allSettled([child]);
        uninstall();
      }
    },
  );

  it.each(["embedded", "CLI"] as const)(
    "isolates standalone %s admission from a retired parent settlement",
    async (execution) => {
      const provider = createWorkerSessionTurnPlacementProvider({
        environments: unusedEnvironments(),
        placements,
      });
      const gate = createDeferred();
      const task = vi.fn(async () => ({ meta: { durationMs: 1 } }));
      let child: Promise<unknown> | undefined;
      let assertParent: (() => void) | undefined;
      await provider.executeLocalTurn(
        {
          sessionId: "parent-session",
          sessionKey: "agent:main:parent",
          agentId: "main",
          runId: "parent-run",
        },
        async () => {
          assertParent = resolveSessionPlacementTurnSettlementAssertion();
          child = gate.promise.then(() => {
            const params = turn("standalone-run");
            return execution === "CLI"
              ? withLocalSessionPlacementTurnSettlement(params, task)
              : withSessionPlacementTurnAdmission(params, params, task);
          });
        },
      );
      try {
        expect(assertParent).toThrow("settlement is closed");
        gate.resolve();
        await expect(child).resolves.toMatchObject({ meta: { durationMs: 1 } });
        expect(task).toHaveBeenCalledOnce();
      } finally {
        gate.resolve();
        await Promise.allSettled([child]);
      }
    },
  );

  it.each(["preflight", "attempt admission", "terminal result"])(
    "retains cancelled pre-backend work until raw return and fences its late %s",
    async (stage) => {
      const provider = createWorkerSessionTurnPlacementProvider({
        environments: unusedEnvironments(),
        placements,
      });
      const uninstall = installSessionPlacementAdmissionProvider(provider);
      const clock = vi.spyOn(Date, "now");
      const operation = createReplyOperation({
        sessionKey: SESSION_KEY,
        sessionId: SESSION_ID,
        resetTriggered: false,
      });
      const params = {
        ...turn("run-preflight"),
        replyOperation: operation,
        abortSignal: operation.abortSignal,
      };
      const lane = createLane(params);
      const started = createDeferred();
      const resume = createDeferred();
      const replacementStarted = createDeferred();
      const finishReplacement = createDeferred();
      const modelStart = vi.fn();
      let oldRun: Promise<unknown> | undefined;
      let replacement: Promise<unknown> | undefined;
      try {
        oldRun = lane.enqueueSession(() =>
          lane.enqueueGlobal(async () => {
            const admittedRunContext = await params.preparedRunAdmission.admit("embedded");
            started.resolve();
            await resume.promise;
            if (stage === "preflight") {
              lane.throwIfAborted();
            } else if (stage === "attempt admission") {
              lane.createAttemptControls({ admittedRunContext }).close();
            }
            modelStart();
            return { meta: { durationMs: 1 } };
          }),
        );
        // Observe rejection immediately so a failing assertion still cleans up the old turn.
        const oldOutcome = oldRun.then(
          () => undefined,
          (error: unknown) => error,
        );
        await started.promise;
        expect(placements.get(SESSION_ID)?.turnClaim).not.toBeNull();
        clock.mockReturnValue(Date.now() + 6 * 60_000);
        vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
        const recovery = abortAndDrainEmbeddedAgentRun({
          sessionId: SESSION_ID,
          sessionKey: SESSION_KEY,
          settleMs: 100,
          forceClear: true,
          reason: "stuck_recovery",
        });
        await vi.advanceTimersByTimeAsync(100);
        await expect(recovery).resolves.toMatchObject({
          aborted: true,
          drained: false,
          forceCleared: false,
        });
        vi.useRealTimers();
        clock.mockRestore();
        expect(placements.get(SESSION_ID)?.turnClaim ?? null).toBeNull();
        let ownerSettled = false;
        void operation.ownerSettlement.then(() => {
          ownerSettled = true;
        });
        replacement = withSessionTurn(
          {
            sessionKey: SESSION_KEY,
            sessionId: SESSION_ID,
            agentId: "main",
            storePath: sessionTarget.storePath,
          },
          () =>
            provider.executeLocalTurn({ ...sessionTarget, runId: "replacement" }, async () => {
              replacementStarted.resolve();
              await finishReplacement.promise;
            }),
        );
        await Promise.resolve();
        expect(ownerSettled).toBe(false);
        expect(placements.get(SESSION_ID)?.turnClaim ?? null).toBeNull();
        resume.resolve();
        expect(await oldOutcome).toBeInstanceOf(Error);
        expect(modelStart).toHaveBeenCalledTimes(stage === "terminal result" ? 1 : 0);
        operation.complete();
        await operation.ownerSettlement;
        await replacementStarted.promise;
        const replacementClaim = placements.get(SESSION_ID)?.turnClaim?.claimId;
        expect(replacementClaim).toBeTruthy();
        await expect(waitForReplyRunSuccessorAdmission(SESSION_KEY, null)).resolves.toMatchObject({
          settled: true,
        });
        expect(placements.get(SESSION_ID)?.turnClaim?.claimId).toBe(replacementClaim);
      } finally {
        vi.useRealTimers();
        clock.mockRestore();
        resume.resolve();
        finishReplacement.resolve();
        await Promise.allSettled([oldRun]);
        operation.complete();
        await Promise.allSettled([replacement]);
        params.preparedRunAdmission.close();
        uninstall();
      }
    },
  );

  it.each(["before", "during"])(
    "does not start local work for a reply cancelled %s admission",
    async (timing) => {
      const provider = createWorkerSessionTurnPlacementProvider({
        environments: unusedEnvironments(),
        placements,
      });
      const uninstall = installSessionPlacementAdmissionProvider(provider);
      const operation = createReplyOperation({
        sessionKey: SESSION_KEY,
        sessionId: SESSION_ID,
        resetTriggered: false,
      });
      const params = {
        ...turn("run-cleared-before-admission"),
        replyOperation: operation,
        abortSignal: operation.abortSignal,
      };
      const run = vi.fn(async () => ({ meta: { durationMs: 1 } }));
      try {
        if (timing === "before") {
          operation.abortByUser();
        }
        await expect(
          withSessionPlacementTurnAdmission(params, params, run, () => {
            if (timing === "during") {
              operation.abortByUser();
            }
          }),
        ).rejects.toThrow();
        expect(run).not.toHaveBeenCalled();
        expect(placements.get(SESSION_ID)?.turnClaim ?? null).toBeNull();
      } finally {
        operation.complete();
        uninstall();
      }
    },
  );
});

describe("worker pre-launch claim recovery", () => {
  beforeEach(setupWorkerTurnLauncherTest);
  afterEach(() => cleanupWorkerTurnLauncherTest({ reuseReadWorkers: true }));

  it.each([
    "workspace resolution",
    "workspace queue",
    "execution-start publication",
    "workspace mutation",
    "dispatch boundary",
    "dispatch",
    "uncertain dispatch",
    "pending result",
  ] as const)("recovers a warm second turn blocked in %s with exact custody", async (stage) => {
    await seedActivePlacement();
    const entered = createDeferred();
    const resume = createDeferred();
    const successorEntered = createDeferred();
    const finishSuccessor = createDeferred();
    const coordinator = createWorkerWorkspaceOperationCoordinator();
    let blockSecond = false;
    let holdSuccessor = true;
    let queuedBlocker: Promise<void> | undefined;
    const launch = vi.fn<WorkerTurnTunnelHandle["launchTurn"]>(async (request) => {
      if (blockSecond && stage === "dispatch boundary") {
        // The transport accepted dispatch before its synchronous handoff callback.
        // Cancellation here makes beginExecution refuse, but cannot undo dispatch custody.
        entered.resolve();
        await resume.promise;
      }
      request.onDispatchReady?.();
      if (blockSecond) {
        if (stage === "pending result") {
          await placements.markWorkspaceResultPending(request.turnClaim);
        }
        entered.resolve();
        await resume.promise;
        if (stage === "dispatch") {
          return {
            stdout: "",
            stderr: "",
            code: 1,
            signal: null,
            killed: true,
            termination: "exit",
          };
        }
        throw new AggregateError(
          [new Error("cancel receipt unavailable")],
          "node cancellation unconfirmed",
        );
      }
      const leafId = await (
        await openSessionManager()
      ).appendMessageAsync(
        makeAgentAssistantMessage({
          content: [{ type: "text", text: "First turn complete" }],
          timestamp: 41,
        }),
      );
      await createWorkerSessionPlacementGate(placements).updateAckCursors({
        claim: request.turnClaim,
        transcriptSeq: 2,
        liveSeq: 1,
      });
      await placements.markWorkspaceResultPending(request.turnClaim);
      return {
        stdout: JSON.stringify({
          status: "completed",
          transcriptLeafId: leafId,
          transcriptNextSeq: 3,
        }),
        stderr: "",
        code: 0,
        signal: null,
        killed: false,
        termination: "exit",
      };
    });
    let environment: ReturnType<typeof attachedEnvironment> = {
      ...attachedEnvironment(),
      nodeDeviceId: "worker-node",
      sshEndpoint: null,
    };
    const destroy = vi.fn(async () => {
      if (environment.leaseId === null) {
        throw new Error("expected leased worker environment");
      }
      environment = {
        ...environment,
        state: "destroyed" as const,
        attachedSessionIds: [],
        tunnelStatus: "stopped",
      };
      writePlacementEnvironmentFixture(database, environment);
      return environment;
    });
    const environments = {
      ...unusedEnvironments(),
      get: () => environment,
      acquireTurnCredential: vi.fn(async () => ({
        ...credential(),
        environmentId: environment.environmentId,
        ownerEpoch: environment.ownerEpoch,
      })),
      acknowledgeCredentialDelivery: async () => true,
      startTunnel: async () => ({
        environmentId: environment.environmentId,
        ownerEpoch: environment.ownerEpoch,
        runWorkspaceCommand: vi.fn(),
        syncWorkspace: vi.fn(),
        stop: vi.fn(),
        measureLaunchTurn,
        readLaunchToolNames,
        launchTurn: launch,
        quiesceWorkspace: async () => ({ assertActive: async () => {}, resume: async () => {} }),
        reconcileWorkspace: async (
          request: Parameters<WorkerTurnTunnelHandle["reconcileWorkspace"]>[0],
        ) => {
          if (request.source.kind !== "local") {
            throw new Error("expected local workspace");
          }
          await request.source.journal.commit(MANIFEST_REF);
          return {
            manifestRef: MANIFEST_REF,
            changed: false,
            verifyStable: async () => {},
            verifyLocalStable: async () => {},
            publishStagedResult: async () => {},
            discardPreparedStagedResult: async () => {},
          };
        },
      }),
      destroy,
    };
    const redispatchPlacement = createWorkerPlacementRedispatch({
      placements,
      resolveDevicePlacementRequirement: async () => ({
        requiredNodeCommands: [],
        consumesWorkerSlot: true,
      }),
      dispatch: async (request, _onTransition, assertCurrent) => {
        assertCurrent?.();
        const recoveredEnvironmentId = `${ENVIRONMENT_ID}-recovered`;
        const recoveredOwnerEpoch = OWNER_EPOCH + 1;
        const recovered = await advancePlacementFixtureToActive(
          placements,
          database,
          {
            sessionId: request.sessionId,
            sessionKey: request.sessionKey,
            agentId: request.agentId,
            executionMode: request.executionMode,
            expectedPlacement: request.expectedPlacement,
          },
          {
            environmentId: recoveredEnvironmentId,
            ownerEpoch: recoveredOwnerEpoch,
            remoteWorkspaceDir: "/workspace/recovered",
            workspaceBaseManifestRef: MANIFEST_REF,
          },
        );
        const recoveredEnvironment = attachedEnvironment();
        if (recoveredEnvironment.leaseId === null) {
          throw new Error("expected leased worker environment");
        }
        environment = {
          ...recoveredEnvironment,
          environmentId: recoveredEnvironmentId,
          ownerEpoch: recoveredOwnerEpoch,
          nodeDeviceId: "worker-node",
          leaseId: "lease-worker-turn-recovered",
          sshEndpoint: null,
        };
        writePlacementEnvironmentFixture(database, environment);
        return recovered;
      },
    });
    let workspaceCalls = 0;
    const provider = createWorkerSessionTurnPlacementProvider({
      environments,
      placements,
      reconcileActivePlacement: async () => {},
      redispatchPlacement,
      resolveWorkspace: async () => {
        workspaceCalls++;
        if (workspaceCalls === 3 && holdSuccessor) {
          successorEntered.resolve();
          await finishSuccessor.promise;
          throw new Error("successor finished");
        }
        if (blockSecond && stage === "workspace resolution") {
          entered.resolve();
          await resume.promise;
        }
        return { kind: "local", path: root };
      },
      workspaceOperations: {
        run(environmentId, task, ...options) {
          const result = coordinator.run(
            environmentId,
            async () => {
              if (blockSecond && stage === "workspace mutation") {
                entered.resolve();
                await resume.promise;
                // The entered writer still owns the exact claim after cancellation.
                expect(placements.get(SESSION_ID)?.turnClaim?.runId).toBe("blocked-second");
              }
              return await task();
            },
            ...options,
          );
          if (blockSecond && stage === "workspace queue") {
            entered.resolve();
          }
          return result;
        },
      },
    });
    const first = turn("warm-first");
    const second: ReturnType<typeof turn> & SessionPlacementTurnParams = {
      ...turn("blocked-second"),
      onExecutionStarted: async () => {
        if (stage === "execution-start publication") {
          entered.resolve();
          await resume.promise;
        }
      },
    };
    let blockedOperation: ReplyOperation | undefined;
    const execute = (input: ReturnType<typeof turn>) =>
      withSessionTurn(
        { sessionId: SESSION_ID, sessionKey: SESSION_KEY, storePath: sessionTarget.storePath },
        async (operation, signal) => {
          if (input === second) {
            blockedOperation = operation;
          }
          return provider.executeTurn(
            { sessionId: SESSION_ID, sessionKey: SESSION_KEY, agentId: "main", runId: input.runId },
            { ...input, replyOperation: operation, abortSignal: signal },
            async () => {
              throw new Error("unexpected local execution");
            },
          );
        },
      );
    let blocked: Promise<unknown> | undefined;
    let successor: Promise<unknown> | undefined;
    let recovery: ReturnType<ReplyOperation["watchdog"]["tick"]> | undefined;
    try {
      await expect(execute(first)).resolves.toMatchObject({
        payloads: [{ text: "First turn complete" }],
      });
      await placements.waitForTurnClaimRelease(SESSION_ID, {});
      expect(placements.get(SESSION_ID)).toMatchObject({ state: "active", turnClaim: null });
      expect(await placements.listPendingWorkspaceResultsAsync()).toEqual([]);
      blockSecond = true;
      if (stage === "workspace queue") {
        queuedBlocker = coordinator.run(ENVIRONMENT_ID, () => resume.promise);
      }
      blocked = execute(second);
      const outcome = blocked.catch((error: unknown) => error);
      await Promise.race([
        entered.promise,
        outcome.then(() => {
          throw new Error("worker settled before reaching cancellation gate");
        }),
      ]);
      const oldClaim = placements.get(SESSION_ID)?.turnClaim;
      expect(oldClaim?.runId).toBe("blocked-second");
      expect(await placements.listPendingWorkspaceResultsAsync()).toHaveLength(
        stage === "pending result" ? 1 : 0,
      );
      const dispatched =
        stage === "dispatch boundary" ||
        stage === "dispatch" ||
        stage === "uncertain dispatch" ||
        stage === "pending result";
      expect(launch).toHaveBeenCalledTimes(dispatched ? 2 : 1);
      let settled = false;
      void outcome.then(() => {
        settled = true;
      });
      if (!blockedOperation) {
        throw new Error("Expected the actual admitted warm turn operation");
      }
      const operation = blockedOperation;
      let ownerSettled = false;
      void operation.ownerSettlement.then(() => {
        ownerSettled = true;
      });
      // Exercise stalled recovery on the exact operation while retaining raw producer custody.
      const deadlineAtMs = operation.watchdog.snapshot().semanticDeadlineAtMs;
      recovery = operation.watchdog.tick(deadlineAtMs);
      expect(operation.abortSignal.aborted).toBe(true);
      expect(operation.result).toMatchObject({ kind: "failed", code: "run_stalled" });
      expect(operation.watchdog.snapshot().recovery?.startedAtMs).toBe(deadlineAtMs);
      if (stage === "workspace mutation" || stage === "execution-start publication" || dispatched) {
        expect(ownerSettled).toBe(false);
        expect(settled).toBe(false);
        expect(placements.get(SESSION_ID)?.turnClaim).toEqual(oldClaim);
        resume.resolve();
      }
      expect(await outcome).toBeInstanceOf(Error);
      if (!dispatched) {
        expect(await outcome).toMatchObject({ name: "AbortError" });
      }
      if (stage === "dispatch boundary" || stage === "uncertain dispatch") {
        const failed = placements.get(SESSION_ID);
        expect(failed).toMatchObject({
          state: "failed",
          turnClaim: null,
          environmentId: ENVIRONMENT_ID,
          activeOwnerEpoch: OWNER_EPOCH,
        });
        if (failed?.state !== "failed") {
          throw new Error("expected failed custody fence");
        }
        expect(canRedispatchFailedWorkerPlacement(failed, environment)).toBe(true);
        expect(environments.stopTunnel).toHaveBeenCalledOnce();
        expect(destroy).toHaveBeenCalledOnce();
        blockSecond = false;
        holdSuccessor = false;
        await expect(execute(turn("successor-third"))).resolves.toMatchObject({
          payloads: [{ text: "First turn complete" }],
        });
        await placements.waitForTurnClaimRelease(SESSION_ID, {});
        expect(placements.get(SESSION_ID)).toMatchObject({
          state: "active",
          environmentId: ENVIRONMENT_ID + "-recovered",
          activeOwnerEpoch: OWNER_EPOCH + 1,
          turnClaim: null,
        });
        expect(launch).toHaveBeenCalledTimes(3);
        return;
      }
      if (stage === "pending result") {
        expect(placements.get(SESSION_ID)?.turnClaim).toEqual(oldClaim);
        const pending = (await placements.listPendingWorkspaceResultsAsync())[0]!;
        expect(pending.claimId).toBe(oldClaim?.claimId);
        expect(pending.recoveryRequestedAtMs).not.toBeNull();
        expect(environments.destroy).not.toHaveBeenCalled();
        return;
      }
      expect(placements.get(SESSION_ID)?.turnClaim).toBeNull();
      successor = execute(turn("successor-third"));
      void successor.catch(() => undefined);
      await Promise.race([
        successorEntered.promise,
        successor.then(
          () => {
            throw new Error("successor settled before reaching workspace gate");
          },
          (error: unknown) => {
            throw error;
          },
        ),
      ]);
      const replacement = placements.get(SESSION_ID)?.turnClaim;
      expect(replacement?.runId).toBe("successor-third");
      resume.resolve();
      expect(placements.get(SESSION_ID)?.turnClaim).toEqual(replacement);
      expect(launch).toHaveBeenCalledTimes(dispatched ? 2 : 1);
      expect(environments.destroy).not.toHaveBeenCalled();
    } finally {
      resume.resolve();
      finishSuccessor.resolve();
      await Promise.allSettled([blocked, successor, queuedBlocker, recovery]);
      first.preparedRunAdmission.close();
      second.preparedRunAdmission.close();
    }
  });
});

describe("worker turn launcher node-loss continuation", () => {
  beforeEach(setupWorkerTurnLauncherTest);
  afterEach(() => cleanupWorkerTurnLauncherTest({ reuseReadWorkers: true }));

  it("retains an interrupted workspace and waits for reconnect before allowing continuation", async () => {
    await seedActivePlacement();
    const loss = new WorkerNodeLostError();
    const reconnect = createDeferred();
    const waiting = createDeferred();
    const order: string[] = [];
    let interruptedClaim: WorkerSessionTurnClaim | undefined;
    const launchTurn = vi.fn(
      async (request: Parameters<WorkerTurnTunnelHandle["launchTurn"]>[0]) => {
        interruptedClaim = request.turnClaim;
        request.onDispatchReady?.();
        throw loss;
      },
    );
    const persistAbandonedPartial = vi.fn<
      NonNullable<WorkerTurnLauncherOptions["persistAbandonedPartial"]>
    >(async (claim) => {
      expect(claim.missingToolResultText).toBe(loss.message);
      order.push("transcript saved");
    });
    const environment = {
      ...attachedEnvironment(),
      nodeDeviceId: "paired-node",
      sshEndpoint: null,
    };
    const environments: WorkerTurnEnvironmentService = {
      ...unusedEnvironments(),
      get: () => environment,
      acquireTurnCredential: async () => credential(),
      acknowledgeCredentialDelivery: async () => true,
      startTunnel: async () =>
        createWorkerTurnTunnel({
          reconcileWorkspace: reconcileUnchangedLocalWorkspace,
          launchTurn,
        }),
      stopTunnel: async () => {
        order.push("tunnel stopped");
      },
    };
    const reconcileActivePlacement = vi.fn(async () => {
      expect(interruptedClaim).toBeDefined();
      await placements.acceptWorkspaceResult(interruptedClaim!);
      await placements.completeWorkspaceResultAndReleaseTurn(interruptedClaim!);
      order.push("workspace settled");
    });
    const provider = createWorkerSessionTurnPlacementProvider({
      placements,
      environments,
      persistAbandonedPartial,
      reconcileActivePlacement,
      waitForAdmissionNode: async ({ signal, assertCurrent }) => {
        expect(signal.aborted).toBe(false);
        expect(await placements.listPendingWorkspaceResultsAsync()).toMatchObject([
          { runId: "node-loss-reconnect", recoveryRequestedAtMs: expect.any(Number) },
        ]);
        waiting.resolve();
        await reconnect.promise;
        assertCurrent();
        order.push("node reconnected");
      },
    });
    const runLocal = vi.fn();
    const attempt = provider
      .executeTurn(
        {
          sessionId: SESSION_ID,
          sessionKey: SESSION_KEY,
          agentId: "main",
          runId: "node-loss-reconnect",
        },
        turn("node-loss-reconnect"),
        runLocal,
      )
      .catch((error: unknown) => error);
    try {
      await awaitGateBeforeSettlement(
        waiting.promise,
        attempt,
        "node loss settled before reconnect admission",
      );
      expect(loss.canContinue).toBe(false);
      expect(reconcileActivePlacement).not.toHaveBeenCalled();
      reconnect.resolve();
      expect(await attempt).toBe(loss);
      expect(loss.canContinue).toBe(true);
      expect(order).toEqual([
        "transcript saved",
        "tunnel stopped",
        "node reconnected",
        "workspace settled",
      ]);
      expect(launchTurn).toHaveBeenCalledOnce();
      expect(runLocal).not.toHaveBeenCalled();
      expect(await placements.listPendingWorkspaceResultsAsync()).toEqual([]);
      expect(placements.get(SESSION_ID)).toMatchObject({ state: "active", turnClaim: null });
    } finally {
      reconnect.resolve();
      await attempt;
    }
  });

  it.each([
    "partial persistence failed",
    "node never reconnects",
    "retained workspace and no reconnect",
  ] as const)("does not allow continuation when %s", async (failure) => {
    await seedActivePlacement();
    const loss = new WorkerNodeLostError();
    const waitForAdmissionNode = vi.fn(async () => {
      throw new Error("node admission expired");
    });
    const reconcileActivePlacement = vi.fn();
    const destroy = vi.fn(async () => attachedEnvironment());
    const launchTurn = vi.fn(
      async (request: Parameters<WorkerTurnTunnelHandle["launchTurn"]>[0]) => {
        request.onDispatchReady?.();
        if (failure !== "retained workspace and no reconnect") {
          await placements.markWorkspaceResultPending(request.turnClaim);
        }
        throw loss;
      },
    );
    const provider = createWorkerSessionTurnPlacementProvider({
      placements,
      waitForAdmissionNode,
      reconcileActivePlacement,
      persistAbandonedPartial: async () => {
        if (failure === "partial persistence failed") {
          throw new Error("transcript unavailable");
        }
      },
      environments: {
        ...unusedEnvironments(),
        get: () => attachedEnvironment(),
        stopTunnel: async () => {},
        destroy,
        acquireTurnCredential: async () => credential(),
        acknowledgeCredentialDelivery: async () => true,
        startTunnel: async () =>
          createWorkerTurnTunnel({
            reconcileWorkspace: reconcileUnchangedLocalWorkspace,
            launchTurn,
          }),
      },
    });
    await expect(
      provider.executeTurn(
        {
          sessionId: SESSION_ID,
          sessionKey: SESSION_KEY,
          agentId: "main",
          runId: "node-loss-unavailable",
        },
        turn("node-loss-unavailable"),
        vi.fn(),
      ),
    ).rejects.toBe(loss);
    expect(loss.canContinue).toBe(false);
    expect(waitForAdmissionNode).toHaveBeenCalledTimes(
      failure === "partial persistence failed" ? 0 : 1,
    );
    expect(launchTurn).toHaveBeenCalledOnce();
    expect(await placements.listPendingWorkspaceResultsAsync()).toHaveLength(1);
    expect(destroy).not.toHaveBeenCalled();
    if (failure === "retained workspace and no reconnect") {
      expect(placements.get(SESSION_ID)).toMatchObject({ state: "active" });
      expect(await placements.listPendingWorkspaceResultsAsync()).toMatchObject([
        { recoveryRequestedAtMs: expect.any(Number) },
      ]);
    }
  });
});
