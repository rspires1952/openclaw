import { expect, it, onTestFinished, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import type { AgentCommandOpts } from "../agents/command/types.js";
import { SubagentLifecycleController } from "../agents/subagents/registry/subagent-registry-lifecycle.js";
import { subagentRuns } from "../agents/subagents/registry/subagent-registry-memory.js";
import { listRunsForControllerFromRuns } from "../agents/subagents/registry/subagent-registry-queries.js";
import { loadSubagentRegistryFromSqlite } from "../agents/subagents/registry/subagent-registry-state.fixture.test-support.js";
import { registerSubagentRun } from "../agents/subagents/registry/subagent-registry.js";
import {
  writeSubagentSessionEntry,
  settleSubagentRegistryPersistenceWork,
} from "../agents/subagents/registry/subagent-registry.persistence.test-support.js";
import type { RpcSourceRef } from "../sessions/session-controller.rpc-sources.js";
import type { UserTurnTranscriptRecorder } from "../sessions/user-turn-transcript.types.js";
import * as agentJobs from "./agent-turn/agent-job.js";
import { dispatchGatewayMethodInProcess } from "./server-plugin-in-process-dispatch.js";
import { holdMetadataThroughSubagentStop } from "./server.private-completion.metadata-overlap.test-support.js";
import { loadSessionEntryAsync } from "./session-entry-load.test-support.js";
import type { agentCommandMock as gatewayAgentCommandMock } from "./test-helpers.js";

type PrivateCompletionStopFixture = {
  kernel: Awaited<ReturnType<(typeof import("./server-kernel.js"))["createGatewayKernel"]>>;
  sequence: number;
  sessionKey: string;
  sessionId: string;
  runId: string;
  storePath: string;
  recorder: (input: unknown) => UserTurnTranscriptRecorder;
  dispatch: (message?: string, onAccepted?: () => void) => Promise<Record<string, unknown>>;
  completions: () => Record<string, unknown>[];
  pending: () => Record<string, unknown>[];
  restart: () => Promise<void>;
  source: (runId?: string) => RpcSourceRef | undefined;
  agentCommandMock: typeof gatewayAgentCommandMock;
};

export function registerPrivateCompletionStopTests(getFixture: () => PrivateCompletionStopFixture) {
  it.for([false, true])(
    "preserves an operator stop after private input consumption across retry and restart (terminal first=%s)",
    async (terminalFirst, { signal }) => {
      const {
        kernel,
        sequence,
        sessionKey,
        sessionId,
        runId,
        storePath,
        recorder,
        dispatch,
        completions,
        pending,
        restart,
        source,
        agentCommandMock,
      } = getFixture();
      const descendantRunId = `private-descendant-${sequence}`;
      const childSessionKey = `agent:main:subagent:${descendantRunId}`;
      const childSessionId = `${descendantRunId}-session`;
      let observedTerminal = false;
      const metadataOverlap = holdMetadataThroughSubagentStop({
        sessionKey: childSessionKey,
        sessionId: childSessionId,
        storePath,
        signal,
        beforeMetadata: () => {
          if (terminalFirst) {
            expect(observedTerminal).toBe(true);
          }
          expect(
            listRunsForControllerFromRuns(loadSubagentRegistryFromSqlite(), sessionKey).find(
              (run) => run.runId === descendantRunId,
            ),
          ).toMatchObject({ endedReason: "subagent-killed", execution: { status: "terminal" } });
        },
      });
      onTestFinished(() => metadataOverlap.dispose());
      const consumed = createDeferred();
      const release = createDeferred();
      signal.addEventListener("abort", () => release.resolve(), { once: true });
      agentCommandMock.mockImplementationOnce(async (input) => {
        const command = input as AgentCommandOpts;
        await command.onExecutionStarted?.();
        await recorder(input).persistApproved();
        consumed.resolve();
        command.abortSignal!.addEventListener("abort", () => release.resolve(), { once: true });
        await release.promise;
        command.abortSignal!.throwIfAborted();
        throw new Error("operator stop must prevent further work");
      });
      const first = dispatch();
      const observed = first.then(
        (value) => ({ value }),
        (error: unknown) => ({ error }),
      );
      await consumed.promise;
      if (terminalFirst) {
        const acquire = vi.spyOn(
          SubagentLifecycleController.prototype,
          "acquireTerminalCompletionLock",
        );
        acquire.mockRestore();
        const lock = vi
          .spyOn(SubagentLifecycleController.prototype, "acquireTerminalCompletionLock")
          .mockImplementation(async function (this: SubagentLifecycleController, targetRunId) {
            const unlock = await acquire.call(this, targetRunId);
            return () => {
              unlock();
              const entry = subagentRuns.get(targetRunId);
              if (
                targetRunId === descendantRunId &&
                entry?.endedReason === "subagent-killed" &&
                entry.killReconciliation?.taskCancellationAccepted === true
              ) {
                observedTerminal = true;
              }
            };
          });
        onTestFinished(() => {
          lock.mockRestore();
        });
      }
      const childStarted = createDeferred();
      const releaseChild = createDeferred();
      let childAbortSignal: AbortSignal | undefined;
      signal.addEventListener("abort", () => releaseChild.resolve(), { once: true });
      await writeSubagentSessionEntry({
        stateDir: process.env.OPENCLAW_STATE_DIR!,
        agentId: "main",
        sessionKey: childSessionKey,
        defaultSessionId: childSessionId,
        lifecycleRevision: "private-descendant-original",
      });
      agentCommandMock.mockImplementationOnce(async (input) => {
        const command = input as AgentCommandOpts;
        expect(command.runId).toBe(descendantRunId);
        expect(command.sessionId).toBe(childSessionId);
        childAbortSignal = command.abortSignal;
        await command.onExecutionStarted?.();
        await command.userTurnTranscriptRecorder?.persistApproved();
        childStarted.resolve();
        command.abortSignal!.addEventListener("abort", () => releaseChild.resolve(), {
          once: true,
        });
        await releaseChild.promise;
        command.abortSignal!.throwIfAborted();
        throw new Error("operator stop must interrupt the continuation child");
      });
      const child = dispatchGatewayMethodInProcess<Record<string, unknown>>(
        "agent",
        {
          sessionKey: childSessionKey,
          expectedExistingSessionId: childSessionId,
          idempotencyKey: descendantRunId,
          message: "Synthetic continuation child",
          deliver: false,
        },
        {
          expectFinal: true,
          forceSyntheticClient: true,
          operatorRoleActor: { kind: "system" },
          resolveGatewayContext: () => kernel.gatewayRequestContext,
        },
      );
      const observedChild = child.then(
        (value) => ({ value }),
        (error: unknown) => ({ error }),
      );
      const childWaitEntered = createDeferred();
      const waitForAgentJob = agentJobs.waitForAgentJob;
      const waitObservation = vi
        .spyOn(agentJobs, "waitForAgentJob")
        .mockImplementation((params) => {
          const wait = waitForAgentJob(params);
          if (params.runId === descendantRunId) {
            childWaitEntered.resolve();
          }
          return wait;
        });
      // Cancellation must release the admission barrier so cleanup can join both producers.
      const releaseWaitAdmission = () => childWaitEntered.resolve();
      signal.addEventListener("abort", releaseWaitAdmission, { once: true });
      try {
        await Promise.race([
          childStarted.promise,
          observedChild.then(() => {
            throw new Error("continuation child finished before execution started");
          }),
        ]);
        await registerSubagentRun({
          runId: descendantRunId,
          childSessionKey,
          childAgentId: "main",
          sessionEntry: (await loadSessionEntryAsync(childSessionKey)).entry,
          requesterSessionKey: sessionKey,
          requesterAgentId: "main",
          requesterTurnRunId: runId,
          requesterDisplayKey: sessionKey,
          task: "synthetic continuation child",
          cleanup: "keep",
          expectsCompletionMessage: false,
        });
        // Keep both producers live until the child's registration and wait request
        // are admitted; a slow socket handshake must not hide the outstanding wait.
        await expect
          .poll(() =>
            listRunsForControllerFromRuns(loadSubagentRegistryFromSqlite(), sessionKey).some(
              (run) => run.runId === descendantRunId,
            ),
          )
          .toBe(true);
        signal.throwIfAborted();
        await childWaitEntered.promise;
        signal.throwIfAborted();
        expect(
          await kernel.gatewayInstanceRuntime.recovery.dispatchSessionMethod("chat.abort", {
            sessionKey,
            runId,
          }),
        ).toMatchObject({ aborted: true });
        expect(childAbortSignal?.aborted).toBe(true);
        if (terminalFirst) {
          expect(observedTerminal).toBe(true);
        }
        await metadataOverlap.assertCompleted();
        expect((await loadSessionEntryAsync(childSessionKey)).entry).toMatchObject({
          sessionId: childSessionId,
          label: metadataOverlap.label,
          abortedLastRun: true,
        });
      } finally {
        metadataOverlap.releaseForCleanup();
        try {
          signal.removeEventListener("abort", releaseWaitAdmission);
          waitObservation.mockRestore();
          if (source(runId) !== undefined) {
            await kernel.gatewayInstanceRuntime.recovery.dispatchSessionMethod("chat.abort", {
              sessionKey,
              runId,
            });
          }
          if (source(descendantRunId) !== undefined) {
            await kernel.gatewayInstanceRuntime.recovery.dispatchSessionMethod("chat.abort", {
              sessionKey: childSessionKey,
              runId: descendantRunId,
            });
          }
          release.resolve();
          releaseChild.resolve();
          await Promise.all([observed, observedChild]);
        } finally {
          await metadataOverlap.dispose();
        }
      }
      await settleSubagentRegistryPersistenceWork();
      expect(await observedChild).toMatchObject({
        value: { status: "timeout", stopReason: "rpc" },
      });
      // The terminal marker is durable before metadata cleanup above. Confirmed
      // requester-wide Stop then retires the child's completion obligation.
      expect(
        listRunsForControllerFromRuns(loadSubagentRegistryFromSqlite(), sessionKey).find(
          (run) => run.runId === descendantRunId,
        ),
      ).toBeUndefined();
      expect(completions()).toMatchObject([{ succeeded: 0 }]);
      expect(JSON.parse(String(completions()[0]?.outcome_json))).toMatchObject({
        reason: "cancelled",
        stopReason: "rpc",
      });
      expect(pending()).toEqual([]);
      expect(await observed).toMatchObject({ value: { status: "timeout", stopReason: "rpc" } });
      // Retire only this run's process projection to exercise the durable receipt.
      // Matching pre-admission Stop cache replay is covered separately above.
      kernel.gatewayRequestContext.dedupe.delete(`agent:${runId}`);
      expect(await dispatch()).toMatchObject({ status: "error", stopReason: "rpc" });
      await restart();
      expect(await dispatch()).toMatchObject({ status: "error", stopReason: "rpc" });
      expect(
        agentCommandMock.mock.calls.map(([input]) => {
          const command = input as AgentCommandOpts;
          return { runId: command.runId, sessionId: command.sessionId };
        }),
      ).toEqual([
        { runId, sessionId },
        { runId: descendantRunId, sessionId: childSessionId },
      ]);
    },
  );
}
