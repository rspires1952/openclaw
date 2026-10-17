import type { ReplyOperation } from "./session-controller.contracts.js";
import { notify } from "./session-controller.lifecycle-state.js";
import {
  getAttachedBackend,
  isCurrentSessionControllerOperation,
  isReplyOperationAbortable,
  type SessionControllerEntry,
} from "./session-controller.state.js";
import { captureSessionControllerStop, stopSession } from "./session-controller.stop.js";
import { lifecycleAdmissionByOperation } from "./session-controller.storage.js";
import type {
  SessionWatchdogEffect,
  SessionWatchdogEffectResult,
} from "./session-controller.watchdog.js";

type ReplyOperationRecoveryContext = {
  readOperation: () => ReplyOperation;
  readOwner: () => SessionControllerEntry;
  isOwnerSettled: () => boolean;
  executionCleanups: ReadonlySet<() => Promise<void>>;
  revokeTerminalProducerFences: () => Promise<{ failures: unknown[]; fenced: boolean }>;
  preserveCleanupOutcome: () => void;
  settleResult: () => void;
  clearState: () => void;
  closeWatchdog: () => void;
  detachUpstreamAbort: () => void;
  settleOwner: () => void;
};

/** Recovery acts on the operation's live custody; all state changes stay with that owner. */
export function createReplyOperationRecovery(context: ReplyOperationRecoveryContext) {
  const retireAdmissionAfterFencing = async (): Promise<boolean> => {
    const operation = context.readOperation();
    const capturedOwner = context.readOwner();
    const sessionKey = operation.key;
    const sessionId = operation.sessionId;
    const agentId = operation.agentId;
    const backend = getAttachedBackend(operation);
    const admittedLease = lifecycleAdmissionByOperation.get(operation)?.lease;
    const canRetire = () =>
      context.readOwner() === capturedOwner &&
      operation.key === sessionKey &&
      operation.sessionId === sessionId &&
      operation.agentId === agentId &&
      isCurrentSessionControllerOperation(operation) &&
      !operation.result &&
      getAttachedBackend(operation) === backend &&
      lifecycleAdmissionByOperation.get(operation)?.lease === admittedLease &&
      ![...(context.readOwner().lifecycle?.effects ?? [])].some(
        (effect) =>
          effect.ref !== admittedLease &&
          (effect.phase === "validating" ||
            effect.phase === "writer" ||
            effect.phase === "acquired"),
      );
    if (!canRetire()) {
      return false;
    }
    const { fenced, failures } = await context.revokeTerminalProducerFences();
    if (failures.length) {
      throw failures.length === 1
        ? failures[0]
        : new AggregateError(failures, "Session admission writer fencing failed");
    }
    if (!fenced || !canRetire()) {
      return false;
    }
    // Clear only admission. The raw execution and its error remain owned until complete().
    context.clearState();
    context.closeWatchdog();
    if (capturedOwner.lifecycle) {
      notify(capturedOwner);
    } else {
      capturedOwner.mailbox?.wake();
    }
    return true;
  };

  const expireOwner = async (
    effect: SessionWatchdogEffect,
    cleanup: boolean,
  ): Promise<SessionWatchdogEffectResult> => {
    const operation = context.readOperation();
    if (!effect.isCurrent()) {
      return context.isOwnerSettled() ? "settled" : "resumed";
    }
    if (await effect.recoverExecutionDeadline?.()) {
      return "resumed";
    }
    if (!effect.isCurrent()) {
      return context.isOwnerSettled() ? "settled" : "resumed";
    }
    const backend = getAttachedBackend(operation);
    const sessionId = operation.sessionId;
    const sessionKey = operation.key;
    const isCurrent = () =>
      effect.isCurrent() &&
      operation.sessionId === sessionId &&
      operation.key === sessionKey &&
      getAttachedBackend(operation) === backend;
    const assertCurrent = () => {
      if (!isCurrent()) {
        throw new Error("Watchdog owner retired before recovery");
      }
    };
    const { recoverTerminalSessionPlacementTurn } =
      await import("../agents/session-placement-admission.js");
    if (!isCurrent()) {
      return context.isOwnerSettled() ? "settled" : "resumed";
    }
    let workerFailure: string | undefined;
    try {
      workerFailure = await recoverTerminalSessionPlacementTurn(
        { sessionId, sessionKey, operation },
        assertCurrent,
      );
    } catch (error) {
      if (!isCurrent()) {
        return context.isOwnerSettled() ? "settled" : "resumed";
      }
      throw error;
    }
    if (!isCurrent()) {
      return context.isOwnerSettled() ? "settled" : "resumed";
    }
    if (workerFailure !== undefined) {
      // Placement recovery recorded the terminal failure; its producer still owns cleanup.
      return context.isOwnerSettled() ? "settled" : "blocked";
    }
    const cleanups = [...context.executionCleanups];
    const failures: unknown[] = [];
    try {
      if (
        !cleanup &&
        effect.reason === "semantic_stall" &&
        isReplyOperationAbortable(operation) &&
        backend?.recoverStalledModelCall
      ) {
        assertCurrent();
        if (backend.recoverStalledModelCall()) {
          return "model_recovery";
        }
      }
      assertCurrent();
      if (cleanup) {
        // After committed output, only this captured owner may retire native resources.
        context.preserveCleanupOutcome();
        context.detachUpstreamAbort();
        backend?.cancel("superseded");
      } else {
        stopSession({
          capture: captureSessionControllerStop({ operations: [operation] }),
          source: "watchdog",
          assertCurrent,
        });
      }
    } catch (error) {
      failures.push(error);
    }
    for (const cleanupOwner of cleanups) {
      // Stop may close the attempt synchronously; captured cleanup custody survives it.
      if (context.isOwnerSettled()) {
        break;
      }
      try {
        await cleanupOwner();
      } catch (error) {
        failures.push(error);
      }
    }
    if (
      cleanup &&
      (operation.result !== null || operation.phase === "yielded") &&
      context.readOwner().active === operation
    ) {
      const { failures: fenceFailures, fenced } = await context.revokeTerminalProducerFences();
      failures.push(...fenceFailures);
      if (fenced) {
        // Durable revocation rejects late writers before this exact operation releases its slot.
        context.settleResult();
        context.clearState();
        context.settleOwner();
      }
    }
    if (failures.length) {
      throw new AggregateError(failures, "Watchdog cleanup remains blocked");
    }
    return context.isOwnerSettled() ? "settled" : "blocked";
  };

  return { retireAdmissionAfterFencing, expireOwner };
}
