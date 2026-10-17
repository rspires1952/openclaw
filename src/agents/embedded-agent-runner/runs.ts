import { resolveTimerTimeoutMs } from "@openclaw/normalization-core/number-coercion";
import { createMessageInjectionAuthority } from "../../auto-reply/reply/message-injection-authority.js";
import {
  getAgentEventLifecycleGeneration,
  isAgentEventLifecycleGenerationCurrent,
} from "../../infra/agent-events.js";
import {
  getActiveAgentRunDelegatedAuthority,
  getAgentRunContext,
} from "../../infra/agent-run-registry.js";
import { notifyGatewayWorkMetricsChanged } from "../../infra/gateway-work-metrics-events.js";
import {
  isDiagnosticEmbeddedRunOwnerClosed,
  markDiagnosticEmbeddedRunStarted,
  markDiagnosticRunProgress,
} from "../../logging/diagnostic-run-activity.js";
import { logMessageQueuedWithBacklogPolicy } from "../../logging/diagnostic-runtime.js";
import { diagnosticLogger as diag, logSessionStateChange } from "../../logging/diagnostic.js";
import { normalizeAgentId, parseAgentSessionKey } from "../../routing/session-key.js";
import type { ReplyBackendHandle } from "../../sessions/session-controller.contracts.js";
import {
  abortActiveReplyRuns,
  resolveActiveReplyOperationForSessionId,
  resolveActiveSessionRunId,
  type ReplyOperation,
  waitForReplyOperationOwnerSettlement,
} from "../../sessions/session-controller.js";
import {
  waitForCapturedSessionNativeAttemptEnd,
  waitForSessionNativeAttemptEnd,
} from "../../sessions/session-controller.native-runtime.js";
import {
  assertSessionControllerOperation,
  getAttachedBackend,
  getSessionControllerEntryForOperation,
  markReplyOperationExecutionStarted,
  resolveReplyRunForCurrentSessionId,
} from "../../sessions/session-controller.state.js";
import {
  captureSessionControllerStop,
  stopSession,
} from "../../sessions/session-controller.stop.js";
import { createDeferredCore } from "../../shared/deferred.js";
import { settlesWithin } from "../../shared/settle-within.js";
import { SessionControllerAdmissionRequiredError } from "../harness/errors.js";
import {
  createAgentRunDirectAbortError,
  createAgentRunRestartAbortError,
} from "../run-termination.js";
import { resolveSessionPlacementForcedTerminalSettlement } from "../session-placement-forced-terminal-settlement.js";
import { getGatewayToolCallerIdentity } from "../tools/gateway-caller-context.js";
import {
  getActiveNativeAttempt,
  activeNativeAttempts,
  attachNativeAttempt,
  detachNativeAttempt,
  embeddedRunCleanupAttachment,
  getControllerEmbeddedAttachment,
  getEmbeddedRunAttachment,
  waitForEmbeddedRunOwnerSettlement,
  ACTIVE_EMBEDDED_RUNS_BY_RUN_ID,
  ACTIVE_EMBEDDED_RUN_SESSION_IDS_BY_FILE,
  ACTIVE_EMBEDDED_RUN_SNAPSHOTS,
  ABANDONED_EMBEDDED_RUNS_BY_SESSION_ID,
  ABANDONED_EMBEDDED_RUN_SESSION_IDS_BY_FILE,
  ABANDONED_EMBEDDED_RUN_SESSION_IDS_BY_KEY,
  EMBEDDED_RUN_COMPLETION_CLAIMS,
  EMBEDDED_RUN_FORCED_TERMINAL_SETTLEMENTS,
  type ActiveEmbeddedRunSnapshot,
  type AbandonedEmbeddedRun,
  type EmbeddedAgentQueueHandle,
  type EmbeddedRunCompletionClaim,
  type EmbeddedRunCompletionRegistration,
  type ActiveEmbeddedRunAttachment,
  type EmbeddedRunRegistration,
} from "./run-state.js";
import { isEmbeddedRunHandleAbortable, isEmbeddedRunHandleInProgress } from "./runs.probes.js";
import {
  clearActiveRunSessionIndex,
  normalizeSessionFileRegistryKey,
} from "./runs.session-index.js";
import { createEmbeddedRunsTestApi } from "./runs.test-cleanup.js";

export type { EmbeddedAgentQueueHandle, EmbeddedAgentQueueMessageOptions } from "./run-state.js";

export type EmbeddedRunTimeoutRecoveryMarker = {
  sessionId: string;
  recoveryToken: symbol;
};

function clearEmbeddedRunAbandonmentBySessionId(sessionId: string): void {
  const abandonedRun = ABANDONED_EMBEDDED_RUNS_BY_SESSION_ID.get(sessionId);
  if (!abandonedRun) {
    return;
  }
  ABANDONED_EMBEDDED_RUNS_BY_SESSION_ID.delete(sessionId);
  const clearIndex = (index: Map<string, string>, key: string | undefined) => {
    if (key && index.get(key) === sessionId) {
      index.delete(key);
    }
  };
  clearIndex(ABANDONED_EMBEDDED_RUN_SESSION_IDS_BY_KEY, abandonedRun.sessionKey?.trim());
  clearIndex(
    ABANDONED_EMBEDDED_RUN_SESSION_IDS_BY_FILE,
    normalizeSessionFileRegistryKey(abandonedRun.sessionFile),
  );
}

function clearEmbeddedRunAbandonment(params: {
  sessionId?: string;
  sessionKey?: string;
  sessionFile?: string;
}): void {
  const normalizedSessionId = params.sessionId?.trim();
  if (normalizedSessionId) {
    clearEmbeddedRunAbandonmentBySessionId(normalizedSessionId);
  }
  for (const [key, index] of [
    [params.sessionKey?.trim(), ABANDONED_EMBEDDED_RUN_SESSION_IDS_BY_KEY],
    [
      normalizeSessionFileRegistryKey(params.sessionFile),
      ABANDONED_EMBEDDED_RUN_SESSION_IDS_BY_FILE,
    ],
  ] as const) {
    const sessionId = key ? index.get(key) : undefined;
    if (sessionId) {
      clearEmbeddedRunAbandonmentBySessionId(sessionId);
    }
  }
}

export function markActiveEmbeddedRunAbandoned(params: {
  sessionId: string;
  handle: EmbeddedAgentQueueHandle;
  sessionKey?: string;
  sessionFile?: string;
  reason: AbandonedEmbeddedRun["reason"];
}): boolean {
  const sessionId = params.sessionId.trim();
  if (!sessionId || getActiveNativeAttempt(sessionId) !== params.handle) {
    return false;
  }
  const abandonedParams = { ...params, runId: params.handle.runId };
  clearEmbeddedRunAbandonment({ ...abandonedParams, sessionId });
  const normalizedSessionFile = normalizeSessionFileRegistryKey(abandonedParams.sessionFile);
  const abandonedRun: AbandonedEmbeddedRun = {
    sessionId,
    ...(abandonedParams.runId?.trim() ? { runId: abandonedParams.runId.trim() } : {}),
    abandonedAtMs: Date.now(),
    reason: abandonedParams.reason,
    ...(abandonedParams.sessionKey?.trim()
      ? { sessionKey: abandonedParams.sessionKey.trim() }
      : {}),
    ...(normalizedSessionFile ? { sessionFile: normalizedSessionFile } : {}),
  };
  ABANDONED_EMBEDDED_RUNS_BY_SESSION_ID.set(sessionId, abandonedRun);
  if (abandonedRun.sessionKey) {
    ABANDONED_EMBEDDED_RUN_SESSION_IDS_BY_KEY.set(abandonedRun.sessionKey, sessionId);
  }
  if (abandonedRun.sessionFile) {
    ABANDONED_EMBEDDED_RUN_SESSION_IDS_BY_FILE.set(abandonedRun.sessionFile, sessionId);
  }
  return true;
}

export function resolveEmbeddedRunAbandonment(params: {
  sessionId?: string;
  sessionKey?: string;
  sessionFile?: string;
}): AbandonedEmbeddedRun["reason"] | undefined {
  const normalizedSessionId = params.sessionId?.trim();
  const normalizedSessionKey = params.sessionKey?.trim();
  const normalizedSessionFile = normalizeSessionFileRegistryKey(params.sessionFile);
  const sessionIds = [
    normalizedSessionId,
    normalizedSessionKey
      ? ABANDONED_EMBEDDED_RUN_SESSION_IDS_BY_KEY.get(normalizedSessionKey)
      : undefined,
    normalizedSessionFile
      ? ABANDONED_EMBEDDED_RUN_SESSION_IDS_BY_FILE.get(normalizedSessionFile)
      : undefined,
  ];
  const reasons = new Set(
    sessionIds.map((sessionId) =>
      sessionId ? ABANDONED_EMBEDDED_RUNS_BY_SESSION_ID.get(sessionId)?.reason : undefined,
    ),
  );
  return reasons.has("timeout")
    ? "timeout"
    : reasons.has("recovering_timeout")
      ? "recovering_timeout"
      : undefined;
}

/**
 * Temporarily releases terminal-timeout delivery suppression while a timed-out
 * attempt is performing an eligible compaction-and-retry recovery.
 */
export function markEmbeddedRunRecoveringTimeout(params: {
  sessionId: string;
  runId?: string;
}): EmbeddedRunTimeoutRecoveryMarker | undefined {
  const abandoned = ABANDONED_EMBEDDED_RUNS_BY_SESSION_ID.get(params.sessionId.trim());
  if (
    !abandoned ||
    abandoned.reason !== "timeout" ||
    (abandoned.runId && abandoned.runId !== params.runId?.trim())
  ) {
    return undefined;
  }
  const recoveryToken = Symbol("openclaw.embeddedRunTimeoutRecovery");
  abandoned.reason = "recovering_timeout";
  abandoned.recoveryToken = recoveryToken;
  return { sessionId: abandoned.sessionId, recoveryToken };
}

/** Restores terminal-timeout suppression when recovery cannot continue. */
export function restoreEmbeddedRunTimeoutAbandonment(
  marker: EmbeddedRunTimeoutRecoveryMarker,
): boolean {
  const abandoned = ABANDONED_EMBEDDED_RUNS_BY_SESSION_ID.get(marker.sessionId.trim());
  if (
    !abandoned ||
    abandoned.reason !== "recovering_timeout" ||
    abandoned.recoveryToken !== marker.recoveryToken
  ) {
    return false;
  }
  abandoned.reason = "timeout";
  delete abandoned.recoveryToken;
  return true;
}

function logActiveRunMessageAccepted(sessionId: string): void {
  // Active-run steering is consumed by the current turn, not queued as another
  // turn for the single idle transition to drain. Keep the event and activity.
  logMessageQueuedWithBacklogPolicy(
    {
      sessionId,
      source: "embedded-agent-runner",
    },
    false,
  );
}

function clearEmbeddedRunAbortability(handle: EmbeddedAgentQueueHandle): void {
  getEmbeddedRunAttachment(handle)?.humanInputWaits?.clear();
  if (!handle.runId || ACTIVE_EMBEDDED_RUNS_BY_RUN_ID.get(handle.runId)?.handle !== handle) {
    return;
  }
  ACTIVE_EMBEDDED_RUNS_BY_RUN_ID.delete(handle.runId);
}

/** TUI preflight requires V2 ownership; failure leaves ordinary input to local queue policy. */
export async function claimPendingEmbeddedAgentQuestionAnswer(
  sessionId: string,
  text: string,
): Promise<{ runId: string } | null> {
  const handle = getActiveNativeAttempt(sessionId);
  const guarded = handle?.messageInjectionV2;
  if (!handle?.runId?.trim() || guarded?.version !== 2 || !guarded.claimPendingUserInputAnswer) {
    return null;
  }
  const runId = handle.runId;
  const registration = getEmbeddedRunAttachment(handle);
  let isCurrent: () => boolean;
  try {
    const operation = resolveActiveReplyOperationForSessionId(sessionId);
    const ownedOperation =
      operation && getAttachedBackend(operation) === handle ? operation : undefined;
    // The captured native attempt, its tool authority, and any owning turn must all stay current.
    isCurrent = () =>
      getActiveNativeAttempt(sessionId) === handle &&
      getEmbeddedRunAttachment(handle) === registration &&
      (!ownedOperation ||
        (resolveActiveReplyOperationForSessionId(sessionId) === ownedOperation &&
          getAttachedBackend(ownedOperation) === handle));
    registration?.toolAuthority?.assertActive();
    if (!guarded.isAvailable() || !isCurrent()) {
      return null;
    }
  } catch {
    return null;
  }
  // V2 carries the captured owner assertion through persistence and final dispatch.
  // An unconfirmed answer must propagate; queue fallback could replay accepted input.
  const assertCurrent = createMessageInjectionAuthority(() => {
    registration?.toolAuthority?.assertActive();
    return isCurrent();
  });
  const claimed = await guarded.claimPendingUserInputAnswer(
    text,
    { isInboundUserMessage: true },
    assertCurrent,
    "run",
  );
  if (!claimed) {
    return null;
  }
  logActiveRunMessageAccepted(sessionId);
  return { runId };
}

/** Interrupt aborts the exact turn and keeps its waiting inputs and children. */
function interruptSessionTurn(operation: ReplyOperation, reason?: "restart"): boolean {
  return stopSession({
    source: "interrupt",
    capture: captureSessionControllerStop({ operations: [operation] }),
    // A direct run abort keeps its typed reason, so callers record it as a direct abort.
    reason:
      reason === "restart" ? createAgentRunRestartAbortError() : createAgentRunDirectAbortError(),
    // The controller result distinguishes a committed abort from an observer failure.
    onError: () => "continue",
  }).aborted;
}

function revokeCompletionClaim(sessionId: string, runId?: string): void {
  const claim = EMBEDDED_RUN_COMPLETION_CLAIMS.get(sessionId);
  if (claim && (runId === undefined || claim.runId === runId)) {
    claim.settleRegistration(undefined);
    EMBEDDED_RUN_COMPLETION_CLAIMS.delete(sessionId);
  }
}

/**
 * Abort embedded OpenClaw runs.
 *
 * - With a sessionId, aborts that single run.
 * - With no sessionId, supports targeted abort modes (for example, compacting runs only).
 */
export function abortEmbeddedAgentRun(sessionId: string, opts?: { reason?: "restart" }): boolean;
export function abortEmbeddedAgentRun(
  sessionId: undefined,
  opts: { mode: "all" | "compacting"; reason?: "restart" },
): boolean;
export function abortEmbeddedAgentRun(
  sessionId?: string,
  opts?: { mode?: "all" | "compacting"; reason?: "restart" },
): boolean {
  if (typeof sessionId === "string" && sessionId.length > 0) {
    const handle = getActiveNativeAttempt(sessionId);
    const operation = handle
      ? getEmbeddedRunAttachment(handle)?.operation
      : resolveActiveReplyOperationForSessionId(sessionId);
    if (operation) {
      return interruptSessionTurn(operation, opts?.reason);
    }
    if (!handle) {
      return false;
    }
    if (
      !isEmbeddedRunHandleAbortable(sessionId, handle, "all") ||
      getActiveNativeAttempt(sessionId) !== handle
    ) {
      return false;
    }
    // Detached runtimes have no session turn; their exact handle owns cancellation.
    try {
      handle.abort(opts?.reason);
    } catch (err) {
      diag.warn(`abort failed: sessionId=${sessionId} err=${String(err)}`);
      return false;
    }
    revokeCompletionClaim(sessionId, handle.runId);
    return true;
  }

  const mode = opts?.mode;
  if (mode !== "all" && mode !== "compacting") {
    return false;
  }
  const detachedTargets = [...activeNativeAttempts()].filter(
    ([, handle]) => !getEmbeddedRunAttachment(handle)?.operation,
  );
  const replyAborted = abortActiveReplyRuns({
    mode,
    onAbortError: (id, err) =>
      diag.warn(`abort failed: sessionId=${id} owner=reply_run err=${String(err)}`),
  });
  let aborted = false;
  for (const [id, handle] of detachedTargets) {
    if (getActiveNativeAttempt(id) !== handle || !isEmbeddedRunHandleAbortable(id, handle, mode)) {
      continue;
    }
    diag.debug(`aborting ${mode === "compacting" ? "compacting " : ""}run: sessionId=${id}`);
    try {
      handle.abort(opts?.reason);
      revokeCompletionClaim(id, handle.runId);
      aborted = true;
    } catch (err) {
      diag.warn(`abort failed: sessionId=${id} err=${String(err)}`);
    }
  }
  return replyAborted || aborted;
}

type EmbeddedHeartbeatPreemptionResult = "not-heartbeat" | "drained" | "timed-out";

export async function preemptAndDrainEmbeddedHeartbeatRun(
  sessionId: string,
  timeoutMs: number,
): Promise<EmbeddedHeartbeatPreemptionResult> {
  const handle = getActiveNativeAttempt(sessionId);
  if (!handle?.preemptByVisibleTurn) {
    return "not-heartbeat";
  }
  const drainPromise = waitForSessionNativeAttemptEnd(sessionId, timeoutMs, handle);
  try {
    if (!handle.preemptByVisibleTurn()) {
      return "not-heartbeat";
    }
  } catch (err) {
    diag.warn(`heartbeat preemption failed: sessionId=${sessionId} err=${String(err)}`);
  } finally {
    notifyGatewayWorkMetricsChanged();
  }
  return (await drainPromise) ? "drained" : "timed-out";
}

export function prepareEmbeddedAgentRunCompletionClaim(
  sessionId: string,
  runId: string,
): {
  bindOperationalRunInstance: (
    instance: NonNullable<EmbeddedRunRegistration["operationalRunInstance"]>,
  ) => boolean;
  claimCompletion: () => boolean;
  claimFailure: () => boolean;
  resolveCurrentRegistration: () => EmbeddedRunCompletionRegistration | undefined;
  registered: Promise<EmbeddedRunCompletionRegistration | undefined>;
} {
  const { promise: registered, resolve: settleRegistration } = createDeferredCore<
    EmbeddedRunCompletionRegistration | undefined
  >();
  const claim: EmbeddedRunCompletionClaim = {
    runId,
    lifecycleGeneration: getAgentEventLifecycleGeneration(),
    promoted: false,
    settleRegistration,
  };
  revokeCompletionClaim(sessionId);
  EMBEDDED_RUN_COMPLETION_CLAIMS.set(sessionId, claim);
  const consume = (allowUnregistered: boolean): boolean => {
    if (EMBEDDED_RUN_COMPLETION_CLAIMS.get(sessionId) !== claim) {
      return false;
    }
    EMBEDDED_RUN_COMPLETION_CLAIMS.delete(sessionId);
    if (!claim.promoted) {
      claim.settleRegistration(undefined);
    }
    return (
      (allowUnregistered || claim.promoted) &&
      !claim.operation?.abortSignal.aborted &&
      isAgentEventLifecycleGenerationCurrent(claim.lifecycleGeneration)
    );
  };
  const bindOperationalRunInstance = (
    instance: NonNullable<EmbeddedRunRegistration["operationalRunInstance"]>,
  ): boolean => {
    if (
      EMBEDDED_RUN_COMPLETION_CLAIMS.get(sessionId) !== claim ||
      !isAgentEventLifecycleGenerationCurrent(claim.lifecycleGeneration) ||
      instance.runId !== runId ||
      (claim.operationalRunInstance !== undefined && claim.operationalRunInstance !== instance)
    ) {
      return false;
    }
    claim.operationalRunInstance = instance;
    return true;
  };
  const resolveCurrentRegistration = (): EmbeddedRunCompletionRegistration | undefined => {
    if (
      EMBEDDED_RUN_COMPLETION_CLAIMS.get(sessionId) !== claim ||
      !isAgentEventLifecycleGenerationCurrent(claim.lifecycleGeneration)
    ) {
      return undefined;
    }
    const handle = getActiveNativeAttempt(sessionId);
    const registration = handle ? getEmbeddedRunAttachment(handle) : undefined;
    const toolAuthority = registration?.toolAuthority;
    if (
      !handle ||
      handle.runId !== runId ||
      !toolAuthority ||
      !claim.operationalRunInstance ||
      registration.operationalRunInstance !== claim.operationalRunInstance
    ) {
      return undefined;
    }
    try {
      toolAuthority.assertActive();
    } catch {
      return undefined;
    }
    return EMBEDDED_RUN_COMPLETION_CLAIMS.get(sessionId) === claim &&
      getActiveNativeAttempt(sessionId) === handle &&
      getEmbeddedRunAttachment(handle) === registration &&
      isAgentEventLifecycleGenerationCurrent(claim.lifecycleGeneration)
      ? { toolAuthority }
      : undefined;
  };
  return {
    bindOperationalRunInstance,
    claimCompletion: () => consume(false),
    claimFailure: () => consume(true),
    resolveCurrentRegistration,
    registered,
  };
}

export function isEmbeddedAgentRunHandleActive(sessionId: string): boolean {
  return Boolean(getActiveNativeAttempt(sessionId));
}

/** True when work other than the externally admitted run currently owns the session. */
export function isEmbeddedAgentSessionHeldByOtherRun(sessionId: string, runId: string): boolean {
  const handle = getActiveNativeAttempt(sessionId);
  return handle
    ? handle.runId !== runId
    : Boolean(resolveActiveReplyOperationForSessionId(sessionId));
}

export function resolveActiveEmbeddedRunHandleSessionId(sessionKey: string): string | undefined {
  const normalizedSessionKey = sessionKey.trim();
  if (!normalizedSessionKey) {
    return undefined;
  }
  const operation = resolveActiveReplyOperationForSessionId(
    resolveActiveSessionRunId(normalizedSessionKey) ?? "",
  );
  return operation && getActiveNativeAttempt(operation.sessionId) ? operation.sessionId : undefined;
}

export type ActiveEmbeddedRunOwner = {
  runId: string;
  sessionId: string;
  sessionKey?: string;
  agentId?: string;
  startedAtMs?: number;
  abort: () => boolean;
  /** Stops this captured owner and distinguishes a frozen writer from a stale target. */
  stop: () => "aborted" | "finalizing" | "unchanged";
  /** Joins this captured native attempt and its producer, never a same-ID successor. */
  waitForSettlement: () => Promise<void>;
};

/** Captures settlement for the exact native attempt currently attached to a session. */
export function captureActiveEmbeddedRunAttemptSettlement(
  sessionId: string,
): Promise<void> | undefined {
  const handle = getActiveNativeAttempt(sessionId);
  const registration = handle ? getEmbeddedRunAttachment(handle) : undefined;
  return registration?.settlement.promise;
}

function projectActiveEmbeddedRunOwner(
  registration: ActiveEmbeddedRunAttachment,
  handle: EmbeddedAgentQueueHandle,
): ActiveEmbeddedRunOwner | undefined {
  const runId = handle.runId;
  if (!runId || !isEmbeddedRunHandleInProgress(handle)) {
    return undefined;
  }
  const stop = (): "aborted" | "finalizing" | "unchanged" => {
    if (
      getActiveNativeAttempt(registration.sessionId) !== handle ||
      getEmbeddedRunAttachment(handle) !== registration ||
      ACTIVE_EMBEDDED_RUNS_BY_RUN_ID.get(runId)?.handle !== handle
    ) {
      return "unchanged";
    }
    if (registration.operation?.abortFrozen || !isEmbeddedRunHandleAbortable(runId, handle)) {
      return "finalizing";
    }
    try {
      if (registration.operation) {
        if (!registration.operation.abortByUser()) {
          return "unchanged";
        }
      } else if (handle.cancel) {
        handle.cancel("user_abort");
      } else {
        handle.abort();
      }
      revokeCompletionClaim(registration.sessionId, runId);
      return "aborted";
    } catch {
      // A throwing backend cannot undo cancellation already committed by its owner.
      return registration.operation?.result?.kind === "aborted" ? "aborted" : "unchanged";
    }
  };
  return {
    runId,
    sessionId: registration.sessionId,
    ...(registration.sessionKey ? { sessionKey: registration.sessionKey } : {}),
    ...(registration.agentId ? { agentId: registration.agentId } : {}),
    ...(handle.startedAtMs === undefined ? {} : { startedAtMs: handle.startedAtMs }),
    waitForSettlement: () => waitForEmbeddedRunOwnerSettlement(registration),
    // A recovered run ID is correlation only. Recheck the captured owner before
    // Stop so a stale UI action cannot abort replacement work in the session.
    stop,
    abort: () => stop() === "aborted",
  };
}

export function resolveActiveEmbeddedRunOwner(
  sessionId: string,
): ActiveEmbeddedRunOwner | undefined {
  const handle = getActiveNativeAttempt(sessionId);
  const registration = handle ? getEmbeddedRunAttachment(handle) : undefined;
  return handle && registration ? projectActiveEmbeddedRunOwner(registration, handle) : undefined;
}

export function resolveActiveEmbeddedRunOwnerForOperation(
  operation: ReplyOperation,
): ActiveEmbeddedRunOwner | undefined {
  const attachment = getControllerEmbeddedAttachment(operation);
  return attachment ? projectActiveEmbeddedRunOwner(attachment, attachment.handle) : undefined;
}

export function resolveActiveEmbeddedRunOwnerByRunId(
  runId: string,
): ActiveEmbeddedRunOwner | undefined {
  const normalizedRunId = runId.trim();
  const handle = normalizedRunId
    ? ACTIVE_EMBEDDED_RUNS_BY_RUN_ID.get(normalizedRunId)?.handle
    : undefined;
  if (!handle) {
    return undefined;
  }
  const registration = getEmbeddedRunAttachment(handle);
  return registration && getActiveNativeAttempt(registration.sessionId) === handle
    ? projectActiveEmbeddedRunOwner(registration, handle)
    : undefined;
}

export type EmbeddedRunDrainTarget = NonNullable<ReturnType<typeof captureEmbeddedRunDrainTarget>>;

/** Lifecycle drains join the captured producer even after its registration is retired. */
export function captureEmbeddedRunDrainTarget(
  sessionId: string,
  owner: { agentId?: string; defaultAgentId?: string },
) {
  const handle = getActiveNativeAttempt(sessionId);
  const registration = handle && getEmbeddedRunAttachment(handle);
  const expectedAgentId = owner.agentId ?? owner.defaultAgentId;
  const recordedAgentId =
    registration?.agentId ??
    parseAgentSessionKey(registration?.sessionKey)?.agentId ??
    owner.defaultAgentId;
  if (
    !handle ||
    !registration ||
    !expectedAgentId ||
    !recordedAgentId ||
    normalizeAgentId(expectedAgentId) !== normalizeAgentId(recordedAgentId)
  ) {
    return undefined;
  }
  let released = false;
  const ended = waitForEmbeddedRunOwnerSettlement(registration).then(() => true);
  const isActive = () => !released && !registration.settled;
  return {
    sessionId,
    agentId: registration.agentId,
    sessionKey: registration.sessionKey,
    isActive,
    isAborted: () => !isEmbeddedRunHandleInProgress(handle),
    abort: () => {
      if (
        !isActive() ||
        getActiveNativeAttempt(sessionId) !== handle ||
        getEmbeddedRunAttachment(handle) !== registration
      ) {
        return false;
      }
      return registration.operation
        ? interruptSessionTurn(registration.operation)
        : abortEmbeddedAgentRun(sessionId);
    },
    waitForEnd: async (timeoutMs: number | null) =>
      timeoutMs === null
        ? await ended
        : await settlesWithin(ended, resolveTimerTimeoutMs(timeoutMs, 100, 100)),
    release: () => {
      released = true;
    },
  };
}

export function isActiveEmbeddedRunId(runId: string): boolean {
  const normalizedRunId = runId.trim();
  const handle = normalizedRunId
    ? ACTIVE_EMBEDDED_RUNS_BY_RUN_ID.get(normalizedRunId)?.handle
    : undefined;
  const registration = handle ? getEmbeddedRunAttachment(handle) : undefined;
  return Boolean(
    handle &&
    registration &&
    getActiveNativeAttempt(registration.sessionId) === handle &&
    isEmbeddedRunHandleInProgress(handle),
  );
}

function resolveActiveEmbeddedRunHandleSessionIdBySessionFile(
  sessionFile: string,
): string | undefined {
  const normalizedSessionFile = normalizeSessionFileRegistryKey(sessionFile);
  return normalizedSessionFile
    ? ACTIVE_EMBEDDED_RUN_SESSION_IDS_BY_FILE.get(normalizedSessionFile)
    : undefined;
}

export { resolveActiveEmbeddedRunHandleSessionIdBySessionFile as resolveActiveEmbeddedRunSessionIdBySessionFile };

export function getActiveEmbeddedRunSnapshot(
  sessionId: string,
): ActiveEmbeddedRunSnapshot | undefined {
  return ACTIVE_EMBEDDED_RUN_SNAPSHOTS.get(sessionId);
}

export type AbortAndDrainEmbeddedAgentRunResult = {
  aborted: boolean;
  drained: boolean;
  forceCleared: boolean;
  recoveringModelAttempt?: true;
};

export async function abortAndDrainEmbeddedAgentRun(params: {
  sessionId: string;
  sessionKey?: string;
  settleMs?: number;
  forceClear?: boolean;
  reason?: string;
}): Promise<AbortAndDrainEmbeddedAgentRunResult> {
  const settleMs = params.settleMs ?? 15_000;
  const settleDeadline = Date.now() + resolveTimerTimeoutMs(settleMs, 100, 100);
  // Capture both owners before cancellation can reenter and install a successor.
  const handle = getActiveNativeAttempt(params.sessionId);
  const registration = handle ? getEmbeddedRunAttachment(handle) : undefined;
  const operation = handle
    ? registration?.operation
    : resolveActiveReplyOperationForSessionId(params.sessionId);
  let aborted = false;
  if (params.reason === "stuck_recovery" && operation) {
    const wasAborted = operation.abortSignal.aborted;
    const decision = await operation.watchdog.tick();
    if (decision.action === "observe" && decision.reason === "model_recovery") {
      return { aborted: true, drained: false, forceCleared: false, recoveringModelAttempt: true };
    }
    // A committed Stop may tick as cleanup-blocked; report it as accepted, not settled.
    aborted =
      (!wasAborted && operation.abortSignal.aborted) ||
      decision.action === "stop" ||
      decision.action === "expire_cleanup";
  } else if (operation) {
    aborted = interruptSessionTurn(operation);
  } else if (
    handle &&
    isEmbeddedRunHandleAbortable(params.sessionId, handle) &&
    getActiveNativeAttempt(params.sessionId) === handle
  ) {
    try {
      if (params.reason === "stuck_recovery" && handle.recoverStalledModelCall?.() === true) {
        return { aborted: true, drained: false, forceCleared: false, recoveringModelAttempt: true };
      }
      handle.abort();
      aborted = true;
    } catch (err) {
      diag.warn(`abort failed: sessionId=${params.sessionId} err=${String(err)}`);
    }
  }
  const remainingMs = Math.max(0, settleDeadline - Date.now());
  const nativeSettlement = waitForCapturedSessionNativeAttemptEnd(
    params.sessionId,
    remainingMs,
    registration,
  );
  const ownerSettlement = operation
    ? waitForReplyOperationOwnerSettlement(operation, remainingMs)
    : Promise.resolve(true);
  // Forced placement cleanup may revoke exact write authority, but cannot prove
  // the raw backend finished. Keep native registration and session custody until it does.
  if (params.forceClear && handle) {
    const cleanup = EMBEDDED_RUN_FORCED_TERMINAL_SETTLEMENTS.get(handle);
    if (cleanup && getEmbeddedRunAttachment(handle)?.operation === operation) {
      await cleanup();
    }
  }
  const [nativeSettled, ownerSettled] = await Promise.all([nativeSettlement, ownerSettlement]);
  return { aborted, drained: nativeSettled && ownerSettled, forceCleared: false };
}

function hasNativeBackendControl(
  handle: EmbeddedAgentQueueHandle,
): handle is EmbeddedAgentQueueHandle & ReplyBackendHandle {
  return handle.kind === "embedded" && typeof handle.cancel === "function";
}

export function setActiveEmbeddedRun(
  sessionId: string,
  handle: EmbeddedAgentQueueHandle,
  sessionKey?: string,
  sessionFile?: string,
  agentId?: string,
  admittedOperation?: ReplyOperation,
  lifecycleGeneration = getAgentEventLifecycleGeneration(),
) {
  const sessionIdentity = { sessionId, sessionKey, sessionFile };
  const incomingLifecycleGeneration =
    handle[embeddedRunCleanupAttachment]?.lifecycleGeneration ?? lifecycleGeneration;
  // The immutable handle generation rejects delayed stale registration even
  // when rotation left no replacement owner in the session slot.
  if (!isAgentEventLifecycleGenerationCurrent(incomingLifecycleGeneration)) {
    revokeCompletionClaim(sessionId, handle.runId);
    try {
      handle.abort("restart");
    } catch (error) {
      diag.warn(`stale run registration abort failed: sessionId=${sessionId} err=${String(error)}`);
      throw error;
    }
    return undefined;
  }
  if (handle.diagnosticOwner && isDiagnosticEmbeddedRunOwnerClosed(handle.diagnosticOwner)) {
    revokeCompletionClaim(sessionId, handle.runId);
    handle.abort("restart");
    return undefined;
  }
  const caller = getGatewayToolCallerIdentity();
  let toolAuthority: EmbeddedRunRegistration["toolAuthority"];
  try {
    toolAuthority = caller?.embeddedRunToolAuthorityBinding?.({
      ...sessionIdentity,
      agentId,
      handle,
    });
  } catch (error) {
    revokeCompletionClaim(sessionId, handle.runId);
    throw error;
  }
  const operation = toolAuthority?.operation ?? admittedOperation;
  if (
    admittedOperation &&
    toolAuthority?.operation &&
    admittedOperation !== toolAuthority.operation
  ) {
    throw new Error("Native registration received conflicting session turn owners");
  }
  if (operation) {
    assertSessionControllerOperation(operation);
    if (
      operation.key !== sessionKey ||
      operation.sessionId !== sessionId ||
      (operation.agentId && agentId && operation.agentId !== normalizeAgentId(agentId))
    ) {
      throw new Error("Native registration does not match the exact admitted session turn");
    }
  } else if (sessionKey && toolAuthority?.detached === true) {
    // A prepared detached attempt can carry a policy key without owning that session.
    toolAuthority.assertActive();
  } else if (sessionKey) {
    throw new SessionControllerAdmissionRequiredError();
  }
  const previousAttachment = operation
    ? getControllerEmbeddedAttachment(operation)
    : getActiveNativeAttempt(sessionId)?.[embeddedRunCleanupAttachment];
  const wasActive = previousAttachment !== undefined;
  if (previousAttachment) {
    const previousHandle = previousAttachment.handle;
    previousAttachment.watchdogAttempt?.close();
    previousAttachment.closeWatchdogWait?.();
    previousHandle.closeDiagnostics?.();
    clearEmbeddedRunAbortability(previousHandle);
    detachNativeAttempt(previousAttachment);
    EMBEDDED_RUN_FORCED_TERMINAL_SETTLEMENTS.delete(previousHandle);
  }
  try {
    toolAuthority?.assertActive();
  } catch (error) {
    revokeCompletionClaim(sessionId, handle.runId);
    throw error;
  }
  clearEmbeddedRunAbandonment({ sessionId, sessionKey, sessionFile });
  if (operation && getAttachedBackend(operation) !== handle) {
    if (hasNativeBackendControl(handle)) {
      operation.attachBackend(handle);
    } else {
      operation.attachBackend({
        ...handle,
        kind: "embedded",
        cancel: (reason) =>
          handle.cancel
            ? handle.cancel(reason)
            : handle.abort(reason === "restart" ? reason : undefined),
      });
    }
  }
  // The dispatch scope carries the admitted instance across both core and
  // plugin attempts. A handle's public runId alone cannot confer wait authority.
  const operationalRunInstance = caller?.operationalRunInstance;
  const runContext = handle.runId ? getAgentRunContext(handle.runId) : undefined;
  const watchdogAttempt = handle.diagnosticOwner?.watchdogAttempt ?? toolAuthority?.watchdogAttempt;
  const registration: EmbeddedRunRegistration = {
    handle,
    lifecycleGeneration: incomingLifecycleGeneration,
    settlement: createDeferredCore(),
    watchdogAttempt,
    projectSessionActive:
      (operation
        ? getSessionControllerEntryForOperation(operation).attachment?.projectSessionActive
        : undefined) ??
      (runContext?.lifecycleGeneration === incomingLifecycleGeneration
        ? runContext.projectSessionActive
        : undefined),
    toolAuthority,
    operationalRunInstance,
    sessionId,
    // Legacy SDK callers may omit this; a matching live binding proves the captured owner.
    agentId: agentId ?? (toolAuthority ? caller?.agentId : undefined),
    ...(sessionKey ? { sessionKey } : {}),
    delegatedAuthority:
      operationalRunInstance?.runId === handle.runId && operationalRunInstance
        ? getActiveAgentRunDelegatedAuthority(operationalRunInstance)
        : undefined,
    onHumanInputResolved: () => {
      if (operation && getActiveNativeAttempt(sessionId) === handle) {
        operation.recordActivity();
      }
      markDiagnosticRunProgress({ sessionId, sessionKey, reason: "human_input:resolved" });
      // A real resolution resumes work and invalidates recovery queued before it.
      // This does not refresh progress while waiting or extend any run deadline.
      logSessionStateChange({
        ...sessionIdentity,
        state: "processing",
        reason: "human_input_resolved",
      });
    },
  };
  let attachment: ActiveEmbeddedRunAttachment = registration;
  if (operation) {
    const controllerAttachment = getSessionControllerEntryForOperation(operation).attachment;
    if (!controllerAttachment || controllerAttachment.operation !== operation) {
      throw new Error("Native registration requires its controller backend attachment");
    }
    attachment = Object.assign(controllerAttachment, registration, { operation });
  }
  attachNativeAttempt(attachment);
  handle[embeddedRunCleanupAttachment] = attachment;
  if (watchdogAttempt && handle.ownsLiveness) {
    const wait = watchdogAttempt.beginWait({
      kind: "runtime_owned",
      isCurrent: () => {
        if (
          getEmbeddedRunAttachment(handle) !== attachment ||
          (operation && getSessionControllerEntryForOperation(operation).attachment !== attachment)
        ) {
          return false;
        }
        toolAuthority?.assertActive();
        return handle.ownsLiveness?.() === true && !handle.isAborted?.() && !handle.isStopped?.();
      },
    });
    attachment.closeWatchdogWait = () => wait.close();
  }
  if (operation) {
    if (toolAuthority?.sourceTurnId) {
      getSessionControllerEntryForOperation(operation).sourceTurnId = toolAuthority.sourceTurnId;
    }
    markReplyOperationExecutionStarted(operation);
    if (operation.phase === "queued") {
      operation.setPhase("running");
    }
  }
  const forcedTerminalSettlement = resolveSessionPlacementForcedTerminalSettlement();
  if (forcedTerminalSettlement) {
    EMBEDDED_RUN_FORCED_TERMINAL_SETTLEMENTS.set(handle, forcedTerminalSettlement);
  }
  if (handle.runId) {
    ACTIVE_EMBEDDED_RUNS_BY_RUN_ID.set(handle.runId, attachment);
  }
  clearActiveRunSessionIndex(ACTIVE_EMBEDDED_RUN_SESSION_IDS_BY_FILE, sessionId);
  const normalizedSessionFile = normalizeSessionFileRegistryKey(sessionFile);
  if (normalizedSessionFile) {
    ACTIVE_EMBEDDED_RUN_SESSION_IDS_BY_FILE.set(normalizedSessionFile, sessionId);
  }
  notifyGatewayWorkMetricsChanged();
  logSessionStateChange({
    ...sessionIdentity,
    state: "processing",
    reason: wasActive ? "run_replaced" : "run_started",
  });
  markDiagnosticEmbeddedRunStarted({
    sessionId,
    sessionKey,
    runId: handle.runId,
    owner: handle.diagnosticOwner,
  });
  const completionClaim = EMBEDDED_RUN_COMPLETION_CLAIMS.get(sessionId);
  if (
    completionClaim &&
    completionClaim.runId === handle.runId &&
    completionClaim.lifecycleGeneration === incomingLifecycleGeneration &&
    (completionClaim.operationalRunInstance === undefined ||
      completionClaim.operationalRunInstance === operationalRunInstance)
  ) {
    completionClaim.operation = operation;
    completionClaim.promoted = true;
    completionClaim.settleRegistration(toolAuthority ? { toolAuthority } : undefined);
  } else if (completionClaim) {
    revokeCompletionClaim(sessionId);
  }
  return attachment;
}

export function updateActiveEmbeddedRunSnapshot(
  sessionId: string,
  snapshot: ActiveEmbeddedRunSnapshot,
) {
  if (!getActiveNativeAttempt(sessionId)) {
    return;
  }
  ACTIVE_EMBEDDED_RUN_SNAPSHOTS.set(sessionId, snapshot);
}

export function clearActiveEmbeddedRun(
  sessionId: string,
  handle: EmbeddedAgentQueueHandle,
  sessionKey?: string,
  sessionFile?: string,
  reason = "run_completed",
  expectedAttachment?: ActiveEmbeddedRunAttachment,
) {
  const activeHandle = getActiveNativeAttempt(sessionId);
  const registration =
    expectedAttachment ?? handle[embeddedRunCleanupAttachment] ?? getEmbeddedRunAttachment(handle);
  if (!registration || registration.settled) {
    return;
  }
  const controllerOwner = resolveReplyRunForCurrentSessionId(sessionId);
  const ownsSessionProjection =
    (activeHandle === handle &&
      (registration.operation === undefined ||
        (controllerOwner.kind === "one" &&
          controllerOwner.operation === registration.operation))) ||
    (!activeHandle && registration.operation !== undefined && controllerOwner.kind === "none");
  registration.closeWatchdogWait?.();
  registration.watchdogAttempt?.close();
  const operation = registration.operation;
  const backend = registration.backend;
  clearEmbeddedRunAbortability(handle);
  detachNativeAttempt(registration);
  notifyGatewayWorkMetricsChanged();
  if (operation && backend) {
    operation.detachBackend(backend);
  }
  // Generation-fenced: closing cannot retire a successor's diagnostic owner.
  handle.closeDiagnostics?.();
  if (ownsSessionProjection) {
    ACTIVE_EMBEDDED_RUN_SNAPSHOTS.delete(sessionId);
    clearActiveRunSessionIndex(ACTIVE_EMBEDDED_RUN_SESSION_IDS_BY_FILE, sessionId);
    logSessionStateChange({
      sessionId,
      sessionKey,
      sessionFile,
      state: "idle",
      reason,
    });
  }
  EMBEDDED_RUN_FORCED_TERMINAL_SETTLEMENTS.delete(handle);
  // Exact attachment waiters own teardown even after another run takes the session slot.
  registration.settled = true;
  registration.settlement.resolve();
}

if (process.env.VITEST || process.env.NODE_ENV === "test") {
  (globalThis as Record<PropertyKey, unknown>)[Symbol.for("openclaw.embeddedRunsTestApi")] =
    createEmbeddedRunsTestApi(clearActiveEmbeddedRun);
}

/* oxlint-disable max-lines -- TODO: split this grandfathered oversized file. */
