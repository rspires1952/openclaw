import {
  collectAmbiguousAutomaticMediaUrls,
  collectAutomaticDeliveredMediaUrls,
  collectDeliveredMediaUrls,
  getAgentCommandDeliveryFailure,
  hasCommittedOutboundDeliveryEvidence,
  hasCompleteAutomaticMediaDeliveryOutcomeEvidence,
  hasExplicitlyVisibleAgentPayload,
  type AgentDeliveryEvidence,
} from "../agents/embedded-agent-runner/delivery-evidence.js";
import { formatGeneratedMediaDeliveryRetryForPrompt } from "../agents/internal-events.js";
import {
  advanceSessionDeliveryAgentRun,
  deferSessionDelivery,
  failSessionDelivery,
  markSessionDeliverySettlement,
} from "../infra/session-delivery-queue-storage.js";
import {
  SessionDeliveryDeadLetteredError,
  SessionDeliveryDeferredError,
  SessionDeliveryRetryChargedError,
  type QueuedSessionDelivery,
  type SessionDeliveryRoute,
} from "../infra/session-delivery-queue.records.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { normalizeMediaReferenceForComparison } from "../media/media-reference-comparison.js";
import type { OpenClawStateWorkerContext } from "../state/openclaw-state-worker-context.types.js";
import { INTERNAL_MESSAGE_CHANNEL } from "../utils/message-channel.js";

const log = createSubsystemLogger("gateway/restart-sentinel");
export const AGENT_DELIVERY_OWNERSHIP_RETRY_MS = 1_000;

type QueuedAgentTurnSessionDelivery = Extract<QueuedSessionDelivery, { kind: "agentTurn" }>;

export async function deadLetterSessionDelivery(
  entry: QueuedAgentTurnSessionDelivery,
  reason: string,
  queueContext: OpenClawStateWorkerContext,
): Promise<never> {
  await markSessionDeliverySettlement(entry, "moved-to-failed", queueContext);
  log.warn("queued session delivery requires durable dead-letter settlement", {
    queueId: entry.id,
  });
  throw new SessionDeliveryDeadLetteredError(reason);
}

function hasQueuedVisibleAgentPayload(result: Pick<AgentDeliveryEvidence, "payloads">): boolean {
  return Array.isArray(result.payloads) && result.payloads.some(hasExplicitlyVisibleAgentPayload);
}

function hasUnexpectedRecoverySideEffects(result: AgentDeliveryEvidence): boolean {
  return (
    result.restartUnsafeSideEffectsDetected === true ||
    result.messagingToolAggregateEvidenceUnaccounted === true ||
    result.messagingToolSentTargetsTruncated === true ||
    result.didSendDeterministicApprovalPrompt === true ||
    hasCommittedOutboundDeliveryEvidence(result)
  );
}

function collectVisiblePayloadMediaUrls(result: AgentDeliveryEvidence): string[] {
  const payloads = Array.isArray(result.payloads) ? result.payloads : [];
  return collectDeliveredMediaUrls({ payloads: payloads.filter(hasExplicitlyVisibleAgentPayload) });
}

function collectQueuedDeliveredMediaUrls(params: {
  result: AgentDeliveryEvidence;
  route: SessionDeliveryRoute;
}): string[] {
  if (params.route.channel === INTERNAL_MESSAGE_CHANNEL) {
    return collectVisiblePayloadMediaUrls(params.result);
  }
  return collectAutomaticDeliveredMediaUrls(params.result);
}

function hasAutomaticVisibleSendEvidence(result: AgentDeliveryEvidence): boolean {
  if (result.deliveryStatus?.status === "sent" || result.deliveryStatus?.status === "suppressed") {
    return hasQueuedVisibleAgentPayload(result);
  }
  const payloads = Array.isArray(result.payloads) ? result.payloads : [];
  const outcomes = Array.isArray(result.deliveryStatus?.payloadOutcomes)
    ? result.deliveryStatus.payloadOutcomes
    : [];
  return outcomes.some((outcome) => {
    if (!outcome || typeof outcome !== "object" || Array.isArray(outcome)) {
      return false;
    }
    const record = outcome as Record<string, unknown>;
    if (
      record.status !== "sent" &&
      record.status !== "suppressed" &&
      record.sentBeforeError !== true
    ) {
      return false;
    }
    const index =
      typeof record.index === "number" && Number.isInteger(record.index) ? record.index : undefined;
    return index !== undefined && hasExplicitlyVisibleAgentPayload(payloads[index]);
  });
}

function hasQueuedVisibleReplyEvidence(params: {
  result: AgentDeliveryEvidence;
  route: SessionDeliveryRoute;
}): boolean {
  if (params.route.channel === INTERNAL_MESSAGE_CHANNEL) {
    return hasQueuedVisibleAgentPayload(params.result);
  }
  return hasAutomaticVisibleSendEvidence(params.result);
}

export async function evaluateQueuedGeneratedMediaAgentResult(params: {
  entry: QueuedAgentTurnSessionDelivery;
  result: AgentDeliveryEvidence;
  route: SessionDeliveryRoute;
  queueContext: OpenClawStateWorkerContext;
  persistInternalMedia?: (mediaUrls: string[]) => Promise<void>;
}) {
  if (hasUnexpectedRecoverySideEffects(params.result)) {
    log.warn("queued generated-media recovery reported an unexpected committed side effect", {
      queueId: params.entry.id,
    });
    await deadLetterSessionDelivery(
      params.entry,
      "queued generated-media delivery dead-lettered after an unexpected committed side effect",
      params.queueContext,
    );
  }
  const expectedMediaUrls = params.entry.expectedMediaUrls ?? [];
  const deliveredMediaUrls = new Set(
    collectQueuedDeliveredMediaUrls(params).map(normalizeMediaReferenceForComparison),
  );
  const isDelivered = (url: string) =>
    deliveredMediaUrls.has(normalizeMediaReferenceForComparison(url));
  const missingMediaUrls = expectedMediaUrls.filter((url) => !isDelivered(url));
  const provenExpectedMediaUrls = expectedMediaUrls.filter(isDelivered);
  const ambiguousMediaUrls = new Set(
    collectAmbiguousAutomaticMediaUrls(params.result).map(normalizeMediaReferenceForComparison),
  );
  const deliveryFailure = getAgentCommandDeliveryFailure(params.result);
  const replySatisfied =
    expectedMediaUrls.length > 0
      ? missingMediaUrls.length === 0
      : hasQueuedVisibleReplyEvidence(params);
  const evidenceTruncated = params.result.payloadsTruncated === true;
  if (evidenceTruncated && !replySatisfied) {
    log.warn("queued generated-media delivery has truncated delivery evidence", {
      queueId: params.entry.id,
    });
    await deadLetterSessionDelivery(
      params.entry,
      "queued generated-media delivery dead-lettered after truncated evidence",
      params.queueContext,
    );
  }
  if (expectedMediaUrls.length > 0 && missingMediaUrls.length === 0) {
    await params.persistInternalMedia?.(provenExpectedMediaUrls);
    return;
  }
  const rearmAgentRun = async (
    reason: string,
    updates?: {
      expectedMediaUrls?: string[];
      message?: string;
      suppressTextDelivery?: boolean;
    },
  ): Promise<never> => {
    const currentAgentRunAttempt = params.entry.agentRunAttempt ?? 0;
    const currentAttemptAlreadyCharged =
      params.entry.lastChargedAgentRunAttempt === currentAgentRunAttempt;
    // Charge the terminal attempt before advancing its identity. Recovery may
    // revisit the same durable evidence, but must never charge that attempt twice.
    if (!currentAttemptAlreadyCharged) {
      await failSessionDelivery(params.entry.id, reason, params.queueContext);
    }
    try {
      await advanceSessionDeliveryAgentRun(params.entry.id, updates, params.queueContext);
      await deferSessionDelivery(
        params.entry.id,
        AGENT_DELIVERY_OWNERSHIP_RETRY_MS,
        params.queueContext,
      );
    } catch (error) {
      log.warn("queued generated-media terminal attempt state transition remains pending", {
        queueId: params.entry.id,
        error: String(error),
      });
      throw new SessionDeliveryRetryChargedError(
        `${reason}; queue state transition failed after retry charge`,
      );
    }
    throw new SessionDeliveryDeferredError(reason);
  };
  if (deliveryFailure && expectedMediaUrls.length > 0) {
    const incompletePartialFailureEvidence =
      params.result.deliveryStatus?.status === "partial_failed" &&
      !hasCompleteAutomaticMediaDeliveryOutcomeEvidence(params.result, missingMediaUrls);
    if (
      incompletePartialFailureEvidence ||
      missingMediaUrls.some((url) =>
        ambiguousMediaUrls.has(normalizeMediaReferenceForComparison(url)),
      )
    ) {
      log.warn("queued generated-media delivery has ambiguous attachment side effects", {
        queueId: params.entry.id,
        error: deliveryFailure,
      });
      await deadLetterSessionDelivery(
        params.entry,
        "queued generated-media delivery dead-lettered after ambiguous side effects",
        params.queueContext,
      );
    }
  } else if (deliveryFailure) {
    if (hasQueuedVisibleReplyEvidence(params)) {
      log.warn("queued generated-media notice may already be visible; refusing duplicate replay", {
        queueId: params.entry.id,
        error: deliveryFailure,
      });
      await deadLetterSessionDelivery(
        params.entry,
        "queued generated-media notice dead-lettered after a visible partial delivery",
        params.queueContext,
      );
    }
    await rearmAgentRun(deliveryFailure);
  }
  if (provenExpectedMediaUrls.length > 0) {
    await params.persistInternalMedia?.(provenExpectedMediaUrls);
  }
  if (missingMediaUrls.length > 0) {
    const retryMessage = formatGeneratedMediaDeliveryRetryForPrompt(missingMediaUrls);
    const qualifier =
      missingMediaUrls.length < expectedMediaUrls.length ? "partially missed" : "missed";
    const reason = `queued generated-media agent turn ${qualifier} expected media: ${missingMediaUrls.join(", ")}`;
    await rearmAgentRun(reason, {
      expectedMediaUrls: missingMediaUrls,
      ...(missingMediaUrls.length < expectedMediaUrls.length ||
      hasQueuedVisibleReplyEvidence(params) ||
      params.result.deliveryStatus?.status === "partial_failed"
        ? { suppressTextDelivery: true }
        : {}),
      ...(retryMessage ? { message: retryMessage } : {}),
    });
  }
  if (expectedMediaUrls.length === 0 && !hasQueuedVisibleReplyEvidence(params)) {
    await rearmAgentRun("queued generated-media agent turn completed without a visible reply");
  }
}
