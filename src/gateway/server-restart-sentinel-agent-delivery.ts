import path from "node:path";
import {
  getGatewayAgentResult,
  type AgentDeliveryEvidence,
} from "../agents/embedded-agent-runner/delivery-evidence.js";
import { buildGeneratedMediaDeliveryContext } from "../agents/internal-events.js";
import type { RuntimeContextFragment } from "../agents/internal-runtime-context.js";
import { reserveSubagentCompletionControllerSource } from "../agents/subagents/announce/subagent-announce-controller-source.js";
import { subagentRuns } from "../agents/subagents/registry/subagent-registry-memory.js";
import { resolveDurableCompletionDeliveryMode } from "../auto-reply/reply/completion-delivery-policy.js";
import { getRuntimeConfig } from "../config/config.js";
import { resolveSessionStorePathCore } from "../config/sessions/paths.js";
import {
  getRestartRecoveryTerminalDeliveryEvidence,
  hasRestartRecoveryTerminalRun,
} from "../config/sessions/restart-recovery-state.js";
import { withSessionEntryReadOnlyInWorker } from "../config/sessions/session-entry-read-runtime.js";
import { sessionMatchesExpectedTranscriptTurn } from "../config/sessions/session-transcript-turn-state.js";
import { SessionTranscriptWriterClaimReboundError } from "../config/sessions/transcript-write-context.js";
import { appendAssistantMessageToSessionTranscript } from "../config/sessions/transcript.js";
import type { SessionEntry } from "../config/sessions/types.js";
import {
  deferSessionDelivery,
  markSessionDeliveryAttemptStarted,
  mergeSessionDeliveryPreparedMediaBlocks,
} from "../infra/session-delivery-queue-storage.js";
import {
  SessionDeliveryDeadLetteredError,
  SessionDeliveryDeferredError,
  SessionDeliverySafeRetryError,
  type QueuedSessionDelivery,
} from "../infra/session-delivery-queue.records.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { getMediaDir } from "../media/store.js";
import { isSessionControllerWorkActive } from "../sessions/session-controller.lifecycle.js";
import {
  retireSessionControllerInput,
  type SessionControllerInput,
} from "../sessions/session-controller.mailbox.js";
import { readAssistantDisplayContent } from "../shared/assistant-display-content.js";
import type { OpenClawStateWorkerContext } from "../state/openclaw-state-worker-context.types.js";
import { INTERNAL_MESSAGE_CHANNEL } from "../utils/message-channel.js";
import {
  attachManagedOutgoingMediaToMessage,
  createManagedOutgoingMediaBlocks,
} from "./managed-image-attachments.js";
import type { GatewayContextResolver } from "./server-methods/types.js";
import { dispatchGatewayLifecycleMethod as dispatchGatewayMethodInProcess } from "./server-recovery-runtime-context.js";
import {
  AGENT_DELIVERY_OWNERSHIP_RETRY_MS,
  deadLetterSessionDelivery,
  evaluateQueuedGeneratedMediaAgentResult,
} from "./server-restart-sentinel-agent-delivery-evidence.js";
import { loadGatewaySessionEntryReadOnlyInWorker } from "./session-utils-store-worker.js";

const log = createSubsystemLogger("gateway/restart-sentinel");
type QueuedAgentTurnSessionDelivery = Extract<QueuedSessionDelivery, { kind: "agentTurn" }>;

function resolveQueuedAgentRunId(entry: QueuedAgentTurnSessionDelivery) {
  const base = entry.idempotencyKey ?? entry.messageId;
  return entry.agentRunAttempt ? `${base}:attempt:${entry.agentRunAttempt}` : base;
}

/** Runs durable generated-media handoffs through the normal owning-session agent loop. */
export async function deliverQueuedGeneratedMediaAgentTurn(params: {
  canonicalKey: string;
  agentId: string;
  storePath: string;
  entry: QueuedSessionDelivery;
  runtimeContextFragments?: RuntimeContextFragment[];
  sessionEntry?: SessionEntry;
  queueContext: OpenClawStateWorkerContext;
  resolveGatewayContext?: GatewayContextResolver;
}): Promise<boolean> {
  if (params.entry.kind !== "agentTurn") {
    return false;
  }
  const entry = params.entry;
  const binding = entry.requesterBinding;
  const route = entry.route;
  if (!route || entry.inputProvenance?.kind !== "inter_session" || !entry.sourceReplyDeliveryMode) {
    if (binding) {
      return deadLetterSessionDelivery(
        entry,
        "bound media completion has no delivery route",
        params.queueContext,
      );
    }
    return false;
  }

  params.queueContext.admission.assertCurrent();
  const assertRequesterAdmissionCurrent = () => {
    params.queueContext.admission.assertCurrent();
    if (!binding) {
      return;
    }
    const context = params.resolveGatewayContext?.();
    if (params.resolveGatewayContext && !context) {
      throw new SessionDeliveryDeferredError("media completion Gateway owner is unavailable");
    }
    const cfg = context?.getRuntimeConfig() ?? getRuntimeConfig();
    const configuredStorePath = resolveSessionStorePathCore(cfg.session?.store, {
      agentId: binding.agentId,
      env: params.queueContext.environment,
    });
    if (
      entry.sessionKey !== binding.sessionKey ||
      params.canonicalKey !== binding.sessionKey ||
      params.agentId !== binding.agentId ||
      path.resolve(params.storePath) !== path.resolve(binding.storePath) ||
      path.resolve(configuredStorePath) !== path.resolve(binding.storePath)
    ) {
      throw new SessionDeliveryDeadLetteredError("media completion requester store changed");
    }
  };
  const readRequester = async (): Promise<SessionEntry | undefined> => {
    if (!binding) {
      return (
        await loadGatewaySessionEntryReadOnlyInWorker({
          cfg: params.resolveGatewayContext?.()?.getRuntimeConfig() ?? getRuntimeConfig(),
          key: entry.sessionKey,
          env: params.queueContext.environment,
          assertActive: params.queueContext.admission.assertCurrent,
          excludeInternalEffects: true,
        })
      ).entry;
    }
    try {
      return await withSessionEntryReadOnlyInWorker(
        { ...binding, env: params.queueContext.environment, hydrateSkillPromptRefs: false },
        assertRequesterAdmissionCurrent,
        async (read) => {
          if (!read.ok) {
            throw read.error;
          }
          if (
            !sessionMatchesExpectedTranscriptTurn(read.value ? { entry: read.value } : undefined, {
              expectedSessionId: binding.sessionId,
              expectedLifecycleRevision: binding.lifecycleRevision,
            })
          ) {
            throw new SessionDeliveryDeadLetteredError(
              "media completion requester session changed",
            );
          }
          return read.value;
        },
      );
    } catch (error) {
      if (error instanceof SessionDeliveryDeadLetteredError) {
        return deadLetterSessionDelivery(entry, error.message, params.queueContext);
      }
      throw error;
    }
  };
  const sessionEntry = binding ? await readRequester() : params.sessionEntry;
  const queuedRunId = resolveQueuedAgentRunId(entry);
  const deliveryMode = resolveDurableCompletionDeliveryMode(entry.sourceReplyDeliveryMode);
  if (deliveryMode === "host_owned" && route.channel === INTERNAL_MESSAGE_CHANNEL) {
    return await deadLetterSessionDelivery(
      entry,
      "queued host-owned generated-media delivery requires an external route",
      params.queueContext,
    );
  }
  const persistInternalMedia =
    route.channel === INTERNAL_MESSAGE_CHANNEL && (entry.expectedMediaUrls?.length ?? 0) > 0
      ? async (mediaUrls: string[], transcriptRunId: string | null) => {
          const sessionId = sessionEntry?.sessionId?.trim();
          if (!sessionId) {
            throw new Error("queued internal generated-media delivery has no owning session");
          }
          const stateDir = params.queueContext.environment.OPENCLAW_STATE_DIR;
          const attachMedia = async (
            messageId: string,
            blocks: Parameters<typeof attachManagedOutgoingMediaToMessage>[0]["blocks"],
          ) => {
            if (!(await attachManagedOutgoingMediaToMessage({ messageId, blocks, stateDir }))) {
              throw new Error("queued internal generated-media artifact attachment failed");
            }
          };
          const preparedMediaBlocks = { ...entry.preparedMediaBlocks };
          const content: Array<Record<string, unknown>> = [];
          for (const mediaUrl of mediaUrls) {
            let blocks = preparedMediaBlocks[mediaUrl];
            if (!blocks) {
              const attachment = entry.expectedMediaAttachments?.[mediaUrl];
              blocks = await createManagedOutgoingMediaBlocks({
                sessionKey: params.canonicalKey,
                agentId: params.agentId,
                items: [
                  {
                    url: mediaUrl,
                    ...(attachment?.name ? { filename: attachment.name } : {}),
                    ...(attachment?.mimeType ? { mimeType: attachment.mimeType } : {}),
                    trustedLocal: true,
                    ...(attachment?.durationMs !== undefined
                      ? { durationMs: attachment.durationMs }
                      : {}),
                    ...(attachment?.width !== undefined ? { width: attachment.width } : {}),
                    ...(attachment?.height !== undefined ? { height: attachment.height } : {}),
                  },
                ],
                stateDir,
                localRoots: [getMediaDir()],
              });
              if (
                !blocks.some(
                  (block) =>
                    block.type === "image" ||
                    block.type === "audio" ||
                    block.type === "video" ||
                    block.type === "attachment",
                )
              ) {
                throw new Error("queued internal generated media could not be prepared");
              }
              blocks = await mergeSessionDeliveryPreparedMediaBlocks(
                entry.id,
                mediaUrl,
                blocks,
                params.queueContext,
              );
              params.queueContext.admission.assertCurrent();
              preparedMediaBlocks[mediaUrl] = blocks;
            }
            content.push(...blocks);
          }
          const scope = {
            agentId: params.agentId,
            sessionKey: params.canonicalKey,
            sessionId,
            storePath: params.storePath,
          };
          const expectedLifecycleRevision = binding
            ? binding.lifecycleRevision
            : (sessionEntry?.cronRunContinuation?.lifecycleRevision ??
              sessionEntry?.lifecycleRevision ??
              null);
          const { enrichAssistantTranscriptMediaForRun, publishAssistantTranscriptRewrite } =
            await import("./server-methods/chat-transcript-persistence.js");
          params.queueContext.admission.assertCurrent();
          let enriched: { messageId: string } | null = null;
          if (transcriptRunId) {
            try {
              enriched = await enrichAssistantTranscriptMediaForRun({
                scope,
                runId: transcriptRunId,
                expectedLifecycleRevision,
                content,
                mediaUrls,
              });
            } catch (error) {
              if (error instanceof SessionTranscriptWriterClaimReboundError) {
                await deadLetterSessionDelivery(
                  entry,
                  "queued internal generated-media delivery lost its owning session",
                  params.queueContext,
                );
              }
              throw error;
            }
          }
          // Keep the existing media-only delivery contract when no model reply
          // was persisted, or an older recovery receipt lacks its run binding.
          params.queueContext.admission.assertCurrent();
          const appended = enriched
            ? { ok: true as const, messageId: enriched.messageId }
            : await appendAssistantMessageToSessionTranscript({
                agentId: params.agentId,
                sessionKey: params.canonicalKey,
                storePath: params.storePath,
                expectedSessionId: sessionId,
                expectedLifecycleRevision,
                content: [],
                displayContent: content,
                idempotencyKey: `${queuedRunId}:generated-media-transcript`,
                updateMode: "inline",
                onMessageCommitted: (receipt, acceptCompletion) => {
                  acceptCompletion(() =>
                    attachMedia(receipt.messageId, readAssistantDisplayContent(receipt.message)),
                  );
                },
              });
          if (!appended.ok) {
            if (appended.code === "session-rebound") {
              await deadLetterSessionDelivery(
                entry,
                "queued internal generated-media delivery lost its owning session",
                params.queueContext,
              );
            }
            throw new Error(
              `queued internal generated-media transcript persistence failed: ${appended.reason}`,
            );
          }
          params.queueContext.admission.assertCurrent();
          if (enriched) {
            await attachMedia(enriched.messageId, content);
            await publishAssistantTranscriptRewrite({ scope, rewritten: [enriched] });
          }
        }
      : undefined;
  const evaluateResult = async (
    result: AgentDeliveryEvidence,
    transcriptRunId: string | null = queuedRunId,
  ): Promise<true> => {
    await evaluateQueuedGeneratedMediaAgentResult({
      entry,
      result,
      route,
      queueContext: params.queueContext,
      ...(persistInternalMedia
        ? { persistInternalMedia: (mediaUrls) => persistInternalMedia(mediaUrls, transcriptRunId) }
        : {}),
    });
    return true;
  };
  const terminalEvidence = getRestartRecoveryTerminalDeliveryEvidence(sessionEntry, queuedRunId);
  if (terminalEvidence) {
    return await evaluateResult(terminalEvidence, terminalEvidence.transcriptRunId ?? null);
  }
  if (hasRestartRecoveryTerminalRun(sessionEntry, queuedRunId)) {
    await deadLetterSessionDelivery(
      entry,
      "queued generated-media agent turn dead-lettered without durable terminal evidence",
      params.queueContext,
    );
  }
  const activeRecoveryClaim =
    sessionEntry?.restartRecoveryDeliverySourceRunId === queuedRunId &&
    Boolean(sessionEntry.restartRecoveryDeliveryRunId);
  if (activeRecoveryClaim) {
    await deferSessionDelivery(entry.id, AGENT_DELIVERY_OWNERSHIP_RETRY_MS, params.queueContext);
    throw new SessionDeliveryDeferredError(
      "queued generated-media agent turn is still owned by agent recovery",
    );
  }
  if (entry.deliveryStartedAt !== undefined) {
    await deadLetterSessionDelivery(
      entry,
      "queued generated-media agent turn dead-lettered after an interrupted unproven attempt",
      params.queueContext,
    );
  }
  const assertRequesterIdle = () => {
    if (
      isSessionControllerWorkActive(
        params.storePath,
        [params.canonicalKey, sessionEntry?.sessionId],
        params.agentId,
      )
    ) {
      throw new SessionDeliveryDeferredError(
        "queued generated-media turn is waiting for its requester to finish",
      );
    }
  };
  try {
    // The originating turn can still own terminal persistence after its model has stopped.
    assertRequesterIdle();
  } catch (error) {
    await deferSessionDelivery(entry.id, AGENT_DELIVERY_OWNERSHIP_RETRY_MS, params.queueContext);
    throw error;
  }
  // `host_owned` is the explicit-send equivalent of message-tool-only policy.
  // The queue owner fixes route/media and disables the model-facing message tool,
  // so only this one system completion can use the normal final-delivery transport.
  const sourceReplyDeliveryMode = "automatic" as const;
  const cronLifecycleRevision = sessionEntry?.cronRunContinuation?.lifecycleRevision?.trim();
  const cronSessionId = cronLifecycleRevision ? sessionEntry?.sessionId?.trim() : undefined;
  // Fence before gateway admission. Recovery clears it only for an explicit
  // pre-acceptance safe retry; accepted or deduped runs may already have effects.
  await markSessionDeliveryAttemptStarted(entry, {
    ...params.queueContext,
    admission: {
      ...params.queueContext.admission,
      assertCurrent: () => {
        params.queueContext.admission.assertCurrent();
        assertRequesterIdle();
      },
    },
  });
  const completionOwner =
    entry.owner?.kind === "subagent_completion" ? subagentRuns.get(entry.owner.runId) : undefined;
  let controllerInput: SessionControllerInput | undefined;
  let accepted = false;
  let response: unknown;
  try {
    assertRequesterAdmissionCurrent();
    controllerInput = completionOwner
      ? await reserveSubagentCompletionControllerSource(completionOwner, queuedRunId)
      : undefined;
    assertRequesterAdmissionCurrent();
    response = await dispatchGatewayMethodInProcess(
      "agent",
      {
        sessionKey: params.canonicalKey,
        ...(binding
          ? {
              agentId: binding.agentId,
              expectedExistingSessionId: binding.sessionId,
              expectedExistingSessionLifecycleRevision: binding.lifecycleRevision,
            }
          : {}),
        message: entry.message,
        deliver: route.channel !== INTERNAL_MESSAGE_CHANNEL,
        bestEffortDeliver: false,
        channel: route.channel,
        accountId: route.accountId,
        to: route.to,
        threadId: route.threadId,
        ...(cronSessionId ? { sessionId: cronSessionId } : {}),
        inputProvenance: entry.inputProvenance,
        sourceReplyDeliveryMode,
        disableMessageTool: true,
        forceRestartSafeTools: true,
        idempotencyKey: queuedRunId,
      },
      {
        controllerInput,
        ...(cronSessionId ? { allowSyntheticCronRunContinuation: true } : {}),
        expectFinal: true,
        ...(binding ? { assertAdmissionCurrent: assertRequesterAdmissionCurrent } : {}),
        forceSyntheticClient: true,
        runtimeContextFragments:
          (entry.expectedMediaUrls?.length ?? 0) > 0
            ? [
                ...((entry.agentRunAttempt ?? 0) > 0 ? [] : (params.runtimeContextFragments ?? [])),
                ...buildGeneratedMediaDeliveryContext(
                  entry.expectedMediaUrls ?? [],
                  (entry.agentRunAttempt ?? 0) > 0,
                ),
              ]
            : params.runtimeContextFragments,
        internalDeliveryMediaUrls: entry.expectedMediaUrls ?? [],
        ...(entry.suppressTextDelivery === true ? { internalDeliverySuppressText: true } : {}),
        ...(params.resolveGatewayContext
          ? { resolveGatewayContext: params.resolveGatewayContext }
          : {}),
        onAccepted: () => {
          accepted = true;
        },
      },
    );
  } catch (error) {
    if (error instanceof SessionDeliveryDeadLetteredError) {
      return deadLetterSessionDelivery(entry, error.message, params.queueContext);
    }
    if (!accepted) {
      throw new SessionDeliverySafeRetryError(
        "queued generated-media agent turn failed before gateway acceptance",
        { cause: error },
      );
    }
    throw error;
  } finally {
    if (controllerInput && !controllerInput.custody.rpcAdopted) {
      retireSessionControllerInput(controllerInput);
    }
  }
  const result = getGatewayAgentResult(response);
  if (!result) {
    const responseStatus =
      response && typeof response === "object"
        ? (response as { status?: unknown }).status
        : undefined;
    const latestEntry = await readRequester();
    if (responseStatus === "accepted") {
      accepted = true;
    }
    if (
      responseStatus === "accepted" ||
      responseStatus === "in_flight" ||
      (latestEntry?.restartRecoveryDeliverySourceRunId === queuedRunId &&
        latestEntry.restartRecoveryDeliveryRunId)
    ) {
      await deferSessionDelivery(entry.id, AGENT_DELIVERY_OWNERSHIP_RETRY_MS, params.queueContext);
      throw new SessionDeliveryDeferredError(
        "queued generated-media agent turn is still owned by agent recovery",
      );
    }
    if (hasRestartRecoveryTerminalRun(latestEntry, queuedRunId)) {
      const latestTerminalEvidence = getRestartRecoveryTerminalDeliveryEvidence(
        latestEntry,
        queuedRunId,
      );
      if (latestTerminalEvidence) {
        return await evaluateResult(
          latestTerminalEvidence,
          latestTerminalEvidence.transcriptRunId ?? null,
        );
      }
      log.warn(
        "queued generated-media agent turn ended without durable delivery evidence; failing closed",
        { queueId: entry.id, runId: queuedRunId },
      );
      await deadLetterSessionDelivery(
        entry,
        "queued generated-media agent turn dead-lettered without durable terminal evidence",
        params.queueContext,
      );
    }
    if (!accepted) {
      throw new SessionDeliverySafeRetryError(
        "queued generated-media agent turn returned no result before gateway acceptance",
      );
    }
    throw new Error("queued generated-media agent turn returned no delivery result");
  }
  return await evaluateResult(result);
}
