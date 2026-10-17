import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resolveSessionAuthSelection } from "../../agents/auth-profiles/session-override.js";
import { getActiveNativeAttempt } from "../../agents/embedded-agent-runner/run-state.js";
import {
  clearActiveEmbeddedRun,
  setActiveEmbeddedRun,
} from "../../agents/embedded-agent-runner/runs.js";
import type { SessionEntry } from "../../config/sessions.js";
import { createReplyOperation } from "../../sessions/session-controller.js";
import {
  reserveSessionControllerSource,
  retireSessionControllerInput,
} from "../../sessions/session-controller.mailbox.js";
import { captureSessionTarget } from "../../sessions/session-controller.target.js";
import { parseInlineSessionDirectives } from "./directive-handling.parse.js";
import { prepareReplyRunAdmission } from "./get-reply-run-admission.js";
import type { PreparedReplyRunContext } from "./get-reply-run-context.js";
import { createModelSelectionStateFixture } from "./model-selection.test-support.js";
import { buildReplyPromptEnvelope } from "./prompt-prelude.js";
import { createQueueTestRun } from "./queue.test-helpers.js";
import { enqueueFollowupRun } from "./queue/enqueue.js";
import { clearFollowupQueue, getExistingFollowupQueue } from "./queue/state.js";
import { bindReplySourceInput } from "./reply-source-binding.js";
import { resolveFollowupRunToolAuthorityFingerprintAsync } from "./reply-tool-authority.test-support.js";
import { createTypingController } from "./typing.js";

vi.mock("../../agents/auth-profiles/session-override.js", () => ({
  resolveSessionAuthSelection: vi.fn(async () => undefined),
}));
vi.mock("./session-system-events.js", () => ({
  drainFormattedSystemEvents: vi.fn(async () => undefined),
}));
vi.mock("./queue/drain.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./queue/drain.js")>()),
  scheduleFollowupDrain: () => {
    throw new Error("Admission fixture must not start a drain");
  },
  kickFollowupDrainIfIdle: () => {
    throw new Error("Admission fixture must not start a drain");
  },
}));
vi.mock("./get-reply-run-helpers.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./get-reply-run-helpers.js")>()),
  loadAgentRunnerRuntime: async () => ({ runReplyAgent: vi.fn() }),
  loadEmbeddedAgentRuntime: async () => ({
    preemptAndDrainEmbeddedHeartbeatRun: async () => "not-heartbeat",
    resolveActiveEmbeddedRunSessionIdBySessionFile: () => undefined,
  }),
  loadSessionUpdatesRuntime: async () => ({
    ensureSkillSnapshot: async ({ sessionEntry }: { sessionEntry: SessionEntry }) => ({
      sessionEntry,
    }),
  }),
}));

function createAdmissionFixture() {
  const sessionKey = "agent:main:slack:channel:room:thread:100.1";
  const sessionId = "session";
  const storePath = "/tmp/agent/sessions/sessions.json";
  const entry: SessionEntry = { sessionId, updatedAt: 1 };
  const ctx = { SessionKey: sessionKey, Provider: "slack", ChatType: "channel" };
  const body = "Use the revised request";
  const context: PreparedReplyRunContext = {
    kind: "ready",
    params: {
      ctx,
      sessionCtx: ctx,
      conversation: { fields: ctx, group: {} },
      cfg: {},
      agentId: "main",
      agentDir: "/tmp/agent",
      agentCfg: {},
      sessionCfg: {},
      commandAuthorized: true,
      command: {
        surface: "slack",
        channel: "slack",
        ownerList: [],
        senderIsOwner: false,
        isAuthorizedSender: true,
        rawBodyNormalized: body,
        commandBodyNormalized: body,
      },
      allowTextCommands: true,
      directives: parseInlineSessionDirectives(body),
      defaultActivation: "always",
      modelState: createModelSelectionStateFixture({
        agentCfg: {},
        provider: "anthropic",
        model: "claude",
      }),
      provider: "anthropic",
      model: "claude",
      typing: createTypingController({}),
      defaultModel: "claude",
      timeoutMs: 30_000,
      isNewSession: false,
      resetTriggered: false,
      systemSent: true,
      sessionKey,
      sessionId,
      storePath,
      sessionStore: { [sessionKey]: entry },
      workspaceDir: "/tmp/workspace",
      abortedLastRun: false,
      resolvedThinkLevel: "off",
      resolvedVerboseLevel: "off",
      resolvedReasoningLevel: "off",
      resolvedElevatedLevel: "off",
      elevatedEnabled: false,
      elevatedAllowed: false,
      blockStreamingEnabled: false,
      resolvedBlockStreamingBreak: "message_end",
    },
    runtimePolicySessionKey: sessionKey,
    isHeartbeat: false,
    explicitThinkingLevelOverride: undefined,
    effectiveQueueMode: undefined,
    promptSessionCtx: ctx,
    inboundEventKind: undefined,
    sourceReplyDeliveryMode: undefined,
    silentReplyPromptMode: "none",
    fullAccessState: { available: false },
    isFirstTurnInSession: false,
    extraSystemPromptParts: [],
    sourceConversationContextByMode: { automatic: "", message_tool_only: "" },
    sourceConversationContextPromptOffset: undefined,
    extraSystemPromptStatic: "",
    cliSessionBindingFacts: { extraSystemPromptStatic: "" },
    baseBodyTrimmedRaw: body,
    effectiveResetTriggered: false,
    isBareSessionReset: false,
    shouldInjectGroupIntro: false,
    typingMode: "never",
    isMainSession: false,
    terminalReplyExpectation: "optional",
    sessionEntry: entry,
    traceRunPhase: async <T>(_name: string, run: () => T | Promise<T>) => await run(),
    prefixedBodyBase: body,
    hasUserBody: true,
    workspaceDir: "/tmp/workspace",
    skillsWorkspaceDir: "/tmp/workspace",
    useFastReplyRuntime: false,
    thinkingRuntime: "embedded",
    buildPromptBodies: (additions) =>
      buildReplyPromptEnvelope({
        ctx,
        sessionCtx: ctx,
        baseBody: body,
        hasUserBody: true,
        inboundUserContext: "",
        isBareSessionReset: false,
        startupAction: "new",
        ...additions,
      }),
    refreshInboundContextAfterAdmissionWait: () => {},
  };
  return { context, entry, sessionKey, sessionId, storePath };
}

afterEach(() => vi.clearAllMocks());

describe("prepared reply transcript identity", () => {
  it("carries an inherited thinking request through admission to candidate selection", async () => {
    const { context, entry } = createAdmissionFixture();
    entry.thinkingLevel = "high";
    const catalog = [{ provider: "fixture", id: "basic", name: "Basic", reasoning: false }];
    const result = await prepareReplyRunAdmission({
      ...context,
      thinkingRuntime: "openclaw",
      params: {
        ...context.params,
        provider: "fixture",
        model: "basic",
        resolvedThinkLevel: "high",
        modelState: {
          ...context.params.modelState,
          allowedModelCatalog: catalog,
          resolveThinkingCatalog: async () => catalog,
        },
      },
    });

    expect(result).toMatchObject({ kind: "ready", resolvedThinkLevel: "high" });
    expect(entry.thinkingLevel).toBe("high");
  });

  it.each(["steer", "followup"] as const)(
    "preserves normal %s policy when the native heartbeat declines preemption",
    async (mode) => {
      const { context, sessionKey, sessionId, storePath } = createAdmissionFixture();
      const target = captureSessionTarget({
        storeScope: storePath,
        sessionKey,
        incarnation: sessionId,
        agentId: "main",
      });
      const operation = createReplyOperation({
        sessionKey,
        sessionId,
        target,
        turnKind: "heartbeat",
        resetTriggered: false,
      });
      const preemptByVisibleTurn = vi.fn(() => false);
      const abort = vi.fn();
      const native = {
        runId: `declined-heartbeat-${mode}`,
        queueMessage: vi.fn(async () => {}),
        isStreaming: () => true,
        isCompacting: () => false,
        preemptByVisibleTurn,
        abort,
      };
      setActiveEmbeddedRun(sessionId, native, sessionKey, undefined, "main", operation);
      const input = reserveSessionControllerSource(sessionKey, { target, policy: { mode } });
      try {
        const prepared = await prepareReplyRunAdmission({
          ...context,
          effectiveQueueMode: mode,
          params: {
            ...context.params,
            opts: bindReplySourceInput({}, input),
          },
        });
        expect(preemptByVisibleTurn).toHaveBeenCalledOnce();
        expect(prepared).toMatchObject({
          kind: "ready",
          isActive: true,
          shouldSteer: mode === "steer",
          shouldFollowup: true,
          resolvedQueue: { mode },
        });
        expect(getActiveNativeAttempt(sessionId)).toBe(native);
        expect(operation.result).toBeNull();
        expect(operation.abortSignal.aborted).toBe(false);
        expect(input.abortSignal.aborted).toBe(false);
        expect(abort).not.toHaveBeenCalled();
        expect(native.queueMessage).not.toHaveBeenCalled();
      } finally {
        retireSessionControllerInput(input);
        clearActiveEmbeddedRun(sessionId, native, sessionKey);
        operation.complete();
      }
    },
  );

  it.each(["steer", "followup"] as const)(
    "preserves %s admission and transcript authority despite an older queued followup",
    async (mode) => {
      const { context, sessionKey, sessionId } = createAdmissionFixture();
      const older = createQueueTestRun({ prompt: "Earlier followup", messageId: `older-${mode}` });
      const operation = createReplyOperation({ sessionKey, sessionId, resetTriggered: false });
      operation.setPhase("running");
      enqueueFollowupRun(sessionKey, older, { mode: "followup" }, "message-id", undefined, false);
      try {
        const prepared = await prepareReplyRunAdmission({ ...context, effectiveQueueMode: mode });
        expect(prepared).toMatchObject({
          kind: "ready",
          isActive: true,
          shouldSteer: mode === "steer",
          shouldFollowup: true,
        });
        expect(getExistingFollowupQueue(sessionKey)?.items).toEqual([older]);
        if (prepared.kind !== "ready") {
          throw new Error("Expected a prepared reply");
        }
        const incoming = createQueueTestRun({ prompt: "Use the revised request" });
        incoming.run = {
          ...incoming.run,
          agentId: "main",
          sessionKey,
          sessionId,
          sessionFile: prepared.preparedSessionState.sessionFile,
        };
        const queued = {
          ...incoming,
          run: {
            ...incoming.run,
            sessionFile:
              normalizeOptionalString(incoming.run.sessionKey) ?? incoming.run.sessionFile,
          },
        };
        expect(await resolveFollowupRunToolAuthorityFingerprintAsync(incoming)).toBe(
          await resolveFollowupRunToolAuthorityFingerprintAsync(queued),
        );
        expect(prepared.preparedSessionState.sessionFile).toBe(sessionKey);
      } finally {
        operation.complete();
        clearFollowupQueue(sessionKey);
      }
    },
  );

  it.each([false, true])(
    "keeps caller-only model auth selection off the shared session (fast=%s)",
    async (fast) => {
      const { context, entry } = createAdmissionFixture();
      entry.authProfileOverride = "fixture:shared";
      entry.authProfileOverrideSource = "user";
      vi.mocked(resolveSessionAuthSelection).mockImplementationOnce(async (params) => {
        expect(params.storePath).toBeUndefined();
        expect(params.sessionEntry).not.toBe(entry);
        if (!params.sessionEntry) {
          throw new Error("Expected an auth selection snapshot");
        }
        params.sessionEntry.authProfileOverride = "fixture:turn";
        return { profileId: "fixture:turn", source: "auto", routeRequirement: undefined };
      });

      const result = await prepareReplyRunAdmission({
        ...context,
        useFastReplyRuntime: fast,
        params: {
          ...context.params,
          provider: "fixture",
          model: "allowed",
          modelState: { ...context.params.modelState, operatorModelOverride: true },
        },
      });
      expect(result).toMatchObject({ kind: "ready", authProfileId: "fixture:turn" });
      expect(resolveSessionAuthSelection).toHaveBeenCalledOnce();
      expect(entry.authProfileOverride).toBe("fixture:shared");
    },
  );
});
