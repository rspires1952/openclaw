import { expect, it, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { isAgentRunDirectAbortReason } from "../../agents/run-termination.js";
import {
  getRpcSource,
  getRpcSourceProjectSessionActive,
  type RpcSourceRef,
} from "../../sessions/session-controller.rpc-sources.js";
import {
  getAgentTestMocks,
  operatorWriteCliClient,
  makeContext,
  requireValue,
  expectRecordFields,
  mockCallArg,
  invokeAgent,
  prime,
} from "./agent.test-harness.js";
import { handleChatAbortRequest } from "./chat-abort-handler.js";
import { handleDirectExternalChatSend } from "./chat-send-external-entry.js";

const mocks = getAgentTestMocks();

export function registerAgentAbortRunExecutionTest() {
  it.each([
    { action: "chat.abort", lateFailure: false },
    { action: "chat.abort", lateFailure: true },
    { action: "/stop", lateFailure: true },
  ] as const)("settles $action (late failure: $lateFailure)", async ({ action, lateFailure }) => {
    prime();
    const finishRun = createDeferred();
    const abortObserved = createDeferred();
    const context = makeContext();
    const runId = "idem-abort-run";
    let acceptedEntry: RpcSourceRef | undefined;
    let capturedSignal: AbortSignal | undefined;
    mocks.agentCommand.mockImplementationOnce(async (opts: { abortSignal?: AbortSignal }) => {
      capturedSignal = opts.abortSignal;
      capturedSignal?.addEventListener("abort", () => abortObserved.resolve(), { once: true });
      const entry = getRpcSource(runId);
      if (entry?.input.abortSignal === opts.abortSignal) {
        acceptedEntry = entry;
      }
      await finishRun.promise;
      if (lateFailure) {
        throw new Error("late provider failure");
      }
      return {
        payloads: [{ text: "late completion" }],
        meta: { durationMs: 1, terminalReply: { disposition: "visible", text: "late completion" } },
      };
    });

    let abort: Promise<void> | undefined;
    try {
      await invokeAgent(
        {
          message: "hi",
          agentId: "main",
          sessionKey: "agent:main:main",
          idempotencyKey: runId,
        },
        { context, reqId: runId },
      );

      const active = requireValue(acceptedEntry, "active run missing");
      expect(getRpcSource(runId)).toBe(active);
      expect(capturedSignal?.aborted).toBe(false);

      const abortRespond = vi.fn();
      const stopOptions = {
        respond: abortRespond,
        context,
        client: null,
        isWebchatConnect: () => false,
      };
      abort =
        action === "chat.abort"
          ? handleChatAbortRequest({
              ...stopOptions,
              params: { sessionKey: "agent:main:main", runId },
              req: { type: "req", id: "abort-req", method: "chat.abort" },
            })
          : handleDirectExternalChatSend({
              ...stopOptions,
              params: {
                sessionKey: "agent:main:main",
                message: "/stop",
                idempotencyKey: "idem-late-failure-stop",
              },
              req: { type: "req", id: "stop-req", method: "chat.send" },
            });

      await Promise.race([abortObserved.promise, abort]);
      expect(capturedSignal?.aborted).toBe(true);
      expect(isAgentRunDirectAbortReason(capturedSignal?.reason)).toBe(true);
      expect(active.adapter.abortStopReason).toBe(action === "chat.abort" ? "rpc" : "stop");
      expect(getRpcSourceProjectSessionActive(active)).toBe(false);
      expect(getRpcSource(runId)).toBe(active);
      expect(abortRespond).not.toHaveBeenCalled();

      // Stop retains the exact source until the cancelled producer actually exits.
      finishRun.resolve();
      await abort;
      expect(mockCallArg(abortRespond)).toBe(true);
      expectRecordFields(mockCallArg(abortRespond, 0, 1), {
        aborted: true,
        runIds: [runId],
      });
    } finally {
      finishRun.resolve();
      await Promise.allSettled([abort]);
      await acceptedEntry?.input.settlement.promise;
    }
    expect(getRpcSource(runId)).toBeUndefined();
    if (lateFailure) {
      const terminal = context.dedupe.get(`agent:${runId}`);
      expect(terminal?.ok).toBe(true);
      expect(terminal?.error).toBeUndefined();
      expectRecordFields(terminal?.payload, { runId, status: "timeout", summary: "aborted" });
    }
  });
}

export function registerAgentAbortStaleKeyExecutionTest() {
  it("chat.abort by runId allows the owner connection to use a stale session key", async () => {
    prime();
    const finishRun = createDeferred();
    const abortObserved = createDeferred();
    const context = makeContext();
    const runId = "idem-abort-stale-session-key";
    let acceptedEntry: RpcSourceRef | undefined;
    let capturedSignal: AbortSignal | undefined;
    mocks.agentCommand.mockImplementationOnce(async (opts: { abortSignal?: AbortSignal }) => {
      capturedSignal = opts.abortSignal;
      capturedSignal?.addEventListener("abort", () => abortObserved.resolve(), { once: true });
      const entry = getRpcSource(runId);
      if (entry?.input.abortSignal === opts.abortSignal) {
        acceptedEntry = entry;
      }
      await finishRun.promise;
      return {
        payloads: [{ text: "late completion" }],
        meta: { durationMs: 1, terminalReply: { disposition: "visible", text: "late completion" } },
      };
    });

    let abort: Promise<void> | undefined;
    try {
      await invokeAgent(
        {
          message: "hi",
          agentId: "main",
          sessionKey: "agent:main:main",
          idempotencyKey: runId,
        },
        {
          context,
          reqId: runId,
          client: { ...operatorWriteCliClient(), connId: "owner-conn" },
        },
      );

      const active = requireValue(acceptedEntry, "active run missing");
      expect(getRpcSource(runId)).toBe(active);
      expect(active.input.target?.sessionKey).toBe("agent:main:main");
      const abortRespond = vi.fn();
      abort = handleChatAbortRequest({
        params: { sessionKey: "agent:main:stale-key", runId },
        respond: abortRespond as never,
        context,
        req: { type: "req", id: "abort-req", method: "chat.abort" },
        client: { ...operatorWriteCliClient(), connId: "owner-conn" },
        isWebchatConnect: () => false,
      });

      await Promise.race([abortObserved.promise, abort]);
      expect(capturedSignal?.aborted).toBe(true);
      expect(getRpcSourceProjectSessionActive(active)).toBe(false);
      expect(getRpcSource(runId)).toBe(active);
      expect(abortRespond).not.toHaveBeenCalled();

      // Stop retains the exact source until the cancelled producer actually exits.
      finishRun.resolve();
      await abort;
      expect(mockCallArg(abortRespond)).toBe(true);
      expectRecordFields(mockCallArg(abortRespond, 0, 1), {
        aborted: true,
        runIds: [runId],
      });
    } finally {
      finishRun.resolve();
      await Promise.allSettled([abort]);
      await acceptedEntry?.input.settlement.promise;
    }
    expect(getRpcSource(runId)).toBeUndefined();
  });
}
