import { afterEach, describe, expect, it, vi } from "vitest";
import { captureSessionControllerSourceSettlement } from "../../../sessions/session-controller.mailbox.js";
import { createDeferredCore } from "../../../shared/deferred.js";
import { createQueueTestRun } from "../queue.test-helpers.js";
import { enqueueFollowupRun } from "./enqueue.js";
import {
  admitFollowupRunLifecycle,
  completeFollowupRunLifecycle,
  markFollowupRunEnqueued,
} from "./lifecycle.js";
import { clearFollowupQueue } from "./state.js";

afterEach(() => vi.useRealTimers());

describe("followup lifecycle heartbeat", () => {
  it("retains message-ID-deduped source custody until abandonment actually settles", async () => {
    const key = "agent:main:dedupe-abandonment";
    const entered = createDeferredCore();
    const release = createDeferredCore();
    const onSettled = vi.fn();
    const run = createQueueTestRun({ prompt: "queued", messageId: "source-1" });
    run.run.agentId = "main";
    run.run.sessionKey = key;
    run.turnAdoptionLifecycle = {
      onAdopted: () => {},
      onAbandoned: async () => {
        entered.resolve();
        await release.promise;
      },
      onSettled,
    };
    enqueueFollowupRun(key, run, { mode: "followup" }, "message-id");
    const input = run.controllerInput!;
    try {
      clearFollowupQueue(key, input.mailbox);
      await entered.promise;
      expect(onSettled).not.toHaveBeenCalled();
      release.resolve();
      await captureSessionControllerSourceSettlement(input);
      expect(onSettled).toHaveBeenCalledOnce();
    } finally {
      release.resolve();
      await captureSessionControllerSourceSettlement(input);
    }
  });
  it("joins already-started admission before settling source custody", async () => {
    const entered = createDeferredCore();
    const release = createDeferredCore();
    const lifecycle = {
      onAdopted: async () => {
        entered.resolve();
        await release.promise;
      },
      onAbandoned: vi.fn(),
      onSettled: vi.fn(),
    };
    const run = {
      ...createQueueTestRun({ prompt: "custody race" }),
      turnAdoptionLifecycle: lifecycle,
    };
    const admission = admitFollowupRunLifecycle(run);
    try {
      await entered.promise;
      completeFollowupRunLifecycle(run);
      expect(lifecycle.onSettled).not.toHaveBeenCalled();
      release.resolve();
      await admission;
      await captureSessionControllerSourceSettlement(run.controllerInput!);
      expect(lifecycle.onSettled).toHaveBeenCalledOnce();
      expect(lifecycle.onAbandoned).not.toHaveBeenCalled();
    } finally {
      release.resolve();
      await admission;
    }
  });

  it.each(["admitted", "completed", "aborted"] as const)(
    "does not start renewal for an already %s lifecycle",
    async (state) => {
      vi.useFakeTimers();
      const abort = new AbortController();
      const lifecycle = {
        admission: "exclusive" as const,
        abortSignal: abort.signal,
        onAdopted: vi.fn(),
        onDeferred: vi.fn(),
        onDeferredHeartbeat: vi.fn(),
        deferredHeartbeatIntervalMs: 100,
        onAbandoned: vi.fn(),
      };
      const run = {
        ...createQueueTestRun({ prompt: "heartbeat" }),
        turnAdoptionLifecycle: lifecycle,
      };
      if (state === "admitted") {
        await admitFollowupRunLifecycle(run);
      } else if (state === "completed") {
        completeFollowupRunLifecycle(run);
      } else {
        abort.abort();
      }
      try {
        markFollowupRunEnqueued(run);
        await vi.advanceTimersByTimeAsync(500);
        expect(lifecycle.onDeferredHeartbeat).not.toHaveBeenCalled();
        expect(vi.getTimerCount()).toBe(0);
      } finally {
        completeFollowupRunLifecycle(run);
      }
    },
  );
});
