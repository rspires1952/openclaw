import { MAX_TIMER_TIMEOUT_MS } from "@openclaw/normalization-core/number-coercion";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../../test/helpers/promise.js";
import {
  clearRuntimeConfigSnapshot,
  setRuntimeConfigSnapshot,
} from "../../../config/runtime-snapshot.js";
import {
  getAgentEventLifecycleGeneration,
  resetAgentEventsForTest,
  rotateAgentEventLifecycleGeneration,
} from "../../../infra/agent-events.js";
import { getCommandLaneSnapshot } from "../../../process/command-queue.js";
import { resetCommandQueueStateForTest } from "../../../process/command-queue.test-support.js";
import { testing } from "../../../sessions/session-controller.test-support.js";
import { prepareSystemAgentRunAdmission } from "../../admitted-run-context.js";
import { createEmbeddedRunLaneController } from "./lane-controller.js";
import { EMBEDDED_RUN_LANE_TIMEOUT_GRACE_MS } from "./lane-runtime.js";
import type { RunEmbeddedAgentParams } from "./params.js";

// mock-isolation: Attempt-control unit cases retain raw work without a persisted transcript writer.
vi.mock("./session-bootstrap.js", () => ({
  claimAgentSessionWriter: vi.fn(async () => undefined),
}));

const globalLane = "test:attempt-owner-deadline";
const cleanups: Array<() => void | Promise<void>> = [];
async function setup() {
  const runId = "attempt-owner";
  const admission = prepareSystemAgentRunAdmission({}, runId, "main", "watchdog-controls");
  cleanups.push(admission.close);
  const admittedRunContext = await admission.admit("embedded");
  let generation = getAgentEventLifecycleGeneration();
  let params: RunEmbeddedAgentParams & { sessionFile: string } = {
    admittedRunContext,
    runId,
    sessionId: "attempt-owner-incarnation",
    sessionKey: "agent:main:attempt-owner",
    sessionFile: "attempt-owner.jsonl",
    prompt: "test",
    workspaceDir: "/tmp/attempt-owner",
    timeoutMs: 1_000,
  };
  setRuntimeConfigSnapshot(params.config ?? {});
  const controller = createEmbeddedRunLaneController({
    getLifecycleGeneration: () => generation,
    getParams: () => params,
    globalLane,
    initialQueuedLifecycleGeneration: generation,
    setLifecycleGeneration: (value) => {
      generation = value;
    },
    setParams: (value) => {
      params = value;
    },
  });
  const entered = createDeferred();
  const raw = createDeferred();
  const run = controller.enqueueSession(() =>
    controller.enqueueGlobal(async () => {
      entered.resolve();
      await raw.promise;
      return { meta: { durationMs: 1 } };
    }),
  );
  cleanups.push(async () => {
    raw.resolve();
    await run.catch(() => {});
  });
  await entered.promise;
  const operation = params.replyOperation!;
  const controls = (
    options: Omit<
      Parameters<typeof controller.createAttemptControls>[0],
      "admittedRunContext"
    > = {},
  ) => {
    const value = controller.createAttemptControls({ ...options, admittedRunContext });
    cleanups.push(value.close);
    return value;
  };
  return { controller, operation, controls, admission, runId, raw, run };
}
beforeEach(() => {
  resetAgentEventsForTest();
  vi.useFakeTimers();
  vi.setSystemTime(1_000);
});
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).toReversed()) {
    await cleanup();
  }
  testing.resetSessionControllerForTest();
  resetCommandQueueStateForTest();
  resetAgentEventsForTest();
  vi.useRealTimers();
  vi.restoreAllMocks();
  clearRuntimeConfigSnapshot();
});

describe("exact attempt deadline transfer", () => {
  it.each(["seeded", "published"] as const)(
    "transfers %s bounded deadlines with cleanup grace into the owner",
    async (mode) => {
      const { operation, controls, controller, raw, run } = await setup();
      const control = controls(mode === "seeded" ? { initialTimeoutMs: 120_000 } : {});
      if (mode === "published") {
        control.onAttemptDeadlineChanged({ kind: "bounded", deadlineAtMs: 121_000 });
      }
      expect(operation.watchdog.snapshot().executionDeadlineAtMs).toBe(
        121_000 + EMBEDDED_RUN_LANE_TIMEOUT_GRACE_MS,
      );
      vi.setSystemTime(150_999);
      await operation.watchdog.tick();
      expect(controller.abortSignal.aborted).toBe(false);
      vi.setSystemTime(151_000);
      await operation.watchdog.tick();
      expect(controller.abortSignal.aborted).toBe(true);
      expect(getCommandLaneSnapshot(globalLane).activeCount).toBe(1);
      expect(operation.result).toMatchObject({ code: "run_stalled" });
      raw.resolve();
      await Promise.allSettled([run]);
      await operation.ownerSettlement;
      expect(getCommandLaneSnapshot(globalLane).activeCount).toBe(0);
    },
  );

  it("keeps unlimited execution separate from semantic liveness and restores the fallback only once", async () => {
    const { operation, controls, controller } = await setup();
    const control = controls({ initialTimeoutMs: MAX_TIMER_TIMEOUT_MS });
    expect(operation.watchdog.snapshot().executionDeadlineAtMs).toBeUndefined();
    vi.setSystemTime(60_000);
    await operation.watchdog.tick();
    expect(controller.abortSignal.aborted).toBe(false);
    control.close();
    expect(operation.watchdog.snapshot().executionDeadlineAtMs).toBe(91_000);
    vi.setSystemTime(70_000);
    control.close();
    expect(operation.watchdog.snapshot().executionDeadlineAtMs).toBe(91_000);
  });

  it("does not clear an enforced maintenance deadline that this attempt never published", async () => {
    const { operation, controls, controller } = await setup();
    controller.setLaneTaskDeadline({ kind: "bounded", deadlineAtMs: 100_000 });
    controls().close();
    expect(operation.watchdog.snapshot().executionDeadlineAtMs).toBe(130_000);
  });

  it.each([
    "closed",
    "superseded",
    "revoked",
    "replaced admission",
    "rotated lifecycle",
    "aborted input",
  ] as const)("rejects callbacks retained after %s", async (invalidation) => {
    const { operation, controls, admission, runId, controller } = await setup();
    const abort = new AbortController();
    const onAbort = vi.fn();
    const old = controls({ abortSignal: abort.signal, initialTimeoutMs: 120_000, onAbort });
    switch (invalidation) {
      case "closed":
        old.close();
        controls({ initialTimeoutMs: 120_000 });
        break;
      case "superseded":
        controls({ initialTimeoutMs: 120_000 });
        break;
      case "revoked":
        admission.close();
        break;
      case "replaced admission": {
        const next = prepareSystemAgentRunAdmission({}, runId, "main", "replacement");
        cleanups.push(next.close);
        await next.admit("embedded");
        break;
      }
      case "rotated lifecycle":
        rotateAgentEventLifecycleGeneration();
        break;
      case "aborted input":
        abort.abort();
        break;
    }
    const deadline = operation.watchdog.snapshot().executionDeadlineAtMs;
    old.onAttemptDeadlineChanged({ kind: "bounded", deadlineAtMs: Date.now() });
    old.onAttemptTimeout(new Error("late timeout"));
    old.onAttemptAbort();
    expect(onAbort).not.toHaveBeenCalled();
    expect(operation.watchdog.snapshot().executionDeadlineAtMs).toBe(deadline);
    if (invalidation !== "rotated lifecycle") {
      expect(controller.abortSignal.aborted).toBe(false);
    }
  });

  it.each(["Stop", "runtime timeout"] as const)(
    "retains raw capacity after %s and one cleanup grace",
    async (source) => {
      const { operation, controls, controller, raw, run } = await setup();
      const control = controls({ initialTimeoutMs: MAX_TIMER_TIMEOUT_MS });
      if (source === "Stop") {
        control.onAttemptAbort();
      } else {
        control.onAttemptTimeout(new Error("runtime timeout"));
      }
      await vi.advanceTimersByTimeAsync(EMBEDDED_RUN_LANE_TIMEOUT_GRACE_MS);
      expect(controller.abortSignal.aborted).toBe(true);
      expect(getCommandLaneSnapshot(globalLane).activeCount).toBe(1);
      let settled = false;
      void operation.ownerSettlement.then(() => {
        settled = true;
      });
      await Promise.resolve();
      expect(settled).toBe(false);
      raw.resolve();
      await Promise.allSettled([run]);
      await operation.ownerSettlement;
    },
  );

  it("commits Stop before reentrant creation of successor controls", async () => {
    const { controller, controls } = await setup();
    const nextAbort = vi.fn();
    const onAbort = vi.fn(() => {
      expect(controller.abortSignal.aborted).toBe(true);
      const next = controls({ initialTimeoutMs: MAX_TIMER_TIMEOUT_MS, onAbort: nextAbort });
      next.onAttemptAbort();
      next.onAttemptDeadlineChanged({ kind: "unlimited" });
    });
    const control = controls({ onAbort });
    control.onAttemptAbort();
    expect(onAbort).toHaveBeenCalledOnce();
    expect(nextAbort).not.toHaveBeenCalled();
    expect(getCommandLaneSnapshot(globalLane).activeCount).toBe(1);
  });
});
