// Command queue tests cover bounded command execution and queue ordering.
import { AsyncLocalStorage } from "node:async_hooks";
import { spawnSync } from "node:child_process";
import { importFreshModule } from "openclaw/plugin-sdk/test-fixtures";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import { resolveRuntimeWorkerArgv, resolveRuntimeWorkerUrl } from "../infra/runtime-worker-url.js";
import { getLaneState } from "./command-queue.state.js";
import { resetCommandQueueStateForTest } from "./command-queue.test-support.js";
import {
  tryBeginGatewayRootWorkAdmission,
  tryBeginGatewaySuspendAdmission,
} from "./gateway-work-admission.js";
import { CommandLane } from "./lanes.js";
import { processProbeEntrypoints } from "./process-probes-runtime.test-support.js";

const diagnosticMocks = vi.hoisted(() => ({
  logLaneEnqueue: vi.fn(),
  logLaneDequeue: vi.fn(),
  diag: {
    debug: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  },
}));

vi.mock("../logging/diagnostic-runtime.js", () => ({
  logLaneEnqueue: diagnosticMocks.logLaneEnqueue,
  logLaneDequeue: diagnosticMocks.logLaneDequeue,
  diagnosticLogger: diagnosticMocks.diag,
}));

type CommandQueueModule = typeof import("./command-queue.js");

let enqueueCommandInLane: CommandQueueModule["enqueueCommandInLane"];
let GatewayDrainingError: CommandQueueModule["GatewayDrainingError"];
let getCommandLaneSnapshot: CommandQueueModule["getCommandLaneSnapshot"];
let getQueueSize: CommandQueueModule["getQueueSize"];
let getTotalQueueSize: CommandQueueModule["getTotalQueueSize"];
let markGatewayDraining: CommandQueueModule["markGatewayDraining"];
let resetAllLanes: CommandQueueModule["resetAllLanes"];
let setCommandLaneConcurrency: CommandQueueModule["setCommandLaneConcurrency"];

function enqueueBlockedMainTask<T = void>(
  onRelease?: () => Promise<T> | T,
): {
  task: Promise<T>;
  release: () => void;
} {
  const deferred = createDeferred();
  const task = enqueueCommandInLane(CommandLane.Main, async () => {
    await deferred.promise;
    return (await onRelease?.()) as T;
  });
  return { task, release: deferred.resolve };
}

function diagnosticDebugMessages(): string[] {
  return diagnosticMocks.diag.debug.mock.calls
    .map(([message]) => message)
    .filter((message): message is string => typeof message === "string");
}

describe("command queue", () => {
  beforeAll(async () => {
    ({
      enqueueCommandInLane,
      GatewayDrainingError,
      getCommandLaneSnapshot,
      getQueueSize,
      getTotalQueueSize,
      markGatewayDraining,
      resetAllLanes,
      setCommandLaneConcurrency,
    } = await import("./command-queue.js"));
  });

  beforeEach(() => {
    vi.useRealTimers();
    resetCommandQueueStateForTest();
    // Queue state is global across module instances, so reset main lane
    // concurrency explicitly to avoid cross-file leakage.
    setCommandLaneConcurrency(CommandLane.Main, 1);
    diagnosticMocks.logLaneEnqueue.mockClear();
    diagnosticMocks.logLaneDequeue.mockClear();
    diagnosticMocks.diag.debug.mockClear();
    diagnosticMocks.diag.warn.mockClear();
    diagnosticMocks.diag.error.mockClear();
  });

  afterEach(() => {
    vi.useRealTimers();
    resetCommandQueueStateForTest();
  });

  it("runs queued tasks and their lifecycle callbacks in their enqueue-time async context", async () => {
    const context = new AsyncLocalStorage<string>();
    const blocker = createDeferred();
    const callbacks: Array<[string, string | undefined]> = [];
    const first = context.run("first", () =>
      enqueueCommandInLane(CommandLane.Main, async () => {
        await blocker.promise;
        return context.getStore();
      }),
    );
    const second = context.run("second", () =>
      enqueueCommandInLane(CommandLane.Main, async () => context.getStore(), {
        warnAfterMs: 0,
        onWait: () => callbacks.push(["wait", context.getStore()]),
        taskTimeoutMs: 60_000,
        taskTimeoutProgressAtMs: () => {
          callbacks.push(["progress", context.getStore()]);
          return Date.now();
        },
        taskTimeoutSubscribe: () => {
          callbacks.push(["subscribe", context.getStore()]);
          return () => callbacks.push(["unsubscribe", context.getStore()]);
        },
      }),
    );

    blocker.resolve();

    await expect(first).resolves.toBe("first");
    await expect(second).resolves.toBe("second");
    expect(callbacks).toEqual([
      ["wait", "second"],
      ["progress", "second"],
      ["subscribe", "second"],
      ["unsubscribe", "second"],
    ]);
  });

  it("preserves priority and FIFO order across partial drains and resumed growth", async () => {
    const lane = "priority-fifo-resume";
    const calls: string[] = [];
    const enqueue = (label: string, priority?: "foreground" | "background") =>
      enqueueCommandInLane(
        lane,
        async () => {
          calls.push(label);
        },
        { priority },
      );

    setCommandLaneConcurrency(lane, 0);
    const normal = Array.from({ length: 20 }, (_, index) => enqueue(`normal-${index}`));

    setCommandLaneConcurrency(lane, 5);
    setCommandLaneConcurrency(lane, 0);
    await Promise.all(normal.slice(0, 5));

    const resumedNormal = Array.from({ length: 20 }, (_, index) => enqueue(`normal-${index + 20}`));
    const background = Array.from({ length: 18 }, (_, index) =>
      enqueue(`background-${index}`, "background"),
    );
    const foreground = Array.from({ length: 18 }, (_, index) =>
      enqueue(`foreground-${index}`, "foreground"),
    );
    setCommandLaneConcurrency(lane, 1);
    await Promise.all([...normal, ...resumedNormal, ...background, ...foreground]);

    expect(calls).toEqual([
      ...Array.from({ length: 5 }, (_, index) => `normal-${index}`),
      ...Array.from({ length: 18 }, (_, index) => `foreground-${index}`),
      ...Array.from({ length: 35 }, (_, index) => `normal-${index + 5}`),
      ...Array.from({ length: 18 }, (_, index) => `background-${index}`),
    ]);
  });

  it("avoids quadratic array work as a paused queue doubles", () => {
    const queueUrl = resolveRuntimeWorkerUrl(processProbeEntrypoints.commandQueue);
    const script = String.raw`
      const { enqueueCommandInLane, setCommandLaneConcurrency } = await import(
        ${JSON.stringify(queueUrl.href)}
      );
      const originalFindIndex = Array.prototype.findIndex;
      const originalShift = Array.prototype.shift;
      let enqueueComparisons = 0;
      let shiftedSlots = 0;

      Array.prototype.findIndex = function (predicate, thisArg) {
        return originalFindIndex.call(this, (value, index, array) => {
          enqueueComparisons += 1;
          return predicate.call(thisArg, value, index, array);
        });
      };
      Array.prototype.shift = function () {
        shiftedSlots += this.length;
        return originalShift.call(this);
      };

      const measureQueueWork = async (count) => {
        const lane = "linear-queue-" + count;
        setCommandLaneConcurrency(lane, 0);
        const comparisonStart = enqueueComparisons;
        const tasks = Array.from({ length: count }, (_, index) =>
          enqueueCommandInLane(lane, async () => index),
        );
        const enqueueWork = enqueueComparisons - comparisonStart;
        const shiftedSlotStart = shiftedSlots;
        setCommandLaneConcurrency(lane, count);
        const dequeueWork = shiftedSlots - shiftedSlotStart;
        await Promise.all(tasks);
        return { enqueueWork, dequeueWork };
      };

      const smaller = await measureQueueWork(256);
      const larger = await measureQueueWork(512);
      process.stdout.write(JSON.stringify({ smaller, larger }));
    `;
    const result = spawnSync(
      process.execPath,
      [...resolveRuntimeWorkerArgv(queueUrl).slice(0, -1), "--input-type=module", "--eval", script],
      {
        cwd: process.cwd(),
        encoding: "utf8",
        env: {
          ...process.env,
          NODE_OPTIONS: undefined,
          VITEST: undefined,
          VITEST_POOL_ID: undefined,
          VITEST_WORKER_ID: undefined,
        },
        timeout: 60_000,
      },
    );

    expect(result.error).toBeUndefined();
    expect(result.status, result.stderr).toBe(0);
    const measurements = JSON.parse(result.stdout) as {
      smaller: { enqueueWork: number; dequeueWork: number };
      larger: { enqueueWork: number; dequeueWork: number };
    };
    expect(measurements.larger.enqueueWork).toBeLessThanOrEqual(
      measurements.smaller.enqueueWork * 3 + 512,
    );
    expect(measurements.larger.dequeueWork).toBeLessThanOrEqual(
      measurements.smaller.dequeueWork * 3 + 512,
    );
  });

  it("does not report capacity waiting for an entry synchronously cancelled during enqueue", async () => {
    const lane = "reentrant-clear";
    setCommandLaneConcurrency(lane, 0);
    const cancellation = new AbortController();
    const reason = new Error("owner cancelled during enqueue");
    diagnosticMocks.logLaneEnqueue.mockImplementationOnce(() => cancellation.abort(reason));
    const onQueued = vi.fn();
    await expect(
      enqueueCommandInLane(lane, async () => undefined, {
        onQueued,
        abortSignal: cancellation.signal,
      }),
    ).rejects.toBe(reason);
    expect(onQueued).not.toHaveBeenCalled();
  });

  it("demotes live model switch lane failures to debug noise", async () => {
    const error = new Error("Live session model switch requested: anthropic/claude-opus-4-6");
    error.name = "LiveSessionModelSwitchError";

    await expect(
      enqueueCommandInLane("nested", async () => {
        throw error;
      }),
    ).rejects.toBe(error);

    expect(diagnosticMocks.diag.error).not.toHaveBeenCalled();
    expect(
      diagnosticDebugMessages().some((message) =>
        message.includes("lane task interrupted: lane=nested"),
      ),
    ).toBe(true);
  });

  it.each([
    "session:probe-setup-inference:openai",
    "session:temp:setup-inference:probe-setup-inference-test-uuid",
  ])("keeps setup-inference probe lane failures quiet: %s", async (lane) => {
    const error = new Error("Authentication failed");

    await expect(
      enqueueCommandInLane(lane, async () => {
        throw error;
      }),
    ).rejects.toBe(error);

    expect(diagnosticMocks.diag.error).not.toHaveBeenCalled();
    expect(
      diagnosticDebugMessages().some((message) =>
        message.includes(`lane task interrupted: lane=${lane}`),
      ),
    ).toBe(false);
  });

  it.each([0, 3])("counts matching targets with lane concurrency %s", async (concurrency) => {
    const lane = CommandLane.CronNested;
    const finish = createDeferred();
    setCommandLaneConcurrency(lane, concurrency);
    const tasks = ["alpha", "beta", undefined].map((agentId) =>
      enqueueCommandInLane(lane, () => finish.promise, {
        sessionTarget: agentId ? { agentId, sessionId: agentId } : undefined,
      }),
    );
    try {
      expect(getQueueSize(lane)).toBe(3);
      expect(getQueueSize(lane, (target) => target?.agentId === "alpha")).toBe(1);
      expect(getQueueSize(lane, (target) => target?.agentId === "beta")).toBe(1);
      expect(getQueueSize(lane, (target) => target?.agentId === "other")).toBe(0);
      expect(getQueueSize(lane, (target) => target === undefined)).toBe(1);
    } finally {
      finish.resolve();
      setCommandLaneConcurrency(lane, 3);
      await Promise.all(tasks);
    }
    expect(getQueueSize(lane, (target) => target?.agentId === "alpha")).toBe(0);
  });

  it("task timeout renews from progress timestamps", async () => {
    const lane = `timeout-progress-lane-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setCommandLaneConcurrency(lane, 1);

    vi.useFakeTimers();
    try {
      let progressAtMs = Date.now();
      const blocker = createDeferred();
      const first = enqueueCommandInLane(
        lane,
        async () => {
          await blocker.promise;
          return "first";
        },
        {
          taskTimeoutMs: 25,
          taskTimeoutProgressAtMs: () => progressAtMs,
        },
      );
      let secondRan = false;
      const second = enqueueCommandInLane(lane, async () => {
        secondRan = true;
        return "second";
      });

      await vi.advanceTimersByTimeAsync(20);
      progressAtMs = Date.now();
      await vi.advanceTimersByTimeAsync(20);
      expect(secondRan).toBe(false);

      blocker.resolve();
      await expect(first).resolves.toBe("first");
      await expect(second).resolves.toBe("second");
      expect(secondRan).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it("task timeout falls back when progress timestamp callback throws", async () => {
    const lane = `timeout-progress-throw-lane-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setCommandLaneConcurrency(lane, 1);

    vi.useFakeTimers();
    try {
      const first = enqueueCommandInLane(lane, async () => new Promise<never>(() => {}), {
        taskTimeoutMs: 25,
        taskTimeoutProgressAtMs: () => {
          throw new Error("progress failed");
        },
      });
      const firstRejected = expect(first).rejects.toMatchObject({
        name: "CommandLaneTaskTimeoutError",
        message: expect.stringContaining("no progress for 25ms (task budget 25ms"),
      });

      await vi.advanceTimersByTimeAsync(25);
      await firstRejected;

      expect(
        diagnosticMocks.diag.warn.mock.calls.some(([message]) =>
          String(message).includes("lane task timeout progress callback failed"),
        ),
      ).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it("owner cancellation rejects pending promises at every priority without releasing active work", async () => {
    const gate = createDeferred();
    const cancellation = new AbortController();
    const first = enqueueCommandInLane(
      CommandLane.Main,
      async () => {
        await gate.promise;
        return "first";
      },
      { abortSignal: cancellation.signal },
    );
    const task = vi.fn(async () => "unexpected");
    const pending = (["background", "normal", "foreground"] as const).map((priority) =>
      enqueueCommandInLane(CommandLane.Main, task, { priority, abortSignal: cancellation.signal }),
    );
    const reason = new Error("owner stopped pending work");
    const outcomes = Promise.allSettled(pending);

    cancellation.abort(reason);
    expect(getCommandLaneSnapshot(CommandLane.Main)).toMatchObject({
      activeCount: 1,
      queuedCount: 0,
    });
    expect(await outcomes).toEqual(pending.map(() => ({ status: "rejected", reason })));
    expect(task).not.toHaveBeenCalled();

    gate.resolve();
    await expect(first).resolves.toBe("first");
  });

  it("keeps draining functional after synchronous onWait failure", async () => {
    const lane = `drain-sync-throw-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setCommandLaneConcurrency(lane, 1);

    const deferred = createDeferred();
    const first = enqueueCommandInLane(lane, async () => {
      await deferred.promise;
      return "first";
    });
    const second = enqueueCommandInLane(lane, async () => "second", {
      warnAfterMs: 0,
      onWait: () => {
        throw new Error("onWait exploded");
      },
    });
    await Promise.resolve();
    expect(getQueueSize(lane)).toBeGreaterThanOrEqual(2);

    deferred.resolve();
    await expect(first).resolves.toBe("first");
    await expect(second).resolves.toBe("second");
  });

  it.each(["admission", "error"] as const)(
    "settles callers and drains the command lane when %s diagnostics throw",
    async (phase) => {
      vi.useFakeTimers();
      const lane = "diagnostic-failure";
      const diagnosticError = new Error("diagnostic sink failed");
      const taskError = new Error("Session transcript keyed user is outside the current turn");
      const gate = createDeferred<string>();
      const firstTask = vi.fn(() => gate.promise);
      const successor = vi.fn(async () => "successor");
      const taskIdentity = { taskKind: "turn", runId: "diagnostic-failure-run" };

      if (phase === "admission") {
        diagnosticMocks.logLaneDequeue.mockImplementationOnce(() => {
          throw diagnosticError;
        });
      }
      const outcomes = Promise.allSettled([
        enqueueCommandInLane(lane, firstTask, { taskIdentity }),
        enqueueCommandInLane(lane, successor),
      ]);
      if (phase !== "admission") {
        diagnosticMocks.diag.error.mockImplementationOnce(() => {
          throw diagnosticError;
        });
      }
      if (phase === "error") {
        gate.reject(taskError);
      } else {
        gate.resolve("first");
      }
      await vi.runAllTimersAsync();

      expect(successor).toHaveBeenCalledOnce();
      expect(firstTask).toHaveBeenCalledTimes(phase === "admission" ? 0 : 1);
      expect(await outcomes).toEqual([
        { status: "rejected", reason: phase === "admission" ? diagnosticError : taskError },
        { status: "fulfilled", value: "successor" },
      ]);
      expect(getCommandLaneSnapshot(lane)).toMatchObject({ activeCount: 0, queuedCount: 0 });
      if (phase !== "admission") {
        expect(diagnosticMocks.diag.warn).toHaveBeenCalledWith(
          expect.stringContaining("lane task diagnostics failed after settlement:"),
          taskIdentity,
        );
      }
    },
  );

  it.each([
    { reason: "restart", message: "Gateway is restarting. Please try again shortly." },
    {
      reason: "stop (SIGTERM)",
      message: "Gateway is shutting down. Please try again once it is back online.",
    },
  ] satisfies {
    reason: Parameters<CommandQueueModule["markGatewayDraining"]>[0];
    message: string;
  }[])("explains why new enqueues are refused for $reason", async ({ reason, message }) => {
    markGatewayDraining(reason);
    const task = vi.fn(async () => "blocked");
    await expect(enqueueCommandInLane(CommandLane.Main, task)).rejects.toMatchObject({
      name: "GatewayDrainingError",
      message,
    });
    expect(task).not.toHaveBeenCalled();
  });

  it("does not affect already-active tasks after markGatewayDraining", async () => {
    const { task, release } = enqueueBlockedMainTask(async () => "ok");
    markGatewayDraining();
    release();
    await expect(task).resolves.toBe("ok");
  });

  it("reversibly fences new enqueues without disturbing an active task", async () => {
    const { task, release } = enqueueBlockedMainTask(async () => "active-finished");
    const suspension = tryBeginGatewaySuspendAdmission(() => {});
    expect(suspension?.commit()).toBe(true);
    await expect(enqueueCommandInLane(CommandLane.Main, async () => "blocked")).rejects.toThrow(
      "Gateway is temporarily paused. Please try again shortly.",
    );

    release();
    await expect(task).resolves.toBe("active-finished");
    expect(suspension?.release()).toBe(true);
    await expect(enqueueCommandInLane(CommandLane.Main, async () => "resumed")).resolves.toBe(
      "resumed",
    );
  });

  it("lets an admitted root enqueue while suspension preparation refuses new work", async () => {
    const continueRoot = createDeferred();
    const root = tryBeginGatewayRootWorkAdmission();
    expect(root).not.toBeNull();
    const result = root?.run(async () => {
      await continueRoot.promise;
      return await enqueueCommandInLane(CommandLane.Main, async () => "continued");
    });
    const suspension = tryBeginGatewaySuspendAdmission(() => {});

    try {
      continueRoot.resolve();
      await expect(result).resolves.toBe("continued");
      await expect(
        enqueueCommandInLane(CommandLane.Main, async () => "blocked"),
      ).rejects.toBeInstanceOf(GatewayDrainingError);
    } finally {
      suspension?.rollback();
      root?.release();
    }
  });

  it("rejects subordinate enqueues from an admitted root after restart drain", async () => {
    const continueRoot = createDeferred();
    const root = tryBeginGatewayRootWorkAdmission();
    expect(root).not.toBeNull();
    const result = root?.run(async () => {
      await continueRoot.promise;
      return await enqueueCommandInLane(CommandLane.Main, async () => "blocked");
    });

    try {
      markGatewayDraining();
      continueRoot.resolve();
      await expect(result).rejects.toBeInstanceOf(GatewayDrainingError);
    } finally {
      root?.release();
    }
  });

  it("re-admits preserved queued work after reset retires its captured root", async () => {
    const outerLane = `restart-outer-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const innerLane = `restart-inner-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setCommandLaneConcurrency(outerLane, 0);
    const root = tryBeginGatewayRootWorkAdmission();
    expect(root).not.toBeNull();
    let task: Promise<string> | undefined;
    await root?.run(async () => {
      task = enqueueCommandInLane(outerLane, async () =>
        enqueueCommandInLane(innerLane, async () => "continued"),
      );
    });

    markGatewayDraining();
    resetAllLanes();
    setCommandLaneConcurrency(outerLane, 1);

    await expect(task).resolves.toBe("continued");
    root?.release();
  });

  it("does not re-admit queued work after its root is released normally", async () => {
    const outerLane = `released-outer-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const innerLane = `released-inner-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setCommandLaneConcurrency(outerLane, 0);
    const root = tryBeginGatewayRootWorkAdmission();
    expect(root).not.toBeNull();
    let task: Promise<string> | undefined;
    await root?.run(async () => {
      task = enqueueCommandInLane(outerLane, async () =>
        enqueueCommandInLane(innerLane, async () => "unexpected"),
      );
    });

    root?.release();
    setCommandLaneConcurrency(outerLane, 1);

    await expect(task).rejects.toBeInstanceOf(GatewayDrainingError);
  });

  it("shares lane state across distinct module instances", async () => {
    const commandQueueA = await importFreshModule<typeof import("./command-queue.js")>(
      import.meta.url,
      "./command-queue.js?scope=shared-a",
    );
    const commandQueueB = await importFreshModule<typeof import("./command-queue.js")>(
      import.meta.url,
      "./command-queue.js?scope=shared-b",
    );
    const lane = `shared-state-${Date.now()}-${Math.random().toString(16).slice(2)}`;

    const blocker = createDeferred();

    commandQueueA.resetAllLanes();

    try {
      const task = commandQueueA.enqueueCommandInLane(lane, async () => {
        await blocker.promise;
        return "done";
      });

      expect(commandQueueB.getQueueSize(lane)).toBe(1);
      expect(commandQueueB.getTotalQueueSize()).toBe(1);

      blocker.resolve();
      await expect(task).resolves.toBe("done");
      expect(commandQueueB.getQueueSize(lane)).toBe(0);
    } finally {
      blocker.resolve();
      commandQueueA.resetAllLanes();
    }
  });
  describe("scoped command lane lifecycle", () => {
    function getCommandLaneRegistryForTest(): Map<string, unknown> {
      const state = (globalThis as Record<PropertyKey, unknown>)[
        Symbol.for("openclaw.commandQueueState")
      ];
      const lanes = (state as { lanes?: unknown } | undefined)?.lanes;
      if (!(lanes instanceof Map)) {
        throw new Error("Expected the shared command lane registry to be initialized");
      }
      return lanes as Map<string, unknown>;
    }

    it.each(["session:probe-", "cli:"])(
      "retires parallel %s lanes after completion",
      async (prefix) => {
        const lanes = getCommandLaneRegistryForTest();
        const baselineSize = lanes.size;
        const allRunsStarted = createDeferred();
        let activeRuns = 0;
        let peakActiveRuns = 0;
        const laneNames = Array.from(
          { length: 10 },
          (_, index) => `${prefix}agent:main:autoqa-${index}`,
        );

        const results = await Promise.all(
          laneNames.map((lane, index) =>
            enqueueCommandInLane(lane, async () => {
              activeRuns += 1;
              peakActiveRuns = Math.max(peakActiveRuns, activeRuns);
              if (activeRuns === laneNames.length) {
                allRunsStarted.resolve();
              }
              await allRunsStarted.promise;
              activeRuns -= 1;
              return index;
            }),
          ),
        );

        expect(results).toEqual(Array.from({ length: 10 }, (_, index) => index));
        expect(peakActiveRuns).toBe(10);
        expect(activeRuns).toBe(0);
        expect(getTotalQueueSize()).toBe(0);
        expect(lanes.size).toBe(baselineSize);
        for (const lane of laneNames) {
          expect(lanes.has(lane)).toBe(false);
        }
      },
    );

    it("keeps a setup-probe lane until its queued successor finishes", async () => {
      const lanes = getCommandLaneRegistryForTest();
      const lane = "session:probe-autoqa-queued";
      const firstGate = createDeferred();
      const secondGate = createDeferred();

      const first = enqueueCommandInLane(lane, async () => {
        await firstGate.promise;
        return "first";
      });
      const second = enqueueCommandInLane(lane, async () => {
        await secondGate.promise;
        return "second";
      });

      expect(lanes.has(lane)).toBe(true);
      firstGate.resolve();
      await expect(first).resolves.toBe("first");
      expect(lanes.has(lane)).toBe(true);
      expect(getCommandLaneSnapshot(lane)).toMatchObject({ activeCount: 1, queuedCount: 0 });

      secondGate.resolve();
      await expect(second).resolves.toBe("second");
      expect(lanes.has(lane)).toBe(false);
    });

    it("updates each session's subagent capacity and retires the queues after completion", async () => {
      const lanes = getCommandLaneRegistryForTest();
      const parents = ["subagent:agent:main:parent-a", "subagent:agent:main:parent-b"];
      const gate = createDeferred();
      setCommandLaneConcurrency(CommandLane.Subagent, 2);
      const runs = parents.flatMap((lane) =>
        Array.from({ length: 3 }, () =>
          enqueueCommandInLane(lane, async () => {
            await gate.promise;
          }),
        ),
      );

      try {
        for (const lane of parents) {
          expect(getCommandLaneSnapshot(lane)).toMatchObject({
            maxConcurrent: 2,
            activeCount: 2,
            queuedCount: 1,
          });
        }

        setCommandLaneConcurrency(CommandLane.Subagent, 1);
        for (const lane of parents) {
          expect(getCommandLaneSnapshot(lane)).toMatchObject({
            maxConcurrent: 1,
            activeCount: 2,
            queuedCount: 1,
          });
        }

        setCommandLaneConcurrency(CommandLane.Subagent, 3);
        for (const lane of parents) {
          expect(getCommandLaneSnapshot(lane)).toMatchObject({
            maxConcurrent: 3,
            activeCount: 3,
            queuedCount: 0,
          });
        }
      } finally {
        gate.resolve();
        await Promise.all(runs);
      }
      for (const lane of parents) {
        expect(lanes.has(lane)).toBe(false);
        expect(getCommandLaneSnapshot(lane).maxConcurrent).toBe(3);
        expect(lanes.has(lane)).toBe(false);
      }
    });

    it("preserves explicitly configured and paused dynamic lanes", async () => {
      const lanes = getCommandLaneRegistryForTest();
      const configuredLane = "session:probe-autoqa-configured";
      const pausedLane = "nested:agent:main:autoqa-paused";

      setCommandLaneConcurrency(configuredLane, 2);
      await Promise.all([
        enqueueCommandInLane(configuredLane, async () => "first"),
        enqueueCommandInLane(configuredLane, async () => "second"),
      ]);

      expect(lanes.has(configuredLane)).toBe(true);
      expect(getCommandLaneSnapshot(configuredLane)).toMatchObject({
        activeCount: 0,
        queuedCount: 0,
        maxConcurrent: 2,
      });

      setCommandLaneConcurrency(pausedLane, 0);
      let pausedRunStarted = false;
      const pausedRun = enqueueCommandInLane(pausedLane, async () => {
        pausedRunStarted = true;
        return "resumed";
      });

      expect(pausedRunStarted).toBe(false);
      expect(lanes.has(pausedLane)).toBe(true);
      expect(getCommandLaneSnapshot(pausedLane)).toMatchObject({
        activeCount: 0,
        queuedCount: 1,
        maxConcurrent: 0,
      });

      setCommandLaneConcurrency(pausedLane, 1);
      await expect(pausedRun).resolves.toBe("resumed");
      expect(lanes.has(pausedLane)).toBe(false);
      expect(lanes.has(configuredLane)).toBe(true);
    });

    it("does not let pre-restart probe completion retire a replacement-generation run", async () => {
      const lanes = getCommandLaneRegistryForTest();
      const lane = "session:probe-autoqa-replacement";
      const staleGate = createDeferred();
      const replacementGate = createDeferred();
      const staleRun = enqueueCommandInLane(
        lane,
        async () => {
          await staleGate.promise;
          return "stale";
        },
        { sessionTarget: { agentId: "stale", sessionId: "stale" } },
      );

      resetAllLanes();
      const replacementRun = enqueueCommandInLane(
        lane,
        async () => {
          await replacementGate.promise;
          return "replacement";
        },
        { sessionTarget: { agentId: "replacement", sessionId: "replacement" } },
      );
      const replacementState = lanes.get(lane);

      staleGate.resolve();
      await expect(staleRun).resolves.toBe("stale");
      expect(lanes.get(lane)).toBe(replacementState);
      expect(getCommandLaneSnapshot(lane)).toMatchObject({ activeCount: 1, queuedCount: 0 });
      expect(getQueueSize(lane, (target) => target?.agentId === "stale")).toBe(0);
      expect(getQueueSize(lane, (target) => target?.agentId === "replacement")).toBe(1);

      replacementGate.resolve();
      await expect(replacementRun).resolves.toBe("replacement");
      expect(lanes.has(lane)).toBe(false);
    });

    it("recreates a maintenance lane for deferred same-session follow-up work", async () => {
      const lanes = getCommandLaneRegistryForTest();
      const lane = "context-engine-turn-maintenance:agent:main:autoqa-rerun";
      const replacementGate = createDeferred();
      const firstRun = enqueueCommandInLane(lane, async () => "first");
      const originalState = lanes.get(lane);
      const replacementRun = firstRun.then(() =>
        enqueueCommandInLane(lane, async () => {
          await replacementGate.promise;
          return "replacement";
        }),
      );

      await expect(firstRun).resolves.toBe("first");
      expect(lanes.has(lane)).toBe(true);
      expect(lanes.get(lane)).not.toBe(originalState);
      expect(getCommandLaneSnapshot(lane)).toMatchObject({ activeCount: 1, queuedCount: 0 });

      replacementGate.resolve();
      await expect(replacementRun).resolves.toBe("replacement");
      expect(lanes.has(lane)).toBe(false);
    });

    it("does not retire a newer lane state when stale work finishes", async () => {
      const lanes = getCommandLaneRegistryForTest();
      const lane = "session:probe-autoqa-recreated-state";
      const staleGate = createDeferred();
      const staleRun = enqueueCommandInLane(lane, async () => {
        await staleGate.promise;
        return "stale";
      });
      lanes.delete(lane);
      const replacementState = getLaneState(lane);
      staleGate.resolve();
      await expect(staleRun).resolves.toBe("stale");

      expect(lanes.get(lane)).toBe(replacementState);
      lanes.delete(lane);
    });

    it("retires a scoped lane after its active task times out", async () => {
      const lanes = getCommandLaneRegistryForTest();
      const lane = "session:probe-autoqa-timed-out";

      vi.useFakeTimers();
      try {
        const timedOut = enqueueCommandInLane(lane, async () => new Promise<never>(() => {}), {
          taskTimeoutMs: 5,
        });
        const rejection = expect(timedOut).rejects.toMatchObject({
          name: "CommandLaneTaskTimeoutError",
        });

        await vi.advanceTimersByTimeAsync(5);
        await rejection;

        expect(lanes.has(lane)).toBe(false);
      } finally {
        vi.useRealTimers();
      }
    });
  });
});
