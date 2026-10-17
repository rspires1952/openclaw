import { afterEach, describe, expect, it, vi } from "vitest";
import {
  clearTestEmbeddedRun as clearActiveEmbeddedRun,
  registerTestEmbeddedRun as setActiveEmbeddedRun,
  createEmbeddedRunHandle,
} from "../agents/embedded-agent-runner/runs.test-support.js";
import { claimAgentRunContext, releaseAgentRunContext } from "../infra/agent-run-registry.js";
import { retireSessionControllerInput } from "../sessions/session-controller.mailbox.js";
import { rpcSourceTesting } from "../sessions/session-lifecycle-admission.test-support.js";
import { createChatAbortMarker } from "./server-chat-state.js";
import { createGatewayMaintenanceStateForTest } from "./test-helpers.maintenance-state.js";
import { createRpcSourceForTest } from "./test-helpers.rpc-source.js";

// mock-isolation: Chat-state timers must not start the unrelated device expiry worker.
vi.mock("../infra/device-bootstrap.js", () => ({
  startDevicePairSetupMaintenance: vi.fn(async () => 0),
}));

const ABORTED_RUN_TTL_MS = 60 * 60_000;

function createMaintenanceTimerDeps() {
  return {
    ...createGatewayMaintenanceStateForTest(),
    runWorktreeGc: async () => undefined,
    runManagedOutgoingMediaGc: async () => undefined,
  };
}

type MaintenanceTimerDeps = ReturnType<typeof createMaintenanceTimerDeps>;

async function createTimedMaintenanceScenario() {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-03-22T00:00:00Z"));
  const { startGatewayMaintenanceTimers } = await import("./server-maintenance.js");
  return { startGatewayMaintenanceTimers, deps: createMaintenanceTimerDeps(), now: Date.now() };
}

async function stopMaintenanceTimers(
  timers: ReturnType<typeof import("./server-maintenance.js").startGatewayMaintenanceTimers>,
) {
  await timers.stopPeriodicTasks();
  await timers.skillUsageCleanup();
}

function staleRunTimestamp(): number {
  return Date.now() - ABORTED_RUN_TTL_MS - 1;
}

function seedStaleRunBuffers(deps: MaintenanceTimerDeps, runId: string): void {
  const now = Date.now();
  vi.setSystemTime(staleRunTimestamp());
  deps.chatRunState.updateBuffer(runId, { delta: "buffer" });
  deps.chatRunState.takeBufferDelta(runId, "buffer");
  Object.assign(deps.chatRunState.getOrCreate(runId), {
    buffer: "buffer",
    rawBuffer: "buffer",
    deltaSentAt: Date.now(),
    assistantScope: { itemId: "assistant-1", prefix: "", boundaryNewlines: 0, separatorLength: 0 },
  });
  vi.setSystemTime(now);
}

function expectStaleRunBuffersPresent(deps: MaintenanceTimerDeps, runId: string): void {
  expect(deps.chatRunState.runs.get(runId)).toMatchObject({
    buffer: "buffer",
    rawBuffer: "buffer",
    display: expect.any(Object),
    deltaSentAt: expect.any(Number),
    assistantScope: { itemId: "assistant-1", prefix: "", boundaryNewlines: 0, separatorLength: 0 },
  });
}

describe("gateway chat-state maintenance", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("keeps stale buffers for active runs that still have abort controllers", async () => {
    const { startGatewayMaintenanceTimers, deps } = await createTimedMaintenanceScenario();
    const runId = "run-active";
    const source = createRpcSourceForTest(
      {},
      { sessionId: "maintenance-session", sessionKey: "main" },
    );
    rpcSourceTesting.set(runId, source);
    seedStaleRunBuffers(deps, runId);

    const timers = startGatewayMaintenanceTimers(deps);

    await vi.advanceTimersByTimeAsync(60_000);

    expectStaleRunBuffersPresent(deps, runId);

    retireSessionControllerInput(source.input);
    await stopMaintenanceTimers(timers);
  });

  it.each(["progress", "registration"])("sweeps abandoned %s records", async (kind) => {
    const { startGatewayMaintenanceTimers, deps, now } = await createTimedMaintenanceScenario();
    vi.setSystemTime(staleRunTimestamp());
    const runId = `orphan-${kind}`;
    if (kind === "registration") {
      deps.chatRunState.registry.add(runId, { sessionKey: "main", clientRunId: runId });
    } else {
      deps.chatRunState.recordProgressEvent(runId, {
        runId,
        seq: 1,
        ts: Date.now(),
        stream: "tool",
        data: { phase: "start", toolCallId: "read-1", name: "read" },
      });
    }
    vi.setSystemTime(now);
    const timers = startGatewayMaintenanceTimers(deps);
    try {
      expect(deps.chatRunState.runs.has(runId)).toBe(true);
      await vi.advanceTimersByTimeAsync(60_000);
      expect(deps.chatRunState.runs.has(runId)).toBe(false);
    } finally {
      await stopMaintenanceTimers(timers);
    }
  });

  it("keeps recently updated records despite an old thinking timestamp", async () => {
    const { startGatewayMaintenanceTimers, deps, now } = await createTimedMaintenanceScenario();
    vi.setSystemTime(staleRunTimestamp());
    deps.chatRunState.getOrCreate("recent").agentText = {
      thinking: { lastSentAt: Date.now() },
    };
    vi.setSystemTime(now);
    deps.chatRunState.getOrCreate("recent").rawBuffer = "Fresh output";
    const timers = startGatewayMaintenanceTimers(deps);
    try {
      await vi.advanceTimersByTimeAsync(60_000);
      expect(deps.chatRunState.resolveBuffer("recent").text).toBe("Fresh output");
    } finally {
      await stopMaintenanceTimers(timers);
    }
  });

  it("sweeps stale aborted records and their buffered state", async () => {
    const { startGatewayMaintenanceTimers, deps } = await createTimedMaintenanceScenario();
    const runId = "orphan-aborted";
    const abortedAt = staleRunTimestamp();
    deps.chatRunState.getOrCreate(runId).abortMarker = createChatAbortMarker(abortedAt);
    seedStaleRunBuffers(deps, runId);
    deps.chatRunState.getOrCreate(runId).agentText = {
      assistant: {
        lastSentAt: abortedAt,
        bufferedEvent: {
          payload: {
            runId,
            seq: 1,
            stream: "assistant",
            ts: Date.now(),
            data: { text: "buffer", delta: "buffer" },
          },
        },
      },
    };
    const timers = startGatewayMaintenanceTimers(deps);
    try {
      expect(deps.chatRunState.runs.has(runId)).toBe(true);
      await vi.advanceTimersByTimeAsync(60_000);
      expect(deps.chatRunState.runs.has(runId)).toBe(false);
      const run = deps.chatRunState.runs.get(runId);
      expect(run?.buffer).toBeUndefined();
      expect(run?.rawBuffer).toBeUndefined();
      expect(run?.deltaSentAt).toBeUndefined();
      expect(run?.assistantScope).toBeUndefined();
      expect(run?.display).toBeUndefined();
      expect(run?.abortMarker).toBeUndefined();
      expect(run?.agentText).toBeUndefined();
    } finally {
      await stopMaintenanceTimers(timers);
    }
  });

  it.each(["execution", "embedded", "queued"] as const)(
    "preserves a live %s owner without a chat abort controller until release",
    async (kind) => {
      const { startGatewayMaintenanceTimers, deps } = await createTimedMaintenanceScenario();
      const runId = `live-${kind}`;
      seedStaleRunBuffers(deps, runId);
      const handle = createEmbeddedRunHandle({ runId });
      const claim =
        kind === "execution"
          ? claimAgentRunContext(
              runId,
              { sessionKey: "main" },
              {
                trackOwner: true,
                ownsContext: true,
                protectFromSweep: true,
              },
            )
          : undefined;
      if (kind === "embedded") {
        setActiveEmbeddedRun("maintenance-session", handle, "main");
      }
      const source =
        kind === "queued"
          ? createRpcSourceForTest(
              {},
              {
                phase: "waiting",
                sessionId: "maintenance-session",
                sessionKey: "main",
              },
            )
          : undefined;
      if (source) {
        rpcSourceTesting.set(runId, source);
      }
      const release = () => {
        releaseAgentRunContext(runId, claim);
        if (source) {
          retireSessionControllerInput(source.input);
          rpcSourceTesting.delete(runId);
        }
        if (kind === "embedded") {
          clearActiveEmbeddedRun("maintenance-session", handle, "main");
        }
      };
      const timers = startGatewayMaintenanceTimers(deps);
      try {
        await vi.advanceTimersByTimeAsync(60_000);
        expectStaleRunBuffersPresent(deps, runId);
        release();
        await vi.advanceTimersByTimeAsync(60_000);
        expect(deps.chatRunState.runs.has(runId)).toBe(false);
      } finally {
        release();
        await stopMaintenanceTimers(timers);
      }
    },
  );
  it("prunes idle tool-event recipients while preserving grace and registered run state", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-22T00:00:00Z"));
    const deps = {
      ...createGatewayMaintenanceStateForTest(),
      logHealth: { info: vi.fn(), error: vi.fn() },
      runWorktreeGc: async () => undefined,
      runDeliveryQueueMediaGc: async () => undefined,
      runManagedOutgoingMediaGc: async () => undefined,
    };
    const { toolEventRecipients, registry, runs } = deps.chatRunState;
    toolEventRecipients.add("active-expired", "conn-active");
    toolEventRecipients.add("registered-expired", "conn-registered");
    registry.add("registered-expired", { sessionKey: "session-1", clientRunId: "client-1" });

    await vi.advanceTimersByTimeAsync(9 * 60_000 + 2_000);
    const { startGatewayMaintenanceTimers } = await import("./server-maintenance.js");
    const timers = startGatewayMaintenanceTimers(deps);
    try {
      await vi.advanceTimersByTimeAsync(27_000);
      toolEventRecipients.add("finalized-expired", "conn-final");
      toolEventRecipients.markFinal("finalized-expired");
      toolEventRecipients.add("recent", "conn-recent");
      await vi.advanceTimersByTimeAsync(2_000);
      toolEventRecipients.add("finalized-grace", "conn-grace");
      toolEventRecipients.markFinal("finalized-grace");

      // The aligned tick at minute ten falls between the two finalization deadlines.
      await vi.advanceTimersByTimeAsync(29_000);
      expect(runs.has("active-expired")).toBe(false);
      expect(runs.has("finalized-expired")).toBe(false);
      expect(runs.get("registered-expired")?.toolRecipient).toBeUndefined();
      expect(registry.peek("registered-expired")?.clientRunId).toBe("client-1");
      expect(toolEventRecipients.get("finalized-grace")).toEqual(new Set(["conn-grace"]));
      expect(toolEventRecipients.get("recent")).toEqual(new Set(["conn-recent"]));

      await vi.advanceTimersByTimeAsync(60_000);
      expect(runs.has("finalized-grace")).toBe(false);
      expect(runs.has("recent")).toBe(true);
    } finally {
      await stopMaintenanceTimers(timers);
    }
  });
});
