// Tests gateway active-run matching by logical session key and backing id.
import { afterEach, expect, it, onTestFinished } from "vitest";
import type { EmbeddedAgentQueueHandle } from "../../agents/embedded-agent-runner/run-state.js";
import { abortEmbeddedAgentRun } from "../../agents/embedded-agent-runner/runs.js";
import {
  clearTestEmbeddedRun as clearActiveEmbeddedRun,
  registerTestEmbeddedRun,
} from "../../agents/embedded-agent-runner/runs.test-support.js";
import {
  addSubagentRunForTests,
  resetSubagentRegistryForTests,
} from "../../agents/subagents/registry/subagent-registry.test-helpers.js";
import { admitReplyTurn } from "../../auto-reply/reply/reply-turn-admission.js";
import {
  clearRuntimeConfigSnapshot,
  setRuntimeConfigSnapshot,
} from "../../config/runtime-snapshot.js";
import { rotateAgentEventLifecycleGeneration } from "../../infra/agent-events.js";
import { registerAgentRunCapacityWait } from "../../infra/agent-run-capacity-wait.js";
import {
  buildProjectedAgentRunIndex,
  claimAgentRunContext,
  releaseAgentRunContext,
  getAgentRunLifecycleGeneration,
  clearAgentRunContext,
  registerAgentRunContext,
} from "../../infra/agent-run-registry.js";
import { bindGatewayContextResolver } from "../../plugins/runtime/gateway-request-scope.js";
import {
  createReplyOperation as createControllerOperation,
  markReplyOperationExecutionStarted,
} from "../../sessions/session-controller.js";
import { captureSessionTarget } from "../../sessions/session-controller.lifecycle.js";
import { retireSessionControllerInput } from "../../sessions/session-controller.mailbox.js";
import { waitForSessionRunEnd } from "../../sessions/session-controller.native-runtime.js";
import {
  isSessionRunActive,
  resolveSessionRunProgressState,
} from "../../sessions/session-controller.queries.js";
import type {
  RpcSourceAdapter,
  RpcSourceIdentity,
} from "../../sessions/session-controller.rpc-sources.js";
import { rpcSourceTesting } from "../../sessions/session-lifecycle-admission.test-support.js";
import { withOpenClawTestState } from "../../test-utils/openclaw-test-state.js";
import { registerChatAbortController } from "../chat-abort.js";
import { buildGatewaySessionSnapshot } from "../session-event-payload.js";
import { createRpcSourceForTest, claimRpcSourceForTest } from "../test-helpers.rpc-source.js";
import {
  createVisibleActiveSessionRunProjector,
  hasRegisteredChatRunForSessionKey,
  resolveVisibleActiveSessionRunState,
} from "./session-active-runs.js";

const releaseFixtureSources: Array<() => void> = [];
const storePath = "/synthetic/active-projection/sessions";

function setActiveEmbeddedRun(
  sessionId: string,
  handle: EmbeddedAgentQueueHandle,
  sessionKey: string,
) {
  return registerTestEmbeddedRun(
    sessionId,
    handle,
    sessionKey,
    undefined,
    undefined,
    undefined,
    undefined,
    { storeScope: storePath },
  );
}

function createReplyOperation(params: Parameters<typeof createControllerOperation>[0]) {
  return createControllerOperation({
    ...params,
    target: captureSessionTarget({
      storeScope: storePath,
      sessionKey: params.sessionKey,
      incarnation: params.sessionId,
      agentId: params.agentId,
    }),
  });
}

afterEach(() => {
  for (const release of releaseFixtureSources.splice(0).toReversed()) {
    release();
  }
});
async function activeSources(
  entries: Array<
    [
      string,
      Partial<RpcSourceAdapter> &
        Partial<RpcSourceIdentity> & {
          projectSessionActive?: boolean;
          storeScope?: string;
          queued?: boolean;
        },
    ]
  >,
) {
  const refs = await Promise.all(
    entries.map(async ([runId, metadata]) => {
      const { sessionKey, sessionId, agentId, storeScope, queued, ...adapter } = metadata;
      const ref = createRpcSourceForTest(adapter, {
        runId,
        storeScope: storeScope ?? storePath,
        sessionKey,
        sessionId: sessionId ?? `fixture-${runId}`,
        agentId,
        accepted: true,
      });
      if (!queued) {
        releaseFixtureSources.push(await claimRpcSourceForTest(ref));
      }
      return [runId, ref] as const;
    }),
  );
  return new Map(refs);
}

type ActiveRunParams = Parameters<typeof resolveVisibleActiveSessionRunState>[0];

function visibleState(
  sessionKey: string,
  options: Omit<ActiveRunParams, "requestedKey" | "canonicalKey" | "storePath"> = {},
) {
  return resolveVisibleActiveSessionRunState({
    storePath,
    requestedKey: sessionKey,
    canonicalKey: sessionKey,
    ...options,
  });
}

it("projects an accepted source before its exact controller claim starts", async () => {
  const sessionKey = "agent:main:queued";
  const sessionId = "queued-session";
  const runId = "queued-run";
  rpcSourceTesting.clear();
  const registration = registerChatAbortController({
    target: captureSessionTarget({
      storeScope: storePath,
      sessionKey,
      incarnation: sessionId,
      agentId: "main",
    }),
    runId,
    sessionId,
    sessionKey,
    agentId: "main",
    timeoutMs: 60_000,
    kind: "agent",
  });
  const state = () =>
    resolveVisibleActiveSessionRunState({
      storePath,
      requestedKey: sessionKey,
      canonicalKey: sessionKey,
      sessionId,
      agentId: "main",
    });

  onTestFinished(() => registration.cleanup());
  registration.initializeTiming();

  expect(state()).toEqual({ active: true, runIds: [runId], status: "queued" });
  if (!registration.entry) {
    throw new Error("Missing RPC source");
  }
  releaseFixtureSources.push(await claimRpcSourceForTest(registration.entry));
  expect(registration.markExecutionStarted()).toBe(true);
  expect(state()).toEqual({ active: true, runIds: [runId] });
  expect(registration.markExecutionStarted()).toBe(false);
  registration.cleanup();
});

it("projects direct subagent activity only for its own current-lifecycle session", async () => {
  const parentKey = "agent:main:main";
  const childKey = "agent:main:subagent:attachment-fix";
  await resetSubagentRegistryForTests({ persist: false });
  setRuntimeConfigSnapshot({});
  await addSubagentRunForTests({
    runId: "run-attachment-fix",
    childSessionKey: childKey,
    controllerSessionKey: parentKey,
    requesterSessionKey: parentKey,
    requesterDisplayKey: "main",
    task: "Fix parent activity indicator",
    cleanup: "keep",
    createdAt: 1,
    startedAt: 2,
  });
  registerAgentRunContext("run-attachment-fix", { sessionKey: childKey, agentId: "main" });
  expect(
    resolveVisibleActiveSessionRunState({
      storePath,
      requestedKey: childKey,
      canonicalKey: childKey,
      agentId: "main",
    }),
  ).toEqual({ active: false, runIds: [] });
  const claim = claimAgentRunContext(
    "run-attachment-fix",
    { sessionKey: childKey, agentId: "main" },
    { trackOwner: true, ownsContext: true },
  );

  try {
    expect(claim).toBeDefined();
    expect(visibleState(childKey, { agentId: "main" })).toEqual({
      active: true,
      runIds: ["run-attachment-fix"],
    });
    const releaseCapacityWait = registerAgentRunCapacityWait(
      "run-attachment-fix",
      getAgentRunLifecycleGeneration(),
    );
    try {
      expect(visibleState(childKey, { agentId: "main" })).toMatchObject({
        active: true,
        status: "queued",
      });
    } finally {
      releaseCapacityWait?.();
    }
    expect(visibleState(parentKey, { agentId: "main" })).toEqual({ active: false, runIds: [] });
    rotateAgentEventLifecycleGeneration();
    expect(visibleState(childKey, { agentId: "main" })).toEqual({ active: false, runIds: [] });
  } finally {
    releaseAgentRunContext("run-attachment-fix", claim);
    clearAgentRunContext("run-attachment-fix");
    await resetSubagentRegistryForTests({ persist: false });
    clearRuntimeConfigSnapshot();
  }
});

it("keeps terminal persistence visible only to chat history", async () => {
  const terminal = {
    sessionKey: "agent:main:main",
    sessionId: "session-main",
    projectSessionActive: false,
    projectSessionTerminalPending: true,
  };
  const terminalRef = createRpcSourceForTest(
    {
      projectSessionActive: terminal.projectSessionActive,
      projectSessionTerminalPending: terminal.projectSessionTerminalPending,
    },
    {
      sessionKey: terminal.sessionKey,
      storeScope: storePath,
      sessionId: terminal.sessionId,
      agentId: "main",
    },
  );
  rpcSourceTesting.reset([["run-terminal", terminalRef]]);
  releaseFixtureSources.push(() => retireSessionControllerInput(terminalRef.input));
  const params = {
    storePath,
    requestedKey: terminal.sessionKey,
    canonicalKey: terminal.sessionKey,
    sessionId: terminal.sessionId,
    agentId: "main",
  };

  expect(resolveVisibleActiveSessionRunState(params)).toEqual({ active: false, runIds: [] });
  expect(
    resolveVisibleActiveSessionRunState({ ...params, includeTerminalPersistence: true }),
  ).toEqual({ active: true });

  terminalRef.adapter.projectSessionTerminalPending = false;
  terminalRef.adapter.projectSessionTerminalPersisted = true;
  expect(
    resolveVisibleActiveSessionRunState({ ...params, includeTerminalPersistence: true }),
  ).toEqual({ active: false, runIds: [] });
});

it("keeps prebuilt active-run indexes in parity with per-row scans", async () => {
  rpcSourceTesting.reset(
    await activeSources([
      ["run-main", { sessionKey: "agent:main:main", sessionId: "session-main" }],
      ["run-global", { sessionKey: "global", agentId: "work" }],
      ["run-hidden", { sessionKey: "agent:main:hidden", projectSessionActive: false }],
    ]),
  );
  registerAgentRunContext("projected-key", {
    projectSessionActive: true,
    sessionKey: "agent:main:projected",
  });
  registerAgentRunContext("projected-id", {
    projectSessionActive: true,
    agentId: "main",
    sessionId: "session-projected",
  });
  try {
    const project = createVisibleActiveSessionRunProjector();
    const cases = [
      { requestedKey: "agent:main:main", canonicalKey: "agent:main:main" },
      { requestedKey: "agent:main:projected", canonicalKey: "agent:main:projected" },
      {
        requestedKey: "agent:main:by-id",
        canonicalKey: "agent:main:by-id",
        sessionId: "session-projected",
      },
      {
        requestedKey: "global",
        canonicalKey: "global",
        agentId: "work",
        defaultAgentId: "main",
      },
      { requestedKey: "agent:main:missing", canonicalKey: "agent:main:missing" },
    ];
    for (const activeCase of cases) {
      expect(project({ storePath, ...activeCase })).toEqual(
        resolveVisibleActiveSessionRunState({ storePath, ...activeCase }),
      );
    }
  } finally {
    clearAgentRunContext("projected-key");
    clearAgentRunContext("projected-id");
  }
});

it("matches session-id-only gateway runs during archive admission", async () => {
  rpcSourceTesting.reset(
    await activeSources([
      [
        "run-1",
        {
          sessionId: "session-1",
          controlUiVisible: true,
          projectSessionActive: true,
        },
      ],
    ]),
  );

  expect(
    resolveVisibleActiveSessionRunState({
      storePath,
      requestedKey: "agent:main:child",
      canonicalKey: "agent:main:child",
      sessionId: "session-1",
      defaultAgentId: "main",
    }).active,
  ).toBe(true);
});

it("returns deterministic protocol IDs for matching exact sources", async () => {
  rpcSourceTesting.reset(
    await activeSources([
      [
        "run-z",
        {
          sessionKey: "main",
          sessionId: "selected",
          storeScope: storePath,
        },
      ],
      [
        "run-a",
        {
          sessionKey: "main",
          sessionId: "selected",
          storeScope: storePath,
          queued: true,
        },
      ],
      [
        "run-hidden",
        { sessionKey: "main", sessionId: "selected", controlUiVisible: false, queued: true },
      ],
      ["run-other", { sessionKey: "other" }],
    ]),
  );

  expect(
    resolveVisibleActiveSessionRunState({
      storePath,
      requestedKey: "main",
      canonicalKey: "main",
      agentId: "main",
      defaultAgentId: "main",
    }),
  ).toEqual({ active: true, runIds: ["run-a", "run-z"] });
});

it("projects a lifecycle-owned worker run without widening event visibility", async () => {
  registerAgentRunContext("worker-run", {
    isControlUiVisible: false,
    projectSessionActive: true,
    sessionId: "worker-session",
    sessionKey: "agent:main:worker",
  });
  try {
    expect(
      visibleState("agent:main:worker", {
        sessionId: "worker-session",
      }),
    ).toEqual({ active: true });
  } finally {
    clearAgentRunContext("worker-run");
  }
});

it("projects reply lifecycle state and admits the next backend after producer completion", async () => {
  const sessionKey = "agent:main:reply-settling";
  const sessionId = "reply-settling-session";
  const project = createVisibleActiveSessionRunProjector();
  const state = () =>
    project({ storePath, requestedKey: sessionKey, canonicalKey: sessionKey, sessionId });
  const operation = createReplyOperation({ sessionKey, sessionId, resetTriggered: false });
  const replacementHandle: EmbeddedAgentQueueHandle = {
    abort: () => undefined,
    isAborted: () => false,
    isCompacting: () => false,
    isStreaming: () => true,
    queueMessage: async () => undefined,
  };
  try {
    expect(state()).toEqual({ active: true });

    operation.markWaitingForGlobalLane();
    expect(state()).toEqual({ active: true });
    operation.markGlobalLaneWaitEnded();

    operation.setPhase("running");
    operation.markWaitingForGlobalLane();
    expect(state()).toEqual({ active: true });
    operation.markGlobalLaneWaitEnded();
    markReplyOperationExecutionStarted(operation);
    expect(state()).toEqual({ active: true });
    operation.markWaitingForGlobalLane();
    expect(state()).toEqual({ active: true });
    operation.markGlobalLaneWaitEnded();
    expect(operation.abortByUser()).toBe(true);
    expect(isSessionRunActive(sessionId)).toBe(true);
    expect(visibleState(sessionKey, { sessionId })).toEqual({ active: false, runIds: [] });

    operation.complete();
    await operation.ownerSettlement;
    setActiveEmbeddedRun(sessionId, replacementHandle, sessionKey);
    expect(state()).toEqual({ active: true });
  } finally {
    clearActiveEmbeddedRun(sessionId, replacementHandle, sessionKey);
    operation.complete();
  }
});

it("preserves an independent lifecycle-owned worker while a reply operation settles", async () => {
  const sessionKey = "agent:main:worker-overlap";
  const sessionId = "worker-overlap-session";
  const operation = createReplyOperation({ sessionKey, sessionId, resetTriggered: false });
  registerAgentRunContext("worker-overlap-run", {
    projectSessionActive: true,
    sessionId,
    sessionKey,
  });
  try {
    expect(visibleState(sessionKey, { sessionId })).toEqual({ active: true });
  } finally {
    operation.complete();
    clearAgentRunContext("worker-overlap-run");
  }
});

it("does not project an aborted embedded handle retained for cleanup as active", async () => {
  const sessionKey = "agent:main:handle-settling";
  const sessionId = "handle-settling-session";
  let aborted = false;
  const handle: EmbeddedAgentQueueHandle = {
    abort: () => {
      aborted = true;
    },
    isAborted: () => aborted,
    isCompacting: () => false,
    // Prompt completion closes steering before post-turn finalization. That
    // state alone must not make a normally finishing run disappear.
    isStopped: () => true,
    isStreaming: () => false,
    queueMessage: async () => undefined,
  };
  setActiveEmbeddedRun(sessionId, handle, sessionKey);
  try {
    expect(visibleState(sessionKey, { sessionId })).toEqual({ active: true });

    expect(abortEmbeddedAgentRun(sessionId)).toBe(true);
    expect(isSessionRunActive(sessionId)).toBe(true);
    expect(visibleState(sessionKey, { sessionId })).toEqual({ active: false, runIds: [] });

    const source = createRpcSourceForTest(
      {},
      { storeScope: storePath, sessionId, sessionKey, accepted: true },
    );
    rpcSourceTesting.reset([["new-run", source]]);
    let admitted = false;
    const successor = claimRpcSourceForTest(source).then((release) => {
      admitted = true;
      releaseFixtureSources.push(release);
    });
    await Promise.resolve();
    expect(admitted).toBe(false);
    expect(
      resolveVisibleActiveSessionRunState({
        storePath,
        requestedKey: sessionKey,
        canonicalKey: sessionKey,
        sessionId,
      }),
    ).toEqual({ active: true, runIds: ["new-run"], status: "queued" });
    clearActiveEmbeddedRun(sessionId, handle, sessionKey);
    await successor;
    expect(
      resolveVisibleActiveSessionRunState({
        storePath,
        requestedKey: sessionKey,
        canonicalKey: sessionKey,
        sessionId,
      }),
    ).toEqual({ active: true, runIds: ["new-run"] });
  } finally {
    clearActiveEmbeddedRun(sessionId, handle, sessionKey);
  }
});

it("counts settled but still registered chat runs for a session key", async () => {
  rpcSourceTesting.reset(
    await activeSources([
      [
        "run-finalizing",
        {
          sessionKey: "agent:main:main",
          sessionId: "session-main",
          projectSessionActive: false,
          controlUiVisible: false,
        },
      ],
      ["run-global-work", { sessionKey: "global", agentId: "work" }],
    ]),
  );

  expect(
    hasRegisteredChatRunForSessionKey({
      storePath,
      sessionKey: "agent:main:main",
      agentId: undefined,
    }),
  ).toBe(true);
  expect(
    hasRegisteredChatRunForSessionKey({
      storePath,
      sessionKey: "agent:other:other",
      agentId: undefined,
    }),
  ).toBe(false);
  expect(
    hasRegisteredChatRunForSessionKey({ storePath, sessionKey: "global", agentId: "work" }),
  ).toBe(true);
  expect(
    hasRegisteredChatRunForSessionKey({ storePath, sessionKey: "global", agentId: "other" }),
  ).toBe(false);
  expect(
    hasRegisteredChatRunForSessionKey({ storePath, sessionKey: "global", agentId: undefined }),
  ).toBe(false);
});

it("matches colliding bare active runs by captured owner", async () => {
  rpcSourceTesting.reset(
    await activeSources([
      ["run-ops", { sessionKey: "incident-42", agentId: "ops" }],
      ["run-research", { sessionKey: "incident-42", agentId: "research" }],
    ]),
  );

  expect(
    resolveVisibleActiveSessionRunState({
      storePath,
      requestedKey: "incident-42",
      canonicalKey: "incident-42",
      agentId: "ops",
      defaultAgentId: "ops",
    }),
  ).toEqual({ active: true, runIds: ["run-ops"] });
  expect(
    resolveVisibleActiveSessionRunState({
      storePath,
      requestedKey: "incident-42",
      canonicalKey: "incident-42",
      agentId: "research",
      defaultAgentId: "ops",
    }),
  ).toEqual({ active: true, runIds: ["run-research"] });
});

it("keeps projected bare runs agent-scoped", async () => {
  registerAgentRunContext("projected-ops", {
    projectSessionActive: true,
    sessionKey: "incident-42",
    sessionId: "shared-id",
    agentId: "ops",
  });
  try {
    const index = buildProjectedAgentRunIndex();
    expect(
      visibleState("incident-42", {
        sessionId: "shared-id",
        agentId: "research",
        projectedAgentRunIndex: index,
      }).active,
    ).toBe(false);
    expect(
      visibleState("incident-42", {
        sessionId: "shared-id",
        agentId: "ops",
        projectedAgentRunIndex: index,
      }).active,
    ).toBe(true);
  } finally {
    clearAgentRunContext("projected-ops");
  }
});

it("resolves projected ownerless bare runs through the stable default owner", async () => {
  registerAgentRunContext("projected-ownerless", {
    projectSessionActive: true,
    sessionKey: "incident-42",
    sessionId: "ownerless-id",
  });
  try {
    const index = buildProjectedAgentRunIndex();
    expect(
      visibleState("incident-42", {
        sessionId: "ownerless-id",
        agentId: "ops",
        defaultAgentId: "ops",
        projectedAgentRunIndex: index,
      }).active,
    ).toBe(true);
    expect(
      visibleState("incident-42", {
        sessionId: "ownerless-id",
        agentId: "research",
        defaultAgentId: "ops",
        projectedAgentRunIndex: index,
      }).active,
    ).toBe(false);
  } finally {
    clearAgentRunContext("projected-ownerless");
  }
});

it.each(["agent:main:command", "global"])(
  "keeps an adopted reply's global alias with its captured agent (source=%s)",
  async (sourceKey) => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const sessionId = "adopted-global-session";
      const adoptedStorePath = state.statePath("adopted-global.sqlite");
      const operation = createControllerOperation({
        sessionKey: sourceKey,
        sessionId,
        agentId: "main",
        resetTriggered: false,
        target: captureSessionTarget({
          storeScope: adoptedStorePath,
          sessionKey: sourceKey,
          incarnation: sessionId,
          agentId: "main",
        }),
      });
      try {
        const admission = await admitReplyTurn({
          storePath: adoptedStorePath,
          sessionKey: "global",
          sessionId,
          agentId: "ops",
          kind: "visible",
          resetTriggered: false,
          adoptOperation: operation,
        });
        expect(admission.status).toBe("owned");
        for (const agentId of ["main", "ops"]) {
          expect(
            resolveVisibleActiveSessionRunState({
              storePath: adoptedStorePath,
              requestedKey: `agent:${agentId}:main`,
              canonicalKey: "global",
              sessionId,
              agentId,
              defaultAgentId: "main",
            }).active,
          ).toBe(agentId === "ops");
        }
      } finally {
        operation.complete();
        await operation.ownerSettlement;
      }
    });
  },
);

it.each(["store", "Gateway"] as const)(
  "excludes another %s's source with the same logical session",
  async (dimension) => {
    const sessionKey = "agent:main:scoped-projection";
    const sessionId = "scoped-projection-session";
    const source = createRpcSourceForTest(
      {},
      {
        sessionKey,
        sessionId,
        agentId: "main",
        storeScope: dimension === "store" ? `${storePath}-other` : storePath,
      },
    );
    rpcSourceTesting.reset([["foreign-run", source]]);
    releaseFixtureSources.push(await claimRpcSourceForTest(source));
    if (dimension === "Gateway") {
      const resolver = () => undefined;
      bindGatewayContextResolver(source.input, resolver);
      bindGatewayContextResolver(source.input.claim!.operation!, resolver);
    }
    expect(visibleState(sessionKey, { sessionId })).toEqual({ active: false, runIds: [] });
    expect(hasRegisteredChatRunForSessionKey({ storePath, sessionKey, agentId: "main" })).toBe(
      false,
    );
  },
);

it("projects only recorded capacity waits as queued and preserves independent running owners", async () => {
  const sessionKey = "agent:main:capacity-wait";
  const sessionId = "capacity-wait-session";
  const runId = "capacity-wait-run";
  registerAgentRunContext(runId, { sessionKey, sessionId, agentId: "main" });
  rpcSourceTesting.clear();
  const registration = registerChatAbortController({
    target: captureSessionTarget({
      storeScope: storePath,
      sessionKey,
      incarnation: sessionId,
      agentId: "main",
    }),
    runId,
    sessionKey,
    sessionId,
    agentId: "main",
    timeoutMs: 60_000,
    kind: "agent",
  });
  onTestFinished(() => registration.cleanup());
  registration.initializeTiming();
  const state = () =>
    resolveVisibleActiveSessionRunState({
      storePath,
      requestedKey: sessionKey,
      canonicalKey: sessionKey,
      sessionId,
    });
  if (!registration.entry) {
    throw new Error("Missing RPC source");
  }
  releaseFixtureSources.push(await claimRpcSourceForTest(registration.entry));
  const releaseWait = registerAgentRunCapacityWait(runId, getAgentRunLifecycleGeneration());
  try {
    expect(state()).toEqual({ active: true, status: "queued", runIds: [runId] });
    registration.markExecutionStarted();
    expect(state()).toEqual({ active: true, status: "queued", runIds: [runId] });
    registerAgentRunContext("capacity-independent", {
      sessionKey,
      sessionId,
      projectSessionActive: true,
    });
    expect(state()).toEqual({ active: true });
    clearAgentRunContext("capacity-independent");
    expect(state()).toEqual({ active: true, status: "queued", runIds: [runId] });
    releaseWait?.();
    expect(state()).toEqual({ active: true, runIds: [runId] });
  } finally {
    releaseWait?.();
    registration.cleanup();
    clearAgentRunContext(runId);
    clearAgentRunContext("capacity-independent");
  }
});

it.each([
  { isControlUiVisible: false },
  { projectSessionActive: false },
  { projectSessionLifecycle: false },
])("capacity evidence does not make a hidden maintenance run visible: %j", (visibility) => {
  const runId = "hidden-capacity";
  const sessionKey = "agent:main:hidden-capacity";
  registerAgentRunContext(runId, { sessionKey, ...visibility });
  const releaseWait = registerAgentRunCapacityWait(runId, getAgentRunLifecycleGeneration());
  try {
    expect(visibleState(sessionKey, {})).toEqual({ active: false, runIds: [] });
  } finally {
    releaseWait?.();
    clearAgentRunContext(runId);
  }
});

it("preserves the completed foreground row while hidden embedded maintenance remains operational", async () => {
  const sessionKey = "agent:main:hidden-maintenance-handle";
  const sessionId = "hidden-maintenance-session";
  const runId = "hidden-maintenance-run";
  let aborted = false;
  const handle: EmbeddedAgentQueueHandle = {
    runId,
    abort: () => {
      aborted = true;
    },
    isAborted: () => aborted,
    isCompacting: () => false,
    isStreaming: () => true,
    queueMessage: async () => undefined,
  };
  registerAgentRunContext(runId, {
    sessionKey,
    sessionId,
    projectSessionActive: false,
    projectSessionLifecycle: false,
    projectSessionMessages: false,
    isControlUiVisible: false,
  });
  setActiveEmbeddedRun(sessionId, handle, sessionKey);
  const state = () =>
    visibleState(sessionKey, {
      sessionId,
    });
  const snapshot = () =>
    buildGatewaySessionSnapshot({
      sessionRow: {
        key: sessionKey,
        kind: "direct",
        updatedAt: 20,
        sessionId,
        lastRunId: "completed-foreground",
        status: "done",
        runtimeMs: 273_418,
      },
      activeRunState: state(),
      includeSession: true,
    });
  let settled = false;
  const cleanup = waitForSessionRunEnd(sessionId, null).then((ended) => {
    settled = true;
    return ended;
  });
  try {
    expect.soft(snapshot()).toMatchObject({
      status: "done",
      hasActiveRun: false,
      lastRunId: "completed-foreground",
      runtimeMs: 273_418,
      session: { status: "done", hasActiveRun: false },
    });
    expect(isSessionRunActive(sessionId)).toBe(true);
    expect(resolveSessionRunProgressState(sessionId)).toBe("running");
    clearAgentRunContext(runId);
    expect.soft(state()).toEqual({ active: false, runIds: [] });
    expect(resolveSessionRunProgressState(sessionId)).toBe("running");
    expect(abortEmbeddedAgentRun(sessionId)).toBe(true);
    expect(aborted).toBe(true);
    expect(isSessionRunActive(sessionId)).toBe(true);
    expect(resolveSessionRunProgressState(sessionId)).toBeUndefined();
    await Promise.resolve();
    expect(settled).toBe(false);
  } finally {
    clearActiveEmbeddedRun(sessionId, handle, sessionKey);
    clearAgentRunContext(runId);
    await expect(cleanup).resolves.toBe(true);
  }
});

it.each(["reply", "remote"] as const)(
  "preserves visible %s activity without overlapping hidden controller ownership",
  async (kind) => {
    const sessionKey = `agent:main:hidden-with-${kind}`;
    const sessionId = `hidden-with-${kind}`;
    const runId = `hidden-${kind}`;
    const visibleRunId = `visible-${kind}`;
    const handle: EmbeddedAgentQueueHandle = {
      runId,
      abort: () => {},
      isCompacting: () => false,
      isStreaming: () => true,
      queueMessage: async () => undefined,
    };
    registerAgentRunContext(runId, { sessionKey, sessionId, projectSessionActive: false });
    setActiveEmbeddedRun(sessionId, handle, sessionKey);
    let reply: ReturnType<typeof createReplyOperation> | undefined;
    if (kind === "reply") {
      expect(() => createReplyOperation({ sessionKey, sessionId, resetTriggered: false })).toThrow(
        "already active",
      );
      clearActiveEmbeddedRun(sessionId, handle, sessionKey);
    }
    if (kind === "reply") {
      reply = createReplyOperation({ sessionKey, sessionId, resetTriggered: false });
      reply.setPhase("running");
      markReplyOperationExecutionStarted(reply);
    } else {
      registerAgentRunContext(visibleRunId, { sessionKey, sessionId, projectSessionActive: true });
    }
    const state = () =>
      visibleState(sessionKey, {
        sessionId,
      });
    try {
      expect(state().active).toBe(true);
      reply?.complete();
      clearAgentRunContext(visibleRunId);
      expect(state()).toEqual({ active: false, runIds: [] });
      expect(resolveSessionRunProgressState(sessionId)).toBe(
        kind === "reply" ? undefined : "running",
      );
    } finally {
      reply?.complete();
      clearAgentRunContext(visibleRunId);
      clearActiveEmbeddedRun(sessionId, handle, sessionKey);
      clearAgentRunContext(runId);
    }
  },
);

it("keeps embedded activity independent of suppressed events", () => {
  const sessionKey = "agent:main:suppressed-events";
  const sessionId = "suppressed-events";
  const runId = "suppressed-events-run";
  const handle: EmbeddedAgentQueueHandle = {
    runId,
    abort: () => {},
    isCompacting: () => false,
    isStreaming: () => true,
    queueMessage: async () => undefined,
  };
  registerAgentRunContext(runId, {
    sessionKey,
    sessionId,
    projectSessionActive: true,
    projectSessionLifecycle: false,
    projectSessionMessages: false,
    isControlUiVisible: false,
  });
  setActiveEmbeddedRun(sessionId, handle, sessionKey);
  try {
    clearAgentRunContext(runId);
    expect(visibleState(sessionKey, { sessionId })).toEqual({ active: true });
  } finally {
    clearActiveEmbeddedRun(sessionId, handle, sessionKey);
    clearAgentRunContext(runId);
  }
});
