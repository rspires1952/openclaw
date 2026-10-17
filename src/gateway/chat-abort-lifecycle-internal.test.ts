import { afterEach, expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import { captureSessionTarget } from "../sessions/session-controller.lifecycle.js";
import {
  captureSessionControllerSourceSettlement,
  trackSessionControllerSourceWork,
} from "../sessions/session-controller.mailbox.js";
import { getRpcSourceIdentity } from "../sessions/session-controller.rpc-sources.js";
import {
  resetSessionControllerStateForTest,
  rpcSourceTesting,
} from "../sessions/session-lifecycle-admission.test-support.js";
import {
  bindChatAbortTerminalDispatch,
  captureChatAbortRegistrationGuard,
  markChatAbortTerminalPersistenceError,
  waitForChatAbortControllerRemoval,
  waitForChatAbortTerminalPersistence,
} from "./chat-abort-lifecycle-internal.js";
import { abortChatRunById, registerChatAbortController } from "./chat-abort.js";
import { createChatRunState } from "./server-chat-state.js";

afterEach(() => resetSessionControllerStateForTest());

function registeredRun(
  options: {
    runId?: string;
    sessionKey?: string;
    sessionId?: string;
    onRemoved?: () => void;
    sourceWork?: Promise<unknown>;
  } = {},
) {
  const runId = options.runId ?? "terminal-drain";
  const sessionKey = options.sessionKey ?? "agent:main:terminal";
  const sessionId = options.sessionId ?? "terminal-session";
  const registration = registerChatAbortController({
    target: captureSessionTarget({
      storeScope: `/synthetic/${runId}/${sessionKey}/sessions`,
      sessionKey,
      incarnation: sessionId,
    }),
    runId,
    sessionId,
    sessionKey,
    timeoutMs: 60_000,
    onRemoved: options.onRemoved,
    sourceWork: options.sourceWork,
  });
  if (!registration.entry) {
    throw new Error("Expected a registered run");
  }
  const entry = registration.entry;
  const drain = () =>
    waitForChatAbortControllerRemoval({
      targets: [{ runId, entry }],
      timeoutMs: 1_000,
    });
  return { runId, entry, registration, drain };
}

it.each(
  ["settled", "pending", "writing", "failed"].flatMap((state) =>
    [false, true].map((alreadyRemoved) => ({ state, alreadyRemoved })),
  ),
)(
  "checks $state terminal ownership when alreadyRemoved=$alreadyRemoved",
  async ({ state, alreadyRemoved }) => {
    const { runId, entry, drain } = registeredRun();
    if (state === "pending") {
      entry.adapter.projectSessionTerminalPending = true;
    } else if (state === "writing") {
      entry.adapter.projectSessionTerminalPersistence = new Promise<void>(() => {});
    } else if (state === "failed") {
      markChatAbortTerminalPersistenceError(entry, new Error("terminal write failed"));
    }
    if (alreadyRemoved) {
      rpcSourceTesting.deleteExpected(runId, entry);
    }
    const result = drain();
    rpcSourceTesting.deleteExpected(runId, entry);
    expect(await result).toBe(state === "settled");
  },
);

it("finishes an empty selection without draining unrelated registrations", async () => {
  const { runId } = registeredRun();
  expect(await waitForChatAbortControllerRemoval({ targets: [], timeoutMs: 1_000 })).toBe(true);
  expect(rpcSourceTesting.has(runId)).toBe(true);
});

it("releases the reserved terminal owner when no lifecycle subscriber adopts it", async () => {
  const { runId, entry, drain } = registeredRun();
  const result = drain();
  expect(
    abortChatRunById(
      {
        chatRunState: createChatRunState(),
        removeChatRun: () => undefined,
        agentRunSeq: new Map(),
        broadcast: () => {},
        nodeSendToSession: () => {},
      },
      { runId, sessionKey: getRpcSourceIdentity(entry).sessionKey },
    ),
  ).toEqual({ aborted: true });
  expect(await result).toBe(true);
  expect(rpcSourceTesting.has(runId)).toBe(false);
});

it("marks captured cancellation without publishing over a replacement", () => {
  const { runId, entry } = registeredRun();
  let replacement: ReturnType<typeof registeredRun> | undefined;
  entry.input.abortSignal.addEventListener("abort", () => {
    rpcSourceTesting.deleteExpected(runId, entry);
    replacement = registeredRun({ runId, sessionKey: "agent:main:replacement" });
  });
  const broadcast = vi.fn();
  expect(
    abortChatRunById(
      {
        chatRunState: createChatRunState(),
        removeChatRun: () => undefined,
        agentRunSeq: new Map(),
        broadcast,
        nodeSendToSession: () => {},
      },
      { runId, sessionKey: getRpcSourceIdentity(entry).sessionKey },
    ),
  ).toEqual({ aborted: true });
  expect(broadcast).not.toHaveBeenCalled();
  expect(entry.input.abortSignal.aborted).toBe(true);
  expect(entry.adapter.terminalOutcomeObserved).toBe(true);
  expect(replacement).toBeDefined();
  expect(replacement?.entry.input.abortSignal.aborted).toBe(false);
  expect(replacement?.entry.adapter.terminalOutcomeObserved).toBeUndefined();
  expect(rpcSourceTesting.get(runId)).toBe(replacement?.entry);
});

it("allows a retired terminal publisher only until another controller reuses its run id", () => {
  const original = registeredRun();
  const isCurrent = captureChatAbortRegistrationGuard([
    { runId: original.runId, entry: original.entry },
  ]);
  expect(isCurrent()).toBe(true);
  rpcSourceTesting.deleteExpected(original.runId, original.entry);
  expect(isCurrent()).toBe(true);
  registeredRun({ runId: original.runId, sessionKey: "agent:other:replacement" });
  expect(isCurrent()).toBe(false);
});

it.each(["fulfilled", "rejected"] as const)(
  "drains a promise-only registration after it is %s",
  async (outcome) => {
    const onRemoved = vi.fn();
    const { runId, entry, registration, drain } = registeredRun({ onRemoved });
    const persistence = createDeferred();
    entry.adapter.projectSessionTerminalPersistence = persistence.promise;
    const result = drain();
    registration.cleanup();
    expect(rpcSourceTesting.get(runId)).toBe(entry);
    expect(onRemoved).not.toHaveBeenCalled();
    const failure = new Error("terminal write failed");
    const sourceSettlement = captureSessionControllerSourceSettlement(entry.input);
    const sourceOutcome =
      outcome === "fulfilled"
        ? expect(sourceSettlement).resolves.toBeUndefined()
        : expect(sourceSettlement).rejects.toBe(failure);
    if (outcome === "fulfilled") {
      persistence.resolve();
    } else {
      persistence.reject(failure);
    }
    await sourceOutcome;
    expect(await result).toBe(outcome === "fulfilled");
    if (outcome === "rejected") {
      await expect(waitForChatAbortTerminalPersistence(entry)).rejects.toBe(failure);
    }
    expect(rpcSourceTesting.has(runId)).toBe(false);
    expect(onRemoved).toHaveBeenCalledOnce();
  },
);

it.each(["fulfilled", "rejected"] as const)(
  "does not retire a replacement persistence owner when an older write is %s",
  async (outcome) => {
    const { entry, registration } = registeredRun();
    const previous = createDeferred();
    const current = createDeferred();
    entry.adapter.projectSessionTerminalPersistence = previous.promise;
    registration.cleanup();
    entry.adapter.projectSessionTerminalPersistence = current.promise;
    if (outcome === "fulfilled") {
      previous.resolve();
    } else {
      previous.reject(new Error("older terminal write failed"));
    }
    await previous.promise.catch(() => {});
    await Promise.resolve();
    expect(entry.adapter.projectSessionTerminalPersistence).toBe(current.promise);
    const joined = waitForChatAbortTerminalPersistence(entry);
    current.resolve();
    await expect(joined).resolves.toBeUndefined();
  },
);

it.each(["settled", "pending", "writing", "failed", "dispatch-failed"] as const)(
  "retains %s terminal ownership while joining the raw producer",
  async (state) => {
    const producer = createDeferred();
    const { entry, registration } = registeredRun({ sourceWork: producer.promise });
    const write = createDeferred();
    if (state === "pending") {
      entry.adapter.projectSessionTerminalPending = true;
    }
    if (state === "writing") {
      entry.adapter.projectSessionTerminalPersistence = write.promise;
    }
    if (state === "failed") {
      markChatAbortTerminalPersistenceError(entry, new Error("write failed"));
    }
    if (state === "dispatch-failed") {
      bindChatAbortTerminalDispatch([entry], Promise.resolve(), {
        failure: { error: new Error("terminal dispatch failed") },
      });
    }
    registration.cleanup();
    let drained = false;
    const settled = captureSessionControllerSourceSettlement(entry.input).then(() => {
      drained = true;
    });
    await Promise.resolve();
    expect(drained).toBe(false);
    entry.adapter.projectSessionTerminalPending = false;
    entry.adapter.projectSessionTerminalPersistence = undefined;
    markChatAbortTerminalPersistenceError(entry, undefined);
    write.resolve();
    producer.resolve();
    await settled;
    expect(drained).toBe(true);
  },
);

it("waits for every selected source without draining unrelated registrations", async () => {
  const first = registeredRun({ runId: "first-tail" });
  const sibling = registeredRun({ runId: "sibling-tail" });
  const unrelated = registeredRun({ runId: "unrelated-tail" });
  const producer = createDeferred();
  trackSessionControllerSourceWork(sibling.entry.input, producer.promise);
  let drained = false;
  const result = waitForChatAbortControllerRemoval({
    targets: [
      { runId: first.runId, entry: first.entry },
      { runId: sibling.runId, entry: sibling.entry },
    ],
    timeoutMs: 1_000,
  }).then((settled) => {
    drained = true;
    return settled;
  });
  first.registration.cleanup();
  sibling.registration.cleanup();
  await first.entry.input.settlement.promise;
  expect(drained).toBe(false);
  expect(rpcSourceTesting.get(unrelated.runId)).toBe(unrelated.entry);
  producer.resolve();
  expect(await result).toBe(true);
  expect(rpcSourceTesting.has(first.runId)).toBe(false);
  expect(rpcSourceTesting.has(sibling.runId)).toBe(false);
  expect(rpcSourceTesting.get(unrelated.runId)).toBe(unrelated.entry);
  unrelated.registration.cleanup();
});
