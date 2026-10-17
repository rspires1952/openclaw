import { afterEach, expect, it } from "vitest";
import type { ReplyOperation } from "../../../sessions/session-controller.contracts.js";
import { captureSessionTarget } from "../../../sessions/session-controller.lifecycle.js";
import { createReplyOperation } from "../../../sessions/session-controller.operation.js";
import { resetSessionControllerStateForTest } from "../../../sessions/session-lifecycle-admission.test-support.js";
import { createSubagentRunRecord } from "../../subagent-test-fixtures.test-helpers.js";
import { isClaimedByLiveRequesterTurn } from "./subagent-requester-turn-liveness.js";

const storePath = "/private/tmp/requester-turn-liveness/main/sessions.json";
const operations: ReplyOperation[] = [];

afterEach(async () => {
  for (const operation of operations) {
    operation.complete();
  }
  await Promise.all(operations.map((operation) => operation.ownerSettlement));
  operations.length = 0;
  resetSessionControllerStateForTest();
});

function requesterTurn() {
  const operation = createReplyOperation({
    sessionKey: "global",
    sessionId: "requester-session",
    agentId: "main",
    resetTriggered: false,
    target: captureSessionTarget({
      storeScope: storePath,
      sessionKey: "global",
      incarnation: "requester-session",
      agentId: "main",
    }),
  });
  operations.push(operation);
  operation.setPhase("running");
  const backend = { kind: "embedded" as const, runId: "requester-turn", cancel: () => {} };
  operation.attachBackend(backend);
  return { operation, backend };
}

function child() {
  return createSubagentRunRecord({
    runId: "liveness-child",
    childSessionKey: "agent:main:subagent:liveness-child",
    requesterSessionKey: "global",
    requesterAgentId: "main",
    requesterStorePath: storePath,
    requesterTurnRunId: "requester-turn",
    expectsCompletionMessage: true,
  });
}

// Settlement runs after the last attempt detaches its backend, before the operation settles.
it("keeps the physical requester live through backend detach and yield until its turn settles", () => {
  const { operation, backend } = requesterTurn();
  const entry = child();

  expect(isClaimedByLiveRequesterTurn(entry)).toBe(true);
  operation.detachBackend(backend);
  expect(isClaimedByLiveRequesterTurn(entry)).toBe(true);
  operation.yield();
  expect(isClaimedByLiveRequesterTurn(entry)).toBe(true);
  operation.complete();
  expect(isClaimedByLiveRequesterTurn(entry)).toBe(false);
});

it.each(["store", "key", "agent", "turn", "no obligation"] as const)(
  "does not borrow live requester ownership across a different %s",
  (difference) => {
    requesterTurn();
    const entry = child();
    if (difference === "store") {
      entry.requesterStorePath = "/private/tmp/requester-turn-liveness/other/sessions.json";
    } else if (difference === "key") {
      entry.requesterSessionKey = "other-requester";
    } else if (difference === "agent") {
      entry.requesterAgentId = "research";
    } else if (difference === "turn") {
      entry.requesterTurnRunId = "other-turn";
    } else {
      entry.expectsCompletionMessage = false;
    }

    expect(isClaimedByLiveRequesterTurn(entry)).toBe(false);
  },
);

it("does not retain a claim when the recorded requester turn is absent", () => {
  const entry = child();
  expect(isClaimedByLiveRequesterTurn(entry)).toBe(false);

  requesterTurn();
  entry.requesterTurnRunId = undefined;
  expect(isClaimedByLiveRequesterTurn(entry)).toBe(false);
});
