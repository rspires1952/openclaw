import { describe, expect, it } from "vitest";
import {
  makeSettledChild,
  REQUESTER,
} from "../announce/subagent-announce.requester-settle-wake.test-support.js";
import {
  buildRequesterSettleWakeIdentity,
  isRequesterSettleWakeForRun,
} from "./subagent-requester-settle-identity.js";

describe("requester settle wake identity", () => {
  // The control scope must recognize exactly the key dispatch used, or a settle
  // turn loses authority over its own children.
  it.each([
    { name: "private batch", yieldedFinalDeliverable: undefined, pause: false },
    { name: "deliverable yielded batch", yieldedFinalDeliverable: true as const, pause: false },
    { name: "paused private batch", yieldedFinalDeliverable: undefined, pause: true },
  ])("matches the dispatched key: $name", ({ yieldedFinalDeliverable, pause }) => {
    const entry = makeSettledChild({
      runId: "run-b",
      requesterAgentId: "main",
      completionTarget: "parent",
      ...(pause ? { pauseReason: "sessions_yield" as const } : {}),
      requesterSettleWake: {
        status: "pending",
        attemptCount: 0,
        batchRunIds: ["run-b"],
        rearmGeneration: 1,
        ...(pause ? { pauseNotice: { acknowledgment: "Waiting for direction" } } : {}),
        ...(yieldedFinalDeliverable ? { yieldedFinalDeliverable } : {}),
      },
    });
    const dispatched = buildRequesterSettleWakeIdentity({
      requesterSessionKey: REQUESTER,
      requesterAgentId: "main",
      batchRunIds: ["run-b"],
      rearmGeneration: 1,
      pause,
    }).runId;
    expect(
      isRequesterSettleWakeForRun({
        entry,
        runId: dispatched,
        requesterSessionKey: REQUESTER,
        requesterAgentId: "main",
        runsById: new Map([["run-b", entry]]),
      }),
    ).toBe(true);
  });

  it("recognizes an admitted frozen wake after an earlier member retires", () => {
    const entry = makeSettledChild({
      runId: "run-b",
      requesterAgentId: "main",
      requesterSettleWake: {
        status: "dispatching",
        batchRunIds: ["retired-run", "run-b"],
        rearmGeneration: 1,
        attemptCount: 1,
      },
    });
    expect(
      isRequesterSettleWakeForRun({
        entry,
        runId: "announce:requester-settle:main:agent:main:main:run-b:yield-1",
        requesterSessionKey: REQUESTER,
        requesterAgentId: "main",
        runsById: new Map([[entry.runId, entry]]),
      }),
    ).toBe(true);
  });

  it.each(["requester", "agent", "store", "generation"] as const)(
    "excludes a frozen member rebound to another %s",
    (difference) => {
      const entry = makeSettledChild({
        runId: "run-b",
        requesterAgentId: "main",
        requesterStorePath: "/synthetic/requester.sqlite",
        requesterSettleWake: {
          status: "pending",
          attemptCount: 0,
          batchRunIds: ["run-b", "run-other"],
          rearmGeneration: 1,
        },
      });
      const other = makeSettledChild({
        ...entry,
        runId: "run-other",
        ...(difference === "requester" ? { requesterSessionKey: "other" } : {}),
        ...(difference === "agent" ? { requesterAgentId: "other" } : {}),
        ...(difference === "store" ? { requesterStorePath: "/synthetic/other.sqlite" } : {}),
        requesterSettleWake: {
          ...entry.requesterSettleWake,
          status: "pending",
          attemptCount: 0,
          rearmGeneration: difference === "generation" ? 2 : 1,
        },
      });
      expect(
        isRequesterSettleWakeForRun({
          entry,
          runId: "announce:requester-settle:main:agent:main:main:run-b:yield-1",
          requesterSessionKey: REQUESTER,
          requesterAgentId: "main",
          runsById: new Map([
            [entry.runId, entry],
            [other.runId, other],
          ]),
        }),
      ).toBe(true);
    },
  );
});
