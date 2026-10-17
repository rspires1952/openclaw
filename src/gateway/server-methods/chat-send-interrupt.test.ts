import { describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { registerAgentSessionLoopTestLifecycle } from "../../agents/sessions/agent-session-loop-correctness.test-support.js";
import { installGatewayTestHooks } from "../test-helpers.js";
import { useBrowserFollowupFixture } from "./chat-send-pending-inputs.test-support.js";
import type { RespondFn } from "./types.js";

installGatewayTestHooks();
registerAgentSessionLoopTestLifecycle();
const createFixture = useBrowserFollowupFixture();

describe("chat interrupt acknowledgement", () => {
  it("acknowledges an interrupt while a finalizing owner refuses cancellation", async () => {
    const fixture = await createFixture();
    const active = fixture.activeRun;
    if (!active) {
      throw new Error("Expected an active fixture run");
    }
    const cleanup = createDeferred();
    active.registerExecutionCleanup(() => cleanup.promise);
    const settled = vi.fn();
    void active.ownerSettlement.then(settled);
    const cancel = vi.fn();
    active.attachBackend({ kind: "embedded", cancel });
    active.freezeAbort();
    fixture.params.queueMode = "interrupt";
    const respond = vi.fn<RespondFn>();
    let sending: Promise<unknown> | undefined;
    try {
      sending = fixture.send(respond);
      await sending;
      expect(cancel).not.toHaveBeenCalled();
      expect(active.abortSignal.aborted).toBe(false);
      expect(settled).not.toHaveBeenCalled();
      expect(respond.mock.calls[0]?.[0]).toBe(true);
      const payload = respond.mock.calls[0]?.[1];
      expect(payload).toMatchObject({ status: "started" });
      expect(payload).not.toHaveProperty("interruptedActiveRun");
    } finally {
      cleanup.resolve();
      active.complete();
      fixture.releaseDispatch();
      await sending;
      await fixture.cleanup();
    }
  }, 10_000);
});
