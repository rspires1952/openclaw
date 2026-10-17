import { expect, it, vi } from "vitest";
import {
  getMemorySearchManagerMock,
  resolveMemorySearchConfigMock,
  resolveSessionAgentIdMock,
} from "./compact.hooks.harness.js";
import type { CompactionMemorySyncFixture } from "./compact.hooks.test.js";

export function registerCompactionMemorySyncCases(getRuntime: () => CompactionMemorySyncFixture) {
  it("skips sync in await mode when postCompactionForce is false", async () => {
    const { compactTesting, compactionConfig, mockCallArg, TEST_SESSION_KEY, TEST_SESSION_FILE } =
      getRuntime();
    const sync = vi.fn(async () => {});
    getMemorySearchManagerMock.mockResolvedValue({ manager: { sync } });
    resolveMemorySearchConfigMock.mockReturnValue({
      sources: ["sessions"],
      sync: {
        sessions: {
          postCompactionForce: false,
        },
      },
    });

    await compactTesting.runPostCompactionSideEffects({
      config: compactionConfig("await"),
      sessionKey: TEST_SESSION_KEY,
      sessionFile: TEST_SESSION_FILE,
    });

    const resolveAgentArg = mockCallArg(resolveSessionAgentIdMock);
    if (typeof resolveAgentArg !== "object" || resolveAgentArg === null) {
      throw new Error("Expected session agent arguments");
    }
    expect(resolveAgentArg).toMatchObject({ sessionKey: TEST_SESSION_KEY });
    expect(Reflect.get(resolveAgentArg, "config")).toBeTypeOf("object");
    expect(getMemorySearchManagerMock).not.toHaveBeenCalled();
    expect(sync).not.toHaveBeenCalled();
  });
}
