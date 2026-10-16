import assert from "node:assert/strict";
import { afterEach, expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import { createChannelTestPluginBase, createTestRegistry } from "../test-utils/channel-plugins.js";
const getCurrentPluginConversationBinding = vi.hoisted(() => vi.fn(async () => null));
vi.mock("./conversation-binding.js", () => ({
  getCurrentPluginConversationBinding,
  requestPluginConversationBinding: vi.fn(),
  detachPluginConversationBinding: vi.fn(),
}));

import { getPluginCommandExecutionCount } from "./command-execution-lock.js";
import { registerPluginCommandInRegistry } from "./command-registration.js";
import { createPluginRecord } from "./loader-records.js";
import {
  createPluginCommandRuntime,
  executePluginCommandDispatch,
  matchPluginCommandInvocation,
  type PluginCommandDispatch,
} from "./plugin-command-runtime.js";
import { PluginInstance } from "./plugin-instance.js";
import { createEmptyPluginRegistry } from "./registry-empty.js";
import { markPluginRegistryRetired } from "./registry-lifecycle.js";
import {
  clearActivePluginRegistry,
  resetPluginRuntimeStateForTest,
  setActivePluginRegistry,
} from "./runtime.js";
import { withPluginRuntimeRegistryScope } from "./runtime/gateway-request-scope.js";
import type { OpenClawPluginCommandDefinition } from "./types.js";

const executionContext = {
  senderId: "user-1",
  channel: "telegram",
  isAuthorizedSender: true,
  commandBody: "/demo",
  config: {},
} as const;

function command(
  name: string,
  handler: OpenClawPluginCommandDefinition["handler"] = async () => ({ text: "ok" }),
  options: Partial<OpenClawPluginCommandDefinition> = {},
): OpenClawPluginCommandDefinition {
  return { name, description: `${name} command`, handler, ...options };
}

function registerCommand(
  registry: ReturnType<typeof createEmptyPluginRegistry>,
  name: string,
  handler: OpenClawPluginCommandDefinition["handler"] = async () => ({ text: "ok" }),
  options: Partial<OpenClawPluginCommandDefinition> & { pluginId?: string } = {},
  pluginRoot?: string,
) {
  const { pluginId = name, ...definition } = options;
  expect(
    registerPluginCommandInRegistry(registry, pluginId, command(name, handler, definition), {
      pluginRoot,
    }),
  ).toEqual({ ok: true });
}

function pluginRecord(pluginId: string) {
  return createPluginRecord({
    id: pluginId,
    source: `/plugins/${pluginId}/index.js`,
    origin: "config",
    enabled: true,
    configSchema: true,
  });
}

function createCleanupRegistry(pluginId: string) {
  const registry = createEmptyPluginRegistry();
  const record = pluginRecord(pluginId);
  registry.plugins.push(record);
  const cleanup = vi.fn<() => void | Promise<void>>();
  new PluginInstance(pluginId, { record, registry }).lifecycle.onDispose(cleanup);
  return { registry, cleanup };
}

function nativeCandidate(name?: string, channel = "telegram") {
  const candidates = createPluginCommandRuntime().listNativeCandidates(channel);
  return (name ? candidates.find((entry) => entry.name === name) : candidates[0])!;
}

function selectCommand(name?: string, channel?: string) {
  const dispatch = nativeCandidate(name, channel).prepareDispatch();
  assert.ok(dispatch.kind === "plugin", "expected plugin command dispatch");
  return dispatch;
}

function executeCommand(registry: ReturnType<typeof createEmptyPluginRegistry>) {
  setActivePluginRegistry(registry);
  return selectCommand().execute(executionContext);
}

function watchSettlement(promise: Promise<void>) {
  const settled = vi.fn();
  void promise.then(settled);
  return settled;
}

function detachedClearCommand(
  registry: ReturnType<typeof createEmptyPluginRegistry>,
  name: string,
) {
  const gate = createDeferred();
  let clearing!: Promise<void>;
  registerCommand(registry, name, async () => {
    clearing = gate.promise.then(() => clearActivePluginRegistry());
    return { text: name };
  });
  return { gate, clearing: () => clearing };
}
function matchCommand(body: string, channel = "telegram", runtime = createPluginCommandRuntime()) {
  return matchPluginCommandInvocation(runtime, body, { channel });
}

afterEach(() => {
  getCurrentPluginConversationBinding.mockClear();
  resetPluginRuntimeStateForTest();
});

it("keeps failed command diagnostics scoped, authorized, redacted, and bounded", async () => {
  const registry = createEmptyPluginRegistry();
  registry.plugins.push({
    ...pluginRecord("recovery"),
    status: "error",
    failurePhase: "validation",
    error: `missing payload token=fixture-secret-value private-detail ${"x".repeat(400)}\n    at loader`,
    commandAliases: [{ name: "recover", kind: "runtime-slash" }],
  });
  setActivePluginRegistry(registry);
  withPluginRuntimeRegistryScope(createEmptyPluginRegistry(), () => {
    expect(matchCommand("/recover stop")).toBeNull();
  });
  const disabled = createEmptyPluginRegistry();
  disabled.plugins.push({ ...registry.plugins[0]!, enabled: false });
  withPluginRuntimeRegistryScope(disabled, () => {
    expect(matchCommand("/recover stop")).toBeNull();
  });
  const match = matchCommand("/recover stop");
  assert.ok(match, "expected failed command diagnostic");
  await expect(
    match.dispatch.execute({ ...executionContext, isAuthorizedSender: false }),
  ).resolves.toEqual({ text: "⚠️ This command requires authorization." });
  const reply = await match.dispatch.execute({
    ...executionContext,
    config: { logging: { redactPatterns: ["private-detail"] } },
  });
  expect(reply.text).toContain("missing payload");
  expect(reply.text).toContain("openclaw doctor");
  expect(reply.text).not.toMatch(/fixture-secret-value|private-detail|at loader/);
  expect(reply.text!.length).toBeLessThan(400);
});

it("binds the request-scoped registry and scopes provider aliases", async () => {
  const ambient = createEmptyPluginRegistry();
  const scoped = createEmptyPluginRegistry();
  const ambientHandler = vi.fn(async () => ({ text: "ambient" }));
  const scopedHandler = vi.fn<OpenClawPluginCommandDefinition["handler"]>(async ({ args }) => ({
    text: `scoped:${args}`,
  }));
  const nativeNames = { discord: "discord-demo" };
  registerCommand(ambient, "demo", ambientHandler, { pluginId: "ambient" });
  registerCommand(scoped, "demo", scopedHandler, {
    pluginId: "scoped",
    channels: ["discord"],
    nativeNames,
    acceptsArgs: true,
  });
  setActivePluginRegistry(ambient);

  await withPluginRuntimeRegistryScope(scoped, async () => {
    const runtime = createPluginCommandRuntime();
    expect(runtime.listNativeCandidates("telegram")).toEqual([]);
    const candidates = runtime.listNativeCandidates("discord");
    expect(candidates.map((candidate) => candidate.name)).toEqual(["discord-demo"]);
    expect(matchCommand("/discord-demo hi", "telegram", runtime)).toBeNull();
    expect(matchCommand("/discord-demo hi", "discord", runtime)).not.toBeNull();
    nativeNames.discord = "renamed-demo";
    expect(matchCommand("/discord-demo hi", "discord", runtime)).toBeNull();
    const match = matchCommand("/renamed-demo hi", "discord", runtime);
    assert.ok(match, "expected scoped command match");
    expect(match.dispatch.kind).toBe("plugin");
    const result = await match.dispatch.execute({
      ...executionContext,
      channel: "discord",
      commandBody: "/renamed-demo hi",
    });
    expect(result).toEqual({ text: "scoped:hi" });
  });
  expect(scopedHandler).toHaveBeenCalledOnce();
  expect(ambientHandler).not.toHaveBeenCalled();
});

it.each(["provider", "fallback"])(
  "resolves %s conversation bindings through the command's selected registry",
  async (resolution) => {
    const createRegistry = (owner: string) =>
      createTestRegistry([
        {
          pluginId: "room-chat",
          source: "test",
          plugin: {
            ...createChannelTestPluginBase({
              id: "room-chat",
              config: { defaultAccountId: () => `${owner}-account` },
            }),
            bindings: {
              resolveCommandConversation: () =>
                resolution === "provider" ? { conversationId: `${owner}-room` } : null,
            },
            messaging: { normalizeTarget: () => `channel:${owner}-room` },
          },
        },
      ]);
    const ambient = createRegistry("ambient");
    const scoped = createRegistry("scoped");
    registerCommand(
      scoped,
      "demo",
      async (ctx) => {
        await ctx.getCurrentConversationBinding();
        return { text: ctx.accountId };
      },
      undefined,
      "/plugins/demo",
    );
    setActivePluginRegistry(ambient);

    const dispatch = withPluginRuntimeRegistryScope(scoped, () =>
      selectCommand(undefined, "room-chat"),
    );
    await expect(
      dispatch.execute({ ...executionContext, channel: "room-chat", to: "room-chat:opaque" }),
    ).resolves.toEqual({ text: "scoped-account" });
    expect(getCurrentPluginConversationBinding).toHaveBeenCalledWith({
      pluginRoot: "/plugins/demo",
      conversation: {
        channel: "room-chat",
        accountId: "scoped-account",
        conversationId: "scoped-room",
      },
    });
  },
);

it("rejects forged, cross-runtime, wrong-channel, and retired selections", async () => {
  const registry = createEmptyPluginRegistry();
  const handler = vi.fn(async () => ({ text: "ok" }));
  registerCommand(registry, "demo", handler);
  setActivePluginRegistry(registry);
  const dispatch = selectCommand();
  const secondDispatch = selectCommand();
  const forged = Object.freeze({
    kind: "plugin",
    execute: dispatch.execute,
  }) as PluginCommandDispatch;

  const invalidReply = { text: expect.stringContaining("no longer valid") };
  await expect(executePluginCommandDispatch(forged, executionContext)).resolves.toMatchObject(
    invalidReply,
  );
  await expect(dispatch.execute.call(secondDispatch, executionContext)).resolves.toMatchObject(
    invalidReply,
  );
  await expect(
    dispatch.execute({ ...executionContext, channel: "discord" }),
  ).resolves.toMatchObject(invalidReply);

  markPluginRegistryRetired(registry);
  await expect(dispatch.execute(executionContext)).resolves.toMatchObject({
    text: expect.stringContaining("registry changed"),
  });
  expect(handler).not.toHaveBeenCalled();
});

it("keeps overlapping executions locked until both handlers settle", async () => {
  const registry = createEmptyPluginRegistry();
  const releases: Array<() => void> = [];
  registerCommand(registry, "slow", async () => {
    await new Promise<void>((resolve) => {
      releases.push(resolve);
    });
    return { text: "done" };
  });
  setActivePluginRegistry(registry);
  const firstRun = selectCommand().execute(executionContext);
  const secondRun = selectCommand().execute(executionContext);
  await vi.waitFor(() => expect(getPluginCommandExecutionCount(registry)).toBe(2));

  expect(registerPluginCommandInRegistry(registry, "blocked", command("blocked"))).toMatchObject({
    ok: false,
  });
  releases.shift()?.();
  await vi.waitFor(() => expect(getPluginCommandExecutionCount(registry)).toBe(1));
  expect(
    registerPluginCommandInRegistry(registry, "still-blocked", command("still-blocked")),
  ).toMatchObject({ ok: false });
  releases.shift()?.();
  await Promise.all([firstRun, secondRun]);
  expect(getPluginCommandExecutionCount(registry)).toBe(0);
  registerCommand(registry, "ready");
});

it("does not prepare arguments for commands that reject them", () => {
  const registry = createEmptyPluginRegistry();
  registerCommand(registry, "demo");
  setActivePluginRegistry(registry);
  expect(nativeCandidate().prepareDispatch("unexpected")).toEqual({ kind: "non-plugin" });
});

it("defers full registry cleanup until an admitted command settles", async () => {
  const { registry, cleanup } = createCleanupRegistry("slow");
  const gate = createDeferred();
  const { promise: started, resolve: entered } = createDeferred();
  registerCommand(registry, "slow", async () => {
    entered();
    await gate.promise;
    return { text: "done" };
  });
  const running = executeCommand(registry);
  await started;
  const clearing = clearActivePluginRegistry();
  const clearSettled = watchSettlement(clearing);
  await Promise.resolve();
  expect(clearSettled).not.toHaveBeenCalled();
  expect(cleanup).not.toHaveBeenCalled();
  gate.resolve();
  await expect(running).resolves.toEqual({ text: "done" });
  await clearing;
  expect(cleanup).toHaveBeenCalledOnce();
});

it.each([false, true])("lets command-triggered clears finish (replaced: %s)", async (replaced) => {
  const { registry, cleanup } = createCleanupRegistry("clear");
  registerCommand(registry, "clear", async () => {
    if (replaced) {
      setActivePluginRegistry(createEmptyPluginRegistry());
    }
    await clearActivePluginRegistry();
    await clearActivePluginRegistry();
    return { text: "cleared" };
  });
  await expect(executeCommand(registry)).resolves.toEqual({ text: "cleared" });
  await clearActivePluginRegistry();
  expect(cleanup).toHaveBeenCalledOnce();
});

it("awaits cleanup from detached handler context after execution settles", async () => {
  const { registry, cleanup } = createCleanupRegistry("scheduled");
  const detached = detachedClearCommand(registry, "scheduled");
  const cleanupGate = createDeferred();
  cleanup.mockImplementationOnce(() => cleanupGate.promise);
  await expect(executeCommand(registry)).resolves.toEqual({ text: "scheduled" });
  expect(getPluginCommandExecutionCount(registry)).toBe(0);
  detached.gate.resolve();
  await vi.waitFor(() => expect(cleanup).toHaveBeenCalledOnce());
  const clearSettled = watchSettlement(detached.clearing());
  await Promise.resolve();
  expect(clearSettled).not.toHaveBeenCalled();
  cleanupGate.resolve();
  await detached.clearing();
  expect(clearSettled).toHaveBeenCalledOnce();
});

it("does not reuse an outer admission for detached nested handler cleanup", async () => {
  const { registry } = createCleanupRegistry("nested");
  const detached = detachedClearCommand(registry, "inner");
  const { promise: outerGate, resolve: releaseOuter } = createDeferred();
  const { promise: outerHoldingGate, resolve: outerHolding } = createDeferred();
  registerCommand(registry, "outer", async () => {
    await innerDispatch.execute({ ...executionContext, commandBody: "/inner" });
    outerHolding();
    await outerGate;
    return { text: "outer" };
  });
  setActivePluginRegistry(registry);
  const innerDispatch = selectCommand("inner");
  const running = selectCommand("outer").execute({ ...executionContext, commandBody: "/outer" });
  await outerHoldingGate;
  expect(getPluginCommandExecutionCount(registry)).toBe(1);
  const clearSettled = watchSettlement(detached.clearing());
  detached.gate.resolve();
  await new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });
  expect(clearSettled).not.toHaveBeenCalled();
  releaseOuter();
  await expect(running).resolves.toEqual({ text: "outer" });
  await detached.clearing();
  expect(clearSettled).toHaveBeenCalledOnce();
});

it("fails factory creation when no registry generation exists", () => {
  resetPluginRuntimeStateForTest();
  expect(() => createPluginCommandRuntime()).toThrow("requires an active or request-scoped");
});
