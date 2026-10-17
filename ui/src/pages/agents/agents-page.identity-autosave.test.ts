/* @vitest-environment jsdom */

import { expect, it, vi } from "vitest";
import { createDeferred } from "../../../../test/helpers/promise.js";
import type { GatewayBrowserClient } from "../../api/gateway.ts";
import type { ApplicationContext } from "../../app/context.ts";
import {
  agentsCapability,
  agentsList,
  createAgentsPage,
  gateway,
  pageContext,
  setPageGateway,
  settingsSelection,
  snapshot,
} from "./agents-page.test-support.ts";

it("debounces identity edits and persists the latest name automatically", async () => {
  const request = vi.fn(async () => ({}));
  const client = { request } as unknown as GatewayBrowserClient;
  const agents = agentsCapability(async () => ({ agentId: "main", workspace: "main", files: [] }));
  const agentIdentity = {
    invalidate: vi.fn(),
    ensure: vi.fn(async () => undefined),
  } as unknown as ApplicationContext["agentIdentity"];
  const page = createAgentsPage();
  page.context = {
    ...pageContext(gateway(snapshot(client)), agents, { agentIdentity }),
    runtimeConfig: {
      runExternalMutation: async (task: (client: GatewayBrowserClient) => Promise<unknown>) => ({
        ok: true as const,
        value: await task(client),
        refresh: { ok: true as const },
      }),
    } as unknown as ApplicationContext["runtimeConfig"],
  };
  setPageGateway(page, client);
  page.agentsSelectedId = "main";

  page.identityDraft = { name: "First name", emoji: null, avatar: null };
  page.scheduleIdentitySave(30);
  page.identityDraft = { name: "Second name", emoji: null, avatar: null };
  page.scheduleIdentitySave(30);
  await vi.waitFor(() => expect(request).toHaveBeenCalledOnce());
  expect(request).toHaveBeenCalledWith("agents.update", {
    agentId: "main",
    name: "Second name",
  });
});

it("retains a newer identity edit for its agent when selection changes during a save", async () => {
  const firstUpdate = createDeferred<object>();
  const request = vi.fn(async (method: string, params: { name?: string }) => {
    if (method === "agents.update" && params.name === "First name") {
      return firstUpdate.promise;
    }
    return {};
  });
  const client = { request } as unknown as GatewayBrowserClient;
  const roster = {
    ...agentsList,
    agents: [...agentsList.agents, { id: "research", name: "Research" }],
  };
  const agents = agentsCapability(async () => ({ agentId: "main", workspace: "main", files: [] }));
  agents.state.agentsList = roster;
  const agentIdentity = {
    invalidate: vi.fn(),
    ensure: vi.fn(async () => undefined),
    get: vi.fn(() => null),
    entries: vi.fn(() => []),
    subscribe: vi.fn(() => () => undefined),
  } as unknown as ApplicationContext["agentIdentity"];
  const selection = settingsSelection(roster, "main");
  const page = createAgentsPage();
  page.context = {
    ...pageContext(gateway(snapshot(client)), agents, { agentIdentity }),
    settingsAgentSelection: selection,
    runtimeConfig: {
      subscribe: vi.fn(() => () => undefined),
      runExternalMutation: async (task: (client: GatewayBrowserClient) => Promise<unknown>) => ({
        ok: true as const,
        value: await task(client),
        refresh: { ok: true as const },
      }),
    } as unknown as ApplicationContext["runtimeConfig"],
  };
  setPageGateway(page, client);
  page.agentsSelectedId = "main";
  page.subscriptions.hostConnected();
  try {
    page.identityDraft = { name: "First name", emoji: null, avatar: null };
    page.scheduleIdentitySave(0);
    await vi.waitFor(() =>
      expect(request).toHaveBeenCalledWith("agents.update", {
        agentId: "main",
        name: "First name",
      }),
    );

    page.identityDraft = { name: "Second name", emoji: null, avatar: null };
    page.scheduleIdentitySave(30);
    selection.set("research");
    expect(page.agentsSelectedId).toBe("research");
    selection.set("main");
    expect(page.identityDraft.name).toBe("Second name");

    firstUpdate.resolve({});
    await vi.waitFor(() =>
      expect(request).toHaveBeenCalledWith("agents.update", {
        agentId: "main",
        name: "Second name",
      }),
    );
  } finally {
    firstUpdate.resolve({});
    page.subscriptions.hostDisconnected();
    selection.dispose();
  }
});
