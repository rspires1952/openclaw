/* @vitest-environment jsdom */

import { expect, it, vi } from "vitest";
import type { GatewayBrowserClient } from "../../api/gateway.ts";
import type { ApplicationContext } from "../../app/context.ts";
import {
  agentsCapability,
  createAgentsPage,
  gateway,
  pageContext,
  setPageGateway,
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
    } as ApplicationContext["runtimeConfig"],
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
