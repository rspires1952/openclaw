import fs from "node:fs";
import path from "node:path";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import {
  createEmbeddedRunHandle,
  registerTestEmbeddedRun,
  clearTestEmbeddedRun,
} from "../../agents/embedded-agent-runner/runs.test-support.js";
import { replaceSessionEntry } from "../../config/sessions/session-accessor.js";
import { resolveSqliteTargetFromSessionStorePath } from "../../config/sessions/session-sqlite-target.js";
import {
  closeOpenClawAgentDatabasesForTest,
  listOpenClawRegisteredAgentDatabases,
  resolveOpenClawAgentSqlitePath,
} from "../../state/openclaw-agent-db.js";
import { closeOpenClawStateDatabaseForTest } from "../../state/openclaw-state-db.js";
import { testState } from "../test-helpers.js";
import {
  directSessionReq,
  getGatewayConfigModule,
  setupGatewaySessionsHandlerTestHarness,
} from "../test/server-sessions.test-helpers.js";
import { createActiveRun, createChatAbortContext } from "./chat.abort.test-helpers.js";

setupGatewaySessionsHandlerTestHarness();

function requireStateDir(): string {
  const stateDir = process.env.OPENCLAW_STATE_DIR;
  if (!stateDir) {
    throw new Error("OPENCLAW_STATE_DIR is required");
  }
  return stateDir;
}

beforeEach(async () => {
  testState.sessionStorePath = undefined;
  testState.sessionConfig = undefined;
  testState.agentsConfig = { entries: { main: {}, work: {} } };
  const { clearConfigCache, clearRuntimeConfigSnapshot } = await getGatewayConfigModule();
  clearRuntimeConfigSnapshot();
  clearConfigCache();
});

afterEach(() => {
  testState.sessionStorePath = undefined;
  testState.sessionConfig = undefined;
  closeOpenClawAgentDatabasesForTest();
  closeOpenClawStateDatabaseForTest();
});

async function configureFixedSessionStore(label = "default"): Promise<string> {
  const storePath = path.join(requireStateDir(), `shared-abort-sessions-${label}`, "sessions.json");
  fs.mkdirSync(path.dirname(storePath), { recursive: true });
  fs.writeFileSync(storePath, "{}\n", "utf8");
  testState.sessionStorePath = storePath;
  testState.agentsConfig = { entries: { main: {} } };
  const { clearConfigCache, clearRuntimeConfigSnapshot } = await getGatewayConfigModule();
  clearRuntimeConfigSnapshot();
  clearConfigCache();
  const { getRuntimeConfig } = await getGatewayConfigModule();
  expect(getRuntimeConfig().session?.store).toBe(storePath);
  return storePath;
}

test("sessions.abort rejects an unknown agent without provisioning its store", async () => {
  const result = await directSessionReq("sessions.abort", { key: "agent:ghost:zzz" });

  expect(result).toMatchObject({
    ok: false,
    error: { code: "INVALID_REQUEST", message: 'agent "ghost" not found' },
  });
  const env = { OPENCLAW_STATE_DIR: requireStateDir() };
  expect(fs.existsSync(path.join(env.OPENCLAW_STATE_DIR, "agents", "ghost"))).toBe(false);
  expect(fs.existsSync(resolveOpenClawAgentSqlitePath({ agentId: "ghost", env }))).toBe(false);
  expect(listOpenClawRegisteredAgentDatabases({ env }).map((entry) => entry.agentId)).not.toContain(
    "ghost",
  );
});

test("sessions.abort aborts a pre-existing session after its agent is removed from config", async () => {
  const agentId = "retired";
  const sessionKey = `agent:${agentId}:existing`;
  const sessionId = "session-retired";
  const runId = "run-retired";
  const storePath = path.join(requireStateDir(), "agents", agentId, "sessions", "sessions.json");
  await replaceSessionEntry({ agentId, sessionKey, storePath }, { sessionId, updatedAt: 42 });
  const activeRun = createActiveRun(sessionKey, { agentId, sessionId });
  const { getRuntimeConfig: _getRuntimeConfig, ...abortContext } = createChatAbortContext({
    sources: new Map([[runId, activeRun]]),
  });

  const result = await directSessionReq(
    "sessions.abort",
    { key: sessionKey },
    {
      context: abortContext,
    },
  );

  expect(result).toMatchObject({
    ok: true,
    payload: { ok: true, abortedRunId: runId, status: "aborted" },
  });
  expect(activeRun.input.abortSignal.aborted).toBe(true);
});

test("sessions.abort aborts an exact active run for an unconfigured agent without a store", async () => {
  const agentId = "active-only";
  const sessionKey = `agent:${agentId}:running`;
  const runId = "run-active-only";
  const activeRun = createActiveRun(sessionKey, { agentId });
  const { getRuntimeConfig: _getRuntimeConfig, ...abortContext } = createChatAbortContext({
    sources: new Map([[runId, activeRun]]),
  });

  const result = await directSessionReq(
    "sessions.abort",
    { key: sessionKey },
    { context: abortContext },
  );

  expect(result).toMatchObject({
    ok: true,
    payload: { ok: true, abortedRunId: runId, status: "aborted" },
  });
  expect(activeRun.input.abortSignal.aborted).toBe(true);
  const env = { OPENCLAW_STATE_DIR: requireStateDir() };
  expect(fs.existsSync(path.join(env.OPENCLAW_STATE_DIR, "agents", agentId))).toBe(false);
  expect(fs.existsSync(resolveOpenClawAgentSqlitePath({ agentId, env }))).toBe(false);
  expect(listOpenClawRegisteredAgentDatabases({ env }).map((entry) => entry.agentId)).not.toContain(
    agentId,
  );
});

test("sessions.abort stops the exact native owner without provisioning its missing store", async () => {
  const agentId = "native-active-only";
  const sessionKey = "agent:" + agentId + ":running";
  const sessionId = "native-active-only-session";
  const runId = "native-active-only-run";
  const cancelled = createDeferred();
  const abort = vi.fn(() => cancelled.resolve());
  const handle = createEmbeddedRunHandle({ runId, abort });
  registerTestEmbeddedRun(sessionId, handle, sessionKey, undefined, agentId);
  const { getRuntimeConfig: _getRuntimeConfig, ...abortContext } = createChatAbortContext({
    getSessionEventSubscriberConnIds: () => new Set(),
  });
  let finished = false;
  const request = directSessionReq(
    "sessions.abort",
    { key: sessionKey, runId },
    { context: abortContext },
  ).then((result) => {
    finished = true;
    return result;
  });
  try {
    const first = await Promise.race([
      cancelled.promise.then(() => "cancelled"),
      request.then((response) => ({ response })),
    ]);
    expect(first).toBe("cancelled");
    expect(abort).toHaveBeenCalledOnce();
    expect(finished).toBe(false);
    clearTestEmbeddedRun(sessionId, handle, sessionKey);
    await expect(request).resolves.toMatchObject({
      ok: true,
      payload: { ok: true, abortedRunId: runId, status: "aborted" },
    });
    const env = { OPENCLAW_STATE_DIR: requireStateDir() };
    expect(fs.existsSync(path.join(env.OPENCLAW_STATE_DIR, "agents", agentId))).toBe(false);
    expect(fs.existsSync(resolveOpenClawAgentSqlitePath({ agentId, env }))).toBe(false);
  } finally {
    clearTestEmbeddedRun(sessionId, handle, sessionKey);
    await request;
  }
});

test("sessions.abort rejects an unknown agent when only the fixed store file exists", async () => {
  const storePath = await configureFixedSessionStore();

  const result = await directSessionReq("sessions.abort", { key: "agent:ghost:missing" });

  expect(result).toMatchObject({
    ok: false,
    error: { code: "INVALID_REQUEST", message: 'agent "ghost" not found' },
  });
  const sqlitePath = resolveSqliteTargetFromSessionStorePath(storePath, {
    agentId: "ghost",
  }).path;
  expect(sqlitePath).toBeDefined();
  expect(fs.existsSync(sqlitePath!)).toBe(false);
});

test("sessions.abort aborts an unconfigured agent with rows in a fixed store", async () => {
  const storePath = await configureFixedSessionStore();
  const agentId = "retired";
  const sessionKey = `agent:${agentId}:existing`;
  const sessionId = "session-retired-fixed";
  const runId = "run-retired-fixed";
  await replaceSessionEntry({ agentId, sessionKey, storePath }, { sessionId, updatedAt: 42 });
  const activeRun = createActiveRun(sessionKey, { agentId, sessionId });
  const { getRuntimeConfig: _getRuntimeConfig, ...abortContext } = createChatAbortContext({
    sources: new Map([[runId, activeRun]]),
  });

  const result = await directSessionReq(
    "sessions.abort",
    { key: sessionKey },
    { context: abortContext },
  );

  expect(result).toMatchObject({
    ok: true,
    payload: { ok: true, abortedRunId: runId, status: "aborted" },
  });
  expect(activeRun.input.abortSignal.aborted).toBe(true);
});

test("sessions.abort finds a retired store only reachable through its deterministic template", async () => {
  const agentId = "template-retired";
  const sessionKey = `agent:${agentId}:existing`;
  const sessionId = "session-template-retired";
  const runId = "run-template-retired";
  const storeTemplate = path.join(
    requireStateDir(),
    "external-abort-stores",
    "sessions-{agentId}.json",
  );
  const storePath = storeTemplate.replace("{agentId}", agentId);
  testState.sessionStorePath = storeTemplate;
  testState.agentsConfig = { entries: { main: {} } };
  const { clearConfigCache, clearRuntimeConfigSnapshot } = await getGatewayConfigModule();
  clearRuntimeConfigSnapshot();
  clearConfigCache();
  await replaceSessionEntry({ agentId, sessionKey, storePath }, { sessionId, updatedAt: 42 });
  const activeRun = createActiveRun(sessionKey, { agentId, sessionId });
  const { getRuntimeConfig: _getRuntimeConfig, ...abortContext } = createChatAbortContext({
    sources: new Map([[runId, activeRun]]),
  });

  const result = await directSessionReq(
    "sessions.abort",
    { key: sessionKey },
    { context: abortContext },
  );

  expect(result).toMatchObject({
    ok: true,
    payload: { ok: true, abortedRunId: runId, status: "aborted" },
  });
  expect(activeRun.input.abortSignal.aborted).toBe(true);
});

test.each(["main"])(
  "sessions.abort does not provision the %s agent store when no run exists",
  async (agentId) => {
    const result = await directSessionReq("sessions.abort", {
      key: `agent:${agentId}:missing`,
    });

    expect(result).toMatchObject({
      ok: true,
      payload: { ok: true, abortedRunId: null, status: "no-active-run" },
    });
    expect(
      fs.existsSync(
        resolveOpenClawAgentSqlitePath({
          agentId,
          env: { OPENCLAW_STATE_DIR: requireStateDir() },
        }),
      ),
    ).toBe(false);
  },
);
