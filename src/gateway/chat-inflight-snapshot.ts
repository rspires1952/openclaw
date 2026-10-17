import { normalizeOptionalLowercaseString } from "@openclaw/normalization-core/string-coerce";
import type { AgentEventPayload } from "../infra/agent-events.js";
import {
  getRpcSourceIdentity,
  getRpcSourceStartedAt,
  isRpcSourceActive,
  listRpcSourceEntries,
  type RpcSourceRef,
} from "../sessions/session-controller.rpc-sources.js";
import type { ChatRunPlanSnapshot, ChatRunState } from "./server-chat-state.js";
import { captureRpcSourceScopeMatcher, type SessionRuntimeScope } from "./session-runtime-scope.js";

export type InFlightRunSnapshot = {
  runId: string;
  text: string;
  startedAt?: number;
  /**
   * True when the in-flight run is owned by a native controller attempt and can
   * only be cancelled through the session-owned abort path (sessions.abort),
   * never through run-specific chat.abort. Control UI uses this to keep Stop
   * routing session-scoped for recovered embedded runs.
   */
  sessionAbortable?: boolean;
  plan?: ChatRunPlanSnapshot;
  events?: AgentEventPayload[];
};

export function projectInFlightRunSnapshot(params: {
  chatRunState: Pick<ChatRunState, "resolveBuffer" | "runs">;
  runId: string;
  startedAtMs?: number;
  sessionAbortable?: boolean;
}): InFlightRunSnapshot {
  const run = params.chatRunState.runs.get(params.runId);
  const projected = params.chatRunState.resolveBuffer(params.runId);
  const plan = run?.planSnapshot;
  const events = run?.progressSnapshot?.events;
  return {
    runId: params.runId,
    text: projected.suppress ? "" : projected.text,
    ...(params.startedAtMs === undefined ? {} : { startedAt: params.startedAtMs }),
    ...(params.sessionAbortable ? { sessionAbortable: true } : {}),
    ...(plan ? { plan } : {}),
    ...(events?.length ? { events } : {}),
  };
}

/** Restore the newest visible run for this Gateway's physical session store. */
export function resolveInFlightRunSnapshot(
  params: SessionRuntimeScope & {
    chatRunState: Pick<ChatRunState, "resolveBuffer" | "runs">;
    requestedSessionKey: string;
    canonicalSessionKey: string;
    agentId?: string;
    defaultAgentId?: string;
  },
): InFlightRunSnapshot | undefined {
  const matchesScope = captureRpcSourceScopeMatcher(params);
  const matchesKey = (entry: RpcSourceRef, key: string): boolean => {
    const identity = getRpcSourceIdentity(entry);
    if (identity.sessionKey !== key) {
      return false;
    }
    if (key !== "global") {
      return true;
    }
    const requestedAgentId =
      normalizeOptionalLowercaseString(params.agentId) ??
      normalizeOptionalLowercaseString(params.defaultAgentId);
    if (!requestedAgentId) {
      return false;
    }
    const runAgentId =
      normalizeOptionalLowercaseString(identity.agentId) ??
      normalizeOptionalLowercaseString(params.defaultAgentId);
    return runAgentId === requestedAgentId;
  };
  let best: { runId: string; startedAtMs: number } | undefined;
  for (const [runId, entry] of listRpcSourceEntries()) {
    if (
      !matchesScope(entry) ||
      !isRpcSourceActive(entry) ||
      entry.adapter.controlUiVisible === false ||
      entry.input.abortSignal.aborted ||
      entry.adapter.kind === "agent" ||
      (!matchesKey(entry, params.requestedSessionKey) &&
        !matchesKey(entry, params.canonicalSessionKey))
    ) {
      continue;
    }
    const startedAtMs = getRpcSourceStartedAt(entry) ?? 0;
    // Timestamp wins over insertion order; runId breaks ties deterministically.
    if (
      best === undefined ||
      startedAtMs > best.startedAtMs ||
      (startedAtMs === best.startedAtMs && runId > best.runId)
    ) {
      best = { runId, startedAtMs };
    }
  }
  if (best === undefined) {
    return undefined;
  }
  // Adopt an active run before its first text so clients can reconcile its reply.
  return projectInFlightRunSnapshot({
    chatRunState: params.chatRunState,
    runId: best.runId,
    startedAtMs: best.startedAtMs,
  });
}
