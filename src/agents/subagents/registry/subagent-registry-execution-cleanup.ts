import { clearGatewayContextResolver } from "../../../plugins/runtime/gateway-request-scope.js";
import { captureSessionTarget } from "../../../sessions/session-controller.lifecycle.js";
import {
  getRpcSourceForTarget,
  getRpcSourceIdentity,
  getRpcSourceLifecycleGeneration,
  type RpcSourceRef,
} from "../../../sessions/session-controller.rpc-sources.js";
import { resolveActiveEmbeddedRunOwnerByRunId } from "../../embedded-agent-runner/runs.js";
import { resolveSubagentChildAuthority } from "./subagent-child-owner-match.js";
import type { SubagentKillSession } from "./subagent-control-session.js";
import { getCurrentSubagentRunOwner, subagentRuns } from "./subagent-registry-memory.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

export type CapturedSubagentExecution = {
  readonly entry: SubagentRunRecord;
  readonly runId: string;
  readonly execution: RpcSourceRef;
};

/** Captures the exact physical backend of a legacy row, not a replacement session. */
export function captureSubagentEmbeddedExecution(entry: SubagentRunRecord) {
  if (resolveSubagentChildAuthority(entry).status === "mismatch") {
    return undefined;
  }
  const owner = resolveActiveEmbeddedRunOwnerByRunId(entry.runId);
  return owner?.sessionKey === entry.childSessionKey &&
    (!entry.childSessionIdentity?.sessionId ||
      owner.sessionId === entry.childSessionIdentity.sessionId)
    ? owner
    : undefined;
}

/** Captures the exact controller source owned by the selected subagent run. */
export function captureSubagentExecution(params: {
  entry: SubagentRunRecord;
  session: SubagentKillSession;
}): CapturedSubagentExecution | undefined {
  const entry = getCurrentSubagentRunOwner(subagentRuns, params.entry) ?? params.entry;
  const authority = resolveSubagentChildAuthority(entry);
  if (authority.status === "mismatch") {
    return undefined;
  }
  const execution = getRpcSourceForTarget(
    entry.runId,
    captureSessionTarget({
      storeScope: params.session.storePath,
      sessionKey: entry.childSessionKey,
      agentId: params.session.agentId,
      incarnation:
        authority.status === "legacy-unverified"
          ? entry.childSessionIdentity?.sessionId
          : params.session.entry?.sessionId,
    }),
  );
  if (!execution || execution.adapter.kind !== "agent") {
    return undefined;
  }
  const identity = getRpcSourceIdentity(execution);
  const lifecycleGeneration = getRpcSourceLifecycleGeneration(execution);
  if (
    identity.sessionKey !== entry.childSessionKey ||
    identity.agentId !== authority.childAgentId ||
    entry.childSessionIdentity?.lifecycleRevision !== params.session.entry?.lifecycleRevision ||
    (authority.status === "legacy-unverified"
      ? entry.childSessionIdentity?.sessionId !== undefined &&
        identity.sessionId !== entry.childSessionIdentity.sessionId
      : identity.sessionId !== params.session.entry?.sessionId) ||
    (entry.execution.lifecycleGeneration !== undefined &&
      lifecycleGeneration !== entry.execution.lifecycleGeneration)
  ) {
    return undefined;
  }
  return { entry, runId: entry.runId, execution };
}

/** Retires the registry projection; controller source settlement retains physical cleanup. */
export function retireSubagentGatewayBinding(observed: SubagentRunRecord): void {
  const entry = getCurrentSubagentRunOwner(subagentRuns, observed) ?? observed;
  clearGatewayContextResolver(entry);
}
