import { readDatabasePathIdentitySync } from "../infra/sqlite-worker-identity.js";
import { normalizeAgentId } from "../routing/session-key.js";
import { resolveGlobalSingleton } from "../shared/global-singleton.js";
import { getAgentDeletionDatabaseCleanup } from "../state/agent-deletion-cleanup.js";
import type { OpenClawAgentDatabaseOptions } from "../state/openclaw-agent-db-contract.js";
import { resolveOpenClawStateSqlitePath } from "../state/openclaw-state-db.paths.js";

export type AgentWorkAdmissionTarget = {
  agentId: string;
  statePath?: string;
  env?: NodeJS.ProcessEnv;
};
export type AgentWorkAdmissionIdentity = { agentId: string; statePath: string };
export type SessionWorkAdmissionClosure = {
  identities: readonly string[];
  agent?: AgentWorkAdmissionIdentity;
  deletionOperationId?: string;
  reason: Error;
};

export class AgentDeletionPendingError extends Error {}

export const sessionWorkAdmissionClosures = resolveGlobalSingleton(
  Symbol.for("openclaw.sessionWorkAdmissionClosures"),
  () => new Set<SessionWorkAdmissionClosure>(),
);

export function agentWorkAdmissionIdentity(
  target: AgentWorkAdmissionTarget,
): AgentWorkAdmissionIdentity {
  return {
    agentId: normalizeAgentId(target.agentId),
    statePath: readDatabasePathIdentitySync(
      target.statePath ?? resolveOpenClawStateSqlitePath(target.env ?? process.env),
    ).canonicalPath,
  };
}

/** Journal publication, not an individual attempt, owns the pending-deletion fence. */
export function publishAgentDeletionWorkAdmission(
  target: AgentWorkAdmissionTarget,
  operationId: string,
  pending: boolean,
): void {
  const agent = agentWorkAdmissionIdentity(target);
  for (const owner of sessionWorkAdmissionClosures) {
    if (
      owner.deletionOperationId !== undefined &&
      matchesAgentWorkAdmission(owner.agent, agent) &&
      (pending || owner.deletionOperationId === operationId)
    ) {
      sessionWorkAdmissionClosures.delete(owner);
    }
  }
  if (pending) {
    sessionWorkAdmissionClosures.add({
      agent,
      identities: [],
      deletionOperationId: operationId,
      reason: new AgentDeletionPendingError(
        `Agent ${agent.agentId} deletion cleanup is still pending; resolve the cleanup failure, then retry agents.delete.`,
      ),
    });
  }
}

/** Deletion's existing ingress fence also owns admission of new session writes. */
export function assertAgentSessionWriteAdmission(
  options: OpenClawAgentDatabaseOptions,
  logicalAgentId = options.agentId,
): void {
  const cleanup = getAgentDeletionDatabaseCleanup(options);
  if (cleanup) {
    cleanup.assertCurrentHost();
    return;
  }
  if (sessionWorkAdmissionClosures.size === 0) {
    return;
  }
  const agent = agentWorkAdmissionIdentity({ agentId: logicalAgentId, env: options.env });
  const closed = [...sessionWorkAdmissionClosures].find(
    (owner) => owner.identities.length === 0 && matchesAgentWorkAdmission(owner.agent, agent),
  );
  if (closed) {
    throw closed.reason;
  }
}

export function matchesAgentWorkAdmission(
  left: AgentWorkAdmissionIdentity | undefined,
  right: AgentWorkAdmissionIdentity | undefined,
): boolean {
  return Boolean(
    left && right && left.agentId === right.agentId && left.statePath === right.statePath,
  );
}
