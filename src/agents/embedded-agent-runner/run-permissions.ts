import {
  getAgentEventLifecycleGeneration,
  isAgentEventLifecycleGenerationCurrent,
} from "../../infra/agent-events.js";
import {
  getAgentRunContext,
  validateAgentRunDelegatedAuthority,
  type AgentRunDelegatedAuthority,
} from "../../infra/agent-run-registry.js";
import { resolveSessionRunProgressState } from "../../sessions/session-controller.queries.js";
import { getActiveNativeAttempt } from "./run-state.js";
import { withAuthorizedPermissionChange } from "./run/permission-change.js";

/** Captures one exact live runtime; a later run must never inherit this update. */
export function prepareEmbeddedRunPermissionChange(sessionId: string) {
  if (!resolveSessionRunProgressState(sessionId)) {
    return { kind: "idle" as const };
  }
  const handle = getActiveNativeAttempt(sessionId);
  if (!handle?.applyPermissionMode) {
    return { kind: "unsupported" as const };
  }
  const applyPermissionMode = handle.applyPermissionMode;
  const generation = getAgentEventLifecycleGeneration();
  const owner = handle.permissionChangeOwner;
  const authority = handle.runId ? getAgentRunContext(handle.runId)?.delegatedAuthority : undefined;
  const ownsRuntime = () => {
    const current = getActiveNativeAttempt(sessionId);
    return (
      isAgentEventLifecycleGenerationCurrent(generation) &&
      (current === handle || (owner !== undefined && current?.permissionChangeOwner === owner))
    );
  };
  return {
    kind: "active" as const,
    stop: () => {
      if (ownsRuntime()) {
        getActiveNativeAttempt(sessionId)?.abort();
      }
    },
    apply: async (
      mode: Parameters<typeof applyPermissionMode>[0],
      revokeApprovals: (authority: AgentRunDelegatedAuthority) => void,
    ): Promise<boolean> => {
      if (!ownsRuntime()) {
        return false;
      }
      const revoke = () => {
        if (!ownsRuntime() || (authority && !validateAgentRunDelegatedAuthority(authority))) {
          throw new Error("Permission change lost its active run. Retry the request.");
        }
        if (authority) {
          revokeApprovals(authority);
        }
      };
      const apply = () => applyPermissionMode(mode, revoke);
      const applied = await (owner ? withAuthorizedPermissionChange(owner, mode, apply) : apply());
      return (
        applied &&
        isAgentEventLifecycleGenerationCurrent(generation) &&
        (!getActiveNativeAttempt(sessionId) || ownsRuntime())
      );
    },
  };
}
