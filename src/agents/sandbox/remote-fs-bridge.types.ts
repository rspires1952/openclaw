import type {
  SandboxBackendCommandParams,
  SandboxBackendCommandResult,
} from "./backend-handle.types.js";
import type { SandboxResolvedPath } from "./fs-bridge.types.js";

export type ResolvedRemotePath = SandboxResolvedPath & {
  writable: boolean;
  mountRootPath: string;
};

export type RemoteShellSandboxHandle = {
  remoteWorkspaceDir: string;
  remoteAgentWorkspaceDir: string;
  runRemoteShellScript(params: SandboxBackendCommandParams): Promise<SandboxBackendCommandResult>;
};
