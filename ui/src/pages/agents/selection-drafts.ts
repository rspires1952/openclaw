import type { GatewayBrowserClient } from "../../api/gateway.ts";
import type { ApplicationContext, ApplicationGatewaySnapshot } from "../../app/context.ts";
import {
  retainAgentFileDrafts,
  type AgentFilesState,
  type RetainedAgentFileDrafts,
} from "./files.ts";
import { resetIdentityDraft } from "./identity-actions.ts";
import type { AgentIdentityDraft } from "./panels-overview.tsx";

type AgentSelectionDraftHost = AgentFilesState & {
  agentsSelectedId: string | null;
  agentFileActive: string | null;
  identityDraft: AgentIdentityDraft;
  identitySaving: boolean;
  identityError: string | null;
};

type Selection = {
  selectedId: string | null;
  profileId: string | null;
};

type RetainedIdentityWork = {
  scope: GatewayScope;
  profileId: string | null;
  identities: Map<string, { profileId: string | null; draft: AgentIdentityDraft }>;
  saves: Map<string, RetainedIdentitySave>;
};

type GatewayScope = {
  gateway: ApplicationContext["gateway"];
  connectionRevision: number;
  gatewayUrl: string;
};

export type RetainedIdentitySave = {
  client: GatewayBrowserClient;
  snapshot: ApplicationGatewaySnapshot | null;
  promise: Promise<void>;
};

const retainedIdentityWork = new WeakMap<ApplicationContext, RetainedIdentityWork>();

function identityWorkFor(context: ApplicationContext): RetainedIdentityWork {
  const gateway = context.gateway;
  const scope: GatewayScope = {
    gateway,
    connectionRevision: gateway.connectionRevision ?? 0,
    gatewayUrl: gateway.connection?.gatewayUrl ?? "",
  };
  let work = retainedIdentityWork.get(context);
  if (
    !work ||
    work.scope.gateway !== scope.gateway ||
    work.scope.connectionRevision !== scope.connectionRevision ||
    work.scope.gatewayUrl !== scope.gatewayUrl
  ) {
    work = { scope, profileId: null, identities: new Map(), saves: new Map() };
    retainedIdentityWork.set(context, work);
  }
  return work;
}

/** Owns private editor drafts across the selected target's presentation lifetime. */
export class AgentSelectionDrafts {
  private readonly files = new Map<string, RetainedAgentFileDrafts>();
  private identities = new Map<string, { profileId: string | null; draft: AgentIdentityDraft }>();
  private profileId: string | null = null;
  private lifetime: RetainedIdentityWork | null = null;

  constructor(
    private readonly host: AgentSelectionDraftHost,
    private readonly resetSelection: () => void,
  ) {}

  bindContext(context: ApplicationContext): Map<string, RetainedIdentitySave> {
    const lifetime = identityWorkFor(context);
    if (this.lifetime && this.lifetime !== lifetime) {
      resetIdentityDraft(this.host);
    }
    this.lifetime = lifetime;
    this.identities = this.lifetime.identities;
    this.profileId = this.lifetime.profileId;
    return this.lifetime.saves;
  }

  retainCurrentIdentity() {
    const { host } = this;
    if (
      host.agentsSelectedId &&
      Object.values(host.identityDraft).some((value) => value !== null)
    ) {
      this.identities.set(host.agentsSelectedId, {
        profileId: this.profileId,
        draft: host.identityDraft,
      });
    }
  }

  observeGateway(snapshot: Pick<ApplicationGatewaySnapshot, "phase" | "selfUser">) {
    if (snapshot.phase !== "connected") {
      return;
    }
    const profileId = snapshot.selfUser?.id ?? null;
    if (profileId !== this.profileId) {
      this.identities.clear();
      this.lifetime?.saves.clear();
      resetIdentityDraft(this.host);
      this.profileId = profileId;
      if (this.lifetime) {
        this.lifetime.profileId = profileId;
      }
    }
  }

  private restoreIdentity(selectedId: string | null, profileId: string | null): boolean {
    if (!selectedId) {
      return false;
    }
    const retained = this.identities.get(selectedId);
    if (!retained) {
      return false;
    }
    if (retained.profileId !== profileId) {
      if (profileId !== null) {
        this.identities.delete(selectedId);
      }
      return false;
    }
    this.identities.delete(selectedId);
    this.host.identityDraft = retained.draft;
    return true;
  }

  select(selection: Selection) {
    const { host } = this;
    const { selectedId, profileId } = selection;
    if (selectedId === host.agentsSelectedId) {
      return this.restoreIdentity(selectedId, profileId);
    }
    if (host.agentsSelectedId) {
      const drafts = retainAgentFileDrafts(host);
      if (drafts) {
        this.files.set(host.agentsSelectedId, drafts);
      }
      this.retainCurrentIdentity();
    }
    host.agentsSelectedId = selectedId;
    this.resetSelection();
    const restoredIdentity = this.restoreIdentity(selectedId, profileId);
    const files = selectedId ? this.files.get(selectedId) : undefined;
    if (files && selectedId) {
      this.files.delete(selectedId);
      host.agentFileEditors = files.editors;
      host.agentFileActive = files.active;
      host.agentFileConflict = files.conflict;
      // Loaded bases stay empty: returning must read disk while retaining the draft's ancestry.
    }
    return restoredIdentity;
  }

  clear() {
    this.files.clear();
    this.identities.clear();
    this.lifetime?.saves.clear();
  }
}
