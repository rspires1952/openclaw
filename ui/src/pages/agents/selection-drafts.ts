import type { ApplicationGatewaySnapshot } from "../../app/context.ts";
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

/** Owns private editor drafts across the selected target's presentation lifetime. */
export class AgentSelectionDrafts {
  private readonly files = new Map<string, RetainedAgentFileDrafts>();
  private readonly identities = new Map<
    string,
    { profileId: string | null; draft: AgentIdentityDraft }
  >();
  private profileId: string | null = null;

  constructor(
    private readonly host: AgentSelectionDraftHost,
    private readonly resetSelection: () => void,
  ) {}

  observeGateway(snapshot: Pick<ApplicationGatewaySnapshot, "phase" | "selfUser">) {
    if (snapshot.phase !== "connected") {
      return;
    }
    const profileId = snapshot.selfUser?.id ?? null;
    if (profileId !== this.profileId) {
      this.identities.clear();
      resetIdentityDraft(this.host);
      this.profileId = profileId;
    }
  }

  select(selection: Selection) {
    const { host } = this;
    const { selectedId, profileId } = selection;
    if (selectedId === host.agentsSelectedId) {
      return false;
    }
    if (host.agentsSelectedId) {
      const drafts = retainAgentFileDrafts(host);
      if (drafts) {
        this.files.set(host.agentsSelectedId, drafts);
      }
      if (Object.values(host.identityDraft).some((value) => value !== null)) {
        this.identities.set(host.agentsSelectedId, {
          profileId: this.profileId,
          draft: host.identityDraft,
        });
      }
    }
    host.agentsSelectedId = selectedId;
    this.resetSelection();
    let restoredIdentity = false;
    if (selectedId) {
      const retained = this.identities.get(selectedId);
      this.identities.delete(selectedId);
      if (retained?.profileId === profileId) {
        host.identityDraft = retained.draft;
        restoredIdentity = true;
      }
    }
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
  }
}
