import type {
  SessionAdmissionDatabaseClaim,
  SessionAdmissionTransition,
} from "../config/sessions/session-accessor.sqlite-entry-admission.js";
import type { SessionEntryTargetPatchScope } from "../config/sessions/session-accessor.types.js";
import type { SessionActor } from "../config/sessions/session-actor-contract.js";
import type { SessionEntryCohortReader } from "../config/sessions/session-entry-read-runtime.types.js";
import type { OpenClawAgentDatabaseIdentity } from "../state/openclaw-agent-db-identity.js";
import type { ReplyBackendHandle, ReplyOperation } from "./session-controller.contracts.js";
import type {
  SessionControllerLifecycle,
  SessionEffectRef,
} from "./session-controller.lifecycle.types.js";
import type { SessionControllerMailbox } from "./session-controller.mailbox.js";
import type { SessionTarget } from "./session-controller.target.js";

export type ReplyRunWaiter = {
  finish: (ended: boolean) => void;
  timer?: NodeJS.Timeout;
};

export type ReplyRunAdmissionSource = {
  sessionId: string;
  sessionIds: Set<string>;
  operation: ReplyOperation;
  databaseIdentity?: OpenClawAgentDatabaseIdentity;
};

export type ReplyRunCompletionObservation = {
  changed: boolean;
  sources: Map<OpenClawAgentDatabaseIdentity | undefined, ReplyRunAdmissionSource>;
};

export type ReplyRunAdmissionBarrier = {
  settled: Promise<void>;
  source: ReplyRunAdmissionSource;
  sources: Map<OpenClawAgentDatabaseIdentity | undefined, ReplyRunAdmissionSource>;
};

export type ReplyOperationAdmission = {
  lease?: SessionEffectRef;
  readonly databaseIdentity?: OpenClawAgentDatabaseIdentity;
  databaseClaim?: SessionAdmissionDatabaseClaim;
  reader?: SessionEntryCohortReader;
  resolveReader?: () => SessionEntryCohortReader | undefined;
  sessionTarget?: SessionEntryTargetPatchScope;
  resolveSessionTarget?: () => SessionEntryTargetPatchScope | undefined;
  sessionActor?: SessionActor;
  acquireSessionActor?: () => Promise<SessionActor | undefined>;
  afterTransition?: (transition: SessionAdmissionTransition) => Promise<void>;
};

/** The exact backend facts owned by one reply operation. Embedded attempts extend
 * this object in place and the runner's run-id index points back to that record. */
type SessionControllerRunAttachment = {
  readonly operation: ReplyOperation;
  backend?: ReplyBackendHandle;
  projectSessionActive?: boolean;
};

/** One retained scheduling owner per canonical key. Native attempts are subordinate
 * to active; waiters, fences and completion observations belong to this same entry.
 * Mailbox and mutation scheduling extend this entry, never a second keyed registry. */
export type SessionControllerEntry = {
  readonly id: string;
  readonly key: string;
  target?: SessionTarget;
  aliases: Set<string>;
  logicalAliases: Set<string>;
  mailbox?: SessionControllerMailbox;
  lifecycle?: SessionControllerLifecycle;
  active?: ReplyOperation;
  attachment?:
    | SessionControllerRunAttachment
    | import("../agents/embedded-agent-runner/run-state.js").EmbeddedRunAttachment;
  sourceTurnId?: string;
  waiters: Set<ReplyRunWaiter>;
  followupBarrier?: ReplyRunAdmissionBarrier;
  successorBarrier?: ReplyRunAdmissionBarrier;
  observations: Set<ReplyRunCompletionObservation>;
};

export type ReplyOperationAfterClear = {
  callbacks: Set<(sessionId: string) => void>;
  barrier?: ReplyRunAdmissionBarrier;
};
export type ReplyOperationSuccessorBarrierGroup = {
  registrationKey: string;
  sources: Map<string, ReplyRunAdmissionSource>;
};
