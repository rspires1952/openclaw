import { defaultRuntime } from "../../../runtime.js";
import { completeSessionControllerSourceLifecycle } from "../../../sessions/session-controller.mailbox-source.js";
import { submitSessionControllerInput } from "../../../sessions/session-controller.mailbox.js";
import { FollowupRunDeferredError, type FollowupRun } from "./types.js";

function custody(run: FollowupRun) {
  if (run.controllerClaim) {
    return run.controllerClaim.custody;
  }
  return (
    run.controllerInput ??
    submitSessionControllerInput(run.run.sessionKey ?? run.run.sessionId, run, { mode: "followup" })
  ).custody;
}

// Native consumption can finish without adoption; disposal alone cannot infer that outcome.
const abandonedDeliveryCorrelations = new WeakMap<ReturnType<typeof custody>, () => void>();

function retainQueuedDeliveryCorrelations(
  run: FollowupRun,
  state: ReturnType<typeof custody>,
): void {
  if (!run.deliveryCorrelations?.length) {
    return;
  }
  const abandon: Array<() => void> = [];
  run.deliveryCorrelations = run.deliveryCorrelations.map((correlation) => {
    let pending = true;
    abandon.push(() => {
      if (!pending) {
        return;
      }
      pending = false;
      try {
        const end = correlation.begin();
        if (typeof end === "function") {
          end({ visibleReplySent: false, failed: true });
        }
      } catch (error) {
        defaultRuntime.error?.(
          `followup queue: delivery correlation cleanup failed: ${String(error)}`,
        );
      }
    });
    return {
      ...correlation,
      begin: () => {
        pending = false;
        const end = correlation.begin();
        if (typeof end !== "function") {
          return end;
        }
        return (settlement: Parameters<typeof end>[0]) => {
          // A retry hands ownership back; a final delivery outcome must not be repeated by Stop.
          pending = settlement === undefined;
          end(settlement);
        };
      },
    };
  });
  abandonedDeliveryCorrelations.set(state, () => {
    for (const notify of abandon) {
      notify();
    }
  });
  const disposeSource = state.disposeSource;
  state.disposeSource = () => {
    const notify = abandonedDeliveryCorrelations.get(state);
    abandonedDeliveryCorrelations.delete(state);
    try {
      disposeSource?.();
    } finally {
      if (!state.adopted) {
        notify?.();
      }
    }
  };
}

export function updateFollowupRetryHeartbeats(
  error: unknown,
  sources: readonly FollowupRun[],
): void {
  for (const source of sources) {
    if (error instanceof FollowupRunDeferredError) {
      startFollowupRunPreAdoptionHeartbeat(source);
    } else {
      custody(source).stopHeartbeat?.();
    }
  }
}

export function startFollowupRunPreAdoptionHeartbeat(
  run: FollowupRun,
  abortSignal?: AbortSignal,
): (() => void) | undefined {
  const lifecycle = run.turnAdoptionLifecycle;
  if (!lifecycle) {
    return undefined;
  }
  const intervalMs = lifecycle.deferredHeartbeatIntervalMs;
  const heartbeat = lifecycle.onDeferredHeartbeat;
  if (
    !heartbeat ||
    !intervalMs ||
    !Number.isFinite(intervalMs) ||
    intervalMs <= 0 ||
    lifecycle.abortSignal?.aborted ||
    abortSignal?.aborted
  ) {
    return undefined;
  }
  const state = custody(run);
  if (state.adopted || state.completed) {
    return undefined;
  }
  state.stopHeartbeat?.();
  const stop = () => {
    clearInterval(timer);
    lifecycle.abortSignal?.removeEventListener("abort", stop);
    abortSignal?.removeEventListener("abort", stop);
    if (state.stopHeartbeat === stop) {
      state.stopHeartbeat = undefined;
    }
  };
  const pulse = () => {
    try {
      heartbeat();
    } catch {
      stop();
    }
  };
  const timer = setInterval(pulse, intervalMs).unref();
  state.stopHeartbeat = stop;
  lifecycle.abortSignal?.addEventListener("abort", stop, { once: true });
  abortSignal?.addEventListener("abort", stop, { once: true });
  pulse();
  return stop;
}
export function markFollowupRunEnqueued(run: FollowupRun): boolean {
  const state = custody(run);
  const authority = run.operatorAuthority;
  authority?.signal?.throwIfAborted();
  authority?.assertCurrent();
  if (!state.enqueued) {
    if (run.turnAdoptionLifecycle?.onDeferred?.() === false) {
      return false;
    }
    try {
      state.releaseAuthority = authority?.retain?.();
    } catch (error) {
      completeFollowupRunLifecycle(run);
      throw error;
    }
    retainQueuedDeliveryCorrelations(run, state);
    state.enqueued = true;
    startFollowupRunPreAdoptionHeartbeat(run);
  }
  return true;
}
export function retireFollowupRunCancellation(run: FollowupRun): void {
  const state = custody(run);
  if (state.cancellationRetired) {
    return;
  }
  state.cancellationRetired = true;
  run.turnAdoptionLifecycle?.onCancellationRetired?.();
}
export async function admitFollowupRunLifecycle(run: FollowupRun): Promise<void> {
  const state = custody(run);
  run.operatorAuthority?.assertCurrent();
  if (state.adopted) {
    return;
  }
  if (state.adopting) {
    return await state.adopting;
  }
  if (state.completed) {
    throw new Error("Input completed before source adoption");
  }
  const admission = Promise.resolve().then(async () => {
    await run.turnAdoptionLifecycle?.onAdopted();
    // A successful custody callback commits before the post-await authority check.
    state.adopted = true;
    state.failure = undefined;
    state.stopHeartbeat?.();
    run.operatorAuthority?.assertCurrent();
  });
  state.adopting = admission;
  try {
    await admission;
  } catch (error) {
    state.failure = error;
    throw error;
  } finally {
    state.adopting = undefined;
  }
}

/** Adapter registers source identity before the canonical custody owner settles it. */
export function completeFollowupRunLifecycle(run: FollowupRun, disposition?: "consumed"): void {
  const state = custody(run);
  if (!state.completed && disposition === "consumed") {
    abandonedDeliveryCorrelations.delete(state);
  }
  completeSessionControllerSourceLifecycle(run, state, disposition);
}

export function completeFollowupRuns(
  items: Iterable<FollowupRun>,
  onError?: (error: unknown) => void,
): void {
  for (const item of items) {
    try {
      completeFollowupRunLifecycle(item);
    } catch (error) {
      if (!onError) {
        throw error;
      }
      onError(error);
    }
  }
}
