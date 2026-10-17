import { setImmediate as nextTurn } from "node:timers/promises";
import { applyRemoteModelCatalogUpdate } from "../agents/prepared-model-runtime.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import type { GatewayActiveWorkInspectors } from "../infra/gateway-active-work.js";
import type { GatewayScheduler } from "../infra/gateway-scheduler.js";
import { createGatewayUpdateLifecycle } from "../infra/update-check-lifecycle.js";
import type { startGatewayUpdateChecks } from "../infra/update-startup.js";
import type { PluginRegistry } from "../plugins/registry.js";
import { withPluginRuntimeRegistryScope } from "../plugins/runtime/gateway-request-scope.js";
import { runWithGatewayIndependentRootWorkAdmission } from "../process/gateway-work-admission.js";
import {
  canReadDetailedUpdateMetadata,
  GATEWAY_EVENT_UPDATE_AVAILABLE,
  projectUpdateAvailable,
  type GatewayUpdateAvailableEventPayload,
} from "./events.js";
import type { GatewayBroadcastToConnIdsFn } from "./server-broadcast-types.js";
import type { GatewayClient } from "./server-methods/shared-types.js";
import { measureStartup, type GatewayStartupTrace } from "./server-startup-trace.js";
import { startUpdateRunWatcher, wakeUpdateRunWatcher } from "./update-run-watcher.js";

export function createDeferredGatewayUpdateCheck(params: {
  scheduler: GatewayScheduler;
  startupTrace?: GatewayStartupTrace;
  loadUpdateChecks: () =>
    | typeof startGatewayUpdateChecks
    | Promise<typeof startGatewayUpdateChecks>;
  getConfig: () => OpenClawConfig;
  /** Catalog republication borrows unchanged plugin instances from this live Gateway registry. */
  getPluginRegistry: () => PluginRegistry | undefined;
  log: {
    info: (msg: string) => void;
    warn: (msg: string) => void;
  };
  isNixMode: boolean;
  broadcastToConnIds: GatewayBroadcastToConnIdsFn;
  getClientConnIds: (filter?: (client: GatewayClient) => boolean) => ReadonlySet<string>;
  waitForPostReadyWork?: () => Promise<void>;
  isClosing?: () => boolean;
  activeWorkInspectors?: Partial<GatewayActiveWorkInspectors>;
}): { start: () => void; stop: () => Promise<void> } {
  let stopped = false;
  let started = false;
  let runWatcher: ReturnType<typeof startUpdateRunWatcher> | undefined;
  let stopPromise: Promise<void> | undefined;
  let latestUpdateAvailable: GatewayUpdateAvailableEventPayload["updateAvailable"] = null;
  let latestSchedule: GatewayUpdateAvailableEventPayload["schedule"];

  const broadcastUpdateAvailable = (payload: GatewayUpdateAvailableEventPayload) => {
    if (stopped || params.isClosing?.()) {
      return;
    }
    const detailedConnIds = params.getClientConnIds((client) =>
      canReadDetailedUpdateMetadata(client.connect.role ?? "operator", client.connect.scopes ?? []),
    );
    const legacyConnIds = new Set(params.getClientConnIds());
    for (const connId of detailedConnIds) {
      legacyConnIds.delete(connId);
    }
    params.broadcastToConnIds(GATEWAY_EVENT_UPDATE_AVAILABLE, payload, detailedConnIds, {
      dropIfSlow: true,
    });
    params.broadcastToConnIds(
      GATEWAY_EVENT_UPDATE_AVAILABLE,
      { updateAvailable: projectUpdateAvailable(payload.updateAvailable, false) ?? null },
      legacyConnIds,
      { dropIfSlow: true },
    );
  };

  // Early RPC discovery shares the same cancellation and publication owner,
  // even before the post-ready background checks have been admitted.
  const lifecycle = createGatewayUpdateLifecycle(params.scheduler, {
    onUpdateAvailableChange: (updateAvailable) => {
      latestUpdateAvailable = updateAvailable;
      const payload: GatewayUpdateAvailableEventPayload = {
        updateAvailable,
        ...(latestSchedule ? { schedule: latestSchedule } : {}),
      };
      broadcastUpdateAvailable(payload);
    },
    onUpdateScheduleChange: (schedule) => {
      latestSchedule = schedule;
      const payload: GatewayUpdateAvailableEventPayload = {
        updateAvailable: latestUpdateAvailable,
        schedule,
      };
      broadcastUpdateAvailable(payload);
    },
  });

  const stop = () => {
    stopped = true;
    return (stopPromise ??= (async () => {
      await Promise.all([lifecycle.stop(), runWatcher?.stop()]);
    })());
  };

  const start = () => {
    if (started || stopped) {
      return;
    }
    started = true;
    runWatcher = startUpdateRunWatcher({
      lifecycle,
      broadcast: (event, payload) =>
        params.broadcastToConnIds(event, payload, params.getClientConnIds()),
      log: params.log,
    });
    void (async () => {
      if (params.waitForPostReadyWork) {
        await params.waitForPostReadyWork();
        await nextTurn();
      }
      if (stopped || params.isClosing?.()) {
        return;
      }
      // Own admitted module loading and startup, but never the unreleased ready barrier.
      await lifecycle.run(async (signal) => {
        let startUpdateChecks: typeof startGatewayUpdateChecks;
        try {
          startUpdateChecks = await params.loadUpdateChecks();
        } catch (err) {
          if (!stopped) {
            params.log.warn(`gateway update check failed to initialize: ${String(err)}`);
          }
          return;
        }
        if (signal.aborted || params.isClosing?.()) {
          return;
        }
        void lifecycle
          .initialize()
          .then((result) => {
            if (result.status.error) {
              throw new Error(result.status.error.message);
            }
          })
          .catch((err: unknown) => {
            if (!stopped) {
              params.log.warn(`gateway update status failed to initialize: ${String(err)}`);
            }
          });
        await nextTurn();
        if (!stopped && !params.isClosing?.()) {
          await runWithGatewayIndependentRootWorkAdmission(
            async () => {
              if (signal.aborted || params.isClosing?.()) {
                return;
              }
              await measureStartup(params.startupTrace, "post-attach.update-check", () =>
                startUpdateChecks({
                  lifecycle,
                  getConfig: params.getConfig,
                  applyRemoteCatalogUpdate: (updateSignal) =>
                    withPluginRuntimeRegistryScope(params.getPluginRegistry(), () =>
                      applyRemoteModelCatalogUpdate(params.getConfig, updateSignal),
                    ),
                  onUpdateRunCreated: wakeUpdateRunWatcher,
                  log: params.log,
                  isNixMode: params.isNixMode,
                  ...(params.activeWorkInspectors
                    ? { activeWorkInspectors: params.activeWorkInspectors }
                    : {}),
                  onUpdateAvailableChange: lifecycle.onUpdateAvailableChange,
                  onUpdateScheduleChange: lifecycle.onUpdateScheduleChange,
                }),
              );
            },
            "startup:update-check",
            signal,
          );
        }
      });
    })().catch((err: unknown) => {
      if (!stopped) {
        params.log.warn(`gateway update check readiness wait failed: ${String(err)}`);
      }
    });
  };

  return { start, stop };
}
