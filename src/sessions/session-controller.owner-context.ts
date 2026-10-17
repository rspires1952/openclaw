import { AsyncLocalStorage } from "node:async_hooks";
import { resolveGlobalSingleton } from "../shared/global-singleton.js";
import type { OwnerContext } from "./session-controller.lifecycle.types.js";

// Leaf module: low-level schedulers import the escape below without loading the controller graph.
// Context carries exact controller objects; it grants no independent admission or authority.
export const ownerContext = resolveGlobalSingleton(
  Symbol.for("openclaw.sessionControllerOwnerContext"),
  () => new AsyncLocalStorage<OwnerContext>(),
);

/** Work that outlives the current turn (required cleanup, scheduled or fire-and-forget work)
 * leaves its controller context, so it acquires its own owner instead of inheriting the
 * retired turn. Nested work that belongs to the turn must not use this. */
export function runWithSessionControllerCleanup<T>(run: () => T): T {
  return ownerContext.exit(run);
}
