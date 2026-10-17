import type { SidebarRecentSession } from "./app-sidebar-session-types.ts";

/** Row lifetimes bound retained projections; no roster-sized invalidation table. */
export class SidebarSessionRowCache {
  private readonly entries = new WeakMap<
    object,
    {
      inputs: readonly unknown[];
      value: SidebarRecentSession;
    }
  >();

  read(
    row: object,
    inputs: readonly unknown[],
    project: () => SidebarRecentSession,
  ): SidebarRecentSession {
    const entry = this.entries.get(row);
    if (
      entry &&
      entry.inputs.length === inputs.length &&
      inputs.every((value, index) => Object.is(value, entry.inputs[index]))
    ) {
      return entry.value;
    }
    const value = project();
    this.entries.set(row, { inputs, value });
    return value;
  }
}
