type TerminalProducerFence = () => Promise<boolean>;

/** Tracks exact persisted-writer fences while their producers can still commit. */
export function createTerminalProducerFenceRegistry(isOwnerSettled: () => boolean) {
  const fences = new Set<TerminalProducerFence>();
  let blocked = false;

  return {
    register(fence: TerminalProducerFence): () => void {
      if (isOwnerSettled()) {
        throw new Error("Operation already settled");
      }
      fences.add(fence);
      return () => fences.delete(fence);
    },
    async revokeAll(): Promise<{ failures: unknown[]; fenced: boolean }> {
      const failures: unknown[] = [];
      let fenced = fences.size > 0;
      for (const fence of fences) {
        try {
          fenced = (await fence()) && fenced;
        } catch (error) {
          fenced = false;
          failures.push(error);
        }
      }
      blocked = !fenced;
      return { failures, fenced };
    },
    get blocked(): boolean {
      return blocked;
    },
  };
}
