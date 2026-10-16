import fs from "node:fs";
import path from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { createSolidPngBuffer } from "../../../test/helpers/image-fixtures.js";
import { writeJsonFile } from "../../../test/helpers/temp-repo.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import * as mediaStore from "../../media/store.js";
import * as webMedia from "../../media/web-media.js";
import { acquirePluginRegistryForInspection } from "../../plugins/loader.js";
import * as pluginFixtures from "../../plugins/loader.test-fixtures.js";
import { setActivePluginRegistry } from "../../plugins/runtime.js";
import { withEnvAsync } from "../../test-utils/env.js";
import * as taskRuntime from "../media-generation-task-status.js";
import { prepareWorkspaceBuildGroup } from "../prepared-model-runtime.facts.js";
import { discardPreparedPluginGeneration } from "../prepared-model-runtime.plugin-lifetime.js";
import { runWithAgentWorkspaceReadiness } from "../workspace-readiness.js";
import { createImageGenerateTool } from "./image-generate-tool.js";
import * as background from "./media-generate-background.js";
import * as media from "./media-resource-fixtures.test-support.js";
import { createMusicGenerateTool } from "./music-generate-tool.js";
import { createVideoGenerateTool } from "./video-generate-tool.js";

const png = createSolidPngBuffer(1, 1, { r: 0, g: 0, b: 0 });

const mp4 = Buffer.from("000000186674797069736f6d0000020069736f6d6d703431", "hex");
type GenerationKind = "image" | "music" | "video";
type NativeCounters = Record<"disposals" | "generated" | "projected", number>;
type ToolOptions = NonNullable<Parameters<typeof createImageGenerateTool>[0]>;
const sessionKey = "agent:main:discord:direct:synthetic-media";
const persistenceFailure = "synthetic first media persistence failure";

function createNativeFixture(kind: GenerationKind, edit = false, count = 1, holdLookup = false) {
  const dir = pluginFixtures.makePluginLoaderTempDir();
  const key = `__openclaw_prepared_${kind}_${path.basename(dir)}`;
  const connections: Array<NativeCounters & { database: DatabaseSync }> = [];
  const lookup = media.phase();
  if (!holdLookup) {
    lookup.release();
  }
  const generation = media.phase();
  Object.defineProperty(globalThis, key, {
    configurable: true,
    value: {
      connections,
      generation,
      lookup,
      png,
      mp4,
      audio: Buffer.from("synthetic native music 42"),
    },
  });
  const id = `prepared-${kind}-resource`;
  const plugin = pluginFixtures.writePlugin({
    dir,
    id,
    body: `const { DatabaseSync } = require("node:sqlite");
module.exports = {
  id: ${JSON.stringify(id)},
  register(api) {
    const state = globalThis[${JSON.stringify(key)}];
    const database = new DatabaseSync(":memory:");
    const connection = { database, disposals: 0, generated: 0, projected: 0 };
    state.connections.push(connection);
    const read = () => database.prepare("SELECT 42 AS value").get().value;
    api.lifecycle.registerRuntimeLifecycle({
      id: "prepared-media-database",
      dispose() {
        read();
        connection.disposals++;
        database.close();
      },
    });
    api.${{ image: "registerImageGenerationProvider", music: "registerMusicGenerationProvider", video: "registerVideoGenerationProvider" }[kind]}({
      id: ${JSON.stringify(id)},
      defaultModel: "fixture-${kind}",
      isConfigured() { return read() === 42; },
      capabilities: ${JSON.stringify({
        generate: { [{ image: "maxCount", music: "maxTracks", video: "maxVideos" }[kind]]: 2 },
        [kind === "video" ? "imageToVideo" : "edit"]: {
          enabled: edit,
          maxInputImages: edit ? 1 : 0,
        },
      })},
${
  kind === "video"
    ? `      async resolveModelCapabilities() {
        read();
        await state.lookup.pause();
        read();
        return {};
      },`
    : ""
}
      async ${{ image: "generateImage", music: "generateMusic", video: "generateVideo" }[kind]}(request) {
        read();
        connection.generated++;
        await state.generation.pause();
        read();
        return {
          ${{ image: "images", music: "tracks", video: "videos" }[kind]}: Array.from({ length: ${kind === "image" ? "request.count ?? 1" : count} }, (_, index) => ({
            buffer: state.${{ image: "png", music: "audio", video: "mp4" }[kind]},
            mimeType: "${{ image: "image/png", music: "audio/mpeg", video: "video/mp4" }[kind]}",
${
  kind === "music"
    ? `            get fileName() {
              connection.projected++;
              return "native-" + index + "-" + read() + ".mp3";
            },`
    : `            fileName: "native-" + index + ".${kind === "image" ? "png" : "mp4"}",`
}
${
  kind === "image"
    ? `            get revisedPrompt() {
              connection.projected++;
              return "native value " + read();
            },`
    : ""
}
          })),
${
  kind === "video"
    ? `          metadata: {
            supportedDurationSeconds: Object.assign([1], {
              filter(predicate) {
                connection.projected++;
                read();
                return Array.prototype.filter.call(this, predicate);
              },
            }),
          },`
    : ""
}
        };
      },
    });
  },
};`,
  });
  writeJsonFile(path.join(dir, "openclaw.plugin.json"), {
    id,
    contracts: { [`${kind}GenerationProviders`]: [id] },
    configSchema: { type: "object", additionalProperties: false, properties: {} },
  });
  const config: OpenClawConfig = {
    agents: { defaults: { mediaModels: { [kind]: { primary: `${id}/fixture-${kind}` } } } },
    plugins: { allow: [id], load: { paths: [plugin.file] }, slots: { memory: "none" } },
  };
  return {
    toolOptions(
      snapshot: ReturnType<typeof media.snapshot>,
      schedule: ToolOptions["scheduleBackgroundWork"],
      toolConfig = config,
    ): ToolOptions {
      return {
        config: toolConfig,
        agentDir: snapshot.agentDir,
        workspaceDir: dir,
        preparedModelRuntime: snapshot,
        agentSessionKey: sessionKey,
        scheduleBackgroundWork: schedule,
      };
    },
    taskHandle: {
      taskId: "reference-task",
      runId: "reference-run",
      detach: true,
      requesterSessionKey: sessionKey,
      taskLabel: "Synthetic reference",
    },
    dir,
    config,
    output:
      kind === "image" ? png : kind === "video" ? mp4 : Buffer.from("synthetic native music 42"),
    connections,
    generation,
    lookup,
    withEnvironment: (run: () => Promise<void>) =>
      withEnvAsync(
        {
          OPENCLAW_HOME: dir,
          OPENCLAW_STATE_DIR: dir,
          OPENCLAW_CONFIG_PATH: path.join(dir, "config.json"),
        },
        run,
      ),
    cleanup() {
      lookup.release();
      generation.release();
      for (const { database } of connections) {
        if (database.isOpen) {
          database.close();
        }
      }
      Reflect.deleteProperty(globalThis, key);
    },
  };
}

async function withPreparedFixture(
  fixture: ReturnType<typeof createNativeFixture>,
  run: (prepared: {
    snapshot: ReturnType<typeof media.snapshot>;
    releaseParent: () => Promise<void>;
  }) => Promise<void>,
) {
  try {
    await fixture.withEnvironment(async () => {
      pluginFixtures.useNoBundledPlugins();
      const first = await acquirePluginRegistryForInspection({ config: fixture.config });
      let prepared: Awaited<ReturnType<typeof prepareWorkspaceBuildGroup>> | undefined;
      const releaseParent = async () => {
        await first.release();
        if (prepared) {
          await discardPreparedPluginGeneration(prepared.pluginGeneration);
        }
      };
      try {
        prepared = await prepareWorkspaceBuildGroup(
          [
            {
              agentId: "main",
              agentDir: path.join(fixture.dir, "agent"),
              workspaceDir: fixture.dir,
              config: fixture.config,
              skipCredentials: true,
            },
          ],
          "static",
          { includeCredentialProviders: false },
          () => first.registry,
        );
        await run({ snapshot: media.snapshot(prepared), releaseParent });
      } finally {
        await releaseParent();
      }
    });
  } finally {
    fixture.cleanup();
  }
}

afterEach(() => {
  media.reset();
  pluginFixtures.resetPluginLoaderTestStateForTest();
});
afterAll(pluginFixtures.cleanupPluginLoaderFixturesForTest);

describe.each(["image", "music", "video"] as const)(
  "prepared %s job registration resources",
  (kind) => {
    const createTool = {
      image: createImageGenerateTool,
      music: createMusicGenerateTool,
      video: createVideoGenerateTool,
    }[kind];
    const lifecycle = {
      image: background.imageGenerationTaskLifecycle,
      music: background.musicGenerationTaskLifecycle,
      video: background.videoGenerationTaskLifecycle,
    }[kind];
    it("waits for the captured workspace before reading a generation reference", async () => {
      const fixture = createNativeFixture(kind, true);
      const workspace = media.phase();
      await withPreparedFixture(fixture, async (prepared) => {
        const referencePath = path.join(fixture.dir, "pending-reference.png");
        const createTask = vi
          .spyOn(lifecycle, "createTaskRun")
          .mockResolvedValue(fixture.taskHandle);
        const schedule = vi.fn();
        const readReference = vi.spyOn(webMedia, "loadWebMedia");
        const tool = await runWithAgentWorkspaceReadiness(
          {
            sessionKey,
            waitUntilReady: workspace.pause,
            assertCurrent: () => {},
          },
          async () => createTool(fixture.toolOptions(prepared.snapshot, schedule)),
        );
        const outcome = tool!.execute("workspace-reference", {
          prompt: "Use the synthetic workspace reference",
          image: referencePath,
        });
        try {
          await media.waitForPhase(workspace.entered, outcome);
          expect(readReference).not.toHaveBeenCalled();
          expect(createTask).not.toHaveBeenCalled();
          const inline = await tool!.execute("inline-reference", {
            prompt: "Use an inline reference while the workspace is pending",
            image: `data:image/png;base64,${png.toString("base64")}`,
          });
          expect(inline.details).toMatchObject({ status: "started" });
          fs.writeFileSync(referencePath, png);
          workspace.release();
          expect((await outcome).details).toMatchObject({ status: "started" });
          expect(readReference).toHaveBeenCalledOnce();
          expect(schedule).toHaveBeenCalledTimes(2);
        } finally {
          workspace.release();
          await outcome.catch(() => {});
        }
      });
    });
    it.each(["request lookup", "duplicate lookup", "reference loading"] as const)(
      "refuses preflight work when the prepared owner releases during %s",
      async (pause) => {
        const fixture = createNativeFixture(kind, true);
        const preflight = media.phase();
        await withPreparedFixture(fixture, async (prepared) => {
          const referencePath = path.join(fixture.dir, "reference.png");
          fs.writeFileSync(referencePath, png);
          const { snapshot } = prepared;
          const createTask = vi
            .spyOn(lifecycle, "createTaskRun")
            .mockResolvedValue(fixture.taskHandle);
          const schedule = vi.fn();
          const loadReference = webMedia.loadWebMedia;
          const reference = vi
            .spyOn(webMedia, "loadWebMedia")
            .mockImplementation(async (...args) => {
              if (pause === "reference loading") {
                await preflight.pause();
              }
              return loadReference(...args);
            });
          const lookup = {
            image: "findDuplicateGuardImageGenerationTaskForSession",
            music: "findDuplicateGuardMusicGenerationTaskForSession",
            video: "findDuplicateGuardVideoGenerationTaskForSession",
          } as const;
          const readTasks = taskRuntime[lookup[kind]];
          let lookups = 0;
          vi.spyOn(taskRuntime, lookup[kind]).mockImplementation(async (ownerKey, options) => {
            const tasks = await readTasks(ownerKey, options);
            if (
              pause !== "reference loading" &&
              ++lookups === (pause === "request lookup" ? 1 : 2)
            ) {
              await preflight.pause();
            }
            return tasks;
          });
          const tool = createTool(
            fixture.toolOptions(snapshot, schedule, {
              ...fixture.config,
              agents: pause === "request lookup" ? undefined : fixture.config.agents,
            }),
          );
          const outcome = tool!
            .execute("preflight-image-call", {
              prompt: "Synthetic media edit",
              image: referencePath,
            })
            .then(
              (value) => ({ value, error: undefined }),
              (error: unknown) => ({ value: undefined, error }),
            );
          try {
            await media.waitForPhase(preflight.entered, outcome);
            await prepared.releaseParent();
            preflight.release();
            const result = await outcome;
            expect(result).toMatchObject({ error: expect.any(Error), value: undefined });
            if (pause !== "reference loading") {
              expect(reference).not.toHaveBeenCalled();
            }
            if (pause === "request lookup") {
              expect(lookups).toBe(1);
            }
            expect(createTask).not.toHaveBeenCalled();
            expect(schedule).not.toHaveBeenCalled();
            expect(fixture.connections[0]).toMatchObject({
              generated: 0,
              database: { isOpen: false },
              disposals: 1,
            });
          } finally {
            preflight.release();
            await outcome;
          }
        });
      },
    );

    it.each(
      kind === "video"
        ? (["queued", "lookup", "saving", "rollback"] as const)
        : (["queued", "saving", "rollback"] as const),
    )("retains the admitted provider through %s after its parent releases", async (phase) => {
      const count = phase === "rollback" ? 2 : 1;
      const fixture = createNativeFixture(kind, false, count, phase === "lookup");
      const saving = media.phase();
      const rollback = media.phase();
      const scheduled: Array<() => Promise<void>> = [];
      let completion: Promise<void> | undefined;
      await withPreparedFixture(fixture, async (prepared) => {
        let successor: Awaited<ReturnType<typeof acquirePluginRegistryForInspection>> | undefined;
        try {
          const { snapshot } = prepared;
          expect(snapshot.mediaCapabilityProviders?.[`${kind}GenerationProviders`]).toHaveLength(1);
          const connection = fixture.connections[0]!;
          const { completed, failed } = media.admitTask(lifecycle, sessionKey);
          const save = mediaStore.saveMediaBuffer;
          const remove = mediaStore.deleteMediaBuffer;
          let saveCalls = 0;
          let savedPath: string | undefined;
          vi.spyOn(mediaStore, "saveMediaBuffer").mockImplementation(async (...args) => {
            saveCalls++;
            if (phase === "rollback" && saveCalls === (kind === "video" ? 2 : 1)) {
              throw new Error(persistenceFailure);
            }
            await saving.pause();
            const saved = await save(...args);
            savedPath = saved.path;
            return saved;
          });
          vi.spyOn(mediaStore, "deleteMediaBuffer").mockImplementation(async (...args) => {
            await rollback.pause();
            return remove(...args);
          });
          const tool = createTool(fixture.toolOptions(snapshot, (work) => scheduled.push(work)));
          expect(tool).not.toBeNull();
          const started = await tool!.execute("image-resource-call", {
            prompt: "Synthetic media resource proof",
            count,
          });
          expect(started.details).toMatchObject({ status: "started" });
          expect(scheduled).toHaveLength(1);
          expect(connection.generated).toBe(0);
          if (phase === "queued") {
            await prepared.releaseParent();
          }
          successor = await acquirePluginRegistryForInspection({ config: fixture.config });
          setActivePluginRegistry(successor.registry);
          expect(fixture.connections).toHaveLength(2);
          if (phase === "queued") {
            expect(connection.database.isOpen).toBe(true);
          }
          completion = scheduled[0]!();
          if (phase === "lookup") {
            await media.waitForPhase(fixture.lookup.entered, completion);
            await prepared.releaseParent();
            expect(connection.database.isOpen).toBe(true);
            expect(connection.generated).toBe(0);
            fixture.lookup.release();
          }
          await media.waitForPhase(fixture.generation.entered, completion);
          fixture.generation.release();
          await media.waitForPhase(saving.entered, completion);
          if (kind !== "video" || phase !== "rollback") {
            await prepared.releaseParent();
          }
          expect(connection).toMatchObject({ database: { isOpen: true }, disposals: 0 });
          expect(fixture.connections[1]!.generated).toBe(0);
          saving.release();
          if (phase === "rollback") {
            await rollback.entered;
            if (kind === "video") {
              await prepared.releaseParent();
            }
            expect(connection.database.isOpen).toBe(true);
            expect(savedPath && fs.existsSync(savedPath)).toBe(true);
            rollback.release();
          }
          await completion;
          expect(connection).toMatchObject({ database: { isOpen: false }, disposals: 1 });
          expect(fixture.connections[1]!.database.isOpen).toBe(true);
          if (phase === "rollback") {
            expect(failed).toHaveBeenCalledWith(
              expect.objectContaining({
                error: expect.objectContaining({
                  message: persistenceFailure,
                }),
              }),
            );
            expect(completed).not.toHaveBeenCalled();
            expect(savedPath && fs.existsSync(savedPath)).toBe(false);
          } else {
            expect(failed).not.toHaveBeenCalled();
            expect(completed).toHaveBeenCalledOnce();
            expect(connection.projected).toBeGreaterThan(0);
            expect(fs.readFileSync(savedPath!)).toEqual(fixture.output);
          }
        } finally {
          fixture.lookup.release();
          fixture.generation.release();
          saving.release();
          rollback.release();
          await completion;
          await prepared.releaseParent();
          await successor?.release();
        }
      });
    });
  },
);
