import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { expect, it } from "vitest";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { resolveSchemaMigrationBackupTables } from "../../state/schema-migration-backup-scope.js";
import { loadStatePortDescriptor } from "./state-port-descriptor.js";
import { resolveStatePortRoots } from "./state-port.js";

const stateDir = path.resolve("/synthetic/selected");
const home = path.resolve("/synthetic/home");
const env = {
  ...process.env,
  HOME: home,
  OPENCLAW_STATE_DIR: "/unselected",
  OPENCLAW_DISABLE_BUNDLED_PLUGINS: "1",
};

it.each<{
  label: string;
  config: OpenClawConfig;
  roots: string[];
}>([
  { label: "selected and retained default", config: {}, roots: [] },
  {
    label: "disabled custom store",
    config: {
      plugins: { entries: { "voice-call": { enabled: false, config: { store: "~/calls" } } } },
    },
    roots: [path.join(home, "calls")],
  },
  {
    label: "both configured package aliases",
    config: {
      plugins: {
        enabled: false,
        entries: {
          "voice-call": { config: { store: path.join(home, "first") } },
          "@openclaw/voice-call": { config: { store: path.join(home, "second") } },
        },
      },
    },
    roots: [path.join(home, "first"), path.join(home, "second")],
  },
  {
    label: "duplicate custom store and excluded Matrix account roots",
    config: {
      plugins: {
        entries: {
          "voice-call": { config: { store: path.join(stateDir, "voice-calls") } },
        },
      },
      channels: {
        matrix: {
          homeserver: "https://matrix.example.test",
          accounts: {
            main: { userId: "@main:example.test" },
            retained: { enabled: false, userId: "@retained:example.test" },
          },
        },
      },
    },
    roots: [],
  },
])("discovers $label with the optional plugin absent", ({ config, roots }) => {
  expect(resolveStatePortRoots({ config, stateDir, env })).toEqual(
    [stateDir, path.join(stateDir, "voice-calls"), ...roots].map((root) =>
      path.join(root, "state", "openclaw.sqlite"),
    ),
  );
});

it("retains the exact predecessor credential tables for PostgreSQL update recovery", async () => {
  const predecessor = await loadStatePortDescriptor(26);
  const current = await loadStatePortDescriptor();
  const tables = ["device_identities", "device_auth_tokens", "gateway_origin_device_tokens"];
  const db = new DatabaseSync(":memory:");
  try {
    db.exec(predecessor.sqlite);
    db.prepare(`INSERT INTO device_identities VALUES (?, ?, ?, ?, ?, ?)`).run(
      "primary",
      "retained-device",
      "public-bytes",
      "private-bytes",
      1,
      2,
    );
    expect(db.prepare("SELECT private_key_pem FROM device_identities").get()).toEqual({
      private_key_pem: "private-bytes",
    });
    for (const table of tables) {
      expect(predecessor.tables.some((entry) => entry.name === table)).toBe(true);
      expect(predecessor.preData).toContain(`CREATE TABLE "${table}"`);
      expect(current.tables.some((entry) => entry.name === table)).toBe(false);
      expect(current.preData).not.toContain(`CREATE TABLE "${table}"`);
    }
    expect(current.tables.some((entry) => entry.name === "exec_approvals_config")).toBe(true);
  } finally {
    db.close();
  }
});

it("captures only the credential retirement scope plus required schema metadata and statistics", () => {
  expect(
    resolveSchemaMigrationBackupTables({ kind: "state", foundVersion: 26, supportedVersion: 27 }),
  ).toEqual([
    "config_machine_state",
    "device_auth_tokens",
    "device_identities",
    "exec_approvals_config",
    "gateway_origin_device_tokens",
    "schema_meta",
    "sqlite_stat1",
    "sqlite_stat4",
  ]);
});
