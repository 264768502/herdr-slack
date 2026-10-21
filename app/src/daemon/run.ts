import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  type ConfigError,
  defaultInstance,
  readConfigFile,
  resolveInstance,
  validateInstance,
  withCredentials,
} from "../config/config.js";
import { stateDir } from "../config/instance.js";
import { detectSecretStore } from "../config/secrets.js";
import { HerdrClient, defaultSocketPath, sessionSocketPath } from "../herdr/client.js";
import { EventTail } from "../herdr/events.js";
import { SessionState } from "../herdr/state.js";
import { SessionRegistry } from "../registry/registry.js";
import { DryRunTransport, formatWrite } from "../slack/dry-run-transport.js";
import { SocketModeTransport } from "../slack/socket-transport.js";
import { Surfaces } from "../slack/surfaces.js";
import type { SlackTransport } from "../slack/transport.js";
import { WebApiTransport } from "../slack/web-api-transport.js";
import { RateBudget } from "./budget.js";
import { HerdBridge } from "./herd-bridge.js";
import { Logger } from "./logger.js";
import { acquireLock, installProcessGuards, onShutdown, writeRecord } from "./supervisor.js";

const VERSION = "0.1.0";

/** Dry-run fallback only — daemon reads absolute path from config (ADR 0002). */
function guessSocketPath(instance: string): string {
  return instance.startsWith("sess-")
    ? sessionSocketPath(instance.slice("sess-".length))
    : defaultSocketPath();
}

export interface RunOptions {
  dryRun?: boolean;
}

async function loadConfig(
  instance: string,
  dryRun: boolean,
  log: Logger,
): Promise<ReturnType<typeof resolveInstance> | null> {
  try {
    const store = await detectSecretStore();
    return await withCredentials(instance, resolveInstance(readConfigFile(), instance), store);
  } catch (error) {
    const e = error as ConfigError;
    if (!dryRun) {
      log.error("config.invalid", { message: e.message, ...(e.fix ? { fix: e.fix } : {}) });
      process.stderr.write(`${e.message}\n${e.fix ? `  fix: ${e.fix}\n` : ""}`);
      return null;
    }
    process.stdout.write(`no usable config (${e.message}); dry run continues with defaults\n`);
    return defaultInstance({ herdrSocketPath: guessSocketPath(instance) });
  }
}

function checkStartable(
  config: ReturnType<typeof resolveInstance>,
  dryRun: boolean,
  log: Logger,
): boolean {
  const problems = validateInstance(config);
  for (const problem of problems) {
    log.error("daemon.refused", { problem, dryRun });
    process.stderr.write(
      `${dryRun ? "not configured for a real start" : "refusing to start"}: ${problem}\n`,
    );
  }
  if (problems.length === 0 || dryRun) return true;
  process.stderr.write("run: herdr-slack doctor\n");
  return false;
}

/**
 * Dry run redirects state to a scratch dir — otherwise it takes the real lock,
 * overwrites daemon.json, and persists synthetic dry-ts-* thread timestamps.
 */
export function redirectStateToScratch(): string {
  const dir = mkdtempSync(path.join(tmpdir(), "herdr-slack-dry-"));
  process.env.HERDR_PLUGIN_STATE_DIR = dir;
  return dir;
}

export async function runDaemon(instance: string, options: RunOptions = {}): Promise<number> {
  const dryRun = options.dryRun === true;
  const log = new Logger(instance);
  // Before anything else can schedule work: a rejection from a tick or a Slack
  // handler must not take the process down, and a true crash must exit 1 so
  // KeepAlive / Restart=on-failure bring us back.
  installProcessGuards(log);
  const scratch = dryRun ? redirectStateToScratch() : null;
  if (scratch) {
    process.stdout.write(`dry run: nothing is sent to Slack; state is scoped to ${scratch}
any problems below are what a real start would refuse on — this run continues past them\n\n`);
  }
  const release = await acquireLock(instance);
  if (!release) {
    log.event("daemon.lock_held", { instance });
    return 0;
  }

  const config = await loadConfig(instance, dryRun, log);
  if (!config || !checkStartable(config, dryRun, log)) {
    await release();
    return 1;
  }

  mkdirSync(stateDir(instance), { recursive: true, mode: 0o700 });
  writeRecord({
    pid: process.pid,
    instance,
    socketPath: config.herdrSocketPath,
    startedAt: new Date().toISOString(),
    version: VERSION,
    slackTeamId: config.slack.teamId,
  });

  const client = new HerdrClient(config.herdrSocketPath);
  const state = new SessionState();
  const tail = new EventTail(client, state);
  const budget = new RateBudget({ totalPerMin: config.rateBudgetPerMin });

  tail.on("status", ({ status, error }) => {
    log.event("herdr.status", { status, ...(error ? { error } : {}) });
  });
  tail.on("applyError", ({ message }) => {
    log.error("herdr.apply_error", { message });
  });
  state.on("transition", (t) => {
    log.event("agent.transition", { terminalId: t.terminalId, from: t.from ?? null, to: t.to });
  });

  const registry = new SessionRegistry(instance);

  // Elect Slack ownership before opening Socket Mode — two Socket Mode clients
  // on the same app race for events and overwrite App Home.
  // Which registry is the bridge's call: it discovers whether another OS user
  // on this machine is already running a herd for this Slack app. A dry run
  // must touch nothing the real install shares (ADR 0007), so it is pinned to
  // the scratch dir and takes no part in discovery.
  const herd = new HerdBridge({
    config,
    instance,
    state,
    tail,
    registry,
    client,
    dryRun,
    log: (line) => log.event("surface", { msg: line }),
    ...(scratch ? { registryDir: path.join(scratch, "herd-registry") } : {}),
    // Ownership is only taken during election, so becoming primary means
    // starting over. Exiting non-zero is what makes the service manager do it.
    onPromotable: () => {
      log.event("daemon.restart_for_ownership", { herdId: herd.herdId });
      process.exit(1);
    },
    // Same reason: discovery runs at boot, so converging on the shared registry
    // means booting again.
    onRegistrySplit: () => {
      log.error("daemon.registry_split", { herdId: herd.herdId, registryDir: herd.registry.root });
      process.exit(1);
    },
  });
  const herdRegistryDir = herd.registry.root;
  // Always elect: in a dry run the scratch registry has no other herd, so this
  // returns primary — but skipping it left the bridge thinking it was a
  // satellite with no owner, which now (correctly) tries to restart for
  // ownership and would kill the dry run.
  const role = await herd.elect();

  const transport: SlackTransport = dryRun
    ? new DryRunTransport((write) => {
        log.event("slack.dry_run", { api: write.api, target: write.target, bytes: write.bytes });
        process.stdout.write(`${formatWrite(write)}\n`);
      })
    : role === "primary"
      ? new SocketModeTransport(
          config,
          (line) => log.event("surface", { msg: line }),
          // Non-zero so the service manager restarts into a clean process.
          () => {
            log.error("daemon.restart_for_wedged_socket");
            process.exit(1);
          },
        )
      : new WebApiTransport(config);

  const surfaces = new Surfaces({
    config,
    instance,
    transport,
    state,
    tail,
    registry,
    budget,
    client,
    herd,
    log: (line) => log.line(line),
  });

  surfaces.start();
  herd.start();
  tail.start();

  try {
    await transport.start();
  } catch (error) {
    log.error("slack.connect_failed", { message: (error as Error).message });
    process.stderr.write(`could not connect to Slack: ${(error as Error).message}\n`);
    process.stderr.write("run: herdr-slack doctor\n");
    herd.stop();
    tail.stop();
    state.dispose();
    await release();
    return 1;
  }

  log.event("daemon.up", {
    dryRun,
    pid: process.pid,
    version: VERSION,
    socketPath: config.herdrSocketPath,
    budgetPerMin: budget.totalPerMin,
    contentMode: config.contentMode,
    slackRole: role,
    herdId: herd.herdId,
    herdRegistryDir,
  });

  if (dryRun) {
    setTimeout(() => {
      const audience = config.allowedUsers.length > 0 ? config.allowedUsers : ["U_DRYRUN"];
      for (const userId of audience) void surfaces.publishHome(userId);
      setTimeout(() => {
        process.stdout.write("\nwatching for changes — ^C to stop\n");
      }, 500).unref?.();
    }, 1_500).unref?.();
  }

  onShutdown(async () => {
    log.event("daemon.shutdown");
    if (scratch) rmSync(scratch, { recursive: true, force: true });
    herd.stop();
    surfaces.stop();
    tail.stop();
    state.dispose();
    await transport.stop();
    await release();
  });

  await new Promise<void>(() => undefined);
  return 0;
}
