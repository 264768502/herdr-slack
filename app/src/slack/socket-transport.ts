import bolt from "@slack/bolt";
import type { InstanceConfig } from "../config/config.js";
import { contextFromPayload } from "./inbound.js";
import {
  DeliveryLedger,
  VIEW_SUBMIT_BUDGET_MS,
  isActionableMessage,
  parseActionPayload,
} from "./transport.js";
import type {
  ActionHandler,
  EphemeralInput,
  HomeHandler,
  InboundContext,
  MessageHandler,
  PostInput,
  SlackTransport,
  UpdateInput,
  ViewSubmitHandler,
} from "./transport.js";

/**
 * Socket continuously not OPEN for this long means Bolt's own reconnect is
 * wedged (2026-08-18: zero TCP sockets to Slack for most of an hour).
 *
 * Recovery is a process restart, not an in-process stop()+start(): that raced
 * Bolt's pending reconnect timers, so each recycle could leave another socket
 * and reconnect loop alive. On 2026-10-21 that had piled up ~60 sockets,
 * rate-limited apps.connections.open, and acks failed with "client is not
 * ready". Transient reconnects and half-open sockets are Bolt's job — its
 * ping/pong monitor drops a dead socket within seconds.
 */
const WEDGED_MS = 180_000;

type AnyRecord = Record<string, unknown>;

/** Bolt payloads → untrusted records; fields read with `str` at the boundary. */
const toRecord = (value: unknown): AnyRecord => value as AnyRecord;

const str = (value: unknown): string => (typeof value === "string" ? value : "");

/** Socket Mode transport with idle watchdog — half-open sockets look like "nothing happening". */
export class SocketModeTransport implements SlackTransport {
  #app: bolt.App;
  #receiver: bolt.SocketModeReceiver;
  #connected = false;
  #lastFrameAt: number | null = null;
  #watchdog: NodeJS.Timeout | null = null;
  /** When the socket was first seen not OPEN; null while healthy. */
  #unhealthySince: number | null = null;
  #connectionHandlers: ((connected: boolean) => void)[] = [];
  /** trigger_ids already handled, to drop Slack's redeliveries. */
  #deliveries = new DeliveryLedger();

  #log: (line: string) => void;
  #onWedged: () => void;

  constructor(
    private readonly config: InstanceConfig,
    log: (line: string) => void = () => undefined,
    onWedged: () => void = () => undefined,
  ) {
    this.#log = log;
    this.#onWedged = onWedged;
    const logger = this.#boltLogger();
    // Built here rather than via `socketMode: true` so the socket client stays
    // reachable: App.receiver is private, and the idle watchdog needs to see
    // Slack's own frames to tell a quiet connection from a dead one.
    this.#receiver = new bolt.SocketModeReceiver({
      appToken: config.slack.appToken,
      logger,
    });
    this.#app = new bolt.App({
      token: config.slack.botToken,
      receiver: this.#receiver,
      // We do our own logging; Bolt's is noisy and can echo payloads. Its
      // warnings still matter though — an unmatched interaction is never acked,
      // which surfaces in Slack as a button that silently does nothing.
      logLevel: "warn" as bolt.LogLevel,
      logger,
    });
    this.#app.error(async (error) => {
      this.#log(`bolt error: ${error instanceof Error ? error.message : String(error)}`);
    });
    // Attached once: the receiver keeps the same client across a recycle, so
    // doing this in start() would stack a listener per reconnect.
    this.#receiver.client.on("ws_message", () => this.#touch());
    // The idle-frame heuristic below only sees silence — it can't tell a
    // quiet-but-healthy connection from one the client itself gave up on.
    // These log the library's own state machine so a stuck reconnect leaves a
    // trail instead of the total silence we saw on 2026-08-18: the daemon held
    // zero TCP connections to Slack for the better part of an hour with no
    // disconnect, no ping/pong warning, and no watchdog line at all.
    this.#receiver.client.on("disconnected", () => {
      this.#log("slack: client state -> disconnected");
      this.#setConnected(false);
    });
    this.#receiver.client.on("reconnecting", () => {
      this.#log("slack: client state -> reconnecting");
      this.#setConnected(false);
    });
    this.#receiver.client.on("connected", () => {
      this.#log("slack: client state -> connected");
      this.#setConnected(true);
    });
  }

  /** Forward Bolt's own warn/error into daemon.log; the daemon has no stderr. */
  #boltLogger(): bolt.Logger {
    let level = "warn" as bolt.LogLevel;
    return {
      debug: () => undefined,
      info: () => undefined,
      warn: (...args: unknown[]) => this.#log(`bolt warn: ${args.map(String).join(" ")}`),
      error: (...args: unknown[]) => this.#log(`bolt error: ${args.map(String).join(" ")}`),
      setLevel: (next: bolt.LogLevel) => {
        level = next;
      },
      getLevel: () => level,
      setName: () => undefined,
    };
  }

  get connected(): boolean {
    return this.#connected;
  }

  get idleMs(): number | null {
    return this.#lastFrameAt === null ? null : Date.now() - this.#lastFrameAt;
  }

  async start(): Promise<void> {
    await this.#app.start();
    this.#setConnected(true);
    this.#watchdog = setInterval(() => this.#checkHealth(), 15_000);
    this.#watchdog.unref?.();
  }

  /**
   * `isActive()` reads the WebSocket's real readyState, so it also catches a
   * socket the client itself has lost track of. A not-OPEN socket is normal
   * while Bolt reconnects; only a long unbroken stretch counts as wedged.
   */
  #checkHealth(): void {
    if (this.#receiver.client.websocket?.isActive()) {
      this.#unhealthySince = null;
      return;
    }
    this.#setConnected(false);
    const now = Date.now();
    this.#unhealthySince ??= now;
    if (now - this.#unhealthySince > WEDGED_MS) {
      this.#log(
        `slack: socket not open for ${Math.round((now - this.#unhealthySince) / 1000)}s, restarting daemon`,
      );
      this.#unhealthySince = null;
      this.#onWedged();
    }
  }

  async stop(): Promise<void> {
    if (this.#watchdog) clearInterval(this.#watchdog);
    this.#watchdog = null;
    this.#setConnected(false);
    await this.#app.stop().catch(() => undefined);
  }

  async postMessage(input: PostInput): Promise<{ ts: string; channel: string }> {
    const result = await this.#app.client.chat.postMessage({
      channel: input.channel,
      text: input.text,
      ...(input.blocks ? { blocks: input.blocks as never } : {}),
      ...(input.threadTs ? { thread_ts: input.threadTs } : {}),
      unfurl_links: false,
      unfurl_media: false,
    });
    return { ts: str(result.ts), channel: str(result.channel) };
  }

  async updateMessage(input: UpdateInput): Promise<void> {
    await this.#app.client.chat.update({
      channel: input.channel,
      ts: input.ts,
      text: input.text,
      ...(input.blocks ? { blocks: input.blocks as never } : {}),
    });
  }

  /** Append Slack's `response_metadata.messages` to WebAPI errors. */
  #detail(error: unknown): string {
    const data = (error as { data?: AnyRecord })?.data;
    const messages = (data?.response_metadata as AnyRecord | undefined)?.messages;
    const base = error instanceof Error ? error.message : String(error);
    return Array.isArray(messages) && messages.length > 0
      ? `${base} — ${messages.join("; ")}`
      : base;
  }

  async publishHome(userId: string, blocks: unknown[]): Promise<void> {
    try {
      await this.#app.client.views.publish({
        user_id: userId,
        view: { type: "home", blocks: blocks as never },
      });
    } catch (error) {
      throw new Error(this.#detail(error));
    }
  }

  async openDm(userId: string): Promise<string> {
    const result = await this.#app.client.conversations.open({ users: userId });
    return str((result.channel as AnyRecord | undefined)?.id);
  }

  async openModal(triggerId: string, view: Record<string, unknown>): Promise<string> {
    const result = await this.#app.client.views.open({
      trigger_id: triggerId,
      view: view as never,
    });
    return str((result.view as AnyRecord | undefined)?.id);
  }

  async updateModal(viewId: string, view: Record<string, unknown>): Promise<void> {
    await this.#app.client.views.update({ view_id: viewId, view: view as never });
  }

  /**
   * Do the work first, then ack — so the modal only closes once the prompt is
   * actually with the agent, and stays open with an error when it is not.
   *
   * The handler races a budget below Slack's 3s cutoff. If it overruns we ack
   * plainly and let it finish in the background: an expired ack is shown to the
   * user as a failure even when the prompt landed, which is worse than closing.
   */
  onViewSubmit(handler: ViewSubmitHandler): void {
    this.#app.view(/.*/, async ({ ack, body, view }) => {
      this.#touch();
      const payload = toRecord(body);
      const viewRecord = toRecord(view);
      const running = handler({
        ctx: this.#context(payload),
        callbackId: str(viewRecord.callback_id),
        view: viewRecord.state,
        privateMetadata: str(viewRecord.private_metadata),
      });

      let timer: NodeJS.Timeout | undefined;
      const budget = new Promise<"timeout">((resolve) => {
        timer = setTimeout(() => resolve("timeout"), VIEW_SUBMIT_BUDGET_MS);
        timer.unref?.();
      });

      try {
        const result = await Promise.race([running.catch(() => undefined), budget]);
        if (result && result !== "timeout" && Object.keys(result.errors).length > 0) {
          await ack({ response_action: "errors", errors: result.errors } as never);
          return;
        }
        await ack();
      } finally {
        if (timer) clearTimeout(timer);
        void running.catch((error: unknown) => {
          this.#log(`view submit failed: ${(error as Error).message}`);
        });
      }
    });
  }

  async postEphemeral(input: EphemeralInput): Promise<void> {
    await this.#app.client.chat.postEphemeral({
      channel: input.channel,
      user: input.user,
      text: input.text,
      ...(input.threadTs ? { thread_ts: input.threadTs } : {}),
    });
  }

  async setThreadTitle(channel: string, threadTs: string, title: string): Promise<void> {
    await this.#app.client.assistant.threads.setTitle({
      channel_id: channel,
      thread_ts: threadTs,
      title,
    });
  }

  async permalink(channel: string, ts: string): Promise<string> {
    const result = await this.#app.client.chat.getPermalink({ channel, message_ts: ts });
    return str(result.permalink);
  }

  onAction(handler: ActionHandler): void {
    this.#app.action(/.*/, async ({ ack, body, action }) => {
      const parsed = parseActionPayload(body, action);
      // Logged before ack so a click is visible even if handling later throws.
      this.#log(`inbound action ${parsed.actionId || "(no action_id)"}`);
      await ack();
      this.#touch();
      // A redelivery carries the same trigger_id, which Slack has already spent
      // — handling it again only produces exchanged_trigger_id and a duplicate
      // action. Acked above regardless, so Slack stops retrying.
      if (parsed.triggerId && !this.#deliveries.first(parsed.triggerId)) {
        this.#log(`duplicate delivery of ${parsed.actionId} ignored`);
        return;
      }
      await handler({ ctx: this.#context(toRecord(body)), ...parsed });
    });
  }

  onMessage(handler: MessageHandler): void {
    this.#app.event("message", async ({ event, body, context }) => {
      this.#touch();
      const record = toRecord(event);
      // Ignore message_changed — card updates are outbound, never prompts.
      if (!isActionableMessage(record, this.config.slack.botUserId)) return;
      const envelope = toRecord(body);
      await handler({
        ctx: {
          teamId: str(record.team) || this.config.slack.teamId,
          userId: str(record.user),
          channel: str(record.channel),
          surface: "conversation",
          ts: str(record.ts),
          ...(record.thread_ts ? { threadTs: str(record.thread_ts) } : {}),
          ...(str(envelope.event_id) ? { eventId: str(envelope.event_id) } : {}),
          ...(typeof context.retryNum === "number" ? { retryNum: context.retryNum } : {}),
        },
        text: str(record.text),
      });
    });
  }

  onHomeOpened(handler: HomeHandler): void {
    this.#app.event("app_home_opened", async ({ event, body }) => {
      this.#touch();
      const record = toRecord(event);
      if (str(record.tab) !== "home") return;
      await handler({
        ctx: {
          teamId: str(toRecord(body).team_id) || this.config.slack.teamId,
          userId: str(record.user),
          channel: str(record.channel),
          surface: "home",
        },
      });
    });
  }

  onConnectionChange(handler: (connected: boolean) => void): void {
    this.#connectionHandlers.push(handler);
  }

  #context(payload: AnyRecord): InboundContext {
    return contextFromPayload(payload, this.config.slack.teamId);
  }

  #touch(): void {
    this.#lastFrameAt = Date.now();
    if (!this.#connected) this.#setConnected(true);
  }

  #setConnected(connected: boolean): void {
    if (this.#connected === connected) return;
    this.#connected = connected;
    if (connected) this.#lastFrameAt = Date.now();
    for (const handler of this.#connectionHandlers) handler(connected);
  }
}
