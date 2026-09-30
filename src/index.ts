import {
	Client,
	type HpkeConfigList,
	type PreparedReport,
	Task,
	type UploadResult,
	type Vdaf,
} from "@thibmeu/dap";
import {
	execute,
	type FetchOptions,
	fetchHpkeConfig,
	readLimited,
	send,
} from "./fetch.ts";

const DEFAULT_BATCH_MS = 1000;
const DEFAULT_MAX_BATCH = 20;

export interface BatchOptions {
	/** Milliseconds to collect reports before uploading. Default 1000; 0 sends each call immediately. */
	readonly batchMs?: number;
	/** Most reports in one upload. Default 20. */
	readonly maxBatch?: number;
}

/** A report's upload outcome. `sent` is false when Sinbad dropped the call. */
export type TrackResult = UploadResult & { readonly sent: boolean };

const DROPPED: TrackResult = Object.freeze({
	accepted: Object.freeze([]),
	rejected: Object.freeze([]),
	ok: false,
	sent: false,
});

function warn(message: string): void {
	globalThis.console?.warn?.(`Sinbad: ${message}`);
}

function drop(message: string): Promise<TrackResult> {
	warn(message);
	return Promise.resolve(DROPPED);
}

type Measurement = number | bigint;
interface Queued {
	readonly measurement: Measurement;
	readonly ready: Promise<{ client: Client; report: PreparedReport }>;
	prepared?: { client: Client; report: PreparedReport };
	resolve(result: TrackResult): void;
	reject(error: unknown): void;
}

/** One report's view of a shared upload result. */
function outcome(result: UploadResult, id: string): TrackResult {
	const rejected = result.rejected.filter((entry) => entry.id === id);
	return Object.freeze({
		accepted: rejected.length ? [] : result.accepted.filter((x) => x === id),
		rejected: Object.freeze(rejected),
		ok: rejected.length === 0,
		sent: true,
	});
}

/** Batch one task's measurements into as few uploads as the options allow. */
function uploader(
	client: (refresh?: boolean) => Promise<Client>,
	options: FetchOptions & BatchOptions,
) {
	const batchMs = options.batchMs ?? DEFAULT_BATCH_MS;
	const maxBatch = options.maxBatch ?? DEFAULT_MAX_BATCH;
	const queue: Queued[] = [];
	let timer: ReturnType<typeof setTimeout> | undefined;

	async function upload(
		active: Client,
		reports: readonly PreparedReport[],
		keepalive: boolean,
	) {
		const result = await execute(active.prepareUpload(reports), {
			...options,
			keepalive,
		});
		return reports.map((report) => outcome(result, report.id));
	}

	async function send(entries: Queued[], unloading: boolean): Promise<void> {
		try {
			const measurements = entries.map((entry) => entry.measurement);
			const prepared =
				unloading && entries.every((entry) => entry.prepared)
					? entries.map((entry) => entry.prepared!)
					: await Promise.all(entries.map((entry) => entry.ready));
			const results = await upload(
				prepared[0]!.client,
				prepared.map((entry) => entry.report),
				unloading,
			);
			const stale = results.flatMap((result, index) =>
				result.rejected[0]?.error === "hpke-unknown-config-id" ? [index] : [],
			);
			if (stale.length) {
				// DAP 19, 4.4.2.2: drop the cached lists, retry the affected reports
				// once with fresh ones, then give up.
				const fresh = await client(true);
				const retried = await upload(
					fresh,
					await fresh.prepareReports(
						stale.map((index) => measurements[index]!),
					),
					unloading,
				);
				stale.forEach((index, i) => {
					results[index] = retried[i]!;
				});
			}
			entries.forEach((entry, index) => {
				entry.resolve(results[index]!);
			});
		} catch (error) {
			for (const entry of entries) entry.reject(error);
		}
	}

	function flush(unloading = false): void {
		if (timer !== undefined) clearTimeout(timer);
		timer = undefined;
		while (queue.length) {
			const batch = queue.splice(0, maxBatch);
			if (!unloading) void send(batch, false);
			else {
				// Send sealed reports before waiting for any unfinished encryption.
				const ready = batch.filter((entry) => entry.prepared);
				const pending = batch.filter((entry) => !entry.prepared);
				if (ready.length) void send(ready, true);
				if (pending.length) void send(pending, true);
			}
		}
	}

	return {
		flush,
		report(measurement: Measurement): Promise<TrackResult> {
			return new Promise((resolve, reject) => {
				const ready = client().then(async (active) => ({
					client: active,
					report: await active.prepareReport(measurement),
				}));
				const entry: Queued = { measurement, ready, resolve, reject };
				void ready.then(
					(prepared) => {
						entry.prepared = prepared;
					},
					() => {},
				);
				queue.push(entry);
				if (queue.length >= maxBatch || batchMs === 0) flush();
				else if (timer === undefined) {
					timer = setTimeout(flush, batchMs);
					// Never keep a Node process alive for a pending batch.
					(timer as { unref?: () => void }).unref?.();
				}
			});
		},
	};
}

/** A provisioned task: its ID and TaskConfiguration in URL-safe Base 64. */
interface TaskDocument {
	readonly id: string;
	readonly configuration: string;
}

function manifestEntries(value: unknown): [string, Task][] {
	if (value === undefined) return [];
	if (!value || typeof value !== "object" || Array.isArray(value))
		throw new TypeError("Expected an analytics task map");
	return Object.entries(value).map(([name, entry]: [string, TaskDocument]) => {
		if (
			!name ||
			typeof entry?.id !== "string" ||
			typeof entry.configuration !== "string"
		)
			throw new TypeError("Invalid analytics task");
		return [
			name,
			Task.decode({
				id: entry.id,
				configuration: Uint8Array.from(
					atob(entry.configuration.replaceAll("-", "+").replaceAll("_", "/")),
					(char) => char.charCodeAt(0),
				),
			}),
		];
	});
}

export type SiteConfig = {
	readonly siteId: string;
	readonly endpoint: string;
} & FetchOptions &
	BatchOptions;

export interface SiteAnalytics {
	/** Count an event, or add a Sum value or Histogram bucket as `{ value }`. */
	track(name: string, properties?: { value: number }): Promise<TrackResult>;
	/** Count a page view. Defaults to `location.pathname` in a browser. */
	page(path?: string): Promise<TrackResult>;
	/** Upload everything queued now, without waiting for the batch window. */
	flush(): void;
	/** Stop listening for page unload. */
	close(): void;
}

/** Fetch the provisioned task manifest for one site. */
export async function createSiteAnalytics(
	config: SiteConfig,
): Promise<SiteAnalytics> {
	if (!config || !/^[A-Za-z0-9_-]+$/.test(config.siteId))
		throw new TypeError("Invalid site ID");
	const base = new URL(config.endpoint);
	const localHttp =
		base.protocol === "http:" &&
		["localhost", "127.0.0.1", "[::1]"].includes(base.hostname);
	if (
		(base.protocol !== "https:" && !localHttp) ||
		base.username ||
		base.password ||
		base.search ||
		base.hash
	)
		throw new TypeError("Invalid analytics endpoint");
	const url = new URL(
		`sites/${config.siteId}/manifest`,
		base.href.endsWith("/") ? base.href : `${base.href}/`,
	);
	const response = await send(
		new Request(url, {
			headers: { accept: "application/json" },
			redirect: "manual",
			credentials: "omit",
		}),
		config,
	);
	const body = await readLimited(response, 65536);
	config.signal?.throwIfAborted();
	if (response.status < 200 || response.status >= 300)
		throw new Error(
			`Site manifest request failed with HTTP ${response.status}`,
		);
	if (
		!/^application\/json(?:\s*;|\s*$)/i.test(
			response.headers.get("content-type") ?? "",
		)
	)
		throw new TypeError("Expected a JSON site manifest");
	const manifest: unknown = JSON.parse(
		new TextDecoder("utf-8", { fatal: true }).decode(body),
	);
	if (!manifest || typeof manifest !== "object" || Array.isArray(manifest))
		throw new TypeError("Invalid site manifest");
	const document = manifest as Record<string, unknown>;
	const eventEntries = manifestEntries(document.events);
	const pageEntries = manifestEntries(document.pages);
	if (eventEntries.length + pageEntries.length === 0)
		throw new TypeError("Empty site manifest");

	// Every task on one Aggregator shares its HPKE list, so fetch it once and
	// only when the first report for that Aggregator is prepared. Manifest size
	// then costs no round trips, and one unreachable Aggregator affects only
	// the tasks that use it.
	const lists = new Map<string, Promise<HpkeConfigList>>();
	const list = (aggregator: string, version: 18 | 19, refresh: boolean) => {
		if (refresh) lists.delete(aggregator);
		let pending = lists.get(aggregator);
		if (!pending) {
			pending = fetchHpkeConfig(aggregator, config, version, refresh).catch(
				(error) => {
					lists.delete(aggregator);
					throw error;
				},
			);
			lists.set(aggregator, pending);
		}
		return pending;
	};

	const queues: { flush(unloading?: boolean): void }[] = [];
	function reporterFor(task: Task) {
		let client: Promise<Client> | undefined;
		const queue = uploader((refresh = false) => {
			if (refresh || !client) {
				client = Promise.all([
					list(task.leader, task.dapVersion, refresh),
					list(task.helper, task.dapVersion, refresh),
				]).then(([leader, helper]) =>
					Client.create<Vdaf>(task, { hpke: { leader, helper } }),
				);
			}
			return client;
		}, config);
		queues.push(queue);
		return queue;
	}

	const events = new Map<
		string,
		(properties?: { value: number }) => Promise<TrackResult>
	>();
	const pages = new Map<string, () => Promise<TrackResult>>();
	for (const [name, task] of eventEntries) {
		const queue = reporterFor(task);
		if (task.vdaf.type === "prio3-count")
			events.set(name, (properties) =>
				properties === undefined
					? queue.report(1)
					: drop(`event "${name}" takes no properties`),
			);
		else
			events.set(name, (properties) =>
				properties &&
				Object.keys(properties).length === 1 &&
				Object.hasOwn(properties, "value")
					? queue.report(properties.value)
					: drop(`event "${name}" requires only a value`),
			);
	}
	for (const [path, task] of pageEntries) {
		if (task.vdaf.type !== "prio3-count")
			throw new TypeError("Pages require Count tasks");
		const queue = reporterFor(task);
		pages.set(path, () => queue.report(1));
	}

	const flushAll = () => {
		for (const queue of queues) queue.flush(true);
	};
	// Anything still queued when the page goes away is sent with keepalive.
	const target = globalThis as unknown as {
		addEventListener?: (type: string, listener: () => void) => void;
		removeEventListener?: (type: string, listener: () => void) => void;
		document?: { visibilityState?: string };
	};
	const onHidden = () => {
		if (target.document?.visibilityState !== "visible") flushAll();
	};
	target.addEventListener?.("pagehide", flushAll);
	target.addEventListener?.("visibilitychange", onHidden);

	return {
		track: (name, properties) =>
			events.get(name)?.(properties) ?? drop(`unknown event "${name}"`),
		page: (path = globalThis.location?.pathname) =>
			(path === undefined ? undefined : pages.get(path))?.() ??
			drop(`unknown page "${String(path)}"`),
		flush: () => {
			for (const queue of queues) queue.flush();
		},
		close: () => {
			target.removeEventListener?.("pagehide", flushAll);
			target.removeEventListener?.("visibilitychange", onHidden);
		},
	};
}

// Calls made before the first init() wait for it, so a page can track as
// soon as it loads.
let release: ((site: Promise<SiteAnalytics | undefined>) => void) | undefined;
let current = new Promise<SiteAnalytics | undefined>((resolve) => {
	release = resolve;
});

/** Single-site browser interface. Calls made before `init()` wait for it. */
export const Sinbad = {
	async init(config: SiteConfig): Promise<void> {
		const next = createSiteAnalytics(config);
		const settled = next.catch((error: unknown) => {
			warn(`initialization failed: ${String(error)}`);
			return undefined;
		});
		const previous = current;
		current = settled;
		release?.(settled);
		release = undefined;
		void settled.then((site) =>
			previous.then((old) => {
				if (old !== site) old?.close();
			}),
		);
		await next;
	},
	track(name: string, properties?: { value: number }): Promise<TrackResult> {
		return current.then((site) => site?.track(name, properties) ?? DROPPED);
	},
	page(path?: string): Promise<TrackResult> {
		// Read the path now: the page may have navigated by the time init ends.
		const at = path ?? globalThis.location?.pathname;
		return current.then((site) => site?.page(at) ?? DROPPED);
	},
	/** Upload everything queued now. */
	flush(): void {
		void current.then((site) => site?.flush());
	},
} as const;
