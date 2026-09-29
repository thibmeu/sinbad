import {
	Client,
	type DAPRequest,
	type HpkeConfigList,
	prio3Count,
	prio3Sum,
	Task,
	type TaskOptions,
	type UploadResult,
} from "dap-ts";
import {
	execute,
	type FetchOptions,
	fetchHpkeConfig,
	fetchHpkeConfigs,
	fromResponse,
	toRequest,
} from "./fetch.js";

const DEFAULT_BATCH_MS = 1000;
const DEFAULT_MAX_BATCH = 20;

export interface BatchOptions {
	/** Milliseconds to collect reports before uploading. Default 1000; 0 sends each call immediately. */
	readonly batchMs?: number;
	/** Most reports in one upload. Default 20. */
	readonly maxBatch?: number;
}

/** Nothing was sent, or the outcome is not observable. */
const NO_RESULT: UploadResult = Object.freeze({
	accepted: Object.freeze([]),
	rejected: Object.freeze([]),
	ok: true,
});

function warn(message: string): void {
	globalThis.console?.warn?.(`Sinbad: ${message}`);
}

/** Send on page unload, where a response cannot be read. */
function beacon(request: DAPRequest): boolean {
	const send = globalThis.navigator?.sendBeacon;
	const type = request.headers["content-type"];
	if (typeof send !== "function" || !request.body || !type) return false;
	try {
		return send.call(
			globalThis.navigator,
			request.url,
			new Blob([request.body as BlobPart], { type }),
		);
	} catch {
		return false;
	}
}

type Measurement = number | bigint;
interface Queued {
	readonly measurement: Measurement;
	resolve(result: UploadResult): void;
	reject(error: unknown): void;
}

/** One report's view of a shared upload result. */
function outcome(result: UploadResult, id: string): UploadResult {
	const rejected = result.rejected.filter((entry) => entry.id === id);
	return Object.freeze({
		accepted: rejected.length ? [] : result.accepted.filter((x) => x === id),
		rejected: Object.freeze(rejected),
		ok: rejected.length === 0,
	});
}

/** Batch one task's measurements into as few uploads as the options allow. */
function uploader(
	client: (refresh?: boolean) => Promise<Client<Measurement>>,
	options: FetchOptions & BatchOptions,
) {
	const batchMs = options.batchMs ?? DEFAULT_BATCH_MS;
	const maxBatch = options.maxBatch ?? DEFAULT_MAX_BATCH;
	let queue: Queued[] = [];
	let timer: ReturnType<typeof setTimeout> | undefined;

	async function send(entries: Queued[], unloading: boolean): Promise<void> {
		try {
			let active = await client();
			const measurements = entries.map((entry) => entry.measurement);
			let reports = await active.prepareReports(measurements);
			let upload = active.prepareUpload(reports);
			if (unloading && beacon(upload.request)) {
				for (const entry of entries) entry.resolve(NO_RESULT);
				return;
			}
			let result = await execute(upload, options);
			if (
				result.rejected.some((entry) => entry.code === "hpke-unknown-config-id")
			) {
				// DAP 19, 4.4.2.2: drop the cached list, retry once with fresh
				// reports, then give up.
				active = await client(true);
				reports = await active.prepareReports(measurements);
				upload = active.prepareUpload(reports);
				result = await execute(upload, options);
			}
			entries.forEach((entry, index) => {
				entry.resolve(outcome(result, reports[index]!.id));
			});
		} catch (error) {
			for (const entry of entries) entry.reject(error);
		}
	}

	function flush(unloading = false): void {
		if (timer !== undefined) clearTimeout(timer);
		timer = undefined;
		const entries = queue;
		queue = [];
		if (entries.length) void send(entries, unloading);
	}

	return {
		flush,
		report(measurement: Measurement): Promise<UploadResult> {
			return new Promise((resolve, reject) => {
				queue.push({ measurement, resolve, reject });
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

/** Create a counter for one provisioned Prio3Count task. */
export async function createCounter(
	task: Task<number>,
	options: FetchOptions & BatchOptions = {},
): Promise<() => Promise<UploadResult>> {
	if (!(task instanceof Task) || task.vdaf.type !== "prio3-count")
		throw new TypeError("Expected a Prio3Count task");
	const client = await Client.create(task, {
		hpke: await fetchHpkeConfigs(task, options),
	});
	const queue = uploader(async () => client as Client<Measurement>, options);
	return () => queue.report(1);
}

type TaskDocument = Omit<TaskOptions<number>, "vdaf"> & {
	readonly vdaf: "count" | "sum";
	readonly maxMeasurement?: number;
};

function manifestEntries(value: unknown): [string, TaskDocument][] {
	if (value === undefined) return [];
	if (!value || typeof value !== "object" || Array.isArray(value))
		throw new TypeError("Expected an analytics task map");
	const entries = Object.entries(value);
	for (const [name, task] of entries) {
		if (!name || !task || typeof task !== "object" || Array.isArray(task))
			throw new TypeError("Invalid analytics task");
	}
	return entries as [string, TaskDocument][];
}

function buildTask(entry: TaskDocument): Task<Measurement> {
	if (entry.vdaf === "count")
		return Task.create({ ...entry, vdaf: prio3Count() }) as Task<Measurement>;
	if (entry.vdaf === "sum") {
		if (
			!Number.isSafeInteger(entry.maxMeasurement) ||
			entry.maxMeasurement! <= 0
		)
			throw new TypeError("Invalid sum bound");
		return Task.create({
			...entry,
			vdaf: prio3Sum(entry.maxMeasurement!),
		}) as Task<Measurement>;
	}
	throw new TypeError(`Unsupported VDAF: ${String(entry.vdaf)}`);
}

export type SiteConfig = {
	readonly siteId: string;
	readonly endpoint: string;
} & FetchOptions &
	BatchOptions;

/** Fetch the provisioned task manifest for one site. */
export async function createSiteAnalytics(config: SiteConfig) {
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
	config.signal?.throwIfAborted();
	const response = await (config.fetch ?? globalThis.fetch)(
		toRequest(
			{ method: "GET", url: url.href, headers: { accept: "application/json" } },
			config,
		),
	);
	const data = await fromResponse(response, { maxResponseSize: 65536 });
	config.signal?.throwIfAborted();
	if (data.status < 200 || data.status >= 300)
		throw new Error(`Site manifest request failed with HTTP ${data.status}`);
	if (
		!/^application\/json(?:\s*;|\s*$)/i.test(data.headers["content-type"] ?? "")
	)
		throw new TypeError("Expected a JSON site manifest");
	const manifest: unknown = JSON.parse(
		new TextDecoder("utf-8", { fatal: true }).decode(data.body),
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
			pending = fetchHpkeConfig(aggregator, config, version).catch((error) => {
				lists.delete(aggregator);
				throw error;
			});
			lists.set(aggregator, pending);
		}
		return pending;
	};

	const queues: { flush(unloading?: boolean): void }[] = [];
	function reporterFor(task: Task<Measurement>) {
		let client: Promise<Client<Measurement>> | undefined;
		const queue = uploader((refresh = false) => {
			if (refresh || !client) {
				client = Promise.all([
					list(task.leader, task.dapVersion, refresh),
					list(task.helper, task.dapVersion, refresh),
				]).then(([leader, helper]) =>
					Client.create(task, { hpke: { leader, helper } }),
				);
			}
			return client;
		}, config);
		queues.push(queue);
		return queue;
	}

	const events = new Map<
		string,
		(properties?: { value: number }) => Promise<UploadResult>
	>();
	const pages = new Map<string, () => Promise<UploadResult>>();
	for (const [name, entry] of eventEntries) {
		const task = buildTask(entry);
		const queue = reporterFor(task);
		if (entry.vdaf === "count")
			events.set(name, (properties) => {
				if (properties !== undefined) {
					warn(`event "${name}" takes no properties`);
					return Promise.resolve(NO_RESULT);
				}
				return queue.report(1);
			});
		else
			events.set(name, (properties) => {
				if (
					!properties ||
					Object.keys(properties).length !== 1 ||
					!Object.hasOwn(properties, "value")
				) {
					warn(`event "${name}" requires only a value`);
					return Promise.resolve(NO_RESULT);
				}
				return queue.report(properties.value);
			});
	}
	for (const [path, entry] of pageEntries) {
		if (entry.vdaf !== "count")
			throw new TypeError("Pages require Count tasks");
		const queue = reporterFor(buildTask(entry));
		pages.set(path, () => queue.report(1));
	}

	const flushAll = () => {
		for (const queue of queues) queue.flush(true);
	};
	// Anything still queued when the page goes away is sent with sendBeacon.
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
		track: (name: string, properties?: { value: number }) => {
			const report = events.get(name);
			if (!report) {
				warn(`unknown event "${name}"`);
				return Promise.resolve(NO_RESULT);
			}
			return report(properties);
		},
		page: (path = globalThis.location?.pathname) => {
			const report = path === undefined ? undefined : pages.get(path);
			if (!report) {
				warn(`unknown page "${String(path)}"`);
				return Promise.resolve(NO_RESULT);
			}
			return report();
		},
		/** Upload everything queued now, without waiting for the batch window. */
		flush: () => {
			for (const queue of queues) queue.flush();
		},
		/** Stop listening for page unload. */
		close: () => {
			target.removeEventListener?.("pagehide", flushAll);
			target.removeEventListener?.("visibilitychange", onHidden);
		},
	};
}

let current: Awaited<ReturnType<typeof createSiteAnalytics>> | undefined;
let initSequence = 0;

/** Single-site browser interface. Await init before calling page or track. */
export const Sinbad = {
	async init(config: SiteConfig): Promise<void> {
		const sequence = ++initSequence;
		const next = await createSiteAnalytics(config);
		if (sequence !== initSequence) {
			next.close();
			return;
		}
		current?.close();
		current = next;
	},
	track(name: string, properties?: { value: number }): Promise<UploadResult> {
		return (
			current?.track(name, properties) ??
			Promise.reject(new Error("Sinbad is not initialized"))
		);
	},
	page(path?: string): Promise<UploadResult> {
		return (
			current?.page(path) ??
			Promise.reject(new Error("Sinbad is not initialized"))
		);
	},
	/** Upload everything queued now. */
	flush(): void {
		current?.flush();
	},
} as const;
