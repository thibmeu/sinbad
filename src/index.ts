import {
	Client,
	prio3Count,
	prio3Sum,
	Task,
	type TaskOptions,
	type UploadResult,
} from "dap-ts";
import {
	execute,
	type FetchOptions,
	fetchHpkeConfigs,
	fromResponse,
	toRequest,
} from "./fetch.js";

async function reporter<M>(task: Task<M>, options: FetchOptions) {
	const client = await Client.create(task, {
		hpke: await fetchHpkeConfigs(task, options),
	});
	return async (measurement: M): Promise<UploadResult> =>
		execute(
			client.prepareUpload([await client.prepareReport(measurement)]),
			options,
		);
}

/** Create a counter for one provisioned Prio3Count task. */
export async function createCounter(
	task: Task<number>,
	options: FetchOptions = {},
): Promise<() => Promise<UploadResult>> {
	if (!(task instanceof Task) || task.vdaf.type !== "prio3-count")
		throw new TypeError("Expected a Prio3Count task");
	const report = await reporter(task, options);
	return () => report(1);
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

/** Fetch the provisioned task manifest for one site. */
export async function createSiteAnalytics(
	config: { readonly siteId: string; readonly endpoint: string } & FetchOptions,
) {
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
	const events = new Map<
		string,
		(properties?: { value: number }) => Promise<UploadResult>
	>();
	const pages = new Map<string, () => Promise<UploadResult>>();
	await Promise.all([
		...eventEntries.map(async ([name, entry]) => {
			if (entry.vdaf === "count") {
				const report = await reporter(
					Task.create({ ...entry, vdaf: prio3Count() }),
					config,
				);
				events.set(name, (properties) => {
					if (properties !== undefined)
						return Promise.reject(
							new TypeError("Count events do not accept properties"),
						);
					return report(1);
				});
			} else if (entry.vdaf === "sum") {
				if (
					!Number.isSafeInteger(entry.maxMeasurement) ||
					entry.maxMeasurement! <= 0
				)
					throw new TypeError("Invalid sum bound");
				const report = await reporter(
					Task.create({
						...entry,
						vdaf: prio3Sum(entry.maxMeasurement!),
					}),
					config,
				);
				events.set(name, (properties) => {
					if (
						!properties ||
						Object.keys(properties).length !== 1 ||
						!Object.hasOwn(properties, "value")
					)
						return Promise.reject(
							new TypeError("Sum events require only a value"),
						);
					return report(properties.value);
				});
			} else {
				throw new TypeError(`Unsupported event VDAF: ${entry.vdaf}`);
			}
		}),
		...pageEntries.map(async ([path, entry]) => {
			if (entry.vdaf !== "count")
				throw new TypeError("Pages require Count tasks");
			const report = await reporter(
				Task.create({ ...entry, vdaf: prio3Count() }),
				config,
			);
			pages.set(path, () => report(1));
		}),
	]);
	return {
		track: (name: string, properties?: { value: number }) =>
			events.get(name)?.(properties) ??
			Promise.reject(new RangeError(`Unknown event: ${name}`)),
		page: (path = globalThis.location?.pathname) =>
			pages.get(path)?.() ??
			Promise.reject(new RangeError(`Unknown page: ${path}`)),
	};
}

let current: Awaited<ReturnType<typeof createSiteAnalytics>> | undefined;
let initSequence = 0;

/** Single-site browser interface. Await init before calling page or track. */
export const Sinbad = {
	async init(config: Parameters<typeof createSiteAnalytics>[0]): Promise<void> {
		const sequence = ++initSequence;
		current = undefined;
		const next = await createSiteAnalytics(config);
		if (sequence === initSequence) current = next;
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
} as const;
