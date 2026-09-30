import {
	type AggregatorHpkeConfigs,
	checkMediaType,
	DAPError,
	HpkeConfigList,
	type PreparedUpload,
	type Task,
	type UploadResult,
} from "@thibmeu/dap";

export interface FetchOptions {
	readonly fetch?: (request: Request) => Promise<Response>;
	readonly signal?: AbortSignal;
	/** Extra headers, such as authentication. The DAP content type cannot change. */
	readonly headers?: HeadersInit;
	/** Let the request outlive the page, as on unload. Browsers cap such bodies at 64 KiB. */
	readonly keepalive?: boolean;
}

/** Send a DAP request with the caller's fetch, headers, and signal. */
export function send(
	request: Request,
	options: FetchOptions = {},
): Promise<Response> {
	options.signal?.throwIfAborted();
	const sent = new Request(request, {
		...(options.signal ? { signal: options.signal } : {}),
		...(options.keepalive ? { keepalive: true } : {}),
	});
	for (const [name, value] of new Headers(options.headers)) {
		if (
			name === "content-type" &&
			sent.headers.has(name) &&
			sent.headers.get(name) !== value
		)
			throw new DAPError(
				"InvalidMessage",
				"Cannot override the DAP content type",
			);
		sent.headers.set(name, value);
	}
	return options.fetch ? options.fetch(sent) : globalThis.fetch(sent);
}

/** Read at most limit bytes of a response or request body. */
export async function readLimited(
	response: Response | Request,
	limit: number,
): Promise<Uint8Array<ArrayBuffer>> {
	const chunks: Uint8Array[] = [];
	let size = 0;
	const reader = response.body?.getReader();
	if (reader)
		try {
			for (;;) {
				const next = await reader.read();
				if (next.done) break;
				size += next.value.length;
				if (size > limit) {
					await reader.cancel();
					throw new DAPError(
						"InvalidResponse",
						"Response exceeds the size limit",
					);
				}
				chunks.push(next.value);
			}
		} finally {
			reader.releaseLock();
		}
	const body = new Uint8Array(size);
	let offset = 0;
	for (const chunk of chunks) {
		body.set(chunk, offset);
		offset += chunk.length;
	}
	return body;
}

/** Send an existing upload once. Authentication and retries belong to the caller. */
export async function execute(
	upload: PreparedUpload,
	options: FetchOptions = {},
): Promise<UploadResult> {
	const result = await upload.process(await send(upload.request, options));
	options.signal?.throwIfAborted();
	return result;
}

/**
 * Retrieve one Aggregator's list. Tasks that share an Aggregator share this
 * resource, so callers should cache it by base URL rather than by task.
 * Refresh after a key rejection bypasses the browser's HTTP cache.
 */
export async function fetchHpkeConfig(
	base: string,
	options: FetchOptions = {},
	dapVersion: 18 | 19 = 19,
	refresh = false,
): Promise<HpkeConfigList> {
	const response = await send(
		new Request(`${base.endsWith("/") ? base : `${base}/`}hpke_config`, {
			headers: { accept: "application/ppm-dap;message=hpke-config-list" },
			redirect: "manual",
			credentials: "omit",
			...(refresh ? { cache: "reload" } : {}),
		}),
		options,
	);
	const body = await readLimited(response, 65537);
	options.signal?.throwIfAborted();
	if (response.status < 200 || response.status >= 300)
		throw new DAPError(
			"HttpError",
			`HPKE config request failed with HTTP ${response.status}`,
		);
	try {
		checkMediaType(
			response.headers.get("content-type"),
			"hpke-config-list",
			dapVersion,
		);
	} catch (cause) {
		throw new DAPError("InvalidResponse", "Unexpected HPKE config media type", {
			cause,
		});
	}
	return HpkeConfigList.parse(body);
}

/** Retrieve both lists explicitly; retain every advertised suite for inspection. */
export async function fetchHpkeConfigs(
	task: Task,
	options: FetchOptions = {},
): Promise<AggregatorHpkeConfigs> {
	const [leader, helper] = await Promise.all([
		fetchHpkeConfig(task.leader, options, task.dapVersion),
		fetchHpkeConfig(task.helper, options, task.dapVersion),
	]);
	return Object.freeze({ leader, helper });
}
