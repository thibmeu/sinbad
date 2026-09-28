import {
	type AggregatorHpkeConfigs,
	checkMediaType,
	DAPError,
	type DAPRequest,
	type DAPResponse,
	HpkeConfigList,
	type PreparedUpload,
	type Task,
	type UploadResult,
} from "dap-ts";

function resource(base: string, path: string): string {
	return `${base.endsWith("/") ? base : `${base}/`}${path}`;
}

export interface FetchOptions {
	readonly fetch?: typeof globalThis.fetch;
	readonly signal?: AbortSignal;
	readonly headers?: HeadersInit;
	readonly maxResponseSize?: number;
}

export function toRequest(
	request: DAPRequest,
	options: Pick<FetchOptions, "headers" | "signal"> = {},
): Request {
	const headers = new Headers(request.headers);
	for (const [name, value] of new Headers(options.headers)) {
		if (
			name === "content-type" &&
			headers.has(name) &&
			headers.get(name) !== value
		) {
			throw new DAPError(
				"InvalidMessage",
				"Cannot override the DAP content type",
			);
		}
		headers.set(name, value);
	}
	return new Request(request.url, {
		method: request.method,
		headers,
		redirect: "error",
		credentials: "omit",
		...(request.body ? { body: new Uint8Array(request.body) } : {}),
		...(options.signal ? { signal: options.signal } : {}),
	});
}

export async function fromResponse(
	response: Response,
	options: Pick<FetchOptions, "maxResponseSize"> = {},
): Promise<DAPResponse> {
	const limit = options.maxResponseSize ?? 1024 * 1024;
	if (!Number.isSafeInteger(limit) || limit < 0)
		throw new DAPError("InvalidResponse", "Invalid response size limit");
	const chunks: Uint8Array[] = [];
	let size = 0;
	const reader = response.body?.getReader();
	if (reader) {
		try {
			while (true) {
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
				chunks.push(next.value.slice());
			}
		} finally {
			reader.releaseLock();
		}
	}
	const body = new Uint8Array(size);
	let offset = 0;
	for (const chunk of chunks) {
		body.set(chunk, offset);
		offset += chunk.length;
	}
	return {
		status: response.status,
		headers: Object.fromEntries(response.headers),
		body,
	};
}

/** Send an existing upload once. Authentication and retries belong to the caller. */
export async function execute(
	upload: PreparedUpload,
	options: FetchOptions = {},
): Promise<UploadResult> {
	options.signal?.throwIfAborted();
	const response = await (options.fetch ?? globalThis.fetch)(
		toRequest(upload.request, options),
	);
	const data = await fromResponse(response, options);
	options.signal?.throwIfAborted();
	return upload.process(data);
}

/** Retrieve both lists explicitly; retain every advertised suite for inspection. */
export async function fetchHpkeConfigs(
	task: Task<unknown>,
	options: FetchOptions = {},
): Promise<AggregatorHpkeConfigs> {
	options.signal?.throwIfAborted();
	const get = async (base: string): Promise<HpkeConfigList> => {
		const response = await (options.fetch ?? globalThis.fetch)(
			toRequest(
				{
					method: "GET",
					url: resource(base, "hpke_config"),
					headers: { accept: "application/ppm-dap;message=hpke-config-list" },
				},
				options,
			),
		);
		const data = await fromResponse(response, {
			maxResponseSize: Math.min(options.maxResponseSize ?? 65537, 65537),
		});
		options.signal?.throwIfAborted();
		if (data.status < 200 || data.status >= 300)
			throw new DAPError(
				"HttpError",
				`HPKE config request failed with HTTP ${data.status}`,
			);
		checkMediaType(data.headers, "hpke-config-list", task.dapVersion);
		return HpkeConfigList.parse(data.body);
	};
	const [leader, helper] = await Promise.all([
		get(task.leader),
		get(task.helper),
	]);
	return Object.freeze({ leader, helper });
}
