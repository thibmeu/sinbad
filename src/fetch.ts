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
import { Effect } from "effect";

export const asError = (cause: unknown): Error =>
	cause instanceof Error ? cause : new Error(String(cause));

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

export function fromResponse(
	response: Response,
	options: Pick<FetchOptions, "maxResponseSize"> = {},
): Effect.Effect<DAPResponse, Error> {
	return Effect.tryPromise({
		try: async () => {
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
		},
		catch: asError,
	});
}

/** Send an existing upload once. Authentication and retries belong to the caller. */
export function execute(
	upload: PreparedUpload,
	options: FetchOptions = {},
): Effect.Effect<UploadResult, Error> {
	return Effect.gen(function* () {
		const response = yield* Effect.tryPromise({
			try: (signal) => {
				options.signal?.throwIfAborted();
				const request = toRequest(upload.request, {
					...options,
					signal: options.signal
						? AbortSignal.any([options.signal, signal])
						: signal,
				});
				return (options.fetch ?? globalThis.fetch)(request);
			},
			catch: asError,
		});
		const data = yield* fromResponse(response, options);
		return yield* Effect.try({
			try: () => {
				options.signal?.throwIfAborted();
				return upload.process(data);
			},
			catch: asError,
		});
	});
}

/** Retrieve both lists explicitly; retain every advertised suite for inspection. */
export function fetchHpkeConfigs(
	task: Task<unknown>,
	options: FetchOptions = {},
): Effect.Effect<AggregatorHpkeConfigs, Error> {
	const get = (base: string): Effect.Effect<HpkeConfigList, Error> =>
		Effect.gen(function* () {
			const response = yield* Effect.tryPromise({
				try: (signal) =>
					(options.fetch ?? globalThis.fetch)(
						toRequest(
							{
								method: "GET",
								url: resource(base, "hpke_config"),
								headers: {
									accept: "application/ppm-dap;message=hpke-config-list",
								},
							},
							{
								...options,
								signal: options.signal
									? AbortSignal.any([options.signal, signal])
									: signal,
							},
						),
					),
				catch: asError,
			});
			const data = yield* fromResponse(response, {
				maxResponseSize: Math.min(options.maxResponseSize ?? 65537, 65537),
			});
			return yield* Effect.try({
				try: () => {
					options.signal?.throwIfAborted();
					if (data.status < 200 || data.status >= 300)
						throw new DAPError(
							"HttpError",
							`HPKE config request failed with HTTP ${data.status}`,
						);
					checkMediaType(data.headers, "hpke-config-list", task.dapVersion);
					return HpkeConfigList.parse(data.body);
				},
				catch: asError,
			});
		});
	return Effect.gen(function* () {
		yield* Effect.try({
			try: () => options.signal?.throwIfAborted(),
			catch: asError,
		});
		const [leader, helper] = yield* Effect.all(
			[get(task.leader), get(task.helper)],
			{ concurrency: 2 },
		);
		return Object.freeze({ leader, helper });
	});
}
