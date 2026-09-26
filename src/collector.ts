import type {
	CollectionProgress,
	CollectionQuery,
	CollectionState,
	Collector,
	PreparedCollection,
} from "dap-ts";
import { DAPError } from "dap-ts";
import { Effect } from "effect";
import {
	asError,
	type FetchOptions,
	fromResponse,
	toRequest,
} from "./fetch.js";

export interface CollectionFetchOptions extends FetchOptions {
	/** Authentication headers, if required by the deployment. */
	readonly headers?: HeadersInit;
	readonly maxPolls?: number;
	readonly minDelayMs?: number;
	readonly maxDelayMs?: number;
}

/** Execute one prepared POST or GET. Save a pending result's state before polling. */
export function executeCollection(
	prepared: PreparedCollection,
	options: CollectionFetchOptions = {},
): Effect.Effect<CollectionProgress, Error> {
	return Effect.gen(function* () {
		const response = yield* Effect.tryPromise({
			try: (signal) => {
				options.signal?.throwIfAborted();
				const request = toRequest(prepared.request, {
					...options,
					signal: options.signal
						? AbortSignal.any([options.signal, signal])
						: signal,
				});
				return (options.fetch ?? globalThis.fetch)(request);
			},
			catch: asError,
		});
		const data = yield* fromResponse(response, {
			maxResponseSize: options.maxResponseSize ?? 1024 * 1024,
		});
		return yield* Effect.tryPromise({
			try: () => {
				options.signal?.throwIfAborted();
				return prepared.process(data);
			},
			catch: asError,
		});
	});
}

function wait(ms: number, signal?: AbortSignal): Promise<void> {
	signal?.throwIfAborted();
	if (ms === 0) return Promise.resolve();
	return new Promise((resolve, reject) => {
		const done = () => {
			clearTimeout(timer);
			signal?.removeEventListener("abort", abort);
		};
		const abort = () => {
			done();
			reject(signal?.reason);
		};
		const timer = setTimeout(() => {
			done();
			resolve();
		}, ms);
		signal?.addEventListener("abort", abort, { once: true });
		if (signal?.aborted) abort();
	});
}

/** Start or resume a collection and return pending state when the poll limit is reached. */
export function collect(
	collector: Collector,
	queryOrState: CollectionQuery | CollectionState,
	options: CollectionFetchOptions = {},
): Effect.Effect<CollectionProgress, Error> {
	return Effect.gen(function* () {
		const maxPolls = options.maxPolls ?? 20;
		const minDelayMs = options.minDelayMs ?? 1000;
		const maxDelayMs = options.maxDelayMs ?? 60000;
		yield* Effect.try({
			try: () => {
				if (!queryOrState || typeof queryOrState !== "object")
					throw new DAPError(
						"InvalidMessage",
						"Expected a collection query or state",
					);
				if (
					!Number.isSafeInteger(maxPolls) ||
					maxPolls < 0 ||
					maxPolls > 1000 ||
					!Number.isSafeInteger(minDelayMs) ||
					minDelayMs < 0 ||
					!Number.isSafeInteger(maxDelayMs) ||
					maxDelayMs < minDelayMs ||
					maxDelayMs > 60000
				)
					throw new DAPError(
						"InvalidMessage",
						"Invalid collection polling limits",
					);
			},
			catch: asError,
		});
		let progress = yield* executeCollection(
			"location" in queryOrState
				? yield* Effect.try({
						try: () => collector.resume(queryOrState),
						catch: asError,
					})
				: yield* Effect.try({
						try: () => collector.prepare(queryOrState),
						catch: asError,
					}),
			options,
		);
		for (let i = 0; i < maxPolls && progress.status === "pending"; i++) {
			const delay = Math.max(minDelayMs, (progress.retryAfter ?? 0) * 1000);
			if (delay > maxDelayMs) return progress;
			yield* Effect.tryPromise({
				try: () => wait(delay, options.signal),
				catch: asError,
			});
			const state = progress.state;
			progress = yield* executeCollection(
				yield* Effect.try({
					try: () => collector.resume(state),
					catch: asError,
				}),
				options,
			);
		}
		return progress;
	});
}
