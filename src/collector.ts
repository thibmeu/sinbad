import {
	type CollectionProgress,
	type CollectionQuery,
	type CollectionState,
	type Collector,
	DAPError,
	type PreparedCollection,
	type Vdaf,
} from "dap-ts";
import { type FetchOptions, send } from "./fetch.ts";

export interface CollectionFetchOptions extends FetchOptions {
	readonly maxPolls?: number;
	readonly minDelayMs?: number;
	readonly maxDelayMs?: number;
}

/** Execute one prepared POST or GET. Save a pending result's state before polling. */
export async function executeCollection<V extends Vdaf>(
	prepared: PreparedCollection<V>,
	options: CollectionFetchOptions = {},
): Promise<CollectionProgress<V>> {
	const progress = await prepared.process(
		await send(prepared.request, options),
	);
	options.signal?.throwIfAborted();
	return progress;
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
export async function collect<V extends Vdaf>(
	collector: Collector<V>,
	queryOrState: CollectionQuery | CollectionState,
	options: CollectionFetchOptions = {},
): Promise<CollectionProgress<V>> {
	const maxPolls = options.maxPolls ?? 20;
	const minDelayMs = options.minDelayMs ?? 1000;
	const maxDelayMs = options.maxDelayMs ?? 60000;
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
		throw new DAPError("InvalidMessage", "Invalid collection polling limits");
	let progress = await executeCollection(
		"location" in queryOrState
			? collector.resume(queryOrState)
			: collector.prepare(queryOrState),
		options,
	);
	for (let i = 0; i < maxPolls && progress.status === "pending"; i++) {
		const delay = Math.max(minDelayMs, (progress.retryAfter ?? 0) * 1000);
		if (delay > maxDelayMs) return progress;
		await wait(delay, options.signal);
		progress = await executeCollection(
			collector.resume(progress.state),
			options,
		);
	}
	return progress;
}
