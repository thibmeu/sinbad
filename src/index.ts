import { Client, Task, type UploadResult } from "dap-ts";
import { Effect } from "effect";
import {
	asError,
	execute,
	type FetchOptions,
	fetchHpkeConfigs,
} from "./fetch.js";

/** Create a counter for one provisioned Prio3Count task. */
export function createCounter(
	task: Task<number>,
	options: FetchOptions = {},
): Effect.Effect<() => Effect.Effect<UploadResult, Error>, Error> {
	return Effect.gen(function* () {
		yield* Effect.try({
			try: () => {
				if (!(task instanceof Task) || task.vdaf.type !== "prio3-count")
					throw new TypeError("Expected a Prio3Count task");
			},
			catch: asError,
		});
		const hpke = yield* fetchHpkeConfigs(task, options);
		const client = yield* Effect.try({
			try: () => new Client(task, { hpke }),
			catch: asError,
		});
		return () =>
			Effect.gen(function* () {
				const report = yield* Effect.tryPromise({
					try: () => client.prepareReport(1),
					catch: asError,
				});
				const upload = yield* Effect.try({
					try: () => client.prepareUpload([report]),
					catch: asError,
				});
				return yield* execute(upload, options);
			});
	});
}
