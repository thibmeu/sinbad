import { prio3Count, prio3Sum, Task } from "dap-ts";
import { Effect } from "effect";
import { collect } from "../src/collector.js";
import { fetchHpkeConfigs } from "../src/fetch.js";
import { createCounter } from "../src/index.js";

export async function checkTypes() {
	const options = {
		id: "8BY0RzZMzxvA46_8ymhzycOB9krN-QIGYvg_RsByGec",
		leader: "https://leader.example/",
		helper: "https://helper.example/",
		timePrecision: 60,
		minBatchSize: 100,
		batchMode: "time-interval" as const,
	};
	const task = Task.create({ ...options, vdaf: prio3Count() });
	const count = await Effect.runPromise(createCounter(task));
	const result = await Effect.runPromise(count());
	result.accepted;
	await Effect.runPromise(fetchHpkeConfigs(task));
	await Effect.runPromise(
		// @ts-expect-error Sinbad's counter only accepts count tasks.
		createCounter(Task.create({ ...options, vdaf: prio3Sum(10) })),
	);
	// @ts-expect-error A collector is required to collect a batch.
	await Effect.runPromise(collect(task, { start: 0, duration: 1 }));
}
