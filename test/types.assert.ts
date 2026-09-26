import { prio3Count, prio3Sum, Task } from "dap-ts";
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
	const count = await createCounter(task);
	const result = await count();
	result.accepted;
	await fetchHpkeConfigs(task);
	// @ts-expect-error Sinbad's counter only accepts count tasks.
	await createCounter(Task.create({ ...options, vdaf: prio3Sum(10) }));
	// @ts-expect-error A collector is required to collect a batch.
	await collect(task, { start: 0, duration: 1 });
}
