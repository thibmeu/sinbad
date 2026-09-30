import { Collector, prio3Count, prio3Histogram, Task } from "@thibmeu/dap";
import { collect } from "../src/collector.ts";
import { fetchHpkeConfigs } from "../src/fetch.ts";
import { Sinbad } from "../src/index.ts";

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
	await Sinbad.init({ siteId: "site", endpoint: "https://analytics.example/" });
	const result = await Sinbad.track("signup");
	const sent: boolean = result.sent;
	(await Sinbad.track("purchase", { value: 49 })).accepted;
	(await Sinbad.page("/")).accepted;
	await fetchHpkeConfigs(task);
	const histogram = Task.create({ ...options, vdaf: prio3Histogram(4, 2) });
	const collector = await Collector.create(histogram, {
		configId: 1,
		privateKey: new Uint8Array(32),
	});
	const progress = await collect(collector, { start: 0, end: 60_000 });
	if (progress.status === "complete") {
		const buckets: readonly bigint[] = progress.value;
		void buckets;
	}
	// @ts-expect-error A collector is required to collect a batch.
	await collect(task, { start: 0, end: 60_000 });
	return sent;
}
