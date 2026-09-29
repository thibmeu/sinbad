import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { cpus } from "node:os";
import { performance } from "node:perf_hooks";
import { Client, HpkeConfigList, prio3Count, Task } from "dap-ts";

const mode = process.argv[2];
const count = Number(process.argv[3] ?? 200);
const concurrency = Number(process.argv[4] ?? 10);
const batchSize = Number(process.argv[5] ?? 1);
const vdaf = process.argv[6] ?? "count";
if (
	!["sinbad", "janus"].includes(mode) ||
	!["count", "sum", "histogram"].includes(vdaf) ||
	(mode === "janus" && vdaf !== "count") ||
	!Number.isSafeInteger(count) ||
	count < 1 ||
	!Number.isSafeInteger(concurrency) ||
	concurrency < 1 ||
	!Number.isSafeInteger(batchSize) ||
	batchSize < 1
)
	throw new Error(
		"Usage: node bench/count.mjs sinbad|janus [count] [concurrency] [batchSize] [count|sum|histogram]",
	);

const ports = mode === "sinbad" ? [9011, 9012] : [9001, 9002];
const containers =
	mode === "sinbad"
		? ["sinbad-leader-1", "sinbad-helper-1"]
		: ["test-leader-1", "test-helper-1"];
const endpoint = (role, path) => `http://127.0.0.1:${ports[role]}/${path}`;
const post = async (url, body) => {
	const response = await fetch(url, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify(body),
	});
	assert.equal(response.status, 200, await response.clone().text());
	const result = await response.json();
	assert.equal(
		result.status,
		"success",
		result.error ?? "Janus provisioning failed",
	);
};
async function config(role) {
	const response = await fetch(endpoint(role, "hpke_config"));
	assert.equal(response.status, 200);
	return HpkeConfigList.parse(new Uint8Array(await response.arrayBuffer()));
}
async function setup() {
	if (mode === "sinbad") {
		const response = await fetch(endpoint(0, "task"));
		assert.equal(response.status, 200);
		const wire = await response.json();
		const wireTask = Task.decode({
			id: wire.id,
			configuration: Uint8Array.fromHex(wire.configuration),
		});
		assert.equal(wireTask.vdaf.type, `prio3-${vdaf}`);
		return wireTask;
	}
	const task = Task.create({
		id: crypto.getRandomValues(new Uint8Array(32)).toBase64({
			alphabet: "base64url",
			omitPadding: true,
		}),
		info: "task-info",
		leader: "http://leader:8080/",
		helper: "http://helper:8080/",
		timePrecision: 60,
		minBatchSize: 1,
		batchMode: "time-interval",
		vdaf: prio3Count(),
		testOnly: { dapVersion: 18, allowInsecureHttp: true },
	});
	for (const role of [0, 1]) {
		for (let attempt = 0; ; attempt++) {
			try {
				const response = await fetch(endpoint(role, "internal/test/ready"), {
					method: "POST",
				});
				if (!response.ok)
					throw new Error(`Janus readiness: ${response.status}`);
				break;
			} catch (error) {
				if (attempt === 59) throw error;
				await new Promise((resolve) => setTimeout(resolve, 1000));
			}
		}
	}
	const leaderConfig = await config(0);
	const common = {
		task_id: task.id,
		leader: "http://leader:8080/",
		helper: "http://helper:8080/",
		vdaf: { type: "Prio3Count" },
		leader_authentication_token: "leader-token",
		vdaf_verify_key: new Uint8Array(32).toBase64({
			alphabet: "base64url",
			omitPadding: true,
		}),
		batch_mode: 1,
		min_batch_size: 1,
		time_precision: 60,
		collector_hpke_config: leaderConfig
			.encode()
			.slice(2)
			.toBase64({ alphabet: "base64url", omitPadding: true }),
		task_start: null,
		task_end: null,
	};
	await post(endpoint(0, "internal/test/add_task"), {
		...common,
		role: "leader",
		collector_authentication_token: "collector-token",
	});
	await post(endpoint(1, "internal/test/add_task"), {
		...common,
		role: "helper",
		collector_authentication_token: null,
	});
	return task;
}
function sample(container) {
	const output = execFileSync(
		"docker",
		[
			"exec",
			container,
			"sh",
			"-c",
			"cat /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.peak",
		],
		{ encoding: "utf8" },
	);
	const memory = output.match(/(\d+)\n(\d+)\s*$/);
	assert.ok(memory);
	return {
		cpuUsec: Number(output.match(/usage_usec (\d+)/)?.[1]),
		currentBytes: Number(memory[1]),
		peakBytes: Number(memory[2]),
	};
}
function finishedJanus(role = 0) {
	return Number(
		execFileSync(
			"docker",
			[
				"exec",
				containers[role],
				"psql",
				"-U",
				"postgres",
				"-Atc",
				"SELECT count(*) FROM report_aggregations WHERE state = 'FINISHED'",
			],
			{ encoding: "utf8" },
		).trim(),
	);
}
const task = await setup();
const client = await Client.create(task, {
	hpke: { leader: await config(0), helper: await config(1) },
});
// Keep report preparation outside the timed window.
const minutesAgo = Number(process.env.BENCH_MINUTES_AGO ?? 10);
if (!Number.isSafeInteger(minutesAgo) || minutesAgo < 1)
	throw new Error("BENCH_MINUTES_AGO must be a positive integer");
const time = Math.floor((Date.now() - minutesAgo * 60_000) / 60_000) * 60_000;
const uploads = [];
for (let i = 0; i < count; i += batchSize) {
	const reports = [];
	for (let j = i; j < Math.min(i + batchSize, count); j++)
		reports.push(
			await client.prepareReport(
				vdaf === "sum" ? 42 : vdaf === "histogram" ? 2 : 1,
				{ time },
			),
		);
	uploads.push(client.prepareUpload(reports));
}
const janusStartingCount =
	mode === "janus" ? [finishedJanus(0), finishedJanus(1)] : [0, 0];
const before = containers.map(sample);
const latencies = new Array(uploads.length);
let next = 0;
const started = performance.now();
await Promise.all(
	Array.from({ length: Math.min(uploads.length, concurrency) }, async () => {
		for (;;) {
			const index = next++;
			if (index >= uploads.length) break;
			const upload = uploads[index];
			const at = performance.now();
			const response = await fetch(endpoint(0, `tasks/${task.id}/reports`), {
				method: "POST",
				headers: upload.request.headers,
				body: upload.request.body,
			});
			const outcome = upload.process({
				status: response.status,
				headers: Object.fromEntries(response.headers),
				body: new Uint8Array(await response.arrayBuffer()),
			});
			assert.equal(
				outcome.accepted.length,
				Math.min(batchSize, count - index * batchSize),
				JSON.stringify(outcome.rejected),
			);
			latencies[index] = performance.now() - at;
		}
	}),
);
const uploadsDone = performance.now();
if (mode === "janus") {
	for (;;) {
		const finished = finishedJanus();
		if (finished === janusStartingCount[0] + count) break;
		if (finished > janusStartingCount[0] + count)
			throw new Error("Unexpected Janus report count");
		if (performance.now() - started > 120_000)
			throw new Error(
				`Janus completed ${finished - janusStartingCount[0]}/${count} reports`,
			);
		await new Promise((resolve) => setTimeout(resolve, 250));
	}
	assert.equal(finishedJanus(1) - janusStartingCount[1], count);
} else {
	const start = time / (1000 * task.timePrecision);
	for (const role of [0, 1]) {
		const response = await fetch(
			endpoint(role, `internal/collect?start=${start}`),
			{
				method: "POST",
				headers: { authorization: "Bearer local-count-token" },
			},
		);
		assert.equal(response.status, 200, await response.clone().text());
		assert.equal((await response.json()).reportCount, count);
	}
}
const completed = performance.now();
const after = containers.map(sample);
latencies.sort((a, b) => a - b);
console.log(
	JSON.stringify(
		{
			mode,
			vdaf,
			count,
			concurrency,
			batchSize,
			cpu: cpus()[0]?.model,
			node: process.version,
			linux: execFileSync("uname", ["-r"], { encoding: "utf8" }).trim(),
			image: execFileSync(
				"docker",
				["inspect", "--format", "{{.Config.Image}}", containers[0]],
				{ encoding: "utf8" },
			).trim(),
			uploadP50Ms: latencies[Math.floor(uploads.length * 0.5)],
			uploadP95Ms:
				latencies[
					Math.min(uploads.length - 1, Math.floor(uploads.length * 0.95))
				],
			uploadReportsPerSecond: (count * 1000) / (uploadsDone - started),
			verifiedReportsPerSecond: (count * 1000) / (completed - started),
			roles: containers.map((name, i) => ({
				name,
				cpuMs: (after[i].cpuUsec - before[i].cpuUsec) / 1000,
				baselineMiB: before[i].currentBytes / 2 ** 20,
				containerPeakMiB: after[i].peakBytes / 2 ** 20,
			})),
		},
		null,
		2,
	),
);
