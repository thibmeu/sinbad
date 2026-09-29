import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { generateKeyPairSync } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import {
	Client,
	Collector,
	HpkeConfigList,
	prio3Count,
	prio3Histogram,
	prio3Sum,
	Task,
} from "dap-ts";
import {
	encodeAggregateShareRequest,
	encodeCollectionJobRequest,
} from "dap-ts/messages";
import { collect } from "../dist/collector.js";

const root = fileURLToPath(new URL("..", import.meta.url));
const kind = process.argv[2] ?? "count";
const vdaf =
	kind === "sum"
		? prio3Sum(1337)
		: kind === "histogram"
			? prio3Histogram(4, 2)
			: kind === "count"
				? prio3Count()
				: undefined;
if (!vdaf) throw new Error("Expected count, sum, or histogram");
const measurement = kind === "sum" ? 42 : kind === "histogram" ? 2 : 1;
const pair = generateKeyPairSync("x25519");
const collectorPrivateKey = Buffer.from(
	pair.privateKey.export({ format: "jwk" }).d,
	"base64url",
);
const collectorPublicKey = Buffer.from(
	pair.publicKey.export({ format: "jwk" }).x,
	"base64url",
);
const directory = await mkdtemp(join(tmpdir(), `sinbad-${kind}-`));
const token = "test-token";
const children = new Set();
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function freePort() {
	const server = createServer();
	await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
	const port = server.address().port;
	await new Promise((resolve) => server.close(resolve));
	return port;
}
async function start(role, port, helperUrl, extra = {}) {
	const child = spawn(process.execPath, ["server/count.js"], {
		cwd: root,
		env: {
			...process.env,
			ROLE: role,
			PORT: String(port),
			HOST: "127.0.0.1",
			DATA_FILE: join(directory, `${role}.sqlite`),
			AUTH_TOKEN: token,
			VERIFY_KEY_HEX: "00".repeat(32),
			VDAF: kind,
			HELPER_URL: helperUrl,
			COLLECTOR_PUBLIC_KEY_HEX: collectorPublicKey.toString("hex"),
			...extra,
		},
		stdio: ["ignore", "pipe", "pipe"],
	});
	children.add(child);
	let errors = "";
	child.stderr.on("data", (chunk) => {
		errors += chunk;
	});
	for (let i = 0; i < 100; i++) {
		if (child.exitCode !== null) throw new Error(`${role} exited: ${errors}`);
		try {
			if ((await fetch(`http://127.0.0.1:${port}/hpke_config`)).ok)
				return child;
		} catch {}
		await delay(50);
	}
	throw new Error(`${role} did not start: ${errors}`);
}
async function stop(child) {
	if (child.exitCode === null) {
		child.kill("SIGTERM");
		await new Promise((resolve) => child.once("exit", resolve));
	}
	children.delete(child);
}
async function startAnalytics(port, leaderUrl) {
	const child = spawn(process.execPath, ["server/analytics.js"], {
		cwd: root,
		env: {
			...process.env,
			PORT: String(port),
			HOST: "127.0.0.1",
			LEADER_URL: leaderUrl,
			DATA_FILE: join(directory, "analytics.sqlite"),
			AUTH_TOKEN: token,
			COLLECTOR_PRIVATE_KEY_HEX: collectorPrivateKey.toString("hex"),
			VDAF: kind,
			METRIC: "test_metric",
			CATEGORY: "/test",
		},
		stdio: ["ignore", "pipe", "pipe"],
	});
	children.add(child);
	let errors = "";
	child.stderr.on("data", (chunk) => {
		errors += chunk;
	});
	for (let i = 0; i < 100; i++) {
		if (child.exitCode !== null) throw new Error(`analytics exited: ${errors}`);
		try {
			if (
				(
					await fetch(
						`http://127.0.0.1:${port}/api/analytics?metric=test_metric&category=%2Ftest&from=0&to=1`,
						{ headers: { authorization: `Bearer ${token}` } },
					)
				).ok
			)
				return child;
		} catch {}
		await delay(50);
	}
	throw new Error(`analytics did not start: ${errors}`);
}
async function config(port) {
	const response = await fetch(`http://127.0.0.1:${port}/hpke_config`);
	assert.equal(response.status, 200);
	return HpkeConfigList.parse(new Uint8Array(await response.arrayBuffer()));
}
try {
	const leaderPort = await freePort();
	const helperPort = await freePort();
	const leaderUrl = `http://127.0.0.1:${leaderPort}/`;
	const helperUrl = `http://127.0.0.1:${helperPort}/`;
	let helper = await start("helper", helperPort, helperUrl, {
		RESPONSE_DELAY_MS: "1500",
	});
	let leader = await start("leader", leaderPort, helperUrl);
	const taskWire = await (await fetch(new URL("task", leaderUrl))).json();
	const task = Task.decode({
		id: taskWire.id,
		configuration: Uint8Array.fromHex(taskWire.configuration),
	}).expect(vdaf);
	const client = await Client.create(task, {
		hpke: {
			leader: await config(leaderPort),
			helper: await config(helperPort),
		},
	});
	for (const [options, code] of [
		[{ time: Date.now() + 10 * 60_000 }, 8],
		[{ publicExtensions: [{ type: 500, data: new Uint8Array() }] }, 7],
	]) {
		const rejected = client.prepareUpload([
			await client.prepareReport(measurement, options),
		]);
		const reply = await fetch(new URL(`tasks/${task.id}/reports`, leaderUrl), {
			method: "POST",
			headers: rejected.request.headers,
			body: rejected.request.body,
		});
		assert.equal(reply.status, 200);
		const outcome = rejected.process({
			status: reply.status,
			headers: Object.fromEntries(reply.headers),
			body: new Uint8Array(await reply.arrayBuffer()),
		});
		assert.equal(outcome.rejected[0]?.rawCode, code);
	}
	const window0 = (Math.floor(Date.now() / 60_000) - 4) * 60_000;
	const prepared = await client.prepareReport(measurement, { time: window0 });
	const upload = client.prepareUpload([prepared]);
	const target = new URL(`tasks/${task.id}/reports`, leaderUrl);
	const pending = fetch(target, {
		method: "POST",
		headers: upload.request.headers,
		body: upload.request.body,
	}).catch(() => {});
	const helperDb = new DatabaseSync(join(directory, "helper.sqlite"));
	let committed = false;
	for (let i = 0; i < 100; i++) {
		if (helperDb.prepare("SELECT COUNT(*) AS n FROM jobs").get().n === 1) {
			committed = true;
			break;
		}
		await delay(20);
	}
	assert.ok(committed, "Helper did not commit");
	const helperRequest = helperDb
		.prepare("SELECT request FROM jobs")
		.get().request;
	await stop(leader);
	await pending;
	helperDb.close();
	await stop(helper);
	helper = await start("helper", helperPort, helperUrl);
	leader = await start("leader", leaderPort, helperUrl);
	const retried = await fetch(target, {
		method: "POST",
		headers: upload.request.headers,
		body: upload.request.body,
	});
	assert.equal(retried.status, 200, await retried.clone().text());
	const result = upload.process({
		status: retried.status,
		headers: Object.fromEntries(retried.headers),
		body: new Uint8Array(await retried.arrayBuffer()),
	});
	assert.equal(result.accepted.length, 1);
	const again = await fetch(target, {
		method: "POST",
		headers: upload.request.headers,
		body: upload.request.body,
	});
	assert.equal(again.status, 200);
	const retriedJob = await fetch(
		new URL(`tasks/${task.id}/aggregation_jobs`, helperUrl),
		{
			method: "POST",
			headers: {
				authorization: `Bearer ${token}`,
				"content-type": "application/ppm-dap;message=aggregation-job-init-req",
			},
			body: helperRequest,
		},
	);
	assert.equal(retriedJob.status, 200);
	const location = retriedJob.headers.get("location");
	assert.match(
		location,
		new RegExp(`^/tasks/${task.id}/aggregation_jobs/[0-9a-f]{64}$`),
	);
	const storedJob = await fetch(new URL(location, helperUrl), {
		headers: { authorization: `Bearer ${token}` },
	});
	assert.equal(storedJob.status, 200);
	assert.deepEqual(
		new Uint8Array(await storedJob.arrayBuffer()),
		new Uint8Array(await retriedJob.arrayBuffer()),
	);
	const collector = await Collector.create(task, {
		configId: 23,
		privateKey: collectorPrivateKey,
	});
	const routedFetch = (request) =>
		fetch(
			new Request(new URL(new URL(request.url).pathname, leaderUrl), request),
		);
	const collectionOptions = {
		fetch: routedFetch,
		headers: { authorization: `Bearer ${token}` },
		maxPolls: 0,
	};
	const wrongChecksum = await fetch(
		new URL(`tasks/${task.id}/aggregate_shares`, helperUrl),
		{
			method: "POST",
			headers: {
				authorization: `Bearer ${token}`,
				"content-type": "application/ppm-dap;message=aggregate-share-req",
			},
			body: encodeAggregateShareRequest(
				encodeCollectionJobRequest(prepared.time, 1),
				1,
				new Uint8Array(32),
			),
		},
	);
	assert.equal(wrongChecksum.status, 409);
	const first = await collect(
		collector,
		{ start: prepared.time, duration: 1 },
		collectionOptions,
	);
	assert.equal(first.status, "complete");
	assert.equal(first.reportCount, 1n);
	assert.deepEqual(
		first.histogram ?? [first.count ?? first.sum],
		kind === "histogram" ? [0n, 0n, 1n, 0n] : [BigInt(measurement)],
	);
	const firstRetry = await collect(
		collector,
		{ start: prepared.time, duration: 1 },
		collectionOptions,
	);
	assert.deepEqual(firstRetry, first);
	const hexCollector = await Collector.create(task, {
		configId: 23,
		privateKey: Uint8Array.fromHex(collectorPrivateKey.toString("hex")),
	});
	assert.deepEqual(
		await collect(
			hexCollector,
			{ start: prepared.time, duration: 1 },
			collectionOptions,
		),
		first,
	);
	const analyticsPort = await freePort();
	let analytics = await startAnalytics(analyticsPort, leaderUrl);
	const analyticsUrl = `http://127.0.0.1:${analyticsPort}`;
	const auth = { authorization: `Bearer ${token}` };
	assert.equal((await fetch(`${analyticsUrl}/api/analytics`)).status, 401);
	const triggered = await fetch(
		`${analyticsUrl}/internal/collect?start=${prepared.time}`,
		{ method: "POST", headers: auth },
	);
	assert.equal(triggered.status, 204, await triggered.text());
	const query = `${analyticsUrl}/api/analytics?metric=test_metric&category=%2Ftest&from=${prepared.time}&to=${prepared.time + 1}`;
	const read = await fetch(query, { headers: auth });
	assert.equal(read.status, 200);
	const data = await read.json();
	assert.deepEqual(
		data.total,
		kind === "histogram" ? ["0", "0", "1", "0"] : String(measurement),
	);
	assert.deepEqual(data.windows, [
		{
			start: prepared.time,
			value: kind === "histogram" ? ["0", "0", "1", "0"] : String(measurement),
			reportCount: "1",
		},
	]);
	await stop(analytics);
	analytics = await startAnalytics(analyticsPort, leaderUrl);
	assert.equal(
		(await (await fetch(query, { headers: auth })).json()).windows.length,
		1,
	);
	await stop(analytics);
	const prior = client.prepareUpload([
		await client.prepareReport(measurement, { time: window0 + 60_000 }),
	]);
	const priorReply = await fetch(target, {
		method: "POST",
		headers: prior.request.headers,
		body: prior.request.body,
	});
	assert.equal(priorReply.status, 200, await priorReply.clone().text());
	assert.equal(
		prior.process({
			status: priorReply.status,
			headers: Object.fromEntries(priorReply.headers),
			body: new Uint8Array(await priorReply.arrayBuffer()),
		}).accepted.length,
		1,
	);
	const batchTime = window0 + 120_000;
	const batch = client.prepareUpload([
		await client.prepareReport(measurement, { time: batchTime }),
		await client.prepareReport(measurement, { time: Date.now() + 10 * 60_000 }),
		await client.prepareReport(measurement, { time: batchTime }),
	]);
	const batchReply = await fetch(target, {
		method: "POST",
		headers: batch.request.headers,
		body: batch.request.body,
	});
	assert.equal(batchReply.status, 200, await batchReply.clone().text());
	const batchBody = new Uint8Array(await batchReply.arrayBuffer());
	const batchResult = batch.process({
		status: batchReply.status,
		headers: Object.fromEntries(batchReply.headers),
		body: batchBody,
	});
	assert.equal(batchResult.accepted.length, 2);
	assert.equal(batchResult.rejected[0]?.rawCode, 8);
	const batchRetry = await fetch(target, {
		method: "POST",
		headers: batch.request.headers,
		body: batch.request.body,
	});
	assert.equal(batchRetry.status, 200);
	assert.deepEqual(new Uint8Array(await batchRetry.arrayBuffer()), batchBody);
	const batchCollection = await collect(
		collector,
		{ start: Math.floor(batchTime / 60_000), duration: 1 },
		collectionOptions,
	);
	assert.equal(batchCollection.status, "complete");
	assert.equal(batchCollection.reportCount, 2n);
	assert.deepEqual(
		batchCollection.histogram ?? [batchCollection.count ?? batchCollection.sum],
		kind === "histogram" ? [0n, 0n, 2n, 0n] : [BigInt(measurement * 2)],
	);
	const pendingTime = Math.floor(window0 / 60_000) + 3;
	analytics = await startAnalytics(analyticsPort, leaderUrl);
	const priorTime = Math.floor(window0 / 60_000) + 1;
	const priorQuery = `${analyticsUrl}/api/analytics?metric=test_metric&category=%2Ftest&from=${priorTime}&to=${priorTime + 1}`;
	let scheduledWindow;
	for (let i = 0; i < 100; i++) {
		scheduledWindow = (
			await (await fetch(priorQuery, { headers: auth })).json()
		).windows[0];
		if (scheduledWindow) break;
		await delay(20);
	}
	assert.equal(
		scheduledWindow?.reportCount,
		"1",
		"Scheduler did not collect a ready window",
	);
	const pendingResponse = await fetch(
		`${analyticsUrl}/internal/collect?start=${pendingTime}`,
		{ method: "POST", headers: auth },
	);
	assert.equal(pendingResponse.status, 204);
	const analyticsDb = new DatabaseSync(join(directory, "analytics.sqlite"));
	assert.ok(
		analyticsDb
			.prepare("SELECT state FROM collections WHERE start=?")
			.get(pendingTime).state,
	);
	analyticsDb.close();
	await stop(analytics);
	const late = client.prepareUpload([
		await client.prepareReport(measurement, { time: pendingTime * 60_000 }),
	]);
	const lateReply = await fetch(target, {
		method: "POST",
		headers: late.request.headers,
		body: late.request.body,
	});
	assert.equal(lateReply.status, 200);
	assert.equal(
		late.process({
			status: lateReply.status,
			headers: Object.fromEntries(lateReply.headers),
			body: new Uint8Array(await lateReply.arrayBuffer()),
		}).accepted.length,
		1,
	);
	analytics = await startAnalytics(analyticsPort, leaderUrl);
	const resumed = await fetch(
		`${analyticsUrl}/internal/collect?start=${pendingTime}`,
		{ method: "POST", headers: auth },
	);
	assert.equal(resumed.status, 204, await resumed.text());
	const resumedQuery = `${analyticsUrl}/api/analytics?metric=test_metric&category=%2Ftest&from=${pendingTime}&to=${pendingTime + 1}`;
	assert.equal(
		(await (await fetch(resumedQuery, { headers: auth })).json()).windows[0]
			.reportCount,
		"1",
	);
	await stop(analytics);
	const afterCollection = client.prepareUpload([
		await client.prepareReport(measurement, { time: batchTime }),
	]);
	const collectedReply = await fetch(target, {
		method: "POST",
		headers: afterCollection.request.headers,
		body: afterCollection.request.body,
	});
	assert.equal(collectedReply.status, 200);
	assert.equal(
		afterCollection.process({
			status: collectedReply.status,
			headers: Object.fromEntries(collectedReply.headers),
			body: new Uint8Array(await collectedReply.arrayBuffer()),
		}).rejected[0]?.rawCode,
		1,
	);
	console.log(
		`${kind} retries and mixed batches commit each accepted report once`,
	);
	await stop(leader);
	await stop(helper);
} finally {
	for (const child of children) child.kill("SIGTERM");
	await rm(directory, { recursive: true, force: true });
}
