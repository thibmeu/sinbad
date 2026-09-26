import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { DAPClient, HpkeConfigList, prio3Count, Task } from "dap-ts";
import { addCountOutputShare } from "dap-ts/aggregator";

const root = fileURLToPath(new URL("..", import.meta.url));
const directory = await mkdtemp(join(tmpdir(), "sinbad-count-"));
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
			HELPER_URL: helperUrl,
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
	const helper = await start("helper", helperPort, helperUrl, {
		RESPONSE_DELAY_MS: "1500",
	});
	let leader = await start("leader", leaderPort, helperUrl);
	const taskWire = await (await fetch(new URL("task", leaderUrl))).json();
	const task = Task.decode({
		id: taskWire.id,
		configuration: Uint8Array.fromHex(taskWire.configuration),
	}).expect(prio3Count());
	const client = new DAPClient(task, {
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
			await client.prepareReport(1, options),
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
	const prepared = await client.prepareReport(1);
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
	const startTime = prepared.time - (prepared.time % task.timePrecision);
	const parts = [];
	for (const port of [leaderPort, helperPort]) {
		const response = await fetch(
			`http://127.0.0.1:${port}/internal/collect?start=${startTime}`,
			{ method: "POST", headers: { authorization: `Bearer ${token}` } },
		);
		assert.equal(response.status, 200, await response.clone().text());
		parts.push(await response.json());
	}
	assert.deepEqual(
		parts.map((part) => part.reportCount),
		[1, 1],
	);
	const total = addCountOutputShare(
		Uint8Array.fromHex(parts[0].share),
		Uint8Array.fromHex(parts[1].share),
	);
	assert.equal(new DataView(total.buffer).getBigUint64(0, true), 1n);
	console.log(
		"Count report committed once after Leader restart and collected from both stores",
	);
	await stop(leader);
	await stop(helper);
} finally {
	for (const child of children) child.kill("SIGTERM");
	await rm(directory, { recursive: true, force: true });
}
