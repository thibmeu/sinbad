import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
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
	type DAPError,
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
import { collect } from "../src/collector.ts";
import { execute } from "../src/fetch.ts";

// Runs a Leader, a Helper, and the analytics backend on loopback ports and
// checks retries, crashes, and collection end to end for one VDAF.

const root = fileURLToPath(new URL("..", import.meta.url));
const kind = process.argv[2] ?? "count";
const vdaf =
	kind === "sum"
		? prio3Sum(1337)
		: kind === "histogram"
			? prio3Histogram(4, 2)
			: prio3Count();
const measurement = kind === "sum" ? 42 : kind === "histogram" ? 2 : 1;
const expected = (reports: number) =>
	kind === "histogram"
		? [0n, 0n, BigInt(reports), 0n]
		: BigInt(measurement * reports);
const pair = generateKeyPairSync("x25519");
const collectorPrivateKey = Buffer.from(
	pair.privateKey.export({ format: "jwk" }).d!,
	"base64url",
);
const collectorPublicKey = Buffer.from(
	pair.publicKey.export({ format: "jwk" }).x!,
	"base64url",
);
const directory = await mkdtemp(join(tmpdir(), `sinbad-${kind}-`));
const token = "test-token";
const auth = { authorization: `Bearer ${token}` };
const children = new Set<ChildProcess>();
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function freePort(): Promise<number> {
	const server = createServer();
	await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
	const { port } = server.address() as { port: number };
	await new Promise((resolve) => server.close(resolve));
	return port;
}
async function spawnServer(
	file: string,
	env: Record<string, string>,
	ready: () => Promise<boolean>,
): Promise<ChildProcess> {
	const child = spawn(process.execPath, [file], {
		cwd: root,
		env: { ...process.env, HOST: "127.0.0.1", AUTH_TOKEN: token, ...env },
		stdio: ["ignore", "pipe", "pipe"],
	});
	children.add(child);
	let errors = "";
	child.stderr!.on("data", (chunk) => {
		errors += chunk;
	});
	for (let i = 0; i < 100; i++) {
		if (child.exitCode !== null) throw new Error(`${file} exited: ${errors}`);
		if (await ready().catch(() => false)) return child;
		await delay(50);
	}
	throw new Error(`${file} did not start: ${errors}`);
}
function startRole(role: string, port: number, helperUrl: string, extra = {}) {
	return spawnServer(
		"server/aggregator.ts",
		{
			ROLE: role,
			PORT: String(port),
			DATA_FILE: join(directory, `${role}.sqlite`),
			VERIFY_KEY_HEX: "00".repeat(32),
			VDAF: kind,
			HELPER_URL: helperUrl,
			AGGREGATE_MS: "100",
			COLLECTOR_PUBLIC_KEY_HEX: collectorPublicKey.toString("hex"),
			...extra,
		},
		async () => (await fetch(`http://127.0.0.1:${port}/hpke_config`)).ok,
	);
}
async function stop(child: ChildProcess): Promise<void> {
	if (child.exitCode === null) {
		child.kill("SIGTERM");
		await new Promise((resolve) => child.once("exit", resolve));
	}
	children.delete(child);
}

try {
	const leaderPort = await freePort();
	const helperPort = await freePort();
	const leaderUrl = `http://127.0.0.1:${leaderPort}/`;
	const helperUrl = `http://127.0.0.1:${helperPort}/`;
	let helper = await startRole("helper", helperPort, helperUrl, {
		RESPONSE_DELAY_MS: "1500",
	});
	let leader = await startRole("leader", leaderPort, helperUrl);
	const wire = await (await fetch(new URL("task", leaderUrl))).json();
	const task = Task.decode({
		id: wire.id,
		configuration: Uint8Array.from(
			Buffer.from(wire.configuration, "base64url"),
		),
	}).expect(vdaf);
	// The task names public HTTPS endpoints; route them to loopback.
	const routed = (request: Request) => {
		const url = new URL(request.url);
		const base =
			url.origin === new URL(task.helper).origin ? helperUrl : leaderUrl;
		return fetch(
			new Request(new URL(url.pathname.slice(1) + url.search, base), request),
		);
	};
	const config = await fetch(new URL("hpke_config", leaderUrl));
	assert.equal(config.headers.get("cache-control"), "public, max-age=86400");
	assert.equal(config.headers.get("access-control-allow-origin"), "*");
	const preflight = await fetch(
		new URL(`tasks/${task.id}/reports`, leaderUrl),
		{
			method: "OPTIONS",
		},
	);
	assert.equal(preflight.status, 204);
	assert.match(
		preflight.headers.get("access-control-allow-headers")!,
		/content-type/,
	);
	const hpkeList = async (base: string) =>
		HpkeConfigList.parse(
			new Uint8Array(
				await (await fetch(new URL("hpke_config", base))).arrayBuffer(),
			),
		);
	const client = await Client.create(task, {
		hpke: {
			leader: await hpkeList(leaderUrl),
			helper: await hpkeList(helperUrl),
		},
	});
	const send = async (times: number[]) =>
		execute(
			client.prepareUpload(
				await Promise.all(
					times.map((time) =>
						client.prepareReport(measurement as never, { time }),
					),
				),
			),
			{ fetch: routed },
		);
	async function waitCommitted(time: number): Promise<void> {
		const stores = ["leader", "helper"].map(
			(role) => new DatabaseSync(join(directory, `${role}.sqlite`)),
		);
		try {
			for (let i = 0; i < 100; i++) {
				if (
					stores.every(
						(db) =>
							db.prepare("SELECT count FROM buckets WHERE time=?").get(time)
								?.count === 1,
					)
				)
					return;
				await delay(50);
			}
			throw new Error(`Report at ${time} was not committed by both roles`);
		} finally {
			for (const db of stores) db.close();
		}
	}

	// Upload checks answer without waiting for the Helper.
	const tooEarly = await send([Date.now() + 10 * 60_000]);
	assert.equal(tooEarly.rejected[0]?.error, "report-too-early");
	const extension = client.prepareUpload([
		await client.prepareReport(measurement as never, {
			publicExtensions: [{ type: 500, data: new Uint8Array() }],
		}),
	]);
	assert.equal(
		(await execute(extension, { fetch: routed })).rejected[0]?.error,
		"unsupported-extension",
	);

	// Two-minute windows, far enough back to be closed.
	const base = Math.floor((Date.now() - 20 * 60_000) / 120_000) * 120_000;

	// The Leader crashes while its job is at the Helper. The Helper has
	// committed; on restart the Leader resends the same job and commits once.
	const first = client.prepareUpload([
		await client.prepareReport(measurement as never, { time: base }),
	]);
	const accepted = await execute(first, { fetch: routed });
	assert.equal(accepted.accepted.length, 1);
	const helperDb = new DatabaseSync(join(directory, "helper.sqlite"));
	for (let i = 0; i < 100 && !helperDb.prepare("SELECT 1 FROM jobs").get(); i++)
		await delay(20);
	assert.ok(
		helperDb.prepare("SELECT 1 FROM jobs").get(),
		"Helper did not commit",
	);
	const helperRequest = helperDb.prepare("SELECT request FROM jobs").get()!
		.request as Uint8Array<ArrayBuffer>;
	helperDb.close();
	await stop(leader);
	await stop(helper);
	helper = await startRole("helper", helperPort, helperUrl);
	leader = await startRole("leader", leaderPort, helperUrl);
	// An identical upload retry is accepted again and not counted twice.
	assert.deepEqual(
		(await execute(first, { fetch: routed })).accepted,
		accepted.accepted,
	);
	assert.equal((await send([base + 60_000])).accepted.length, 1);

	// A retried job returns the stored response at a stable location.
	const retriedJob = await fetch(
		new URL(`tasks/${task.id}/aggregation_jobs`, helperUrl),
		{
			method: "POST",
			headers: {
				...auth,
				"content-type": "application/ppm-dap;message=aggregation-job-init-req",
			},
			body: helperRequest,
		},
	);
	assert.equal(retriedJob.status, 200);
	const stored = await fetch(
		new URL(retriedJob.headers.get("location")!, helperUrl),
		{
			headers: auth,
		},
	);
	assert.deepEqual(
		new Uint8Array(await stored.arrayBuffer()),
		new Uint8Array(await retriedJob.arrayBuffer()),
	);

	// A Leader that disagrees with the Helper's batch gets batchMismatch.
	const unit = task.timePrecision;
	const mismatch = await fetch(
		new URL(`tasks/${task.id}/aggregate_shares`, helperUrl),
		{
			method: "POST",
			headers: {
				...auth,
				"content-type": "application/ppm-dap;message=aggregate-share-req",
			},
			body: encodeAggregateShareRequest(
				encodeCollectionJobRequest(base / 1000 / unit, 2),
				2,
				new Uint8Array(32),
			) as Uint8Array<ArrayBuffer>,
		},
	);
	assert.equal(mismatch.status, 400);
	assert.equal(
		(await mismatch.json()).type,
		"urn:ietf:params:ppm:dap:error:batchMismatch",
	);

	// Collect one two-bucket window.
	const collector = await Collector.create(task, {
		configId: 23,
		privateKey: collectorPrivateKey,
	});
	const options = {
		fetch: routed,
		headers: auth,
		maxPolls: 30,
		minDelayMs: 100,
	};
	const query = { start: base, end: base + 120_000 };
	const result = await collect(collector, query, options);
	assert.equal(result.status, "complete");
	if (result.status !== "complete") throw new Error("unreachable");
	assert.equal(result.reportCount, 2);
	assert.deepEqual(result.value, expected(2));
	assert.deepEqual(result.interval, query);
	assert.deepEqual(await collect(collector, query, options), result);
	await assert.rejects(
		collect(collector, { start: base + 60_000, end: base + 180_000 }, options),
		(error: DAPError) => error.problem?.dapError === "batchOverlap",
	);
	assert.equal((await send([base])).rejected[0]?.error, "batch-collected");
	// A Helper outage after the Leader reserves a window must not make the
	// collection report an overlap with itself on retry.
	const retryWindow = base + 720_000;
	assert.equal((await send([retryWindow])).accepted.length, 1);
	await waitCommitted(retryWindow);
	await stop(helper);
	const retryQuery = { start: retryWindow, end: retryWindow + 120_000 };
	await assert.rejects(
		collect(collector, retryQuery, { ...options, maxPolls: 0 }),
		(error: DAPError) => error.code === "HttpError",
	);
	helper = await startRole("helper", helperPort, helperUrl);
	const recovered = await collect(collector, retryQuery, options);
	assert.equal(recovered.status, "complete");
	if (recovered.status === "complete") assert.equal(recovered.reportCount, 1);

	// The analytics backend collects ready two-minute windows on its own.
	const analyticsPort = await freePort();
	const analyticsUrl = `http://127.0.0.1:${analyticsPort}`;
	const startAnalytics = () =>
		spawnServer(
			"server/analytics.ts",
			{
				PORT: String(analyticsPort),
				LEADER_URL: leaderUrl,
				DATA_FILE: join(directory, "analytics.sqlite"),
				COLLECTOR_PRIVATE_KEY_HEX: collectorPrivateKey.toString("hex"),
				METRIC: "test_metric",
				CATEGORY: "/test",
				WINDOW_MS: "120000",
				SCAN_MS: "200",
			},
			async () =>
				(await fetch(`${analyticsUrl}/api/x`, { headers: auth })).status ===
				404,
		);
	const read = async (from: number, to: number) =>
		(
			await fetch(
				`${analyticsUrl}/api/analytics?metric=test_metric&category=%2Ftest&from=${from}&to=${to}`,
				{ headers: auth },
			)
		).json();
	const next = base + 120_000;
	assert.equal((await send([next, next + 60_000])).accepted.length, 2);
	let analytics = await startAnalytics();
	assert.equal((await fetch(`${analyticsUrl}/api/analytics`)).status, 401);
	let data: {
		total: string | string[];
		windows: { start: number; reportCount: number }[];
	} = { total: "", windows: [] };
	for (let i = 0; i < 100; i++) {
		data = await read(next, next + 120_000);
		if (data.windows.length) break;
		await delay(50);
	}
	const text = (value: bigint | readonly bigint[]) =>
		Array.isArray(value) ? value.map(String) : String(value);
	assert.deepEqual(data.windows, [
		{
			start: next,
			end: next + 120_000,
			value: text(expected(2)),
			reportCount: 2,
		},
	]);
	assert.deepEqual(data.total, text(expected(2)));

	// An empty window stays pending at the Leader, survives a restart, and
	// completes once it has enough reports.
	const empty = base + 240_000;
	const trigger = () =>
		fetch(`${analyticsUrl}/internal/collect?start=${empty}`, {
			method: "POST",
			headers: auth,
		});
	assert.equal((await trigger()).status, 204);
	// A pending job does not block a wider one, and a collector can drop it.
	const wider = await collect(
		collector,
		{ start: empty, end: empty + 180_000 },
		{ ...options, maxPolls: 0 },
	);
	assert.equal(wider.status, "pending");
	if (wider.status === "pending") {
		const dropped = await fetch(
			new URL(new URL(wider.state.location).pathname.slice(1), leaderUrl),
			{ method: "DELETE", headers: auth },
		);
		assert.equal(dropped.status, 204);
	}
	const analyticsDb = new DatabaseSync(join(directory, "analytics.sqlite"));
	assert.ok(
		analyticsDb
			.prepare("SELECT state FROM collections WHERE start=?")
			.get(empty)?.state,
	);
	analyticsDb.close();
	await stop(analytics);
	assert.equal((await send([empty])).accepted.length, 1);
	analytics = await startAnalytics();
	for (let i = 0; i < 100; i++) {
		data = await read(empty, empty + 120_000);
		if (data.windows.length) break;
		await trigger();
		await delay(100);
	}
	assert.equal(data.windows[0]?.reportCount, 1);
	await stop(analytics);
	// Persist the query before contacting the Leader, so a failed request
	// survives a backend restart even without a Location response.
	const offline = base + 600_000;
	analytics = await startAnalytics();
	await stop(leader);
	const failed = await fetch(
		`${analyticsUrl}/internal/collect?start=${offline}`,
		{
			method: "POST",
			headers: auth,
		},
	);
	assert.equal(failed.status, 502);
	const savedIntent = new DatabaseSync(join(directory, "analytics.sqlite"));
	assert.deepEqual(
		JSON.parse(
			String(
				savedIntent
					.prepare("SELECT state FROM collections WHERE start=?")
					.get(offline)?.state,
			),
		),
		{ start: offline, end: offline + 120_000 },
	);
	savedIntent.close();
	await stop(analytics);
	leader = await startRole("leader", leaderPort, helperUrl);
	assert.equal((await send([offline])).accepted.length, 1);
	analytics = await startAnalytics();
	for (let i = 0; i < 100; i++) {
		data = await read(offline, offline + 120_000);
		if (data.windows.length) break;
		await delay(100);
	}
	assert.equal(data.windows[0]?.reportCount, 1);
	await stop(analytics);

	// Two overlapping requests can pass an asynchronous HPKE seal at once;
	// only one may commit its interval at the Helper.
	const overlap = base + 480_000;
	assert.equal((await send([overlap])).accepted.length, 1);
	await waitCommitted(overlap);
	const helperStore = new DatabaseSync(join(directory, "helper.sqlite"));
	const bucket = helperStore
		.prepare("SELECT bucket FROM buckets WHERE time=?")
		.get(overlap)?.bucket as Uint8Array;
	helperStore.close();
	const shareLength = kind === "histogram" ? 64 : 8;
	const checksum = bucket.slice(shareLength + 8, shareLength + 40);
	const requests = [1, 2].map((duration) =>
		fetch(new URL(`tasks/${task.id}/aggregate_shares`, helperUrl), {
			method: "POST",
			headers: {
				...auth,
				"content-type": "application/ppm-dap;message=aggregate-share-req",
			},
			body: encodeAggregateShareRequest(
				encodeCollectionJobRequest(
					overlap / (task.timePrecision * 1000),
					duration,
				),
				1,
				checksum,
			) as Uint8Array<ArrayBuffer>,
		}),
	);
	const simultaneous = await Promise.all(requests);
	assert.deepEqual(
		simultaneous.map((reply) => reply.status).sort(),
		[201, 400],
	);
	assert.equal(
		(await simultaneous.find((reply) => reply.status === 400)!.json()).type,
		"urn:ietf:params:ppm:dap:error:batchOverlap",
	);
	const checkedStore = new DatabaseSync(join(directory, "helper.sqlite"));
	assert.equal(
		checkedStore
			.prepare("SELECT COUNT(*) AS n FROM collections WHERE start<=? AND end>?")
			.get(overlap, overlap)?.n,
		1,
	);
	checkedStore.close();
	console.log(
		`${kind}: crashes, retries, and collection commit each report once`,
	);
} finally {
	for (const child of children) child.kill("SIGTERM");
	await rm(directory, { recursive: true, force: true });
}
