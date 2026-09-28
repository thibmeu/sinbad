import { createServer } from "node:http";
import { DatabaseSync } from "node:sqlite";
import { Collector, prio3Count, prio3Histogram, prio3Sum, Task } from "dap-ts";
import { collect } from "../dist/collector.js";

const leaderUrl = new URL(process.env.LEADER_URL);
const token = process.env.AUTH_TOKEN;
const privateKey = Uint8Array.fromHex(
	process.env.COLLECTOR_PRIVATE_KEY_HEX ?? "",
);
const configId = Number(process.env.COLLECTOR_CONFIG_ID ?? 23);
const metric = process.env.METRIC ?? "page_views";
const category = process.env.CATEGORY ?? "/";
const port = Number(process.env.PORT ?? 8090);
if (
	!token ||
	privateKey.length !== 32 ||
	!Number.isInteger(configId) ||
	configId < 0 ||
	configId > 255 ||
	!Number.isInteger(port) ||
	port < 1 ||
	port > 65535 ||
	!metric ||
	!category
)
	throw new Error("Invalid analytics configuration");
const wireResponse = await fetch(new URL("task", leaderUrl));
if (!wireResponse.ok)
	throw new Error(`Leader task request failed: ${wireResponse.status}`);
const wire = await wireResponse.json();
const kind = process.env.VDAF ?? "count";
const vdaf =
	kind === "sum"
		? prio3Sum(1337)
		: kind === "histogram"
			? prio3Histogram(
					Number(process.env.HISTOGRAM_LENGTH ?? 4),
					Number(process.env.HISTOGRAM_CHUNK_LENGTH ?? 2),
				)
			: kind === "count"
				? prio3Count()
				: undefined;
if (!vdaf) throw new Error("Invalid VDAF");
const task = Task.decode({
	id: wire.id,
	configuration: Uint8Array.fromHex(wire.configuration),
}).expect(vdaf);
const collector = await Collector.create(task, { configId, privateKey });
const db = new DatabaseSync(process.env.DATA_FILE ?? "analytics.sqlite");
db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS collections (start INTEGER PRIMARY KEY, state TEXT, value TEXT, report_count TEXT);`);
const identity = JSON.stringify({
	task: task.id,
	configuration: wire.configuration,
	metric,
	category,
});
const savedIdentity = db
	.prepare("SELECT value FROM meta WHERE name='identity'")
	.get()?.value;
if (savedIdentity && savedIdentity !== identity)
	throw new Error("Database belongs to another metric");
if (!savedIdentity)
	db.prepare("INSERT INTO meta(name,value) VALUES ('identity',?)").run(
		identity,
	);
const get = db.prepare("SELECT * FROM collections WHERE start=?");
const save = db.prepare(
	"INSERT INTO collections(start,state,value,report_count) VALUES (?,?,?,?) ON CONFLICT(start) DO UPDATE SET state=excluded.state,value=excluded.value,report_count=excluded.report_count",
);
const list = db.prepare(
	"SELECT start,value,report_count FROM collections WHERE start>=? AND start<? AND value IS NOT NULL ORDER BY start",
);
const active = new Map();
const routedFetch = (request) => {
	const url = new URL(request.url);
	if (url.origin !== new URL(task.leader).origin)
		throw new Error("Unexpected collector target");
	return fetch(new Request(new URL(url.pathname, leaderUrl), request));
};

async function collectWindow(start) {
	if (
		!Number.isSafeInteger(start) ||
		start < 0 ||
		start >= Math.floor(Date.now() / (task.timePrecision * 1000))
	)
		throw new RangeError("Expected a closed time window");
	if (get.get(start)?.value != null) return;
	if (active.has(start)) return active.get(start);
	const pending = (async () => {
		const row = get.get(start);
		if (!row)
			save.run(start, JSON.stringify({ start, duration: 1 }), null, null);
		const progress = await collect(
			collector,
			row?.state ? JSON.parse(row.state) : { start, duration: 1 },
			{
				fetch: routedFetch,
				headers: { authorization: `Bearer ${token}` },
				maxPolls: 0,
			},
		);
		if (progress.status === "pending")
			save.run(start, JSON.stringify(progress.state), null, null);
		else {
			const value =
				"count" in progress
					? progress.count.toString()
					: "sum" in progress
						? progress.sum.toString()
						: progress.histogram.map((part) => part.toString());
			save.run(
				start,
				null,
				JSON.stringify(value),
				progress.reportCount.toString(),
			);
		}
	})();
	active.set(start, pending);
	try {
		await pending;
	} finally {
		active.delete(start);
	}
}

async function scheduled() {
	try {
		const before = Math.floor(Date.now() / (task.timePrecision * 1000));
		const readyResponse = await fetch(
			new URL(`internal/ready?before=${before}`, leaderUrl),
			{
				headers: { authorization: `Bearer ${token}` },
			},
		);
		if (!readyResponse.ok)
			throw new Error(`Leader ready request failed: ${readyResponse.status}`);
		const ready = await readyResponse.json();
		if (
			!Array.isArray(ready) ||
			ready.some((start) => !Number.isSafeInteger(start))
		)
			throw new Error("Invalid Leader ready response");
		const pending = db
			.prepare("SELECT start FROM collections WHERE state IS NOT NULL")
			.all()
			.map((row) => row.start);
		for (const start of new Set([...pending, ...ready])) {
			try {
				await collectWindow(start);
			} catch (error) {
				console.error("Collection failed", start, error);
			}
		}
	} catch (error) {
		console.error("Collection scan failed", error);
	}
}
setInterval(scheduled, Math.min(task.timePrecision * 1000, 60_000)).unref();
void scheduled();

const server = createServer(async (request, response) => {
	try {
		if (request.headers.authorization !== `Bearer ${token}`) {
			response.writeHead(401).end();
			return;
		}
		const url = new URL(request.url, `http://localhost:${port}`);
		if (request.method === "POST" && url.pathname === "/internal/collect") {
			const raw = url.searchParams.get("start");
			if (!raw || !/^[0-9]+$/.test(raw))
				throw new RangeError("Invalid window start");
			const start = Number(raw);
			await collectWindow(start);
			response.writeHead(204).end();
		} else if (request.method === "GET" && url.pathname === "/api/analytics") {
			const rawFrom = url.searchParams.get("from");
			const rawTo = url.searchParams.get("to");
			const from = Number(rawFrom);
			const to = Number(rawTo);
			if (
				url.searchParams.get("metric") !== metric ||
				url.searchParams.get("category") !== category ||
				!rawFrom ||
				!rawTo ||
				!/^[0-9]+$/.test(rawFrom) ||
				!/^[0-9]+$/.test(rawTo) ||
				!Number.isSafeInteger(from) ||
				!Number.isSafeInteger(to) ||
				from < 0 ||
				to <= from ||
				to - from > 10080
			)
				throw new RangeError("Invalid analytics query");
			const windows = list.all(from, to).map((row) => ({
				start: row.start,
				value: JSON.parse(row.value),
				reportCount: row.report_count,
			}));
			const total =
				kind === "histogram"
					? Array.from({ length: task.vdaf.length }, (_, i) =>
							windows
								.reduce((sum, row) => sum + BigInt(row.value[i]), 0n)
								.toString(),
						)
					: windows
							.reduce((sum, row) => sum + BigInt(row.value), 0n)
							.toString();
			response
				.writeHead(200, {
					"content-type": "application/json",
					"cache-control": "no-store",
				})
				.end(
					JSON.stringify({
						metric,
						category,
						timePrecision: task.timePrecision,
						total,
						windows,
					}),
				);
		} else response.writeHead(404).end();
	} catch (error) {
		console.error(error);
		response
			.writeHead(error instanceof RangeError ? 400 : 502)
			.end(error.message);
	}
});
server.listen(port, process.env.HOST ?? "127.0.0.1", () =>
	console.log(`analytics listening on ${port}`),
);
