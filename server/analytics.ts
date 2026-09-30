import { DatabaseSync } from "node:sqlite";
import {
	type CollectionQuery,
	type CollectionState,
	Collector,
	Task,
} from "dap-ts";
import { collect } from "../src/collector.ts";
import { bearer, HttpError, port as parsePort, serve } from "./http.ts";

// The collector backend for one metric. It collects fixed windows of the
// task's buckets once each, since DAP collects every bucket at most once,
// and serves the stored totals.

const env = process.env;
const leaderUrl = new URL(env.LEADER_URL ?? "");
const token = env.AUTH_TOKEN;
const privateKey = Uint8Array.fromHex(env.COLLECTOR_PRIVATE_KEY_HEX ?? "");
const configId = Number(env.COLLECTOR_CONFIG_ID ?? 23);
const metric = env.METRIC ?? "page_views";
const category = env.CATEGORY ?? "/";
const port = parsePort(env.PORT, 8090);
if (!token || privateKey.length !== 32 || !metric || !category)
	throw new Error("Invalid analytics configuration");
const authenticate = bearer(token);

const wire = await (await fetch(new URL("task", leaderUrl))).json();
const task = Task.decode({
	id: wire.id,
	configuration: Uint8Array.from(Buffer.from(wire.configuration, "base64url")),
});
// A window is the smallest interval published, so it must hold at least the
// minimum batch; an hour of one-minute buckets suits a quiet site.
const window = Number(env.WINDOW_MS ?? 3_600_000);
if (
	!Number.isSafeInteger(window) ||
	window <= 0 ||
	window % (task.timePrecision * 1000)
)
	throw new Error("WINDOW_MS must be a multiple of the task's time precision");
const collector = await Collector.create(task, { configId, privateKey });

const db = new DatabaseSync(env.DATA_FILE ?? "analytics.sqlite");
db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS collections (start INTEGER PRIMARY KEY, state TEXT, value TEXT, report_count INTEGER);`);
const identity = JSON.stringify({
	task: task.id,
	configuration: wire.configuration,
	metric,
	category,
	window,
});
const saved = db
	.prepare("SELECT value FROM meta WHERE name='identity'")
	.get()?.value;
if (saved && saved !== identity)
	throw new Error("Database belongs to another metric or window");
if (!saved)
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
const pendingWindows = db.prepare(
	"SELECT start FROM collections WHERE state IS NOT NULL",
);

// The task names the Leader by its public URL; this backend may reach it elsewhere.
const routedFetch = (request: Request) => {
	const url = new URL(request.url);
	if (url.origin !== new URL(task.leader).origin)
		throw new Error("Unexpected collector target");
	return fetch(new Request(new URL(url.pathname, leaderUrl), request));
};
const closed = () => Math.floor(Date.now() / window) * window;

const active = new Map<number, Promise<void>>();
async function collectWindow(start: number): Promise<void> {
	if (
		!Number.isSafeInteger(start) ||
		start < 0 ||
		start % window ||
		start >= closed()
	)
		throw new HttpError(400, "Expected a closed window");
	if (get.get(start)?.value != null) return;
	if (!active.has(start))
		active.set(
			start,
			(async () => {
				const state = get.get(start)?.state;
				// A crash after the Leader reserves this window must leave a
				// durable query that can be posted again on restart.
				if (!state)
					save.run(
						start,
						JSON.stringify({ start, end: start + window }),
						null,
						null,
					);
				const progress = await collect(
					collector,
					state
						? (JSON.parse(String(state)) as CollectionQuery | CollectionState)
						: { start, end: start + window },
					{
						fetch: routedFetch,
						headers: { authorization: `Bearer ${token}` },
						maxPolls: 0,
					},
				);
				if (progress.status === "pending")
					save.run(start, JSON.stringify(progress.state), null, null);
				else
					save.run(
						start,
						null,
						JSON.stringify(
							Array.isArray(progress.value)
								? progress.value.map(String)
								: String(progress.value),
						),
						progress.reportCount,
					);
			})().finally(() => active.delete(start)),
		);
	await active.get(start);
}

async function scheduled(): Promise<void> {
	try {
		const reply = await fetch(
			new URL(`internal/ready?window=${window}&before=${closed()}`, leaderUrl),
			{ headers: { authorization: `Bearer ${token}` } },
		);
		if (!reply.ok) throw new Error(`Leader ready request: ${reply.status}`);
		const ready = (await reply.json()) as { start: number }[];
		const starts = new Set([
			...pendingWindows.all().map((row) => Number(row.start)),
			...ready.map((row) => row.start),
		]);
		for (const start of starts)
			await collectWindow(start).catch((error) =>
				console.error("Collection failed", start, error),
			);
	} catch (error) {
		console.error("Collection scan failed", error);
	}
}
setInterval(scheduled, Number(env.SCAN_MS ?? 60_000)).unref();
void scheduled();

serve(
	async (request) => {
		try {
			authenticate(request);
			const url = new URL(request.url);
			if (request.method === "POST" && url.pathname === "/internal/collect") {
				await collectWindow(Number(url.searchParams.get("start")));
				return new Response(null, { status: 204 });
			}
			if (request.method === "GET" && url.pathname === "/api/analytics") {
				const from = Number(url.searchParams.get("from"));
				const to = Number(url.searchParams.get("to"));
				if (
					url.searchParams.get("metric") !== metric ||
					url.searchParams.get("category") !== category ||
					!Number.isSafeInteger(from) ||
					!Number.isSafeInteger(to) ||
					from < 0 ||
					to <= from ||
					to - from > 366 * 86_400_000
				)
					throw new HttpError(400, "Invalid analytics query");
				const windows = list.all(from, to).map((row) => ({
					start: Number(row.start),
					end: Number(row.start) + window,
					value: JSON.parse(String(row.value)) as string | string[],
					reportCount: Number(row.report_count),
				}));
				const sum = (values: string[]) =>
					values.reduce((total, value) => total + BigInt(value), 0n).toString();
				const total =
					task.vdaf.type === "prio3-histogram"
						? Array.from({ length: task.vdaf.length }, (_, i) =>
								sum(windows.map((row) => (row.value as string[])[i]!)),
							)
						: sum(windows.map((row) => row.value as string));
				return Response.json(
					{ metric, category, window, total, windows },
					{ headers: { "cache-control": "no-store" } },
				);
			}
			return new Response("Not found", { status: 404 });
		} catch (error) {
			if (error instanceof HttpError)
				return new Response(error.message, { status: error.status });
			console.error(error);
			return new Response("Collection failed", { status: 502 });
		}
	},
	port,
	env.HOST ?? "127.0.0.1",
	"analytics",
);
