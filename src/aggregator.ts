import {
	type AggregatorOptions,
	checkMediaType,
	DAPError,
	Helper,
	Leader,
	problemResponse,
	type ReportRef,
	type ReportRejectionEntry,
	type Task,
} from "dap-ts";
import { readLimited } from "./fetch.ts";

// A DAP 19 Leader or Helper for one task over a small synchronous SQL
// interface, so the same code runs on node:sqlite and on a Durable Object.
// The Leader stores uploads and aggregates them in `aggregate()` passes the
// host schedules, so an upload never waits for the Helper and a Helper
// outage only delays aggregation.

export type SqlValue = string | number | null | Uint8Array;
export type Row = Record<string, SqlValue>;

/** Synchronous SQLite access. `transaction` runs `fn` atomically. */
export interface Storage {
	query(sql: string, ...params: SqlValue[]): Row[];
	transaction<T>(fn: () => T): T;
}

export interface SinbadAggregatorOptions extends AggregatorOptions {
	readonly role: "leader" | "helper";
	readonly task: Task;
	readonly storage: Storage;
	/**
	 * Bearer token this role requires: from the Leader on the Helper, from
	 * the collector on the Leader.
	 */
	readonly token: string;
	/** Leader only: where and how to reach the Helper. */
	readonly helper?: {
		readonly url: string;
		readonly token: string;
		readonly fetch?: (request: Request) => Promise<Response>;
	};
	/** Leader only: called when an aggregation pass has work to do. */
	readonly schedule?: () => void;
	/** Most reports in one aggregation job. Default 500. */
	readonly maxJobSize?: number;
}

export interface SinbadAggregator {
	readonly role: "leader" | "helper";
	readonly task: Task;
	fetch(request: Request): Promise<Response>;
	/**
	 * Leader only: send saved jobs and build new ones from stored reports.
	 * Resolves once no stored report is left; throws if the Helper failed,
	 * in which case run it again later.
	 */
	aggregate(): Promise<void>;
}

export class HttpError extends Error {
	readonly status: number;
	constructor(status: number, message: string) {
		super(message);
		this.status = status;
	}
}

const SCHEMA = [
	// Every report ID this role accepted, for replay checks. The Leader keeps
	// the encoded report until an aggregation job takes it.
	"CREATE TABLE IF NOT EXISTS reports (id TEXT PRIMARY KEY, time INTEGER NOT NULL, digest BLOB NOT NULL, report BLOB)",
	"CREATE INDEX IF NOT EXISTS waiting ON reports(time) WHERE report IS NOT NULL",
	"CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, request BLOB NOT NULL, state BLOB, times TEXT, response BLOB)",
	"CREATE TABLE IF NOT EXISTS buckets (time INTEGER PRIMARY KEY, bucket BLOB NOT NULL, count INTEGER NOT NULL, pending INTEGER NOT NULL)",
	// Leader collection jobs and Helper aggregate shares. A collected
	// interval takes no more reports.
	"CREATE TABLE IF NOT EXISTS collections (id TEXT PRIMARY KEY, request BLOB NOT NULL, start INTEGER NOT NULL, end INTEGER NOT NULL, collected INTEGER NOT NULL, response BLOB, error TEXT)",
	"CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value BLOB NOT NULL)",
];

const MAX_BODY = 1024 * 1024;

// Browsers upload across origins; the HPKE list and uploads are public.
export const cors = {
	"access-control-allow-origin": "*",
	"access-control-allow-methods": "GET, POST",
	"access-control-allow-headers": "content-type",
	"access-control-max-age": "86400",
};

const bytes = (value: SqlValue | undefined) =>
	new Uint8Array(value as Uint8Array);
const same = (a: Uint8Array, b: Uint8Array) =>
	a.length === b.length && a.every((byte, i) => byte === b[i]);
const hex = (value: Uint8Array) =>
	Array.from(value, (byte) => byte.toString(16).padStart(2, "0")).join("");
const base64url = (value: Uint8Array) =>
	btoa(String.fromCharCode(...value))
		.replaceAll("+", "-")
		.replaceAll("/", "_")
		.replace(/=+$/, "");
const digest = async (value: Uint8Array<ArrayBuffer>) =>
	new Uint8Array(await crypto.subtle.digest("SHA-256", value));

/** Compare bearer tokens over digests, so timing does not depend on the offer. */
async function authenticate(request: Request, token: string): Promise<void> {
	// An unset token closes the resource rather than accepting "Bearer ".
	if (!token) throw new HttpError(401, "Unauthorized");
	const encode = (value: string) => new TextEncoder().encode(value);
	const [offered, expected] = await Promise.all([
		digest(encode(request.headers.get("authorization") ?? "")),
		digest(encode(`Bearer ${token}`)),
	]);
	let difference = 0;
	for (let i = 0; i < 32; i++) difference |= offered[i]! ^ expected[i]!;
	if (difference) throw new HttpError(401, "Unauthorized");
}

function media(request: Request, message: string): void {
	try {
		checkMediaType(request.headers.get("content-type"), message);
	} catch {
		throw new HttpError(415, "Wrong media type");
	}
}

function dap(
	body: Uint8Array<ArrayBuffer>,
	message: string,
	status = 200,
	headers: Record<string, string> = {},
): Response {
	return new Response(body.length ? body : null, {
		status,
		headers: {
			"cache-control": "no-store",
			...(body.length
				? { "content-type": `application/ppm-dap;message=${message}` }
				: {}),
			...headers,
		},
	});
}

const bodyOf = (request: Request) => readLimited(request, MAX_BODY);

export async function createAggregator(
	options: SinbadAggregatorOptions,
): Promise<SinbadAggregator> {
	const { role, task, storage } = options;
	const leader =
		role === "leader" ? await Leader.create(task, options) : undefined;
	const helper =
		role === "helper" ? await Helper.create(task, options) : undefined;
	const aggregator = (leader ?? helper)!;
	const hpkeConfigs = aggregator.hpkeConfigs.encode();
	const configuration = task.encodeConfiguration();
	const unit = task.timePrecision * 1000;
	const q = storage.query.bind(storage);
	const one = (sql: string, ...params: SqlValue[]) => q(sql, ...params)[0];
	const transaction = <T>(fn: () => T) => storage.transaction(fn);

	for (const statement of SCHEMA) q(statement);
	// The task stays fixed for the life of the storage.
	const saved = one("SELECT value FROM meta WHERE name='task'")?.value;
	if (saved && !same(bytes(saved), configuration))
		throw new Error("Storage belongs to a different task configuration");
	if (!saved)
		q("INSERT INTO meta(name,value) VALUES ('task',?)", configuration);

	const collected = (time: number) =>
		Boolean(
			one(
				"SELECT 1 FROM collections WHERE collected=1 AND start<=? AND end>? LIMIT 1",
				time,
				time,
			),
		);
	const bucketRow = (time: number) =>
		one("SELECT * FROM buckets WHERE time=?", time);
	const putBucket = (
		time: number,
		bucket: Uint8Array,
		count: number,
		pending: number,
	) =>
		q(
			"INSERT INTO buckets(time,bucket,count,pending) VALUES (?,?,?,?) ON CONFLICT(time) DO UPDATE SET bucket=excluded.bucket,count=excluded.count,pending=excluded.pending",
			time,
			bucket,
			count,
			pending,
		);

	/** Commit one output share, keeping the bucket's report count queryable. */
	function commit(
		report: ReportRef & { outputShare: Uint8Array },
		pendingDelta = 0,
	): void {
		const row = bucketRow(report.time);
		const bucket = aggregator.addToBucket(
			row ? bytes(row.bucket) : undefined,
			report,
		);
		putBucket(
			report.time,
			bucket,
			aggregator.bucketReportCount(bucket),
			Number(row?.pending ?? 0) + pendingDelta,
		);
	}
	function pend(time: number, delta: number): void {
		const row = bucketRow(time);
		putBucket(
			time,
			row ? bytes(row.bucket) : aggregator.mergeBuckets([]),
			Number(row?.count ?? 0),
			Number(row?.pending ?? 0) + delta,
		);
	}

	// --- Leader ---------------------------------------------------------------

	async function upload(request: Request): Promise<Response> {
		media(request, "upload-req");
		const checked = leader!.upload(await bodyOf(request));
		const digests = await Promise.all(
			checked.reports.map((report) => digest(report.report)),
		);
		const refused: ReportRejectionEntry[] = [];
		transaction(() => {
			checked.reports.forEach((report, i) => {
				const prior = one("SELECT digest FROM reports WHERE id=?", report.id);
				// DAP 19, 4.4.2.2: an identical retry is accepted again; another
				// report with the same ID is a replay.
				if (prior) {
					if (!same(bytes(prior.digest), digests[i]!))
						refused.push({ id: report.id, error: "report-replayed" });
				} else if (collected(report.time))
					refused.push({ id: report.id, error: "batch-collected" });
				else
					q(
						"INSERT INTO reports(id,time,digest,report) VALUES (?,?,?,?)",
						report.id,
						report.time,
						digests[i]!,
						report.report,
					);
			});
		});
		if (checked.reports.length > refused.length) options.schedule?.();
		return dap(checked.respond(refused), "upload-errors");
	}

	function toHelper(
		path: string,
		message: string,
		body: Uint8Array<ArrayBuffer>,
	): Promise<Response> {
		const target = options.helper;
		if (!target) throw new Error("The Leader needs a Helper URL and token");
		const request = new Request(
			new URL(`tasks/${task.id}/${path}`, target.url),
			{
				method: "POST",
				headers: {
					authorization: `Bearer ${target.token}`,
					"content-type": `application/ppm-dap;message=${message}`,
				},
				body,
			},
		);
		return target.fetch ? target.fetch(request) : fetch(request);
	}

	interface LeaderJob {
		id: string;
		request: Uint8Array<ArrayBuffer>;
		state: Uint8Array;
		/** The bucket of each report in the job, released if the job is abandoned. */
		times: number[];
	}

	/** Send a saved job, idempotently, and commit its results once. */
	async function drive(job: LeaderJob): Promise<void> {
		const reply = await toHelper(
			"aggregation_jobs",
			"aggregation-job-init-req",
			job.request,
		);
		const response = new Uint8Array(await reply.arrayBuffer());
		let results: ReturnType<Leader["finish"]> | undefined;
		if (reply.status >= 400 && reply.status < 500)
			// The Helper refused the job itself; abandon it (DAP 19, 4.5.5).
			console.error(
				"Helper refused job",
				job.id,
				reply.status,
				new TextDecoder().decode(response),
			);
		else if (!reply.ok || !response.length)
			throw new Error(`Helper job ${job.id} not ready: HTTP ${reply.status}`);
		else {
			checkMediaType(reply.headers.get("content-type"), "aggregation-job-resp");
			results = leader!.finish(job.state, response);
		}
		transaction(() => {
			if (one("SELECT response FROM jobs WHERE id=?", job.id)?.response) return;
			if (results)
				for (const result of results)
					if (result.outputShare) commit(result, -1);
					else pend(result.time, -1);
			else for (const time of job.times) pend(time, -1);
			q("UPDATE jobs SET response=? WHERE id=?", response, job.id);
		});
	}

	async function pass(): Promise<void> {
		for (const job of q("SELECT * FROM jobs WHERE response IS NULL"))
			await drive({
				id: String(job.id),
				request: bytes(job.request),
				state: bytes(job.state),
				times: JSON.parse(String(job.times)),
			});
		for (;;) {
			const rows = q(
				"SELECT id,report FROM reports WHERE report IS NOT NULL ORDER BY time LIMIT ?",
				options.maxJobSize ?? 500,
			);
			if (!rows.length) return;
			const job = await leader!.prepare(rows.map((row) => bytes(row.report)));
			const id = job.request ? hex(await digest(job.request)) : "";
			const times = job.reports.map((report) => report.time);
			transaction(() => {
				for (const row of rows)
					q("UPDATE reports SET report=NULL WHERE id=?", row.id!);
				if (!job.request) return;
				for (const time of times) pend(time, 1);
				q(
					"INSERT INTO jobs(id,request,state,times,response) VALUES (?,?,?,?,NULL)",
					id,
					job.request,
					job.state,
					JSON.stringify(times),
				);
			});
			for (const rejected of job.rejected)
				console.error("Report rejected", rejected.id, rejected.error);
			if (job.request)
				await drive({ id, request: job.request, state: job.state, times });
		}
	}
	// One pass at a time; callers that arrive during a pass share it.
	let running: Promise<void> | undefined;
	const aggregate = () => {
		if (!leader) return Promise.resolve();
		running ??= pass().finally(() => {
			running = undefined;
		});
		return running;
	};

	/** Finish a collection once its interval is closed, aggregated, and big enough. */
	async function tryCollect(id: string): Promise<void> {
		const row = one("SELECT * FROM collections WHERE id=?", id)!;
		if (row.response || row.error) return;
		const start = Number(row.start);
		const end = Number(row.end);
		if (end > Math.floor((options.clock ?? Date.now)() / unit) * unit) return;
		const job = leader!.collection(bytes(row.request));
		const shareRequest = transaction(() => {
			if (
				one(
					"SELECT 1 FROM reports WHERE report IS NOT NULL AND time>=? AND time<? LIMIT 1",
					start,
					end,
				)
			)
				return undefined;
			const buckets = q(
				"SELECT bucket,pending FROM buckets WHERE time>=? AND time<?",
				start,
				end,
			);
			if (buckets.some((bucket) => Number(bucket.pending))) return undefined;
			const merged = leader!.mergeBuckets(buckets.map((b) => bytes(b.bucket)));
			// DAP 19, 4.6.1: wait for more reports rather than fail.
			if (leader!.bucketReportCount(merged) < task.minBatchSize)
				return undefined;
			q("UPDATE collections SET collected=1 WHERE id=?", id);
			return { merged, body: job.aggregateShareRequest(merged) };
		});
		if (!shareRequest) return;
		const reply = await toHelper(
			"aggregate_shares",
			"aggregate-share-req",
			shareRequest.body,
		);
		const body = new Uint8Array(await reply.arrayBuffer());
		const finish = (response: Uint8Array | null, error: string | null) =>
			q(
				"UPDATE collections SET response=?, error=? WHERE id=?",
				response,
				error,
				id,
			);
		if (reply.status >= 400 && reply.status < 500) {
			// DAP 19, 4.6.1: fail the collection job with the Helper's error.
			let type = "about:blank";
			try {
				type = String(JSON.parse(new TextDecoder().decode(body)).type ?? type);
			} catch {}
			finish(null, type);
			return;
		}
		if (!reply.ok)
			throw new Error(`Helper aggregate share: HTTP ${reply.status}`);
		checkMediaType(reply.headers.get("content-type"), "aggregate-share");
		finish(await job.finish(shareRequest.merged, body), null);
	}

	const collecting = new Map<string, Promise<void>>();
	async function collectionJob(
		request: Request,
		id?: string,
	): Promise<Response> {
		await authenticate(request, options.token);
		if (!options.collector)
			throw new HttpError(503, "Collector key is not configured");
		if (request.method === "POST") {
			media(request, "collection-job-req");
			const body = await bodyOf(request);
			const job = leader!.collection(body);
			const jobId = base64url((await digest(body)).subarray(0, 16));
			id = jobId;
			transaction(() => {
				if (one("SELECT 1 FROM collections WHERE id=?", jobId)) return;
				if (
					one(
						"SELECT 1 FROM collections WHERE id!=? AND start<? AND end>? LIMIT 1",
						jobId,
						job.interval.end,
						job.interval.start,
					)
				)
					throw new DAPError(
						"InvalidMessage",
						"Interval overlaps another collection",
						{ type: "batchOverlap" },
					);
				q(
					"INSERT INTO collections(id,request,start,end,collected,response) VALUES (?,?,?,?,0,NULL)",
					jobId,
					body,
					job.interval.start,
					job.interval.end,
				);
			});
		}
		const jobId = id!;
		if (!one("SELECT 1 FROM collections WHERE id=?", jobId))
			throw new HttpError(404, "Collection not found");
		if (!collecting.has(jobId))
			collecting.set(
				jobId,
				tryCollect(jobId).finally(() => collecting.delete(jobId)),
			);
		await collecting.get(jobId);
		const row = one("SELECT * FROM collections WHERE id=?", jobId)!;
		const location = `/tasks/${task.id}/collection_jobs/${jobId}`;
		if (row.error)
			return Response.json(
				{
					type: row.error,
					title: "Aggregate share request failed",
					taskid: task.id,
				},
				{
					status: 400,
					headers: { "content-type": "application/problem+json" },
				},
			);
		return row.response
			? dap(bytes(row.response), "collection-job-resp", 200, { location })
			: dap(new Uint8Array(), "", 200, { location, "retry-after": "1" });
	}

	// --- Helper ---------------------------------------------------------------

	async function aggregationJob(request: Request): Promise<Response> {
		await authenticate(request, options.token);
		media(request, "aggregation-job-init-req");
		const body = await bodyOf(request);
		const id = hex(await digest(body));
		const location = `/tasks/${task.id}/aggregation_jobs/${id}`;
		const saved = one("SELECT response FROM jobs WHERE id=?", id);
		if (saved)
			return dap(bytes(saved.response), "aggregation-job-resp", 200, {
				location,
			});
		const verified = await helper!.verify(body);
		const response = transaction(() => {
			const prior = one("SELECT response FROM jobs WHERE id=?", id);
			if (prior) return bytes(prior.response);
			const refused: ReportRejectionEntry[] = [];
			for (const report of verified.reports) {
				if (!report.outputShare) continue;
				if (one("SELECT 1 FROM reports WHERE id=?", report.id))
					refused.push({ id: report.id, error: "report-replayed" });
				else if (collected(report.time))
					refused.push({ id: report.id, error: "batch-collected" });
				else {
					q(
						"INSERT INTO reports(id,time,digest,report) VALUES (?,?,?,NULL)",
						report.id,
						report.time,
						new Uint8Array(),
					);
					commit(report);
				}
			}
			const sealed = verified.seal(refused);
			q(
				"INSERT INTO jobs(id,request,state,times,response) VALUES (?,?,NULL,NULL,?)",
				id,
				body,
				sealed,
			);
			return sealed;
		});
		return dap(response, "aggregation-job-resp", 201, { location });
	}

	async function aggregateShare(request: Request): Promise<Response> {
		await authenticate(request, options.token);
		media(request, "aggregate-share-req");
		if (!options.collector)
			throw new HttpError(503, "Collector key is not configured");
		const body = await bodyOf(request);
		const id = base64url((await digest(body)).subarray(0, 16));
		const location = `/tasks/${task.id}/aggregate_shares/${id}`;
		const saved = one("SELECT response FROM collections WHERE id=?", id);
		if (saved?.response)
			return dap(bytes(saved.response), "aggregate-share", 200, { location });
		const job = helper!.aggregateShare(body);
		const { start, end } = job.interval;
		if (
			one(
				"SELECT 1 FROM collections WHERE id!=? AND start<? AND end>? LIMIT 1",
				id,
				end,
				start,
			)
		)
			throw new DAPError(
				"InvalidMessage",
				"Interval overlaps another collection",
				{ type: "batchOverlap" },
			);
		const merged = helper!.mergeBuckets(
			q("SELECT bucket FROM buckets WHERE time>=? AND time<?", start, end).map(
				(row) => bytes(row.bucket),
			),
		);
		const response = await job.finish(merged);
		transaction(() => {
			if (!one("SELECT 1 FROM collections WHERE id=?", id))
				q(
					"INSERT INTO collections(id,request,start,end,collected,response) VALUES (?,?,?,?,1,?)",
					id,
					body,
					start,
					end,
					response,
				);
		});
		return dap(response, "aggregate-share", 201, { location });
	}

	// --- Routes ---------------------------------------------------------------

	async function route(request: Request): Promise<Response> {
		const { pathname, searchParams } = new URL(request.url);
		const method = request.method;
		const base = `/tasks/${task.id}/`;
		const publicPath =
			pathname === "/hpke_config" ||
			(leader !== undefined && pathname === `${base}reports`);
		if (method === "OPTIONS" && publicPath)
			return new Response(null, { status: 204, headers: cors });
		if (method === "GET" && pathname === "/hpke_config")
			// DAP 19, 4.4.1: clients should cache the list.
			return new Response(hpkeConfigs, {
				headers: {
					...cors,
					"content-type": "application/ppm-dap;message=hpke-config-list",
					"cache-control": "public, max-age=86400",
				},
			});
		if (method === "GET" && pathname === "/task")
			return Response.json({
				id: task.id,
				configuration: base64url(configuration),
			});
		if (leader && method === "POST" && pathname === `${base}reports`) {
			const response = await upload(request);
			for (const [name, value] of Object.entries(cors))
				response.headers.set(name, value);
			return response;
		}
		if (leader && method === "POST" && pathname === `${base}collection_jobs`)
			return collectionJob(request);
		if (
			leader &&
			method === "GET" &&
			pathname.startsWith(`${base}collection_jobs/`)
		)
			return collectionJob(
				request,
				pathname.slice(`${base}collection_jobs/`.length),
			);
		if (leader && method === "GET" && pathname === "/internal/ready") {
			// Closed windows a collector can take now, for an analytics backend.
			await authenticate(request, options.token);
			const window = Number(searchParams.get("window"));
			const before = Number(searchParams.get("before"));
			if (
				!Number.isSafeInteger(window) ||
				window <= 0 ||
				window % unit ||
				!Number.isSafeInteger(before)
			)
				throw new HttpError(400, "Invalid window");
			return Response.json(
				q(
					`SELECT start, reports FROM (
						SELECT time - time % ? AS start, SUM(count) AS reports, SUM(pending) AS pending
						FROM buckets WHERE time<? GROUP BY 1) w
					WHERE pending=0 AND reports>=?
						AND NOT EXISTS (SELECT 1 FROM collections c WHERE c.start<w.start+? AND c.end>w.start)
						AND NOT EXISTS (SELECT 1 FROM reports r WHERE r.report IS NOT NULL AND r.time>=w.start AND r.time<w.start+?)
					ORDER BY start LIMIT 100`,
					window,
					before,
					task.minBatchSize,
					window,
					window,
				),
			);
		}
		if (helper && method === "POST" && pathname === `${base}aggregation_jobs`)
			return aggregationJob(request);
		if (
			helper &&
			method === "GET" &&
			pathname.startsWith(`${base}aggregation_jobs/`)
		) {
			await authenticate(request, options.token);
			const job = one(
				"SELECT response FROM jobs WHERE id=?",
				pathname.slice(`${base}aggregation_jobs/`.length),
			);
			if (!job)
				throw new DAPError("InvalidMessage", "Unknown aggregation job", {
					type: "unrecognizedAggregationJob",
				});
			return dap(bytes(job.response), "aggregation-job-resp");
		}
		if (helper && method === "POST" && pathname === `${base}aggregate_shares`)
			return aggregateShare(request);
		if (pathname.startsWith("/tasks/") && !pathname.startsWith(base))
			throw new DAPError("InvalidMessage", "Unknown task", {
				type: "unrecognizedTask",
			});
		throw new HttpError(404, "Not found");
	}

	return {
		role,
		task,
		aggregate,
		async fetch(request) {
			try {
				return await route(request);
			} catch (error) {
				if (error instanceof HttpError)
					return new Response(error.message, { status: error.status });
				if (!(error instanceof DAPError)) console.error(error);
				// DAP 19, 3.6: problem details; unexpected errors stay opaque.
				return problemResponse(error, task.id);
			}
		},
	};
}
