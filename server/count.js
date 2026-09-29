import { createHash, generateKeyPairSync, timingSafeEqual } from "node:crypto";
import { createServer } from "node:http";
import { DatabaseSync } from "node:sqlite";
import {
	checkMediaType,
	Helper,
	Leader,
	prio3Count,
	prio3Histogram,
	prio3Sum,
	Task,
} from "dap-ts";
import {
	decodeAggregateShare,
	decodeAggregateShareRequest,
	decodeCollectionJobRequest,
	decodeUploadRequest,
	encodeAggregateShare,
	encodeAggregateShareRequest,
	encodeCollectionJobResponse,
	encodeHpkeConfigList,
} from "dap-ts/messages";

const role = process.env.ROLE;
if (role !== "leader" && role !== "helper")
	throw new Error("ROLE must be leader or helper");
const port = Number(process.env.PORT ?? 8080);
const token = process.env.AUTH_TOKEN;
const verifyKey = Uint8Array.fromHex(process.env.VERIFY_KEY_HEX ?? "");
if (
	!Number.isInteger(port) ||
	port < 1 ||
	port > 65535 ||
	!token ||
	verifyKey.length !== 32
)
	throw new Error("Invalid server configuration");
const tokenHash = createHash("sha256").update(`Bearer ${token}`).digest();
const vdaf =
	process.env.VDAF === "sum"
		? prio3Sum(1337)
		: process.env.VDAF === "histogram"
			? prio3Histogram(
					Number(process.env.HISTOGRAM_LENGTH ?? 4),
					Number(process.env.HISTOGRAM_CHUNK_LENGTH ?? 2),
				)
			: process.env.VDAF === undefined || process.env.VDAF === "count"
				? prio3Count()
				: undefined;
if (!vdaf) throw new Error("Invalid VDAF");
const task = Task.create({
	id: "8BY0RzZMzxvA46_8ymhzycOB9krN-QIGYvg_RsByGec",
	info: `sinbad-${process.env.VDAF ?? "count"}-v1`,
	leader: "https://leader.example/",
	helper: "https://helper.example/",
	timePrecision: 60,
	// A real deployment must not publish an aggregate over a handful of
	// reports. Raise MIN_BATCH_SIZE before exposing this to anyone.
	minBatchSize: Number(process.env.MIN_BATCH_SIZE ?? 1),
	batchMode: "time-interval",
	vdaf,
});
const db = new DatabaseSync(process.env.DATA_FILE ?? `${role}.sqlite`);
db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, request BLOB NOT NULL, state BLOB, report_id BLOB NOT NULL, time INTEGER NOT NULL, upload BLOB, response BLOB, upload_response BLOB, initial_errors BLOB);
CREATE TABLE IF NOT EXISTS reports (id BLOB PRIMARY KEY);
CREATE TABLE IF NOT EXISTS buckets (start INTEGER PRIMARY KEY, share BLOB NOT NULL, count INTEGER NOT NULL, pending INTEGER NOT NULL, collected INTEGER NOT NULL, checksum BLOB);
CREATE TABLE IF NOT EXISTS collection_jobs (id TEXT PRIMARY KEY, request BLOB NOT NULL, start INTEGER NOT NULL UNIQUE, response BLOB);
CREATE INDEX IF NOT EXISTS ready_buckets ON buckets(collected,pending,start);`);
if (
	!db
		.prepare("PRAGMA table_info(jobs)")
		.all()
		.some((column) => column.name === "initial_errors")
)
	db.exec("ALTER TABLE jobs ADD COLUMN initial_errors BLOB");
if (
	!db
		.prepare("PRAGMA table_info(buckets)")
		.all()
		.some((column) => column.name === "checksum")
)
	db.exec("ALTER TABLE buckets ADD COLUMN checksum BLOB");
db.exec(
	"UPDATE buckets SET checksum=zeroblob(32) WHERE checksum IS NULL AND count=0",
);
if (
	db
		.prepare(
			"SELECT 1 FROM buckets WHERE checksum IS NULL AND count>0 AND collected=0 LIMIT 1",
		)
		.get()
)
	throw new Error(
		"Existing uncollected buckets lack report checksums; use a fresh database or migrate them",
	);
const sql = {
	meta: db.prepare("SELECT value FROM meta WHERE name=?"),
	putMeta: db.prepare("INSERT INTO meta(name,value) VALUES (?,?)"),
	job: db.prepare("SELECT * FROM jobs WHERE id=?"),
	leaderJob: db.prepare(
		"INSERT INTO jobs(id,request,state,report_id,time,upload,initial_errors) VALUES (?,?,?,?,?,?,?)",
	),
	helperJob: db.prepare(
		"INSERT INTO jobs(id,request,report_id,time,response) VALUES (?,?,?,?,?)",
	),
	finish: db.prepare(
		"UPDATE jobs SET response=?, upload_response=? WHERE id=?",
	),
	report: db.prepare("SELECT 1 FROM reports WHERE id=?"),
	claim: db.prepare("INSERT INTO reports(id) VALUES (?)"),
	bucket: db.prepare("SELECT * FROM buckets WHERE start=?"),
	ensureBucket: db.prepare(
		"INSERT OR IGNORE INTO buckets(start,share,count,pending,collected,checksum) VALUES (?,?,?,?,0,?)",
	),
	updateBucket: db.prepare(
		"UPDATE buckets SET share=?, count=?, pending=?, checksum=? WHERE start=?",
	),
	collect: db.prepare("UPDATE buckets SET collected=1 WHERE start=?"),
	collectionJob: db.prepare("SELECT * FROM collection_jobs WHERE id=?"),
	collectionForStart: db.prepare("SELECT * FROM collection_jobs WHERE start=?"),
	insertCollection: db.prepare(
		"INSERT INTO collection_jobs(id,request,start) VALUES (?,?,?)",
	),
	finishCollection: db.prepare(
		"UPDATE collection_jobs SET response=? WHERE id=?",
	),
	ready: db.prepare(
		"SELECT start FROM buckets WHERE collected=0 AND pending=0 AND count>=? AND start<? ORDER BY start LIMIT 100",
	),
};
const zero = new Uint8Array(
	task.vdaf.type === "prio3-histogram" ? task.vdaf.length * 16 : 8,
);
// Histogram Leader state includes one Field128 share per bucket and a joint seed.
const stateLength =
	task.vdaf.type === "prio3-histogram" ? task.vdaf.length * 16 + 32 : 8;
const owned = (bytes) => new Uint8Array(bytes);
const zeroChecksum = new Uint8Array(32);
const same = (a, b) =>
	a.length === b.length && a.every((value, i) => value === b[i]);
function bucket(time) {
	// DAP report times are already expressed in time-precision units.
	const start = Number(time);
	sql.ensureBucket.run(start, zero, 0, 0, zeroChecksum);
	return sql.bucket.get(start);
}
function transaction(fn) {
	db.exec("BEGIN IMMEDIATE");
	try {
		const value = fn();
		db.exec("COMMIT");
		return value;
	} catch (error) {
		db.exec("ROLLBACK");
		throw error;
	}
}
function add(row, share, reportId, pending = row.pending) {
	if (!row.checksum)
		throw new HttpError(409, "Legacy bucket has no report checksum");
	const checksum = owned(row.checksum);
	const digest = createHash("sha256").update(reportId).digest();
	for (let i = 0; i < 32; i++) checksum[i] ^= digest[i];
	sql.updateBucket.run(
		aggregator.addShare(owned(row.share), share),
		row.count + 1,
		pending,
		checksum,
		row.start,
	);
}
const uploadError = (id, code) => Uint8Array.of(...id, code);

let privateKey = sql.meta.get("private")?.value;
let publicKey = sql.meta.get("public")?.value;
const savedTask = sql.meta.get("task")?.value;
const taskBytes = task.encodeConfiguration();
if (savedTask && !same(savedTask, taskBytes))
	throw new Error("Database belongs to a different task configuration");
if (!savedTask) sql.putMeta.run("task", taskBytes);
if (!privateKey || !publicKey) {
	const pair = generateKeyPairSync("x25519");
	privateKey = Buffer.from(
		pair.privateKey.export({ format: "jwk" }).d,
		"base64url",
	);
	publicKey = Buffer.from(
		pair.publicKey.export({ format: "jwk" }).x,
		"base64url",
	);
	transaction(() => {
		sql.putMeta.run("private", privateKey);
		sql.putMeta.run("public", publicKey);
	});
}
const configId = role === "leader" ? 7 : 8;
const aggregator = await (role === "leader" ? Leader : Helper).create(task, {
	hpke: { configId, privateKey: owned(privateKey) },
	verificationKeyId: 0,
	verifyKey,
});
const hpkeConfigs = encodeHpkeConfigList([
	{ id: configId, kemId: 32, kdfId: 1, aeadId: 1, publicKey: owned(publicKey) },
]);
const collectorKey = process.env.COLLECTOR_PUBLIC_KEY_HEX
	? {
			id: Number(process.env.COLLECTOR_CONFIG_ID ?? 23),
			kemId: 32,
			kdfId: 1,
			aeadId: 1,
			publicKey: Uint8Array.fromHex(process.env.COLLECTOR_PUBLIC_KEY_HEX),
		}
	: undefined;
if (
	collectorKey &&
	(!Number.isInteger(collectorKey.id) ||
		collectorKey.id < 0 ||
		collectorKey.id > 255 ||
		collectorKey.publicKey.length !== 32)
)
	throw new Error("Invalid collector HPKE configuration");
if (collectorKey) {
	const encoded = Buffer.concat([
		Buffer.from([collectorKey.id]),
		collectorKey.publicKey,
	]);
	const saved = sql.meta.get("collector")?.value;
	if (saved && !same(saved, encoded))
		throw new Error("Database belongs to another collector key");
	if (!saved) sql.putMeta.run("collector", encoded);
}

class HttpError extends Error {
	constructor(status, message, type = "invalidMessage") {
		super(message);
		this.status = status;
		this.type = type;
	}
}

/** DAP 19, 3.6: report errors as RFC 9457 problem details. */
function problem(response, error) {
	const status = error?.status ?? 500;
	// Only an error this server raised deliberately is safe to describe.
	const known = error instanceof HttpError;
	send(
		response,
		status,
		JSON.stringify({
			type: known
				? `urn:ietf:params:ppm:dap:error:${error.type}`
				: "about:blank",
			title: known ? error.message : "Internal server error",
			status,
			taskid: task.id,
		}),
		"application/problem+json",
	);
}
function authenticated(request) {
	// Hash first so the comparison is over equal-length buffers.
	const offered = createHash("sha256")
		.update(String(request.headers.authorization ?? ""))
		.digest();
	if (!timingSafeEqual(offered, tokenHash))
		throw new HttpError(401, "Unauthorized");
}
function media(request, name) {
	// RFC 9110 allows whitespace and extra parameters, so parse rather than
	// compare the header verbatim.
	try {
		checkMediaType(
			{ "content-type": request.headers["content-type"] ?? "" },
			name,
		);
	} catch {
		throw new HttpError(415, "Wrong media type");
	}
}
function send(
	response,
	status,
	body,
	type = "application/octet-stream",
	headers = {},
) {
	response.writeHead(status, {
		"content-type": type,
		"cache-control": "no-store",
		...headers,
	});
	response.end(body);
}
async function readBody(request) {
	const chunks = [];
	let size = 0;
	for await (const chunk of request) {
		size += chunk.length;
		if (size > 1024 * 1024) throw new HttpError(413, "Request too large");
		chunks.push(chunk);
	}
	return Buffer.concat(chunks, size);
}

async function helperJob(request, response) {
	authenticated(request);
	media(request, "aggregation-job-init-req");
	const body = await readBody(request);
	const jobId = createHash("sha256").update(body).digest("hex");
	const location = `/tasks/${task.id}/aggregation_jobs/${jobId}`;
	const saved = sql.job.get(jobId);
	if (saved) {
		send(
			response,
			200,
			saved.response,
			"application/ppm-dap;message=aggregation-job-resp",
			{ location },
		);
		return;
	}
	const result = await aggregator.verify(body);
	const outbound = transaction(() => {
		const prior = sql.job.get(jobId);
		if (prior) return owned(prior.response);
		const responses = [];
		for (const report of result.reports) {
			let value = report.response;
			if (report.outputShare) {
				const row = bucket(report.time);
				if (sql.report.get(report.reportId))
					value = aggregator.reject(report.reportId, 2);
				else if (row.collected) value = aggregator.reject(report.reportId, 1);
				else {
					sql.claim.run(report.reportId);
					add(row, report.outputShare, report.reportId);
				}
			}
			responses.push(value);
		}
		// seal() checks that the list still lines up with the job.
		const value = Buffer.from(result.seal(responses));
		sql.helperJob.run(
			jobId,
			body,
			result.reports[0].reportId,
			Number(result.reports[0].time),
			value,
		);
		return value;
	});
	if (Number(process.env.RESPONSE_DELAY_MS) > 0)
		await new Promise((resolve) =>
			setTimeout(resolve, Number(process.env.RESPONSE_DELAY_MS)),
		);
	send(
		response,
		200,
		outbound,
		"application/ppm-dap;message=aggregation-job-resp",
		{ location },
	);
}

const reportErrorCodes = {
	ReportTooEarly: 8,
	ReportDropped: 3,
	InvalidReport: 7,
	InvalidHpkeConfig: 4,
	DecryptionFailed: 5,
};

async function leaderUpload(request, response) {
	media(request, "upload-req");
	const body = await readBody(request);
	const reports = decodeUploadRequest(body);
	const ids = reports.map((report) => report.metadata.id.toHex());
	if (new Set(ids).size !== ids.length)
		throw new HttpError(400, "Duplicate report ID in upload");
	const jobId =
		reports.length === 1
			? ids[0]
			: createHash("sha256").update(body).digest("hex");
	let saved = sql.job.get(jobId);
	if (saved && !same(saved.upload, body)) {
		send(
			response,
			200,
			uploadError(reports[0].metadata.id, 2),
			"application/ppm-dap;message=upload-errors",
		);
		return;
	}
	if (!saved) {
		const errors = new Map();
		const candidates = [];
		for (const report of reports) {
			const id = report.metadata.id.toHex();
			const row = sql.bucket.get(Number(report.metadata.time));
			if (sql.report.get(report.metadata.id)) errors.set(id, 2);
			else if (row?.collected) errors.set(id, 1);
			else candidates.push(report);
		}
		const job = candidates.length
			? await aggregator.prepare(candidates)
			: { reports: [], rejected: [] };
		for (const rejected of job.rejected) {
			const code = reportErrorCodes[rejected.error.code];
			if (!code) throw rejected.error;
			errors.set(rejected.reportId.toHex(), code);
		}
		const initialErrors = Buffer.concat(
			reports.flatMap((report) => {
				const code = errors.get(report.metadata.id.toHex());
				return code ? [uploadError(report.metadata.id, code)] : [];
			}),
		);
		if (!job.reports.length) {
			send(
				response,
				200,
				initialErrors,
				"application/ppm-dap;message=upload-errors",
			);
			return;
		}
		transaction(() => {
			const prior = sql.job.get(jobId);
			if (prior) {
				if (!same(prior.upload, body))
					throw new HttpError(409, "Job ID conflict");
				return;
			}
			for (const report of job.reports) {
				const row = bucket(report.time);
				if (sql.report.get(report.reportId) || row.collected)
					throw new HttpError(409, "Report replayed or bucket collected");
				sql.updateBucket.run(
					row.share,
					row.count,
					row.pending + 1,
					row.checksum,
					row.start,
				);
			}
			sql.leaderJob.run(
				jobId,
				job.request,
				Buffer.concat(job.reports.map((report) => report.state)),
				Buffer.concat(job.reports.map((report) => report.reportId)),
				Number(job.reports[0].time),
				body,
				initialErrors,
			);
		});
		saved = sql.job.get(jobId);
	}
	if (saved.upload_response) {
		send(
			response,
			200,
			saved.upload_response,
			"application/ppm-dap;message=upload-errors",
		);
		return;
	}
	const peer = await fetch(
		new URL(`tasks/${task.id}/aggregation_jobs`, process.env.HELPER_URL),
		{
			method: "POST",
			headers: {
				authorization: `Bearer ${token}`,
				"content-type": "application/ppm-dap;message=aggregation-job-init-req",
			},
			body: saved.request,
		},
	);
	if (!peer.ok) throw new HttpError(502, `Helper returned ${peer.status}`);
	const inbound = new Uint8Array(await peer.arrayBuffer());
	const times = new Map(
		reports.map((report) => [report.metadata.id.toHex(), report.metadata.time]),
	);
	const states = [];
	for (let offset = 0; offset < saved.report_id.length; offset += 16) {
		const reportId = owned(saved.report_id.subarray(offset, offset + 16));
		states.push({
			reportId,
			time: times.get(reportId.toHex()),
			state: owned(
				saved.state.subarray(
					(offset / 16) * stateLength,
					(offset / 16 + 1) * stateLength,
				),
			),
		});
	}
	const results = aggregator.finish(states, inbound);
	const outbound = transaction(() => {
		const current = sql.job.get(jobId);
		if (current.upload_response) {
			if (!same(current.response, inbound))
				throw new HttpError(409, "Helper response changed");
			return owned(current.upload_response);
		}
		const errors = new Map();
		for (
			let offset = 0;
			offset < (current.initial_errors?.length ?? 0);
			offset += 17
		)
			errors.set(
				current.initial_errors.subarray(offset, offset + 16).toHex(),
				current.initial_errors[offset + 16],
			);
		for (const result of results) {
			const row = bucket(result.time);
			if ("outputShare" in result) {
				if (sql.report.get(result.reportId) || row.collected)
					throw new HttpError(409, "Report cannot be committed");
				sql.claim.run(result.reportId);
				add(row, result.outputShare, result.reportId, row.pending - 1);
			} else {
				errors.set(result.reportId.toHex(), result.reportError);
				sql.updateBucket.run(
					row.share,
					row.count,
					row.pending - 1,
					row.checksum,
					row.start,
				);
			}
		}
		const value = Buffer.concat(
			reports.flatMap((report) => {
				const code = errors.get(report.metadata.id.toHex());
				return code ? [uploadError(report.metadata.id, code)] : [];
			}),
		);
		sql.finish.run(inbound, value, jobId);
		return value;
	});
	send(response, 200, outbound, "application/ppm-dap;message=upload-errors");
}

function collectionInterval(body) {
	let query;
	try {
		query = decodeCollectionJobRequest(body);
	} catch {
		throw new HttpError(400, "Invalid collection request");
	}
	const start = Number(query.start);
	if (
		!Number.isSafeInteger(start) ||
		query.duration !== 1n ||
		start >= Math.floor(Date.now() / (task.timePrecision * 1000))
	)
		throw new HttpError(400, "Expected one closed time window", "batchInvalid");
	return start;
}

async function helperAggregateShare(request, response) {
	authenticated(request);
	media(request, "aggregate-share-req");
	if (!collectorKey)
		throw new HttpError(503, "Collector key is not configured");
	const body = await readBody(request);
	let query;
	try {
		query = decodeAggregateShareRequest(body);
	} catch {
		throw new HttpError(400, "Invalid aggregate share request");
	}
	const start = collectionInterval(query.collectionRequest);
	const id = createHash("sha256").update(body).digest("hex");
	const saved = sql.collectionJob.get(id);
	if (saved?.response) {
		send(
			response,
			200,
			saved.response,
			"application/ppm-dap;message=aggregate-share",
		);
		return;
	}
	if (sql.collectionForStart.get(start) && !saved)
		throw new HttpError(409, "Window already has a collection", "batchOverlap");
	const row = bucket(start);
	if (
		row.pending ||
		row.count < task.minBatchSize ||
		!row.checksum ||
		BigInt(row.count) !== query.reportCount ||
		!same(row.checksum, query.checksum) ||
		(row.collected && !saved)
	)
		throw new HttpError(409, "Aggregate batch does not match", "batchMismatch");
	const encrypted = encodeAggregateShare(
		await aggregator.encryptShare(
			query.collectionRequest,
			owned(row.share),
			collectorKey,
		),
	);
	const result = transaction(() => {
		const current = bucket(start);
		if (
			current.pending ||
			current.count !== row.count ||
			!same(current.checksum, row.checksum)
		)
			throw new HttpError(409, "Aggregate batch changed", "batchMismatch");
		if (!saved) sql.insertCollection.run(id, body, start);
		sql.collect.run(start);
		sql.finishCollection.run(encrypted, id);
		return encrypted;
	});
	send(response, 200, result, "application/ppm-dap;message=aggregate-share");
}

async function collectionJob(request, response, id) {
	authenticated(request);
	if (!collectorKey)
		throw new HttpError(503, "Collector key is not configured");
	let saved;
	if (request.method === "POST") {
		media(request, "collection-job-req");
		const body = await readBody(request);
		const start = collectionInterval(body);
		id = createHash("sha256")
			.update(body)
			.digest()
			.subarray(0, 16)
			.toString("base64url");
		saved = sql.collectionJob.get(id);
		if (saved && !same(saved.request, body))
			throw new HttpError(409, "Collection ID conflict");
		if (!saved) {
			transaction(() => {
				if (
					sql.collectionForStart.get(start) ||
					sql.bucket.get(start)?.collected
				)
					throw new HttpError(409, "Window already collected", "batchOverlap");
				sql.insertCollection.run(id, body, start);
			});
			saved = sql.collectionJob.get(id);
		}
	} else {
		if (!/^[A-Za-z0-9_-]{22}$/.test(id))
			throw new HttpError(400, "Invalid collection ID");
		saved = sql.collectionJob.get(id);
		if (!saved) throw new HttpError(404, "Collection not found");
	}
	const location = `/tasks/${task.id}/collection_jobs/${id}`;
	if (!saved.response) {
		const row = bucket(saved.start);
		if (!row.pending && row.count >= task.minBatchSize && row.checksum) {
			transaction(() => {
				const current = bucket(saved.start);
				if (
					current.pending ||
					current.count !== row.count ||
					!same(current.checksum, row.checksum)
				)
					throw new HttpError(409, "Collection batch changed", "batchMismatch");
				sql.collect.run(saved.start);
			});
			const peer = await fetch(
				new URL(`tasks/${task.id}/aggregate_shares`, process.env.HELPER_URL),
				{
					method: "POST",
					headers: {
						authorization: `Bearer ${token}`,
						"content-type": "application/ppm-dap;message=aggregate-share-req",
					},
					body: encodeAggregateShareRequest(
						saved.request,
						row.count,
						owned(row.checksum),
					),
				},
			);
			if (!peer.ok) throw new HttpError(502, `Helper returned ${peer.status}`);
			const helper = decodeAggregateShare(
				new Uint8Array(await peer.arrayBuffer()),
			);
			const leader = await aggregator.encryptShare(
				saved.request,
				owned(row.share),
				collectorKey,
			);
			const value = encodeCollectionJobResponse({
				reportCount: BigInt(row.count),
				start: BigInt(saved.start),
				duration: 1n,
				leader,
				helper,
			});
			transaction(() => {
				if (!sql.collectionJob.get(id).response)
					sql.finishCollection.run(value, id);
			});
			saved = sql.collectionJob.get(id);
		}
	}
	send(
		response,
		200,
		saved.response ?? new Uint8Array(),
		saved.response
			? "application/ppm-dap;message=collection-job-resp"
			: "application/octet-stream",
		{ location, ...(saved.response ? {} : { "retry-after": "1" }) },
	);
}

const server = createServer(async (request, response) => {
	try {
		const url = new URL(request.url, `http://localhost:${port}`);
		if (request.method === "GET" && url.pathname === "/hpke_config")
			send(
				response,
				200,
				hpkeConfigs,
				"application/ppm-dap;message=hpke-config-list",
			);
		else if (request.method === "GET" && url.pathname === "/task")
			send(
				response,
				200,
				JSON.stringify({
					id: task.id,
					configuration: task.encodeConfiguration().toHex(),
				}),
				"application/json",
			);
		else if (
			request.method === "GET" &&
			role === "leader" &&
			url.pathname === "/internal/ready"
		) {
			authenticated(request);
			const before = url.searchParams.get("before");
			if (
				!before ||
				!/^[0-9]+$/.test(before) ||
				!Number.isSafeInteger(Number(before))
			)
				throw new HttpError(400, "Invalid window boundary");
			send(
				response,
				200,
				JSON.stringify(
					sql.ready
						.all(task.minBatchSize, Number(before))
						.map((row) => row.start),
				),
				"application/json",
			);
		} else if (
			request.method === "POST" &&
			role === "leader" &&
			url.pathname === `/tasks/${task.id}/reports`
		)
			await leaderUpload(request, response);
		else if (
			request.method === "POST" &&
			role === "helper" &&
			url.pathname === `/tasks/${task.id}/aggregation_jobs`
		)
			await helperJob(request, response);
		else if (
			request.method === "GET" &&
			role === "helper" &&
			url.pathname.startsWith(`/tasks/${task.id}/aggregation_jobs/`)
		) {
			authenticated(request);
			const jobId = url.pathname.split("/").at(-1);
			if (!/^[0-9a-f]{64}$/.test(jobId))
				throw new HttpError(400, "Invalid job ID");
			const job = sql.job.get(jobId);
			if (!job)
				throw new HttpError(404, "Job not found", "unrecognizedAggregationJob");
			send(
				response,
				200,
				job.response,
				"application/ppm-dap;message=aggregation-job-resp",
			);
		} else if (
			role === "helper" &&
			request.method === "POST" &&
			url.pathname === `/tasks/${task.id}/aggregate_shares`
		)
			await helperAggregateShare(request, response);
		else if (
			role === "leader" &&
			request.method === "POST" &&
			url.pathname === `/tasks/${task.id}/collection_jobs`
		)
			await collectionJob(request, response);
		else if (
			role === "leader" &&
			request.method === "GET" &&
			url.pathname.startsWith(`/tasks/${task.id}/collection_jobs/`)
		)
			await collectionJob(request, response, url.pathname.split("/").at(-1));
		else throw new HttpError(404, "Not found", "unrecognizedTask");
	} catch (error) {
		console.error(error);
		if (!response.headersSent) problem(response, error);
		else response.end();
	}
});
server.listen(port, process.env.HOST ?? "127.0.0.1", () =>
	console.log(`${role} listening on ${port}`),
);
