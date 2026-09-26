import { createHash, generateKeyPairSync } from "node:crypto";
import { createServer } from "node:http";
import { DatabaseSync } from "node:sqlite";
import { prio3Count, Task } from "dap-ts";
import {
	addCountOutputShare,
	encodeCountJobRejection,
	helperCountBatchInit,
	leaderCountBatchFinish,
	leaderCountBatchInit,
	prepareAggregatorKey,
} from "dap-ts/aggregator";
import { decodeUploadRequest, encodeHpkeConfigList } from "dap-ts/messages";

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
const task = Task.create({
	id: "8BY0RzZMzxvA46_8ymhzycOB9krN-QIGYvg_RsByGec",
	info: "sinbad-count-v1",
	leader: "https://leader.example/",
	helper: "https://helper.example/",
	timePrecision: 60,
	minBatchSize: 1,
	batchMode: "time-interval",
	vdaf: prio3Count(),
});
const db = new DatabaseSync(process.env.DATA_FILE ?? `${role}.sqlite`);
db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, request BLOB NOT NULL, state BLOB, report_id BLOB NOT NULL, time INTEGER NOT NULL, upload BLOB, response BLOB, upload_response BLOB, initial_errors BLOB);
CREATE TABLE IF NOT EXISTS reports (id BLOB PRIMARY KEY);
CREATE TABLE IF NOT EXISTS buckets (start INTEGER PRIMARY KEY, share BLOB NOT NULL, count INTEGER NOT NULL, pending INTEGER NOT NULL, collected INTEGER NOT NULL);`);
if (
	!db
		.prepare("PRAGMA table_info(jobs)")
		.all()
		.some((column) => column.name === "initial_errors")
)
	db.exec("ALTER TABLE jobs ADD COLUMN initial_errors BLOB");
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
		"INSERT OR IGNORE INTO buckets(start,share,count,pending,collected) VALUES (?,?,?,?,0)",
	),
	updateBucket: db.prepare(
		"UPDATE buckets SET share=?, count=?, pending=? WHERE start=?",
	),
	collect: db.prepare("UPDATE buckets SET collected=1 WHERE start=?"),
};
const zero = new Uint8Array(8);
const owned = (bytes) => new Uint8Array(bytes);
const same = (a, b) =>
	a.length === b.length && a.every((value, i) => value === b[i]);
function bucket(time) {
	// DAP report times are already expressed in time-precision units.
	const start = Number(time);
	sql.ensureBucket.run(start, zero, 0, 0);
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
function add(row, share, pending = row.pending) {
	sql.updateBucket.run(
		addCountOutputShare(owned(row.share), share),
		row.count + 1,
		pending,
		row.start,
	);
}
const uploadError = (id, code) => Uint8Array.of(...id, code);

let privateKey = sql.meta.get("private")?.value;
let publicKey = sql.meta.get("public")?.value;
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
const key = await prepareAggregatorKey({
	configId,
	privateKey: owned(privateKey),
});
const hpkeConfigs = encodeHpkeConfigList([
	{ id: configId, kemId: 32, kdfId: 1, aeadId: 1, publicKey: owned(publicKey) },
]);

class HttpError extends Error {
	constructor(status, message) {
		super(message);
		this.status = status;
	}
}
function authenticated(request) {
	if (request.headers.authorization !== `Bearer ${token}`)
		throw new HttpError(401, "Unauthorized");
}
function media(request, name) {
	if (request.headers["content-type"] !== `application/ppm-dap;message=${name}`)
		throw new HttpError(415, "Wrong media type");
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
	const result = await helperCountBatchInit(task, body, key, 0, verifyKey);
	const outbound = transaction(() => {
		const prior = sql.job.get(jobId);
		if (prior) return owned(prior.response);
		const responses = [];
		for (const report of result.reports) {
			let value = report.response;
			if (report.outputShare) {
				const row = bucket(report.time);
				if (sql.report.get(report.reportId))
					value = encodeCountJobRejection(report.reportId, 2);
				else if (row.collected)
					value = encodeCountJobRejection(report.reportId, 1);
				else {
					sql.claim.run(report.reportId);
					add(row, report.outputShare);
				}
			}
			responses.push(value);
		}
		const value = Buffer.concat(responses);
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
			? await leaderCountBatchInit(task, candidates, key, 0, verifyKey)
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
				sql.updateBucket.run(row.share, row.count, row.pending + 1, row.start);
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
			state: owned(saved.state.subarray(offset / 2, offset / 2 + 8)),
		});
	}
	const results = leaderCountBatchFinish(states, inbound);
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
				add(row, result.outputShare, row.pending - 1);
			} else {
				errors.set(result.reportId.toHex(), result.reportError);
				sql.updateBucket.run(row.share, row.count, row.pending - 1, row.start);
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

function collect(request, response, url) {
	authenticated(request);
	const start = Number(url.searchParams.get("start"));
	if (!Number.isSafeInteger(start) || start < 0)
		throw new HttpError(400, "Invalid bucket start");
	const result = transaction(() => {
		const row = bucket(BigInt(start));
		if (row.pending || row.count < task.minBatchSize)
			throw new HttpError(409, "Bucket unavailable");
		sql.collect.run(start);
		return { reportCount: row.count, share: owned(row.share).toHex() };
	});
	send(response, 200, JSON.stringify(result), "application/json");
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
			if (!job) throw new HttpError(404, "Job not found");
			send(
				response,
				200,
				job.response,
				"application/ppm-dap;message=aggregation-job-resp",
			);
		} else if (
			request.method === "POST" &&
			url.pathname === "/internal/collect"
		)
			collect(request, response, url);
		else throw new HttpError(404, "Not found");
	} catch (error) {
		console.error(error);
		if (!response.headersSent)
			send(response, error.status ?? 500, error.message, "text/plain");
		else response.end();
	}
});
server.listen(port, process.env.HOST ?? "127.0.0.1", () =>
	console.log(`${role} listening on ${port}`),
);
