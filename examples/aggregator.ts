import { generateKeyPairSync } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { prio3Count, prio3Histogram, prio3Sum, Task } from "@thibmeu/dap";
import {
	createAggregator,
	type SqlValue,
	type Storage,
} from "../src/aggregator.ts";
import { port as parsePort, serve } from "./http.ts";

// Runs the Sinbad aggregator core for one task on Node, with node:sqlite.

const env = process.env;
const role = env.ROLE;
if (role !== "leader" && role !== "helper")
	throw new Error("ROLE must be leader or helper");
const token = env.AUTH_TOKEN;
const verifyKey = Uint8Array.fromHex(env.VERIFY_KEY_HEX ?? "");
if (!token || verifyKey.length !== 32)
	throw new Error("Set AUTH_TOKEN and a 32-byte VERIFY_KEY_HEX");
const kind = env.VDAF ?? "count";
const vdaf =
	kind === "sum"
		? prio3Sum(1337)
		: kind === "histogram"
			? prio3Histogram(
					Number(env.HISTOGRAM_LENGTH ?? 4),
					Number(env.HISTOGRAM_CHUNK_LENGTH ?? 2),
				)
			: kind === "count"
				? prio3Count()
				: undefined;
if (!vdaf) throw new Error("VDAF must be count, sum, or histogram");
const task = Task.create({
	id: "8BY0RzZMzxvA46_8ymhzycOB9krN-QIGYvg_RsByGec",
	info: `sinbad-${kind}-v1`,
	leader: "https://leader.example/",
	helper: "https://helper.example/",
	timePrecision: Number(env.TIME_PRECISION ?? 60),
	// A real deployment must not publish an aggregate over a handful of
	// reports. Raise MIN_BATCH_SIZE before exposing this to anyone.
	minBatchSize: Number(env.MIN_BATCH_SIZE ?? 1),
	batchMode: "time-interval",
	vdaf,
});

const db = new DatabaseSync(env.DATA_FILE ?? `${role}.sqlite`);
db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
const storage: Storage = {
	query: (sql: string, ...params: SqlValue[]) =>
		db.prepare(sql).all(...params) as Record<string, SqlValue>[],
	transaction(fn) {
		db.exec("BEGIN IMMEDIATE");
		try {
			const value = fn();
			db.exec("COMMIT");
			return value;
		} catch (error) {
			db.exec("ROLLBACK");
			throw error;
		}
	},
};
// The HPKE key lives as long as the database.
db.exec(
	"CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value BLOB NOT NULL)",
);
let privateKey = storage.query("SELECT value FROM meta WHERE name='private'")[0]
	?.value as Uint8Array | undefined;
if (!privateKey) {
	const pair = generateKeyPairSync("x25519");
	privateKey = Buffer.from(
		pair.privateKey.export({ format: "jwk" }).d!,
		"base64url",
	);
	storage.query(
		"INSERT INTO meta(name,value) VALUES ('private',?)",
		privateKey,
	);
}

// One aggregation pass at a time; an upload during a pass schedules another.
let again = false;
let running = false;
async function wake(): Promise<void> {
	if (running) {
		again = true;
		return;
	}
	running = true;
	do {
		again = false;
		try {
			await aggregator.aggregate();
		} catch (error) {
			console.error("Aggregation pass failed", error);
		}
	} while (again);
	running = false;
}

const aggregator = await createAggregator({
	role,
	task,
	storage,
	token,
	hpkeKeys: [
		{
			configId: role === "leader" ? 7 : 8,
			privateKey: new Uint8Array(privateKey),
		},
	],
	verifyKeys: [{ id: 0, key: verifyKey }],
	...(env.COLLECTOR_PUBLIC_KEY_HEX
		? {
				collector: {
					id: Number(env.COLLECTOR_CONFIG_ID ?? 23),
					kemId: 32,
					kdfId: 1,
					aeadId: 1,
					publicKey: Uint8Array.fromHex(env.COLLECTOR_PUBLIC_KEY_HEX),
				},
			}
		: {}),
	...(role === "leader" && env.HELPER_URL
		? { helper: { url: env.HELPER_URL, token } }
		: {}),
	...(env.MAX_JOB_SIZE ? { maxJobSize: Number(env.MAX_JOB_SIZE) } : {}),
	schedule: () => void wake(),
});
if (role === "leader") {
	setInterval(() => void wake(), Number(env.AGGREGATE_MS ?? 1000)).unref();
	void wake();
}

const delay = Number(env.RESPONSE_DELAY_MS);
serve(
	async (request) => {
		const response = await aggregator.fetch(request);
		// Test hook: hold the Helper's answer after it has committed.
		if (
			delay > 0 &&
			new URL(request.url).pathname.endsWith("/aggregation_jobs")
		)
			await new Promise((resolve) => setTimeout(resolve, delay));
		return response;
	},
	parsePort(env.PORT, 8080),
	env.HOST ?? "127.0.0.1",
	role,
);
