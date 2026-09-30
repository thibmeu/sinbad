import { DatabaseSync } from "node:sqlite";
import {
	Client,
	HpkeConfigList,
	prio3Count,
	prio3Histogram,
	Task,
	type Vdaf,
} from "@thibmeu/dap";
import { expect, it, vi } from "vitest";
import { createAggregator, type Storage } from "../src/aggregator.ts";

function sqlite(db: DatabaseSync): Storage {
	return {
		query: (sql, ...params) =>
			db.prepare(sql).all(...params) as ReturnType<Storage["query"]>,
		transaction: (fn) => {
			db.exec("BEGIN");
			try {
				const result = fn();
				db.exec("COMMIT");
				return result;
			} catch (error) {
				db.exec("ROLLBACK");
				throw error;
			}
		},
	};
}

function options(url: string, vdaf: Vdaf = prio3Count(), fill = 0) {
	const db = new DatabaseSync(":memory:");
	const storage = sqlite(db);
	const task = Task.create({
		id: new Uint8Array(32).fill(fill).toBase64({
			alphabet: "base64url",
			omitPadding: true,
		}),
		leader: "https://leader.example/",
		helper: "https://helper.example/",
		timePrecision: 60,
		minBatchSize: 100,
		batchMode: "time-interval",
		vdaf,
	});
	const fetch = vi.fn(
		async (_request: Request) => new Response(null, { status: 503 }),
	);
	return {
		db,
		fetch,
		config: {
			role: "leader" as const,
			task,
			storage,
			token: "collector-token",
			helper: { url, token: "leader-token", fetch },
			hpkeKeys: [
				{ configId: 1, privateKey: crypto.getRandomValues(new Uint8Array(32)) },
			],
			verifyKeys: [{ id: 0, key: crypto.getRandomValues(new Uint8Array(32)) }],
		},
	};
}

it.each([
	"http://remote.example/",
	"ftp://remote.example/",
	"https://user:password@helper.example/",
	"https://helper.example/?query=1",
	"https://helper.example/#fragment",
])("rejects unsafe Helper override %s", async (url) => {
	const { db, config } = options(url);
	try {
		await expect(createAggregator(config)).rejects.toThrow(
			"Invalid Helper URL",
		);
	} finally {
		db.close();
	}
});

it.each([
	"https://helper.example/",
	"http://localhost/",
	"http://127.0.0.1/",
	"http://[::1]/",
])("sends authenticated Helper requests safely to %s", async (url) => {
	const { db, fetch, config } = options(url);
	try {
		const leader = await createAggregator(config);
		const response = await leader.fetch(
			new Request("https://leader.example/hpke_config"),
		);
		const { HpkeConfigList } = await import("@thibmeu/dap");
		const hpke = HpkeConfigList.parse(
			new Uint8Array(await response.arrayBuffer()),
		);
		const client = await Client.create(config.task, {
			hpke: { leader: hpke, helper: hpke },
		});
		const upload = client.prepareUpload([await client.prepareReport(1)]);
		await leader.fetch(upload.request);
		await expect(leader.aggregate()).rejects.toThrow();
		expect(fetch).toHaveBeenCalledTimes(1);
		const request = fetch.mock.calls[0]![0];
		expect(new URL(request.url).origin).toBe(new URL(url).origin);
		expect(request.headers.get("authorization")).toBe("Bearer leader-token");
		expect(request.redirect).toBe("manual");
		expect(request.credentials).toBe("omit");
	} finally {
		db.close();
	}
});

it("refuses storage that belongs to another task ID or role", async () => {
	const { db, config } = options("https://helper.example/");
	try {
		await createAggregator(config);
		const other = options("https://helper.example/", prio3Count(), 1).config;
		await expect(
			createAggregator({ ...other, storage: config.storage }),
		).rejects.toThrow("id differs");
		await expect(
			createAggregator({ ...config, role: "helper" }),
		).rejects.toThrow("role differs");
		await createAggregator(config);
	} finally {
		db.close();
	}
});

it("splits a large-histogram backlog into jobs the Helper accepts", async () => {
	const vdaf = prio3Histogram(512, 512);
	const leaderSide = options("https://helper.example/", vdaf);
	const helperSide = options("https://helper.example/", vdaf);
	try {
		const helper = await createAggregator({
			...helperSide.config,
			role: "helper",
			token: "leader-token",
			verifyKeys: leaderSide.config.verifyKeys,
		});
		const leader = await createAggregator({
			...leaderSide.config,
			helper: {
				url: "https://helper.example/",
				token: "leader-token",
				fetch: (request) => helper.fetch(request),
			},
		});
		const list = async (aggregator: typeof leader) =>
			HpkeConfigList.parse(
				new Uint8Array(
					await (
						await aggregator.fetch(
							new Request("https://aggregator.example/hpke_config"),
						)
					).arrayBuffer(),
				),
			);
		const client = await Client.create(leaderSide.config.task, {
			hpke: { leader: await list(leader), helper: await list(helper) },
		});
		const reports = await client.prepareReports(
			Array.from({ length: 100 }, (_, i) => i % 512),
		);
		// The verifier grows with the chunk length: each report adds about 16 KB
		// to a job, so 100 reports exceed the Helper's 1 MiB limit.
		for (let i = 0; i < reports.length; i += 10) {
			const response = await leader.fetch(
				client.prepareUpload(reports.slice(i, i + 10)).request,
			);
			expect(response.status).toBe(200);
		}
		await leader.aggregate();
		for (const { db } of [leaderSide, helperSide])
			expect(db.prepare("SELECT SUM(count) AS n FROM buckets").get()?.n).toBe(
				100,
			);
	} finally {
		leaderSide.db.close();
		helperSide.db.close();
	}
}, 120_000);
