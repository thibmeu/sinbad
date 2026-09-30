import { DatabaseSync } from "node:sqlite";
import { Client, prio3Count, Task } from "@thibmeu/dap";
import { expect, it, vi } from "vitest";
import { createAggregator, type Storage } from "../src/aggregator.ts";

function options(url: string) {
	const db = new DatabaseSync(":memory:");
	const storage: Storage = {
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
	const task = Task.create({
		id: new Uint8Array(32).toBase64({
			alphabet: "base64url",
			omitPadding: true,
		}),
		leader: "https://leader.example/",
		helper: "https://helper.example/",
		timePrecision: 60,
		minBatchSize: 100,
		batchMode: "time-interval",
		vdaf: prio3Count(),
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
