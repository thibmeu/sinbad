import { prio3Count, Task } from "dap-ts";
import { encodeHpkeConfigList } from "dap-ts/messages";
import { expect, it, vi } from "vitest";
import { createCounter, Sinbad } from "../src/index.js";

it("fetches HPKE configs once and uploads fresh count reports", async () => {
	const task = Task.create({
		id: "8BY0RzZMzxvA46_8ymhzycOB9krN-QIGYvg_RsByGec",
		leader: "https://leader.example/",
		helper: "https://helper.example/",
		timePrecision: 60,
		minBatchSize: 100,
		batchMode: "time-interval",
		vdaf: prio3Count(),
	});
	const list = encodeHpkeConfigList([
		{
			id: 1,
			kemId: 32,
			kdfId: 1,
			aeadId: 1,
			publicKey: Uint8Array.fromHex(
				"37fda3567bdbd628e88668c3c8d7e97fa41e9b4fc1409b43f8f051270229af08",
			),
		},
	]);
	const requests: Request[] = [];
	const fetch = vi.fn(async (request: Request) => {
		requests.push(request);
		if (request.method === "GET") {
			return new Response(list, {
				headers: {
					"content-type": "application/ppm-dap;message=hpke-config-list",
				},
			});
		}
		return new Response(null, { status: 200 });
	});
	const count = await createCounter(task, { fetch });
	expect((await count()).accepted).toHaveLength(1);
	expect((await count()).accepted).toHaveLength(1);
	expect(requests.map((request) => request.method)).toEqual([
		"GET",
		"GET",
		"POST",
		"POST",
	]);
	expect(new Uint8Array(await requests[2]!.arrayBuffer())).not.toEqual(
		new Uint8Array(await requests[3]!.arrayBuffer()),
	);
});

it("initializes a site manifest and sends count, page, and bounded sum reports", async () => {
	const countId = "8BY0RzZMzxvA46_8ymhzycOB9krN-QIGYvg_RsByGec";
	const pageId = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
	const sumId = btoa(
		String.fromCharCode(...new Uint8Array(32).fill(3)),
	).replace(/=+$/, "");
	const base = {
		leader: "https://leader.example/",
		helper: "https://helper.example/",
		timePrecision: 60,
		minBatchSize: 100,
		batchMode: "time-interval",
	};
	const manifest = {
		events: {
			signup: { ...base, id: countId, vdaf: "count" },
			purchase: { ...base, id: sumId, vdaf: "sum", maxMeasurement: 100 },
		},
		pages: { "/pricing": { ...base, id: pageId, vdaf: "count" } },
	};
	const list = encodeHpkeConfigList([
		{
			id: 1,
			kemId: 32,
			kdfId: 1,
			aeadId: 1,
			publicKey: Uint8Array.fromHex(
				"37fda3567bdbd628e88668c3c8d7e97fa41e9b4fc1409b43f8f051270229af08",
			),
		},
	]);
	const requests: Request[] = [];
	const fetch = vi.fn(async (request: Request) => {
		requests.push(request);
		if (request.url.endsWith("/manifest"))
			return new Response(JSON.stringify(manifest), {
				headers: { "content-type": "application/json" },
			});
		if (request.method === "GET")
			return new Response(list, {
				headers: {
					"content-type": "application/ppm-dap;message=hpke-config-list",
				},
			});
		return new Response(null, { status: 200 });
	});
	await expect(
		Sinbad.init({
			siteId: "../other",
			endpoint: "https://analytics.example/",
			fetch,
		}),
	).rejects.toThrow("Invalid site ID");
	expect(requests).toHaveLength(0);
	await Sinbad.init({
		siteId: "my-site",
		endpoint: "https://analytics.example/",
		fetch,
	});
	expect(requests[0]!.url).toBe(
		"https://analytics.example/sites/my-site/manifest",
	);
	expect((await Sinbad.track("signup")).accepted).toHaveLength(1);
	expect((await Sinbad.page("/pricing")).accepted).toHaveLength(1);
	vi.stubGlobal("location", { pathname: "/pricing" });
	try {
		expect((await Sinbad.page()).accepted).toHaveLength(1);
	} finally {
		vi.unstubAllGlobals();
	}
	expect((await Sinbad.track("purchase", { value: 49 })).accepted).toHaveLength(
		1,
	);
	const uploads = requests.filter((request) => request.method === "POST");
	expect(uploads.map((request) => request.url)).toEqual([
		expect.stringContaining(countId),
		expect.stringContaining(pageId),
		expect.stringContaining(pageId),
		expect.stringContaining(sumId),
	]);
	await expect(Sinbad.track("signup", { value: 1 })).rejects.toThrow(
		"Count events",
	);
	await expect(Sinbad.track("purchase")).rejects.toThrow(
		"require only a value",
	);
	await expect(Sinbad.track("purchase", { value: 101 })).rejects.toThrow();
	await expect(Sinbad.track("missing")).rejects.toThrow("Unknown event");
	await expect(Sinbad.page("/missing")).rejects.toThrow("Unknown page");
	await expect(
		Sinbad.track("purchase", { value: 49, extra: 1 } as { value: number }),
	).rejects.toThrow("require only a value");
	expect(requests.filter((request) => request.method === "POST")).toHaveLength(
		4,
	);
});
