import { prio3Count, Task } from "dap-ts";
import { decodeUploadRequest, encodeHpkeConfigList } from "dap-ts/messages";
import { expect, it, vi } from "vitest";
import { createCounter, createSiteAnalytics, Sinbad } from "../src/index.js";

const configList = encodeHpkeConfigList([
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
	const count = await createCounter(task, { fetch, batchMs: 0 });
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
	const requests: Request[] = [];
	const fetch = vi.fn(async (request: Request) => {
		requests.push(request);
		if (request.url.endsWith("/manifest"))
			return new Response(JSON.stringify(manifest), {
				headers: { "content-type": "application/json" },
			});
		if (request.method === "GET")
			return new Response(configList, {
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
		batchMs: 0,
	});
	// Manifest size costs no round trips: init fetches only the manifest.
	expect(requests.map((request) => request.url)).toEqual([
		"https://analytics.example/sites/my-site/manifest",
	]);

	expect((await Sinbad.track("signup")).accepted).toHaveLength(1);
	// The first report fetched both HPKE lists; later tasks reuse them.
	expect(requests.filter((request) => request.method === "GET")).toHaveLength(
		3,
	);
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
	expect(requests.filter((request) => request.method === "GET")).toHaveLength(
		3,
	);
	const uploads = requests.filter((request) => request.method === "POST");
	expect(uploads.map((request) => request.url)).toEqual([
		expect.stringContaining(countId),
		expect.stringContaining(pageId),
		expect.stringContaining(pageId),
		expect.stringContaining(sumId),
	]);

	// Unknown names and unusable properties warn and do nothing; an analytics
	// call must not break the page it runs on.
	const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
	try {
		for (const call of [
			Sinbad.track("signup", { value: 1 }),
			Sinbad.track("purchase"),
			Sinbad.track("missing"),
			Sinbad.page("/missing"),
			Sinbad.track("purchase", { value: 49, extra: 1 } as { value: number }),
		])
			expect(await call).toMatchObject({ ok: true, accepted: [] });
		expect(warn).toHaveBeenCalledTimes(5);
	} finally {
		warn.mockRestore();
	}
	// A measurement outside the task bound is a real error and still rejects.
	await expect(Sinbad.track("purchase", { value: 101 })).rejects.toThrow();
	expect(requests.filter((request) => request.method === "POST")).toHaveLength(
		4,
	);
});

it("batches reports for one task into a single upload", async () => {
	const manifest = {
		events: {
			click: {
				id: "8BY0RzZMzxvA46_8ymhzycOB9krN-QIGYvg_RsByGec",
				leader: "https://leader.example/",
				helper: "https://helper.example/",
				timePrecision: 60,
				minBatchSize: 100,
				batchMode: "time-interval",
				vdaf: "count",
			},
		},
	};
	const requests: Request[] = [];
	const fetch = vi.fn(async (request: Request) => {
		requests.push(request);
		if (request.url.endsWith("/manifest"))
			return new Response(JSON.stringify(manifest), {
				headers: { "content-type": "application/json" },
			});
		if (request.method === "GET")
			return new Response(configList, {
				headers: {
					"content-type": "application/ppm-dap;message=hpke-config-list",
				},
			});
		return new Response(null, { status: 200 });
	});
	const site = await createSiteAnalytics({
		siteId: "batch",
		endpoint: "https://analytics.example/",
		fetch,
		batchMs: 50,
	});
	try {
		const results = await Promise.all([
			site.track("click"),
			site.track("click"),
			site.track("click"),
		]);
		for (const result of results) expect(result.ok).toBe(true);
		const uploads = requests.filter((request) => request.method === "POST");
		expect(uploads).toHaveLength(1);
		// Three concatenated reports in one upload-req.
		expect(
			decodeUploadRequest(new Uint8Array(await uploads[0]!.arrayBuffer())),
		).toHaveLength(3);
	} finally {
		site.close();
	}
});

it("refetches HPKE configs once after hpke_unknown_config_id", async () => {
	const manifest = {
		events: {
			click: {
				id: "8BY0RzZMzxvA46_8ymhzycOB9krN-QIGYvg_RsByGec",
				leader: "https://leader.example/",
				helper: "https://helper.example/",
				timePrecision: 60,
				minBatchSize: 100,
				batchMode: "time-interval",
				vdaf: "count",
			},
		},
	};
	let uploads = 0;
	let gets = 0;
	let failures = 1;
	const fetch = vi.fn(async (request: Request) => {
		if (request.url.endsWith("/manifest"))
			return new Response(JSON.stringify(manifest), {
				headers: { "content-type": "application/json" },
			});
		if (request.method === "GET") {
			gets++;
			return new Response(configList, {
				headers: {
					"content-type": "application/ppm-dap;message=hpke-config-list",
				},
			});
		}
		uploads++;
		if (failures <= 0) return new Response(null, { status: 200 });
		failures--;
		// DAP 19, 4.4.2.2: the Leader does not know this config ID.
		const reports = decodeUploadRequest(
			new Uint8Array(await request.arrayBuffer()),
		);
		const errors = new Uint8Array(17 * reports.length);
		reports.forEach((report, index) => {
			errors.set(report.metadata.id, index * 17);
			errors[index * 17 + 16] = 4;
		});
		return new Response(errors, {
			headers: { "content-type": "application/ppm-dap;message=upload-errors" },
		});
	});
	const site = await createSiteAnalytics({
		siteId: "rotate",
		endpoint: "https://analytics.example/",
		fetch,
		batchMs: 0,
	});
	try {
		expect((await site.track("click")).ok).toBe(true);
		expect(uploads).toBe(2);
		// Both lists were discarded and retrieved again for the retry.
		expect(gets).toBe(4);
	} finally {
		site.close();
	}

	// When the retry fails too, the client gives up rather than looping.
	uploads = 0;
	failures = Number.POSITIVE_INFINITY;
	const stubborn = await createSiteAnalytics({
		siteId: "rotate",
		endpoint: "https://analytics.example/",
		fetch,
		batchMs: 0,
	});
	try {
		const result = await stubborn.track("click");
		expect(result.ok).toBe(false);
		expect(result.rejected[0]?.code).toBe("hpke-unknown-config-id");
		expect(uploads).toBe(2);
	} finally {
		stubborn.close();
	}
});
