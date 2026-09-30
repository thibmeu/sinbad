import {
	Client,
	prio3Count,
	prio3Histogram,
	prio3Sum,
	Task,
	type Vdaf,
} from "@thibmeu/dap";
import {
	decodeUploadRequest,
	encodeHpkeConfigList,
} from "@thibmeu/dap/messages";
import { expect, it, vi } from "vitest";
import { createSiteAnalytics, Sinbad } from "../src/index.ts";

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
const hpkeResponse = () =>
	new Response(configList, {
		headers: { "content-type": "application/ppm-dap;message=hpke-config-list" },
	});

/** A manifest entry: task ID and TaskConfiguration in URL-safe Base 64. */
function entry(fill: number, vdaf: Vdaf) {
	const task = Task.create({
		id: new Uint8Array(32).fill(fill).toBase64({
			alphabet: "base64url",
			omitPadding: true,
		}),
		leader: "https://leader.example/",
		helper: "https://helper.example/",
		timePrecision: 3600,
		minBatchSize: 100,
		batchMode: "time-interval",
		vdaf,
	});
	return {
		id: task.id,
		configuration: task.encodeConfiguration().toBase64({
			alphabet: "base64url",
			omitPadding: true,
		}),
	};
}

/** A fake site: serves the manifest and HPKE lists, and lets `upload` answer reports. */
function site(
	manifest: unknown,
	upload: (request: Request) => Promise<Response> = async () =>
		new Response(null),
) {
	const requests: Request[] = [];
	const fetch = vi.fn(async (request: Request) => {
		requests.push(request.clone());
		if (request.url.endsWith("/manifest")) return Response.json(manifest);
		if (request.method === "GET") return hpkeResponse();
		return upload(request);
	});
	return {
		fetch,
		requests,
		posts: () => requests.filter((r) => r.method === "POST"),
	};
}

it("queues calls made before init and sends them once it resolves", async () => {
	const { fetch, posts } = site({ events: { early: entry(9, prio3Count()) } });
	const early = Sinbad.track("early");
	await Promise.resolve();
	expect(fetch).not.toHaveBeenCalled();
	await Sinbad.init({
		siteId: "early",
		endpoint: "https://analytics.example/",
		fetch,
		batchMs: 0,
	});
	expect(await early).toMatchObject({ ok: true, sent: true });
	expect(posts()).toHaveLength(1);
});

it("initializes a site manifest and sends count, page, sum, and histogram reports", async () => {
	const count = entry(1, prio3Count());
	const page = entry(2, prio3Count());
	const sum = entry(3, prio3Sum(100));
	const histogram = entry(4, prio3Histogram(4, 2));
	const manifest = {
		events: { signup: count, purchase: sum, plan: histogram },
		pages: { "/pricing": page },
	};
	const { fetch, requests, posts } = site(manifest);
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
	expect(await Sinbad.track("signup")).toMatchObject({ ok: true, sent: true });
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
	expect((await Sinbad.track("purchase", { value: 49 })).ok).toBe(true);
	expect((await Sinbad.track("plan", { value: 3 })).ok).toBe(true);
	expect(requests.filter((request) => request.method === "GET")).toHaveLength(
		3,
	);
	expect(posts().map((request) => request.url)).toEqual(
		[count, page, page, sum, histogram].map((task) =>
			expect.stringContaining(task.id),
		),
	);

	// Unknown names and unusable properties warn and send nothing; an
	// analytics call must not break the page it runs on.
	const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
	try {
		for (const call of [
			Sinbad.track("signup", { value: 1 }),
			Sinbad.track("purchase"),
			Sinbad.track("missing"),
			Sinbad.page("/missing"),
			Sinbad.track("purchase", { value: 49, extra: 1 } as { value: number }),
		])
			expect(await call).toEqual({
				ok: false,
				sent: false,
				accepted: [],
				rejected: [],
			});
		expect(warn).toHaveBeenCalledTimes(5);
	} finally {
		warn.mockRestore();
	}
	// A measurement outside the task bound is a real error and still rejects.
	await expect(Sinbad.track("purchase", { value: 101 })).rejects.toThrow();
	await expect(Sinbad.track("plan", { value: 4 })).rejects.toThrow();
	expect(posts()).toHaveLength(5);
});

it("batches reports for one task into a single upload", async () => {
	const { fetch, posts } = site({ events: { click: entry(1, prio3Count()) } });
	const analytics = await createSiteAnalytics({
		siteId: "batch",
		endpoint: "https://analytics.example/",
		fetch,
		batchMs: 50,
	});
	try {
		const results = await Promise.all([
			analytics.track("click"),
			analytics.track("click"),
			analytics.track("click"),
		]);
		for (const result of results) expect(result.ok).toBe(true);
		expect(posts()).toHaveLength(1);
		expect(
			decodeUploadRequest(new Uint8Array(await posts()[0]!.arrayBuffer())),
		).toHaveLength(3);
	} finally {
		analytics.close();
	}
});

it("sends what is queued with keepalive when the page is hidden", async () => {
	const prepare = vi.spyOn(Client.prototype, "prepareReport");
	const listeners = new Map<string, () => void>();
	vi.stubGlobal("addEventListener", (type: string, listener: () => void) =>
		listeners.set(type, listener),
	);
	vi.stubGlobal("removeEventListener", (type: string) =>
		listeners.delete(type),
	);
	const { fetch, posts } = site({ events: { click: entry(1, prio3Count()) } });
	try {
		const analytics = await createSiteAnalytics({
			siteId: "unload",
			endpoint: "https://analytics.example/",
			fetch,
			batchMs: 60_000,
		});
		const pending = analytics.track("click");
		await vi.waitFor(() => expect(prepare).toHaveBeenCalledTimes(1));
		await prepare.mock.results[0]!.value;
		await Promise.resolve();
		listeners.get("pagehide")!();
		// Dispatch starts inside the unload listener, before any further await.
		expect(posts()).toHaveLength(1);
		expect((await pending).ok).toBe(true);
		expect(posts()[0]!.keepalive).toBe(true);
		analytics.close();
		expect(listeners.size).toBe(0);
	} finally {
		prepare.mockRestore();
		vi.unstubAllGlobals();
	}
});

/** Answer hpke_unknown_config_id for the reports `reject` picks. */
const staleConfig =
	(reject: (index: number) => boolean) => async (request: Request) => {
		const reports = decodeUploadRequest(
			new Uint8Array(await request.arrayBuffer()),
		);
		const errors = reports.flatMap((report, index) =>
			reject(index) ? [...report.metadata.id, 4] : [],
		);
		return new Response(errors.length ? Uint8Array.from(errors) : null, {
			headers: { "content-type": "application/ppm-dap;message=upload-errors" },
		});
	};

it("refetches HPKE configs once after hpke_unknown_config_id and retries only those reports", async () => {
	vi.useFakeTimers({ toFake: ["Date"] });
	let failures = 1;
	const { fetch, requests, posts } = site(
		{ events: { click: entry(1, prio3Count()) } },
		async (request) => {
			if (failures-- <= 0) return new Response(null);
			// The retry happens two batch buckets later.
			vi.setSystemTime(Date.now() + 2 * 3600_000);
			return staleConfig((index) => index === 1)(request);
		},
	);
	const analytics = await createSiteAnalytics({
		siteId: "rotate",
		endpoint: "https://analytics.example/",
		fetch,
		batchMs: 20,
	});
	try {
		const results = await Promise.all([
			analytics.track("click"),
			analytics.track("click"),
		]);
		expect(results.map((result) => result.ok)).toEqual([true, true]);
		expect(posts()).toHaveLength(2);
		const [first, retry] = await Promise.all(
			posts().map(async (post) =>
				decodeUploadRequest(new Uint8Array(await post.arrayBuffer())),
			),
		);
		// The retry carries only the rejected report, so none is counted twice,
		// and keeps its original time, so it stays in its batch bucket.
		expect(retry).toHaveLength(1);
		expect(retry![0]!.metadata.time).toBe(first![1]!.metadata.time);
		// Both lists were discarded and retrieved again for the retry.
		expect(requests.filter((r) => r.url.endsWith("hpke_config"))).toHaveLength(
			4,
		);
		expect(
			requests
				.filter((r) => r.url.endsWith("hpke_config"))
				.map((request) => request.cache),
		).toEqual(["default", "default", "reload", "reload"]);
	} finally {
		analytics.close();
		vi.useRealTimers();
	}

	// When the retry fails too, the client gives up rather than looping.
	const stubborn = site(
		{ events: { click: entry(1, prio3Count()) } },
		staleConfig(() => true),
	);
	const again = await createSiteAnalytics({
		siteId: "rotate",
		endpoint: "https://analytics.example/",
		fetch: stubborn.fetch,
		batchMs: 0,
	});
	try {
		const result = await again.track("click");
		expect(result.ok).toBe(false);
		expect(result.rejected[0]?.error).toBe("hpke-unknown-config-id");
		expect(stubborn.posts()).toHaveLength(2);
	} finally {
		again.close();
	}
});

it("tracks again once a failed HPKE retrieval recovers", async () => {
	let down = true;
	const { fetch, posts } = site({ events: { click: entry(1, prio3Count()) } });
	const analytics = await createSiteAnalytics({
		siteId: "offline",
		endpoint: "https://analytics.example/",
		fetch: async (request) => {
			if (down && request.url.endsWith("hpke_config"))
				throw new TypeError("offline");
			return fetch(request);
		},
		batchMs: 0,
	});
	try {
		await expect(analytics.track("click")).rejects.toThrow("offline");
		down = false;
		expect((await analytics.track("click")).ok).toBe(true);
		expect(posts()).toHaveLength(1);
	} finally {
		analytics.close();
	}
});

it("uploads the valid reports of a batch holding an invalid measurement", async () => {
	const { fetch, posts } = site({
		events: { purchase: entry(2, prio3Sum(100)) },
	});
	const analytics = await createSiteAnalytics({
		siteId: "mixed",
		endpoint: "https://analytics.example/",
		fetch,
		batchMs: 50,
	});
	try {
		const results = await Promise.allSettled(
			[10, 101, 20].map((value) => analytics.track("purchase", { value })),
		);
		expect(results.map((result) => result.status)).toEqual([
			"fulfilled",
			"rejected",
			"fulfilled",
		]);
		expect(posts()).toHaveLength(1);
		expect(
			decodeUploadRequest(new Uint8Array(await posts()[0]!.arrayBuffer())),
		).toHaveLength(2);
	} finally {
		analytics.close();
	}
});

it("rejects manifests that do not decode into tasks", async () => {
	for (const manifest of [
		{},
		{ events: { click: { id: "x", configuration: "AA" } } },
		{ events: { click: { ...entry(1, prio3Count()), configuration: 3 } } },
		{ pages: { "/": entry(1, prio3Sum(5)) } },
	])
		await expect(
			createSiteAnalytics({
				siteId: "bad",
				endpoint: "https://analytics.example/",
				fetch: site(manifest).fetch,
			}),
		).rejects.toThrow();
});

it("rejects invalid batching options before fetching", async () => {
	const fetch = vi.fn();
	for (const maxBatch of [0, -1, 0.5, NaN, Infinity]) {
		await expect(
			createSiteAnalytics({
				siteId: "test",
				endpoint: "https://analytics.example/",
				fetch,
				maxBatch,
			}),
		).rejects.toThrow("Invalid maxBatch");
	}
	for (const batchMs of [-1, 0.5, NaN, Infinity, 2147483648]) {
		await expect(
			createSiteAnalytics({
				siteId: "test",
				endpoint: "https://analytics.example/",
				fetch,
				batchMs,
			}),
		).rejects.toThrow("Invalid batchMs");
	}
	expect(fetch).not.toHaveBeenCalled();
});

it("keeps manifest credentials out of HPKE retrieval and uploads", async () => {
	const { fetch, requests } = site({
		events: { test: entry(11, prio3Count()) },
	});
	const analytics = await createSiteAnalytics({
		siteId: "test",
		endpoint: "https://analytics.example/",
		fetch,
		headers: {
			authorization: "Bearer manifest-secret",
			"x-api-key": "manifest-key",
		},
		batchMs: 0,
	});
	try {
		expect(await analytics.track("test")).toMatchObject({
			sent: true,
			ok: true,
		});
		expect(requests[0]!.headers.get("authorization")).toBe(
			"Bearer manifest-secret",
		);
		expect(requests[0]!.headers.get("x-api-key")).toBe("manifest-key");
		expect(requests.slice(1).map((r) => new URL(r.url).origin)).toEqual([
			"https://leader.example",
			"https://helper.example",
			"https://leader.example",
		]);
		for (const request of requests.slice(1)) {
			expect(request.headers.has("authorization")).toBe(false);
			expect(request.headers.has("x-api-key")).toBe(false);
		}
	} finally {
		analytics.close();
	}
});
