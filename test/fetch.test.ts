import { Client, HpkeConfigList, prio3Count, Task } from "@thibmeu/dap";
import { encodeHpkeConfigList } from "@thibmeu/dap/messages";
import { expect, it, vi } from "vitest";
import { execute, fetchHpkeConfigs, readLimited, send } from "../src/fetch.ts";

const task = Task.create({
	id: "8BY0RzZMzxvA46_8ymhzycOB9krN-QIGYvg_RsByGec",
	leader: "https://l/",
	helper: "https://h/",
	timePrecision: 60,
	minBatchSize: 100,
	batchMode: "time-interval",
	vdaf: prio3Count(),
});
const list = HpkeConfigList.parse(
	encodeHpkeConfigList([
		{
			id: 1,
			kemId: 32,
			kdfId: 1,
			aeadId: 1,
			publicKey: Uint8Array.fromHex(
				"37fda3567bdbd628e88668c3c8d7e97fa41e9b4fc1409b43f8f051270229af08",
			),
		},
	]),
);
const hpke = { leader: list, helper: list };

it("fetches both HPKE lists once when requested", async () => {
	const fetch = vi.fn(async (request: Request) => {
		expect(request.method).toBe("GET");
		expect(request.redirect).toBe("manual");
		expect(request.credentials).toBe("omit");
		const list = request.url.startsWith(task.leader)
			? hpke.leader
			: hpke.helper;
		return new Response(list.encode(), {
			headers: {
				"content-type": "application/ppm-dap;message=hpke-config-list",
			},
		});
	});
	const lists = await fetchHpkeConfigs(task, { fetch });
	expect(fetch.mock.calls.map(([request]) => request.url).sort()).toEqual([
		"https://h/hpke_config",
		"https://l/hpke_config",
	]);
	const client = await Client.create(task, { hpke: lists });
	await client.prepareReport(1);
	expect(fetch).toHaveBeenCalledTimes(2);
});

it("executes uploads once, allowing authentication and replay of the same bytes", async () => {
	const client = await Client.create(task, { hpke });
	const upload = client.prepareUpload(await client.prepareReports([1, 0]));
	const bodies: Uint8Array[] = [];
	const fetch = vi.fn(async (request: Request) => {
		expect(request.headers.get("authorization")).toBe("Bearer test");
		expect(request.headers.get("content-type")).toBe(
			"application/ppm-dap;message=upload-req",
		);
		bodies.push(new Uint8Array(await request.arrayBuffer()));
		return new Response(null, { status: 200 });
	});
	for (let i = 0; i < 2; i++)
		expect(
			(
				await execute(upload, {
					fetch,
					headers: { authorization: "Bearer test" },
				})
			).ok,
		).toBe(true);
	expect(fetch).toHaveBeenCalledTimes(2);
	expect(bodies[0]).toEqual(bodies[1]);
	expect(() =>
		send(upload.request, {
			fetch,
			headers: { "Content-Type": "application/json" },
		}),
	).toThrow();
	const keepalive = vi.fn(async (request: Request) => {
		expect(request.keepalive).toBe(true);
		return new Response(null, { status: 200 });
	});
	await execute(upload, { fetch: keepalive, keepalive: true });
	expect(keepalive).toHaveBeenCalledTimes(1);
});

it("does not retry HTTP or network failures, and passes cancellation through", async () => {
	const client = await Client.create(task, { hpke });
	const upload = client.prepareUpload([await client.prepareReport(1)]);
	const fetch = vi.fn(async () => new Response(null, { status: 503 }));
	await expect(execute(upload, { fetch })).rejects.toMatchObject({
		code: "HttpError",
	});
	expect(fetch).toHaveBeenCalledTimes(1);
	const abort = AbortSignal.abort();
	await expect(execute(upload, { fetch, signal: abort })).rejects.toThrow();
	expect(fetch).toHaveBeenCalledTimes(1);
	const controller = new AbortController();
	let seen: Request | undefined;
	await send(upload.request, {
		signal: controller.signal,
		fetch: async (request) => {
			seen = request;
			return new Response(null);
		},
	});
	controller.abort();
	expect(seen?.signal.aborted).toBe(true);
	const failed = vi.fn(async () => {
		throw new TypeError("network failure");
	});
	await expect(execute(upload, { fetch: failed })).rejects.toThrow(
		"network failure",
	);
	expect(failed).toHaveBeenCalledTimes(1);
});

it("reads highly fragmented responses without spreading chunks into arguments", async () => {
	let remaining = 150_000;
	const response = new Response(
		new ReadableStream({
			pull(controller) {
				if (remaining-- > 0) controller.enqueue(Uint8Array.of(42));
				else controller.close();
			},
		}),
	);
	expect(await readLimited(response, 150_000)).toEqual(
		new Uint8Array(150_000).fill(42),
	);
});

it("bounds streamed responses and cancels an oversized body", async () => {
	let cancelled = false;
	const body = new ReadableStream<Uint8Array>({
		pull(controller) {
			controller.enqueue(new Uint8Array(3));
		},
		cancel() {
			cancelled = true;
		},
	});
	await expect(readLimited(new Response(body), 4)).rejects.toMatchObject({
		code: "InvalidResponse",
	});
	expect(cancelled).toBe(true);
	expect(await readLimited(new Response(new Uint8Array([1, 2, 3])), 3)).toEqual(
		new Uint8Array([1, 2, 3]),
	);
});

it("rejects invalid HPKE responses before constructing a client", async () => {
	for (const response of [
		new Response(null, { status: 404 }),
		new Response(hpke.leader.encode(), {
			headers: { "content-type": "application/json" },
		}),
		new Response(new Uint8Array(2), {
			headers: {
				"content-type": "application/ppm-dap;message=hpke-config-list",
			},
		}),
	]) {
		await expect(
			fetchHpkeConfigs(task, { fetch: async () => response.clone() }),
		).rejects.toThrow();
	}
});
