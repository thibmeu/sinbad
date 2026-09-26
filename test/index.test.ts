import { prio3Count, Task } from "dap-ts";
import { encodeHpkeConfigList } from "dap-ts/messages";
import { Effect } from "effect";
import { expect, it, vi } from "vitest";
import { createCounter } from "../src/index.js";

it("fetches both HPKE lists once and uploads one report per count", async () => {
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
	const count = await Effect.runPromise(createCounter(task, { fetch }));
	expect((await Effect.runPromise(count())).accepted).toHaveLength(1);
	expect((await Effect.runPromise(count())).accepted).toHaveLength(1);
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
