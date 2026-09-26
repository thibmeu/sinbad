import type { Collector, PreparedCollection } from "dap-ts/collector";
import { expect, it, vi } from "vitest";
import { collect, executeCollection } from "../src/collector.js";

const location = "https://leader.example/tasks/task/collection_jobs/job";

function pending(method: "POST" | "GET"): PreparedCollection {
	return {
		request: { method, url: location, headers: {} },
		process: async (response) => {
			if (response.status !== 200) throw new Error("Unexpected status");
			return {
				status: "pending",
				state: { location, start: "10", duration: "2" },
				retryAfter: 0,
			};
		},
	};
}

const collector = {
	prepare: () => pending("POST"),
	resume: () => pending("GET"),
} as unknown as Collector;

it("polls with authentication and returns resumable state at the limit", async () => {
	const fetch = vi.fn(async (request: Request) => {
		expect(request.headers.get("authorization")).toBe("Bearer test");
		return new Response(null, { status: 200 });
	});
	const result = await collect(
		collector,
		{ start: 10, duration: 2 },
		{
			fetch,
			headers: { authorization: "Bearer test" },
			maxPolls: 2,
			minDelayMs: 0,
		},
	);
	expect(result.status).toBe("pending");
	expect(fetch.mock.calls.map(([request]) => request.method)).toEqual([
		"POST",
		"GET",
		"GET",
	]);
	if (result.status !== "pending") return;
	await executeCollection(collector.resume(result.state), {
		fetch,
		headers: { authorization: "Bearer test" },
	});
	expect(fetch).toHaveBeenCalledTimes(4);
});

it("stops polling on cancellation and long Retry-After", async () => {
	const controller = new AbortController();
	const abortingFetch = vi.fn(async () => {
		setTimeout(() => controller.abort(), 0);
		return new Response(null, { status: 200 });
	});
	await expect(
		collect(
			collector,
			{ start: 10, duration: 2 },
			{
				fetch: abortingFetch,
				signal: controller.signal,
			},
		),
	).rejects.toThrow();
	expect(abortingFetch).toHaveBeenCalledTimes(1);
	const slow = {
		...collector,
		prepare: () => ({
			...pending("POST"),
			process: async () => ({
				status: "pending" as const,
				state: { location, start: "10", duration: "2" },
				retryAfter: 300,
			}),
		}),
	} as Collector;
	const fetch = vi.fn(async () => new Response(null, { status: 200 }));
	expect(
		(await collect(slow, { start: 10, duration: 2 }, { fetch })).status,
	).toBe("pending");
	expect(fetch).toHaveBeenCalledTimes(1);
});
