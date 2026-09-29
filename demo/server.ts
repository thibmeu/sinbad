import { readFile } from "node:fs/promises";
import { HttpError, port as parsePort, serve } from "../server/http.ts";

// Serves the demo page and its manifest, and forwards DAP requests to the
// example aggregators, which the task names by placeholder HTTPS hosts.

const env = process.env;
const upstreams: Record<string, URL> = {
	leader: new URL(env.LEADER_URL ?? "http://127.0.0.1:9011/"),
	helper: new URL(env.HELPER_URL ?? "http://127.0.0.1:9012/"),
};
const port = parsePort(env.PORT, 8080);
const task = await (await fetch(new URL("task", upstreams.leader))).json();
const files: Record<string, [string, string]> = {
	"/": ["index.html", "text/html; charset=utf-8"],
	"/style.css": ["style.css", "text/css; charset=utf-8"],
	"/app.bundle.js": ["app.bundle.js", "text/javascript; charset=utf-8"],
};

serve(
	async (request) => {
		const { pathname, search } = new URL(request.url);
		if (request.method === "GET" && pathname === "/sites/demo/manifest")
			return Response.json(
				{ events: { click: task } },
				{ headers: { "cache-control": "no-store" } },
			);
		const file = files[pathname];
		if (request.method === "GET" && file)
			return new Response(await readFile(new URL(file[0], import.meta.url)), {
				headers: { "content-type": file[1], "cache-control": "no-store" },
			});
		const [, role, rest] = /^\/(leader|helper)\/(.*)$/.exec(pathname) ?? [];
		if (
			!role ||
			!(rest === "hpke_config" || rest === `tasks/${task.id}/reports`)
		)
			throw new HttpError(404, "Not found");
		return fetch(new URL(rest + search, upstreams[role]), {
			method: request.method,
			headers: request.headers,
			...(request.method === "POST"
				? { body: await request.arrayBuffer() }
				: {}),
		});
	},
	port,
	env.HOST ?? "127.0.0.1",
	"demo",
);
