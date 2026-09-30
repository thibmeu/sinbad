import { Sinbad } from "sinbad";

const status = document.querySelector<HTMLElement>("#status")!;
const button = document.querySelector<HTMLButtonElement>("#track")!;

// The page talks to the DAP roles through its own origin. Copy the body:
// passing the Request as the init of a new one would stream it, which
// browsers refuse over HTTP/1.1.
const proxyFetch = async (request: Request) => {
	const url = new URL(request.url);
	if (url.origin === location.origin) return fetch(request);
	const role = url.hostname.split(".")[0];
	if (role !== "leader" && role !== "helper")
		throw new Error("Unknown DAP endpoint");
	return fetch(`/${role}${url.pathname}${url.search}`, {
		method: request.method,
		headers: request.headers,
		keepalive: request.keepalive,
		...(request.method === "POST" ? { body: await request.arrayBuffer() } : {}),
	});
};

try {
	// batchMs 0 keeps the example button responsive; a real site would keep the default.
	await Sinbad.init({
		siteId: "example",
		endpoint: location.origin,
		fetch: proxyFetch,
		batchMs: 0,
	});
	button.disabled = false;
	status.textContent = "Ready";
	button.addEventListener("click", async () => {
		button.disabled = true;
		status.textContent = "Sending…";
		try {
			const result = await Sinbad.track("click");
			status.textContent = result.ok ? "Event accepted" : "Event rejected";
		} catch (error) {
			status.textContent =
				error instanceof Error ? error.message : "Upload failed";
		} finally {
			button.disabled = false;
		}
	});
} catch (error) {
	status.textContent = error instanceof Error ? error.message : "Setup failed";
}
