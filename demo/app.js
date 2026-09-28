import { Sinbad } from "sinbad";

const status = document.querySelector("#status");
const button = document.querySelector("#track");

try {
  const proxyFetch = (input) => {
    const request = new Request(input);
    const url = new URL(request.url);
    if (url.origin === location.origin) return fetch(request);
    const role = url.hostname;
    if (role !== "leader" && role !== "helper") throw new Error("Unknown DAP endpoint");
    const proxy = new URL(`/${role}${url.pathname}${url.search}`, location.origin);
    return fetch(new Request(proxy, request));
  };
  await Sinbad.init({ siteId: "demo", endpoint: location.origin, fetch: proxyFetch });
  button.disabled = false;
  status.textContent = "Ready";
  button.addEventListener("click", async () => {
    button.disabled = true;
    status.textContent = "Sending…";
    try {
      const result = await Sinbad.track("click");
      status.textContent = result.ok ? "Event accepted" : "Event rejected";
    } catch (error) {
      status.textContent = error instanceof Error ? error.message : "Upload failed";
    } finally {
      button.disabled = false;
    }
  });
} catch (error) {
  status.textContent = error instanceof Error ? error.message : "Setup failed";
}
