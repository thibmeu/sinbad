import { prio3Count, Task } from "dap-ts";
import { createCounter } from "sinbad";

const status = document.querySelector("#status");
const button = document.querySelector("#count");

try {
  const response = await fetch("/config.json");
  if (!response.ok) throw new Error(`Configuration: HTTP ${response.status}`);
  const config = await response.json();
  const task = Task.create({ ...config, vdaf: prio3Count() });
  const proxyFetch = (input) => {
    const request = new Request(input);
    const url = new URL(request.url);
    const role = url.hostname;
    if (role !== "leader" && role !== "helper") throw new Error("Unknown DAP endpoint");
    const proxy = new URL(`/${role}${url.pathname}${url.search}`, location.origin);
    return fetch(new Request(proxy, request));
  };
  const count = await createCounter(task, { fetch: proxyFetch });
  button.disabled = false;
  status.textContent = "Ready";
  button.addEventListener("click", async () => {
    button.disabled = true;
    status.textContent = "Sending…";
    try {
      const result = await count();
      status.textContent = result.ok ? "Count report accepted" : "Count report rejected";
    } catch (error) {
      status.textContent = error instanceof Error ? error.message : "Upload failed";
    } finally {
      button.disabled = false;
    }
  });
} catch (error) {
  status.textContent = error instanceof Error ? error.message : "Setup failed";
}
