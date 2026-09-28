import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { prio3Count, Task } from "dap-ts";
import { fetchHpkeConfigs } from "sinbad/fetch";

const origin = process.env.PUBLIC_ORIGIN ?? "http://localhost:8080";
const leaderUrl = process.env.LEADER_URL ?? "http://127.0.0.1:9001/";
const helperUrl = process.env.HELPER_URL ?? "http://127.0.0.1:9002/";
const port = Number(process.env.PORT ?? 8080);
if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error("Invalid PORT");
const publicUrl = new URL(origin);
if (!(["http:", "https:"].includes(publicUrl.protocol) && !publicUrl.username && !publicUrl.password && publicUrl.pathname === "/" && !publicUrl.search && !publicUrl.hash)) {
  throw new Error("PUBLIC_ORIGIN must be an HTTP(S) origin");
}

// DAP authenticates these endpoint bytes; the browser only changes the HTTP route.
const endpoints = {
  leader: "http://leader:8080/",
  helper: "http://helper:8080/",
};
const upstreams = { leader: new URL(leaderUrl), helper: new URL(helperUrl) };

function upstream(url) {
  const parsed = new URL(url);
  for (const role of ["leader", "helper"]) {
    const prefix = `/${role}/`;
    if (parsed.origin === publicUrl.origin && parsed.pathname.startsWith(prefix)) {
      return new URL(parsed.pathname.slice(prefix.length) + parsed.search, upstreams[role]);
    }
    if (parsed.origin === new URL(endpoints[role]).origin) {
      return new URL(parsed.pathname.slice(1) + parsed.search, upstreams[role]);
    }
  }
  throw new Error("Unknown DAP endpoint");
}

async function internalFetch(input) {
  const request = new Request(input);
  return fetch(new Request(upstream(request.url), request));
}

async function post(url, body) {
  const response = await fetch(new URL("internal/test/add_task", url), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(`Janus provisioning returned HTTP ${response.status}`);
  const result = await response.json();
  if (result.status !== "success") throw new Error(`Janus provisioning failed: ${result.error ?? "unknown error"}`);
}

async function provision() {
  const config = {
    id: randomBytes(32).toString("base64url"),
    // The pinned Janus interop add_task endpoint provisions this task info.
    info: "task-info",
    ...endpoints,
    timePrecision: 60,
    minBatchSize: 1,
    batchMode: "time-interval",
    testOnly: { dapVersion: 18, allowInsecureHttp: true },
  };
  const task = Task.create({ ...config, vdaf: prio3Count() });
  let hpke;
  for (let attempt = 0; attempt < 60; attempt++) {
    try {
      hpke = await fetchHpkeConfigs(task, { fetch: internalFetch });
      break;
    } catch (error) {
      if (attempt === 59) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
  const common = {
    task_id: task.id,
    leader: endpoints.leader,
    helper: endpoints.helper,
    vdaf: { type: "Prio3Count" },
    leader_authentication_token: "leader-token",
    vdaf_verify_key: randomBytes(32).toString("base64url"),
    batch_mode: 1,
    min_batch_size: 1,
    time_precision: 60,
    collector_hpke_config: Buffer.from(hpke.leader.encode().slice(2)).toString("base64url"),
    task_start: null,
    task_end: null,
  };
  await post(leaderUrl, { ...common, role: "leader", collector_authentication_token: "collector-token" });
  await post(helperUrl, { ...common, role: "helper", collector_authentication_token: null });
  return config;
}

const config = await provision();
const files = {
  "/": ["index.html", "text/html; charset=utf-8"],
  "/style.css": ["style.css", "text/css; charset=utf-8"],
  "/app.bundle.js": ["app.bundle.js", "text/javascript; charset=utf-8"],
};
const server = createServer(async (request, response) => {
  try {
    const path = new URL(request.url, publicUrl).pathname;
    if (request.method === "GET" && path === "/sites/demo/manifest") {
      response.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
      response.end(JSON.stringify({ events: { click: { ...config, vdaf: "count" } } }));
      return;
    }
    if (request.method === "GET" && files[path]) {
      const [name, type] = files[path];
      const body = await readFile(new URL(name, import.meta.url));
      response.writeHead(200, { "content-type": type, "cache-control": "no-store" });
      response.end(body);
      return;
    }
    const hpke = request.method === "GET" && /^\/(leader|helper)\/hpke_config$/.test(path);
    const upload = request.method === "POST" && path === `/leader/tasks/${config.id}/reports`;
    if (hpke || upload) {
      const chunks = [];
      let size = 0;
      for await (const chunk of request) {
        size += chunk.length;
        if (size > 1024 * 1024) throw new Error("DAP request too large");
        chunks.push(chunk);
      }
      const headers = new Headers();
      if (request.headers["content-type"]) headers.set("content-type", request.headers["content-type"]);
      if (request.headers.accept) headers.set("accept", request.headers.accept);
      const target = upstream(new URL(request.url, publicUrl));
      const result = await fetch(target, {
        method: request.method,
        headers,
        body: request.method === "POST" ? Buffer.concat(chunks) : undefined,
        redirect: "manual",
      });
      const body = Buffer.from(await result.arrayBuffer());
      if (body.length > 1024 * 1024) throw new Error("DAP response too large");
      response.writeHead(result.status, { "content-type": result.headers.get("content-type") ?? "application/octet-stream" });
      response.end(body);
      return;
    }
    response.writeHead(404);
    response.end("Not found");
  } catch (error) {
    console.error(error);
    response.writeHead(502);
    response.end("Demo request failed");
  }
});
server.listen(port, process.env.HOST ?? "127.0.0.1", () => console.log(`Demo: ${origin}`));
