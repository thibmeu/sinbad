# ⛵ Sinbad

A small analytics library and Fetch transport for [dap-ts](https://github.com/thibmeu/dap-ts).

```js
import { Sinbad } from "sinbad";

Sinbad.init({ siteId: "my-site", endpoint: "https://analytics.example/" });
Sinbad.page();
Sinbad.track("signup");
Sinbad.track("purchase", { value: 49 });
Sinbad.track("plan", { value: 2 });
```

`init` fetches `GET https://analytics.example/sites/my-site/manifest` and nothing else, however many tasks the manifest names. Calls made before `init` resolves wait for it, so a page can track as soon as it loads.

`page()` counts a view of `location.pathname` in a browser; pass a path in other runtimes. `track(name)` counts an event. `track(name, { value })` adds an integer to a bounded Sum task, in units chosen by the site such as cents, or reports a bucket index to a Histogram task. Every report is encrypted to the two aggregators.

Calls return Promises of a result with `accepted`, `rejected`, `ok`, and `sent`. An unknown event or page name, or properties an event does not take, logs a warning and resolves with `sent: false`: analytics must not break the page it runs on. Calls also resolve that way if `init` fails. A value outside its task's bound is a programming error and rejects. Network failures reject too; Sinbad does not retry them.

## Manifest

The manifest maps event names and page paths to provisioned tasks. Each entry is the task ID and its encoded DAP TaskConfiguration, both in URL-safe Base 64 without padding, as dap-ts's `task.id` and `task.encodeConfiguration()` produce them. The configuration fixes the VDAF, so Sinbad needs nothing else:

```json
{
  "events": {
    "signup": { "id": "<task-id>", "configuration": "<task-configuration>" },
    "purchase": { "id": "<task-id>", "configuration": "<task-configuration>" }
  },
  "pages": {
    "/pricing": { "id": "<task-id>", "configuration": "<task-configuration>" }
  }
}
```

Use a separate task per entry. Pages must use Count tasks. Event names and paths select tasks locally; aggregators receive task IDs and encrypted reports. Other event properties and user identities are not supported.

## Batching

Reports for one task are collected for `batchMs` milliseconds (default 1000) and uploaded together, up to `maxBatch` reports (default 20) per request. Set `batchMs: 0` to send each call immediately. `Sinbad.flush()` uploads everything queued now.

Whatever is still queued when the page goes away is sent on `pagehide`, or when `visibilitychange` reports the page hidden, with `fetch(..., { keepalive: true })`. In headless Chrome, only such a request reached a cross-origin Leader during navigation: `navigator.sendBeacon` sends credentials, so its preflight needs `Access-Control-Allow-Credentials`, and a plain fetch can be cancelled. Browsers cap keepalive bodies at 64 KiB in total, which is about 280 Count reports or 24 Histogram reports of 100 buckets.

Each Aggregator's HPKE configuration is fetched once, on the first report that needs it, and shared by every task on that Aggregator. A manifest with twenty pages therefore costs two configuration requests, not forty, and one unreachable Aggregator affects only the tasks that use it. If the Leader answers `hpke-unknown-config-id`, Sinbad discards the cached lists, retrieves them again, and retries only the reports rejected that way, once, as DAP 19 Section 4.4.2.2 requires.

`createSiteAnalytics(config)` returns the same interface without the module-level singleton, plus `flush()` and `close()`. Call `close()` to remove the unload listeners.

A custom `fetch` receives a Web `Request`. To send it elsewhere, copy the body with `await request.arrayBuffer()`: passing the Request as the init of a new one turns the body into a stream, which browsers refuse over HTTP/1.1.

## Fetch transport

`sinbad/fetch` exports `send(request, options)`, `execute(preparedUpload, options)`, `fetchHpkeConfig(base, options)`, `fetchHpkeConfigs(task, options)`, and `readLimited(response, limit)`. Options take a custom `fetch`, extra `headers`, an abort `signal`, and `keepalive`. `execute()` sends an existing upload once; callers decide whether to retry it.

`sinbad/collector` exports `collect(collector, queryOrState, options)` and `executeCollection(preparedCollection, options)`. Collection credentials and the collector private key belong on a backend:

```js
import { Collector } from "dap-ts";
import { collect } from "sinbad/collector";

const collector = await Collector.create(task, { configId, privateKey });
const day = Date.UTC(2026, 8, 28);
const progress = await collect(
  collector,
  { start: day, end: day + 86_400_000 },
  { headers: { authorization: `Bearer ${collectorToken}` } },
);
if (progress.status === "pending") saveForLater(progress.state);
else console.log(progress.value, progress.reportCount);
```

`collect` polls up to `maxPolls` times (default 20), honouring `Retry-After` between `minDelayMs` and `maxDelayMs`, then returns the pending state to resume later.

The package builds ESM JavaScript and TypeScript declarations from strict TypeScript source. Everything else in the repository is TypeScript too and runs directly on Node 26, which strips types without a build step.

## Example aggregators

`server/aggregator.ts` runs a DAP 19 Leader or Helper for one task, with its own SQLite file and HPKE key. It is an example, not a hardened service.

The Leader checks each upload, stores the reports, and answers at once. A background loop builds aggregation jobs from stored reports, saves each job before sending it to the Helper, and commits each share once when the Helper answers. A Helper outage or a Leader restart only delays aggregation: saved jobs are sent again with the same bytes, and the Helper returns its stored response. The Leader serves `/hpke_config` with a one-day cache lifetime and answers CORS preflights on it and on the upload resource, so browsers on other origins can report.

A collection may span any number of buckets. The Leader keeps a collection job pending until its interval has closed, every report in it is aggregated, and the batch reaches the minimum size, then collects it and refuses later reports for it with `batch-collected`. An interval that overlaps an earlier collection fails with `batchOverlap`. Errors are RFC 9457 problem documents carrying DAP's error types, built with dap-ts's `problemResponse`.

```sh
cd ../dap-ts && npm ci && npm run build
cd ../sinbad && npm ci
docker compose -f compose.count.yaml up -d
```

The Leader listens on `127.0.0.1:9011` and the Helper on `127.0.0.1:9012`. Named Docker volumes keep their SQLite files across restarts. `GET /task` returns the task in manifest form. The task names fixed placeholder HTTPS hosts, so callers route requests to the loopback ports. The Compose token and verification key are fixed test values. Set `COLLECTOR_PUBLIC_KEY_HEX` to a raw X25519 public key before starting Compose to enable collection; the private key stays with the analytics backend. `VDAF` selects `count`, `sum`, or `histogram` (with `HISTOGRAM_LENGTH` and `HISTOGRAM_CHUNK_LENGTH`), and `MIN_BATCH_SIZE` defaults to 1, which publishes individual measurements.

`server/analytics.ts` is the collector backend for one metric. It needs `LEADER_URL`, `COLLECTOR_PRIVATE_KEY_HEX`, and `AUTH_TOKEN`, which must match the Leader's. It collects fixed windows of `WINDOW_MS` milliseconds (default one hour, a multiple of the task's time precision) as soon as the Leader reports them ready, and resumes pending ones after a restart. Choose a window that gathers at least the minimum batch: DAP collects each bucket once, so smaller windows cannot be merged later. An authenticated `POST /internal/collect?start=...` collects one window. `GET /api/analytics?metric=page_views&category=%2Fpricing&from=...&to=...` returns the completed windows and their `total`, with Unix-millisecond bounds and decimal-string values.

`npm run test:aggregate-server` exercises Count, Sum, and Histogram on loopback ports: a Leader crash while its job is at the Helper, identical and conflicting retries, a two-bucket collection, overlap and mismatch errors, and the analytics backend across restarts. With Compose running, `npm run bench:count -- sinbad 500 10 50` uploads 500 prepared Count reports in 10 uploads of 50 at concurrency 10, waits until the Leader has aggregated all of them, and reports upload latency, verified throughput, container CPU time, and peak memory. Start Compose with `VDAF=sum` or `VDAF=histogram HISTOGRAM_LENGTH=100 HISTOGRAM_CHUNK_LENGTH=10` and pass `sum` or `histogram` as the last argument for the other VDAFs. `npm run bench:count -- janus 500 10 50` runs the same workload against the pinned Janus DAP 18 image started by `scripts/up.sh`.

## Local demo

The demo is a page with one button that counts clicks through the example aggregators. With Compose running:

```sh
npm run demo
```

Open <http://localhost:8080>. The demo server builds the manifest from the Leader's task and forwards the page's DAP requests to the two roles. Set `LEADER_URL`, `HELPER_URL`, `HOST`, and `PORT` for other addresses. It binds to `127.0.0.1` by default and is for local development.

## Continuous integration

`.github/workflows/ci.yml` runs the unit tests, typecheck, lint, build, demo bundle, and the loopback aggregator tests. It builds against a checkout of the sibling dap-ts repository. Because both repositories are private, the default `GITHUB_TOKEN` cannot read dap-ts, so the workflow needs a repository secret `DAP_TS_TOKEN` with read access to it. Without that secret the workflow skips instead of failing on every push.

## Security and privacy

The dap-ts cryptography has not been audited, and this package is a prototype.

DAP hides what a measurement says. It does not hide that a client reported. The Leader sees each upload's source IP address, its arrival time, and its task ID, and DAP 19 Section 8 lists that metadata as a way for an Aggregator or a network observer to identify participating clients.

Sinbad selects a task per event name and per page path, so the task ID distinguishes which page a visitor loaded. Together with the source IP that gives the Leader a clickstream, which is the thing DAP is meant to prevent. Batching reduces how many requests carry that signal, but does not remove it.

Before running this against real visitors:

- Put an anonymizing proxy in front of the Leader and forward reports over Oblivious HTTP, as described in DAP 19 Section 8.4. The proxy, not an Aggregator, then sees the client address.
- Keep the page categories few and fixed. One task per arbitrary URL both breaks batch sizes and sharpens this signal.
- Set a minimum batch size that actually protects a single visitor, and a time precision and collection window coarse enough to reach it.
- Run the Leader and Helper as genuinely independent operators. DAP gives no privacy if they collude.

DAP does not provide differential privacy.

## License

[MIT](LICENSE). Janus is a separate MPL-2.0 project and is built from its pinned source by `scripts/up.sh` for benchmarks.
