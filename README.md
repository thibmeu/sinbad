# Sinbad

A small analytics library and Fetch transport for [dap-ts](https://github.com/thibmeu/dap-ts).

```js
import { Sinbad } from "sinbad";

await Sinbad.init({ siteId: "my-site", endpoint: "https://analytics.example/" });
await Sinbad.page();
await Sinbad.track("signup");
await Sinbad.track("purchase", { value: 49 });
```

`init` fetches `GET https://analytics.example/sites/my-site/manifest` and nothing else, however many tasks the manifest names. `page()` uses `location.pathname` in a browser; pass a path in other runtimes. `track("purchase", { value: 49 })` sends an encrypted bounded Sum measurement. The value must be an integer in units chosen by the site, such as cents. Count events and pages send encrypted `1` measurements. Calls return Promises of dap-ts upload results, so callers can inspect `result.ok` or handle network errors.

An unknown event or page name, or properties an event does not take, logs a warning and does nothing. Analytics must not break the page it runs on. A measurement outside its task's bound is a programming error and still rejects.

## Batching

Reports for one task are collected for `batchMs` milliseconds (default 1000) and uploaded together, up to `maxBatch` reports (default 20) per request. Set `batchMs: 0` to send each call immediately. `Sinbad.flush()` uploads everything queued now.

Whatever is still queued when the page goes away is sent with `navigator.sendBeacon` on `pagehide` and on `visibilitychange` to hidden, so a report fired during navigation is not lost. A beacon cannot read the response, so those calls resolve with an empty result.

Each Aggregator's HPKE configuration is fetched once, on the first report that needs it, and shared by every task on that Aggregator. A manifest with twenty pages therefore costs two configuration requests, not forty, and one unreachable Aggregator affects only the tasks that use it. If the Leader answers with `hpke_unknown_config_id`, Sinbad discards the cached lists, retrieves them again, and retries the upload once with freshly prepared reports, as DAP 19 Section 4.4.2.2 requires.

`createSiteAnalytics(config)` returns the same interface without the module-level singleton, plus `flush()` and `close()`. Call `close()` to remove the unload listeners.

The manifest is JSON. Each entry contains a task configuration plus `vdaf: "count"` or `vdaf: "sum"`; Sum entries also contain `maxMeasurement`. For example:

```json
{
  "events": {
    "signup": { "id": "<task-id>", "leader": "https://leader.example/", "helper": "https://helper.example/", "timePrecision": 60, "minBatchSize": 100, "batchMode": "time-interval", "vdaf": "count" },
    "purchase": { "id": "<task-id>", "leader": "https://leader.example/", "helper": "https://helper.example/", "timePrecision": 60, "minBatchSize": 100, "batchMode": "time-interval", "vdaf": "sum", "maxMeasurement": 10000 }
  },
  "pages": {
    "/pricing": { "id": "<task-id>", "leader": "https://leader.example/", "helper": "https://helper.example/", "timePrecision": 60, "minBatchSize": 100, "batchMode": "time-interval", "vdaf": "count" }
  }
}
```

Replace each `<task-id>` with a separate provisioned task ID. Event names and paths select tasks locally; aggregators receive task IDs and encrypted reports. Other event properties and user identities are not supported. `createCounter(task)` remains available when the caller already has a Count task object.

## Fetch transport

`sinbad/fetch` exports `fetchHpkeConfigs(task)`, `execute(preparedUpload)`,
`toRequest()`, and `fromResponse()`. The first two accept a custom Fetch
implementation, headers, and an abort signal. All async operations return Promises. `execute()` sends an existing
upload once; callers decide whether to retry it.

`sinbad/collector` exports `collect(collector, queryOrState)` and
`executeCollection(preparedCollection)`. Collection credentials and the
collector private key belong on a backend:

```js
import { Collector } from "dap-ts";
import { collect } from "sinbad/collector";

const collector = await Collector.create(task, { configId, privateKey });
const progress = await collect(collector, { start: batchStart, duration: 1 }, {
  headers: { authorization: `Bearer ${collectorToken}` },
});
if (progress.status === "pending") saveForLater(progress.state);
else console.log(progress.count ?? progress.sum);
```

The package builds ESM JavaScript and TypeScript declarations from strict
TypeScript source. The demo remains plain HTML, CSS, and JavaScript.

## Local demo

The demo is plain HTML, CSS, and JavaScript. It uses the pinned Janus interop image from the dap-ts test suite. Janus in this image speaks DAP 18, so the demo uses dap-ts's explicit test-only compatibility option. It is for local development.

Keep `sinbad` and `dap-ts` as sibling directories, then build dap-ts and start the two aggregators:

```sh
cd ../dap-ts
npm ci
npm run build
cd ../sinbad
npm install
./scripts/up.sh
npm run demo
```

Open <http://localhost:8080>. The demo server provisions a new count task on both aggregators at startup and proxies DAP requests through the same origin. `docker compose down -v` stops the aggregators. You can set `PUBLIC_ORIGIN`, `LEADER_URL`, `HELPER_URL`, `HOST`, and `PORT` for a different address. The server binds to `127.0.0.1` by default.

The demo reports accepted uploads. Collection and display of aggregate totals are not implemented yet. DAP also needs a collector, batch policy, and independent leader and helper operations before it can serve real analytics. The dap-ts cryptography has not been audited.

## DAP 19 aggregator server example

`server/count.js` runs as a leader or helper. Each role has its own SQLite file
and HPKE key. It puts reports in each upload into one aggregation job,
saves the exact request, and commits each verified share once. A retry after a
lost response returns the saved bytes. It supports DAP 19 collection jobs and
encrypts each role's aggregate share to a provisioned collector key. Collection
closes one completed time window. The collection `start` parameter is a DAP
time-precision unit, not Unix seconds.

Build the sibling package, install Sinbad, and start both roles:

```sh
cd ../dap-ts && npm ci && npm run build
cd ../sinbad && npm ci
docker compose -f compose.count.yaml up -d
```

Errors from these roles are RFC 9457 problem documents carrying the DAP error
tokens from Section 3.6, so a client can tell `batchMismatch` from
`unrecognizedTask` without guessing from the status code.

The leader listens on `127.0.0.1:9011` and the helper on `127.0.0.1:9012`.
Named Docker volumes keep their SQLite files across restarts. The task uses
the DAP 19 configuration returned by `GET /task`; each role serves its HPKE
config at `GET /hpke_config`. The addresses in the task configuration are
fixed HTTPS names for local testing, so the caller routes requests to the
loopback ports. The Compose token and verification key are fixed test values.
Set `COLLECTOR_PUBLIC_KEY_HEX` to the raw X25519 public key before starting
Compose to enable collection. The private key stays in a separate backend.

`server/analytics.js` is that backend example. It needs `LEADER_URL`,
`COLLECTOR_PRIVATE_KEY_HEX`, and `AUTH_TOKEN`; set `DATA_FILE`, `METRIC`,
`CATEGORY`, and `VDAF` to configure one named metric and its SQLite database.
Its token must match the leader's token. It retries pending jobs after restarts
and scans closed windows with committed reports every minute. An authenticated
`POST /internal/collect?start=...` collects a specific older window.
`GET /api/analytics?metric=page_views&category=%2Fpricing&from=...&to=...`
returns completed windows and a `total`; all aggregate values and report counts
are decimal strings. `from` is inclusive, `to` is exclusive, and both use DAP
time-precision units. This example configures one task and category per process.

Run `npm run test:aggregate-server` to exercise Count, Sum, and Histogram
through role restarts, mixed batches, exact retries, and collection. The test
uses temporary SQLite files and loopback ports. Stop the Compose example
with `docker compose -f compose.count.yaml down`; add `-v` to discard its data.

With both roles running, `npm run bench:count -- sinbad 500 10 50` measures 500
prepared Count reports in 10 uploads of 50 at concurrency 10. It checks the stored bucket counts
and reports upload latency, verified throughput, container CPU time, and peak
memory. Set `BENCH_MINUTES_AGO` to use a different minute for another run.
For Sum, start Compose with `VDAF=sum` and pass `sum` as the benchmark's last
argument. For Histogram, use `VDAF=histogram HISTOGRAM_LENGTH=100 HISTOGRAM_CHUNK_LENGTH=10`
and pass `histogram`. Keep each run in a fresh, uncollected time bucket.

## Security and privacy

The dap-ts cryptography has not been audited, and this package is a prototype.

DAP hides what a measurement says. It does not hide that a client reported.
The Leader sees each upload's source IP address, its arrival time, and its task
ID, and DAP 19 Section 8 lists that metadata as a way for an Aggregator or a
network observer to identify participating clients.

Sinbad selects a task per event name and per page path, so the task ID
distinguishes which page a visitor loaded. Together with the source IP that
gives the Leader a clickstream, which is the thing DAP is meant to prevent.
Batching reduces how many requests carry that signal, but does not remove it.

Before running this against real visitors:

- Put an anonymizing proxy in front of the Leader and forward reports over
  Oblivious HTTP, as described in DAP 19 Section 8.4. The proxy, not an
  Aggregator, then sees the client address.
- Keep the page categories few and fixed. One task per arbitrary URL both
  breaks batch sizes and sharpens this signal.
- Set a minimum batch size that actually protects a single visitor. The
  example aggregators default to 1, which publishes individual measurements;
  set `MIN_BATCH_SIZE` before exposing them.
- Run the Leader and Helper as genuinely independent operators. DAP gives no
  privacy if they collude.

DAP does not provide differential privacy.

## License

[MIT](LICENSE). Janus is a separate MPL-2.0 project and is built from its pinned source by the local demo script.
