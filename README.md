# ⛵ Sinbad

[![NPM](https://img.shields.io/npm/v/sinbad?style=flat-square)](https://www.npmjs.com/package/sinbad)
[![License](https://img.shields.io/npm/l/sinbad?style=flat-square)](LICENSE)

Web analytics over the Distributed Aggregation Protocol, built on
[@thibmeu/dap](https://github.com/thibmeu/dap-ts). Pages report encrypted
counts and values to two aggregators, and the site owner collects only
aggregates.

## Features

- **Browser analytics**: page views, events, and bounded values, each reported to its own DAP task
- **One request to start**: a site manifest names every task; each aggregator's HPKE configuration is fetched once
- **Batched delivery**: reports are grouped per task and still sent when the page closes
- **Fetch transport**: upload, HPKE retrieval, and collection polling for @thibmeu/dap
- **Aggregators**: a Leader and Helper over synchronous SQLite, with example servers

## Installation

```bash
npm install sinbad @thibmeu/dap
```

## Quick start

```typescript
import { Sinbad } from "sinbad";

Sinbad.init({ siteId: "my-site", endpoint: "https://analytics.example/" });
Sinbad.page();
Sinbad.track("signup");
Sinbad.track("purchase", { value: 49 });
```

`init` fetches `https://analytics.example/sites/my-site/manifest` and nothing
else. Calls made before it resolves wait for it, so a page can track as soon
as it loads.

`page()` counts a view of `location.pathname`; pass a path in other runtimes.
`track(name)` counts an event. `track(name, { value })` adds an integer to a
Sum task, in units the site chooses such as cents, or reports a bucket index
to a Histogram task.

Each call resolves with `accepted`, `rejected`, `ok`, and `sent`. An unknown
event or page, or properties an event does not take, logs a warning and
resolves with `sent: false`, as do calls after a failed `init`: analytics must
not break the page. A value outside its task's bound rejects, and so do
network failures. Sinbad does not retry them.

`createSiteAnalytics(config)` returns the same interface without the
module-level singleton, plus `flush()` and `close()`, which removes the unload
listeners.

## Manifest

The manifest maps event names and page paths to provisioned tasks. Each entry
holds the task ID and its encoded DAP task configuration, both URL-safe Base64
without padding, as `task.id` and `task.encodeConfiguration()` produce them.
The configuration fixes the VDAF, so Sinbad needs nothing else.

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

Use one task per entry; pages must use Count tasks. Names and paths select a
task locally; aggregators only see task IDs and encrypted reports.

## Delivery

Sinbad encrypts each report when `track()` or `page()` is called, then
uploads a task's reports together every `batchMs` milliseconds (default 1000),
up to `maxBatch` per request (default 20). `batchMs: 0` sends each call at
once and `Sinbad.flush()` sends everything queued.

On `pagehide`, or when the page becomes hidden, queued reports go out with
`fetch(..., { keepalive: true })`. A report still waiting for keys or
encryption at that point may be lost. Browsers cap keepalive bodies at 64 KiB
in total, about 280 Count reports or 24 Histogram reports of 100 buckets.
`navigator.sendBeacon` is not used: it sends credentials, which a
cross-origin Leader would have to allow.

Each aggregator's HPKE configuration is fetched once, on the first report that
needs it, and shared by every task on that aggregator. If the Leader answers
`hpke-unknown-config-id`, Sinbad refetches the configurations, bypassing the
HTTP cache, and retries the rejected reports once, as
[draft-ietf-ppm-dap-19, Section 4.4.2.2](https://www.ietf.org/archive/id/draft-ietf-ppm-dap-19.html#section-4.4.2.2)
recommends.

A custom `fetch` receives a Web `Request`. To send it elsewhere, copy the body
with `await request.arrayBuffer()`, since browsers refuse a streamed body over
HTTP/1.1. That copy can delay dispatch during `pagehide`.

## Collection

`sinbad/collector` polls a collection job for a @thibmeu/dap `Collector`. Keep
the collector's private key and credentials on a backend.

```typescript
import { Collector } from "@thibmeu/dap";
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

`collect` polls up to `maxPolls` times (default 20), honouring `Retry-After`
between `minDelayMs` and `maxDelayMs`, then returns the pending state to
resume later.

`sinbad/fetch` has the lower-level pieces: `send`, `execute` for a prepared
upload, `fetchHpkeConfig`, `fetchHpkeConfigs`, and `readLimited`. They take a
custom `fetch`, extra `headers`, an abort `signal`, and `keepalive`.
In `createSiteAnalytics`, `headers` are sent only with the manifest request,
never to aggregators.

## Aggregators

`sinbad/aggregator` runs a DAP Leader or Helper for one task over synchronous
SQLite, such as `node:sqlite`. The Leader's `helper.url` must be HTTPS, or
HTTP on loopback for local development.

```typescript
import { createAggregator } from "sinbad/aggregator";

const leader = await createAggregator({
  role: "leader",
  task,
  storage, // { query(sql, ...params), transaction(fn) }
  token: collectorToken,
  helper: { url: "https://helper.example/", token: leaderToken },
  hpkeKeys: [{ configId: 7, privateKey }],
  verifyKeys: [{ id: 0, key: verifyKey }],
  collector: collectorHpkeConfig,
});

// Serve DAP requests with leader.fetch(request), and aggregate one pass at a
// time. A failed pass is safe to run again.
for (;;) {
  await leader.aggregate().catch(console.error);
  await new Promise((resolve) => setTimeout(resolve, 1000));
}
```

The Leader stores uploads and answers at once. `aggregate()` sends saved jobs
and builds new ones from stored reports. A job is saved before it is sent and
each share is committed once, so a Helper outage or a Leader restart only
delays aggregation. Pass `schedule` to be told when an upload brings new work.
The Leader serves `/hpke_config` and accepts cross-origin uploads.

A collection may span many buckets. The Leader completes it once the interval
has closed, all its reports are aggregated, and the batch reaches the minimum
size; later reports for it get `batch-collected`, and overlapping collections
get `batchOverlap`. Errors are RFC 9457 problem documents.

## Examples

`examples/aggregator.ts` runs one role on `node:sqlite`, and
`examples/analytics.ts` is a collector backend that collects fixed windows and
serves the totals. Both are examples, not hardened services.

```sh
npm ci
docker compose -f examples/compose.yaml up -d  # Leader on :9011, Helper on :9012
npm run examples:site                           # http://localhost:8080
```

Set `VDAF` to `count`, `sum`, or `histogram`, and `COLLECTOR_PUBLIC_KEY_HEX`
to enable collection. `MIN_BATCH_SIZE` defaults to 1, which publishes
individual measurements. The Compose token and verification key are fixed
test values.

`npm run test:aggregate-server` exercises crashes, retries, and collection for
each VDAF on loopback ports. `npm run bench:count` measures upload and
aggregation throughput against Compose.

## Security considerations

**Not audited.** @thibmeu/dap has not been audited either.

DAP hides what a client reported, not that it reported. The Leader sees each
upload's source IP, arrival time, and task ID
([draft-ietf-ppm-dap-19, Section 8](https://www.ietf.org/archive/id/draft-ietf-ppm-dap-19.html#section-8)).
Sinbad uses a task per page path, so the task ID says which page a visitor
loaded, and with the source IP that is a clickstream. Batching reduces the
number of requests but does not remove the signal.

Before tracking real visitors:

- Send reports through an Oblivious HTTP relay ([Section 8.4](https://www.ietf.org/archive/id/draft-ietf-ppm-dap-19.html#section-8.4)), so the relay sees the client address instead of the Leader.
- Keep page categories few and fixed. A task per arbitrary URL both shrinks batches and sharpens the signal.
- Set a minimum batch size that protects a single visitor, with a time precision and collection window coarse enough to reach it.
- Run the Leader and Helper as independent operators. DAP gives no privacy if they collude.

DAP does not provide differential privacy.

## License

[MIT](LICENSE). Janus, used for benchmarks, is a separate MPL-2.0 project built from pinned source by `scripts/up.sh`.
