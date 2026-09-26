# Sinbad

A small analytics library and Fetch transport for [dap-ts](https://github.com/thibmeu/dap-ts).

```js
import { prio3Count, Task } from "dap-ts";
import { createCounter } from "sinbad";
import { Effect } from "effect";

const task = Task.create({
  id: "8BY0RzZMzxvA46_8ymhzycOB9krN-QIGYvg_RsByGec",
  info: "page-views-v1",
  leader: "https://leader.example/",
  helper: "https://helper.example/",
  timePrecision: 60,
  minBatchSize: 100,
  batchMode: "time-interval",
  vdaf: prio3Count(),
});
const count = await Effect.runPromise(createCounter(task));
const result = await Effect.runPromise(count());
if (!result.ok) console.warn(result.rejected);
```

The task must already be provisioned on both aggregators. Each call sends one Prio3Count measurement with value `1`. `createCounter` fetches the leader and helper HPKE configurations once. It accepts a custom `fetch`, request `headers`, and an abort `signal`. It returns an Effect whose counter returns an Effect of dap-ts's upload result. Effects are lazy; run them at the application edge with `Effect.runPromise` or compose them with other Effects.

## Fetch transport

`sinbad/fetch` exports `fetchHpkeConfigs(task)`, `execute(preparedUpload)`,
`toRequest()`, and `fromResponse()`. The first two accept a custom Fetch
implementation, headers, and an abort signal. All async operations return Effects. `execute()` sends an existing
upload once; callers decide whether to retry it.

`sinbad/collector` exports `collect(collector, queryOrState)` and
`executeCollection(preparedCollection)`. Collection credentials and the
collector private key belong on a backend:

```js
import { Collector } from "dap-ts";
import { collect } from "sinbad/collector";
import { Effect } from "effect";

const collector = await Collector.create(task, { configId, privateKey });
const progress = await Effect.runPromise(collect(collector, { start: batchStart, duration: 1 }, {
  headers: { authorization: `Bearer ${collectorToken}` },
}));
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
lost response returns the saved bytes. This example has an internal bucket
collection endpoint; it does not implement DAP collection jobs.
The collection `start` parameter is a DAP time-precision unit, not Unix seconds.

Build the sibling package, install Sinbad, and start both roles:

```sh
cd ../dap-ts && npm ci && npm run build
cd ../sinbad && npm ci
docker compose -f compose.count.yaml up -d
```

The leader listens on `127.0.0.1:9011` and the helper on `127.0.0.1:9012`.
Named Docker volumes keep their SQLite files across restarts. The task uses
the DAP 19 configuration returned by `GET /task`; each role serves its HPKE
config at `GET /hpke_config`. The addresses in the task configuration are
fixed HTTPS names for local testing, so the caller routes requests to the
loopback ports. The Compose token and verification key are fixed test values.

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

## License

[MIT](LICENSE). Janus is a separate MPL-2.0 project and is built from its pinned source by the local demo script.
