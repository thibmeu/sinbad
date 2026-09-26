# Sinbad

A small analytics library and Fetch transport for [dap-ts](https://github.com/thibmeu/dap-ts).

```js
import { prio3Count, Task } from "dap-ts";
import { createCounter } from "sinbad";

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
const count = await createCounter(task);
const result = await count();
if (!result.ok) console.warn(result.rejected);
```

The task must already be provisioned on both aggregators. Each call sends one Prio3Count measurement with value `1`. `createCounter` fetches the leader and helper HPKE configurations once. It accepts a custom `fetch`, request `headers`, and an abort `signal`. It returns dap-ts's upload result.

## Fetch transport

`sinbad/fetch` exports `fetchHpkeConfigs(task)`, `execute(preparedUpload)`,
`toRequest()`, and `fromResponse()`. The first two accept a custom Fetch
implementation, headers, and an abort signal. `execute()` sends an existing
upload once; callers decide whether to retry it.

`sinbad/collector` exports `collect(collector, queryOrState)` and
`executeCollection(preparedCollection)`. Collection credentials and the
collector private key belong on a backend:

```js
import { Collector } from "dap-ts/collector";
import { collect } from "sinbad/collector";

const collector = new Collector(task, { configId, privateKey });
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

## License

[MIT](LICENSE). Janus is a separate MPL-2.0 project and is built from its pinned source by the local demo script.
