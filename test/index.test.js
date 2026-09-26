import assert from "node:assert/strict";
import { test } from "node:test";
import { prio3Count, Task } from "dap-ts";
import { encodeHpkeConfigList } from "dap-ts/messages";
import { createCounter } from "../src/index.js";

test("fetches both HPKE lists once and uploads one report per count", async () => {
  const task = Task.create({
    id: "8BY0RzZMzxvA46_8ymhzycOB9krN-QIGYvg_RsByGec",
    leader: "https://leader.example/",
    helper: "https://helper.example/",
    timePrecision: 60,
    minBatchSize: 100,
    batchMode: "time-interval",
    vdaf: prio3Count(),
  });
  const list = encodeHpkeConfigList([{
    id: 1,
    kemId: 32,
    kdfId: 1,
    aeadId: 1,
    publicKey: Uint8Array.fromHex("37fda3567bdbd628e88668c3c8d7e97fa41e9b4fc1409b43f8f051270229af08"),
  }]);
  const requests = [];
  const mockFetch = async (request) => {
    requests.push(request);
    if (request.method === "GET") {
      return new Response(list, { headers: { "content-type": "application/ppm-dap;message=hpke-config-list" } });
    }
    return new Response(null, { status: 200 });
  };
  const count = await createCounter(task, { fetch: mockFetch });
  assert.equal((await count()).accepted.length, 1);
  assert.equal((await count()).accepted.length, 1);
  assert.deepEqual(requests.map((request) => request.method), ["GET", "GET", "POST", "POST"]);
  assert.notDeepEqual(new Uint8Array(await requests[2].arrayBuffer()), new Uint8Array(await requests[3].arrayBuffer()));
});
