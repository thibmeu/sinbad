import { DAPClient, Task } from "dap-ts";
import { execute, fetchHpkeConfigs } from "dap-ts/fetch";

/** Create a counter for one provisioned Prio3Count task. */
export async function createCounter(task, options = {}) {
  if (!(task instanceof Task)) throw new TypeError("Expected a DAP task");
  const hpke = await fetchHpkeConfigs(task, options);
  const client = new DAPClient(task, { hpke });
  return async function count() {
    const report = await client.prepareReport(1);
    return execute(client.prepareUpload([report]), options);
  };
}
