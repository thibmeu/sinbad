import { DAPClient, Task, type UploadResult } from "dap-ts";
import { execute, type FetchOptions, fetchHpkeConfigs } from "./fetch.js";

/** Create a counter for one provisioned Prio3Count task. */
export async function createCounter(
	task: Task<number>,
	options: FetchOptions = {},
): Promise<() => Promise<UploadResult>> {
	if (!(task instanceof Task) || task.vdaf.type !== "prio3-count")
		throw new TypeError("Expected a Prio3Count task");
	const hpke = await fetchHpkeConfigs(task, options);
	const client = new DAPClient(task, { hpke });
	return async () => {
		const report = await client.prepareReport(1);
		return execute(client.prepareUpload([report]), options);
	};
}
